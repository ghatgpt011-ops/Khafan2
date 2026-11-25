import asyncio
import json
import os
import re
import time
from datetime import datetime
from threading import Thread
from urllib.parse import urlparse

from flask import Flask, jsonify, request
from splusthon import SoroushClient
from splusthon.sessions import StringSession

try:
    import psutil
except Exception:
    psutil = None

PORT = int(os.environ.get("PORT", 8080))
SESSION_FILE = "soroush_session.txt"
JOBS_FILE = "fonex_jobs.json"
MAX_ACTIVE_SPAM = 3
MAX_TARGETS_PER_JOB = 3

KICK_KEYWORDS = (
    "session",
    "auth",
    "unauth",
    "revok",
    "terminat",
    "log out",
    "logged out",
    "logout",
    "invalid",
    "expired",
    "401",
    "403",
    "خروج",
    "منقضی",
    "نامعتبر",
)

app = Flask(__name__)

accounts = []
groups = []
jobs = []
job_seq = 0

state = {
    "ping": "-",
    "cpu": "0",
    "ram": "0",
    "auth_msg": "آماده ورود شماره",
    "phone": "",
    "healer_status": "آماده",
    "soroush_connected": False,
    "kicked_out": False,
}

client = None
phone_code_hash = None
client_ready = False
bot_loop = None
op_lock = asyncio.Lock()


@app.after_request
def add_cors(resp):
    resp.headers["Access-Control-Allow-Origin"] = "*"
    resp.headers["Access-Control-Allow-Headers"] = "*"
    resp.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
    return resp


def now_str():
    return datetime.now().strftime("%H:%M:%S")


def add_log(job, text):
    logs = job.get("logs")
    if not isinstance(logs, list):
        logs = []
    logs.append(f"[{now_str()}] {text}")
    job["logs"] = logs[-30:]


def save_session(s):
    try:
        with open(SESSION_FILE, "w", encoding="utf-8") as f:
            f.write(s or "")
    except Exception:
        pass


def load_session():
    if os.path.exists(SESSION_FILE):
        try:
            with open(SESSION_FILE, "r", encoding="utf-8") as f:
                return f.read().strip()
        except Exception:
            pass
    return ""


def save_jobs():
    try:
        with open(JOBS_FILE, "w", encoding="utf-8") as f:
            json.dump(jobs, f, ensure_ascii=False, indent=2)
    except Exception:
        pass


def load_jobs():
    global job_seq
    cleaned = []
    if os.path.exists(JOBS_FILE):
        try:
            with open(JOBS_FILE, "r", encoding="utf-8") as f:
                arr = json.load(f)
                if isinstance(arr, list):
                    for j in arr:
                        if not isinstance(j, dict):
                            continue
                        if j.get("mode") != "spam":
                            continue
                        j["mode"] = "spam"
                        j["targets"] = (j.get("targets") or [])[:MAX_TARGETS_PER_JOB]
                        j.setdefault("t_idx", 0)
                        j.setdefault("sent_burst", 0)
                        j.setdefault("heal_state", "normal")
                        j.setdefault("last_error", "")
                        j.setdefault("ok", 0)
                        j.setdefault("fail", 0)
                        j.setdefault("logs", [])
                        cleaned.append(j)
        except Exception:
            cleaned = []
    job_seq = len(cleaned)
    return cleaned


def clean_target(raw):
    if isinstance(raw, (list, tuple, dict)):
        raw = raw[0] if isinstance(raw, (list, tuple)) and raw else ""
    s = str(raw or "").strip().replace("\u200f", "").replace("\u200e", "")
    if not s:
        return ""
    if s.endswith("-") and s[:-1].isdigit():
        s = "-" + s[:-1]
    if "splus.ir" in s or s.startswith("http://") or s.startswith("https://"):
        url = s if s.startswith("http") else "https://" + s
        try:
            parts = [p for p in urlparse(url).path.split("/") if p]
            if parts:
                if parts[0] == "c" and len(parts) >= 2:
                    cid = parts[1].strip()
                    if cid.endswith("-") and cid[:-1].isdigit():
                        cid = "-" + cid[:-1]
                    if cid.lstrip("-").isdigit():
                        return f"-100{cid.lstrip('-')}" if not cid.startswith("-") else cid
                if len(parts) >= 2 and parts[-1].isdigit():
                    return parts[-2].strip()
                return parts[0].strip()
        except Exception:
            pass
    m = re.search(r"(?:https?://)?(?:www\.)?splus\.ir/([^/\s?#]+)", s)
    if m:
        return m.group(1).strip()
    if s.startswith("@"):
        return s[1:].strip()
    return s


def peer_candidates(raw):
    base = clean_target(raw)
    if not base:
        return []
    out = []
    if base.lstrip("-").isdigit():
        num = int(base)
        abs_s = base.lstrip("-")
        for cand in (num, int(abs_s), int(f"-100{abs_s}"), int(f"-{abs_s}")):
            if cand not in out:
                out.append(cand)
    else:
        out.append(base)
    return out


def is_kicked_error(err):
    txt = str(err or "").lower()
    return any(k in txt for k in KICK_KEYWORDS)


def mark_kicked_out(reason="اکانت از سشن خارج شد."):
    global client_ready
    client_ready = False
    state["soroush_connected"] = False
    state["kicked_out"] = True
    state["healer_status"] = "خارج شده از اکانت"
    state["auth_msg"] = f"خروج از اکانت تشخیص داده شد: {reason}"
    accounts.clear()
    save_session("")
    changed = False
    for j in jobs:
        if j.get("running"):
            j["running"] = False
            j["heal_state"] = "kicked"
            j["last_error"] = "خروج از اکانت"
            add_log(j, "KICKED -> متوقف شد (خروج از اکانت)")
            changed = True
    if changed:
        save_jobs()


def run_async(coro):
    if bot_loop and not bot_loop.is_closed():
        return asyncio.run_coroutine_threadsafe(coro, bot_loop)


async def try_reconnect_session():
    global client, client_ready
    sess = load_session()
    if not sess:
        return False
    try:
        if client is not None and hasattr(client, "disconnect"):
            try:
                r = client.disconnect()
                if asyncio.iscoroutine(r):
                    await asyncio.wait_for(r, timeout=4)
            except Exception:
                pass
        c = SoroushClient(StringSession(sess))
        t0 = time.perf_counter()
        await asyncio.wait_for(c.connect(), timeout=14)
        if hasattr(c, "get_me"):
            me = await asyncio.wait_for(c.get_me(), timeout=6)
            if me is None:
                mark_kicked_out("سشن نامعتبر است.")
                return False
            name = getattr(me, "first_name", None) or state.get("phone") or "Account"
            accounts[:] = [{"name": str(name), "phone": state.get("phone") or str(name)}]
        client = c
        client_ready = True
        state["kicked_out"] = False
        state["soroush_connected"] = True
        state["healer_status"] = "متصل به سروش"
        state["ping"] = f"{int((time.perf_counter() - t0) * 1000)} ms"
        return True
    except Exception as e:
        if is_kicked_error(e):
            mark_kicked_out(str(e)[:80])
        return False


async def check_alive():
    if not client_ready or client is None:
        return False
    if not hasattr(client, "get_me"):
        return True
    try:
        t0 = time.perf_counter()
        me = await asyncio.wait_for(client.get_me(), timeout=5)
        state["ping"] = f"{int((time.perf_counter() - t0) * 1000)} ms"
        if me is None:
            mark_kicked_out("پاسخ get_me خالی است.")
            return False
        state["kicked_out"] = False
        state["soroush_connected"] = True
        state["healer_status"] = "متصل به سروش"
        return True
    except Exception as e:
        if is_kicked_error(e):
            mark_kicked_out(str(e)[:80])
        return False


async def do_send_code(phone):
    global client, phone_code_hash, client_ready
    state["phone"] = phone
    state["kicked_out"] = False
    state["auth_msg"] = "در حال ارسال کد..."
    try:
        if client is not None and hasattr(client, "disconnect"):
            try:
                r = client.disconnect()
                if asyncio.iscoroutine(r):
                    await asyncio.wait_for(r, timeout=4)
            except Exception:
                pass
        c = SoroushClient(StringSession())
        client = c
        client_ready = False
        await asyncio.wait_for(c.connect(), timeout=15)
        if hasattr(c, "send_code_request"):
            res = await asyncio.wait_for(c.send_code_request(phone), timeout=15)
        elif hasattr(c, "send_code"):
            res = await asyncio.wait_for(c.send_code(phone), timeout=15)
        else:
            res = await asyncio.wait_for(c.sign_in(phone=phone), timeout=15)
        phone_code_hash = getattr(res, "phone_code_hash", None)
        state["auth_msg"] = "کد ارسال شد؛ کد را وارد کنید."
    except Exception as e:
        state["auth_msg"] = f"خطا در ارسال کد: {e}"


async def do_verify_code(code):
    global phone_code_hash, client_ready
    phone = state.get("phone", "").strip()
    state["auth_msg"] = "در حال تأیید کد..."
    try:
        c = client
        if not c:
            raise RuntimeError("کلاینت آماده نیست.")
        signed = False
        if phone_code_hash:
            for fn in (
                lambda: c.sign_in(phone=phone, code=code, phone_code_hash=phone_code_hash),
                lambda: c.sign_in(phone, code, phone_code_hash),
            ):
                try:
                    await asyncio.wait_for(fn(), timeout=15)
                    signed = True
                    break
                except TypeError:
                    continue
        if not signed:
            await asyncio.wait_for(c.sign_in(phone=phone, code=code), timeout=15)

        sess = c.session.save() if getattr(c, "session", None) else ""
        if sess:
            save_session(sess)
        client_ready = True
        state["kicked_out"] = False
        state["soroush_connected"] = True
        state["healer_status"] = "متصل به سروش"
        phone_code_hash = None
        name = phone
        try:
            me = await asyncio.wait_for(c.get_me(), timeout=5)
            if me and getattr(me, "first_name", None):
                name = str(me.first_name)
        except Exception:
            pass
        accounts[:] = [{"name": name, "phone": phone}]
        state["auth_msg"] = f"اکانت {name} متصل شد."
    except Exception as e:
        state["auth_msg"] = f"خطا در تأیید کد: {e}"


async def refresh_groups():
    global groups
    if not await check_alive():
        return
    try:
        if hasattr(client, "get_dialogs"):
            dialogs = await asyncio.wait_for(client.get_dialogs(limit=100), timeout=6)
            items = []
            seen = set()
            for d in list(dialogs or []):
                did = str(getattr(d, "id", "") or "")
                if not did or did in seen:
                    continue
                seen.add(did)
                name = getattr(d, "name", None) or getattr(d, "title", None) or did
                items.append({"id": did, "name": str(name)})
            if items:
                groups[:] = items
    except Exception as e:
        if is_kicked_error(e):
            mark_kicked_out(str(e)[:80])


async def send_to_target_once(raw_target, payload):
    if not client_ready or client is None:
        raise RuntimeError("کلاینت آماده نیست.")
    cands = peer_candidates(raw_target)
    if not cands:
        raise ValueError("مقصد نامعتبر است.")

    last_err = None
    for peer in cands:
        try:
            res = client.send_message(peer, str(payload))
            if asyncio.iscoroutine(res):
                await asyncio.wait_for(res, timeout=10)
            return peer
        except Exception as e:
            last_err = e
            if is_kicked_error(e):
                raise
            continue
    raise last_err if last_err else RuntimeError("ارسال ناموفق بود.")


async def smart_heal(job, raw_target, err):
    if is_kicked_error(err):
        mark_kicked_out(str(err)[:80])
        return False

    job["heal_state"] = "healing"
    job["last_error"] = str(err)[:90]
    add_log(job, f"HEAL -> اصلاح خطا و ادامه روی {clean_target(raw_target)}...")

    # اصلاح ۱: نرمال‌سازی مقصد و تلاش مجدد
    normalized = clean_target(raw_target)
    if normalized:
        try:
            await send_to_target_once(normalized, job.get("payload", ""))
            job["heal_state"] = "resumed"
            job["last_error"] = ""
            add_log(job, f"FIXED & RESUMED -> {normalized}")
            return True
        except Exception as e1:
            if is_kicked_error(e1):
                mark_kicked_out(str(e1)[:80])
                return False

    # اصلاح ۲: بازیابی اتصال کلاینت و ادامه همان مقصد و متن
    reconnected = await try_reconnect_session()
    if reconnected:
        try:
            await send_to_target_once(normalized or raw_target, job.get("payload", ""))
            job["heal_state"] = "resumed"
            job["last_error"] = ""
            add_log(job, f"RECONNECTED & RESUMED -> {normalized or raw_target}")
            return True
        except Exception as e2:
            if is_kicked_error(e2):
                mark_kicked_out(str(e2)[:80])
                return False
            job["last_error"] = str(e2)[:90]

    job["heal_state"] = "normal"
    return False


async def run_spam_worker(job):
    while job in jobs:
        if not job.get("running"):
            await asyncio.sleep(0.25)
            continue

        if state.get("kicked_out") or not client_ready:
            job["running"] = False
            job["heal_state"] = "kicked"
            job["last_error"] = "خروج از اکانت"
            add_log(job, "STOP -> خروج از اکانت")
            save_jobs()
            continue

        max_c = int(job.get("max_count") or 0)
        if max_c > 0 and (int(job.get("ok") or 0) + int(job.get("fail") or 0)) >= max_c:
            job["running"] = False
            job["heal_state"] = "completed"
            save_jobs()
            continue

        targets = [clean_target(x) for x in (job.get("targets") or [])[:MAX_TARGETS_PER_JOB] if clean_target(x)]
        if not targets:
            job["running"] = False
            add_log(job, "STOP -> مقصد معتبر نیست")
            save_jobs()
            continue

        idx = int(job.get("t_idx") or 0) % len(targets)
        current_target = targets[idx]
        payload = str(job.get("payload") or "")

        async with op_lock:
            try:
                await send_to_target_once(current_target, payload)
                job["ok"] = int(job.get("ok") or 0) + 1
                job["t_idx"] = (idx + 1) % len(targets)
                job["sent_burst"] = int(job.get("sent_burst") or 0) + 1
                job["heal_state"] = "running"
                job["last_error"] = ""
                add_log(job, f"SPAM OK -> {current_target}")
            except Exception as e:
                err_txt = str(e)
                healed = await smart_heal(job, current_target, e)
                if healed:
                    job["ok"] = int(job.get("ok") or 0) + 1
                    job["t_idx"] = (idx + 1) % len(targets)
                    job["sent_burst"] = int(job.get("sent_burst") or 0) + 1
                else:
                    job["fail"] = int(job.get("fail") or 0) + 1
                    job["last_error"] = err_txt[:90]
                    # حتی در خطای معمولی، در چرخه بعدی حرکت می‌کند تا تسک قفل نشود
                    job["t_idx"] = (idx + 1) % len(targets)
                    add_log(job, f"ERR -> {err_txt[:75]}")

        rest_every = int(job.get("rest_every") or 0)
        rest_ms = int(job.get("rest_ms") or 0)
        if rest_every > 0 and rest_ms > 0 and int(job.get("sent_burst") or 0) >= rest_every:
            job["sent_burst"] = 0
            await asyncio.sleep(rest_ms / 1000.0)
            continue

        await asyncio.sleep(max(50, int(job.get("speed_ms") or 500)) / 1000.0)


async def watcher_loop():
    while True:
        await asyncio.sleep(8)
        if client_ready and client is not None:
            await check_alive()


async def supervisor():
    active = {}
    asyncio.create_task(watcher_loop())
    while True:
        ids = {j["id"] for j in jobs}
        for k in list(active.keys()):
            if k not in ids or active[k].done():
                active.pop(k, None)
        for j in list(jobs):
            if j["id"] not in active:
                active[j["id"]] = asyncio.create_task(run_spam_worker(j))
        await asyncio.sleep(0.4)


async def async_main():
    global bot_loop
    bot_loop = asyncio.get_running_loop()
    asyncio.create_task(supervisor())
    while True:
        await asyncio.sleep(2)


def start_bot_thread():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    while True:
        try:
            loop.run_until_complete(async_main())
        except Exception:
            time.sleep(1)


@app.route("/")
@app.route("/health")
def health():
    return "OK", 200


@app.route("/api/send_phone")
def api_send_phone():
    phone = (request.args.get("phone") or "").strip()
    fut = run_async(do_send_code(phone))
    if fut:
        try:
            fut.result(timeout=18)
        except Exception:
            pass
    return jsonify({"ok": True, "auth_msg": state["auth_msg"]})


@app.route("/api/verify_code")
def api_verify_code():
    code = (request.args.get("code") or "").strip()
    fut = run_async(do_verify_code(code))
    if fut:
        try:
            fut.result(timeout=18)
        except Exception:
            pass
    return jsonify({"ok": True, "auth_msg": state["auth_msg"]})


@app.route("/api/del_account")
def api_del_account():
    global client_ready
    idx = int(request.args.get("index", -1))
    if 0 <= idx < len(accounts):
        accounts.pop(idx)
    save_session("")
    client_ready = False
    state["soroush_connected"] = False
    state["kicked_out"] = False
    state["healer_status"] = "حذف‌شده"
    return jsonify({"ok": True})


@app.route("/api/groups")
def api_groups():
    fut = run_async(refresh_groups())
    if fut:
        try:
            fut.result(timeout=7)
        except Exception:
            pass
    return jsonify(groups)


@app.route("/api/add_job")
def api_add_job():
    global job_seq
    if state.get("kicked_out") or not client_ready or not accounts:
        return jsonify({"ok": False, "msg": "اکانت متصل نیست یا از سشن خارج شده است."})

    payload = (request.args.get("payload") or "").strip()
    raw_targets = (request.args.get("targets") or "").strip()
    targets = [clean_target(x) for x in re.split(r"[,\n]+", raw_targets) if clean_target(x)]
    if not targets or not payload:
        return jsonify({"ok": False, "msg": "متن اسپم و مقصد را وارد کنید."})

    if len(targets) > MAX_TARGETS_PER_JOB:
        return jsonify({"ok": False, "msg": f"حداکثر {MAX_TARGETS_PER_JOB} مقصد در هر اسپم مجاز است."})

    active_spam = sum(1 for j in jobs if j.get("running") and j.get("mode") == "spam")
    if active_spam >= MAX_ACTIVE_SPAM:
        return jsonify({"ok": False, "msg": f"فقط {MAX_ACTIVE_SPAM} اسپم هم‌زمان قابل اجراست."})

    job_seq += 1
    job = {
        "id": str(100 + job_seq),
        "mode": "spam",
        "payload": payload,
        "targets": targets[:MAX_TARGETS_PER_JOB],
        "t_idx": 0,
        "sent_burst": 0,
        "speed_ms": max(50, int(request.args.get("speed_ms") or 500)),
        "max_count": max(0, int(request.args.get("max_count") or 0)),
        "rest_every": max(0, int(request.args.get("rest_every") or 0)),
        "rest_ms": max(0, int(request.args.get("rest_ms") or 0)),
        "running": True,
        "heal_state": "running",
        "last_error": "",
        "ok": 0,
        "fail": 0,
        "logs": [f"[{now_str()}] started (spam)"],
    }
    jobs.append(job)
    save_jobs()
    return jsonify({"ok": True, "msg": f"تسک اسپم #{job['id']} شروع شد."})


@app.route("/api/job_ctrl")
def api_job_ctrl():
    jid = (request.args.get("id") or "").strip()
    act = (request.args.get("act") or "").strip()
    for j in jobs:
        if j["id"] == jid:
            if act == "toggle":
                if not j.get("running"):
                    if state.get("kicked_out") or not client_ready or not accounts:
                        return jsonify({"ok": False, "msg": "اکانت از سشن خارج شده یا متصل نیست."})
                    active_spam = sum(1 for x in jobs if x.get("running") and x.get("mode") == "spam")
                    if active_spam >= MAX_ACTIVE_SPAM:
                        return jsonify({"ok": False, "msg": f"فقط {MAX_ACTIVE_SPAM} اسپم هم‌زمان قابل اجراست."})
                j["running"] = not j.get("running", False)
                j["heal_state"] = "running" if j["running"] else "paused"
            elif act == "delete":
                jobs.remove(j)
            break
    save_jobs()
    return jsonify({"ok": True})


@app.route("/api/state")
def api_state():
    if psutil:
        try:
            state["cpu"] = str(int(psutil.cpu_percent(interval=None)))
            state["ram"] = str(int(psutil.virtual_memory().percent))
        except Exception:
            pass
    return jsonify({
        "ping": state["ping"],
        "cpu": state["cpu"],
        "ram": state["ram"],
        "healer_status": state["healer_status"],
        "soroush_connected": state["soroush_connected"],
        "kicked_out": state["kicked_out"],
        "auth_msg": state["auth_msg"],
        "accounts": accounts,
        "groups": groups,
        "jobs": jobs,
    })


loaded_jobs = load_jobs()
jobs[:] = loaded_jobs if isinstance(loaded_jobs, list) else []

Thread(target=start_bot_thread, daemon=True).start()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT, threaded=True)
