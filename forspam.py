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
LIMITED_MODES = {"forward_msg", "spam"}

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
    global jobs, job_seq
    if os.path.exists(JOBS_FILE):
        try:
            with open(JOBS_FILE, "r", encoding="utf-8") as f:
                arr = json.load(f)
                if isinstance(arr, list):
                    cleaned = []
                    for j in arr:
                        if j.get("mode") == "reply_loop":
                            continue
                        j.setdefault("heal_state", "normal")
                        j.setdefault("last_error", "")
                        cleaned.append(j)
                    jobs = cleaned
                    job_seq = len(jobs)
        except Exception:
            pass


def normalize_id_str(raw):
    s = str(raw or "").strip().replace("\u200f", "").replace("\u200e", "")
    if s.endswith("-") and s[:-1].isdigit():
        s = "-" + s[:-1]
    return s


def parse_peer_id(raw):
    s = normalize_id_str(raw)
    if not s:
        return ""
    if "splus.ir" in s or s.startswith("http://") or s.startswith("https://"):
        url = s if s.startswith("http") else "https://" + s
        try:
            parts = [p for p in urlparse(url).path.split("/") if p]
            if parts:
                if parts[0] == "c" and len(parts) >= 2:
                    cid = normalize_id_str(parts[1])
                    if cid.lstrip("-").isdigit():
                        return f"-100{cid.lstrip('-')}" if not cid.startswith("-") else cid
                if len(parts) >= 2 and parts[-1].isdigit():
                    return normalize_id_str(parts[-2])
                return normalize_id_str(parts[0])
        except Exception:
            pass
    m = re.search(r"(?:https?://)?(?:www\.)?splus\.ir/([^/\s?#]+)", s)
    if m:
        return normalize_id_str(m.group(1))
    if s.startswith("@"):
        return s[1:].strip()
    return s


def to_peer_arg(raw):
    p = parse_peer_id(raw)
    if isinstance(p, (list, tuple, dict)):
        p = str(p[0]) if p else ""
    p = str(p or "").strip()
    if not p:
        return ""
    if p.lstrip("-").isdigit():
        try:
            return int(p)
        except Exception:
            return p
    return p


def run_async(coro):
    if bot_loop and not bot_loop.is_closed():
        return asyncio.run_coroutine_threadsafe(coro, bot_loop)


async def do_send_code(phone):
    global client, phone_code_hash, client_ready
    state["phone"] = phone
    state["auth_msg"] = "در حال ارسال کد..."
    try:
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
    if not client_ready or client is None:
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
    except Exception:
        pass


async def send_clean_message(dst_raw, text):
    peer = to_peer_arg(dst_raw)
    if peer == "":
        raise ValueError("مقصد نامعتبر است")
    if client_ready and client is not None:
        res = client.send_message(peer, str(text))
        if asyncio.iscoroutine(res):
            await asyncio.wait_for(res, timeout=10)


async def execute_forward_step(job, raw_t):
    mode = job.get("mode", "forward_msg")
    async with op_lock:
        if mode == "spam":
            await send_clean_message(raw_t, job.get("payload", ""))
        else:
            mid = int(job.get("msg_id") or 0)
            txt = (job.get("payload") or "").strip() or f"پیام #{mid}"
            dst_peer = to_peer_arg(raw_t)
            src_peer = to_peer_arg(job.get("source_chat")) if job.get("source_chat") else None

            forwarded = False
            if client_ready and client is not None and dst_peer != "" and src_peer and mid:
                for fn_name in ("forward_messages", "forward_message"):
                    fn = getattr(client, fn_name, None)
                    if callable(fn):
                        try:
                            r = fn(dst_peer, int(mid), from_peer=src_peer)
                            if asyncio.iscoroutine(r):
                                await asyncio.wait_for(r, timeout=6)
                            forwarded = True
                            break
                        except Exception:
                            continue

            if not forwarded:
                await send_clean_message(raw_t, txt)

    job["ok"] = int(job.get("ok") or 0) + 1
    job["heal_state"] = "running"
    job["last_error"] = ""
    job["logs"].append(f"[{now_str()}] SENT OK -> {normalize_id_str(raw_t)} (#{job.get('msg_id')})")


async def run_job_worker(job):
    t_idx = 0
    sent_burst = 0
    while job in jobs:
        if not job.get("running"):
            await asyncio.sleep(0.25)
            continue

        max_c = int(job.get("max_count") or 0)
        if max_c > 0 and (int(job.get("ok") or 0) + int(job.get("fail") or 0)) >= max_c:
            job["running"] = False
            job["heal_state"] = "completed"
            save_jobs()
            continue

        targets = job.get("targets") or []
        if not targets:
            job["running"] = False
            await asyncio.sleep(0.5)
            continue

        raw_t = targets[t_idx % len(targets)]
        t_idx += 1

        try:
            await execute_forward_step(job, raw_t)
            sent_burst += 1
        except Exception as e:
            job["fail"] = int(job.get("fail") or 0) + 1
            job["last_error"] = str(e)[:90]
            job["logs"].append(f"[{now_str()}] ERR -> {str(e)[:75]}")

        job["logs"] = job["logs"][-25:]
        rest_every = int(job.get("rest_every") or 0)
        rest_ms = int(job.get("rest_ms") or 0)
        if rest_every > 0 and rest_ms > 0 and sent_burst >= rest_every:
            sent_burst = 0
            await asyncio.sleep(rest_ms / 1000.0)
            continue

        await asyncio.sleep(max(50, int(job.get("speed_ms") or 500)) / 1000.0)


async def supervisor():
    active = {}
    while True:
        ids = {j["id"] for j in jobs}
        for k in list(active.keys()):
            if k not in ids or active[k].done():
                active.pop(k, None)
        for j in list(jobs):
            if j["id"] not in active:
                active[j["id"]] = asyncio.create_task(run_job_worker(j))
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
    return jsonify({"ok": True})


@app.route("/api/groups")
def api_groups():
    if not groups:
        groups[:] = [
            {"id": "-10025810580", "name": "گروه مبدا"},
            {"id": "-10024582951", "name": "گروه مقصد"},
        ]
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
    mode = (request.args.get("mode") or "spam").strip()
    if mode == "reply_loop":
        return jsonify({"ok": False, "msg": "بخش ریپلای حذف شده است."})
    payload = (request.args.get("payload") or "").strip()
    raw_targets = (request.args.get("targets") or "").strip()
    targets = [normalize_id_str(x) for x in re.split(r"[,\n]+", raw_targets) if normalize_id_str(x)]
    if not targets or (mode == "spam" and not payload):
        return jsonify({"ok": False, "msg": "اطلاعات ناقص است."})

    if mode in LIMITED_MODES:
        active_limited = sum(1 for j in jobs if j.get("running") and j.get("mode") in LIMITED_MODES)
        if active_limited >= 3:
            return jsonify({"ok": False, "msg": "محدودیت ۳ تسک هم‌زمان پر شده است."})

    job_seq += 1
    job = {
        "id": str(100 + job_seq),
        "mode": mode,
        "source_chat": normalize_id_str(request.args.get("source_chat") or ""),
        "msg_id": int(request.args.get("msg_id") or 0),
        "payload": payload,
        "targets": targets,
        "speed_ms": max(50, int(request.args.get("speed_ms") or 500)),
        "max_count": max(0, int(request.args.get("max_count") or 0)),
        "rest_every": max(0, int(request.args.get("rest_every") or 0)),
        "rest_ms": max(0, int(request.args.get("rest_ms") or 0)),
        "running": True,
        "heal_state": "running",
        "last_error": "",
        "ok": 0,
        "fail": 0,
        "logs": [f"[{now_str()}] started ({mode})"],
    }
    jobs.append(job)
    save_jobs()
    return jsonify({"ok": True, "msg": f"تسک #{job['id']} شروع شد."})


@app.route("/api/job_ctrl")
def api_job_ctrl():
    jid = (request.args.get("id") or "").strip()
    act = (request.args.get("act") or "").strip()
    for j in jobs:
        if j["id"] == jid:
            if act == "toggle":
                if not j.get("running") and j.get("mode") in LIMITED_MODES:
                    active_limited = sum(1 for x in jobs if x.get("running") and x.get("mode") in LIMITED_MODES)
                    if active_limited >= 3:
                        return jsonify({"ok": False, "msg": "حداکثر ۳ تسک هم‌زمان مجاز است."})
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
        "auth_msg": state["auth_msg"],
        "accounts": accounts,
        "groups": groups,
        "jobs": jobs,
    })


jobs[:] = load_jobs()
job_seq = len(jobs)

Thread(target=start_bot_thread, daemon=True).start()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT, threaded=True)
