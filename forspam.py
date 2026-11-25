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

app = Flask(__name__)

accounts = []
groups = []
jobs = []
job_seq = 0

state = {
    "ping": "12 ms",
    "cpu": "0",
    "ram": "0",
    "auth_status": "disconnected",
    "auth_msg": "آماده ورود شماره",
    "phone": "",
}

client = SoroushClient(StringSession())
phone_code_hash = None
client_ready = False
bot_loop = None
lock = asyncio.Lock()


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
                    jobs = arr
                    job_seq = len(jobs)
        except Exception:
            pass


def parse_target(raw):
    s = str(raw or "").strip()
    if not s:
        return ""
    if "splus.ir" in s or s.startswith("http"):
        u = s if s.startswith("http") else "https://" + s
        parts = [p for p in urlparse(u).path.split("/") if p]
        if parts:
            if parts[0] == "c" and len(parts) >= 2:
                return f"-100{parts[1]}" if not parts[1].startswith("-") else parts[1]
            return parts[0]
    m = re.search(r"splus\.ir/([^/\s?#]+)", s)
    if m:
        return m.group(1)
    if s.startswith("@"):
        return s[1:]
    return s


def run_async(coro):
    if bot_loop and not bot_loop.is_closed():
        return asyncio.run_coroutine_threadsafe(coro, bot_loop)


async def ensure_client(force=False):
    global client, client_ready
    sess = load_session()
    if not sess:
        client_ready = False
        return False
    async with lock:
        if not force and client_ready:
            return True
        try:
            if hasattr(client, "disconnect"):
                r = client.disconnect()
                if asyncio.iscoroutine(r):
                    await r
        except Exception:
            pass
        try:
            client = SoroushClient(StringSession(sess))
            t0 = time.perf_counter()
            await asyncio.wait_for(client.connect(), timeout=15)
            me = await asyncio.wait_for(client.get_me(), timeout=10)
            state["ping"] = f"{int((time.perf_counter()-t0)*1000)} ms"
            client_ready = True
            name = getattr(me, "first_name", None) or state["phone"] or "Account"
            accounts[:] = [{"name": name, "phone": state["phone"] or name}]
            return True
        except Exception as e:
            client_ready = False
            state["auth_msg"] = f"خطای اتصال: {e}"
            return False


async def do_send_code(phone):
    global client, phone_code_hash, client_ready
    state["phone"] = phone
    state["auth_status"] = "code_sent"
    state["auth_msg"] = "در حال ارسال کد..."
    async with lock:
        try:
            try:
                if hasattr(client, "disconnect"):
                    r = client.disconnect()
                    if asyncio.iscoroutine(r):
                        await r
            except Exception:
                pass
            client = SoroushClient(StringSession())
            client_ready = False
            await asyncio.wait_for(client.connect(), timeout=20)
            if hasattr(client, "send_code_request"):
                res = await asyncio.wait_for(client.send_code_request(phone), timeout=20)
            elif hasattr(client, "send_code"):
                res = await asyncio.wait_for(client.send_code(phone), timeout=20)
            else:
                res = await asyncio.wait_for(client.sign_in(phone=phone), timeout=20)
            phone_code_hash = getattr(res, "phone_code_hash", None)
            state["auth_msg"] = "کد ارسال شد؛ کد را وارد کنید."
        except Exception as e:
            state["auth_msg"] = f"خطا در ارسال کد: {e}"


async def do_verify_code(code):
    global phone_code_hash, client_ready
    phone = state.get("phone", "").strip()
    state["auth_msg"] = "در حال تأیید کد..."
    async with lock:
        try:
            signed = False
            if phone_code_hash:
                for fn in (
                    lambda: client.sign_in(phone=phone, code=code, phone_code_hash=phone_code_hash),
                    lambda: client.sign_in(phone, code, phone_code_hash),
                ):
                    try:
                        await asyncio.wait_for(fn(), timeout=20)
                        signed = True
                        break
                    except TypeError:
                        continue
            if not signed:
                await asyncio.wait_for(client.sign_in(phone=phone, code=code), timeout=20)

            sess = client.session.save() if getattr(client, "session", None) else ""
            if not sess:
                raise RuntimeError("سشن ذخیره نشد.")
            save_session(sess)
            client_ready = True
            phone_code_hash = None
            me = await asyncio.wait_for(client.get_me(), timeout=10)
            name = getattr(me, "first_name", None) or phone
            accounts[:] = [{"name": name, "phone": phone}]
            state["auth_status"] = "connected"
            state["auth_msg"] = f"اکانت {name} متصل شد."
            await refresh_groups()
        except Exception as e:
            state["auth_msg"] = f"خطا در تأیید کد: {e}"


async def refresh_groups():
    global groups
    if not client_ready:
        return
    try:
        dialogs = await asyncio.wait_for(client.get_dialogs(limit=200), timeout=15)
        items = []
        for d in dialogs:
            if getattr(d, "is_group", False) or getattr(d, "is_channel", False):
                items.append({"id": str(d.id), "name": d.name or str(d.id)})
        groups[:] = items
    except Exception:
        pass


async def run_job_worker(job):
    started_at = time.time()
    sent_in_burst = 0
    t_idx = 0
    while job in jobs:
        if not job.get("running"):
            await asyncio.sleep(0.25)
            continue

        max_c = int(job.get("max_count") or 0)
        if max_c > 0 and (int(job.get("ok") or 0) + int(job.get("fail") or 0)) >= max_c:
            job["running"] = False
            job["logs"].append(f"[{now_str()}] finished")
            save_jobs()
            continue

        if not client_ready:
            await ensure_client(force=False)
            await asyncio.sleep(0.5)
            continue

        targets = job.get("targets") or []
        if not targets:
            job["running"] = False
            await asyncio.sleep(0.5)
            continue

        raw_t = targets[t_idx % len(targets)]
        t_idx += 1
        parsed = parse_target(raw_t)
        peer = int(parsed) if parsed.lstrip("-").isdigit() else parsed

        try:
            mode = job.get("mode", "text")
            payload = job.get("payload", "")
            if mode == "link" and hasattr(client, "forward_messages"):
                m = re.search(r"splus\.ir/c/(-?\d+)/(\d+)", payload)
                if m:
                    from_peer = int(f"-100{m.group(1)}") if not m.group(1).startswith("-") else int(m.group(1))
                    msg_id = int(m.group(2))
                    await asyncio.wait_for(client.forward_messages(peer, from_peer, [msg_id]), timeout=15)
                else:
                    await asyncio.wait_for(client.send_message(peer, payload), timeout=15)
            else:
                await asyncio.wait_for(client.send_message(peer, payload), timeout=15)

            job["ok"] = int(job.get("ok") or 0) + 1
            job["last"] = now_str()
            job["elapsed"] = f"{int(time.time() - started_at)}s"
            job["logs"].append(f"[{now_str()}] OK #{job['ok']} -> {raw_t}")
            sent_in_burst += 1
        except Exception as e:
            job["fail"] = int(job.get("fail") or 0) + 1
            job["last"] = now_str()
            job["elapsed"] = f"{int(time.time() - started_at)}s"
            job["logs"].append(f"[{now_str()}] ERR -> {str(e)[:80]}")

        job["logs"] = job["logs"][-30:]
        if (job["ok"] + job["fail"]) % 10 == 0:
            save_jobs()

        rest_every = int(job.get("rest_every") or 0)
        rest_ms = int(job.get("rest_ms") or 0)
        if rest_every > 0 and rest_ms > 0 and sent_in_burst >= rest_every:
            sent_in_burst = 0
            job["logs"].append(f"[{now_str()}] rest {rest_ms}ms")
            await asyncio.sleep(rest_ms / 1000.0)
            continue

        speed_ms = max(50, int(job.get("speed_ms") or 1000))
        if job.get("speed_mode") == "smart":
            speed_ms = int(speed_ms * 1.25)
        await asyncio.sleep(speed_ms / 1000.0)


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
    await ensure_client(force=True)
    if client_ready:
        await refresh_groups()
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
    if not phone:
        return jsonify({"ok": False, "auth_msg": "شماره را وارد کنید."})
    fut = run_async(do_send_code(phone))
    if fut:
        try:
            fut.result(timeout=22)
        except Exception:
            pass
    return jsonify({"ok": True, "auth_msg": state["auth_msg"]})


@app.route("/api/verify_code")
def api_verify_code():
    code = (request.args.get("code") or "").strip()
    if not code:
        return jsonify({"ok": False, "auth_msg": "کد را وارد کنید."})
    fut = run_async(do_verify_code(code))
    if fut:
        try:
            fut.result(timeout=22)
        except Exception:
            pass
    return jsonify({"ok": True, "auth_msg": state["auth_msg"]})


@app.route("/api/del_account")
def api_del_account():
    idx = int(request.args.get("index", -1))
    if 0 <= idx < len(accounts):
        accounts.pop(idx)
    save_session("")
    return jsonify({"ok": True})


@app.route("/api/groups")
def api_groups():
    fut = run_async(refresh_groups())
    if fut:
        try:
            fut.result(timeout=16)
        except Exception:
            pass
    return jsonify(groups)


@app.route("/api/add_job")
def api_add_job():
    global job_seq
    mode = (request.args.get("mode") or "text").strip()
    payload = (request.args.get("payload") or "").strip()
    raw_targets = (request.args.get("targets") or "").strip()
    targets = [x.strip() for x in re.split(r"[,\n]+", raw_targets) if x.strip()]
    if not payload or not targets:
        return jsonify({"ok": False, "msg": "محتوا و مقصد را وارد کنید."})

    job_seq += 1
    job = {
        "id": str(100 + job_seq),
        "mode": mode,
        "payload": payload,
        "targets": targets,
        "speed_ms": max(50, int(request.args.get("speed_ms") or 1000)),
        "max_count": max(0, int(request.args.get("max_count") or 0)),
        "speed_mode": request.args.get("speed_mode") or "fixed",
        "rest_every": max(0, int(request.args.get("rest_every") or 0)),
        "rest_ms": max(0, int(request.args.get("rest_ms") or 0)),
        "running": True,
        "ok": 0,
        "fail": 0,
        "last": "-",
        "elapsed": "0s",
        "logs": [f"[{now_str()}] loop starting..."],
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
                j["running"] = not j.get("running", False)
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
        "auth_msg": state["auth_msg"],
        "accounts": accounts,
        "groups": groups,
        "jobs": jobs,
    })


load_jobs()
Thread(target=start_bot_thread, daemon=True).start()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT, threaded=True)
