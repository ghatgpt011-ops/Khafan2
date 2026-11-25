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
SIDES_FILE = "fonex_sides.json"

app = Flask(__name__)

accounts = []
groups = []
jobs = []
sides = []
job_seq = 0
side_seq = 0

state = {
    "ping": "12 ms",
    "cpu": "0",
    "ram": "0",
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


def save_json(path, data):
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception:
        pass


def load_json(path, default):
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                d = json.load(f)
                if isinstance(d, type(default)):
                    return d
        except Exception:
            pass
    return default


def parse_peer(raw):
    s = str(raw or "").strip()
    if not s:
        return ""
    if "splus.ir" in s or s.startswith("http"):
        u = s if s.startswith("http") else "https://" + s
        parts = [p for p in urlparse(u).path.split("/") if p]
        if parts:
            if parts[0] == "c" and len(parts) >= 2:
                return int(f"-100{parts[1]}") if not parts[1].startswith("-") else int(parts[1])
            return parts[0]
    m = re.search(r"splus\.ir/([^/\s?#]+)", s)
    if m:
        s = m.group(1)
    if s.startswith("@"):
        s = s[1:]
    return int(s) if s.lstrip("-").isdigit() else s


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


async def fetch_recent_messages(chat_id, limit=5):
    if not client_ready:
        await ensure_client(force=False)
    if not client_ready:
        return []
    peer = parse_peer(chat_id)
    msgs = []
    try:
        async for m in client.get_messages(peer, limit=limit):
            sender = getattr(m, "from_user", None) or getattr(m, "sender_chat", None)
            sid = getattr(sender, "id", None) or getattr(m, "sender_id", "") or ""
            sname = (
                getattr(sender, "first_name", None)
                or getattr(sender, "title", None)
                or getattr(sender, "username", None)
                or str(sid)
            )
            txt = getattr(m, "text", None) or getattr(m, "caption", None) or ""
            msgs.append({
                "id": getattr(m, "id", 0),
                "sender_id": str(sid),
                "sender_name": str(sname),
                "text": str(txt),
            })
    except Exception:
        pass
    return msgs


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
            save_json(JOBS_FILE, jobs)
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
        peer = parse_peer(raw_t)

        try:
            mode = job.get("mode", "spam")
            if mode == "forward_msg" and hasattr(client, "forward_messages"):
                src_peer = parse_peer(job.get("source_chat"))
                msg_id = int(job.get("msg_id") or 0)
                await asyncio.wait_for(client.forward_messages(peer, src_peer, [msg_id]), timeout=15)
            else:
                await asyncio.wait_for(client.send_message(peer, job.get("payload", "")), timeout=15)

            job["ok"] = int(job.get("ok") or 0) + 1
            job["logs"].append(f"[{now_str()}] OK -> {raw_t}")
            sent_burst += 1
        except Exception as e:
            job["fail"] = int(job.get("fail") or 0) + 1
            job["logs"].append(f"[{now_str()}] ERR -> {str(e)[:75]}")

        job["logs"] = job["logs"][-25:]
        rest_every = int(job.get("rest_every") or 0)
        rest_ms = int(job.get("rest_ms") or 0)
        if rest_every > 0 and rest_ms > 0 and sent_burst >= rest_every:
            sent_burst = 0
            await asyncio.sleep(rest_ms / 1000.0)
            continue

        await asyncio.sleep(max(50, int(job.get("speed_ms") or 500)) / 1000.0)


async def offline_side_watcher():
    # پایش سایدهای افلاینی بگیر بر اساس دقیقه
    last_seen_map = {}
    replied_marker = {}
    while True:
        await asyncio.sleep(10)
        if not client_ready:
            continue
        for s in list(sides):
            if s.get("enabled") is False:
                continue
            users = s.get("users") or []
            chat_id = s.get("chat_id") or ""
            reply_text = (s.get("reply_text") or "").strip()
            minutes = max(1, int(s.get("minutes") or 3))
            if not users or not chat_id or not reply_text:
                continue

            peer = parse_peer(chat_id)
            target_ids = {str(u.get("id")) for u in users if u.get("id")}
            now_ts = time.time()
            latest_msg_id = None

            try:
                async for m in client.get_messages(peer, limit=15):
                    mid = getattr(m, "id", None)
                    if latest_msg_id is None and mid is not None:
                        latest_msg_id = mid
                    sender = getattr(m, "from_user", None) or getattr(m, "sender_chat", None)
                    sid = str(getattr(sender, "id", None) or getattr(m, "sender_id", "") or "")
                    if sid in target_ids:
                        last_seen_map[(s["id"], sid)] = now_ts
            except Exception:
                continue

            all_offline = True
            for sid in target_ids:
                seen_ts = last_seen_map.get((s["id"], sid))
                if seen_ts is None:
                    last_seen_map[(s["id"], sid)] = now_ts
                    all_offline = False
                elif (now_ts - seen_ts) < (minutes * 60):
                    all_offline = False

            if all_offline and latest_msg_id and replied_marker.get(s["id"]) != latest_msg_id:
                try:
                    await asyncio.wait_for(
                        client.send_message(peer, reply_text, reply_to_message_id=latest_msg_id),
                        timeout=15,
                    )
                    replied_marker[s["id"]] = latest_msg_id
                except Exception:
                    pass


async def supervisor():
    active = {}
    asyncio.create_task(offline_side_watcher())
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


@app.route("/api/recent_messages")
def api_recent_messages():
    chat_id = (request.args.get("chat_id") or "").strip()
    limit = max(1, min(20, int(request.args.get("limit") or 5)))
    fut = run_async(fetch_recent_messages(chat_id, limit=limit))
    if fut:
        try:
            return jsonify(fut.result(timeout=18))
        except Exception:
            pass
    return jsonify([])


@app.route("/api/add_job")
def api_add_job():
    global job_seq
    mode = (request.args.get("mode") or "spam").strip()
    payload = (request.args.get("payload") or "").strip()
    raw_targets = (request.args.get("targets") or "").strip()
    targets = [x.strip() for x in re.split(r"[,\n]+", raw_targets) if x.strip()]
    if not targets or (mode != "forward_msg" and not payload):
        return jsonify({"ok": False, "msg": "اطلاعات ناقص است."})

    job_seq += 1
    job = {
        "id": str(100 + job_seq),
        "mode": mode,
        "source_chat": (request.args.get("source_chat") or "").strip(),
        "msg_id": int(request.args.get("msg_id") or 0),
        "payload": payload,
        "targets": targets,
        "speed_ms": max(50, int(request.args.get("speed_ms") or 500)),
        "max_count": max(0, int(request.args.get("max_count") or 0)),
        "rest_every": max(0, int(request.args.get("rest_every") or 0)),
        "rest_ms": max(0, int(request.args.get("rest_ms") or 0)),
        "running": True,
        "ok": 0,
        "fail": 0,
        "logs": [f"[{now_str()}] started ({mode})"],
    }
    jobs.append(job)
    save_json(JOBS_FILE, jobs)
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
    save_json(JOBS_FILE, jobs)
    return jsonify({"ok": True})


@app.route("/api/side_save")
def api_side_save():
    global side_seq
    sid = (request.args.get("id") or "").strip()
    name = (request.args.get("name") or "ساید").strip()
    chat_id = (request.args.get("chat_id") or "").strip()
    minutes = max(1, int(request.args.get("minutes") or 3))
    reply_text = (request.args.get("reply_text") or "").strip()
    enabled = (request.args.get("enabled") or "true").lower() == "true"
    try:
        users = json.loads(request.args.get("users") or "[]")
    except Exception:
        users = []

    target_side = None
    for s in sides:
        if sid and s["id"] == sid:
            target_side = s
            break

    if target_side is None:
        side_seq += 1
        target_side = {"id": f"side_{side_seq}_{int(time.time())}", "name": name}
        sides.append(target_side)

    target_side.update({
        "name": name,
        "chat_id": chat_id,
        "minutes": minutes,
        "reply_text": reply_text,
        "enabled": enabled,
        "users": users,
    })
    save_json(SIDES_FILE, sides)
    return jsonify({"ok": True, "msg": "ساید ذخیره شد.", "side": target_side})


@app.route("/api/side_delete")
def api_side_delete():
    sid = (request.args.get("id") or "").strip()
    sides[:] = [s for s in sides if s["id"] != sid]
    save_json(SIDES_FILE, sides)
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
        "sides": sides,
    })


jobs[:] = load_json(JOBS_FILE, [])
sides[:] = load_json(SIDES_FILE, [])
job_seq = len(jobs)
side_seq = len(sides)

Thread(target=start_bot_thread, daemon=True).start()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT, threaded=True)
