import asyncio
import inspect
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
LIMITED_MODES = {"forward_msg", "spam", "reply_loop"}

app = Flask(__name__)

accounts = []
groups = []
jobs = []
job_seq = 0

state = {
    "ping": "12 ms",
    "cpu": "0",
    "ram": "0",
    "auth_msg": "آماده ورود شماره",
    "phone": "",
    "healer_status": "فعال",
}

client = SoroushClient(StringSession())
phone_code_hash = None
client_ready = False
healing_in_progress = False
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
                    for j in arr:
                        j.setdefault("heal_state", "normal")
                        j.setdefault("last_error", "")
                    jobs = arr
                    job_seq = len(jobs)
        except Exception:
            pass


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
    if s.startswith("@"):
        s = s[1:]
    return int(s) if s.lstrip("-").isdigit() else s


def candidate_peers_for_chat(raw_chat):
    cands = []
    if raw_chat is None or str(raw_chat).strip() == "":
        return cands
    s = str(raw_chat).strip()
    if s.lstrip("-").isdigit():
        abs_s = s.lstrip("-")
        for v in (int(s), int(abs_s), int(f"-100{abs_s}"), int(f"-{abs_s}")):
            if v not in cands:
                cands.append(v)
    else:
        cands.append(s.lstrip("@"))
    return cands


async def resolve_target_entity(raw_peer):
    for peer in candidate_peers_for_chat(raw_peer):
        for fn_name in ("get_input_entity", "get_entity"):
            fn = getattr(client, fn_name, None)
            if callable(fn):
                try:
                    res = fn(peer)
                    ent = await asyncio.wait_for(res, timeout=10) if asyncio.iscoroutine(res) else res
                    if ent is not None:
                        return ent
                except Exception:
                    continue
    return parse_peer(raw_peer)


def serialize_msg(m, chat_id=""):
    sender = getattr(m, "from_user", None) or getattr(m, "sender_chat", None)
    sid = getattr(sender, "id", None) or getattr(m, "sender_id", "") or ""
    sname = (
        getattr(sender, "first_name", None)
        or getattr(sender, "title", None)
        or getattr(sender, "username", None)
        or str(sid)
    )
    txt = getattr(m, "text", None) or getattr(m, "caption", None) or getattr(m, "message", None) or ""
    if not txt and getattr(m, "media", None):
        txt = "[پیام رسانه‌ای / فایل]"
    return {
        "id": getattr(m, "id", 0),
        "chat_id": str(chat_id),
        "sender_id": str(sid),
        "sender_name": str(sname or "کاربر"),
        "text": str(txt or "[بدون متن]"),
    }


def run_async(coro):
    if bot_loop and not bot_loop.is_closed():
        return asyncio.run_coroutine_threadsafe(coro, bot_loop)


async def ensure_client(force=False):
    global client, client_ready, healing_in_progress
    sess = load_session()
    if not sess:
        client_ready = False
        return False
    async with lock:
        if not force and client_ready:
            return True
        healing_in_progress = True
        state["healer_status"] = "در حال بازیابی اتصال..."
        try:
            try:
                if hasattr(client, "disconnect"):
                    r = client.disconnect()
                    if asyncio.iscoroutine(r):
                        await r
            except Exception:
                pass

            client = SoroushClient(StringSession(sess))
            t0 = time.perf_counter()
            await asyncio.wait_for(client.connect(), timeout=15)
            me = await asyncio.wait_for(client.get_me(), timeout=10)
            state["ping"] = f"{int((time.perf_counter()-t0)*1000)} ms"
            client_ready = True
            healing_in_progress = False
            state["healer_status"] = "متصل و پایدار"
            name = getattr(me, "first_name", None) or state["phone"] or "Account"
            accounts[:] = [{"name": name, "phone": state["phone"] or name}]
            return True
        except Exception as e:
            client_ready = False
            healing_in_progress = False
            state["healer_status"] = f"انتظار برای رفع خطا: {str(e)[:60]}"
            state["auth_msg"] = f"خطای اتصال (تلاش مجدد خودکار): {e}"
            return False


async def smart_heal_and_resume(job, err_msg):
    job["heal_state"] = "recovering"
    job["last_error"] = err_msg[:90]
    job["logs"].append(f"[{now_str()}] HEAL -> بازیابی و ادامه دستور...")
    job["logs"] = job["logs"][-25:]

    for attempt in range(1, 5):
        if not job.get("running") or job not in jobs:
            return False
        ok = await ensure_client(force=True)
        if ok:
            job["heal_state"] = "resumed"
            job["last_error"] = ""
            job["logs"].append(f"[{now_str()}] RESUMED -> ادامه دستور {job.get('mode')}")
            job["logs"] = job["logs"][-25:]
            return True
        await asyncio.sleep(min(8, attempt * 2))
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
        await ensure_client(force=False)
    if not client_ready:
        return
    try:
        dialogs = await asyncio.wait_for(client.get_dialogs(limit=200), timeout=15)
        if asyncio.iscoroutine(dialogs):
            dialogs = await dialogs
        items = []
        for d in dialogs:
            if getattr(d, "is_group", False) or getattr(d, "is_channel", False):
                items.append({
                    "id": str(d.id),
                    "name": getattr(d, "name", None) or getattr(d, "title", None) or str(d.id),
                })
        groups[:] = items
    except Exception:
        pass


async def fetch_recent_messages(chat_id, limit=10):
    if not client_ready:
        await ensure_client(force=False)
    if not client_ready:
        return {"ok": False, "msg": "اکانت متصل نیست.", "messages": []}

    last_err = None
    for p in candidate_peers_for_chat(chat_id):
        ent = await resolve_target_entity(p)
        for target in (ent, p):
            try:
                res = client.get_messages(target, limit=limit)
                if asyncio.iscoroutine(res):
                    res = await res
                out = []
                if hasattr(res, "__aiter__"):
                    async for m in res:
                        if m:
                            out.append(serialize_msg(m, str(chat_id)))
                        if len(out) >= limit:
                            break
                elif isinstance(res, (list, tuple)):
                    for m in res:
                        if m:
                            out.append(serialize_msg(m, str(chat_id)))
                        if len(out) >= limit:
                            break
                if out:
                    return {"ok": True, "messages": out[:limit]}
            except Exception as e:
                last_err = e
                continue

    return {"ok": False, "msg": f"دریافت پیام‌های اخیر ناموفق بود: {last_err or 'پیامی یافت نشد'}", "messages": []}


async def send_reply_compatible(peer, text, msg_id):
    if not msg_id:
        return await asyncio.wait_for(client.send_message(peer, text), timeout=15)

    sig = None
    try:
        sig = inspect.signature(client.send_message)
    except Exception:
        sig = None

    kw_candidates = []
    if sig and sig.parameters:
        params = list(sig.parameters.keys())
        for cand in ("reply_to", "reply_to_msg_id", "reply_to_message_id", "reply_id", "message_id"):
            if cand in params:
                kw_candidates.append({cand: int(msg_id)})
    for cand in ("reply_to", "reply_to_msg_id", "reply_id"):
        if not any(cand in d for d in kw_candidates):
            kw_candidates.append({cand: int(msg_id)})

    for kw in kw_candidates:
        try:
            return await asyncio.wait_for(client.send_message(peer, text, **kw), timeout=15)
        except TypeError:
            continue

    return await asyncio.wait_for(client.send_message(peer, text), timeout=15)


async def send_forward_compatible(dst_ent, src_raw, msg_id, fallback_text=""):
    fwd_fn = getattr(client, "forward_messages", None) or getattr(client, "forward_message", None)
    if callable(fwd_fn) and msg_id and src_raw:
        src_ent = await resolve_target_entity(src_raw)
        for dt, st in ((dst_ent, src_ent), (dst_ent, parse_peer(src_raw))):
            if not st:
                continue
            for fn in (
                lambda a=dt, b=st: fwd_fn(a, [int(msg_id)], b),
                lambda a=dt, b=st: fwd_fn(a, int(msg_id), b),
            ):
                try:
                    res = fn()
                    return await asyncio.wait_for(res, timeout=15) if asyncio.iscoroutine(res) else res
                except Exception:
                    continue

    text_out = (fallback_text or "").strip() or f"پیام #{msg_id}"
    return await asyncio.wait_for(client.send_message(dst_ent, text_out), timeout=15)


async def execute_single_step(job, dst_ent, raw_t):
    mode = job.get("mode", "spam")
    if mode == "forward_msg":
        await send_forward_compatible(
            dst_ent,
            job.get("source_chat"),
            int(job.get("msg_id") or 0),
            fallback_text=job.get("payload", ""),
        )
    elif mode == "reply_loop":
        await send_reply_compatible(dst_ent, job.get("payload", ""), int(job.get("msg_id") or 0))
    else:
        await asyncio.wait_for(client.send_message(dst_ent, job.get("payload", "")), timeout=15)

    job["ok"] = int(job.get("ok") or 0) + 1
    job["heal_state"] = "running"
    job["last_error"] = ""
    job["logs"].append(f"[{now_str()}] OK [{mode}] -> {raw_t}")


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

        if not client_ready:
            await smart_heal_and_resume(job, "اتصال کلاینت برقرار نبود")
            await asyncio.sleep(0.5)
            continue

        targets = job.get("targets") or []
        if not targets:
            job["running"] = False
            await asyncio.sleep(0.5)
            continue

        raw_t = targets[t_idx % len(targets)]
        t_idx += 1
        dst_ent = await resolve_target_entity(raw_t)

        try:
            await execute_single_step(job, dst_ent, raw_t)
            sent_burst += 1
        except Exception as e:
            err_txt = str(e)
            job["fail"] = int(job.get("fail") or 0) + 1
            job["logs"].append(f"[{now_str()}] ERR -> {err_txt[:80]}")

            resumed = await smart_heal_and_resume(job, err_txt)
            if resumed and job.get("running"):
                try:
                    await execute_single_step(job, dst_ent, raw_t)
                    sent_burst += 1
                except Exception as e2:
                    job["logs"].append(f"[{now_str()}] RETRY-ERR -> {str(e2)[:75]}")

        job["logs"] = job["logs"][-25:]
        rest_every = int(job.get("rest_every") or 0)
        rest_ms = int(job.get("rest_ms") or 0)
        if rest_every > 0 and rest_ms > 0 and sent_burst >= rest_every:
            sent_burst = 0
            await asyncio.sleep(rest_ms / 1000.0)
            continue

        await asyncio.sleep(max(50, int(job.get("speed_ms") or 500)) / 1000.0)


async def background_connection_guard():
    while True:
        await asyncio.sleep(8)
        has_active = any(j.get("running") for j in jobs)
        if has_active and not client_ready and not healing_in_progress:
            await ensure_client(force=True)


async def supervisor():
    active = {}
    asyncio.create_task(background_connection_guard())
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
    limit = max(1, min(10, int(request.args.get("limit") or 10)))
    if not chat_id:
        return jsonify({"ok": False, "msg": "ابتدا مبدا را انتخاب کنید.", "messages": []})
    fut = run_async(fetch_recent_messages(chat_id, limit=limit))
    if fut:
        try:
            return jsonify(fut.result(timeout=20))
        except Exception as e:
            return jsonify({"ok": False, "msg": f"خطا: {e}", "messages": []})
    return jsonify({"ok": False, "msg": "سرویس پیام آماده نیست.", "messages": []})


@app.route("/api/add_job")
def api_add_job():
    global job_seq
    mode = (request.args.get("mode") or "spam").strip()
    payload = (request.args.get("payload") or "").strip()
    raw_targets = (request.args.get("targets") or "").strip()
    targets = [x.strip() for x in re.split(r"[,\n]+", raw_targets) if x.strip()]
    if not targets or (mode in {"spam", "reply_loop"} and not payload):
        return jsonify({"ok": False, "msg": "اطلاعات ناقص است."})

    if mode in LIMITED_MODES:
        active_limited = sum(1 for j in jobs if j.get("running") and j.get("mode") in LIMITED_MODES)
        if active_limited >= 3:
            return jsonify({"ok": False, "msg": "محدودیت ۳ تسک هم‌زمان برای فوروارد/اسپم/ریپلای پر شده است."})

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
