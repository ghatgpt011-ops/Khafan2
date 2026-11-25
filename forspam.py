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
LIMITED_MODES = {"forward_msg", "spam"}

app = Flask(__name__)

accounts = []
groups = []
cached_dialogs = []
jobs = []
job_seq = 0

state = {
    "ping": "-",
    "cpu": "0",
    "ram": "0",
    "auth_msg": "آماده ورود شماره",
    "phone": "",
    "healer_status": "در انتظار اتصال",
    "soroush_connected": False,
}

client = SoroushClient(StringSession())
phone_code_hash = None
client_ready = False
healing_in_progress = False
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


def parse_peer_id(raw):
    s = str(raw or "").strip()
    if not s:
        return ""
    if "splus.ir" in s or s.startswith("http://") or s.startswith("https://"):
        url = s if s.startswith("http") else "https://" + s
        try:
            parts = [p for p in urlparse(url).path.split("/") if p]
            if parts:
                if parts[0] == "c" and len(parts) >= 2 and parts[1].lstrip("-").isdigit():
                    cid = parts[1]
                    return f"-100{cid}" if not cid.startswith("-") else cid
                if len(parts) >= 2 and parts[-1].isdigit():
                    return parts[-2]
                return parts[0]
        except Exception:
            pass
    m = re.search(r"(?:https?://)?(?:www\.)?splus\.ir/([^/\s?#]+)", s)
    if m:
        return m.group(1).strip()
    if s.startswith("@"):
        return s[1:].strip()
    return s


def to_peer_arg(raw):
    p = parse_peer_id(raw)
    return int(p) if p.lstrip("-").isdigit() else p


def peer_variants(raw):
    out = []
    p = parse_peer_id(raw)
    if not p:
        return out
    if p.lstrip("-").isdigit():
        num = int(p)
        abs_s = p.lstrip("-")
        for cand in (num, int(abs_s), int(f"-100{abs_s}"), int(f"-{abs_s}")):
            if cand not in out:
                out.append(cand)
    else:
        out.append(p)
    return out


async def maybe_await(val, timeout=15):
    if inspect.isawaitable(val):
        return await asyncio.wait_for(val, timeout=timeout)
    return val


def extract_msg_text(m):
    for attr in ("text", "message", "raw_text", "caption", "body", "content"):
        val = getattr(m, attr, None)
        if isinstance(val, str) and val.strip():
            return val.strip()
    if isinstance(m, dict):
        for k in ("text", "message", "raw_text", "caption", "body"):
            if m.get(k):
                return str(m[k]).strip()
    if getattr(m, "media", None):
        return "[پیام مدیا / فایل]"
    return ""


def extract_msg_id(m, idx):
    for attr in ("id", "message_id", "msg_id"):
        val = getattr(m, attr, None)
        if val is not None:
            try:
                return int(val)
            except Exception:
                pass
    if isinstance(m, dict):
        for k in ("id", "message_id", "msg_id"):
            if m.get(k) is not None:
                try:
                    return int(m[k])
                except Exception:
                    pass
    return idx


def serialize_msg(m, chat_id="", idx=1):
    sender = getattr(m, "from_user", None) or getattr(m, "sender_chat", None)
    sid = getattr(sender, "id", None) or getattr(m, "sender_id", "") or ""
    sname = (
        getattr(sender, "first_name", None)
        or getattr(sender, "title", None)
        or getattr(sender, "username", None)
        or str(sid)
    )
    txt = extract_msg_text(m) or "[بدون متن]"
    return {
        "id": extract_msg_id(m, idx),
        "chat_id": str(chat_id),
        "sender_id": str(sid),
        "sender_name": str(sname or "کاربر"),
        "text": str(txt),
    }


def run_async(coro):
    if bot_loop and not bot_loop.is_closed():
        return asyncio.run_coroutine_threadsafe(coro, bot_loop)


async def ensure_client(force=False):
    global client, client_ready, healing_in_progress
    sess = load_session()
    if not sess:
        client_ready = False
        state["soroush_connected"] = False
        state["healer_status"] = "سشن ثبت نشده"
        return False

    if not force and client_ready and client is not None:
        try:
            t0 = time.perf_counter()
            me = await maybe_await(client.get_me(), timeout=8)
            if me is not None:
                state["ping"] = f"{int((time.perf_counter() - t0) * 1000)} ms"
                state["soroush_connected"] = True
                state["healer_status"] = "متصل به سروش"
                name = getattr(me, "first_name", None) or state["phone"] or "Account"
                accounts[:] = [{"name": name, "phone": state["phone"] or name}]
                return True
        except Exception:
            client_ready = False

    healing_in_progress = True
    state["healer_status"] = "در حال اتصال به سروش..."
    try:
        if client is not None and hasattr(client, "disconnect"):
            try:
                await maybe_await(client.disconnect(), timeout=5)
            except Exception:
                pass

        new_c = SoroushClient(StringSession(sess))
        t0 = time.perf_counter()
        await maybe_await(new_c.connect(), timeout=18)
        me = await maybe_await(new_c.get_me(), timeout=12)
        if me is None:
            raise RuntimeError("پاسخ get_me خالی است.")

        client = new_c
        client_ready = True
        healing_in_progress = False
        state["soroush_connected"] = True
        state["ping"] = f"{int((time.perf_counter() - t0) * 1000)} ms"
        state["healer_status"] = "متصل به سروش"
        name = getattr(me, "first_name", None) or state["phone"] or "Account"
        accounts[:] = [{"name": name, "phone": state["phone"] or name}]
        return True
    except Exception as e:
        client_ready = False
        healing_in_progress = False
        state["soroush_connected"] = False
        state["healer_status"] = f"عدم اتصال: {str(e)[:55]}"
        state["auth_msg"] = f"خطای اتصال به سروش: {e}"
        return False


async def smart_heal_and_resume(job, err_msg):
    job["heal_state"] = "recovering"
    job["last_error"] = err_msg[:90]
    job["logs"].append(f"[{now_str()}] HEAL -> بازیابی و ادامه فوروارد...")
    job["logs"] = job["logs"][-25:]

    for attempt in range(1, 5):
        if not job.get("running") or job not in jobs:
            return False
        ok = await ensure_client(force=True)
        if ok:
            job["heal_state"] = "resumed"
            job["last_error"] = ""
            job["logs"].append(f"[{now_str()}] RESUMED -> ادامه فوروارد")
            job["logs"] = job["logs"][-25:]
            return True
        await asyncio.sleep(min(8, attempt * 2))
    return False


async def do_send_code(phone):
    global client, phone_code_hash, client_ready
    state["phone"] = phone
    state["auth_msg"] = "در حال ارسال کد..."
    try:
        if client is not None and hasattr(client, "disconnect"):
            try:
                await maybe_await(client.disconnect(), timeout=5)
            except Exception:
                pass
        c = SoroushClient(StringSession())
        client = c
        client_ready = False
        await maybe_await(c.connect(), timeout=20)
        if hasattr(c, "send_code_request"):
            res = await maybe_await(c.send_code_request(phone), timeout=20)
        elif hasattr(c, "send_code"):
            res = await maybe_await(c.send_code(phone), timeout=20)
        else:
            res = await maybe_await(c.sign_in(phone=phone), timeout=20)
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
                    await maybe_await(fn(), timeout=20)
                    signed = True
                    break
                except TypeError:
                    continue
        if not signed:
            await maybe_await(c.sign_in(phone=phone, code=code), timeout=20)

        sess = c.session.save() if getattr(c, "session", None) else ""
        if not sess:
            raise RuntimeError("سشن ذخیره نشد.")
        save_session(sess)
        client_ready = True
        state["soroush_connected"] = True
        phone_code_hash = None
        me = await maybe_await(c.get_me(), timeout=10)
        name = getattr(me, "first_name", None) or phone
        accounts[:] = [{"name": name, "phone": phone}]
        state["auth_msg"] = f"اکانت {name} متصل شد."
        state["healer_status"] = "متصل به سروش"
    except Exception as e:
        state["auth_msg"] = f"خطا در تأیید کد: {e}"

    if client_ready:
        await refresh_groups()


async def refresh_groups():
    global groups, cached_dialogs
    ok = await ensure_client(force=False)
    if not ok or client is None:
        return
    try:
        raw = []
        for fn_name in ("get_dialogs", "iter_dialogs", "get_chats"):
            fn = getattr(client, fn_name, None)
            if not callable(fn):
                continue
            try:
                res = fn(limit=250)
            except TypeError:
                res = fn()
            if hasattr(res, "__aiter__"):
                async for d in res:
                    raw.append(d)
                break
            val = await maybe_await(res, timeout=16)
            if val:
                raw = list(val) if isinstance(val, (list, tuple)) else [val]
                break

        cached_dialogs[:] = list(raw)
        items = []
        seen = set()
        for d in raw:
            did = getattr(d, "id", None)
            if did is None:
                ent = getattr(d, "entity", None)
                did = getattr(ent, "id", None)
            if did is None:
                continue
            sid = str(did)
            if sid in seen:
                continue
            seen.add(sid)
            name = getattr(d, "name", None) or getattr(d, "title", None) or sid
            items.append({"id": sid, "name": str(name)})
        if items:
            groups[:] = items
    except Exception:
        pass


async def _collect_messages(res, chat_id, limit):
    out = []
    if hasattr(res, "__aiter__"):
        async for m in res:
            if m:
                out.append(serialize_msg(m, chat_id, len(out) + 1))
            if len(out) >= limit:
                break
    elif isinstance(res, (list, tuple)):
        for m in res:
            if m:
                out.append(serialize_msg(m, chat_id, len(out) + 1))
            if len(out) >= limit:
                break
    elif res:
        out.append(serialize_msg(res, chat_id, 1))
    return out[:limit]


async def fetch_recent_messages(chat_id, limit=10):
    ok = await ensure_client(force=True)
    if not ok or client is None:
        return {
            "ok": False,
            "msg": f"عدم اتصال به سروش ({state.get('healer_status') or 'سشن نامعتبر'}). ابتدا اکانت را وصل کنید.",
            "messages": [],
        }

    variants = peer_variants(chat_id)
    if not variants:
        return {"ok": False, "msg": "شناسه چت معتبر نیست.", "messages": []}

    last_err = ""
    for peer in variants:
        for fn_name in ("get_messages", "iter_messages", "get_history"):
            fn = getattr(client, fn_name, None)
            if not callable(fn):
                continue
            for call_fn in (
                lambda p=peer, l=limit: fn(p, limit=l),
                lambda p=peer, l=limit: fn(p, l),
            ):
                try:
                    raw = call_fn()
                    if not hasattr(raw, "__aiter__"):
                        raw = await maybe_await(raw, timeout=14)
                    msgs = await _collect_messages(raw, str(chat_id), limit)
                    if msgs:
                        return {"ok": True, "messages": msgs}
                except Exception as e:
                    last_err = str(e)
                    continue

    # بررسی آخرین پیام کش‌شده در دیالوگ‌ها به عنوان مسیر جایگزین
    if not cached_dialogs:
        await refresh_groups()
    str_vars = {str(v) for v in variants}
    for d in cached_dialogs:
        did = str(getattr(d, "id", "") or getattr(getattr(d, "entity", None), "id", "") or "")
        if did in str_vars:
            lm = getattr(d, "message", None) or getattr(d, "last_message", None)
            if lm:
                return {"ok": True, "messages": [serialize_msg(lm, str(chat_id), 1)]}

    return {
        "ok": False,
        "msg": f"پیامی دریافت نشد ({last_err or 'عدم دسترسی یا خالی بودن چت'}).",
        "messages": [],
    }


async def send_forward_reliable(dst_raw, src_raw, msg_id, fallback_text=""):
    dst_cands = peer_variants(dst_raw) or [to_peer_arg(dst_raw)]
    src_cands = peer_variants(src_raw) if src_raw else []
    msg_id_val = int(msg_id) if str(msg_id).isdigit() else msg_id

    for dst in dst_cands:
        for fn_name in ("forward_messages", "forward_message"):
            fwd_fn = getattr(client, fn_name, None)
            if callable(fwd_fn) and msg_id_val and src_cands:
                for src in src_cands:
                    for call_fn in (
                        lambda d=dst, m=msg_id_val, s=src: fwd_fn(d, m, from_peer=s),
                        lambda d=dst, m=msg_id_val, s=src: fwd_fn(d, [m], from_peer=s),
                        lambda d=dst, m=msg_id_val, s=src: fwd_fn(d, m, s),
                        lambda d=dst, m=msg_id_val, s=src: fwd_fn(d, [m], s),
                    ):
                        try:
                            res = call_fn()
                            return await maybe_await(res, timeout=15)
                        except TypeError:
                            continue
                        except Exception:
                            continue

    # فال‌بک قطعی: ارسال پیام به مقصد تا جریان فوروارد هرگز متوقف نشود
    txt = (fallback_text or "").strip() or f"پیام #{msg_id_val}"
    last_ex = None
    for dst in dst_cands:
        try:
            return await maybe_await(client.send_message(dst, txt), timeout=15)
        except Exception as e:
            last_ex = e
            continue
    if last_ex:
        raise last_ex


async def execute_single_step(job, raw_t):
    mode = job.get("mode", "forward_msg")
    async with op_lock:
        if mode == "spam":
            dst_cands = peer_variants(raw_t) or [to_peer_arg(raw_t)]
            last_ex = None
            for dst in dst_cands:
                try:
                    await maybe_await(client.send_message(dst, job.get("payload", "")), timeout=15)
                    last_ex = None
                    break
                except Exception as e:
                    last_ex = e
            if last_ex:
                raise last_ex
        else:
            await send_forward_reliable(
                raw_t,
                job.get("source_chat"),
                job.get("msg_id") or 0,
                fallback_text=job.get("payload", ""),
            )

    job["ok"] = int(job.get("ok") or 0) + 1
    job["heal_state"] = "running"
    job["last_error"] = ""
    job["logs"].append(f"[{now_str()}] FWD OK -> {raw_t}")


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

        if not client_ready or client is None:
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

        try:
            await execute_single_step(job, raw_t)
            sent_burst += 1
        except Exception as e:
            err_txt = str(e)
            job["fail"] = int(job.get("fail") or 0) + 1
            job["logs"].append(f"[{now_str()}] ERR -> {err_txt[:80]}")

            resumed = await smart_heal_and_resume(job, err_txt)
            if resumed and job.get("running"):
                try:
                    await execute_single_step(job, raw_t)
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
        await asyncio.sleep(10)
        if load_session() and not client_ready and not healing_in_progress:
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
    if load_session():
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
            fut.result(timeout=24)
        except Exception:
            pass
    return jsonify({"ok": True, "auth_msg": state["auth_msg"]})


@app.route("/api/verify_code")
def api_verify_code():
    code = (request.args.get("code") or "").strip()
    fut = run_async(do_verify_code(code))
    if fut:
        try:
            fut.result(timeout=24)
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
    state["healer_status"] = "سشن حذف شد"
    return jsonify({"ok": True})


@app.route("/api/groups")
def api_groups():
    fut = run_async(refresh_groups())
    if fut:
        try:
            fut.result(timeout=20)
        except Exception:
            pass
    return jsonify(groups)


@app.route("/api/recent_messages")
def api_recent_messages():
    chat_id = (request.args.get("chat_id") or "").strip()
    limit = max(1, min(10, int(request.args.get("limit") or 10)))
    if not chat_id:
        return jsonify({"ok": False, "msg": "ابتدا چت را انتخاب کنید.", "messages": []})
    fut = run_async(fetch_recent_messages(chat_id, limit=limit))
    if fut:
        try:
            return jsonify(fut.result(timeout=26))
        except Exception as e:
            return jsonify({"ok": False, "msg": f"خطا: {e}", "messages": []})
    return jsonify({"ok": False, "msg": "سرویس پیام آماده نیست.", "messages": []})


@app.route("/api/add_job")
def api_add_job():
    global job_seq
    mode = (request.args.get("mode") or "forward_msg").strip()
    if mode == "reply_loop":
        return jsonify({"ok": False, "msg": "بخش ریپلای حذف شده است."})
    payload = (request.args.get("payload") or "").strip()
    raw_targets = (request.args.get("targets") or "").strip()
    targets = [x.strip() for x in re.split(r"[,\n]+", raw_targets) if x.strip()]
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
