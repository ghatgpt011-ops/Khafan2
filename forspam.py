import asyncio
import json
import os
import re
import time
from datetime import datetime
from threading import Thread
from urllib.parse import urlparse

from flask import Flask, jsonify, request

try:
    from splusthon import SoroushClient
    from splusthon.sessions import StringSession
except Exception:
    SoroushClient = None
    StringSession = None

try:
    import psutil
except Exception:
    psutil = None

PORT = int(os.environ.get("PORT", 8080))
SESSIONS_FILE = "soroush_sessions.json"
JOBS_FILE = "fonex_jobs.json"
MAX_ACTIVE_JOBS = 3
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

# ساختار هر اکانت:
# {"key": str, "phone": str, "name": str, "session": str, "connected": bool, "kicked": bool}
accounts = []
active_acc_key = ""

# داده‌های مستقل برای هر اکانت بر اساس acc_key
groups_by_acc = {}
jobs = []
offline_rules = []
enemy_rules = []
clients_by_acc = {}
pending_auth_by_acc = {}

job_seq = 0
offline_seq = 0
enemy_seq = 0

state = {
    "ping": "12 ms",
    "cpu": "0",
    "ram": "0",
    "auth_msg": "آماده ورود شماره",
    "phone": "",
    "healer_status": "آماده",
    "soroush_connected": False,
    "kicked_out": False,
}

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


def norm_acc_key(val):
    s = str(val or "").strip().replace("\u200f", "").replace("\u200e", "")
    s = re.sub(r"[^\d+A-Za-z_-]", "", s)
    return s


def get_active_account():
    global active_acc_key
    if active_acc_key:
        for a in accounts:
            if a.get("key") == active_acc_key:
                return a
    if accounts:
        active_acc_key = accounts[0]["key"]
        return accounts[0]
    return None


def resolve_acc_key(raw_key=""):
    k = norm_acc_key(raw_key)
    if k:
        for a in accounts:
            if a.get("key") == k or norm_acc_key(a.get("phone")) == k:
                return a["key"]
        return k
    act = get_active_account()
    return act["key"] if act else "default"


def get_acc_label(acc_key):
    for a in accounts:
        if a.get("key") == acc_key:
            return a.get("name") or a.get("phone") or acc_key
    return acc_key or "اکانت"


def add_log(job, text):
    logs = job.get("logs")
    if not isinstance(logs, list):
        logs = []
    logs.append(f"[{now_str()}] {text}")
    job["logs"] = logs[-30:]


def save_sessions():
    try:
        payload = {
            "active_acc_key": active_acc_key,
            "accounts": [
                {
                    "key": a.get("key", ""),
                    "phone": a.get("phone", ""),
                    "name": a.get("name", ""),
                    "session": a.get("session", ""),
                }
                for a in accounts
                if a.get("session")
            ],
        }
        with open(SESSIONS_FILE, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
    except Exception:
        pass


def load_sessions():
    global active_acc_key
    loaded = []
    if os.path.exists(SESSIONS_FILE):
        try:
            with open(SESSIONS_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, dict):
                    active_acc_key = norm_acc_key(data.get("active_acc_key") or "")
                    for item in data.get("accounts") or []:
                        if not isinstance(item, dict):
                            continue
                        sess = str(item.get("session") or "").strip()
                        phone = str(item.get("phone") or "").strip()
                        key = norm_acc_key(item.get("key") or phone)
                        if key and sess:
                            loaded.append({
                                "key": key,
                                "phone": phone or key,
                                "name": str(item.get("name") or phone or key),
                                "session": sess,
                                "connected": False,
                                "kicked": False,
                            })
        except Exception:
            loaded = []
    if not active_acc_key and loaded:
        active_acc_key = loaded[0]["key"]
    return loaded


def save_jobs():
    try:
        with open(JOBS_FILE, "w", encoding="utf-8") as f:
            json.dump(jobs, f, ensure_ascii=False, indent=2)
    except Exception:
        pass


def load_jobs():
    global job_seq
    cleaned = []
    default_key = resolve_acc_key("")
    if os.path.exists(JOBS_FILE):
        try:
            with open(JOBS_FILE, "r", encoding="utf-8") as f:
                arr = json.load(f)
                if isinstance(arr, list):
                    for j in arr:
                        if not isinstance(j, dict):
                            continue
                        mode = j.get("mode") if j.get("mode") in ("spam", "forward", "reply_chain") else "spam"
                        j["mode"] = mode
                        j["acc_key"] = norm_acc_key(j.get("acc_key") or "") or default_key
                        j["targets"] = (j.get("targets") or [])[:MAX_TARGETS_PER_JOB]
                        j.setdefault("reply_to_msg_id", 0)
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
                    return f"-100{cid.lstrip('-')}" if cid.lstrip("-").isdigit() else cid
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


def parse_splus_link(link_raw):
    s = str(link_raw or "").strip().replace("\u200f", "").replace("\u200e", "")
    raw_chat = ""
    msg_id = 0

    m_c = re.search(r"(?:https?://)?(?:www\.)?splus\.ir/c/([^/\s?#]+)/(\d+)", s)
    if m_c:
        cid = m_c.group(1).strip()
        if cid.endswith("-") and cid[:-1].isdigit():
            cid = "-" + cid[:-1]
        raw_chat = f"-100{cid.lstrip('-')}" if cid.lstrip("-").isdigit() else cid
        msg_id = int(m_c.group(2))
        return raw_chat, msg_id, s

    m_any = re.search(r"(?:https?://)?(?:www\.)?splus\.ir/([^/\s?#]+)/(\d+)", s)
    if m_any:
        raw_chat = m_any.group(1).lstrip("@").strip()
        msg_id = int(m_any.group(2))
        return raw_chat, msg_id, s

    if s:
        url = s if s.startswith("http") else "https://" + s
        parts = [p for p in urlparse(url).path.split("/") if p]
        if len(parts) >= 2 and parts[-1].isdigit():
            msg_id = int(parts[-1])
            raw_chat = parts[-2].lstrip("@").strip()
        elif s.isdigit():
            msg_id = int(s)

    return raw_chat or "unknown_chat", msg_id or 1, s


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


def extract_msg_text(obj):
    if obj is None:
        return ""
    if isinstance(obj, str):
        return obj.strip()
    if isinstance(obj, (list, tuple)):
        for item in obj:
            t = extract_msg_text(item)
            if t:
                return t
        return ""
    for attr in ("text", "message", "caption", "body", "content"):
        val = getattr(obj, attr, None)
        if isinstance(val, str) and val.strip():
            return val.strip()
    inner = getattr(obj, "message_obj", None) or getattr(obj, "msg", None)
    if inner is not None and inner is not obj and not isinstance(inner, list):
        return extract_msg_text(inner)
    return ""


def get_msg_id_val(obj):
    if obj is None:
        return 0
    for attr in ("id", "message_id", "msg_id"):
        v = getattr(obj, attr, None)
        if v is not None and str(v).isdigit():
            return int(v)
    return 0


def get_sender_info(msg_obj):
    if msg_obj is None:
        return "", ""
    sender = (
        getattr(msg_obj, "sender", None)
        or getattr(msg_obj, "from_user", None)
        or getattr(msg_obj, "user", None)
    )
    uid = ""
    uname = ""
    for attr in ("sender_id", "from_id", "user_id"):
        val = getattr(msg_obj, attr, None)
        if val is not None and str(val).strip():
            uid = str(val).strip()
            break
    if sender is not None:
        sid = getattr(sender, "id", None)
        if sid is not None and str(sid).strip():
            uid = str(sid).strip()
        first = str(getattr(sender, "first_name", "") or "").strip()
        last = str(getattr(sender, "last_name", "") or "").strip()
        u = str(getattr(sender, "username", "") or "").lstrip("@").strip()
        full = (first + " " + last).strip()
        uname = full or (f"@{u}" if u else "") or uid
    return uid, (uname or uid)


async def maybe_await(res, timeout=10):
    if asyncio.iscoroutine(res):
        return await asyncio.wait_for(res, timeout=timeout)
    return res


def is_kicked_error(err):
    txt = str(err or "").lower()
    return any(k in txt for k in KICK_KEYWORDS)


def mark_account_kicked(acc_key, reason="اکانت از سشن خارج شد."):
    for a in accounts:
        if a.get("key") == acc_key:
            a["connected"] = False
            a["kicked"] = True
            a["session"] = ""
    clients_by_acc.pop(acc_key, None)
    save_sessions()

    changed = False
    for j in jobs:
        if j.get("acc_key") == acc_key and j.get("running"):
            j["running"] = False
            j["heal_state"] = "kicked"
            j["last_error"] = "خروج از اکانت"
            add_log(j, "KICKED -> متوقف شد (خروج از اکانت)")
            changed = True
    for er in enemy_rules:
        if er.get("acc_key") == acc_key and er.get("running"):
            er["running"] = False
            er["status"] = "خروج از اکانت"
    for ofr in offline_rules:
        if ofr.get("acc_key") == acc_key and ofr.get("running"):
            ofr["running"] = False
    if changed:
        save_jobs()

    if acc_key == active_acc_key:
        state["soroush_connected"] = False
        state["kicked_out"] = True
        state["healer_status"] = "خارج شده از اکانت"
        state["auth_msg"] = f"خروج از اکانت تشخیص داده شد: {reason}"


def is_acc_ready(acc_key):
    c_info = clients_by_acc.get(acc_key)
    if not c_info or not c_info.get("ready") or c_info.get("client") is None:
        return False
    for a in accounts:
        if a.get("key") == acc_key and a.get("kicked"):
            return False
    return True


def get_acc_client(acc_key):
    c_info = clients_by_acc.get(acc_key)
    return c_info.get("client") if c_info else None


def run_async(coro):
    if bot_loop and not bot_loop.is_closed():
        return asyncio.run_coroutine_threadsafe(coro, bot_loop)


async def connect_single_account(acc_obj, force_new=False):
    acc_key = acc_obj["key"]
    if not SoroushClient or not StringSession:
        return False
    sess = (acc_obj.get("session") or "").strip()
    if not sess and not force_new:
        return False

    prev = clients_by_acc.get(acc_key, {}).get("client")
    if prev is not None and hasattr(prev, "disconnect"):
        try:
            await maybe_await(prev.disconnect(), timeout=4)
        except Exception:
            pass

    try:
        c = SoroushClient(StringSession(sess)) if sess else SoroushClient(StringSession())
        t0 = time.perf_counter()
        await maybe_await(c.connect(), timeout=14)
        if sess and hasattr(c, "get_me"):
            me = await maybe_await(c.get_me(), timeout=6)
            if me is None:
                mark_account_kicked(acc_key, "سشن نامعتبر است.")
                return False
            name = getattr(me, "first_name", None) or acc_obj.get("name") or acc_obj.get("phone") or acc_key
            acc_obj["name"] = str(name)
        acc_obj["connected"] = True
        acc_obj["kicked"] = False
        clients_by_acc[acc_key] = {"client": c, "ready": True if sess else False}
        if acc_key == active_acc_key:
            state["ping"] = f"{int((time.perf_counter() - t0) * 1000)} ms"
            state["kicked_out"] = False
            state["soroush_connected"] = bool(sess)
            state["healer_status"] = "متصل به سروش" if sess else "در انتظار ورود"
        return True
    except Exception as e:
        if is_kicked_error(e):
            mark_account_kicked(acc_key, str(e)[:80])
        return False


async def check_acc_alive(acc_key):
    c = get_acc_client(acc_key)
    if not is_acc_ready(acc_key) or c is None:
        return False
    if not hasattr(c, "get_me"):
        return True
    try:
        t0 = time.perf_counter()
        me = await maybe_await(c.get_me(), timeout=5)
        if acc_key == active_acc_key:
            state["ping"] = f"{int((time.perf_counter() - t0) * 1000)} ms"
        if me is None:
            mark_account_kicked(acc_key, "پاسخ get_me خالی است.")
            return False
        for a in accounts:
            if a.get("key") == acc_key:
                a["connected"] = True
                a["kicked"] = False
        if acc_key == active_acc_key:
            state["kicked_out"] = False
            state["soroush_connected"] = True
            state["healer_status"] = "متصل به سروش"
        return True
    except Exception as e:
        if is_kicked_error(e):
            mark_account_kicked(acc_key, str(e)[:80])
        return False


async def do_send_code(phone):
    global active_acc_key
    acc_key = norm_acc_key(phone) or f"acc_{len(accounts)+1}"
    active_acc_key = acc_key
    state["phone"] = phone
    state["kicked_out"] = False
    state["auth_msg"] = "در حال ارسال کد..."

    if not SoroushClient or not StringSession:
        state["auth_msg"] = "کتابخانه splusthon روی سرور نصب نیست."
        return

    try:
        prev = clients_by_acc.get(acc_key, {}).get("client")
        if prev is not None and hasattr(prev, "disconnect"):
            try:
                await maybe_await(prev.disconnect(), timeout=4)
            except Exception:
                pass

        c = SoroushClient(StringSession())
        clients_by_acc[acc_key] = {"client": c, "ready": False}
        await maybe_await(c.connect(), timeout=15)

        if hasattr(c, "send_code_request"):
            res = await maybe_await(c.send_code_request(phone), timeout=15)
        elif hasattr(c, "send_code"):
            res = await maybe_await(c.send_code(phone), timeout=15)
        else:
            res = await maybe_await(c.sign_in(phone=phone), timeout=15)

        pending_auth_by_acc[acc_key] = {
            "phone": phone,
            "phone_code_hash": getattr(res, "phone_code_hash", None),
        }
        state["auth_msg"] = f"کد برای {phone} ارسال شد؛ کد را وارد کنید."
    except Exception as e:
        state["auth_msg"] = f"خطا در ارسال کد: {e}"


async def do_verify_code(code):
    global active_acc_key
    acc_key = active_acc_key or norm_acc_key(state.get("phone"))
    pend = pending_auth_by_acc.get(acc_key) or {}
    phone = pend.get("phone") or state.get("phone", "").strip()
    phone_code_hash = pend.get("phone_code_hash")
    state["auth_msg"] = "در حال تأیید کد..."

    try:
        c = get_acc_client(acc_key)
        if not c:
            raise RuntimeError("کلاینت آماده نیست.")

        signed = False
        if phone_code_hash:
            for fn in (
                lambda: c.sign_in(phone=phone, code=code, phone_code_hash=phone_code_hash),
                lambda: c.sign_in(phone, code, phone_code_hash),
            ):
                try:
                    await maybe_await(fn(), timeout=15)
                    signed = True
                    break
                except TypeError:
                    continue
        if not signed:
            await maybe_await(c.sign_in(phone=phone, code=code), timeout=15)

        sess = c.session.save() if getattr(c, "session", None) else ""
        clients_by_acc[acc_key] = {"client": c, "ready": True}
        pending_auth_by_acc.pop(acc_key, None)

        name = phone
        try:
            me = await maybe_await(c.get_me(), timeout=5)
            if me and getattr(me, "first_name", None):
                name = str(me.first_name)
        except Exception:
            pass

        found = False
        for a in accounts:
            if a.get("key") == acc_key:
                a["phone"] = phone
                a["name"] = name
                a["session"] = sess
                a["connected"] = True
                a["kicked"] = False
                found = True
                break
        if not found:
            accounts.append({
                "key": acc_key,
                "phone": phone,
                "name": name,
                "session": sess,
                "connected": True,
                "kicked": False,
            })

        active_acc_key = acc_key
        save_sessions()
        state["kicked_out"] = False
        state["soroush_connected"] = True
        state["healer_status"] = "متصل به سروش"
        state["auth_msg"] = f"اکانت {name} متصل شد."
    except Exception as e:
        state["auth_msg"] = f"خطا در تأیید کد: {e}"


async def refresh_groups(acc_key=""):
    target_key = resolve_acc_key(acc_key)
    if not await check_acc_alive(target_key):
        return groups_by_acc.get(target_key, [])
    c = get_acc_client(target_key)
    try:
        if hasattr(c, "get_dialogs"):
            dialogs = await maybe_await(c.get_dialogs(limit=100), timeout=7)
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
                groups_by_acc[target_key] = items
    except Exception as e:
        if is_kicked_error(e):
            mark_account_kicked(target_key, str(e)[:80])
    return groups_by_acc.get(target_key, [])


async def find_channel_in_dialogs(acc_key, raw_chat):
    s = str(raw_chat or "").strip()
    abs_id = s.lstrip("-")
    candidates = peer_candidates(s)
    matched_name = ""
    c = get_acc_client(acc_key)

    if is_acc_ready(acc_key) and c is not None and hasattr(c, "get_dialogs"):
        try:
            dlgs = await maybe_await(c.get_dialogs(limit=100), timeout=8)
            for d in list(dlgs or []):
                entity = getattr(d, "entity", None) or d
                did_str = str(getattr(d, "id", None) or getattr(entity, "id", None) or "").strip()
                uname = str(getattr(entity, "username", None) or "").lstrip("@").strip()
                title = str(
                    getattr(d, "name", None)
                    or getattr(d, "title", None)
                    or getattr(entity, "title", None)
                    or ""
                ).strip()

                if abs_id and (abs_id in did_str or did_str.lstrip("-") == abs_id):
                    matched_name = title
                    if did_str and did_str.lstrip("-").isdigit():
                        for v in peer_candidates(did_str):
                            if v not in candidates:
                                candidates.insert(0, v)
                    break
                if uname and s.lstrip("@").lower() == uname.lower():
                    matched_name = title
                    break
        except Exception as e:
            if is_kicked_error(e):
                mark_account_kicked(acc_key, str(e)[:80])

    return candidates, matched_name


async def fetch_single_message(acc_key, raw_chat, msg_id):
    peer_cands, channel_name = await find_channel_in_dialogs(acc_key, raw_chat)
    resolved_chat = str(peer_cands[0]) if peer_cands else str(raw_chat)
    c = get_acc_client(acc_key)
    if not is_acc_ready(acc_key) or c is None or not hasattr(c, "get_messages"):
        return None, resolved_chat, channel_name

    for peer in peer_cands:
        try:
            res = await maybe_await(c.get_messages(peer, ids=int(msg_id)), timeout=7)
            if res is not None:
                if isinstance(res, (list, tuple)):
                    for item in res:
                        if item is not None:
                            return item, str(peer), channel_name
                else:
                    return res, str(peer), channel_name
        except Exception as e:
            if is_kicked_error(e):
                mark_account_kicked(acc_key, str(e)[:80])
                return None, resolved_chat, channel_name

    for peer in peer_cands:
        try:
            hist = await maybe_await(c.get_messages(peer, limit=50), timeout=7)
            if isinstance(hist, (list, tuple)):
                for item in hist:
                    if get_msg_id_val(item) == int(msg_id):
                        return item, str(peer), channel_name
        except Exception as e:
            if is_kicked_error(e):
                mark_account_kicked(acc_key, str(e)[:80])
                return None, resolved_chat, channel_name

    return None, resolved_chat, channel_name


async def read_channel_message_from_link(acc_key, link_raw):
    raw_chat, msg_id, clean_link = parse_splus_link(link_raw)
    msg_obj, resolved_chat_id, channel_name = await fetch_single_message(acc_key, raw_chat, msg_id)
    found_text = extract_msg_text(msg_obj)
    source_label = f"پیام #{msg_id} از کانال" if found_text else "پیدا نشد"
    note = ""
    if not found_text:
        if is_acc_ready(acc_key):
            note = f"پیام #{msg_id} از کانال ({raw_chat}) خوانده نشد؛ متن را اینجا وارد کن و تأیید بزن."
        else:
            note = "ابتدا وارد حساب سروش شو تا پیام از کانال خوانده شود."

    return {
        "ok": True,
        "chat_id": resolved_chat_id,
        "channel_name": channel_name,
        "msg_id": int(msg_id),
        "link": clean_link,
        "source_label": source_label,
        "text": found_text,
        "note": note,
    }


async def identify_user_from_link(acc_key, link_raw, forced_chat=""):
    raw_chat, msg_id, clean_link = parse_splus_link(link_raw)
    chat_to_use = clean_target(forced_chat) or raw_chat
    msg_obj, resolved_chat_id, channel_name = await fetch_single_message(acc_key, chat_to_use, msg_id)
    user_id, user_name = get_sender_info(msg_obj)
    if not user_id and not user_name:
        return {
            "ok": False,
            "msg": f"کاربر پیام #{msg_id} در چت {resolved_chat_id} شناسایی نشد (مطمئن شو اکانت در چت دسترسی دارد).",
        }
    return {
        "ok": True,
        "chat_id": resolved_chat_id,
        "channel_name": channel_name,
        "msg_id": int(msg_id),
        "user_id": str(user_id or user_name),
        "user_name": str(user_name or user_id),
        "link": clean_link,
        "msg": f"کاربر «{user_name or user_id}» در چت {channel_name or resolved_chat_id} شناسایی شد.",
    }


async def send_reply_message(c, peer, text_payload, reply_to_id):
    sent_ok = False
    last_err = None
    for kw_name in ("reply_to", "reply_to_msg_id"):
        try:
            await maybe_await(c.send_message(peer, text_payload, **{kw_name: int(reply_to_id)}), timeout=7)
            sent_ok = True
            break
        except TypeError as te:
            last_err = te
            continue
        except Exception as e:
            last_err = e
            if is_kicked_error(e):
                raise
            break
    if not sent_ok:
        await maybe_await(c.send_message(peer, text_payload), timeout=7)
    return last_err


async def execute_job_step(job, raw_target):
    acc_key = job.get("acc_key") or resolve_acc_key("")
    c = get_acc_client(acc_key)
    if not is_acc_ready(acc_key) or c is None:
        raise RuntimeError("اکانت متصل نیست.")

    mode = job.get("mode") or "spam"
    payload = str(job.get("payload") or "")
    dst_cands = peer_candidates(raw_target)
    if not dst_cands:
        raise ValueError("مقصد نامعتبر است.")

    if mode == "reply_chain":
        reply_msg_id = int(job.get("reply_to_msg_id") or job.get("msg_id") or 0)
        if reply_msg_id <= 0:
            raise ValueError("شناسه پیام ریپلای مشخص نیست.")
        if not payload.strip():
            raise ValueError("متن ریپلای خالی است.")
        last_err = None
        for peer in dst_cands:
            try:
                await send_reply_message(c, peer, payload, reply_msg_id)
                add_log(job, f"REPLY #{reply_msg_id} OK -> {raw_target}")
                return
            except Exception as e:
                last_err = e
                if is_kicked_error(e):
                    raise
        raise last_err if last_err else RuntimeError("ریپلای انجام نشد.")

    if mode == "forward":
        src_cands = peer_candidates(job.get("source_chat")) if job.get("source_chat") else []
        msg_id = int(job.get("msg_id") or 0)
        sent_ok = False
        last_err = None

        if src_cands and msg_id > 0:
            for fn_name in ("forward_messages", "forward_message"):
                fn = getattr(c, fn_name, None)
                if not callable(fn):
                    continue
                for dst_p in dst_cands:
                    for src_p in src_cands:
                        try:
                            await maybe_await(fn(dst_p, msg_id, from_peer=src_p), timeout=8)
                            sent_ok = True
                            add_log(job, f"FWD OK -> {raw_target} (#{msg_id})")
                            break
                        except Exception as e:
                            last_err = e
                            if is_kicked_error(e):
                                raise
                    if sent_ok:
                        break
                if sent_ok:
                    break

        if not sent_ok:
            if not payload.strip():
                raise RuntimeError("متن پیام خالی است.")
            for dst_p in dst_cands:
                try:
                    await maybe_await(c.send_message(dst_p, payload), timeout=8)
                    sent_ok = True
                    add_log(job, f"FWD/MSG OK -> {raw_target} (#{msg_id})")
                    break
                except Exception as e:
                    last_err = e
                    if is_kicked_error(e):
                        raise

        if not sent_ok:
            raise last_err if last_err else RuntimeError("فوروارد انجام نشد.")
        return

    last_err = None
    for peer in dst_cands:
        try:
            await maybe_await(c.send_message(peer, payload), timeout=8)
            add_log(job, f"SPAM OK -> {raw_target}")
            return
        except Exception as e:
            last_err = e
            if is_kicked_error(e):
                raise
    raise last_err if last_err else RuntimeError("ارسال اسپم ناموفق بود.")


async def smart_heal(job, raw_target, err):
    acc_key = job.get("acc_key") or resolve_acc_key("")
    if is_kicked_error(err):
        mark_account_kicked(acc_key, str(err)[:80])
        return False

    job["heal_state"] = "healing"
    job["last_error"] = str(err)[:90]
    add_log(job, f"HEAL -> اصلاح خطا و ادامه روی {clean_target(raw_target)}...")

    normalized = clean_target(raw_target)
    if normalized:
        try:
            await execute_job_step(job, normalized)
            job["heal_state"] = "resumed"
            job["last_error"] = ""
            add_log(job, f"FIXED & RESUMED -> {normalized}")
            return True
        except Exception as e1:
            if is_kicked_error(e1):
                mark_account_kicked(acc_key, str(e1)[:80])
                return False

    acc_obj = next((a for a in accounts if a.get("key") == acc_key), None)
    reconnected = await connect_single_account(acc_obj) if acc_obj else False
    if reconnected:
        try:
            await execute_job_step(job, normalized or raw_target)
            job["heal_state"] = "resumed"
            job["last_error"] = ""
            add_log(job, f"RECONNECTED & RESUMED -> {normalized or raw_target}")
            return True
        except Exception as e2:
            if is_kicked_error(e2):
                mark_account_kicked(acc_key, str(e2)[:80])
                return False
            job["last_error"] = str(e2)[:90]

    job["heal_state"] = "normal"
    return False


async def run_job_worker(job):
    while job in jobs:
        if not job.get("running"):
            await asyncio.sleep(0.2)
            continue

        acc_key = job.get("acc_key") or resolve_acc_key("")
        if not is_acc_ready(acc_key):
            job["running"] = False
            job["heal_state"] = "kicked"
            job["last_error"] = "اکانت متصل نیست"
            add_log(job, "STOP -> اکانت متصل نیست")
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

        async with op_lock:
            try:
                await execute_job_step(job, current_target)
                job["ok"] = int(job.get("ok") or 0) + 1
                job["t_idx"] = (idx + 1) % len(targets)
                job["sent_burst"] = int(job.get("sent_burst") or 0) + 1
                job["heal_state"] = "running"
                job["last_error"] = ""
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
                    job["t_idx"] = (idx + 1) % len(targets)
                    add_log(job, f"ERR -> {err_txt[:75]}")

        rest_every = int(job.get("rest_every") or 0)
        rest_ms = int(job.get("rest_ms") or 0)
        if rest_every > 0 and rest_ms > 0 and int(job.get("sent_burst") or 0) >= rest_every:
            job["sent_burst"] = 0
            await asyncio.sleep(rest_ms / 1000.0)
            continue

        await asyncio.sleep(max(40, int(job.get("speed_ms") or 500)) / 1000.0)


async def poll_offline_rules():
    now_ts = time.time()
    for rule in list(offline_rules):
        if not rule.get("running"):
            continue
        acc_key = rule.get("acc_key") or resolve_acc_key("")
        c = get_acc_client(acc_key)
        if not is_acc_ready(acc_key) or c is None or not hasattr(c, "get_messages"):
            continue
        peers = peer_candidates(rule.get("chat_id"))
        if not peers:
            continue
        users = rule.get("users") or []
        if not users:
            continue

        timeout_sec = max(10, float(rule.get("minutes") or 1) * 60.0)
        reply_text = str(rule.get("reply_text") or "").strip()

        for p in peers:
            try:
                msgs = await maybe_await(c.get_messages(p, limit=15), timeout=5)
                if not isinstance(msgs, (list, tuple)):
                    continue
                for u in users:
                    uid_key = str(u.get("user_id") or "").strip().lower()
                    uname_key = str(u.get("user_name") or "").strip().lower()
                    matched_msg = None
                    for m in msgs:
                        uid, uname = get_sender_info(m)
                        if (uid_key and uid_key == str(uid).lower()) or (
                            uname_key and uname_key in f"{uid} {uname}".lower()
                        ):
                            matched_msg = m
                            break

                    if matched_msg is not None:
                        mid = get_msg_id_val(matched_msg)
                        if mid and mid != int(u.get("last_msg_id") or 0):
                            u["last_msg_id"] = mid
                            u["last_msg_ts"] = now_ts
                            u["replied_for_msg_id"] = 0
                            u["status"] = f"پیام #{mid} ثبت شد (در حال پایش سکوت)"

                    last_ts = float(u.get("last_msg_ts") or 0)
                    cur_mid = int(u.get("last_msg_id") or 0)
                    replied_mid = int(u.get("replied_for_msg_id") or 0)
                    if last_ts > 0 and cur_mid > 0 and replied_mid != cur_mid and reply_text:
                        elapsed = now_ts - last_ts
                        if elapsed >= timeout_sec:
                            async with op_lock:
                                await send_reply_message(c, p, reply_text, cur_mid)
                            u["replied_for_msg_id"] = cur_mid
                            u["replied_count"] = int(u.get("replied_count") or 0) + 1
                            rule["replied_count"] = int(rule.get("replied_count") or 0) + 1
                            u["status"] = f"آفلاین شد → ریپلای روی #{cur_mid} ({now_str()})"
                        else:
                            rem_sec = int(timeout_sec - elapsed)
                            u["status"] = f"فعال (مانده تا آفلاین: {rem_sec}s)"
            except Exception as e:
                if is_kicked_error(e):
                    mark_account_kicked(acc_key, str(e)[:80])
                    break


async def poll_enemy_rules():
    for er in list(enemy_rules):
        if not er.get("running"):
            continue
        acc_key = er.get("acc_key") or resolve_acc_key("")
        c = get_acc_client(acc_key)
        if not is_acc_ready(acc_key) or c is None or not hasattr(c, "get_messages"):
            continue

        max_c = int(er.get("max_count") or 0)
        if max_c > 0 and int(er.get("replied_count") or 0) >= max_c:
            er["running"] = False
            er["status"] = "تکمیل شد"
            continue

        peers = peer_candidates(er.get("chat_id"))
        if not peers:
            continue
        enemy_uid = str(er.get("user_id") or "").strip().lower()
        enemy_name = str(er.get("user_name") or "").strip().lower()
        seen_ids = set(er.get("replied_msg_ids") or [])

        for p in peers:
            try:
                msgs = await maybe_await(c.get_messages(p, limit=5), timeout=3)
                if not isinstance(msgs, (list, tuple)):
                    continue
                ordered = sorted(
                    [m for m in msgs if get_msg_id_val(m) > 0],
                    key=lambda x: get_msg_id_val(x),
                )
                for m in ordered:
                    mid = get_msg_id_val(m)
                    if mid in seen_ids:
                        continue
                    uid, uname = get_sender_info(m)
                    u_match = (enemy_uid and enemy_uid == str(uid).lower()) or (
                        enemy_name and enemy_name in f"{uid} {uname}".lower()
                    )
                    if u_match:
                        async with op_lock:
                            try:
                                await send_reply_message(c, p, er.get("reply_text") or "", mid)
                                seen_ids.add(mid)
                                er["replied_msg_ids"] = list(seen_ids)[-80:]
                                er["replied_count"] = int(er.get("replied_count") or 0) + 1
                                er["last_reply_at"] = now_str()
                                er["status"] = f"ریپلای آنی روی پیام #{mid}"
                                if max_c > 0 and int(er.get("replied_count") or 0) >= max_c:
                                    er["running"] = False
                                    er["status"] = "تکمیل شد"
                                    break
                            except Exception as e:
                                er["last_error"] = str(e)[:70]
            except Exception as e:
                if is_kicked_error(e):
                    mark_account_kicked(acc_key, str(e)[:80])
                    break


async def watcher_loop():
    tick = 0
    for a in list(accounts):
        await connect_single_account(a)

    while True:
        await asyncio.sleep(0.35)
        tick += 1
        await poll_enemy_rules()
        if tick % 10 == 0:
            await poll_offline_rules()
        if tick % 25 == 0:
            for a in list(accounts):
                await check_acc_alive(a["key"])


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
                active[j["id"]] = asyncio.create_task(run_job_worker(j))
        await asyncio.sleep(0.35)


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
    return jsonify({"ok": True, "auth_msg": state["auth_msg"], "active_acc_key": active_acc_key})


@app.route("/api/verify_code")
def api_verify_code():
    code = (request.args.get("code") or "").strip()
    fut = run_async(do_verify_code(code))
    if fut:
        try:
            fut.result(timeout=18)
        except Exception:
            pass
    return jsonify({"ok": True, "auth_msg": state["auth_msg"], "active_acc_key": active_acc_key})


@app.route("/api/switch_account")
def api_switch_account():
    global active_acc_key
    key = norm_acc_key(request.args.get("key") or "")
    for a in accounts:
        if a.get("key") == key:
            active_acc_key = key
            state["phone"] = a.get("phone", "")
            state["kicked_out"] = bool(a.get("kicked"))
            state["soroush_connected"] = is_acc_ready(key)
            state["auth_msg"] = f"اکانت فعال: {a.get('name') or a.get('phone')}"
            save_sessions()
            return jsonify({"ok": True, "active_acc_key": active_acc_key})
    return jsonify({"ok": False, "msg": "اکانت پیدا نشد."})


@app.route("/api/del_account")
def api_del_account():
    global active_acc_key
    idx = int(request.args.get("index", -1))
    removed = None
    if 0 <= idx < len(accounts):
        removed = accounts.pop(idx)
    if removed:
        rkey = removed["key"]
        clients_by_acc.pop(rkey, None)
        groups_by_acc.pop(rkey, None)
        jobs[:] = [j for j in jobs if j.get("acc_key") != rkey]
        enemy_rules[:] = [e for e in enemy_rules if e.get("acc_key") != rkey]
        offline_rules[:] = [r for r in offline_rules if r.get("acc_key") != rkey]
        save_jobs()
    active_acc_key = accounts[0]["key"] if accounts else ""
    save_sessions()
    state["soroush_connected"] = is_acc_ready(active_acc_key) if active_acc_key else False
    state["kicked_out"] = False
    state["healer_status"] = "حذف‌شده"
    return jsonify({"ok": True, "active_acc_key": active_acc_key})


@app.route("/api/groups")
def api_groups():
    acc_key = resolve_acc_key(request.args.get("acc_key") or "")
    fut = run_async(refresh_groups(acc_key))
    if fut:
        try:
            res = fut.result(timeout=7)
            return jsonify(res if isinstance(res, list) else groups_by_acc.get(acc_key, []))
        except Exception:
            pass
    return jsonify(groups_by_acc.get(acc_key, []))


@app.route("/api/fetch_msg")
def api_fetch_msg():
    link = (request.args.get("link") or "").strip()
    acc_key = resolve_acc_key(request.args.get("acc_key") or "")
    if not link:
        return jsonify({"ok": False, "msg": "لینک پیام را وارد کنید."})
    fut = run_async(read_channel_message_from_link(acc_key, link))
    if fut:
        try:
            return jsonify(fut.result(timeout=12))
        except Exception as e:
            return jsonify({"ok": False, "msg": str(e)})
    raw_chat, msg_id, clean_link = parse_splus_link(link)
    return jsonify({
        "ok": True,
        "chat_id": raw_chat,
        "channel_name": "",
        "msg_id": msg_id,
        "link": clean_link,
        "source_label": "پیدا نشد",
        "text": "",
        "note": "متن از کانال خوانده نشد.",
    })


@app.route("/api/identify_user")
def api_identify_user():
    link = (request.args.get("link") or "").strip()
    chat_id = (request.args.get("chat_id") or "").strip()
    acc_key = resolve_acc_key(request.args.get("acc_key") or "")
    if not link:
        return jsonify({"ok": False, "msg": "لینک پیام را وارد کنید."})
    fut = run_async(identify_user_from_link(acc_key, link, chat_id))
    if fut:
        try:
            return jsonify(fut.result(timeout=12))
        except Exception as e:
            return jsonify({"ok": False, "msg": str(e)})
    return jsonify({"ok": False, "msg": "خطا در شناسایی کاربر."})


@app.route("/api/save_offline_rule")
def api_save_offline_rule():
    global offline_seq
    acc_key = resolve_acc_key(request.args.get("acc_key") or "")
    if not is_acc_ready(acc_key):
        return jsonify({"ok": False, "msg": "اکانت انتخاب‌شده متصل نیست."})

    chat_id = clean_target(request.args.get("chat_id") or "")
    minutes = max(0.1, float(request.args.get("minutes") or 3))
    reply_text = (request.args.get("reply_text") or "").strip()
    raw_users = request.args.get("users") or "[]"
    try:
        users_in = json.loads(raw_users)
    except Exception:
        users_in = []

    if not chat_id:
        return jsonify({"ok": False, "msg": "ابتدا چت را انتخاب کنید."})
    if not users_in:
        return jsonify({"ok": False, "msg": "حداقل یک کاربر را با لینک پیام به لیست اضافه کنید."})
    if not reply_text:
        return jsonify({"ok": False, "msg": "متن ریپلای آفلاین را وارد کنید."})

    cleaned_users = []
    now_ts = time.time()
    for u in users_in:
        if not isinstance(u, dict):
            continue
        uid = str(u.get("user_id") or "").strip()
        uname = str(u.get("user_name") or uid).strip()
        mid = int(u.get("msg_id") or 0)
        if uid or uname:
            cleaned_users.append({
                "user_id": uid or uname,
                "user_name": uname or uid,
                "last_msg_id": mid,
                "last_msg_ts": now_ts if mid > 0 else 0,
                "replied_for_msg_id": 0,
                "replied_count": 0,
                "status": f"در انتظار سکوت {minutes} دقیقه‌ای",
            })

    if not cleaned_users:
        return jsonify({"ok": False, "msg": "کاربر معتبری در لیست نیست."})

    offline_seq += 1
    rule = {
        "id": str(offline_seq),
        "acc_key": acc_key,
        "acc_label": get_acc_label(acc_key),
        "chat_id": chat_id,
        "minutes": minutes,
        "reply_text": reply_text,
        "users": cleaned_users,
        "replied_count": 0,
        "running": True,
    }
    offline_rules.append(rule)
    return jsonify({"ok": True, "msg": f"قانون آفلاینی بگیر برای اکانت {get_acc_label(acc_key)} در چت {chat_id} فعال شد."})


@app.route("/api/offline_ctrl")
def api_offline_ctrl():
    oid = (request.args.get("id") or "").strip()
    act = (request.args.get("act") or "").strip()
    for r in list(offline_rules):
        if r["id"] == oid:
            if act == "toggle":
                if not r.get("running") and not is_acc_ready(r.get("acc_key") or ""):
                    return jsonify({"ok": False, "msg": "اکانت مربوط به این قانون متصل نیست."})
                r["running"] = not r.get("running", False)
            elif act == "delete":
                offline_rules.remove(r)
            break
    return jsonify({"ok": True})


@app.route("/api/add_enemy")
def api_add_enemy():
    global enemy_seq
    acc_key = resolve_acc_key(request.args.get("acc_key") or "")
    if not is_acc_ready(acc_key):
        return jsonify({"ok": False, "msg": "اکانت انتخاب‌شده متصل نیست."})

    chat_id = clean_target(request.args.get("chat_id") or "")
    user_id = (request.args.get("user_id") or "").strip()
    user_name = (request.args.get("user_name") or user_id).strip()
    reply_text = (request.args.get("reply_text") or "").strip()
    max_count = max(0, int(request.args.get("max_count") or 0))

    if not chat_id or not user_id:
        return jsonify({"ok": False, "msg": "ابتدا با لینک پیام، کاربر را در چت شناسایی کنید."})
    if not reply_text:
        return jsonify({"ok": False, "msg": "متن ریپلای دشمن را وارد کنید."})

    enemy_seq += 1
    er = {
        "id": str(enemy_seq),
        "acc_key": acc_key,
        "acc_label": get_acc_label(acc_key),
        "chat_id": chat_id,
        "user_id": user_id,
        "user_name": user_name,
        "reply_text": reply_text,
        "max_count": max_count,
        "replied_count": 0,
        "replied_msg_ids": [],
        "last_reply_at": "-",
        "last_error": "",
        "status": "آماده ریپلای آنی",
        "running": True,
    }
    enemy_rules.append(er)
    return jsonify({"ok": True, "msg": f"دشمن «{user_name}» برای اکانت {get_acc_label(acc_key)} ثبت شد."})


@app.route("/api/enemy_ctrl")
def api_enemy_ctrl():
    eid = (request.args.get("id") or "").strip()
    act = (request.args.get("act") or "").strip()
    for er in list(enemy_rules):
        if er["id"] == eid:
            if act == "toggle":
                if not er.get("running") and not is_acc_ready(er.get("acc_key") or ""):
                    return jsonify({"ok": False, "msg": "اکانت مربوط به این قانون متصل نیست."})
                er["running"] = not er.get("running", False)
                er["status"] = "آماده ریپلای آنی" if er["running"] else "متوقف"
            elif act == "delete":
                enemy_rules.remove(er)
            break
    return jsonify({"ok": True})


@app.route("/api/add_job")
def api_add_job():
    global job_seq
    acc_key = resolve_acc_key(request.args.get("acc_key") or "")
    if not is_acc_ready(acc_key):
        return jsonify({"ok": False, "msg": "اکانت انتخاب‌شده متصل نیست یا از سشن خارج شده است."})

    mode = (request.args.get("mode") or "spam").strip().lower()
    if mode not in ("spam", "forward", "reply_chain"):
        mode = "spam"

    payload = request.args.get("payload") or ""
    raw_link = (request.args.get("link") or "").strip()
    raw_targets = (request.args.get("targets") or "").strip()
    reply_to_msg_id = int(request.args.get("reply_to_msg_id") or request.args.get("msg_id") or 0)

    if mode == "reply_chain" and raw_link:
        parsed_chat, parsed_msg_id, _ = parse_splus_link(raw_link)
        if not raw_targets and parsed_chat and parsed_chat != "unknown_chat":
            raw_targets = parsed_chat
        if reply_to_msg_id <= 0 and parsed_msg_id > 0:
            reply_to_msg_id = parsed_msg_id

    targets = [clean_target(x) for x in re.split(r"[,\n]+", raw_targets) if clean_target(x)][:MAX_TARGETS_PER_JOB]

    if not targets:
        return jsonify({"ok": False, "msg": "لینک پیام یا مقصد معتبر وارد کنید."})
    if mode == "reply_chain" and reply_to_msg_id <= 0:
        return jsonify({"ok": False, "msg": "لینک پیام معتبر برای ریپلای وارد کنید."})
    if not payload.strip():
        return jsonify({"ok": False, "msg": "متن پیام خالی است."})

    active_count = sum(1 for j in jobs if j.get("running"))
    if active_count >= MAX_ACTIVE_JOBS:
        return jsonify({"ok": False, "msg": f"فقط {MAX_ACTIVE_JOBS} تسک هم‌زمان قابل اجراست."})

    job_seq += 1
    acc_label = get_acc_label(acc_key)
    job = {
        "id": str(100 + job_seq),
        "acc_key": acc_key,
        "acc_label": acc_label,
        "mode": mode,
        "source_chat": clean_target(request.args.get("source_chat") or ""),
        "msg_id": int(request.args.get("msg_id") or reply_to_msg_id or 0),
        "reply_to_msg_id": reply_to_msg_id,
        "link": raw_link,
        "payload": payload,
        "targets": targets,
        "t_idx": 0,
        "sent_burst": 0,
        "speed_ms": max(40, int(request.args.get("speed_ms") or 500)),
        "max_count": max(0, int(request.args.get("max_count") or 0)),
        "rest_every": max(0, int(request.args.get("rest_every") or 0)),
        "rest_ms": max(0, int(request.args.get("rest_ms") or 0)),
        "running": True,
        "heal_state": "running",
        "last_error": "",
        "ok": 0,
        "fail": 0,
        "logs": [f"[{now_str()}] started ({mode}) on {acc_label}"],
    }
    jobs.append(job)
    save_jobs()
    return jsonify({"ok": True, "msg": f"تسک {mode} #{job['id']} روی اکانت {acc_label} شروع شد."})


@app.route("/api/job_ctrl")
def api_job_ctrl():
    jid = (request.args.get("id") or "").strip()
    act = (request.args.get("act") or "").strip()
    for j in jobs:
        if j["id"] == jid:
            if act == "toggle":
                if not j.get("running"):
                    j_acc = j.get("acc_key") or resolve_acc_key("")
                    if not is_acc_ready(j_acc):
                        return jsonify({"ok": False, "msg": "اکانت این تسک متصل نیست."})
                    active_count = sum(1 for x in jobs if x.get("running"))
                    if active_count >= MAX_ACTIVE_JOBS:
                        return jsonify({"ok": False, "msg": f"فقط {MAX_ACTIVE_JOBS} تسک هم‌زمان قابل اجراست."})
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

    cur_acc = get_active_account()
    cur_key = cur_acc["key"] if cur_acc else ""
    cur_ready = is_acc_ready(cur_key) if cur_key else False
    cur_kicked = bool(cur_acc.get("kicked")) if cur_acc else False
    cur_groups = groups_by_acc.get(cur_key, [])

    acc_list_out = [
        {
            "key": a["key"],
            "name": a["name"],
            "phone": a["phone"],
            "connected": is_acc_ready(a["key"]),
            "kicked": bool(a.get("kicked")),
            "active": a["key"] == cur_key,
        }
        for a in accounts
    ]

    return jsonify({
        "ping": state["ping"],
        "cpu": state["cpu"],
        "ram": state["ram"],
        "healer_status": state["healer_status"],
        "soroush_connected": cur_ready,
        "kicked_out": cur_kicked,
        "auth_msg": state["auth_msg"],
        "active_acc_key": cur_key,
        "accounts": acc_list_out,
        "groups": cur_groups,
        "jobs": jobs,
        "offline_rules": offline_rules,
        "enemy_rules": enemy_rules,
    })


accounts[:] = load_sessions()
loaded_jobs = load_jobs()
jobs[:] = loaded_jobs if isinstance(loaded_jobs, list) else []

Thread(target=start_bot_thread, daemon=True).start()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT, threaded=True)
