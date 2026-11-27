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


def _resolve_state_dir():
    preferred = (os.environ.get("FONEX_STATE_DIR") or "").strip()
    candidates = [d for d in (preferred, "/tmp/fonex_state", ".") if d]
    for d in candidates:
        try:
            os.makedirs(d, exist_ok=True)
            test_path = os.path.join(d, ".write_test")
            with open(test_path, "w", encoding="utf-8") as tf:
                tf.write("ok")
            os.remove(test_path)
            return d
        except Exception:
            continue
    return "."


STATE_DIR = _resolve_state_dir()


def _state_path(filename):
    return os.path.join(STATE_DIR, filename)


SESSIONS_FILE = _state_path("soroush_sessions.json")
JOBS_FILE = _state_path("fonex_jobs.json")
ENEMIES_FILE = _state_path("fonex_enemies.json")
GROUPS_CACHE_FILE = _state_path("fonex_groups_cache.json")

MAX_ACTIVE_JOBS = 5
MAX_TARGETS_PER_JOB = 3

KICK_KEYWORDS = (
    "session_revoked",
    "auth_key_unregistered",
    "session expired",
    "session_expired",
    "user_deactivated",
    "logged out",
    "log out",
)

app = Flask(__name__)

accounts = []
active_acc_key = ""

groups_by_acc = {}
jobs = []
enemy_rules = []
clients_by_acc = {}
pending_auth_by_acc = {}

job_seq = 100
enemy_seq = 0
event_seq = 1
_refreshing_keys = set()

state = {
    "ping": "12 ms",
    "cpu": "0",
    "ram": "0",
    "auth_msg": "آماده ورود شماره",
    "phone": "",
    "healer_status": "آماده",
    "soroush_connected": False,
    "kicked_out": False,
    "event_notice": "",
}

bot_loop = None


@app.after_request
def add_cors(resp):
    resp.headers["Access-Control-Allow-Origin"] = "*"
    resp.headers["Access-Control-Allow-Headers"] = "*"
    resp.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
    return resp


def now_str():
    return datetime.now().strftime("%H:%M:%S")


def emit_soroush_event(notice_text=""):
    global event_seq
    event_seq += 1
    if notice_text:
        state["event_notice"] = notice_text
    return event_seq


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
    job["logs"] = logs[-35:]


def parse_multi_texts(raw_val):
    if isinstance(raw_val, list):
        return [str(x).strip() for x in raw_val if str(x).strip()]
    s = str(raw_val or "").strip()
    if not s:
        return []
    if s.startswith("["):
        try:
            arr = json.loads(s)
            if isinstance(arr, list):
                return [str(x).strip() for x in arr if str(x).strip()]
        except Exception:
            pass
    parts = [p.strip() for p in re.split(r"\n?\|\|\|\n?", s) if p.strip()]
    return parts if parts else [s]


def _atomic_write_json(filepath, data):
    tmp = filepath + ".tmp"
    bak = filepath + ".bak"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        if os.path.exists(filepath):
            try:
                with open(filepath, "r", encoding="utf-8") as src, open(bak, "w", encoding="utf-8") as dst:
                    dst.write(src.read())
            except Exception:
                pass
        os.replace(tmp, filepath)
    except Exception:
        try:
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except Exception:
            pass


def _read_json_with_fallback(filepath, default_val):
    for cand in (filepath, filepath + ".bak"):
        if os.path.exists(cand):
            try:
                with open(cand, "r", encoding="utf-8") as f:
                    parsed = json.load(f)
                    if parsed is not None:
                        return parsed
            except Exception:
                continue
    return default_val


def save_sessions():
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
    _atomic_write_json(SESSIONS_FILE, payload)


def load_sessions():
    global active_acc_key
    loaded = []
    data = _read_json_with_fallback(SESSIONS_FILE, {})
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
    env_sess = (os.environ.get("SOROUSH_SESSION") or "").strip()
    env_phone = (os.environ.get("SOROUSH_PHONE") or "env_account").strip()
    if env_sess and not loaded:
        ekey = norm_acc_key(env_phone) or "env_acc"
        loaded.append({
            "key": ekey,
            "phone": env_phone,
            "name": env_phone,
            "session": env_sess,
            "connected": False,
            "kicked": False,
        })
    if not active_acc_key and loaded:
        active_acc_key = loaded[0]["key"]
    return loaded


def save_jobs():
    _atomic_write_json(JOBS_FILE, jobs)


def load_jobs():
    global job_seq
    cleaned = []
    max_num = 100
    default_key = resolve_acc_key("")
    arr = _read_json_with_fallback(JOBS_FILE, [])
    if isinstance(arr, list):
        for j in arr:
            if not isinstance(j, dict):
                continue
            jid_str = str(j.get("id") or "").strip()
            if jid_str.isdigit() and int(jid_str) > max_num:
                max_num = int(jid_str)
            mode = j.get("mode") if j.get("mode") in ("spam", "forward") else "spam"
            j["mode"] = mode
            j["acc_key"] = norm_acc_key(j.get("acc_key") or "") or default_key
            j["targets"] = (j.get("targets") or [])[:MAX_TARGETS_PER_JOB]
            texts = parse_multi_texts(j.get("texts") or j.get("payload") or "")
            j["texts"] = texts
            j["payload"] = texts[0] if texts else str(j.get("payload") or "")
            j.setdefault("text_idx", 0)
            j.setdefault("t_idx", 0)
            j.setdefault("sent_burst", 0)
            j.setdefault("speed_ms", 500)
            j.setdefault("last_step_ms", 0)
            j.setdefault("effective_delay_ms", int(j.get("speed_ms") or 500))
            j.setdefault("heal_state", "running" if j.get("running") else "paused")
            j.setdefault("last_error", "")
            j.setdefault("ok", 0)
            j.setdefault("fail", 0)
            j.setdefault("logs", [])
            if j.get("running"):
                add_log(j, "RESTORED -> ادامه خودکار پس از ری‌استارت سرور")
            cleaned.append(j)
    job_seq = max(max_num, 100 + len(cleaned))
    return cleaned


def save_enemies():
    _atomic_write_json(ENEMIES_FILE, enemy_rules)


def load_enemies():
    global enemy_seq
    cleaned = []
    max_num = 0
    arr = _read_json_with_fallback(ENEMIES_FILE, [])
    if isinstance(arr, list):
        for er in arr:
            if isinstance(er, dict):
                eid_str = str(er.get("id") or "").strip()
                if eid_str.isdigit() and int(eid_str) > max_num:
                    max_num = int(eid_str)
                r_texts = parse_multi_texts(er.get("reply_texts") or er.get("reply_text") or "")
                er["reply_texts"] = r_texts
                er["reply_text"] = r_texts[0] if r_texts else str(er.get("reply_text") or "")
                er.setdefault("reply_idx", 0)
                er.setdefault("speed_ms", 40)
                er.setdefault("last_step_ms", 0)
                er.setdefault("replied_msg_ids", [])
                er.setdefault("last_seen_msg_id", 0)
                cleaned.append(er)
    enemy_seq = max(max_num, len(cleaned))
    return cleaned


def save_groups_cache():
    _atomic_write_json(GROUPS_CACHE_FILE, groups_by_acc)


def load_groups_cache():
    data = _read_json_with_fallback(GROUPS_CACHE_FILE, {})
    return data if isinstance(data, dict) else {}


def normalize_numeric_id(raw_num):
    s = str(raw_num or "").strip().replace("\u200f", "").replace("\u200e", "")
    if s.endswith("-") and s[:-1].isdigit():
        s = "-" + s[:-1]
    digits = s.lstrip("-")
    if not digits.isdigit():
        return s
    if digits.startswith("100") and len(digits) > 5:
        base_id = digits[3:]
        return f"-100{base_id}"
    return f"-100{digits}" if s.startswith("-") else digits


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
                    return normalize_numeric_id(parts[1].strip())
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
    if s.lstrip("-").isdigit():
        return normalize_numeric_id(s)
    return s


def parse_splus_link(link_raw):
    s = str(link_raw or "").strip().replace("\u200f", "").replace("\u200e", "")
    raw_chat = ""
    msg_id = 0

    m_c = re.search(r"(?:https?://)?(?:www\.)?splus\.ir/c/([^/\s?#]+)/(\d+)", s)
    if m_c:
        raw_chat = normalize_numeric_id(m_c.group(1).strip())
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
        digits = base.lstrip("-")
        core = digits[3:] if (digits.startswith("100") and len(digits) > 5) else digits
        for cand_s in (
            base,
            f"-100{core}",
            f"-{core}",
            core,
            f"-100{digits}",
            f"-{digits}",
            digits,
        ):
            if cand_s.lstrip("-").isdigit():
                val = int(cand_s)
                if val not in out:
                    out.append(val)
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
    for attr in ("text", "message", "caption", "body", "content", "raw_text"):
        val = getattr(obj, attr, None)
        if isinstance(val, str) and val.strip():
            return val.strip()
    for inner_attr in ("message_obj", "msg", "media"):
        inner = getattr(obj, inner_attr, None)
        if inner is not None and inner is not obj and not isinstance(inner, list):
            t = extract_msg_text(inner)
            if t:
                return t
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


async def maybe_await(res, timeout=14):
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
    enemy_changed = False
    for er in enemy_rules:
        if er.get("acc_key") == acc_key and er.get("running"):
            er["running"] = False
            er["status"] = "خروج از اکانت"
            enemy_changed = True
    if changed:
        save_jobs()
    if enemy_changed:
        save_enemies()

    if acc_key == active_acc_key:
        state["soroush_connected"] = False
        state["kicked_out"] = True
        state["healer_status"] = "خارج شده از اکانت"
        state["auth_msg"] = f"خروج از اکانت تشخیص داده شد: {reason}"
    emit_soroush_event("وضعیت اکانت تغییر کرد")


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


def schedule_bg(coro):
    try:
        return run_async(coro)
    except Exception:
        return None


def pick_next_job_text(job):
    texts = parse_multi_texts(job.get("texts") or job.get("payload") or "")
    if not texts:
        return str(job.get("payload") or "")
    idx = int(job.get("text_idx") or 0) % len(texts)
    chosen = texts[idx]
    job["text_idx"] = (idx + 1) % len(texts)
    job["payload"] = chosen
    return chosen


def pick_next_enemy_text(er):
    texts = parse_multi_texts(er.get("reply_texts") or er.get("reply_text") or "")
    if not texts:
        return str(er.get("reply_text") or "")
    idx = int(er.get("reply_idx") or 0) % len(texts)
    chosen = texts[idx]
    er["reply_idx"] = (idx + 1) % len(texts)
    er["reply_text"] = chosen
    return chosen


def _classify_dialog_kind(d, entity):
    if getattr(d, "is_group", False) or getattr(d, "is_channel", False):
        return "group"
    if getattr(entity, "megagroup", False) or getattr(entity, "broadcast", False):
        return "group"
    cls_name = type(entity).__name__.lower()
    if "channel" in cls_name or "chat" in cls_name:
        return "group"
    if getattr(d, "is_user", False) or "user" in cls_name:
        return "private"
    did_s = str(getattr(d, "id", None) or getattr(entity, "id", None) or "").strip()
    if did_s.startswith("-"):
        return "group"
    return "private"


async def refresh_groups(acc_key="", force_alive=True, kind="all"):
    target_key = resolve_acc_key(acc_key)
    if target_key in _refreshing_keys:
        existing = groups_by_acc.get(target_key, [])
        if kind == "group":
            return [x for x in existing if x.get("kind") == "group"]
        if kind == "private":
            return [x for x in existing if x.get("kind") == "private"]
        return existing

    _refreshing_keys.add(target_key)
    try:
        if force_alive:
            alive = await check_acc_alive(target_key)
            if not alive:
                acc_obj = next((a for a in accounts if a.get("key") == target_key), None)
                if acc_obj and acc_obj.get("session") and not acc_obj.get("kicked"):
                    await connect_single_account(acc_obj)

        c = get_acc_client(target_key)
        if not c:
            existing = groups_by_acc.get(target_key, [])
            if kind == "group":
                return [x for x in existing if x.get("kind") == "group"]
            if kind == "private":
                return [x for x in existing if x.get("kind") == "private"]
            return existing

        dialogs = None
        for attempt in range(2):
            try:
                if hasattr(c, "get_dialogs"):
                    dialogs = await maybe_await(c.get_dialogs(limit=150), timeout=9)
                elif hasattr(c, "iter_dialogs"):
                    tmp = []
                    async for d in c.iter_dialogs(limit=150):
                        tmp.append(d)
                    dialogs = tmp
                if dialogs is not None:
                    break
            except Exception as e:
                if is_kicked_error(e):
                    mark_account_kicked(target_key, str(e)[:80])
                    return []
                if attempt == 0:
                    acc_obj = next((a for a in accounts if a.get("key") == target_key), None)
                    if acc_obj and acc_obj.get("session") and not acc_obj.get("kicked"):
                        await connect_single_account(acc_obj)
                        c = get_acc_client(target_key)

        items = []
        seen = set()
        for d in list(dialogs or []):
            entity = getattr(d, "entity", None) or d
            did = str(getattr(d, "id", None) or getattr(entity, "id", None) or "").strip()
            if not did or did in seen:
                continue
            seen.add(did)
            name = (
                getattr(d, "name", None)
                or getattr(d, "title", None)
                or getattr(entity, "title", None)
                or getattr(entity, "first_name", None)
                or did
            )
            dkind = _classify_dialog_kind(d, entity)
            items.append({"id": did, "name": str(name), "kind": dkind})

        if items:
            groups_by_acc[target_key] = items
            save_groups_cache()
            emit_soroush_event(f"لیست چت‌ها ({len(items)}) دریافت شد")
    finally:
        _refreshing_keys.discard(target_key)

    all_items = groups_by_acc.get(target_key, [])
    if kind == "group":
        return [x for x in all_items if x.get("kind") == "group"]
    if kind == "private":
        return [x for x in all_items if x.get("kind") == "private"]
    return all_items


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

    for attempt in range(2):
        try:
            c = SoroushClient(StringSession(sess)) if sess else SoroushClient(StringSession())
            t0 = time.perf_counter()
            await maybe_await(c.connect(), timeout=15)
            if sess and hasattr(c, "get_me"):
                me = await maybe_await(c.get_me(), timeout=7)
                if me is None and attempt == 0:
                    await asyncio.sleep(0.5)
                    continue
                name = getattr(me, "first_name", None) or acc_obj.get("name") or acc_obj.get("phone") or acc_key
                acc_obj["name"] = str(name)
            acc_obj["connected"] = True
            acc_obj["kicked"] = False
            clients_by_acc[acc_key] = {"client": c, "ready": True if sess else False}
            if acc_key == active_acc_key:
                state["ping"] = f"{int((time.perf_counter() - t0) * 1000)} ms"
                state["kicked_out"] = False
                state["soroush_connected"] = bool(sess)
                state["healer_status"] = "متصل به سروش (پایدار)" if sess else "در انتظار ورود"
            if sess:
                save_sessions()
                emit_soroush_event(f"سروش به پنل متصل شد ({acc_obj.get('name') or acc_key})")
                if not groups_by_acc.get(acc_key):
                    asyncio.create_task(refresh_groups(acc_key, force_alive=False))
            return True
        except Exception as e:
            if is_kicked_error(e):
                mark_account_kicked(acc_key, str(e)[:80])
                return False
            if attempt == 0:
                await asyncio.sleep(0.6)
                continue
            return False
    return False


async def ensure_accounts_connected():
    connected_any = False
    for a in list(accounts):
        if a.get("session") and not a.get("kicked") and not is_acc_ready(a["key"]):
            ok = await connect_single_account(a)
            if ok:
                connected_any = True
    if connected_any:
        emit_soroush_event("اتصال سروش برقرار شد و اطلاعات بروز شد")


async def check_acc_alive(acc_key):
    c = get_acc_client(acc_key)
    if not is_acc_ready(acc_key) or c is None:
        acc_obj = next((a for a in accounts if a.get("key") == acc_key), None)
        if acc_obj and acc_obj.get("session") and not acc_obj.get("kicked"):
            return await connect_single_account(acc_obj)
        return False
    if not hasattr(c, "get_me"):
        return True
    try:
        t0 = time.perf_counter()
        me = await maybe_await(c.get_me(), timeout=6)
        if acc_key == active_acc_key:
            state["ping"] = f"{int((time.perf_counter() - t0) * 1000)} ms"
        if me is not None:
            for a in accounts:
                if a.get("key") == acc_key:
                    a["connected"] = True
                    a["kicked"] = False
            if acc_key == active_acc_key:
                state["kicked_out"] = False
                state["soroush_connected"] = True
                state["healer_status"] = "متصل به سروش (پایدار)"
        return True
    except Exception as e:
        if is_kicked_error(e):
            mark_account_kicked(acc_key, str(e)[:80])
            return False
        acc_obj = next((a for a in accounts if a.get("key") == acc_key), None)
        if acc_obj and acc_obj.get("session") and not acc_obj.get("kicked"):
            asyncio.create_task(connect_single_account(acc_obj))
        return True


async def do_send_code(phone):
    global active_acc_key
    phone = str(phone or "").strip()
    acc_key = norm_acc_key(phone) or f"acc_{len(accounts)+1}"
    active_acc_key = acc_key
    state["phone"] = phone
    state["kicked_out"] = False
    state["auth_msg"] = "در حال ارسال کد..."

    if not any(a.get("key") == acc_key for a in accounts):
        accounts.append({
            "key": acc_key,
            "phone": phone,
            "name": phone,
            "session": "",
            "connected": False,
            "kicked": False,
        })
    else:
        for a in accounts:
            if a.get("key") == acc_key:
                a["phone"] = phone or a.get("phone") or acc_key
                a["kicked"] = False

    emit_soroush_event(f"شماره {phone} ثبت شد")

    if not SoroushClient or not StringSession:
        state["auth_msg"] = "کتابخانه splusthon روی سرور نصب نیست."
        return

    last_err = None
    for attempt in range(3):
        try:
            prev = clients_by_acc.get(acc_key, {}).get("client")
            if prev is not None and hasattr(prev, "disconnect"):
                try:
                    await maybe_await(prev.disconnect(), timeout=4)
                except Exception:
                    pass

            c = SoroushClient(StringSession())
            clients_by_acc[acc_key] = {"client": c, "ready": False}
            await maybe_await(c.connect(), timeout=16)

            res = None
            sent_ok = False
            for fn_call in (
                lambda: c.send_code_request(phone) if hasattr(c, "send_code_request") else None,
                lambda: c.send_code(phone) if hasattr(c, "send_code") else None,
                lambda: c.sign_in(phone=phone) if hasattr(c, "sign_in") else None,
            ):
                coro = fn_call()
                if coro is not None:
                    try:
                        res = await maybe_await(coro, timeout=16)
                        sent_ok = True
                        break
                    except TypeError:
                        continue

            if not sent_ok:
                raise RuntimeError("متد ارسال کد در splusthon یافت نشد.")

            code_hash = getattr(res, "phone_code_hash", None)
            if not code_hash and isinstance(res, dict):
                code_hash = res.get("phone_code_hash")

            pending_auth_by_acc[acc_key] = {
                "phone": phone,
                "phone_code_hash": code_hash,
            }
            state["auth_msg"] = f"کد برای {phone} ارسال شد؛ کد را وارد کنید."
            return
        except Exception as e:
            last_err = e
            if attempt < 2:
                await asyncio.sleep(0.8)
                continue

    state["auth_msg"] = f"خطا در ارسال کد: {last_err or 'عدم پاسخ سرور'}"


async def do_verify_code(code):
    global active_acc_key
    code = str(code or "").strip()
    acc_key = active_acc_key or norm_acc_key(state.get("phone"))
    pend = pending_auth_by_acc.get(acc_key) or {}
    phone = pend.get("phone") or state.get("phone", "").strip()
    phone_code_hash = pend.get("phone_code_hash")
    state["auth_msg"] = "در حال تأیید کد..."

    try:
        c = get_acc_client(acc_key)
        if not c:
            if SoroushClient and StringSession:
                c = SoroushClient(StringSession())
                clients_by_acc[acc_key] = {"client": c, "ready": False}
                await maybe_await(c.connect(), timeout=15)
            else:
                raise RuntimeError("کلاینت آماده نیست.")

        signed = False
        attempts = []
        if phone_code_hash:
            attempts.extend([
                lambda: c.sign_in(phone=phone, code=code, phone_code_hash=phone_code_hash),
                lambda: c.sign_in(phone, code, phone_code_hash),
            ])
        attempts.extend([
            lambda: c.sign_in(phone=phone, code=code),
            lambda: c.sign_in(phone, code),
        ])

        last_type_err = None
        for fn in attempts:
            try:
                await maybe_await(fn(), timeout=15)
                signed = True
                break
            except TypeError as te:
                last_type_err = te
                continue

        if not signed and last_type_err:
            raise last_type_err

        sess = c.session.save() if getattr(c, "session", None) else ""
        clients_by_acc[acc_key] = {"client": c, "ready": True}
        pending_auth_by_acc.pop(acc_key, None)

        name = phone
        try:
            me = await maybe_await(c.get_me(), timeout=6)
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
        state["healer_status"] = "متصل به سروش (پایدار)"
        state["auth_msg"] = f"اکانت {name} متصل شد."
        emit_soroush_event(f"اکانت {name} به سروش وصل شد")
        try:
            await refresh_groups(acc_key, force_alive=False)
        except Exception:
            pass
    except Exception as e:
        state["auth_msg"] = f"خطا در تأیید کد: {e}"


async def find_channel_in_dialogs(acc_key, raw_chat):
    s = str(raw_chat or "").strip()
    abs_id = s.lstrip("-")
    core_id = abs_id[3:] if (abs_id.startswith("100") and len(abs_id) > 5) else abs_id
    candidates = peer_candidates(s)
    matched_name = ""
    c = get_acc_client(acc_key)

    if is_acc_ready(acc_key) and c is not None and hasattr(c, "get_dialogs"):
        try:
            dlgs = await maybe_await(c.get_dialogs(limit=150), timeout=8)
            for d in list(dlgs or []):
                entity = getattr(d, "entity", None) or d
                did_str = str(getattr(d, "id", None) or getattr(entity, "id", None) or "").strip()
                did_abs = did_str.lstrip("-")
                did_core = did_abs[3:] if (did_abs.startswith("100") and len(did_abs) > 5) else did_abs
                uname = str(getattr(entity, "username", None) or "").lstrip("@").strip()
                title = str(
                    getattr(d, "name", None)
                    or getattr(d, "title", None)
                    or getattr(entity, "title", None)
                    or getattr(entity, "first_name", None)
                    or ""
                ).strip()

                if (abs_id and (abs_id == did_abs or (core_id and core_id == did_core))) or (
                    uname and s.lstrip("@").lower() == uname.lower()
                ):
                    matched_name = title
                    if did_str and did_abs.isdigit():
                        for v in peer_candidates(did_str):
                            if v not in candidates:
                                candidates.insert(0, v)
                    break
        except Exception as e:
            if is_kicked_error(e):
                mark_account_kicked(acc_key, str(e)[:80])

    return candidates, matched_name


async def fetch_single_message(acc_key, raw_chat, msg_id):
    peer_cands, channel_name = await find_channel_in_dialogs(acc_key, raw_chat)
    resolved_chat = str(peer_cands[0]) if peer_cands else clean_target(raw_chat)
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
            continue

    for peer in peer_cands:
        try:
            hist = await maybe_await(c.get_messages(peer, limit=60), timeout=7)
            if isinstance(hist, (list, tuple)):
                for item in hist:
                    if get_msg_id_val(item) == int(msg_id):
                        return item, str(peer), channel_name
        except Exception as e:
            if is_kicked_error(e):
                mark_account_kicked(acc_key, str(e)[:80])
                return None, resolved_chat, channel_name
            continue

    return None, resolved_chat, channel_name


async def read_channel_message_from_link(acc_key, link_raw):
    raw_chat, msg_id, clean_link = parse_splus_link(link_raw)
    msg_obj, resolved_chat_id, channel_name = await fetch_single_message(acc_key, raw_chat, msg_id)
    found_text = extract_msg_text(msg_obj)
    uid, uname = get_sender_info(msg_obj)
    chat_display = channel_name or resolved_chat_id
    source_label = f"پیام #{msg_id} از چت «{chat_display}» پیدا شد" if found_text else f"پیام #{msg_id} در چت «{chat_display}» (متن را تایید یا تکمیل کنید)"
    note = ""
    if not found_text:
        if is_acc_ready(acc_key):
            note = f"متن مستقیم پیام #{msg_id} خوانده نشد؛ متن را وارد کن و «تأیید پیام» بزن."
        else:
            note = "اکانت متصل نیست؛ متن را وارد کن و «تأیید پیام» بزن."

    return {
        "ok": True,
        "chat_id": resolved_chat_id,
        "channel_name": chat_display,
        "msg_id": int(msg_id),
        "user_id": str(uid or ""),
        "user_name": str(uname or ""),
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
    chat_display = channel_name or resolved_chat_id
    if not user_id and not user_name:
        return {
            "ok": False,
            "msg": f"کاربر پیام #{msg_id} در چت «{chat_display}» شناسایی نشد.",
        }
    return {
        "ok": True,
        "chat_id": resolved_chat_id,
        "channel_name": chat_display,
        "msg_id": int(msg_id),
        "user_id": str(user_id or user_name),
        "user_name": str(user_name or user_id),
        "link": clean_link,
        "msg": f"کاربر «{user_name or user_id}» فقط در چت «{chat_display}» هدف قرار گرفت.",
    }


async def send_reply_message(c, peer_or_list, text_payload, reply_to_id):
    peers = peer_or_list if isinstance(peer_or_list, (list, tuple)) else peer_candidates(peer_or_list)
    last_err = None
    for peer in peers:
        for kw_name in ("reply_to", "reply_to_msg_id"):
            try:
                await maybe_await(c.send_message(peer, text_payload, **{kw_name: int(reply_to_id)}), timeout=6)
                return None
            except TypeError as te:
                last_err = te
                continue
            except Exception as e:
                last_err = e
                if is_kicked_error(e):
                    raise
                break
        try:
            await maybe_await(c.send_message(peer, text_payload), timeout=6)
            return None
        except Exception as e2:
            last_err = e2
            if is_kicked_error(e2):
                raise
            continue
    raise last_err if last_err else RuntimeError("ریپلای ارسال نشد.")


async def execute_job_step(job, raw_target, pre_text=None):
    acc_key = job.get("acc_key") or resolve_acc_key("")
    c = get_acc_client(acc_key)
    if not is_acc_ready(acc_key) or c is None:
        acc_obj = next((a for a in accounts if a.get("key") == acc_key), None)
        if acc_obj and acc_obj.get("session") and not acc_obj.get("kicked"):
            await connect_single_account(acc_obj)
            c = get_acc_client(acc_key)
    if not is_acc_ready(acc_key) or c is None:
        raise RuntimeError("در حال اتصال مجدد به سروش...")

    mode = job.get("mode") or "spam"
    payload = str(pre_text if pre_text is not None else (job.get("payload") or ""))
    dst_cands = peer_candidates(raw_target)
    if not dst_cands:
        raise ValueError("مقصد نامعتبر است.")

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
                        for call_fn in (
                            lambda: fn(dst_p, msg_id, from_peer=src_p),
                            lambda: fn(dst_p, [msg_id], from_peer=src_p),
                        ):
                            try:
                                await maybe_await(call_fn(), timeout=8)
                                sent_ok = True
                                add_log(job, f"FWD OK -> {clean_target(raw_target)} (#{msg_id})")
                                break
                            except TypeError:
                                continue
                            except Exception as e:
                                last_err = e
                                if is_kicked_error(e):
                                    raise
                        if sent_ok:
                            break
                    if sent_ok:
                        break
                if sent_ok:
                    break

        if not sent_ok and payload.strip():
            for dst_p in dst_cands:
                try:
                    await maybe_await(c.send_message(dst_p, payload), timeout=8)
                    sent_ok = True
                    add_log(job, f"FWD/MSG OK -> {clean_target(raw_target)} (#{msg_id})")
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
            add_log(job, f"SPAM OK -> {clean_target(raw_target)}")
            return
        except Exception as e:
            last_err = e
            if is_kicked_error(e):
                raise
    raise last_err if last_err else RuntimeError("ارسال اسپم ناموفق بود.")


async def run_job_worker(job):
    while job in jobs:
        if not job.get("running"):
            await asyncio.sleep(0.2)
            continue

        acc_key = job.get("acc_key") or resolve_acc_key("")
        if not is_acc_ready(acc_key):
            acc_obj = next((a for a in accounts if a.get("key") == acc_key), None)
            if acc_obj and acc_obj.get("session") and not acc_obj.get("kicked"):
                await connect_single_account(acc_obj)
            if not is_acc_ready(acc_key):
                job["heal_state"] = "reconnecting"
                job["last_error"] = "در حال حفظ اتصال با سروش..."
                await asyncio.sleep(0.7)
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
        chosen_text = pick_next_job_text(job)
        configured_delay_ms = max(40, int(job.get("speed_ms") or 500))
        job["speed_ms"] = configured_delay_ms

        t_start = time.perf_counter()
        try:
            await execute_job_step(job, current_target, pre_text=chosen_text)
            step_ms = int((time.perf_counter() - t_start) * 1000)
            job["ok"] = int(job.get("ok") or 0) + 1
            job["t_idx"] = (idx + 1) % len(targets)
            job["sent_burst"] = int(job.get("sent_burst") or 0) + 1
            job["last_step_ms"] = step_ms
            job["effective_delay_ms"] = max(step_ms, configured_delay_ms)
            job["heal_state"] = "running"
            job["last_error"] = ""
            add_log(job, f"SPEED -> تنظیم: {configured_delay_ms}ms | اجرا: {step_ms}ms")
            if int(job.get("ok") or 0) % 10 == 0:
                save_jobs()
        except Exception as e:
            step_ms = int((time.perf_counter() - t_start) * 1000)
            err_txt = str(e)
            if is_kicked_error(err_txt):
                mark_account_kicked(acc_key, err_txt[:80])
                continue
            job["fail"] = int(job.get("fail") or 0) + 1
            job["last_step_ms"] = step_ms
            job["effective_delay_ms"] = max(step_ms, configured_delay_ms)
            job["last_error"] = err_txt[:90]
            job["t_idx"] = (idx + 1) % len(targets)
            add_log(job, f"ERR ({step_ms}ms) -> {err_txt[:75]}")

        rest_every = int(job.get("rest_every") or 0)
        rest_ms = int(job.get("rest_ms") or 0)
        if rest_every > 0 and rest_ms > 0 and int(job.get("sent_burst") or 0) >= rest_every:
            job["sent_burst"] = 0
            job["effective_delay_ms"] = max(configured_delay_ms, rest_ms)
            await asyncio.sleep(rest_ms / 1000.0)
            continue

        await asyncio.sleep(configured_delay_ms / 1000.0)


async def _poll_single_enemy(er):
    if not er.get("running"):
        return
    acc_key = er.get("acc_key") or resolve_acc_key("")
    c = get_acc_client(acc_key)
    if not is_acc_ready(acc_key) or c is None or not hasattr(c, "get_messages"):
        return

    max_c = int(er.get("max_count") or 0)
    if max_c > 0 and int(er.get("replied_count") or 0) >= max_c:
        er["running"] = False
        er["status"] = "تکمیل شد"
        save_enemies()
        return

    peers = peer_candidates(er.get("chat_id"))
    if not peers:
        return

    enemy_uid = str(er.get("user_id") or "").strip().lower()
    enemy_name = str(er.get("user_name") or "").strip().lower()
    seen_ids = set(er.get("replied_msg_ids") or [])
    last_seen_id = int(er.get("last_seen_msg_id") or 0)

    for p in peers:
        try:
            t0 = time.perf_counter()
            msgs = await maybe_await(c.get_messages(p, limit=8), timeout=3.5)
            if not isinstance(msgs, (list, tuple)):
                continue
            ordered = sorted(
                [m for m in msgs if get_msg_id_val(m) > 0],
                key=lambda x: get_msg_id_val(x),
            )
            if not ordered:
                continue

            if last_seen_id == 0:
                last_seen_id = get_msg_id_val(ordered[-1])
                er["last_seen_msg_id"] = last_seen_id
                continue

            new_msgs = [m for m in ordered if get_msg_id_val(m) > last_seen_id and get_msg_id_val(m) not in seen_ids]
            if not new_msgs:
                continue

            er["last_seen_msg_id"] = max(last_seen_id, get_msg_id_val(new_msgs[-1]))

            for m in new_msgs:
                mid = get_msg_id_val(m)
                uid, uname = get_sender_info(m)
                u_match = (enemy_uid and enemy_uid == str(uid).lower()) or (
                    enemy_name and enemy_name in f"{uid} {uname}".lower()
                )
                if u_match:
                    next_reply = pick_next_enemy_text(er)
                    await send_reply_message(c, [p], next_reply, mid)
                    step_ms = int((time.perf_counter() - t0) * 1000)
                    seen_ids.add(mid)
                    er["replied_msg_ids"] = list(seen_ids)[-160:]
                    er["replied_count"] = int(er.get("replied_count") or 0) + 1
                    er["last_step_ms"] = step_ms
                    er["last_reply_at"] = now_str()
                    er["status"] = f"ریپلای سریع روی پیام #{mid} ({step_ms}ms)"
                    if max_c > 0 and int(er.get("replied_count") or 0) >= max_c:
                        er["running"] = False
                        er["status"] = "تکمیل شد"
                    save_enemies()
                    emit_soroush_event(f"ریپلای سریع روی پیام #{mid} ارسال شد")
                    if not er.get("running"):
                        break
            break
        except Exception as e:
            if is_kicked_error(e):
                mark_account_kicked(acc_key, str(e)[:80])
                break


async def poll_enemy_rules():
    active_rules = [er for er in list(enemy_rules) if er.get("running")]
    if not active_rules:
        return
    await asyncio.gather(*[_poll_single_enemy(er) for er in active_rules], return_exceptions=True)


async def enemy_fast_loop():
    while True:
        try:
            await poll_enemy_rules()
        except Exception:
            pass
        await asyncio.sleep(0.18)


async def watcher_loop():
    tick = 0
    await ensure_accounts_connected()
    asyncio.create_task(enemy_fast_loop())

    while True:
        await asyncio.sleep(0.45)
        tick += 1
        if tick % 7 == 0:
            await ensure_accounts_connected()
        if tick % 16 == 0:
            for a in list(accounts):
                await check_acc_alive(a["key"])
        if tick % 80 == 0:
            save_sessions()
            save_jobs()
            save_enemies()
            if groups_by_acc:
                save_groups_cache()


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


@app.route("/api/hello")
def api_hello():
    cur_key = resolve_acc_key(request.args.get("acc_key") or "")
    if cur_key and not is_acc_ready(cur_key):
        schedule_bg(ensure_accounts_connected())
    if cur_key and not groups_by_acc.get(cur_key):
        schedule_bg(refresh_groups(cur_key, force_alive=True))
    return jsonify({
        "ok": True,
        "returning": True,
        "active_acc_key": cur_key,
        "event_seq": event_seq,
        "event_notice": state["event_notice"],
    })


@app.route("/api/soroush_ping")
def api_soroush_ping():
    cur_key = resolve_acc_key(request.args.get("acc_key") or "")
    if cur_key and not is_acc_ready(cur_key):
        fut = run_async(ensure_accounts_connected())
        if fut:
            try:
                fut.result(timeout=7)
            except Exception:
                pass
    return jsonify({
        "ok": True,
        "soroush_connected": is_acc_ready(cur_key) if cur_key else False,
        "event_seq": event_seq,
        "event_notice": state["event_notice"] or "سروش وصل است",
    })


@app.route("/api/send_phone")
def api_send_phone():
    phone = (request.args.get("phone") or "").strip()
    fut = run_async(do_send_code(phone))
    if fut:
        try:
            fut.result(timeout=25)
        except Exception:
            pass
    return jsonify({"ok": True, "auth_msg": state["auth_msg"], "active_acc_key": active_acc_key, "event_seq": event_seq})


@app.route("/api/verify_code")
def api_verify_code():
    code = (request.args.get("code") or "").strip()
    fut = run_async(do_verify_code(code))
    if fut:
        try:
            fut.result(timeout=20)
        except Exception:
            pass
    return jsonify({
        "ok": True,
        "auth_msg": state["auth_msg"],
        "active_acc_key": active_acc_key,
        "event_seq": event_seq,
        "event_notice": state["event_notice"],
    })


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
            emit_soroush_event(f"نمایش اکانت تغییر کرد به {a.get('name') or a.get('phone')}")
            save_sessions()
            if not groups_by_acc.get(key):
                fut = run_async(refresh_groups(key))
                if fut:
                    try:
                        fut.result(timeout=6)
                    except Exception:
                        pass
            return jsonify({
                "ok": True,
                "active_acc_key": active_acc_key,
                "groups": groups_by_acc.get(key, []),
                "event_seq": event_seq,
            })
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
        save_groups_cache()
        jobs[:] = [j for j in jobs if j.get("acc_key") != rkey]
        enemy_rules[:] = [e for e in enemy_rules if e.get("acc_key") != rkey]
        save_jobs()
        save_enemies()
    active_acc_key = accounts[0]["key"] if accounts else ""
    save_sessions()
    state["soroush_connected"] = is_acc_ready(active_acc_key) if active_acc_key else False
    state["kicked_out"] = False
    state["healer_status"] = "حذف‌شده"
    emit_soroush_event("لیست اکانت‌ها بروز شد")
    return jsonify({"ok": True, "active_acc_key": active_acc_key, "event_seq": event_seq})


@app.route("/api/groups")
def api_groups():
    acc_key = resolve_acc_key(request.args.get("acc_key") or "")
    kind = (request.args.get("kind") or "all").strip().lower()
    if kind not in ("all", "group", "private"):
        kind = "all"
    fut = run_async(refresh_groups(acc_key, kind=kind))
    if fut:
        try:
            res = fut.result(timeout=8)
            if isinstance(res, list) and res:
                return jsonify(res)
        except Exception:
            pass
    cached = groups_by_acc.get(acc_key, [])
    if kind == "group":
        return jsonify([x for x in cached if x.get("kind") == "group"])
    if kind == "private":
        return jsonify([x for x in cached if x.get("kind") == "private"])
    return jsonify(cached)


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
        "channel_name": raw_chat,
        "msg_id": msg_id,
        "link": clean_link,
        "source_label": f"پیام #{msg_id} از چت {raw_chat}",
        "text": "",
        "note": "متن از سرور برنگشت؛ متن را وارد کن و «تأیید پیام» بزن.",
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


@app.route("/api/add_enemy")
def api_add_enemy():
    global enemy_seq
    acc_key = resolve_acc_key(request.args.get("acc_key") or "")
    if not is_acc_ready(acc_key):
        acc_obj = next((a for a in accounts if a.get("key") == acc_key), None)
        if acc_obj and acc_obj.get("session") and not acc_obj.get("kicked"):
            fut = run_async(connect_single_account(acc_obj))
            if fut:
                try:
                    fut.result(timeout=8)
                except Exception:
                    pass
    if not is_acc_ready(acc_key):
        return jsonify({"ok": False, "msg": "اکانت انتخاب‌شده متصل نیست."})

    chat_id = clean_target(request.args.get("chat_id") or "")
    user_id = (request.args.get("user_id") or "").strip()
    user_name = (request.args.get("user_name") or user_id).strip()
    reply_texts = parse_multi_texts(request.args.get("reply_texts") or request.args.get("reply_text") or "")
    max_count = max(0, int(request.args.get("max_count") or 0))
    speed_ms = max(20, int(request.args.get("speed_ms") or 40))

    if not chat_id or not user_id:
        return jsonify({"ok": False, "msg": "ابتدا با لینک پیام، کاربر را در چت شناسایی کنید."})
    if not reply_texts:
        return jsonify({"ok": False, "msg": "حداقل یک متن ریپلای دشمن وارد کنید."})

    enemy_seq += 1
    er = {
        "id": str(enemy_seq),
        "acc_key": acc_key,
        "acc_label": get_acc_label(acc_key),
        "chat_id": chat_id,
        "user_id": user_id,
        "user_name": user_name,
        "reply_texts": reply_texts,
        "reply_text": reply_texts[0],
        "reply_idx": 0,
        "speed_ms": speed_ms,
        "last_step_ms": 0,
        "max_count": max_count,
        "replied_count": 0,
        "replied_msg_ids": [],
        "last_seen_msg_id": 0,
        "last_reply_at": "-",
        "last_error": "",
        "status": "آماده ریپلای سریع در همین چت",
        "running": True,
    }
    enemy_rules.append(er)
    save_enemies()
    emit_soroush_event(f"دشمن «{user_name}» در چت {chat_id} فعال شد")
    return jsonify({
        "ok": True,
        "msg": f"دشمن «{user_name}» فقط در چت {chat_id} با {len(reply_texts)} متن فعال شد."
    })


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
                er["last_seen_msg_id"] = 0
                er["status"] = "آماده ریپلای سریع در همین چت" if er["running"] else "متوقف"
            elif act == "delete":
                enemy_rules.remove(er)
            break
    save_enemies()
    emit_soroush_event("وضعیت لیست دشمن‌ها بروز شد")
    return jsonify({"ok": True})


@app.route("/api/add_job")
def api_add_job():
    global job_seq
    acc_key = resolve_acc_key(request.args.get("acc_key") or "")
    if not is_acc_ready(acc_key):
        acc_obj = next((a for a in accounts if a.get("key") == acc_key), None)
        if acc_obj and acc_obj.get("session") and not acc_obj.get("kicked"):
            fut = run_async(connect_single_account(acc_obj))
            if fut:
                try:
                    fut.result(timeout=8)
                except Exception:
                    pass
    if not is_acc_ready(acc_key):
        return jsonify({"ok": False, "msg": "اکانت انتخاب‌شده متصل نیست؛ کمی صبر کن و دوباره بزن."})

    mode = (request.args.get("mode") or "spam").strip().lower()
    if mode not in ("spam", "forward"):
        mode = "spam"

    texts = parse_multi_texts(request.args.get("texts") or request.args.get("payload") or "")
    payload = texts[0] if texts else (request.args.get("payload") or "")
    raw_link = (request.args.get("link") or "").strip()
    raw_targets = (request.args.get("targets") or "").strip()

    parsed_chat = ""
    parsed_msg_id = 0
    if mode == "forward" and raw_link:
        parsed_chat, parsed_msg_id, _ = parse_splus_link(raw_link)

    targets = [clean_target(x) for x in re.split(r"[,\n]+", raw_targets) if clean_target(x)][:MAX_TARGETS_PER_JOB]
    if not targets:
        return jsonify({"ok": False, "msg": "مقصد معتبر وارد کنید."})

    msg_id = int(request.args.get("msg_id") or parsed_msg_id or 0)
    src_chat = clean_target(request.args.get("source_chat") or parsed_chat or "")

    if mode == "forward":
        if not payload.strip() and msg_id <= 0:
            return jsonify({"ok": False, "msg": "ابتدا لینک پیام را خوانده و تأیید کنید."})
    else:
        if not payload.strip():
            return jsonify({"ok": False, "msg": "متن پیام خالی است."})

    active_count = sum(1 for j in jobs if j.get("running"))
    if active_count >= MAX_ACTIVE_JOBS:
        return jsonify({"ok": False, "msg": f"فقط {MAX_ACTIVE_JOBS} تسک هم‌زمان قابل اجراست."})

    job_seq += 1
    acc_label = get_acc_label(acc_key)
    speed_ms = max(40, int(request.args.get("speed_ms") or 500))
    job = {
        "id": str(job_seq),
        "acc_key": acc_key,
        "acc_label": acc_label,
        "mode": mode,
        "source_chat": src_chat,
        "msg_id": msg_id,
        "link": raw_link,
        "texts": texts if texts else ([payload] if payload else []),
        "payload": payload,
        "text_idx": 0,
        "targets": targets,
        "t_idx": 0,
        "sent_burst": 0,
        "speed_ms": speed_ms,
        "last_step_ms": 0,
        "effective_delay_ms": speed_ms,
        "max_count": max(0, int(request.args.get("max_count") or 0)),
        "rest_every": max(0, int(request.args.get("rest_every") or 0)),
        "rest_ms": max(0, int(request.args.get("rest_ms") or 0)),
        "running": True,
        "heal_state": "running",
        "last_error": "",
        "ok": 0,
        "fail": 0,
        "logs": [f"[{now_str()}] started ({mode}) on {acc_label} | speed={speed_ms}ms"],
    }
    jobs.append(job)
    save_jobs()
    emit_soroush_event(f"تسک #{job['id']} شروع شد")
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
    emit_soroush_event("لیست تسک‌ها بروز شد")
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

    if cur_key and not cur_ready and not cur_kicked:
        schedule_bg(ensure_accounts_connected())
    if cur_key and not cur_groups and cur_ready:
        schedule_bg(refresh_groups(cur_key, force_alive=False))

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
        "event_seq": event_seq,
        "event_notice": state["event_notice"],
        "active_acc_key": cur_key,
        "accounts": acc_list_out,
        "groups": cur_groups,
        "jobs": jobs,
        "enemy_rules": enemy_rules,
    })


accounts[:] = load_sessions()
groups_by_acc.update(load_groups_cache())
loaded_jobs = load_jobs()
jobs[:] = loaded_jobs if isinstance(loaded_jobs, list) else []
loaded_enemies = load_enemies()
enemy_rules[:] = loaded_enemies if isinstance(loaded_enemies, list) else []

Thread(target=start_bot_thread, daemon=True).start()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT, threaded=True)
