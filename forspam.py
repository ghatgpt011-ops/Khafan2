import asyncio
from collections import deque
from datetime import datetime
import io
import json
import logging
import os
import re
import sys
from threading import Thread
import time
from urllib.parse import urlparse

from flask import Flask, request
from splusthon import SoroushClient
from splusthon.sessions import StringSession

# ================= تنظیمات اصلی =================
MAX_CONCURRENT_JOBS = 3
PORT = int(os.environ.get("PORT", 8080))
PANEL_PATH = os.environ.get("PANEL_PATH", "/panel")
if not PANEL_PATH.startswith("/"):
    PANEL_PATH = "/" + PANEL_PATH

STATE_FILE = "soroush_panel_state.json"

for _name in ("splusthon", "asyncio", "werkzeug"):
    lg = logging.getLogger(_name)
    lg.setLevel(logging.CRITICAL)
    lg.propagate = False

app = Flask(__name__)

# ================= وضعیت مشترک سرور =================
io_lock = asyncio.Lock()
connect_lock = asyncio.Lock()

bot_loop = None
client = None

account_state = {
    "phone": "",
    "name": "",
    "session": "",
    "connected": False,
    "ping": "-",
    "status": "در انتظار وارد کردن شماره",
}

login_state = {
    "phone": "",
    "code_hash": None,
    "step": "idle",  # idle | code_sent | connected
    "message": "شماره تلفن را وارد کنید (مثال: +98912...)",
}

dialogs_cache = {
    "groups": [],
    "privates": [],
}

jobs = []  # حداکثر 3 تسک فعال هم‌زمان (spam و forward)
job_counter = 0
last_keepalive_ts = 0.0
terminal_logs = deque(maxlen=250)


def now_str() -> str:
    return datetime.now().strftime("%H:%M:%S")


def push_log(level: str, msg: str):
    line = f"[{now_str()}] [{level}] {msg}"
    terminal_logs.append(line)
    try:
        sys.__stdout__.write(line + "\n")
        sys.__stdout__.flush()
    except Exception:
        pass


class _TeeStream(io.TextIOBase):
    def __init__(self, orig_stream, tag: str = "PROC"):
        self.orig = orig_stream
        self.tag = tag
        self._buf = ""

    def write(self, s):
        if not s:
            return 0
        text = str(s)
        if self.orig:
            try:
                self.orig.write(text)
                self.orig.flush()
            except Exception:
                pass
        self._buf += text
        while "\n" in self._buf:
            line, self._buf = self._buf.split("\n", 1)
            clean = line.strip()
            if clean and not clean.startswith(" * Serving Flask app") and "werkzeug" not in clean.lower():
                terminal_logs.append(f"[{now_str()}] [{self.tag}] {clean}")
        return len(text)

    def flush(self):
        if self.orig:
            try:
                self.orig.flush()
            except Exception:
                pass


def install_terminal_capture():
    if not isinstance(sys.stdout, _TeeStream):
        sys.stdout = _TeeStream(sys.__stdout__, "OUT")
    if not isinstance(sys.stderr, _TeeStream):
        sys.stderr = _TeeStream(sys.__stderr__, "ERR")


install_terminal_capture()


def is_auth_error(err) -> bool:
    s = str(err or "").lower()
    markers = (
        "authkeyunregistered",
        "auth_key_unregistered",
        "key is not registered",
        "session_revoked",
        "auth_key_duplicated",
        "user_deactivated",
        "unauthorized",
        "logged out",
        "log out",
        "invalid session",
        "401",
    )
    return any(m in s for m in markers)


def classify_error(err) -> tuple[str, str]:
    """خطایاب هوشمند: تشخیص علت خطا و تعیین اقدام اصلاحی"""
    raw = str(err or "خطای ناشناخته").strip()
    low = raw.lower()
    if is_auth_error(raw):
        return (
            "SESSION_REVOKED",
            "اکانت از نشست خارج شده یا سشن باطل شده؛ نیاز به ورود مجدد با کد",
        )
    if "timeout" in low or "timed out" in low:
        return (
            "TIMEOUT",
            "پاسخ سرور زمان‌بر شد؛ بازسازی اتصال و ادامه همان دستور",
        )
    if "flood" in low or "wait" in low or "limit" in low or "429" in low:
        return (
            "RATE_LIMIT",
            "محدودیت موقت ارسال؛ مکث کوتاه و ادامه همان دستور",
        )
    if "network" in low or "connection" in low or "disconnect" in low or "socket" in low:
        return (
            "NETWORK",
            "قطع کوتاه ارتباط شبکه؛ برقراری مجدد اتصال و ازسرگیری همان دستور",
        )
    if "peer" in low or "chat" in low or "user" in low or "not found" in low or "400" in low:
        return (
            "TARGET_OR_PARAM",
            "مقصد/شناسه نامعتبر یا عدم دسترسی؛ بررسی شناسه و تلاش مجدد روی همان دستور",
        )
    return (
        "SERVER_ERROR",
        "خطای سمت سرور؛ اصلاح وضعیت اتصال و بازگشت به همان دستور",
    )


async def handle_session_revoked(reason: str):
    global client
    account_state["connected"] = False
    account_state["session"] = ""
    account_state["ping"] = "خارج‌شده"
    account_state["status"] = "اکانت از نشست خارج شد؛ شماره و کد جدید وارد کنید."
    login_state["step"] = "idle"
    login_state["code_hash"] = None
    login_state["message"] = "نشست قبلی بسته شد. برای اتصال مجدد شماره را وارد کنید."
    await close_client_quietly(client)
    client = None
    save_state()
    push_log("AUTH_OUT", f"تشخیص خروج از اکانت: {reason[:100]}")


def save_state():
    payload = {
        "account": {
            "phone": account_state.get("phone", ""),
            "name": account_state.get("name", ""),
            "session": account_state.get("session", ""),
        },
        "dialogs": dialogs_cache,
        "jobs": [
            {
                "id": j["id"],
                "kind": j["kind"],
                "source": j.get("source", ""),
                "source_name": j.get("source_name", ""),
                "target": j.get("target", ""),
                "target_name": j.get("target_name", ""),
                "msg_id": j.get("msg_id", ""),
                "msg_preview": j.get("msg_preview", ""),
                "texts": j.get("texts", []),
                "text_idx": int(j.get("text_idx", 0)),
                "speed": float(j.get("speed", 1.0)),
                "max_count": int(j.get("max_count", 0)),
                "sent": int(j.get("sent", 0)),
                "running": bool(j.get("running", False)),
                "last_time": j.get("last_time", "-"),
                "last_err": j.get("last_err", ""),
                "current_cmd": j.get("current_cmd", ""),
                "status_text": j.get("status_text", "آماده"),
            }
            for j in jobs
        ],
    }
    try:
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
    except Exception as e:
        push_log("ERR", f"خطا در ذخیره وضعیت: {e}")


def load_state():
    global job_counter
    if not os.path.exists(STATE_FILE):
        push_log("SYS", "فایل وضعیت یافت نشد؛ شروع با وضعیت اولیه.")
        return
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        push_log("ERR", f"خواندن فایل وضعیت ناموفق بود: {e}")
        return

    acc = data.get("account") or {}
    account_state["phone"] = str(acc.get("phone") or "")
    account_state["name"] = str(acc.get("name") or "")
    account_state["session"] = str(acc.get("session") or "")
    if account_state["session"]:
        account_state["status"] = "سشن بارگذاری شد؛ در حال اتصال..."
        login_state["phone"] = account_state["phone"]
        login_state["step"] = "connected"
        login_state["message"] = f"سشن ذخیره‌شده برای {account_state['phone']} موجود است."

    dl = data.get("dialogs") or {}
    dialogs_cache["groups"] = list(dl.get("groups") or [])
    dialogs_cache["privates"] = list(dl.get("privates") or [])

    loaded_jobs = data.get("jobs") or []
    running_seen = 0
    for item in loaded_jobs:
        is_run = bool(item.get("running", False))
        if is_run:
            running_seen += 1
            if running_seen > MAX_CONCURRENT_JOBS:
                is_run = False

        job = {
            "id": str(item.get("id") or f"job_{int(time.time()*1000)}"),
            "kind": item.get("kind") or "spam",
            "source": str(item.get("source") or ""),
            "source_name": str(item.get("source_name") or item.get("source") or ""),
            "target": str(item.get("target") or ""),
            "target_name": str(item.get("target_name") or item.get("target") or ""),
            "msg_id": str(item.get("msg_id") or ""),
            "msg_preview": str(item.get("msg_preview") or ""),
            "texts": list(item.get("texts") or []),
            "text_idx": int(item.get("text_idx") or 0),
            "speed": max(0.05, float(item.get("speed") or 1.0)),
            "max_count": max(0, int(item.get("max_count") or 0)),
            "sent": max(0, int(item.get("sent") or 0)),
            "running": is_run,
            "last_time": str(item.get("last_time") or "-"),
            "last_err": "",
            "current_cmd": str(item.get("current_cmd") or ""),
            "status_text": "بازیابی خودکار پس از راه‌اندازی" if is_run else "متوقف",
        }
        jobs.append(job)

    job_counter = len(jobs)
    push_log("SYS", f"وضعیت بازیابی شد ({len(jobs)} مأموریت، {running_seen} فعال).")


def active_running_count(exclude_id: str = "") -> int:
    return sum(1 for j in jobs if j.get("running") and j.get("id") != exclude_id)


def parse_peer_id(raw: str) -> str:
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


def to_peer_arg(raw: str):
    p = parse_peer_id(raw)
    return int(p) if p.lstrip("-").isdigit() else p


def find_dialog_name(peer_str: str) -> str:
    p = parse_peer_id(peer_str)
    for bucket in ("groups", "privates"):
        for item in dialogs_cache.get(bucket, []):
            if str(item.get("id")) == p or str(item.get("name")) == p:
                return str(item.get("name") or p)
    return p


def extract_user_display(me, fallback: str) -> str:
    if not me:
        return fallback
    fn = (getattr(me, "first_name", None) or "").strip()
    ln = (getattr(me, "last_name", None) or "").strip()
    un = (getattr(me, "username", None) or "").strip()
    full = f"{fn} {ln}".strip()
    if full:
        return full
    if un:
        return f"@{un}"
    uid = getattr(me, "id", None)
    return str(uid) if uid is not None else fallback


def classify_dialog(d):
    ent = getattr(d, "entity", None)
    did = getattr(d, "id", None)
    if did is None:
        did = getattr(ent, "id", None)
    if did is None:
        return None

    name = (
        getattr(d, "name", None)
        or getattr(d, "title", None)
        or getattr(ent, "title", None)
        or getattr(ent, "name", None)
        or getattr(ent, "first_name", None)
        or str(did)
    )

    is_channel = bool(getattr(d, "is_channel", False) or getattr(ent, "broadcast", False))
    is_group = bool(
        getattr(d, "is_group", False)
        or getattr(ent, "megagroup", False)
        or getattr(ent, "gigagroup", False)
    )

    if is_channel:
        dtype = "کانال"
        bucket = "groups"
    elif is_group:
        dtype = "گروه"
        bucket = "groups"
    else:
        dtype = "شخصی"
        bucket = "privates"

    return {"id": str(did), "name": str(name), "type": dtype, "bucket": bucket}


def extract_msg_text(m) -> str:
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


def extract_msg_id(m, idx: int) -> int:
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


# ================= مدیریت اتصال کلاینت سروش =================
async def close_client_quietly(c):
    if c is None:
        return
    for attr in ("_client", "client", "_session_http", "http_session", "_sender", "_updates"):
        sub = getattr(c, attr, None)
        if sub is not None and hasattr(sub, "close"):
            try:
                res = sub.close()
                if asyncio.iscoroutine(res):
                    await res
            except Exception:
                pass
    if hasattr(c, "disconnect"):
        try:
            res = c.disconnect()
            if asyncio.iscoroutine(res):
                await res
        except Exception:
            pass


async def ensure_connected(force: bool = False) -> bool:
    global client
    sess = (account_state.get("session") or "").strip()
    if not sess:
        account_state["connected"] = False
        if not account_state.get("status"):
            account_state["status"] = "سشن ثبت نشده؛ شماره و کد را وارد کنید."
        return False

    if login_state.get("step") == "code_sent":
        return False

    async with connect_lock:
        if not force and account_state.get("connected") and client is not None:
            try:
                t0 = time.perf_counter()
                me = await asyncio.wait_for(client.get_me(), timeout=7)
                if me is None:
                    raise RuntimeError("Session logged out (get_me returned None)")
                ms = int((time.perf_counter() - t0) * 1000)
                account_state["name"] = extract_user_display(me, account_state.get("phone") or "کاربر")
                account_state["ping"] = f"{ms} ms"
                account_state["status"] = f"آنلاین ({account_state['name']})"
                return True
            except Exception as e:
                if is_auth_error(e) or "returned none" in str(e).lower():
                    await handle_session_revoked(str(e))
                    return False

        try:
            await close_client_quietly(client)
            new_c = SoroushClient(StringSession(sess))
            t0 = time.perf_counter()
            await asyncio.wait_for(new_c.connect(), timeout=18)
            me = await asyncio.wait_for(new_c.get_me(), timeout=12)
            if me is None:
                raise RuntimeError("Session logged out (get_me returned None)")
            ms = int((time.perf_counter() - t0) * 1000)
            client = new_c
            account_state["connected"] = True
            account_state["name"] = extract_user_display(me, account_state.get("phone") or "کاربر")
            account_state["ping"] = f"{ms} ms"
            account_state["status"] = f"آنلاین ({account_state['name']})"
            save_state()
            return True
        except Exception as e:
            if is_auth_error(e) or "returned none" in str(e).lower():
                await handle_session_revoked(str(e))
                return False
            err_code, hint = classify_error(e)
            account_state["connected"] = False
            account_state["ping"] = err_code
            account_state["status"] = f"[{err_code}] {hint}"
            push_log("FIX", f"اتصال مجدد: [{err_code}] {hint} | {str(e)[:75]}")
            return False


async def do_send_code(phone: str):
    global client
    login_state["phone"] = phone
    login_state["code_hash"] = None
    login_state["step"] = "code_sent"
    login_state["message"] = f"در حال ارسال کد به {phone}..."
    push_log("AUTH", f"درخواست ارسال کد برای {phone}")

    async with connect_lock:
        try:
            await close_client_quietly(client)
            c = SoroushClient(StringSession())
            client = c
            await asyncio.wait_for(c.connect(), timeout=20)

            if hasattr(c, "send_code_request"):
                sent = await asyncio.wait_for(c.send_code_request(phone), timeout=20)
            elif hasattr(c, "send_code"):
                sent = await asyncio.wait_for(c.send_code(phone), timeout=20)
            else:
                sent = await asyncio.wait_for(c.sign_in(phone=phone), timeout=20)

            code_hash = None
            if sent is not None:
                code_hash = getattr(sent, "phone_code_hash", None) or (
                    sent.get("phone_code_hash") if isinstance(sent, dict) else None
                )
            login_state["code_hash"] = str(code_hash) if code_hash else None
            login_state["step"] = "code_sent"
            login_state["message"] = f"کد تأیید برای {phone} ارسال شد."
            push_log("AUTH", f"کد برای {phone} ارسال شد.")
        except Exception as e:
            err_code, hint = classify_error(e)
            login_state["step"] = "code_sent"
            login_state["message"] = f"خطا در ارسال کد [{err_code}]: {e}"
            push_log("ERR", f"ارسال کد [{err_code}] ({hint}): {e}")


async def do_verify_code(code: str):
    global client
    phone = (login_state.get("phone") or account_state.get("phone") or "").strip()
    if not phone:
        login_state["message"] = "ابتدا شماره تلفن را وارد کنید."
        return

    login_state["message"] = "در حال بررسی کد..."
    async with connect_lock:
        try:
            c = client
            if not c or not hasattr(c, "sign_in"):
                raise RuntimeError("کلاینت آماده نیست.")

            signed = False
            last_err = None
            ch = login_state.get("code_hash")
            if ch:
                for fn in (
                    lambda: c.sign_in(phone=phone, code=code, phone_code_hash=ch),
                    lambda: c.sign_in(phone, code, ch),
                ):
                    try:
                        await asyncio.wait_for(fn(), timeout=20)
                        signed = True
                        break
                    except TypeError:
                        continue
                    except Exception as ex:
                        last_err = ex

            if not signed:
                await asyncio.wait_for(c.sign_in(phone=phone, code=code), timeout=20)
                signed = True

            if not signed:
                raise RuntimeError(str(last_err or "ورود انجام نشد."))

            sess = c.session.save() if getattr(c, "session", None) else ""
            if not sess:
                raise RuntimeError("سشن ذخیره نشد.")

            me = None
            try:
                me = await asyncio.wait_for(c.get_me(), timeout=10)
            except Exception:
                pass

            account_state["phone"] = phone
            account_state["session"] = sess
            account_state["name"] = extract_user_display(me, phone)
            account_state["connected"] = True
            account_state["ping"] = "آنلاین"
            account_state["status"] = f"متصل ({account_state['name']})"

            login_state["step"] = "connected"
            login_state["code_hash"] = None
            login_state["message"] = f"اکانت {account_state['name']} متصل و ذخیره شد."

            save_state()
            push_log("AUTH", f"اکانت {account_state['name']} ({phone}) با موفقیت متصل شد.")
        except Exception as e:
            err_code, hint = classify_error(e)
            login_state["step"] = "code_sent"
            login_state["message"] = f"خطا در تأیید کد [{err_code}]: {e}"
            push_log("ERR", f"تأیید کد [{err_code}] ({hint}): {e}")

    if account_state.get("connected"):
        await do_fetch_dialogs("all")


async def do_fetch_dialogs(mode: str = "all"):
    ok = await ensure_connected(force=False)
    if not ok or client is None:
        return {
            "ok": False,
            "error": account_state.get("status") or "اتصال برقرار نیست.",
            "groups": dialogs_cache["groups"],
            "privates": dialogs_cache["privates"],
        }

    async with connect_lock:
        try:
            raw = []
            if hasattr(client, "get_dialogs"):
                res = await asyncio.wait_for(client.get_dialogs(limit=250), timeout=16)
                raw = list(res) if res else []
            elif hasattr(client, "iter_dialogs"):
                async for d in client.iter_dialogs(limit=250):
                    raw.append(d)

            grps = []
            privs = []
            seen_g = set()
            seen_p = set()

            for d in raw:
                info = classify_dialog(d)
                if not info:
                    continue
                entry = {"id": info["id"], "name": info["name"], "type": info["type"]}
                if info["bucket"] == "groups":
                    if entry["id"] not in seen_g:
                        seen_g.add(entry["id"])
                        grps.append(entry)
                else:
                    if entry["id"] not in seen_p:
                        seen_p.add(entry["id"])
                        privs.append(entry)

            if grps:
                dialogs_cache["groups"] = grps
            if privs:
                dialogs_cache["privates"] = privs

            for j in jobs:
                if j.get("source"):
                    j["source_name"] = find_dialog_name(j["source"])
                if j.get("target"):
                    j["target_name"] = find_dialog_name(j["target"])

            save_state()
            if mode == "groups":
                push_log("INFO", f"دریافت گروه/کانال انجام شد ({len(dialogs_cache['groups'])} مورد).")
            elif mode == "privates":
                push_log("INFO", f"دریافت چت شخصی انجام شد ({len(dialogs_cache['privates'])} مورد).")
            else:
                push_log("INFO", f"لیست چت‌ها بروزرسانی شد ({len(grps)} گروه/کانال، {len(privs)} شخصی).")

            return {
                "ok": True,
                "error": "",
                "groups": dialogs_cache["groups"],
                "privates": dialogs_cache["privates"],
            }
        except Exception as e:
            if is_auth_error(e):
                await handle_session_revoked(str(e))
                return {
                    "ok": False,
                    "error": account_state["status"],
                    "groups": dialogs_cache["groups"],
                    "privates": dialogs_cache["privates"],
                }
            err_code, hint = classify_error(e)
            push_log("ERR", f"دریافت چت‌ها [{err_code}] ({hint}): {e}")
            return {
                "ok": False,
                "error": f"[{err_code}] {e}",
                "groups": dialogs_cache["groups"],
                "privates": dialogs_cache["privates"],
            }


async def do_fetch_last5(source_raw: str):
    ok = await ensure_connected(force=False)
    if not ok or client is None:
        return {"ok": False, "error": account_state.get("status") or "اتصال برقرار نیست.", "messages": []}

    peer = to_peer_arg(source_raw)
    if not peer:
        return {"ok": False, "error": "مبدأ معتبر نیست.", "messages": []}

    async with connect_lock:
        try:
            raw_msgs = []
            if hasattr(client, "get_messages"):
                res = await asyncio.wait_for(client.get_messages(peer, limit=5), timeout=15)
                raw_msgs = list(res) if res else []
            elif hasattr(client, "iter_messages"):
                async for m in client.iter_messages(peer, limit=5):
                    raw_msgs.append(m)
            else:
                raise RuntimeError("متد خواندن پیام در کلاینت یافت نشد.")

            out = []
            for idx, m in enumerate(raw_msgs[:5], start=1):
                mid = extract_msg_id(m, idx)
                full = extract_msg_text(m) or "[پیام مدیا / بدون متن]"
                single_line = " ".join(full.splitlines()).strip()
                preview = (single_line[:70] + "...") if len(single_line) > 70 else single_line
                out.append({"index": idx, "id": mid, "preview": preview, "text": full})

            push_log("INFO", f"۵ پیام اخیر از مبدأ {parse_peer_id(source_raw)} دریافت شد.")
            return {"ok": True, "error": "", "messages": out}
        except Exception as e:
            if is_auth_error(e):
                await handle_session_revoked(str(e))
                return {"ok": False, "error": account_state["status"], "messages": []}
            err_code, hint = classify_error(e)
            push_log("ERR", f"دریافت ۵ پیام اخیر [{err_code}] ({hint}): {e}")
            return {"ok": False, "error": f"[{err_code}] {e}", "messages": []}


# ================= اجرای دستورات اسپم و فوروارد =================
async def perform_spam_send(tgt_peer, text: str):
    if client is None:
        raise RuntimeError("کلاینت متصل نیست.")
    async with io_lock:
        return await asyncio.wait_for(client.send_message(tgt_peer, text), timeout=14)


async def perform_forward_send(src_peer, tgt_peer, msg_id_str: str, fallback_text: str):
    if client is None:
        raise RuntimeError("کلاینت متصل نیست.")
    msg_id_val = int(msg_id_str) if str(msg_id_str).isdigit() else msg_id_str
    async with io_lock:
        if hasattr(client, "forward_messages"):
            try:
                return await asyncio.wait_for(
                    client.forward_messages(tgt_peer, msg_id_val, from_peer=src_peer),
                    timeout=15,
                )
            except TypeError:
                try:
                    return await asyncio.wait_for(
                        client.forward_messages(tgt_peer, msg_id_val, src_peer),
                        timeout=15,
                    )
                except Exception as e:
                    if is_auth_error(e):
                        raise
            except Exception as e:
                if is_auth_error(e):
                    raise
        elif hasattr(client, "forward_message"):
            try:
                return await asyncio.wait_for(
                    client.forward_message(tgt_peer, msg_id_val, from_peer=src_peer),
                    timeout=15,
                )
            except TypeError:
                try:
                    return await asyncio.wait_for(
                        client.forward_message(tgt_peer, msg_id_val, src_peer),
                        timeout=15,
                    )
                except Exception as e:
                    if is_auth_error(e):
                        raise
            except Exception as e:
                if is_auth_error(e):
                    raise

        txt = (fallback_text or "").strip()
        if not txt and hasattr(client, "get_messages"):
            try:
                fetched = await asyncio.wait_for(client.get_messages(src_peer, ids=msg_id_val), timeout=10)
                if fetched:
                    first = fetched[0] if isinstance(fetched, (list, tuple)) else fetched
                    txt = extract_msg_text(first)
            except Exception:
                pass
        return await asyncio.wait_for(client.send_message(tgt_peer, txt or f"پیام #{msg_id_val}"), timeout=15)


async def run_job_worker(job: dict):
    err_streak = 0
    while job in jobs:
        try:
            if not job.get("running"):
                await asyncio.sleep(0.25)
                continue

            mc = int(job.get("max_count") or 0)
            sent = int(job.get("sent") or 0)
            if mc > 0 and sent >= mc:
                job["running"] = False
                job["status_text"] = f"تکمیل شد ({mc}/{mc})"
                job["current_cmd"] = ""
                save_state()
                push_log("DONE", f"مأموریت {job['id']} با {mc} ارسال به پایان رسید.")
                await asyncio.sleep(0.4)
                continue

            if not account_state.get("connected") or client is None:
                if not account_state.get("session"):
                    job["status_text"] = "در انتظار ورود مجدد به اکانت (سشن خارج شده)"
                else:
                    job["status_text"] = "در انتظار اتصال پایدار به سروش..."
                await asyncio.sleep(0.8)
                continue

            t0 = time.perf_counter()
            tgt_label = job.get("target_name") or job["target"]
            next_num = sent + 1

            if job["kind"] == "spam":
                texts = job.get("texts") or []
                if not texts:
                    job["running"] = False
                    job["status_text"] = "متنی تعریف نشده است"
                    save_state()
                    continue
                idx = int(job.get("text_idx") or 0) % len(texts)
                msg = texts[idx]
                job["current_cmd"] = f"اسپم به {tgt_label} | متن #{idx + 1}"
                job["status_text"] = "در حال ارسال..."

                await perform_spam_send(to_peer_arg(job["target"]), msg)

                job["text_idx"] = (idx + 1) % len(texts)
                short_msg = " ".join(msg.splitlines()).strip()[:45]
                push_log("SEND", f"[{job['id']}] #{next_num} -> {tgt_label} | {short_msg}")
            else:
                src_label = job.get("source_name") or job.get("source")
                mid_str = str(job.get("msg_id") or "")
                job["current_cmd"] = f"فوروارد پیام #{mid_str} از {src_label} به {tgt_label}"
                job["status_text"] = "در حال فوروارد..."

                await perform_forward_send(
                    to_peer_arg(job.get("source") or ""),
                    to_peer_arg(job["target"]),
                    mid_str,
                    str(job.get("msg_preview") or ""),
                )
                push_log("FWD", f"[{job['id']}] #{next_num} | پیام #{mid_str} از {src_label} -> {tgt_label}")

            job["sent"] = next_num
            job["last_time"] = now_str()
            job["last_err"] = ""
            job["status_text"] = f"فعال · ارسال #{job['sent']}"
            err_streak = 0

            if job["sent"] % 10 == 0:
                save_state()

            if int(job.get("max_count") or 0) > 0 and job["sent"] >= int(job["max_count"]):
                job["running"] = False
                job["status_text"] = f"تکمیل شد ({job['sent']}/{job['max_count']})"
                job["current_cmd"] = ""
                save_state()
                push_log("DONE", f"مأموریت {job['id']} تکمیل شد.")
                continue

            elapsed = time.perf_counter() - t0
            spd = max(0.05, float(job.get("speed") or 1.0))
            if spd - elapsed > 0:
                await asyncio.sleep(spd - elapsed)

        except Exception as e:
            err_code, hint = classify_error(e)
            cmd_info = job.get("current_cmd") or job["kind"]

            if err_code == "SESSION_REVOKED":
                await handle_session_revoked(f"در حین اجرای [{cmd_info}]: {e}")
                job["last_err"] = f"[{err_code}] اکانت از نشست خارج شد: {str(e)[:80]}"
                job["status_text"] = "خطایاب: سشن نامعتبر/خارج‌شده؛ منتظر ورود مجدد"
                await asyncio.sleep(2.0)
                continue

            err_streak += 1
            job["last_err"] = f"[{err_code}] {str(e)[:90]}"
            job["status_text"] = f"خطایاب [{err_code}]: {hint} (تلاش {err_streak})"
            push_log("FIX", f"خطایاب روی {job['id']} | دستور: {cmd_info} | علت: [{err_code}] {hint} | خطا: {str(e)[:75]}")

            if err_streak >= 2:
                account_state["connected"] = False
                await ensure_connected(force=True)
                err_streak = 0

            await asyncio.sleep(0.8 if err_code != "RATE_LIMIT" else 2.5)


async def supervisor():
    workers = {}
    while True:
        try:
            current_ids = {j["id"] for j in jobs}
            for jid in list(workers.keys()):
                if jid not in current_ids or workers[jid].done():
                    workers.pop(jid, None)
            for j in list(jobs):
                if j["id"] not in workers:
                    workers[j["id"]] = asyncio.create_task(run_job_worker(j))
        except Exception as e:
            push_log("ERR", f"خطا در ناظر مأموریت‌ها: {e}")
        await asyncio.sleep(0.4)


async def keepalive_worker():
    global last_keepalive_ts
    while True:
        try:
            now_m = time.monotonic()
            if account_state.get("session") and login_state.get("step") != "code_sent":
                if not account_state.get("connected") or (now_m - last_keepalive_ts) >= 300.0:
                    ok = await ensure_connected(force=False)
                    if ok:
                        last_keepalive_ts = time.monotonic()
                        if not dialogs_cache["groups"] and not dialogs_cache["privates"]:
                            await do_fetch_dialogs("all")
        except Exception as e:
            push_log("NET", f"بررسی نگهبان اتصال: {e}")
        await asyncio.sleep(7)


async def async_entry():
    global bot_loop
    bot_loop = asyncio.get_running_loop()
    push_log("SYS", f"موتور ۲۴ ساعته راه‌اندازی شد (حداکثر {MAX_CONCURRENT_JOBS} مأموریت هم‌زمان).")

    asyncio.create_task(supervisor())
    asyncio.create_task(keepalive_worker())

    if account_state.get("session"):
        ok = await ensure_connected(force=True)
        if ok:
            globals()["last_keepalive_ts"] = time.monotonic()
            await do_fetch_dialogs("all")

    while True:
        await asyncio.sleep(1)


def run_async_thread():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    while True:
        try:
            loop.run_until_complete(async_entry())
        except Exception as e:
            push_log("ERR", f"راه‌اندازی مجدد حلقه اصلی: {e}")
            time.sleep(1)


# ================= رابط وب ساده با گزینه‌های رنگی =================
PANEL_HTML = """<!DOCTYPE html>
<html lang="fa" dir="rtl">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>پنل مدیریت سروش</title>
<style>
  body {
    font-family: Tahoma, Arial, sans-serif;
    background: #111318;
    color: #eef1f6;
    margin: 0;
    padding: 12px;
  }
  .wrap { max-width: 560px; margin: 0 auto; }
  .box {
    background: #191d26;
    border: 1px solid #2b3242;
    border-radius: 10px;
    padding: 12px;
    margin-bottom: 12px;
  }
  h3 {
    margin: 0 0 10px;
    font-size: 14px;
    border-bottom: 1px solid #2b3242;
    padding-bottom: 6px;
    color: #fff;
  }
  label { display: block; font-size: 12px; color: #b7c0d1; margin: 8px 0 4px; }
  input, select, textarea {
    width: 100%;
    box-sizing: border-box;
    padding: 8px 10px;
    border-radius: 7px;
    border: 1px solid #353e52;
    background: #0f1218;
    color: #fff;
    font-family: inherit;
    font-size: 13px;
  }
  textarea { min-height: 56px; resize: vertical; }
  button {
    width: 100%;
    padding: 9px 11px;
    margin-top: 8px;
    border: none;
    border-radius: 7px;
    color: #fff;
    font-weight: bold;
    font-family: inherit;
    font-size: 13px;
    cursor: pointer;
    background: #2563eb;
  }
  .c-blue { background: #2563eb; }
  .c-green { background: #16a34a; }
  .c-amber { background: #d97706; }
  .c-red { background: #dc2626; }
  .c-purple { background: #7c3aed; }
  .c-teal { background: #0d9488; }
  .row { display: flex; gap: 6px; flex-wrap: wrap; align-items: center; }
  .row > button.half { flex: 1; min-width: 140px; }
  .mini { width: auto; padding: 5px 10px; font-size: 11px; margin-top: 0; }
  .list-area {
    margin-top: 8px;
    padding: 8px;
    background: #10131a;
    border: 1px solid #262d3d;
    border-radius: 7px;
    max-height: 150px;
    overflow-y: auto;
  }
  .item-btn {
    width: auto;
    display: inline-block;
    margin: 3px;
    padding: 5px 9px;
    font-size: 12px;
    background: #1e293b;
    border: 1px solid #334155;
  }
  .msg-row {
    background: #121620;
    border: 1px solid #293142;
    border-radius: 7px;
    padding: 7px 9px;
    margin-top: 6px;
    font-size: 12px;
    cursor: pointer;
  }
  .msg-row.active { border-color: #16a34a; background: #13261b; }
  .job-card {
    background: #121620;
    border: 1px solid #2a3244;
    border-radius: 8px;
    padding: 8px 10px;
    margin-top: 8px;
    font-size: 12px;
    line-height: 1.7;
  }
  .hint { font-size: 11px; color: #9aa5b8; margin-top: 5px; word-break: break-word; }
  .err-txt { color: #fca5a5; font-size: 11px; margin-top: 3px; }
  .term {
    background: #090b0f;
    color: #86efac;
    border: 1px solid #263042;
    border-radius: 7px;
    padding: 8px;
    height: 185px;
    overflow-y: auto;
    font-family: Consolas, monospace;
    font-size: 11px;
    line-height: 1.5;
    white-space: pre-wrap;
    direction: ltr;
    text-align: left;
  }
</style>
</head>
<body>
<div class="wrap">

  <!-- 1. اتصال شماره و کد -->
  <div class="box">
    <h3>اتصال اکانت سروش</h3>
    <div class="hint" id="acc_status_box">وضعیت: در حال بارگذاری...</div>
    <label>شماره تلفن:</label>
    <input id="phone_inp" type="text" placeholder="+98912..." dir="ltr">
    <button class="c-blue" onclick="sendPhone()">ارسال کد</button>

    <div id="code_area" style="display:none; margin-top:6px;">
      <label>کد تأیید:</label>
      <input id="code_inp" type="text" placeholder="کد دریافتی" dir="ltr">
      <button class="c-green" onclick="verifyCode()">تأیید و ثبت</button>
    </div>
    <div class="hint" id="login_msg">-</div>
  </div>

  <!-- 2. مأموریت‌های فعال (حداکثر 3 تا هم‌زمان) -->
  <div class="box">
    <h3>مأموریت‌های فعال (حداکثر ۳ مورد هم‌زمان)</h3>
    <div class="hint" id="slots_info">فعال: 0 / 3</div>
    <div id="jobs_box"><div class="hint">هنوز مأموریتی ثبت نشده است.</div></div>
  </div>

  <!-- 3. دریافت گروه / شخصی و ارسال اسپم -->
  <div class="box">
    <h3>دریافت گروه / شخصی و ارسال اسپم</h3>
    <div class="row">
      <button type="button" class="half c-teal" onclick="fetchDialogs('groups', 'spam')">دریافت گروه</button>
      <button type="button" class="half c-purple" onclick="fetchDialogs('privates', 'spam')">دریافت شخصی</button>
    </div>
    <div id="spam_dialogs_view" class="list-area">
      <div class="hint">روی «دریافت گروه» یا «دریافت شخصی» بزنید تا لیست نمایش داده شود.</div>
    </div>

    <label>مقصد اسپم (انتخاب از لیست بالا یا وارد کردن دستی):</label>
    <input id="spam_target" type="text" placeholder="آیدی، @username یا لینک...">

    <label>متن پیام (برای چند متن، با دکمه افزودن):</label>
    <div id="spam_texts_wrap">
      <textarea class="spam-t" placeholder="متن پیام..."></textarea>
    </div>
    <button type="button" class="c-blue" onclick="addTextArea()">افزودن متن دیگر</button>

    <label>سرعت / فاصله زمانی (ثانیه):</label>
    <input id="spam_speed" type="number" value="1" step="0.1" min="0.05">

    <label>تعداد ارسال (0 = نامحدود):</label>
    <input id="spam_count" type="number" value="0" step="1" min="0">

    <button type="button" class="c-green" onclick="startSpamJob()">شروع اسپم</button>
    <div class="hint" id="spam_feedback"></div>
  </div>

  <!-- 4. تعیین مبدأ، 5 پیام اخیر، مقصد و فوروارد -->
  <div class="box">
    <h3>تعیین مبدأ و فوروارد پیام</h3>
    <div class="row">
      <button type="button" class="half c-teal" onclick="fetchDialogs('groups', 'fwd_src')">دریافت گروه</button>
      <button type="button" class="half c-purple" onclick="fetchDialogs('privates', 'fwd_src')">دریافت شخصی</button>
    </div>
    <div id="fwd_src_dialogs_view" class="list-area">
      <div class="hint">روی «دریافت گروه» یا «دریافت شخصی» بزنید تا لیست نمایش داده شود.</div>
    </div>

    <label>تعیین مبدأ (از لیست بالا انتخاب کنید یا وارد کنید):</label>
    <select id="src_select" onchange="onSelectSource(this.value)">
      <option value="">-- انتخاب مبدأ --</option>
    </select>
    <input id="fwd_source" type="text" placeholder="مبدأ (گروه، کانال یا شخصی)..." style="margin-top:5px;">

    <button type="button" class="c-amber" onclick="loadLast5()">نمایش ۵ پیام اخیر</button>
    <div id="last5_list"><div class="hint">پیامی نمایش داده نشده است.</div></div>

    <label>شناسه پیام انتخاب‌شده:</label>
    <input id="fwd_msg_id" type="text" placeholder="با کلیک روی یکی از ۵ پیام پر می‌شود" dir="ltr">
    <div class="hint" id="fwd_msg_prev">پیام انتخاب‌شده: -</div>

    <div class="row" style="margin-top:8px;">
      <button type="button" class="half c-teal" onclick="fetchDialogs('groups', 'fwd_tgt')">دریافت گروه مقصد</button>
      <button type="button" class="half c-purple" onclick="fetchDialogs('privates', 'fwd_tgt')">دریافت شخصی مقصد</button>
    </div>
    <div id="fwd_tgt_dialogs_view" class="list-area">
      <div class="hint">روی «دریافت گروه مقصد» یا «دریافت شخصی مقصد» بزنید تا لیست نمایش داده شود.</div>
    </div>

    <label>تعیین مقصد فوروارد:</label>
    <select id="tgt_select" onchange="onSelectTarget(this.value)">
      <option value="">-- انتخاب مقصد --</option>
    </select>
    <input id="fwd_target" type="text" placeholder="مقصد..." style="margin-top:5px;">

    <label>سرعت / فاصله زمانی (ثانیه):</label>
    <input id="fwd_speed" type="number" value="1" step="0.1" min="0.05">

    <label>تعداد فوروارد (0 = نامحدود):</label>
    <input id="fwd_count" type="number" value="0" step="1" min="0">

    <button type="button" class="c-green" onclick="startForwardJob()">شروع فوروارد</button>
    <div class="hint" id="fwd_feedback"></div>
  </div>

  <!-- 5. ترمینال زنده سرور -->
  <div class="box">
    <h3>ترمینال زنده سرور</h3>
    <div id="terminal_box" class="term">در حال اتصال به ترمینال سرور...</div>
  </div>

</div>

<script>
let cachedGroups = [];
let cachedPrivates = [];
let selectedMsgText = '';
let forceCodeVisible = false;

function esc(v){
  return String(v || '').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
}

function addTextArea(){
  const wrap = document.getElementById('spam_texts_wrap');
  const ta = document.createElement('textarea');
  ta.className = 'spam-t';
  ta.style.marginTop = '6px';
  ta.placeholder = 'متن جایگزین...';
  wrap.appendChild(ta);
}

function getSpamTexts(){
  return Array.from(document.querySelectorAll('.spam-t'))
    .map(el => el.value.trim())
    .filter(Boolean);
}

async function sendPhone(){
  const p = document.getElementById('phone_inp').value.trim();
  if(!p){ alert('شماره را وارد کنید'); return; }
  forceCodeVisible = true;
  document.getElementById('code_area').style.display = 'block';
  document.getElementById('login_msg').innerText = 'در حال ارسال کد...';
  await fetch('/api/send_code?phone=' + encodeURIComponent(p));
  setTimeout(refreshState, 600);
}

async function verifyCode(){
  const c = document.getElementById('code_inp').value.trim();
  if(!c){ alert('کد را وارد کنید'); return; }
  document.getElementById('login_msg').innerText = 'در حال بررسی کد...';
  await fetch('/api/verify_code?code=' + encodeURIComponent(c));
  setTimeout(refreshState, 800);
}

function getDialogBoxId(role){
  if(role === 'spam') return 'spam_dialogs_view';
  if(role === 'fwd_tgt') return 'fwd_tgt_dialogs_view';
  return 'fwd_src_dialogs_view';
}

function renderDialogButtons(boxId, items, role, emptyText){
  const box = document.getElementById(boxId);
  if(!Array.isArray(items) || !items.length){
    box.innerHTML = `<div class="hint">${esc(emptyText)}</div>`;
    return;
  }
  box.innerHTML = items.map(it =>
    `<button type="button" class="item-btn" onclick="applyPickedDialog('${role}', '${esc(it.id)}')">[${esc(it.type)}] ${esc(it.name)}</button>`
  ).join('');
}

function applyPickedDialog(role, id){
  if(role === 'spam'){
    document.getElementById('spam_target').value = id;
  } else if(role === 'fwd_src'){
    document.getElementById('fwd_source').value = id;
    document.getElementById('src_select').value = id;
  } else if(role === 'fwd_tgt'){
    document.getElementById('fwd_target').value = id;
    document.getElementById('tgt_select').value = id;
  }
}

function rebuildSelects(){
  const combined = [];
  cachedGroups.forEach(g => combined.push(g));
  cachedPrivates.forEach(p => combined.push(p));

  const opts = '<option value="">-- انتخاب از لیست --</option>' + combined.map(x =>
    `<option value="${esc(x.id)}">[${esc(x.type)}] ${esc(x.name)}</option>`
  ).join('');

  const curSrc = document.getElementById('src_select').value;
  const curTgt = document.getElementById('tgt_select').value;

  document.getElementById('src_select').innerHTML = opts;
  document.getElementById('tgt_select').innerHTML = opts;

  if(curSrc) document.getElementById('src_select').value = curSrc;
  if(curTgt) document.getElementById('tgt_select').value = curTgt;
}

function onSelectSource(val){
  if(val) document.getElementById('fwd_source').value = val;
}
function onSelectTarget(val){
  if(val) document.getElementById('fwd_target').value = val;
}

async function fetchDialogs(mode, role){
  const targetBoxId = getDialogBoxId(role);
  const box = document.getElementById(targetBoxId);
  box.innerHTML = '<div class="hint">در حال دریافت اطلاعات از سروش...</div>';

  const r = await fetch('/api/dialogs?mode=' + encodeURIComponent(mode));
  const data = await r.json();
  if(!data.ok){
    box.innerHTML = `<div class="err-txt">${esc(data.error || 'خطا در دریافت')}</div>`;
    return;
  }
  cachedGroups = data.groups || [];
  cachedPrivates = data.privates || [];
  rebuildSelects();

  if(mode === 'privates'){
    renderDialogButtons(targetBoxId, cachedPrivates, role, 'چت شخصی یافت نشد.');
  } else {
    renderDialogButtons(targetBoxId, cachedGroups, role, 'گروه یا کانال یافت نشد.');
  }
}

async function loadLast5(){
  const src = document.getElementById('fwd_source').value.trim() || document.getElementById('src_select').value.trim();
  const box = document.getElementById('last5_list');
  if(!src){
    box.innerHTML = '<div class="err-txt">ابتدا مبدأ را انتخاب یا وارد کنید.</div>';
    return;
  }
  box.innerHTML = '<div class="hint">در حال دریافت ۵ پیام اخیر...</div>';
  const r = await fetch('/api/last5?source=' + encodeURIComponent(src));
  const data = await r.json();
  if(!data.ok){
    box.innerHTML = `<div class="err-txt">${esc(data.error || 'خطا در خواندن پیام‌ها')}</div>`;
    return;
  }
  const msgs = data.messages || [];
  if(!msgs.length){
    box.innerHTML = '<div class="hint">پیامی یافت نشد.</div>';
    return;
  }
  box.innerHTML = msgs.map(m => {
    const b64 = btoa(unescape(encodeURIComponent(m.text || m.preview || '')));
    return `<div class="msg-row" onclick="chooseMsg(this, '${esc(m.id)}', '${b64}')">
      <b>#${m.index}</b> (ID: <span dir="ltr">${esc(m.id)}</span>) — ${esc(m.preview)}
    </div>`;
  }).join('');
}

function chooseMsg(el, mid, b64){
  document.querySelectorAll('.msg-row').forEach(x => x.classList.remove('active'));
  el.classList.add('active');
  document.getElementById('fwd_msg_id').value = mid;
  const txt = decodeURIComponent(escape(atob(b64)));
  selectedMsgText = txt;
  document.getElementById('fwd_msg_prev').innerText = 'پیام انتخاب‌شده: ' + txt.slice(0, 70);
}

async function startSpamJob(){
  const target = document.getElementById('spam_target').value.trim();
  const texts = getSpamTexts();
  const speed = document.getElementById('spam_speed').value;
  const max_count = document.getElementById('spam_count').value;
  const fb = document.getElementById('spam_feedback');
  if(!target || !texts.length){
    fb.innerText = 'مقصد و حداقل یک متن را وارد کنید.';
    return;
  }
  const r = await fetch('/api/jobs/spam', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({target, texts, speed, max_count})
  });
  const out = await r.json();
  fb.innerText = out.msg || '';
  refreshState();
}

async function startForwardJob(){
  const source = document.getElementById('fwd_source').value.trim() || document.getElementById('src_select').value.trim();
  const target = document.getElementById('fwd_target').value.trim() || document.getElementById('tgt_select').value.trim();
  const msg_id = document.getElementById('fwd_msg_id').value.trim();
  const speed = document.getElementById('fwd_speed').value;
  const max_count = document.getElementById('fwd_count').value;
  const fb = document.getElementById('fwd_feedback');
  if(!source || !target || !msg_id){
    fb.innerText = 'مبدأ، پیام و مقصد را کامل کنید.';
    return;
  }
  const r = await fetch('/api/jobs/forward', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({source, target, msg_id, msg_preview: selectedMsgText, speed, max_count})
  });
  const out = await r.json();
  fb.innerText = out.msg || '';
  refreshState();
}

async function jobAction(id, action){
  const r = await fetch(`/api/jobs/${action}?id=` + encodeURIComponent(id));
  const out = await r.json();
  if(out && out.msg) alert(out.msg);
  refreshState();
}

function renderJobs(list, maxSlots){
  const box = document.getElementById('jobs_box');
  const activeCnt = (list || []).filter(j => j.running).length;
  document.getElementById('slots_info').innerText = `مأموریت‌های فعال: ${activeCnt} / ${maxSlots}`;

  if(!Array.isArray(list) || !list.length){
    box.innerHTML = '<div class="hint">هنوز مأموریتی ثبت نشده است.</div>';
    return;
  }

  box.innerHTML = list.map(j => {
    const kindLabel = j.kind === 'forward' ? 'فوروارد' : 'اسپم';
    const lim = j.max_count > 0 ? `${j.sent} / ${j.max_count}` : `${j.sent} / ∞`;
    const route = j.kind === 'forward'
      ? `${esc(j.source_name || j.source)} ← ${esc(j.target_name || j.target)} (پیام #${esc(j.msg_id)})`
      : `مقصد: ${esc(j.target_name || j.target)}`;

    return `<div class="job-card">
      <div><b>[${kindLabel}]</b> ${route}</div>
      <div>سرعت: <b>${esc(j.speed)}s</b> | ارسال: <b>${esc(lim)}</b> | آخرین: <b>${esc(j.last_time)}</b></div>
      <div>وضعیت: <b>${j.running ? 'در حال اجرا' : 'متوقف'}</b> — ${esc(j.status_text || '')}</div>
      ${j.current_cmd ? `<div class="hint">دستور فعلی: ${esc(j.current_cmd)}</div>` : ''}
      ${j.last_err ? `<div class="err-txt" dir="ltr">${esc(j.last_err)}</div>` : ''}
      <div class="row" style="margin-top:6px;">
        ${j.running
          ? `<button type="button" class="mini c-amber" onclick="jobAction('${esc(j.id)}','pause')">توقف</button>`
          : `<button type="button" class="mini c-green" onclick="jobAction('${esc(j.id)}','resume')">شروع / ادامه</button>`}
        <button type="button" class="mini c-red" onclick="jobAction('${esc(j.id)}','delete')">حذف</button>
      </div>
    </div>`;
  }).join('');
}

async function refreshState(){
  try{
    const r = await fetch('/api/state');
    const d = await r.json();

    const acc = d.account || {};
    document.getElementById('acc_status_box').innerText =
      `وضعیت: ${acc.status || '-'} | پینگ: ${acc.ping || '-'}`;
    document.getElementById('login_msg').innerText = d.login_msg || '-';

    if(d.login_step === 'connected'){ forceCodeVisible = false; }
    const showCode = forceCodeVisible || (d.login_step === 'code_sent');
    document.getElementById('code_area').style.display = showCode ? 'block' : 'none';

    if(Array.isArray(d.groups)) cachedGroups = d.groups;
    if(Array.isArray(d.privates)) cachedPrivates = d.privates;
    rebuildSelects();

    renderJobs(d.jobs || [], d.max_jobs || 3);

    const term = document.getElementById('terminal_box');
    const atBottom = (term.scrollTop + term.clientHeight) >= (term.scrollHeight - 25);
    term.innerText = (d.logs || []).join('\\n');
    if(atBottom) term.scrollTop = term.scrollHeight;
  }catch(e){}
}

setInterval(refreshState, 1500);
refreshState();
</script>
</body>
</html>
"""


# ================= مسیرهای وب و API =================
def run_coro(coro, timeout=18):
    if not bot_loop or bot_loop.is_closed():
        return None
    fut = asyncio.run_coroutine_threadsafe(coro, bot_loop)
    try:
        return fut.result(timeout=timeout)
    except Exception:
        return None


def json_out(payload, code=200):
    return app.response_class(
        response=json.dumps(payload, ensure_ascii=False),
        status=code,
        mimetype="application/json; charset=utf-8",
    )


@app.route("/")
def index_route():
    return f"OK - Open {PANEL_PATH}", 200


@app.route("/health")
def health_route():
    return "OK", 200


@app.route(PANEL_PATH)
def panel_route():
    return PANEL_HTML


@app.route("/api/send_code")
def api_send_code():
    phone = (request.args.get("phone") or "").strip()
    if phone and bot_loop and not bot_loop.is_closed():
        login_state["phone"] = phone
        login_state["step"] = "code_sent"
        login_state["message"] = f"در حال ارسال کد به {phone}..."
        asyncio.run_coroutine_threadsafe(do_send_code(phone), bot_loop)
    return json_out({"ok": True})


@app.route("/api/verify_code")
def api_verify_code():
    code = (request.args.get("code") or "").strip()
    if code and bot_loop and not bot_loop.is_closed():
        asyncio.run_coroutine_threadsafe(do_verify_code(code), bot_loop)
    return json_out({"ok": True})


@app.route("/api/dialogs")
def api_dialogs():
    mode = (request.args.get("mode") or "all").strip()
    res = run_coro(do_fetch_dialogs(mode), timeout=18)
    if isinstance(res, dict):
        return json_out(res)
    return json_out({
        "ok": True,
        "error": "",
        "groups": dialogs_cache["groups"],
        "privates": dialogs_cache["privates"],
    })


@app.route("/api/last5")
def api_last5():
    src = (request.args.get("source") or "").strip()
    res = run_coro(do_fetch_last5(src), timeout=18)
    if isinstance(res, dict):
        return json_out(res)
    return json_out({"ok": False, "error": "پاسخ دریافت نشد.", "messages": []})


@app.route("/api/jobs/spam", methods=["POST"])
def api_create_spam():
    global job_counter
    body = request.get_json(silent=True) or {}
    target = parse_peer_id(body.get("target") or "")
    texts = [str(t).strip() for t in (body.get("texts") or []) if str(t).strip()]
    try:
        speed = max(0.05, float(body.get("speed") or 1.0))
    except Exception:
        speed = 1.0
    try:
        max_count = max(0, int(body.get("max_count") or 0))
    except Exception:
        max_count = 0

    if not target or not texts:
        return json_out({"ok": False, "msg": "مقصد یا متن معتبر نیست."})

    if active_running_count() >= MAX_CONCURRENT_JOBS:
        return json_out({"ok": False, "msg": f"حداکثر {MAX_CONCURRENT_JOBS} مأموریت هم‌زمان مجاز است. ابتدا یکی را متوقف یا حذف کنید."})

    job_counter += 1
    job = {
        "id": f"sp_{job_counter}_{int(time.time()*1000)%10000}",
        "kind": "spam",
        "source": "",
        "source_name": "",
        "target": target,
        "target_name": find_dialog_name(target),
        "msg_id": "",
        "msg_preview": "",
        "texts": texts,
        "text_idx": 0,
        "speed": speed,
        "max_count": max_count,
        "sent": 0,
        "running": True,
        "last_time": "-",
        "last_err": "",
        "current_cmd": f"اسپم به {find_dialog_name(target)}",
        "status_text": "ثبت شد و در حال شروع",
    }
    jobs.append(job)
    save_state()
    push_log("JOB", f"مأموریت اسپم {job['id']} به مقصد {job['target_name']} ثبت و فعال شد.")
    return json_out({"ok": True, "msg": "مأموریت اسپم روی سرور ثبت و فعال شد."})


@app.route("/api/jobs/forward", methods=["POST"])
def api_create_forward():
    global job_counter
    body = request.get_json(silent=True) or {}
    source = parse_peer_id(body.get("source") or "")
    target = parse_peer_id(body.get("target") or "")
    msg_id = str(body.get("msg_id") or "").strip()
    msg_preview = str(body.get("msg_preview") or "").strip()
    try:
        speed = max(0.05, float(body.get("speed") or 1.0))
    except Exception:
        speed = 1.0
    try:
        max_count = max(0, int(body.get("max_count") or 0))
    except Exception:
        max_count = 0

    if not source or not target or not msg_id:
        return json_out({"ok": False, "msg": "مبدأ، شناسه پیام یا مقصد ناقص است."})

    if active_running_count() >= MAX_CONCURRENT_JOBS:
        return json_out({"ok": False, "msg": f"حداکثر {MAX_CONCURRENT_JOBS} مأموریت هم‌زمان مجاز است. ابتدا یکی را متوقف یا حذف کنید."})

    job_counter += 1
    src_name = find_dialog_name(source)
    tgt_name = find_dialog_name(target)
    job = {
        "id": f"fw_{job_counter}_{int(time.time()*1000)%10000}",
        "kind": "forward",
        "source": source,
        "source_name": src_name,
        "target": target,
        "target_name": tgt_name,
        "msg_id": msg_id,
        "msg_preview": msg_preview,
        "texts": [],
        "text_idx": 0,
        "speed": speed,
        "max_count": max_count,
        "sent": 0,
        "running": True,
        "last_time": "-",
        "last_err": "",
        "current_cmd": f"فوروارد #{msg_id} از {src_name} به {tgt_name}",
        "status_text": "ثبت شد و در حال شروع",
    }
    jobs.append(job)
    save_state()
    push_log("JOB", f"مأموریت فوروارد {job['id']} ({src_name} -> {tgt_name}) ثبت و فعال شد.")
    return json_out({"ok": True, "msg": "مأموریت فوروارد روی سرور ثبت و فعال شد."})


@app.route("/api/jobs/pause")
def api_pause_job():
    jid = (request.args.get("id") or "").strip()
    for j in jobs:
        if j["id"] == jid:
            j["running"] = False
            j["status_text"] = "متوقف‌شده توسط کاربر"
            j["current_cmd"] = ""
            push_log("JOB", f"مأموریت {jid} متوقف شد.")
    save_state()
    return json_out({"ok": True})


@app.route("/api/jobs/resume")
def api_resume_job():
    jid = (request.args.get("id") or "").strip()
    target_job = next((j for j in jobs if j["id"] == jid), None)
    if not target_job:
        return json_out({"ok": False, "msg": "مأموریت پیدا نشد."})
    if not target_job.get("running") and active_running_count(exclude_id=jid) >= MAX_CONCURRENT_JOBS:
        return json_out({"ok": False, "msg": f"هم‌زمان فقط {MAX_CONCURRENT_JOBS} مأموریت می‌توانند فعال باشند."})

    mc = int(target_job.get("max_count") or 0)
    if mc > 0 and int(target_job.get("sent") or 0) >= mc:
        target_job["sent"] = 0
    target_job["running"] = True
    target_job["last_err"] = ""
    target_job["status_text"] = "در حال ازسرگیری..."
    save_state()
    push_log("JOB", f"مأموریت {jid} فعال شد.")
    return json_out({"ok": True})


@app.route("/api/jobs/delete")
def api_delete_job():
    jid = (request.args.get("id") or "").strip()
    jobs[:] = [j for j in jobs if j["id"] != jid]
    save_state()
    push_log("JOB", f"مأموریت {jid} حذف شد.")
    return json_out({"ok": True})


@app.route("/api/state")
def api_state():
    return json_out({
        "max_jobs": MAX_CONCURRENT_JOBS,
        "account": {
            "phone": account_state.get("phone", ""),
            "name": account_state.get("name", ""),
            "connected": bool(account_state.get("connected")),
            "ping": account_state.get("ping", "-"),
            "status": account_state.get("status", ""),
        },
        "login_step": login_state.get("step", "idle"),
        "login_msg": login_state.get("message", ""),
        "groups": dialogs_cache["groups"],
        "privates": dialogs_cache["privates"],
        "jobs": jobs,
        "logs": list(terminal_logs),
    })


def run_http():
    app.run(host="0.0.0.0", port=PORT, use_reloader=False, threaded=True)


if __name__ == "__main__":
    load_state()
    Thread(target=run_async_thread, daemon=True).start()
    Thread(target=run_http, daemon=True).start()
    while True:
        time.sleep(1)
