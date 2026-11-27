import asyncio
import json
import os
import threading
import time
import uuid
from flask import Flask, jsonify, request, send_from_directory

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
HTML_DIR = os.path.join(BASE_DIR, "app", "src", "main", "assets")
STATE_FILE = os.path.join(BASE_DIR, "fonex_state.json")

MAX_ACCOUNTS = 2
MAX_CONCURRENT_TASKS = 3

app = Flask(__name__)
state_lock = threading.Lock()


def default_state():
    return {
        "accounts": [],
        "active_account": None,
        "pending_auth": {},
        "tasks": [],
        "stats": {"done": 0, "recovered": 0, "heartbeats": 0},
        "last_heartbeat": time.time(),
    }


def load_state():
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            base = default_state()
            base.update(data)
            base["accounts"] = base.get("accounts", [])[:MAX_ACCOUNTS]
            base["stats"] = {
                "done": base.get("stats", {}).get("done", 0),
                "recovered": base.get("stats", {}).get("recovered", 0),
                "heartbeats": base.get("stats", {}).get("heartbeats", 0),
            }
            return base
        except Exception:
            pass
    return default_state()


def save_state(st):
    tmp = STATE_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(st, f, ensure_ascii=False, indent=2)
    os.replace(tmp, STATE_FILE)


STATE = load_state()


def get_sys_stats():
    ram_pct = "-"
    cpu_pct = "-"
    try:
        with open("/proc/meminfo", "r", encoding="utf-8") as f:
            mem = {}
            for line in f:
                parts = line.split(":")
                if len(parts) == 2:
                    mem[parts[0].strip()] = int(parts[1].strip().split()[0])
            total = mem.get("MemTotal", 0)
            avail = mem.get("MemAvailable", 0)
            if total > 0:
                ram_pct = round(((total - avail) / total) * 100, 1)
    except Exception:
        pass
    try:
        load1, _, _ = os.getloadavg()
        cores = os.cpu_count() or 1
        cpu_pct = round(min(100.0, (load1 / cores) * 100), 1)
    except Exception:
        pass
    return ram_pct, cpu_pct


async def _send_soroush_action(task, account):
    """
    اینجا را به متدهای واقعی splusthon وصل کن تا ارسال/فوروارد واقعی روی اکانت انجام شود.
    """
    mode = task.get("mode")
    acc_name = account.get("phone") or account.get("key")
    if mode == "spam":
        texts = task.get("texts") or ["سلام"]
        idx = task.get("text_index", 0) % len(texts)
        text = texts[idx]
        task["text_index"] = (idx + 1) % len(texts)
        task["current_command"] = f"اسپم ({text[:22]})"
        task["last_action"] = f"اکانت {acc_name} -> دستور اسپم در {task['target_chat']}"
        # await client.send_message(task["target_chat"], text)
        await asyncio.sleep(0.05)
    elif mode == "forward":
        task["current_command"] = f"فوروارد پیام {task['message_id']} از {task['source_chat']}"
        task["last_action"] = f"اکانت {acc_name} -> دستور فوروارد به {task['target_chat']}"
        # await client.forward_messages(task["target_chat"], task["source_chat"], int(task["message_id"]))
        await asyncio.sleep(0.05)
    else:
        raise ValueError(f"حالت نامعتبر: {mode}")


def _task_runner(task_id):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    while True:
        with state_lock:
            task = next((t for t in STATE["tasks"] if t["id"] == task_id), None)
            if not task or task.get("deleted"):
                break
            status = task.get("status")
            if status in ("stopped", "done"):
                time.sleep(0.5)
                continue
            acc = next((a for a in STATE["accounts"] if a["key"] == task.get("account_key")), None)
            if not acc:
                task["status"] = "stopped"
                task["recovery_info"] = "اکانت تسک یافت نشد؛ متوقف شد."
                save_state(STATE)
                break

            acc_label = acc.get("phone") or acc["key"]
            task["account_label"] = acc_label
            task["status"] = "running"
            task["memory_snapshot"] = (
                f"اکانت: {acc_label} | مقصد: {task['target_chat']} | دستور: {task['mode']}"
            )
            save_state(STATE)

        try:
            loop.run_until_complete(_send_soroush_action(task, acc))
            with state_lock:
                t = next((x for x in STATE["tasks"] if x["id"] == task_id), None)
                if not t or t.get("deleted"):
                    break
                acc_label = acc.get("phone") or acc["key"]
                t["sent_count"] = t.get("sent_count", 0) + 1
                t["memory_snapshot"] = (
                    f"اکانت: {acc_label} | مقصد: {t['target_chat']} | "
                    f"دستور: {t['mode']} | مرحله: {t['sent_count']}"
                )
                t["recovery_info"] = f"اتصال پایدار و ثبت‌شده ({acc_label} | {t['mode']})"
                STATE["stats"]["done"] = STATE["stats"].get("done", 0) + 1
                max_c = int(t.get("max_count") or 0)
                if max_c > 0 and t["sent_count"] >= max_c:
                    t["status"] = "done"
                delay_sec = max(0.1, int(t.get("delay_ms") or 800) / 1000.0)
                save_state(STATE)
            time.sleep(delay_sec)

        except Exception as exc:
            with state_lock:
                t = next((x for x in STATE["tasks"] if x["id"] == task_id), None)
                if not t or t.get("deleted"):
                    break
                acc_label = t.get("account_label") or t.get("account_key")
                t["status"] = "recovering"
                t["last_error"] = str(exc)
                t["memory_snapshot"] = (
                    f"اکانت: {acc_label} | مقصد: {t['target_chat']} | دستور: {t['mode']}"
                )
                t["recovery_info"] = (
                    f"خطا گرفته و اصلاح شد -> اکانت {acc_label} در {t['target_chat']} "
                    f"با دستور {t['mode']}؛ ادامه از شمارنده {t.get('sent_count', 0)}"
                )
                STATE["stats"]["recovered"] = STATE["stats"].get("recovered", 0) + 1
                save_state(STATE)
            time.sleep(2.0)

    loop.close()


def start_worker_if_needed(task_id):
    th = threading.Thread(target=_task_runner, args=(task_id,), daemon=True)
    th.start()


def _keepalive_and_memory_loop():
    """حفظ مداوم نبض سرویس و یادآوری وضعیت هر تسک بدون فراموشی"""
    while True:
        time.sleep(5)
        with state_lock:
            STATE["last_heartbeat"] = time.time()
            STATE["stats"]["heartbeats"] = STATE["stats"].get("heartbeats", 0) + 1
            for t in STATE.get("tasks", []):
                if not t.get("deleted") and t.get("status") in ("running", "recovering"):
                    acc_label = t.get("account_label") or t.get("account_key")
                    t["memory_snapshot"] = (
                        f"اکانت: {acc_label} | مقصد: {t.get('target_chat')} | "
                        f"دستور: {t.get('mode')} | گام: {t.get('sent_count', 0)}"
                    )
            save_state(STATE)


def recover_on_boot():
    with state_lock:
        STATE["accounts"] = STATE.get("accounts", [])[:MAX_ACCOUNTS]
        active_tasks = [
            t for t in STATE.get("tasks", [])
            if not t.get("deleted") and t.get("status") in ("running", "recovering")
        ]
        for idx, t in enumerate(active_tasks):
            acc = next((a for a in STATE["accounts"] if a["key"] == t.get("account_key")), None)
            acc_label = (acc.get("phone") if acc else None) or t.get("account_label") or t.get("account_key")
            t["account_label"] = acc_label
            if idx < MAX_CONCURRENT_TASKS:
                t["status"] = "running"
                t["memory_snapshot"] = (
                    f"اکانت: {acc_label} | مقصد: {t.get('target_chat')} | دستور: {t.get('mode')}"
                )
                t["recovery_info"] = (
                    f"بازیابی فوری پس از ری‌استارت -> اکانت {acc_label} | "
                    f"مقصد {t.get('target_chat')} | دستور {t.get('mode')}"
                )
                start_worker_if_needed(t["id"])
            else:
                t["status"] = "stopped"
                t["recovery_info"] = "به دلیل سقف ۳ تسک هم‌زمان در حالت توقف قرار گرفت."
        save_state(STATE)


@app.route("/")
def index():
    if os.path.exists(os.path.join(HTML_DIR, "panel.html")):
        return send_from_directory(HTML_DIR, "panel.html")
    return "OK", 200


@app.route("/health")
def health():
    with state_lock:
        hb = STATE.get("last_heartbeat", time.time())
    return jsonify({"ok": True, "alive": True, "last_heartbeat": hb})


@app.route("/api/state")
def api_state():
    with state_lock:
        ram, cpu = get_sys_stats()
        visible_tasks = [t for t in STATE["tasks"] if not t.get("deleted")]
        payload = {
            "accounts": [
                {k: v for k, v in a.items() if k != "session"}
                for a in STATE["accounts"][:MAX_ACCOUNTS]
            ],
            "active_account": STATE.get("active_account"),
            "tasks": visible_tasks,
            "stats": {
                "done": STATE["stats"].get("done", 0),
                "recovered": STATE["stats"].get("recovered", 0),
                "heartbeats": STATE["stats"].get("heartbeats", 0),
                "ram": ram,
                "cpu": cpu,
            },
        }
    return jsonify(payload)


@app.route("/api/auth/send_code", methods=["POST"])
def api_send_code():
    data = request.get_json(force=True) or {}
    phone = (data.get("phone") or "").strip()
    if not phone:
        return jsonify({"error": "شماره الزامی است"}), 400
    with state_lock:
        STATE["pending_auth"][phone] = {"ts": time.time(), "code_hash": uuid.uuid4().hex[:8]}
        save_state(STATE)
    return jsonify({"ok": True, "phone": phone})


@app.route("/api/auth/verify", methods=["POST"])
def api_verify():
    data = request.get_json(force=True) or {}
    phone = (data.get("phone") or "").strip()
    code = (data.get("code") or "").strip()
    if not phone or not code:
        return jsonify({"error": "شماره و کد الزامی است"}), 400

    with state_lock:
        if len(STATE["accounts"]) >= MAX_ACCOUNTS:
            return jsonify({"error": "حداکثر ۲ اکانت مجاز است؛ ابتدا یکی را حذف کنید."}), 400
        key = f"acc_{len(STATE['accounts']) + 1}_{uuid.uuid4().hex[:4]}"
        acc = {"key": key, "phone": phone, "label": phone, "session": f"sess_{uuid.uuid4().hex}"}
        STATE["accounts"].append(acc)
        STATE["active_account"] = key
        STATE["pending_auth"].pop(phone, None)
        save_state(STATE)
    return jsonify({"ok": True, "account": {"key": key, "phone": phone}})


@app.route("/api/accounts/switch", methods=["POST"])
def api_switch_acc():
    data = request.get_json(force=True) or {}
    key = data.get("key")
    with state_lock:
        if not any(a["key"] == key for a in STATE["accounts"]):
            return jsonify({"error": "اکانت پیدا نشد"}), 404
        STATE["active_account"] = key
        save_state(STATE)
    return jsonify({"ok": True})


@app.route("/api/accounts/remove", methods=["POST"])
def api_remove_acc():
    data = request.get_json(force=True) or {}
    key = data.get("key")
    with state_lock:
        STATE["accounts"] = [a for a in STATE["accounts"] if a["key"] != key]
        if STATE.get("active_account") == key:
            STATE["active_account"] = STATE["accounts"][0]["key"] if STATE["accounts"] else None
        save_state(STATE)
    return jsonify({"ok": True})


@app.route("/api/tasks/create", methods=["POST"])
def api_create_task():
    data = request.get_json(force=True) or {}
    mode = (data.get("mode") or "spam").strip()
    if mode not in ("spam", "forward"):
        return jsonify({"error": "فقط حالت spam یا forward مجاز است"}), 400

    account_key = data.get("account_key")
    target_chat = (data.get("target_chat") or "").strip()
    if not account_key or not target_chat:
        return jsonify({"error": "اکانت و مقصد الزامی است"}), 400

    with state_lock:
        running = sum(
            1 for t in STATE["tasks"]
            if not t.get("deleted") and t.get("status") in ("running", "recovering")
        )
        if running >= MAX_CONCURRENT_TASKS:
            return jsonify({"error": "حداکثر ۳ تسک هم‌زمان مجاز است"}), 400

        acc = next((a for a in STATE["accounts"] if a["key"] == account_key), None)
        acc_label = (acc.get("phone") if acc else None) or account_key
        source_chat = (data.get("source_chat") or "").strip()
        message_id = (data.get("message_id") or "").strip()

        task = {
            "id": uuid.uuid4().hex[:8],
            "mode": mode,
            "account_key": account_key,
            "account_label": acc_label,
            "target_chat": target_chat,
            "texts": data.get("texts") or [],
            "text_index": 0,
            "source_chat": source_chat,
            "message_id": message_id,
            "delay_ms": int(data.get("delay_ms") or 800),
            "max_count": int(data.get("max_count") or 0),
            "sent_count": 0,
            "status": "running",
            "current_command": (
                f"اسپم در {target_chat}" if mode == "spam"
                else f"فوروارد {message_id} از {source_chat} به {target_chat}"
            ),
            "memory_snapshot": (
                f"اکانت: {acc_label} | مقصد: {target_chat} | دستور: {mode}"
            ),
            "last_action": f"شروع {mode} روی اکانت {acc_label} در {target_chat}",
            "recovery_info": "اتصال فعال و در حال حفظ حافظه",
            "deleted": False,
        }
        STATE["tasks"].append(task)
        save_state(STATE)

    start_worker_if_needed(task["id"])
    return jsonify({"ok": True, "task": task})


@app.route("/api/tasks/stop", methods=["POST"])
def api_stop_task():
    tid = (request.get_json(force=True) or {}).get("id")
    with state_lock:
        for t in STATE["tasks"]:
            if t["id"] == tid:
                t["status"] = "stopped"
        save_state(STATE)
    return jsonify({"ok": True})


@app.route("/api/tasks/resume", methods=["POST"])
def api_resume_task():
    tid = (request.get_json(force=True) or {}).get("id")
    with state_lock:
        running = sum(
            1 for t in STATE["tasks"]
            if not t.get("deleted") and t.get("status") in ("running", "recovering")
        )
        if running >= MAX_CONCURRENT_TASKS:
            return jsonify({"error": "حداکثر ۳ تسک هم‌زمان مجاز است"}), 400
        for t in STATE["tasks"]:
            if t["id"] == tid:
                t["status"] = "running"
                start_worker_if_needed(tid)
        save_state(STATE)
    return jsonify({"ok": True})


@app.route("/api/tasks/delete", methods=["POST"])
def api_delete_task():
    tid = (request.get_json(force=True) or {}).get("id")
    with state_lock:
        for t in STATE["tasks"]:
            if t["id"] == tid:
                t["deleted"] = True
                t["status"] = "stopped"
        save_state(STATE)
    return jsonify({"ok": True})


recover_on_boot()
threading.Thread(target=_keepalive_and_memory_loop, daemon=True).start()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
