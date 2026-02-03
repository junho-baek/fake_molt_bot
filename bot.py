#!/usr/bin/env python3
import json
import os
import queue
import subprocess
import sys
import threading
import time
import urllib.request
from typing import Dict

API_BASE = "https://api.telegram.org"


def _load_dotenv(path: str) -> None:
    if not os.path.exists(path):
        return
    try:
        with open(path, "r", encoding="utf-8") as f:
            for raw in f:
                line = raw.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, value = line.split("=", 1)
                key = key.strip()
                value = value.strip().strip("\"").strip("'")
                if key and key not in os.environ:
                    os.environ[key] = value
    except Exception:
        return


_load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
if not TOKEN:
    print("TELEGRAM_BOT_TOKEN is required", file=sys.stderr)
    sys.exit(1)

STATE_PATH = os.environ.get("BOT_STATE_PATH", os.path.join(os.path.dirname(__file__), "state.json"))
CODEX_CWD = os.environ.get("CODEX_CWD", os.getcwd())
APPROVAL_POLICY = os.environ.get("APPROVAL_POLICY", "never")
SANDBOX_POLICY = os.environ.get("SANDBOX_POLICY", "dangerFullAccess")
BOT_LOG_LEVEL = os.environ.get("BOT_LOG_LEVEL", "info").strip().lower()
ALLOWED_CHAT_IDS = os.environ.get("ALLOWED_CHAT_IDS", "").strip()
if ALLOWED_CHAT_IDS:
    ALLOWED_CHAT_IDS_SET = {int(x) for x in ALLOWED_CHAT_IDS.split(",") if x.strip().isdigit()}
else:
    ALLOWED_CHAT_IDS_SET = None


def _http_get_json(url: str) -> dict:
    with urllib.request.urlopen(url, timeout=60) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _http_post_json(url: str, payload: dict) -> dict:
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=60) as resp:
        return json.loads(resp.read().decode("utf-8"))


def tg_send_message(chat_id: int, text: str) -> None:
    url = f"{API_BASE}/bot{TOKEN}/sendMessage"
    payload = {"chat_id": chat_id, "text": text}
    _http_post_json(url, payload)

def _log(msg: str, level: str = "info") -> None:
    if BOT_LOG_LEVEL == "silent":
        return
    if BOT_LOG_LEVEL == "error" and level != "error":
        return
    if BOT_LOG_LEVEL == "warn" and level not in ("warn", "error"):
        return
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


class CodexAppServer:
    def __init__(self, cwd: str):
        self.cwd = cwd
        self.proc = subprocess.Popen(
            ["codex", "app-server"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            cwd=self.cwd,
            text=True,
            bufsize=1,
        )
        self._next_id = 1
        self._lock = threading.Lock()
        self._responses: Dict[int, queue.Queue] = {}
        self._orphan_responses: Dict[int, dict] = {}
        self._turn_text: Dict[str, str] = {}
        self._turn_done: Dict[str, threading.Event] = {}
        self._active_turn_id: str | None = None
        self._turn_meta: Dict[str, dict] = {}

        self._reader = threading.Thread(target=self._read_loop, daemon=True)
        self._reader.start()

        if self.proc.stderr is not None:
            threading.Thread(target=self._stderr_loop, daemon=True).start()

        self._initialize()

    def _read_loop(self) -> None:
        assert self.proc.stdout is not None
        for line in self.proc.stdout:
            try:
                line = line.strip()
                if not line:
                    continue
                try:
                    msg = json.loads(line)
                except json.JSONDecodeError:
                    continue

                if "id" in msg and ("result" in msg or "error" in msg):
                    msg_id = int(msg["id"])
                    q = self._responses.get(msg_id)
                    if q:
                        q.put(msg)
                    else:
                        self._orphan_responses[msg_id] = msg
                        _log(f"codex orphan response id={msg_id}")
                    continue
                if "id" in msg and "method" in msg:
                    # Server-initiated request; respond with method not supported.
                    self._send_response_error(msg["id"], -32601, "Method not supported")
                    continue

                method = msg.get("method")
                params = msg.get("params", {})

                if method == "codex/event/item_completed":
                    msg = params.get("msg", {})
                    item = msg.get("item", {})
                    turn_id = msg.get("turn_id")
                    thread_id = msg.get("thread_id")
                    content = item.get("content")
                    text = None
                    if isinstance(content, list):
                        parts = []
                        for c in content:
                            if isinstance(c, dict) and c.get("type", "").lower() == "text":
                                parts.append(c.get("text", ""))
                        text = "".join(parts).strip()
                    if text and turn_id and thread_id:
                        key = f"{thread_id}:{turn_id}"
                        self._turn_text[key] = text
                    continue

                if method and method.endswith("item/completed"):
                    item = params.get("item", {})
                    if item.get("type") == "agentMessage":
                        turn_id = params.get("turnId") or params.get("turn_id") or item.get("turnId")
                        content = item.get("content")
                        text = None
                        if isinstance(content, str):
                            text = content
                        elif isinstance(content, dict):
                            text = content.get("text")
                        elif isinstance(content, list):
                            parts = []
                            for c in content:
                                if isinstance(c, dict) and c.get("type") == "text":
                                    parts.append(c.get("text", ""))
                                elif isinstance(c, str):
                                    parts.append(c)
                            text = "".join(parts).strip()
                        if text and turn_id:
                            self._turn_text[turn_id] = self._turn_text.get(turn_id, "") + text
                    continue

                if method and method.endswith("turn/completed"):
                    turn = params.get("turn", {})
                    turn_id = params.get("turnId") or params.get("turn_id") or turn.get("id")
                    thread_id = params.get("threadId") or params.get("thread_id") or turn.get("threadId")
                    if turn_id and thread_id:
                        key = f"{thread_id}:{turn_id}"
                        ev = self._turn_done.get(key)
                        if ev:
                            ev.set()
                    continue
            except Exception as e:
                _log(f"codex read loop error: {e}")

    def _stderr_loop(self) -> None:
        assert self.proc.stderr is not None
        for line in self.proc.stderr:
            line = line.rstrip()
            if line:
                _log(f"codex stderr: {line}", level="warn")

    def _rpc(self, method: str, params: dict) -> dict:
        req_id, q = self._send_request(method, params)
        msg = q.get(timeout=120)
        del self._responses[req_id]
        if "error" in msg:
            raise RuntimeError(msg["error"])
        return msg["result"]

    def _send_request(self, method: str, params: dict) -> tuple[int, queue.Queue]:
        with self._lock:
            req_id = self._next_id
            self._next_id += 1
        q: queue.Queue = queue.Queue()
        orphan = self._orphan_responses.pop(req_id, None)
        self._responses[req_id] = q
        req = {"id": req_id, "method": method, "params": params}
        assert self.proc.stdin is not None
        self.proc.stdin.write(json.dumps(req) + "\n")
        self.proc.stdin.flush()
        if orphan is not None:
            q.put(orphan)
        return req_id, q

    def _notify(self, method: str, params: dict) -> None:
        req = {"method": method, "params": params}
        assert self.proc.stdin is not None
        self.proc.stdin.write(json.dumps(req) + "\n")
        self.proc.stdin.flush()

    def _send_response_error(self, req_id: int, code: int, message: str) -> None:
        resp = {"id": req_id, "error": {"code": code, "message": message}}
        assert self.proc.stdin is not None
        self.proc.stdin.write(json.dumps(resp) + "\n")
        self.proc.stdin.flush()

    def _initialize(self) -> None:
        req_id, q = self._send_request(
            "initialize",
            {
                "protocolVersion": 1,
                "capabilities": {},
                "clientInfo": {
                    "name": "telegram-codex-bot",
                    "title": "Telegram Codex Bot",
                    "version": "0.1.0",
                },
            },
        )
        msg = q.get(timeout=120)
        del self._responses[req_id]
        if "error" in msg:
            raise RuntimeError(msg["error"])
        self._notify("initialized", {})

    def thread_start(self) -> str:
        result = self._rpc("thread/start", {})
        thread = result.get("thread", {})
        return thread.get("id")

    def turn_start(self, thread_id: str, prompt: str) -> str:
        params = {
            "threadId": thread_id,
            "input": [{"type": "text", "text": prompt}],
            "approvalPolicy": APPROVAL_POLICY,
            "sandboxPolicy": {"type": SANDBOX_POLICY},
        }
        result = self._rpc("turn/start", params)
        turn = result.get("turn", {})
        turn_id = turn.get("id")
        if turn_id:
            self._active_turn_id = str(turn_id)
        return turn_id

    def wait_turn(self, thread_id: str, turn_id: str, timeout: int = 600) -> str:
        key = f"{thread_id}:{turn_id}"
        ev = self._turn_done.setdefault(key, threading.Event())
        ev.wait(timeout=timeout)
        return self._turn_text.get(key, "(no response)")


class State:
    def __init__(self, path: str):
        self.path = path
        self.threads: Dict[str, dict] = {}
        self.active: Dict[str, str] = {}
        self._load()

    def _load(self) -> None:
        if not os.path.exists(self.path):
            return
        try:
            with open(self.path, "r", encoding="utf-8") as f:
                data = json.load(f)
            self.threads = data.get("threads", {})
            self.active = data.get("active", {})
            # Backward compatibility: threads was {chat_id: thread_id}
            for chat_id, v in list(self.threads.items()):
                if isinstance(v, str):
                    self.threads[chat_id] = {"default": v}
        except Exception:
            self.threads = {}
            self.active = {}

    def save(self) -> None:
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump({"threads": self.threads, "active": self.active}, f, ensure_ascii=False, indent=2)



def main() -> None:
    state = State(STATE_PATH)
    codex = CodexAppServer(CODEX_CWD)

    offset = 0

    print("Bot started. Polling updates...", flush=True)
    while True:
        url = f"{API_BASE}/bot{TOKEN}/getUpdates?timeout=60&offset={offset}"
        _log(f"telegram poll offset={offset}", level="debug")
        try:
            data = _http_get_json(url)
        except Exception as e:
            _log(f"telegram poll error: {e}", level="error")
            time.sleep(2)
            continue
        if data.get("ok"):
            _log(f"telegram poll ok, updates={len(data.get('result', []))}", level="debug")
        if not data.get("ok"):
            time.sleep(1)
            continue
        for update in data.get("result", []):
            offset = update["update_id"] + 1
            message = update.get("message") or update.get("edited_message")
            if not message:
                continue
            chat_id = message["chat"]["id"]
            if ALLOWED_CHAT_IDS_SET is not None and chat_id not in ALLOWED_CHAT_IDS_SET:
                tg_send_message(chat_id, "이 봇은 허용된 사용자만 사용할 수 있습니다.")
                continue
            text = (message.get("text") or "").strip()
            if not text:
                continue

            _log(f"telegram inbound chat_id={chat_id} text={text!r}")

            if text == "/start":
                tg_send_message(chat_id, "Codex 로컬 봇입니다. 메시지를 보내면 로컬 Codex에 전달합니다. /new 로 새 대화.")
                continue
            if text == "/new":
                active = state.active.get(str(chat_id), "default")
                threads = state.threads.get(str(chat_id), {})
                threads.pop(active, None)
                state.threads[str(chat_id)] = threads
                state.save()
                tg_send_message(chat_id, f"세션 '{active}' 새 대화로 시작합니다.")
                continue
            if text == "/list":
                threads = state.threads.get(str(chat_id), {})
                if isinstance(threads, str):
                    threads = {"default": threads}
                    state.threads[str(chat_id)] = threads
                active = state.active.get(str(chat_id), "default")
                names = sorted(threads.keys())
                if not names:
                    tg_send_message(chat_id, "등록된 세션이 없습니다. /<이름> 으로 세션을 만들 수 있어요.")
                else:
                    lines = ["세션 목록:"] + [f"- {n}{' (active)' if n == active else ''}" for n in names]
                    tg_send_message(chat_id, "\n".join(lines))
                continue
            if text == "/session":
                active = state.active.get(str(chat_id), "default")
                tg_send_message(chat_id, f"현재 세션: {active}")
                continue

            if text.startswith("/") and len(text) > 1 and " " not in text:
                # Treat /이름 as session selector
                session_name = text[1:].split("@", 1)[0]
                state.active[str(chat_id)] = session_name
                threads = state.threads.get(str(chat_id), {})
                if isinstance(threads, str):
                    threads = {"default": threads}
                if session_name not in threads:
                    threads[session_name] = None
                    state.threads[str(chat_id)] = threads
                    state.save()
                    tg_send_message(chat_id, f"세션 '{session_name}' 생성 및 선택 완료.")
                else:
                    state.save()
                    tg_send_message(chat_id, f"세션 '{session_name}' 선택 완료.")
                continue

            active = state.active.get(str(chat_id), "default")
            threads = state.threads.get(str(chat_id), {})
            if isinstance(threads, str):
                threads = {"default": threads}
                state.threads[str(chat_id)] = threads
            thread_id = threads.get(active)
            if not thread_id:
                thread_id = codex.thread_start()
                if not thread_id:
                    tg_send_message(chat_id, "Codex 스레드 생성 실패. 터미널 로그를 확인하세요.")
                    continue
                threads[active] = thread_id
                state.threads[str(chat_id)] = threads
                state.active[str(chat_id)] = active
                state.save()

            tg_send_message(chat_id, "요청 처리 중…")
            prompt_to_send = text
            if active and active != "default":
                # Lightweight persona tag per session
                prompt_to_send = f"세션명은 '{active}'야. 너는 {active}로 답해. 질문: {text}"
            try:
                _log(f"codex turn/start thread_id={thread_id}")
                turn_id = codex.turn_start(thread_id, prompt_to_send)
                if not turn_id:
                    tg_send_message(chat_id, "Codex 턴 시작 실패. 터미널 로그를 확인하세요.")
                    continue
                meta_key = f"{thread_id}:{turn_id}"
                codex._turn_meta[meta_key] = {"chat_id": chat_id, "session": active}
                _log(f"codex turn_id={turn_id} waiting...")
                reply = codex.wait_turn(thread_id, turn_id)
                _log(f"codex reply len={len(reply)}")
            except Exception as e:
                err_text = str(e)
                _log(f"codex error: {err_text}")
                if "thread not found" in err_text:
                    _log("thread not found; creating new thread and retrying once")
                    try:
                        thread_id = codex.thread_start()
                        if not thread_id:
                            tg_send_message(chat_id, "Codex 스레드 재생성 실패.")
                            continue
                        threads = state.threads.get(str(chat_id), {})
                        if isinstance(threads, str):
                            threads = {"default": threads}
                        threads[active] = thread_id
                        state.threads[str(chat_id)] = threads
                        state.active[str(chat_id)] = active
                        state.save()
                        _log(f"codex retry turn/start thread_id={thread_id}")
                        turn_id = codex.turn_start(thread_id, prompt_to_send)
                        if not turn_id:
                            tg_send_message(chat_id, "Codex 턴 시작 실패.")
                            continue
                        meta_key = f"{thread_id}:{turn_id}"
                        codex._turn_meta[meta_key] = {"chat_id": chat_id, "session": active}
                        reply = codex.wait_turn(thread_id, turn_id)
                        _log(f"codex reply len={len(reply)}")
                    except Exception as e2:
                        reply = f"오류: {e2}"
                        _log(f"codex retry error: {e2}")
                else:
                    reply = f"오류: {e}"

            # Drop stale replies if session switched while this turn was running.
            meta_key = f"{thread_id}:{turn_id}" if "turn_id" in locals() else None
            meta = codex._turn_meta.pop(meta_key, None) if meta_key else None
            current_active = state.active.get(str(chat_id), "default")
            if meta and meta.get("session") != current_active:
                _log(f"stale reply dropped (session changed {meta.get('session')} -> {current_active})")
            else:
                if meta and meta.get("session") and meta.get("session") != "default":
                    reply = f"[{meta.get('session')}] {reply}"
                tg_send_message(chat_id, reply[:4000])


if __name__ == "__main__":
    main()
