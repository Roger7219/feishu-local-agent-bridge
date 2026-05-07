#!/usr/bin/env python3
from __future__ import annotations

import argparse
import collections
import datetime as dt
import hashlib
import json
import os
import re
import signal
import shutil
import subprocess
import sys
import threading
import time
import traceback
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Deque, Dict, List, Optional, Tuple

try:
    import yaml  # type: ignore
except Exception:  # pragma: no cover
    yaml = None

SENSITIVE_RE = re.compile(
    r"(?i)(access_token|refresh_token|app_secret|client_secret|secret|cookie|authorization|password|token)\s*[:=]\s*[^\s,;\]}]+"
)
WRITE_INTENT_RE = re.compile(
    r"(delete|drop|truncate|update|insert|grant|revoke|rm\s+-rf|curl\s+|python\s+|bash\s+|zsh\s+|shell|删除|修改|更新|授权|赋权|建表|创建任务|审批)",
    re.I,
)


class BridgeError(Exception):
    pass


@dataclass
class ToolResult:
    ok: bool
    text: str
    tool: str
    output_path: str = ""
    summary: Dict[str, Any] | None = None


class JsonAuditLogger:
    def __init__(self, log_dir: Path) -> None:
        self.log_dir = log_dir
        self.log_dir.mkdir(parents=True, exist_ok=True)

    def write(self, record: Dict[str, Any]) -> None:
        path = self.log_dir / f"bridge-{dt.datetime.now().strftime('%Y%m%d')}.jsonl"
        safe = redact_obj(record)
        with path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(safe, ensure_ascii=False, sort_keys=True) + "\n")


class SlidingWindowLimiter:
    def __init__(self) -> None:
        self._events: Dict[str, Deque[float]] = collections.defaultdict(collections.deque)

    def allow(self, key: str, limit: int, window_seconds: int) -> bool:
        if limit <= 0:
            return True
        now = time.time()
        q = self._events[key]
        cutoff = now - window_seconds
        while q and q[0] < cutoff:
            q.popleft()
        if len(q) >= limit:
            return False
        q.append(now)
        return True


def load_config(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise BridgeError(f"config not found: {path}")
    if yaml is None:
        raise BridgeError("PyYAML is required to read config.yaml")
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


def redact_text(text: str) -> str:
    return SENSITIVE_RE.sub(lambda m: m.group(1) + "=***REDACTED***", text)


def redact_obj(value: Any) -> Any:
    if isinstance(value, str):
        return redact_text(value)
    if isinstance(value, list):
        return [redact_obj(v) for v in value]
    if isinstance(value, dict):
        out = {}
        for k, v in value.items():
            if re.search(r"(?i)(secret|token|cookie|authorization|password)", str(k)):
                out[k] = "***REDACTED***"
            else:
                out[k] = redact_obj(v)
        return out
    return value


def run_command(args: List[str], timeout: int, cwd: Optional[Path] = None, env: Optional[Dict[str, str]] = None) -> subprocess.CompletedProcess[str]:
    # Never use shell=True; user messages are passed as plain arguments only.
    return subprocess.run(args, text=True, capture_output=True, timeout=timeout, cwd=str(cwd) if cwd else None, env=env)


def normalize_incoming_text(text: str, bot_name: str) -> str:
    clean = text.strip()
    clean = re.sub(r"@[_\-A-Za-z0-9\u4e00-\u9fff]+", " ", clean)
    if bot_name:
        clean = clean.replace(bot_name, " ")
    return re.sub(r"\s+", " ", clean).strip()


def today_output_dir(base: Path) -> Path:
    path = base / dt.datetime.now().strftime("%Y%m%d")
    path.mkdir(parents=True, exist_ok=True)
    return path


def maybe_spool(text: str, output_dir: Path, prefix: str, max_chars: int) -> Tuple[str, str]:
    if len(text) <= max_chars:
        return text, ""
    output_dir.mkdir(parents=True, exist_ok=True)
    digest = hashlib.md5(text.encode("utf-8")).hexdigest()[:10]
    path = output_dir / f"{prefix}_{dt.datetime.now().strftime('%H%M%S')}_{digest}.txt"
    path.write_text(text, encoding="utf-8")
    clipped = text[:max_chars] + f"\n\n...result is too long; full output saved to:\n{path}"
    return clipped, str(path)


def render_command(template: List[Any], query: str, event: Dict[str, Any]) -> List[str]:
    if not template:
        raise BridgeError("local agent command is not configured")
    values = {
        "query": query,
        "chat_id": str(event.get("chat_id") or ""),
        "sender_id": str(event.get("sender_id") or ""),
        "message_id": str(event.get("message_id") or event.get("event_id") or ""),
    }
    rendered = []
    for item in template:
        rendered.append(str(item).format(**values))
    return rendered


class LocalAgentTool:
    name = "local_agent"

    def __init__(self, cfg: Dict[str, Any], output_base: Path, timeout: int, max_reply_chars: int) -> None:
        self.cfg = cfg
        self.output_base = output_base
        self.timeout = timeout
        self.max_reply_chars = max_reply_chars

    def run(self, query: str, event: Dict[str, Any]) -> ToolResult:
        if not self.cfg.get("enabled", True):
            return ToolResult(False, "Local agent is disabled in config.yaml.", self.name)
        command = render_command(self.cfg.get("command") or [], query, event)
        cwd = Path(str(self.cfg.get("cwd") or ".")).expanduser()
        extra_env = {str(k): str(v) for k, v in (self.cfg.get("env") or {}).items()}
        env = os.environ.copy()
        env.update(extra_env)
        proc = run_command(command, timeout=self.timeout, cwd=cwd if str(cwd) else None, env=env)
        output = (proc.stdout or "").strip()
        error = (proc.stderr or "").strip()
        if proc.returncode != 0:
            detail = redact_text(error or output or f"local agent exited with code {proc.returncode}")
            raise BridgeError(detail[:2000])
        text = self.extract_reply(output)
        out_dir = today_output_dir(self.output_base)
        text, spooled = maybe_spool(redact_text(text), out_dir, "local_agent", self.max_reply_chars)
        return ToolResult(True, text or "Local agent completed with empty output.", self.name, output_path=spooled)

    @staticmethod
    def extract_reply(output: str) -> str:
        if not output:
            return ""
        try:
            data = json.loads(output)
        except json.JSONDecodeError:
            return output
        if isinstance(data, dict):
            for key in ("reply", "text", "message", "output", "result"):
                value = data.get(key)
                if isinstance(value, str):
                    return value
            return json.dumps(data, ensure_ascii=False, indent=2)
        return json.dumps(data, ensure_ascii=False, indent=2)


class Router:
    def __init__(self, cfg: Dict[str, Any]) -> None:
        self.cfg = cfg
        sec = cfg.get("security", {})
        output_base = Path(cfg.get("output", {}).get("dir", "./outputs")).expanduser()
        timeout = int(sec.get("command_timeout_seconds", 120))
        max_reply = int(sec.get("max_reply_chars", 3000))
        self.local_agent = LocalAgentTool(cfg.get("agent", {}), output_base, timeout, max_reply)

    def route(self, text: str) -> Tuple[str, Dict[str, str]]:
        clean = normalize_incoming_text(text, self.cfg.get("feishu", {}).get("bot_name", ""))
        if not clean or clean.lower() in {"帮助", "help", "/help"}:
            return "help", {}
        if self.cfg.get("security", {}).get("block_write_intents", False) and WRITE_INTENT_RE.search(clean):
            return "reject_write", {}
        return "local_agent", {"query": clean}

    def execute(self, text: str, event: Optional[Dict[str, Any]] = None) -> ToolResult:
        event = event or {}
        route, params = self.route(text)
        if route == "help":
            return ToolResult(True, help_text(), "help")
        if route == "reject_write":
            return ToolResult(False, "This bridge is configured to reject write-like requests from chat.", "security")
        return self.local_agent.run(params["query"], event)


def help_text(prefix: str = "") -> str:
    body = """Feishu Local Agent Bridge

Send a message to this bot and the bridge forwards it to the configured local agent command.

Admin setup:
1. Edit config.yaml -> agent.command.
2. Use {query} as the placeholder for the incoming chat text.
3. Keep command as an argument list; the bridge never runs shell=True.

Example command:
agent:
  command: ["python3", "/path/to/agent.py", "--query", "{query}"]"""
    return (prefix + "\n" + body).strip() if prefix else body


class Bridge:
    def __init__(self, cfg: Dict[str, Any], dry_run: bool = False) -> None:
        self.cfg = cfg
        self.dry_run = dry_run
        log_dir = Path(cfg.get("logging", {}).get("dir", "./logs")).expanduser()
        self.audit = JsonAuditLogger(log_dir)
        self.router = Router(cfg)
        self.limiter = SlidingWindowLimiter()
        self.proc: subprocess.Popen[str] | None = None
        self._stopping = False

    def is_allowed_event(self, event: Dict[str, Any]) -> Tuple[bool, str]:
        sec = self.cfg.get("security", {})
        sender = str(event.get("sender_id") or "")
        chat_id = str(event.get("chat_id") or "")
        chat_type = str(event.get("chat_type") or "")
        content = str(event.get("content") or "")
        if sender in set(sec.get("blocked_open_ids") or []):
            return False, "sender blocked"
        allowed_chats = set(sec.get("allowed_chat_ids") or [])
        if allowed_chats and chat_id not in allowed_chats:
            return False, "chat not allowed"
        if not self.limiter.allow(f"user:min:{sender}", int(sec.get("rate_limit_per_user_per_minute", 5)), 60):
            return False, "user minute rate limited"
        if not self.limiter.allow(f"user:hour:{sender}", int(sec.get("rate_limit_per_user_per_hour", 50)), 3600):
            return False, "user hour rate limited"
        if not self.limiter.allow(f"chat:min:{chat_id}", int(sec.get("rate_limit_per_chat_per_minute", 20)), 60):
            return False, "chat minute rate limited"
        if chat_type == "p2p":
            return True, "p2p"
        bot_name = str(self.cfg.get("feishu", {}).get("bot_name") or "")
        if bot_name and (f"@{bot_name}" in content or bot_name in content):
            return True, "mentioned"
        return False, "group message without mention"

    def handle_event(self, event: Dict[str, Any]) -> None:
        event_id = str(event.get("event_id") or event.get("message_id") or time.time())
        chat_id = str(event.get("chat_id") or "")
        content = str(event.get("content") or "")
        start = time.time()
        allowed, reason = self.is_allowed_event(event)
        record = {
            "ts": dt.datetime.now().isoformat(),
            "event_id": event_id,
            "chat_id": chat_id,
            "sender_open_id": event.get("sender_id"),
            "chat_type": event.get("chat_type"),
            "input_preview": content[:300],
            "allowed": allowed,
            "allow_reason": reason,
        }
        if not allowed:
            self.audit.write({**record, "status": "ignored"})
            return
        try:
            result = self.router.execute(content, event)
            elapsed = round(time.time() - start, 3)
            self.audit.write({**record, "status": "ok" if result.ok else "handled", "tool": result.tool, "elapsed": elapsed, "output_path": result.output_path, "summary": result.summary})
            self.reply(chat_id, result.text, event_id)
        except Exception as exc:
            elapsed = round(time.time() - start, 3)
            err = redact_text(str(exc))[:1200]
            self.audit.write({**record, "status": "error", "elapsed": elapsed, "error": err, "trace": traceback.format_exc()[-4000:]})
            self.reply(chat_id, f"Query failed: {err}", event_id)

    def reply(self, chat_id: str, text: str, event_id: str) -> None:
        if self.dry_run:
            print(json.dumps({"dry_run_reply": {"chat_id": chat_id, "text": text}}, ensure_ascii=False))
            return
        send_as = str(self.cfg.get("feishu", {}).get("send_as", "user"))
        idem = hashlib.md5(f"{event_id}:{text}".encode("utf-8")).hexdigest()
        args = ["lark-cli", "im", "+messages-send", "--as", send_as, "--chat-id", chat_id, "--text", text, "--idempotency-key", idem]
        proc = run_command(args, timeout=60)
        if proc.returncode != 0:
            if send_as == "bot":
                fallback = ["lark-cli", "im", "+messages-send", "--as", "user", "--chat-id", chat_id, "--text", text, "--idempotency-key", idem]
                proc2 = run_command(fallback, timeout=60)
                if proc2.returncode == 0:
                    return
                raise BridgeError(redact_text((proc2.stderr or proc2.stdout or "reply failed").strip()))
            raise BridgeError(redact_text((proc.stderr or proc.stdout or "reply failed").strip()))

    def start(self) -> int:
        event_key = str(self.cfg.get("feishu", {}).get("event_key") or "im.message.receive_v1")
        event_as = str(self.cfg.get("feishu", {}).get("event_as") or "bot")
        args = ["lark-cli", "event", "consume", event_key, "--as", event_as]
        while not self._stopping:
            self.proc = subprocess.Popen(args, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, bufsize=1)
            assert self.proc.stdout is not None
            print(f"[bridge] consuming {event_key} as {event_as}; pid={self.proc.pid}", file=sys.stderr, flush=True)
            if self.proc.stderr is not None:
                threading.Thread(target=self._drain_stderr, args=(self.proc.stderr,), daemon=True).start()
            threading.Thread(target=self._stdin_heartbeat, args=(self.proc,), daemon=True).start()
            for line in self.proc.stdout:
                line = line.strip()
                if not line:
                    continue
                try:
                    event = json.loads(line)
                except json.JSONDecodeError:
                    print(f"[bridge] skip non-json line: {redact_text(line)[:300]}", file=sys.stderr, flush=True)
                    continue
                self.handle_event(event)
            rc = self.proc.wait()
            print(f"[bridge] event consumer exited rc={rc}; stopping={self._stopping}", file=sys.stderr, flush=True)
            if not self._stopping:
                time.sleep(3)
        return 0

    def _drain_stderr(self, stream: Any) -> None:
        for line in stream:
            print(f"[lark-event] {redact_text(line.rstrip())}", file=sys.stderr, flush=True)

    def _stdin_heartbeat(self, proc: subprocess.Popen[str]) -> None:
        while not self._stopping and proc.poll() is None:
            time.sleep(15)

    def stop(self) -> None:
        self._stopping = True
        if self.proc and self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.proc.kill()


def preflight_check(cfg: Dict[str, Any]) -> int:
    checks = []
    doctor = run_command(["lark-cli", "doctor"], timeout=30)
    checks.append({"name": "lark-cli doctor", "ok": doctor.returncode == 0, "detail": redact_text((doctor.stdout or doctor.stderr)[-1200:])})
    event_key = str(cfg.get("feishu", {}).get("event_key") or "im.message.receive_v1")
    schema = run_command(["lark-cli", "event", "schema", event_key, "--json"], timeout=30)
    checks.append({"name": f"event schema {event_key}", "ok": schema.returncode == 0, "detail": redact_text((schema.stdout or schema.stderr)[-1200:])})
    command = [str(x) for x in (cfg.get("agent", {}).get("command") or [])]
    executable = command[0] if command else ""
    executable_ok = bool(executable and (Path(executable).expanduser().exists() or shutil.which(executable)))
    checks.append({"name": "local agent command", "ok": executable_ok, "detail": executable or "not configured"})
    output_dir = Path(str(cfg.get("output", {}).get("dir", "./outputs"))).expanduser()
    output_dir.mkdir(parents=True, exist_ok=True)
    checks.append({"name": "output dir writable", "ok": os.access(output_dir, os.W_OK), "detail": str(output_dir)})
    print(json.dumps({"ok": all(c["ok"] for c in checks), "checks": checks}, ensure_ascii=False, indent=2))
    return 0 if all(c["ok"] for c in checks) else 1


def main() -> int:
    parser = argparse.ArgumentParser(description="Feishu local Agent Bridge")
    parser.add_argument("--config", default="config.yaml")
    parser.add_argument("--dry-run", action="store_true", help="print replies instead of sending to Feishu")
    parser.add_argument("--check", action="store_true", help="run local preflight checks and exit")
    parser.add_argument("--once-event-json", default="", help="handle one event JSON and exit; useful for tests")
    args = parser.parse_args()
    cfg = load_config(Path(args.config).expanduser())
    if args.check:
        return preflight_check(cfg)
    bridge = Bridge(cfg, dry_run=args.dry_run)

    def _handle_signal(_signum: int, _frame: Any) -> None:
        bridge.stop()
        raise SystemExit(0)

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)
    if args.once_event_json:
        bridge.handle_event(json.loads(args.once_event_json))
        return 0
    return bridge.start()


if __name__ == "__main__":
    raise SystemExit(main())
