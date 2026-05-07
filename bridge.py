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
import subprocess
import sys
import threading
import time
import traceback
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Deque, Dict, Iterable, List, Optional, Tuple

try:
    import yaml  # type: ignore
except Exception:  # pragma: no cover
    yaml = None

TABLE_RE = re.compile(r"^[A-Za-z0-9_.]+$")
TABLE_TOKEN_RE = re.compile(r"\b[A-Za-z][A-Za-z0-9_]*(?:\.[A-Za-z][A-Za-z0-9_]*)?\b")
SENSITIVE_RE = re.compile(
    r"(?i)(DP_SESSION_ID|TGC|dunCookie|access_token|refresh_token|app_secret|cookie|authorization|token)\s*[:=]\s*[^\s,;\]}]+"
)
WRITE_INTENT_RE = re.compile(r"(建表|创建任务|授权|赋权|删除|修改|更新|上线|审批|发消息|执行\s*shell|shell|rm\s+-rf|curl\s+|python\s+|bash\s+|zsh\s+)", re.I)


class BridgeError(Exception):
    pass


@dataclass
class ToolResult:
    ok: bool
    text: str
    tool: str
    output_path: str = ""
    raw_summary: Dict[str, Any] | None = None


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
    text = path.read_text(encoding="utf-8")
    if yaml is not None:
        return yaml.safe_load(text) or {}
    raise BridgeError("PyYAML is required to read config.yaml")


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
            if re.search(r"(?i)(secret|token|cookie|authorization|password|tgc)", str(k)):
                out[k] = "***REDACTED***"
            else:
                out[k] = redact_obj(v)
        return out
    return value


def run_command(args: List[str], timeout: int, cwd: Optional[Path] = None) -> subprocess.CompletedProcess[str]:
    # Never use shell=True; all external commands are argument arrays.
    return subprocess.run(args, text=True, capture_output=True, timeout=timeout, cwd=str(cwd) if cwd else None)


def parse_json_from_stdout(stdout: str) -> Any:
    text = stdout.strip()
    if not text:
        raise BridgeError("empty command output")
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass
    # Some tools print logs before/after JSON. Decode the last JSON object/array.
    decoder = json.JSONDecoder()
    candidates = []
    for idx, ch in enumerate(text):
        if ch in "[{":
            try:
                obj, end = decoder.raw_decode(text[idx:])
                if text[idx + end :].strip() == "":
                    candidates.append(obj)
            except json.JSONDecodeError:
                continue
    if candidates:
        return candidates[-1]
    raise BridgeError("command output is not JSON")


def safe_table_name(value: str) -> str:
    table = value.strip().strip("`，。,. ")
    if not TABLE_RE.fullmatch(table):
        raise BridgeError("表名只允许字母、数字、下划线和点号")
    if "." in table:
        db, tbl = table.split(".", 1)
        if not db or not tbl or "." in tbl:
            raise BridgeError("表名格式应为 table 或 db.table")
    return table


def extract_table_token(text: str, prefer_raw: bool = False) -> str:
    tokens = [m.group(0) for m in TABLE_TOKEN_RE.finditer(text)]
    if prefer_raw:
        for token in tokens:
            token_lower = token.lower()
            if "raw" in token_lower and token_lower not in {"raw", "raw表"}:
                return safe_table_name(token)
    # Prefer db.table, then any non-keyword token.
    for token in tokens:
        if "." in token:
            return safe_table_name(token)
    stop = {"raw", "help", "agent", "select", "from", "where"}
    for token in tokens:
        if token.lower() not in stop:
            return safe_table_name(token)
    raise BridgeError("没有识别到合法表名")


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
    clipped = text[:max_chars] + f"\n\n...结果过长，完整内容已保存到：\n{path}"
    return clipped, str(path)


class RawModelLineageTool:
    name = "raw_model_lineage"

    def __init__(self, cfg: Dict[str, Any], output_base: Path, timeout: int, max_reply_chars: int) -> None:
        self.cfg = cfg
        self.output_base = output_base
        self.timeout = timeout
        self.max_reply_chars = max_reply_chars

    def run(self, raw_table: str) -> ToolResult:
        script = Path(str(self.cfg.get("script", ""))).expanduser()
        if not script.exists():
            raise BridgeError(f"raw lineage script not found: {script}")
        table = safe_table_name(raw_table)
        out_dir = today_output_dir(self.output_base)
        excel_path = out_dir / f"raw_model_lineage_{table.replace('.', '_')}.xlsx"
        args = [
            sys.executable,
            str(script),
            "--raw-table",
            table,
            "--limit",
            str(int(self.cfg.get("limit", 10))),
            "--excel-out",
            str(excel_path),
            "--json",
        ]
        proc = run_command(args, timeout=self.timeout)
        if proc.returncode != 0:
            raise BridgeError(redact_text((proc.stderr or proc.stdout or "查询失败").strip())[:1200])
        data = parse_json_from_stdout(proc.stdout)
        text = self.format_result(table, data, excel_path)
        text, spooled = maybe_spool(text, out_dir, "raw_model_lineage", self.max_reply_chars)
        return ToolResult(ok=True, text=text, tool=self.name, output_path=spooled or str(excel_path), raw_summary=summarize_json(data))

    @staticmethod
    def format_result(table: str, data: Any, excel_path: Path) -> str:
        if not isinstance(data, dict):
            return f"raw 表查询完成：{table}\n结果：{json.dumps(data, ensure_ascii=False)[:2000]}\nExcel：{excel_path}"
        status = data.get("status") or data.get("状态") or ""
        reason = data.get("reason") or data.get("message") or ""
        candidates = data.get("candidates") or data.get("selected") or data.get("results") or []
        if not candidates and isinstance(data.get("data"), dict):
            candidates = data["data"].get("candidates") or []
        lines = [f"raw 表：{table}"]
        if status:
            lines.append(f"状态：{status}")
        if reason:
            lines.append(f"说明：{reason}")
        if candidates:
            lines.append("候选模型表：")
            for idx, item in enumerate(candidates[:10], 1):
                if not isinstance(item, dict):
                    lines.append(f"{idx}. {item}")
                    continue
                name = item.get("名称") or item.get("候选模型表") or item.get("table") or item.get("model_table") or ""
                prob = item.get("probability") or item.get("概率") or item.get("score") or item.get("分数") or ""
                owner = item.get("表负责人") or item.get("owner") or ""
                task_owner = item.get("任务负责人") or ""
                reason_i = item.get("reason") or item.get("理由") or ""
                extra = "，".join(str(x) for x in [f"概率/分数:{prob}" if prob != "" else "", f"表负责人:{owner}" if owner else "", f"任务负责人:{task_owner}" if task_owner else ""] if x)
                lines.append(f"{idx}. {name}" + (f"（{extra}）" if extra else ""))
                if reason_i:
                    lines.append(f"   理由：{reason_i[:160]}")
        else:
            lines.append("暂无符合条件的候选模型表。")
        lines.append(f"结果 Excel：{excel_path}")
        return "\n".join(lines)


class TableMetadataTool:
    name = "table_metadata"

    def __init__(self, cfg: Dict[str, Any], output_base: Path, timeout: int, max_reply_chars: int) -> None:
        self.cfg = cfg
        self.opencli_site = str(cfg.get("opencli_site") or "data-map")
        self.output_base = output_base
        self.timeout = timeout
        self.max_reply_chars = max_reply_chars

    def run(self, table: str) -> ToolResult:
        table = safe_table_name(table)
        search = run_command(["opencli", self.opencli_site, "table-search", "--keyword", table, "-f", "json"], timeout=self.timeout)
        if search.returncode != 0:
            raise BridgeError(redact_text((search.stderr or search.stdout or "表搜索失败").strip())[:1200])
        search_data = parse_json_from_stdout(search.stdout)
        table_id, full_name, candidates = self.pick_table(search_data, table)
        if not table_id:
            text = "未唯一定位到表。候选：\n" + "\n".join(candidates[:10])
            return ToolResult(ok=False, text=text, tool=self.name, raw_summary={"candidates": candidates[:10]})
        info = run_command(["opencli", self.opencli_site, "map-table", "--table-id", str(table_id), "-f", "json"], timeout=self.timeout)
        cols = run_command(["opencli", self.opencli_site, "map-columns", "--table-id", str(table_id), "--limit", "200", "-f", "json"], timeout=self.timeout)
        if info.returncode != 0:
            raise BridgeError(redact_text((info.stderr or info.stdout or "表详情查询失败").strip())[:1200])
        if cols.returncode != 0:
            raise BridgeError(redact_text((cols.stderr or cols.stdout or "字段查询失败").strip())[:1200])
        info_data = parse_json_from_stdout(info.stdout)
        cols_data = parse_json_from_stdout(cols.stdout)
        text = self.format_result(table_id, full_name, info_data, cols_data)
        out_dir = today_output_dir(self.output_base)
        text, spooled = maybe_spool(text, out_dir, "table_metadata", self.max_reply_chars)
        return ToolResult(ok=True, text=text, tool=self.name, output_path=spooled, raw_summary={"table_id": table_id, "full_name": full_name})

    @staticmethod
    def rows(data: Any) -> List[Dict[str, Any]]:
        if isinstance(data, list):
            return [x for x in data if isinstance(x, dict)]
        if isinstance(data, dict):
            for key in ("data", "rows", "result", "items"):
                val = data.get(key)
                if isinstance(val, list):
                    return [x for x in val if isinstance(x, dict)]
                if isinstance(val, dict):
                    nested = TableMetadataTool.rows(val)
                    if nested:
                        return nested
        return []

    def pick_table(self, data: Any, query: str) -> Tuple[str, str, List[str]]:
        rows = self.rows(data)
        candidates = []
        exact = []
        q_tail = query.split(".")[-1].lower()
        q_full = query.lower()
        for row in rows:
            tid = str(row.get("table_id") or row.get("id") or "").strip()
            name = str(row.get("库表名") or row.get("table_name") or row.get("display_name") or row.get("name") or "").strip()
            if not name and row.get("db_name") and row.get("table_name"):
                name = f"{row.get('db_name')}.{row.get('table_name')}"
            if tid or name:
                candidates.append(f"{tid} {name}".strip())
            tail = name.split(".")[-1].lower()
            if tid and (name.lower() == q_full or tail == q_tail):
                exact.append((tid, name))
        if len(exact) == 1:
            return exact[0][0], exact[0][1], candidates
        return "", "", candidates

    @staticmethod
    def format_result(table_id: str, full_name: str, info_data: Any, cols_data: Any) -> str:
        info_rows = TableMetadataTool.rows(info_data)
        col_rows = TableMetadataTool.rows(cols_data)
        info = info_rows[0] if info_rows else {}
        lines = [f"表：{full_name}", f"table_id：{table_id}"]
        for label in ["说明", "负责人", "表层级", "业务域", "主题域", "更新周期", "组织"]:
            if info.get(label):
                lines.append(f"{label}：{info.get(label)}")
        if col_rows:
            lines.append("字段：")
            for col in col_rows[:80]:
                name = col.get("字段名") or col.get("name") or ""
                typ = col.get("字段类型") or col.get("type") or ""
                comment = col.get("注释") or col.get("comment") or ""
                part = col.get("是否分区") or ""
                lines.append(f"- {name} {typ} {comment}" + (f" 分区:{part}" if part else ""))
            if len(col_rows) > 80:
                lines.append(f"... 共 {len(col_rows)} 个字段，仅展示前 80 个")
        return "\n".join(lines)


class SourceRawCheckTool:
    name = "source_raw_check"

    def __init__(self, cfg: Dict[str, Any], timeout: int) -> None:
        self.cfg = cfg
        self.opencli_site = str(cfg.get("opencli_site") or "data-map")
        self.timeout = timeout

    def run(self, source_table: str) -> ToolResult:
        source_table = safe_table_name(source_table)
        if "." not in source_table:
            raise BridgeError("查询源表是否接入 raw 时，请提供源库.源表，例如 db.table")
        db, table = source_table.split(".", 1)
        candidates = [
            f"raw_mysql_{db}_{table}_full_1d",
            f"raw_tdb_{db}_{table}_full_1d",
        ]
        hits: List[Dict[str, str]] = []
        diagnostics = []
        checked_tasks: List[Dict[str, str]] = []
        for candidate in candidates:
            hits.extend(self.search_raw_tables(candidate, exact_tail=True, diagnostics=diagnostics))

        # Broad search: many raw tables include business infixes between source DB and table names.
        broad_rows = self.search_raw_tables(table, exact_tail=False, diagnostics=diagnostics)
        for row in broad_rows:
            raw_tail = row.get("raw_table", "").split(".")[-1]
            if self.looks_like_raw_for_source(raw_tail, db, table):
                hits.append(row)

        # 任务校验：搜索抽数任务，读取 detail，确认 reader 源表等于输入源表。
        task_rows = self.search_tasks(table, diagnostics)
        for task in task_rows:
            scheduler_id = task.get("id", "")
            detail = self.fetch_task_detail(scheduler_id, diagnostics)
            if not detail:
                continue
            source_tables, target_tables = self.extract_exchange_tables(detail)
            checked_tasks.append({
                "scheduler_id": scheduler_id,
                "task_name": task.get("任务名称", ""),
                "source_tables": ",".join(source_tables),
                "target_tables": ",".join(target_tables),
            })
            if source_table in source_tables:
                for target in target_tables:
                    if target and "hive" not in target.lower():
                        hits.append({
                            "raw_table": target,
                            "table_id": "",
                            "scheduler_id": scheduler_id,
                            "task_name": task.get("任务名称", ""),
                            "verified_source": source_table,
                        })

        hits = self.dedupe_hits(hits)
        lines = [f"源表：{source_table}"]
        if hits:
            lines.append("已找到接入的 Hive/raw 表：")
            for hit in hits:
                extras = []
                if hit.get("table_id"):
                    extras.append(f"table_id={hit['table_id']}")
                if hit.get("scheduler_id"):
                    extras.append(f"任务ID={hit['scheduler_id']}")
                if hit.get("verified_source"):
                    extras.append("已校验任务源表")
                suffix = f"（{'，'.join(extras)}）" if extras else ""
                lines.append(f"- {hit['raw_table']}{suffix}")
        else:
            lines.append("未找到已校验源表的 Hive/raw 接入结果。")
            lines.append("检查过的候选：")
            lines.extend(f"- {x}" for x in candidates)
        if checked_tasks:
            lines.append("检查过的抽数任务：")
            for task in checked_tasks[:5]:
                lines.append(f"- {task['scheduler_id']} {task['task_name']}")
                lines.append(f"  源表：{task['source_tables'] or '-'}")
                lines.append(f"  目标：{task['target_tables'] or '-'}")
        if diagnostics:
            lines.append("诊断信息：")
            lines.extend(f"- {x}" for x in diagnostics[:5])
        return ToolResult(ok=bool(hits), text="\n".join(lines), tool=self.name, raw_summary={"hits": len(hits), "candidates": candidates})

    def search_raw_tables(self, keyword: str, exact_tail: bool, diagnostics: List[str]) -> List[Dict[str, str]]:
        proc = run_command(["opencli", self.opencli_site, "table-search", "--keyword", keyword, "--limit", "20", "-f", "json"], timeout=self.timeout)
        if proc.returncode != 0:
            diagnostics.append(f"{keyword}: 数据地图查询失败 {redact_text((proc.stderr or proc.stdout).strip())[:300]}")
            return []
        try:
            rows = TableMetadataTool.rows(parse_json_from_stdout(proc.stdout))
        except Exception as exc:
            diagnostics.append(f"{keyword}: 数据地图结果解析失败 {exc}")
            return []
        out = []
        for row in rows:
            name = str(row.get("库表名") or row.get("table_name") or row.get("display_name") or row.get("name") or "").strip()
            if not name and row.get("db_name") and row.get("table_name"):
                name = f"{row.get('db_name')}.{row.get('table_name')}"
            tail = name.split(".")[-1]
            table_id = str(row.get("table_id") or row.get("id") or "").strip()
            if not tail.startswith(("raw_", "ods_", "ods")):
                continue
            if exact_tail and tail != keyword:
                continue
            out.append({"raw_table": name or tail, "table_id": table_id})
        return out

    def search_tasks(self, keyword: str, diagnostics: List[str]) -> List[Dict[str, str]]:
        proc = run_command(["opencli", self.opencli_site, "task-info", "--task-name", keyword, "--limit", "20", "-f", "json"], timeout=self.timeout)
        if proc.returncode != 0:
            diagnostics.append(f"{keyword}: 任务查询失败 {redact_text((proc.stderr or proc.stdout).strip())[:300]}")
            return []
        try:
            rows = TableMetadataTool.rows(parse_json_from_stdout(proc.stdout))
        except Exception as exc:
            diagnostics.append(f"{keyword}: 任务结果解析失败 {exc}")
            return []
        return [{k: str(v or "") for k, v in row.items()} for row in rows]

    def fetch_task_detail(self, scheduler_id: str, diagnostics: List[str]) -> Dict[str, Any]:
        if not scheduler_id:
            return {}
        url_template = str(self.cfg.get("task_detail_url_template") or "")
        if not url_template:
            diagnostics.append(f"{scheduler_id}: 未配置 task_detail_url_template，无法校验任务 detail")
            return {}
        cookie_path = str(self.cfg.get("cookie_file") or os.environ.get("FEISHU_BRIDGE_DP_COOKIE_FILE") or "")
        if not cookie_path:
            diagnostics.append(f"{scheduler_id}: 未配置 cookie_file，无法校验任务 detail")
            return {}
        cookie_file = Path(cookie_path).expanduser()
        if not cookie_file.exists():
            diagnostics.append(f"{scheduler_id}: 缺少 cookie 文件，无法校验任务 detail")
            return {}
        cookie = cookie_file.read_text(encoding="utf-8").strip()
        encoded_id = urllib.parse.quote(str(scheduler_id))
        url = url_template.format(scheduler_id=encoded_id)
        headers = {
            "Cookie": cookie,
            "Accept": "application/json",
            "User-Agent": "Mozilla/5.0",
        }
        referer_template = str(self.cfg.get("task_detail_referer_template") or "")
        if referer_template:
            headers["Referer"] = referer_template.format(scheduler_id=encoded_id)
        req = urllib.request.Request(url, headers=headers)
        try:
            raw = urllib.request.urlopen(req, timeout=self.timeout).read().decode("utf-8")
            body = json.loads(raw)
        except Exception as exc:
            diagnostics.append(f"{scheduler_id}: 任务 detail 查询失败 {exc}")
            return {}
        if body.get("status") != "success" or body.get("code") != 200:
            diagnostics.append(f"{scheduler_id}: 任务 detail 返回异常 {str(body)[:300]}")
            return {}
        return body.get("data") or {}

    @staticmethod
    def extract_exchange_tables(detail: Dict[str, Any]) -> Tuple[List[str], List[str]]:
        raw_detail = detail.get("detail") or "{}"
        try:
            payload = json.loads(raw_detail) if isinstance(raw_detail, str) else raw_detail
        except Exception:
            return [], []
        sources: List[str] = []
        targets: List[str] = []
        for step in payload.get("steps") or []:
            if not isinstance(step, dict):
                continue
            category = step.get("category")
            param = step.get("parameter") or {}
            if category == "reader":
                for conn in param.get("connection") or []:
                    for tbl in conn.get("table") or []:
                        if isinstance(tbl, str):
                            sources.append(tbl)
            if category == "writer":
                tbl = param.get("table")
                if isinstance(tbl, str):
                    targets.append(tbl)
        return sorted(set(sources)), sorted(set(targets))

    @staticmethod
    def looks_like_raw_for_source(raw_tail: str, db: str, table: str) -> bool:
        text = raw_tail.lower()
        if not text.startswith(("raw_", "ods_")):
            return False
        table_tokens = [t for t in table.lower().split("_") if len(t) >= 2]
        # Require all meaningful source table tokens; db token can be partially represented with business infix.
        return all(token in text for token in table_tokens)

    @staticmethod
    def dedupe_hits(hits: List[Dict[str, str]]) -> List[Dict[str, str]]:
        merged: Dict[str, Dict[str, str]] = {}
        for hit in hits:
            key = hit.get("raw_table", "")
            if not key:
                continue
            current = merged.setdefault(key, dict(hit))
            for k, v in hit.items():
                if v and not current.get(k):
                    current[k] = v
                if k == "verified_source" and v:
                    current[k] = v
        return list(merged.values())


def summarize_json(data: Any) -> Dict[str, Any]:
    if isinstance(data, dict):
        return {k: (len(v) if isinstance(v, list) else v) for k, v in data.items() if k in {"status", "reason", "message", "candidates", "results"}}
    return {"type": type(data).__name__}


class Router:
    def __init__(self, cfg: Dict[str, Any]) -> None:
        self.cfg = cfg
        sec = cfg.get("security", {})
        output_base = Path(cfg.get("output", {}).get("dir", "./outputs")).expanduser()
        timeout = int(sec.get("command_timeout_seconds", 120))
        max_reply = int(sec.get("max_reply_chars", 3000))
        self.raw_tool = RawModelLineageTool(cfg.get("tools", {}).get("raw_model_lineage", {}), output_base, timeout, max_reply)
        self.source_raw_tool = SourceRawCheckTool(cfg.get("tools", {}).get("source_raw_check", {}), timeout)
        self.meta_tool = TableMetadataTool(cfg.get("tools", {}).get("table_metadata", {}), output_base, timeout, max_reply)

    def route(self, text: str) -> Tuple[str, Dict[str, str]]:
        clean = normalize_incoming_text(text, self.cfg.get("feishu", {}).get("bot_name", ""))
        if not clean or clean in {"帮助", "help", "/help"}:
            return "help", {}
        if WRITE_INTENT_RE.search(clean):
            return "reject_write", {}
        if "接入" in clean and any(k in clean.lower() for k in ["raw", "hive", "ods"]):
            table = extract_table_token(clean, prefer_raw=False)
            return "source_raw_check", {"source_table": table}
        if "raw表" in clean and "模型" not in clean:
            table = extract_table_token(clean, prefer_raw=False)
            return "source_raw_check", {"source_table": table}
        if any(k in clean for k in ["模型表", "对应模型", "血缘"]) or "raw" in clean.lower():
            table = extract_table_token(clean, prefer_raw=True)
            return "raw_model_lineage", {"raw_table": table}
        if any(k in clean for k in ["元数据", "负责人", "字段", "业务域", "主题域", "表描述", "表说明"]):
            table = extract_table_token(clean)
            return "table_metadata", {"table": table}
        if any(k in clean for k in ["查", "查询", "看下", "看看", "是否", "有没有"]):
            table = extract_table_token(clean)
            return "table_metadata", {"table": table}
        return "unknown", {}

    def execute(self, text: str) -> ToolResult:
        route, params = self.route(text)
        if route == "help":
            return ToolResult(True, help_text(), "help")
        if route == "reject_write":
            return ToolResult(False, "为保护本机安全，飞书入口第一版只支持只读查询，不执行建表、授权、删除、修改、shell 等写操作。", "security")
        if route == "raw_model_lineage":
            if not self.cfg.get("tools", {}).get("raw_model_lineage", {}).get("enabled", True):
                return ToolResult(False, "raw 模型血缘工具未启用。", route)
            return self.raw_tool.run(params["raw_table"])
        if route == "source_raw_check":
            if not self.cfg.get("tools", {}).get("source_raw_check", {}).get("enabled", True):
                return ToolResult(False, "源表 raw 接入检查工具未启用。", route)
            return self.source_raw_tool.run(params["source_table"])
        if route == "table_metadata":
            if not self.cfg.get("tools", {}).get("table_metadata", {}).get("enabled", True):
                return ToolResult(False, "表元数据工具未启用。", route)
            return self.meta_tool.run(params["table"])
        return ToolResult(False, help_text(prefix="暂不支持这个问题。"), "unknown")


def normalize_incoming_text(text: str, bot_name: str) -> str:
    clean = text.strip()
    clean = re.sub(r"@[_\-A-Za-z0-9\u4e00-\u9fff]+", " ", clean)
    if bot_name:
        clean = clean.replace(bot_name, " ")
    return re.sub(r"\s+", " ", clean).strip()


def help_text(prefix: str = "") -> str:
    body = """本地 Agent Bridge 支持查询类问题：
1. 查 raw 表对应模型表：查 raw_xxx 对应模型表
2. 查源表是否接入 raw/hive：db.table 是否有接入raw表 / hive表
3. 查表元数据/字段/负责人：查表 db.table 元数据
4. 其它带表名的查询：会优先返回该表数据地图元信息

安全限制：不执行 shell，不做建表/授权/删除/修改/发消息等写操作。"""
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
            result = self.router.execute(content)
            elapsed = round(time.time() - start, 3)
            self.audit.write({**record, "status": "ok" if result.ok else "handled", "tool": result.tool, "elapsed": elapsed, "output_path": result.output_path, "summary": result.raw_summary})
            self.reply(chat_id, result.text, event_id)
        except Exception as exc:
            elapsed = round(time.time() - start, 3)
            err = redact_text(str(exc))[:1200]
            self.audit.write({**record, "status": "error", "elapsed": elapsed, "error": err, "trace": traceback.format_exc()[-4000:]})
            self.reply(chat_id, f"查询失败：{err}", event_id)

    def reply(self, chat_id: str, text: str, event_id: str) -> None:
        if self.dry_run:
            print(json.dumps({"dry_run_reply": {"chat_id": chat_id, "text": text}}, ensure_ascii=False))
            return
        send_as = str(self.cfg.get("feishu", {}).get("send_as", "user"))
        idem = hashlib.md5(f"{event_id}:{text}".encode("utf-8")).hexdigest()
        args = ["lark-cli", "im", "+messages-send", "--as", send_as, "--chat-id", chat_id, "--text", text, "--idempotency-key", idem]
        proc = run_command(args, timeout=60)
        if proc.returncode != 0:
            # If bot identity is not ready, fall back to user only when explicitly enabled by config/default.
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
            # lark-cli event consume exits when stdin closes, so keep a pipe open and heartbeat it.
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
    raw_script = Path(str(cfg.get("tools", {}).get("raw_model_lineage", {}).get("script", ""))).expanduser()
    checks.append({"name": "raw_model_lineage script", "ok": raw_script.exists(), "detail": str(raw_script)})
    output_dir = Path(str(cfg.get("output", {}).get("dir", "./outputs"))).expanduser()
    output_dir.mkdir(parents=True, exist_ok=True)
    checks.append({"name": "output dir writable", "ok": os.access(output_dir, os.W_OK), "detail": str(output_dir)})
    print(json.dumps({"ok": all(c["ok"] for c in checks), "checks": checks}, ensure_ascii=False, indent=2))
    return 0 if all(c["ok"] for c in checks) else 1


def main() -> int:
    parser = argparse.ArgumentParser(description="Feishu local readonly Agent Bridge")
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
