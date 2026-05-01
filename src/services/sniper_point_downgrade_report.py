# -*- coding: utf-8 -*-
"""Summaries for protectively downgraded sniper/action levels."""

from __future__ import annotations

import json
from collections import Counter
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DEFAULT_AUDIT_PATH = PROJECT_ROOT / "reports" / "sniper_point_downgrade_audit.jsonl"
DEFAULT_REPORT_PATH = PROJECT_ROOT / "reports" / "sniper_point_downgrade_summary.md"

FIELD_LABELS = {
    "all": "整组点位",
    "ideal_buy": "买入/接回区",
    "secondary_buy": "加仓/转强线",
    "stop_loss": "止损/防守线",
    "take_profit": "目标/反抽线",
}
EVENT_LABELS = {
    "sniper_point_context_mismatch": "行情上下文不一致",
    "sniper_point_protective_downgrade": "点位偏离保护",
}


def build_sniper_point_downgrade_summary(
    audit_path: str | Path = DEFAULT_AUDIT_PATH,
    *,
    max_recent_rows: int = 20,
) -> dict[str, Any]:
    """Build a compact dashboard from the local downgrade audit ledger."""
    path = Path(audit_path)
    rows = _read_jsonl(path)
    events = [row for row in rows if row.get("event") in EVENT_LABELS]
    events.sort(key=lambda row: str(row.get("created_at") or ""), reverse=True)

    field_counts = Counter(str(row.get("field") or "unknown") for row in events)
    event_counts = Counter(str(row.get("event") or "unknown") for row in events)
    source_counts = Counter(_source(row) for row in events)
    symbol_counts = Counter(_symbol_label(row) for row in events)

    return {
        "audit_path": str(path),
        "audit_exists": path.exists(),
        "total_events": len(events),
        "unique_symbols": len(symbol_counts),
        "latest_created_at": events[0].get("created_at") if events else None,
        "by_event": _counter_rows(event_counts, labeler=lambda key: EVENT_LABELS.get(key, key)),
        "by_field": _counter_rows(field_counts, labeler=lambda key: FIELD_LABELS.get(key, key)),
        "by_source": _counter_rows(source_counts),
        "by_symbol": _counter_rows(symbol_counts),
        "recent_events": [_recent_event(row) for row in events[: max(0, max_recent_rows)]],
    }


def render_sniper_point_downgrade_summary(summary: dict[str, Any]) -> str:
    """Render a human-readable Markdown report."""
    lines = [
        "# 狙击点位保护降级摘要",
        "",
        f"- 审计文件: `{summary.get('audit_path')}`",
        f"- 总拦截次数: {summary.get('total_events', 0)}",
        f"- 涉及标的数: {summary.get('unique_symbols', 0)}",
        f"- 最近拦截时间: {summary.get('latest_created_at') or '暂无'}",
    ]

    if not summary.get("audit_exists"):
        lines.extend(["", "> 当前还没有审计文件。等出现异常点位并被保护性降级后，这里会自动有记录。"])
        return "\n".join(lines) + "\n"

    if not summary.get("total_events"):
        lines.extend(["", "> 当前审计文件存在，但还没有保护性降级事件。"])
        return "\n".join(lines) + "\n"

    lines.extend(["", "## 事件分布", "", "| 类型 | 次数 |", "| --- | ---: |"])
    for row in summary.get("by_event", []):
        lines.append(f"| {row['label']} | {row['count']} |")

    lines.extend(["", "## 字段分布", "", "| 字段 | 次数 |", "| --- | ---: |"])
    for row in summary.get("by_field", []):
        lines.append(f"| {row['label']} | {row['count']} |")

    lines.extend(["", "## 来源分布", "", "| 来源 | 次数 |", "| --- | ---: |"])
    for row in summary.get("by_source", []):
        lines.append(f"| {row['label']} | {row['count']} |")

    lines.extend(["", "## 最近明细", "", "| 时间 | 标的 | 类型 | 字段 | 当前价 | 原点位 | 最大偏离 | 降级后 |", "| --- | --- | --- | --- | ---: | --- | ---: | --- |"])
    for row in summary.get("recent_events", []):
        lines.append(
            "| {created_at} | {symbol} | {event_label} | {field_label} | {current_price} | {price_values} | {max_distance_pct} | {downgraded_value} |".format(
                **row
            )
        )

    return "\n".join(lines) + "\n"


def write_sniper_point_downgrade_summary(
    audit_path: str | Path = DEFAULT_AUDIT_PATH,
    output_path: str | Path = DEFAULT_REPORT_PATH,
    *,
    max_recent_rows: int = 20,
) -> Path:
    """Write the downgrade summary report and return its path."""
    summary = build_sniper_point_downgrade_summary(audit_path, max_recent_rows=max_recent_rows)
    rendered = render_sniper_point_downgrade_summary(summary)
    target = Path(output_path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(rendered, encoding="utf-8")
    return target


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(row, dict):
            rows.append(row)
    return rows


def _counter_rows(counter: Counter[str], *, labeler: Any | None = None) -> list[dict[str, Any]]:
    labeler = labeler or (lambda key: key)
    return [
        {"key": key, "label": labeler(key), "count": count}
        for key, count in sorted(counter.items(), key=lambda item: (-item[1], item[0]))
    ]


def _context(row: dict[str, Any]) -> dict[str, Any]:
    context = row.get("context") or {}
    return context if isinstance(context, dict) else {}


def _source(row: dict[str, Any]) -> str:
    return str(_context(row).get("source") or "unknown")


def _symbol_label(row: dict[str, Any]) -> str:
    context = _context(row)
    code = str(context.get("code") or context.get("symbol") or context.get("target_symbol") or "").strip()
    name = str(context.get("name") or context.get("stock_name") or "").strip()
    if name and code:
        return f"{name}({code})"
    return name or code or "未知标的"


def _recent_event(row: dict[str, Any]) -> dict[str, str]:
    field = str(row.get("field") or "unknown")
    event = str(row.get("event") or "unknown")
    distances = row.get("distance_pct_values") or []
    max_distance = max((float(item) for item in distances if _is_number(item)), default=0.0)
    if event == "sniper_point_context_mismatch":
        max_distance = float(row.get("mismatch_pct") or 0.0)
    return {
        "created_at": str(row.get("created_at") or ""),
        "symbol": _symbol_label(row),
        "event": event,
        "event_label": EVENT_LABELS.get(event, event),
        "field": field,
        "field_label": FIELD_LABELS.get(field, field),
        "current_price": _format_number(row.get("current_price")),
        "price_values": _format_context_prices(row) if event == "sniper_point_context_mismatch" else _format_prices(row.get("price_values")),
        "max_distance_pct": f"{max_distance:.1f}%",
        "downgraded_value": _format_downgraded_value(row.get("downgraded_value")),
    }


def _format_prices(value: Any) -> str:
    if isinstance(value, list):
        formatted = [_format_number(item) for item in value if _format_number(item) != "-"]
        return ", ".join(formatted) if formatted else "-"
    return _format_number(value)


def _format_number(value: Any) -> str:
    if not _is_number(value):
        return "-"
    number = float(value)
    text = f"{number:.2f}"
    return text.rstrip("0").rstrip(".")


def _format_context_prices(row: dict[str, Any]) -> str:
    candidates = row.get("current_price_candidates") or []
    if not isinstance(candidates, list):
        return "-"
    parts: list[str] = []
    for item in candidates:
        if not isinstance(item, dict):
            continue
        price = _format_number(item.get("value"))
        if price == "-":
            continue
        parts.append(f"{item.get('source')}: {price}")
    return "; ".join(parts) if parts else "-"


def _format_downgraded_value(value: Any) -> str:
    if isinstance(value, dict):
        texts = [str(item) for item in value.values() if item]
        return "；".join(dict.fromkeys(texts))
    return str(value or "")


def _is_number(value: Any) -> bool:
    try:
        float(value)
    except (TypeError, ValueError):
        return False
    return True
