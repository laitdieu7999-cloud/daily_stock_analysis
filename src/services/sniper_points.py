# -*- coding: utf-8 -*-
"""Display helpers for stock sniper/action levels."""

from __future__ import annotations

import json
import re
from collections.abc import Mapping, Sequence
from datetime import datetime
from pathlib import Path
from typing import Any


SNIPER_POINT_FIELDS = ("ideal_buy", "secondary_buy", "stop_loss", "take_profit")
STANDARD_SNIPER_LABELS = {
    "ideal_buy": "买入区",
    "secondary_buy": "加仓区",
    "stop_loss": "止损线",
    "take_profit": "目标区",
}

_PLACEHOLDER_VALUES = {
    "",
    "-",
    "—",
    "n/a",
    "na",
    "none",
    "null",
    "nan",
    "not applicable",
    "数据缺失",
    "未知",
    "无",
    "暂无",
    "不适用",
    "待补充",
}

_LABEL_PREFIXES = (
    "理想买入点",
    "理想入场位",
    "次优买入点",
    "次优入场位",
    "止损位",
    "止损价",
    "持仓防守线",
    "目标位",
    "止盈位",
    "目标区",
    "反弹压力",
    "反抽出局",
    "反抽出局线",
    "重新评估线",
    "确认转强线",
    "Ideal Entry",
    "Secondary Entry",
    "Stop Loss",
    "Target",
)

_PRICE_KEYWORDS = (
    "price",
    "value",
    "level",
    "entry",
    "buy",
    "target",
    "stop",
    "stop_loss",
    "take_profit",
    "ideal_buy",
    "secondary_buy",
)
_LOW_KEYS = ("low", "lower", "min", "from", "start", "left", "下限")
_HIGH_KEYS = ("high", "upper", "max", "to", "end", "right", "上限")
_REASON_KEYS = ("reason", "condition", "basis", "desc", "description", "comment", "note", "理由", "条件", "依据")

_MAX_LEVEL_DISTANCE_PCT = {
    "ideal_buy": 20.0,
    "secondary_buy": 20.0,
    "stop_loss": 20.0,
    "take_profit": 20.0,
}
_MAX_CONTEXT_PRICE_SPREAD_PCT = 3.0

SNIPER_POINT_DOWNGRADE_AUDIT_PATH = (
    Path(__file__).resolve().parent.parent.parent / "reports" / "sniper_point_downgrade_audit.jsonl"
)


def clean_sniper_points(points: Mapping[str, Any] | None) -> dict[str, str | None]:
    """Normalize a sniper-points dict for display/API output."""
    source = points or {}
    return {field: clean_sniper_value(source.get(field)) for field in SNIPER_POINT_FIELDS}


def refine_sniper_points_for_context(
    points: Mapping[str, Any] | None,
    *,
    current_price: Any = None,
    decision_type: str | None = None,
    operation_advice: str | None = None,
    trend_prediction: str | None = None,
    dashboard: Mapping[str, Any] | None = None,
    trend_analysis: Mapping[str, Any] | None = None,
    market_snapshot: Mapping[str, Any] | None = None,
    audit_context: Mapping[str, Any] | None = None,
) -> dict[str, str | None]:
    """Clean levels and rewrite bearish reports so far-away targets become pressure lines."""
    cleaned = clean_sniper_points(points)

    price_context = _resolve_current_price_context(
        current_price=current_price,
        dashboard=dashboard,
        trend_analysis=trend_analysis,
        market_snapshot=market_snapshot,
    )
    current = price_context["current"]
    price_position = price_context["price_position"]

    is_bearish = _is_bearish_context(decision_type, operation_advice, trend_prediction)
    if price_context["mismatch"]:
        return _apply_context_mismatch_guard(
            cleaned,
            current=current,
            bearish=is_bearish,
            source_points=points,
            price_context=price_context,
            audit_context=audit_context,
        )

    if not is_bearish:
        return _apply_deviation_guard(
            cleaned,
            current=current,
            bearish=False,
            source_points=points,
            audit_context=audit_context,
        )

    trend = trend_analysis if isinstance(trend_analysis, Mapping) else {}
    candidates = _collect_resistance_candidates(
        points=points,
        cleaned=cleaned,
        current=current,
        price_position=price_position,
        trend_analysis=trend,
    )

    if candidates:
        primary = candidates[0]
        secondary = next((item for item in candidates[1:] if abs(item - primary) > max(primary * 0.003, 0.01)), None)
        cleaned["ideal_buy"] = _bearish_recheck_text(primary, current)
        cleaned["secondary_buy"] = (
            f"确认转强：站稳{_format_price_number(secondary)}元且止跌后再看"
            if secondary is not None
            else f"确认转强：连续站稳{_format_price_number(primary)}元后再看"
        )
        cleaned["take_profit"] = f"反抽出局线：{_format_price_number(primary)}元附近（不是止盈目标）"
    elif cleaned.get("take_profit") and re.search(r"目标|止盈|风险回报", str(cleaned.get("take_profit"))):
        cleaned["take_profit"] = "不设目标；持币观望"

    if cleaned.get("stop_loss") and cleaned.get("stop_loss") != "N/A":
        cleaned["stop_loss"] = _compact_bearish_stop_loss(cleaned["stop_loss"], current)
    if cleaned.get("stop_loss") == "N/A" and current is not None:
        cleaned["stop_loss"] = f"{_format_price_number(current)}元附近离场"
    return _apply_deviation_guard(
        cleaned,
        current=current,
        bearish=True,
        source_points=points,
        audit_context=audit_context,
    )


def clean_sniper_items(points: Mapping[str, Any] | None, *, max_items: int | None = None) -> list[tuple[str, str]]:
    """Normalize arbitrary sniper-point key/value pairs for compact display."""
    if not isinstance(points, Mapping):
        return []

    ordered_keys = [
        *[key for key in SNIPER_POINT_FIELDS if key in points],
        *[key for key in points.keys() if key not in SNIPER_POINT_FIELDS],
    ]
    items: list[tuple[str, str]] = []
    for key in ordered_keys:
        value = clean_sniper_value(points.get(key))
        if value == "N/A":
            continue
        label = STANDARD_SNIPER_LABELS.get(str(key), _clean_label(key))
        if not label:
            continue
        pair = (label, value)
        if pair not in items:
            items.append(pair)
        if max_items is not None and len(items) >= max_items:
            break
    return items


def clean_sniper_value(value: Any) -> str:
    """Return a compact human-readable sniper level, never raw JSON/list/dict text."""
    return _truncate_display(_clean_sniper_value(value))


def extract_sniper_price(value: Any) -> float | None:
    """Extract the first actionable price from a sniper value."""
    if isinstance(value, Mapping):
        for key in (*_PRICE_KEYWORDS, *_LOW_KEYS, *_HIGH_KEYS):
            number = _coerce_number(_lookup_key(value, key))
            if number is not None:
                return number
        return None
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        for item in value:
            number = extract_sniper_price(item)
            if number is not None:
                return number
        return None
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return abs(float(value)) if float(value) != 0 else None
    if isinstance(value, str):
        parsed = _parse_jsonish(value)
        if parsed is not None:
            return extract_sniper_price(parsed)
        return _extract_first_price(value)
    return None


def _resolve_current_price_context(
    *,
    current_price: Any = None,
    dashboard: Mapping[str, Any] | None = None,
    trend_analysis: Mapping[str, Any] | None = None,
    market_snapshot: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Resolve current price and flag suspiciously inconsistent quote sources."""
    price_position: Mapping[str, Any] = {}
    if isinstance(dashboard, Mapping):
        data_perspective = dashboard.get("data_perspective") or {}
        if isinstance(data_perspective, Mapping):
            raw_price_position = data_perspective.get("price_position") or {}
            if isinstance(raw_price_position, Mapping):
                price_position = raw_price_position

    candidates: list[dict[str, Any]] = []

    def add(source: str, value: Any) -> None:
        number = _coerce_number(value)
        if number is None:
            return
        if any(abs(number - item["value"]) <= max(number * 0.001, 0.001) for item in candidates):
            return
        candidates.append({"source": source, "value": number})

    add("result.current_price", current_price)
    add("dashboard.price_position.current_price", price_position.get("current_price"))

    if isinstance(market_snapshot, Mapping):
        add("market_snapshot.current_price", market_snapshot.get("current_price"))
        add("market_snapshot.price", market_snapshot.get("price"))

    trend = trend_analysis if isinstance(trend_analysis, Mapping) else {}
    add("trend_analysis.current_price", trend.get("current_price"))

    current = candidates[0]["value"] if candidates else None
    mismatch = False
    spread_pct = 0.0
    if len(candidates) >= 2:
        values = [float(item["value"]) for item in candidates]
        low = min(values)
        high = max(values)
        if low > 0:
            spread_pct = (high - low) / low * 100
            mismatch = spread_pct > _MAX_CONTEXT_PRICE_SPREAD_PCT

    return {
        "current": current,
        "price_position": price_position,
        "candidates": candidates,
        "mismatch": mismatch,
        "spread_pct": spread_pct,
        "limit_pct": _MAX_CONTEXT_PRICE_SPREAD_PCT,
    }


def _is_bearish_context(
    decision_type: str | None,
    operation_advice: str | None,
    trend_prediction: str | None,
) -> bool:
    joined = " ".join(str(item or "") for item in (decision_type, operation_advice, trend_prediction)).lower()
    bearish_tokens = (
        "sell",
        "bear",
        "卖",
        "清仓",
        "止损",
        "离场",
        "看空",
        "空头",
        "弱势",
    )
    return any(token in joined for token in bearish_tokens)


def _collect_resistance_candidates(
    *,
    points: Mapping[str, Any] | None,
    cleaned: Mapping[str, Any],
    current: float | None,
    price_position: Mapping[str, Any],
    trend_analysis: Mapping[str, Any],
) -> list[float]:
    raw_values: list[Any] = [
        price_position.get("resistance_level"),
        price_position.get("ma5"),
        price_position.get("ma10"),
        trend_analysis.get("ma5"),
        trend_analysis.get("ma10"),
        cleaned.get("ideal_buy"),
        cleaned.get("secondary_buy"),
    ]
    if isinstance(points, Mapping):
        raw_values.extend([points.get("ideal_buy"), points.get("secondary_buy")])

    candidates: list[float] = []
    for value in raw_values:
        number = extract_sniper_price(value)
        if number is None:
            continue
        if current is not None and number <= current:
            continue
        if all(abs(number - existing) > max(number * 0.002, 0.01) for existing in candidates):
            candidates.append(number)
    return sorted(candidates)


def _bearish_recheck_text(level: float, current: float | None) -> str:
    if current is None or current <= 0:
        return f"暂不接回；重新站回{_format_price_number(level)}元后再评估"
    distance_pct = (level - current) / current * 100
    return f"暂不接回；重新站回{_format_price_number(level)}元后再评估（较现价+{distance_pct:.1f}%）"


def _apply_context_mismatch_guard(
    cleaned: Mapping[str, str | None],
    *,
    current: float | None,
    bearish: bool,
    source_points: Mapping[str, Any] | None,
    price_context: Mapping[str, Any],
    audit_context: Mapping[str, Any] | None,
) -> dict[str, str | None]:
    """Downgrade all displayed levels when quote sources disagree."""
    guarded = dict(cleaned)
    changed: dict[str, str | None] = {}
    for field in SNIPER_POINT_FIELDS:
        value = guarded.get(field)
        if not value or value == "N/A":
            continue
        guarded[field] = _context_mismatch_fallback(field, bearish=bearish)
        changed[field] = guarded[field]

    if changed:
        _record_context_mismatch_audit(
            current=current,
            bearish=bearish,
            source_points=source_points,
            cleaned_points={field: cleaned.get(field) for field in SNIPER_POINT_FIELDS},
            downgraded_points=changed,
            price_context=price_context,
            audit_context=audit_context,
        )
    return guarded


def _apply_deviation_guard(
    cleaned: Mapping[str, str | None],
    *,
    current: float | None,
    bearish: bool,
    source_points: Mapping[str, Any] | None = None,
    audit_context: Mapping[str, Any] | None = None,
) -> dict[str, str | None]:
    """Downgrade suspiciously far levels instead of showing them as executable prices."""
    guarded = dict(cleaned)
    if current is None or current <= 0:
        return guarded

    for field in SNIPER_POINT_FIELDS:
        value = guarded.get(field)
        if not value or value == "N/A":
            continue
        prices = _extract_guard_prices(str(value))
        if not prices:
            continue
        limit = _MAX_LEVEL_DISTANCE_PCT[field]
        distances = [abs(price - current) / current * 100 for price in prices]
        if any(distance > limit for distance in distances):
            original_cleaned = str(value)
            guarded[field] = _deviation_fallback(field, current=current, bearish=bearish)
            _record_downgrade_audit(
                field=field,
                current=current,
                bearish=bearish,
                limit_pct=limit,
                price_values=prices,
                distance_pct_values=distances,
                source_value=(source_points or {}).get(field) if isinstance(source_points, Mapping) else None,
                cleaned_value=original_cleaned,
                downgraded_value=guarded[field],
                audit_context=audit_context,
            )
    return guarded


def _record_context_mismatch_audit(
    *,
    current: float | None,
    bearish: bool,
    source_points: Mapping[str, Any] | None,
    cleaned_points: Mapping[str, Any],
    downgraded_points: Mapping[str, Any],
    price_context: Mapping[str, Any],
    audit_context: Mapping[str, Any] | None,
) -> None:
    """Append a structured local audit record when quote context is inconsistent."""
    if not audit_context:
        return
    try:
        record = {
            "created_at": datetime.now().isoformat(timespec="seconds"),
            "event": "sniper_point_context_mismatch",
            "field": "all",
            "current_price": round(float(current), 6) if current is not None else None,
            "bearish": bool(bearish),
            "limit_pct": float(price_context.get("limit_pct") or _MAX_CONTEXT_PRICE_SPREAD_PCT),
            "mismatch_pct": round(float(price_context.get("spread_pct") or 0.0), 4),
            "current_price_candidates": _json_safe_value(price_context.get("candidates") or []),
            "source_value": _json_safe_value(dict(source_points or {})),
            "cleaned_value": _json_safe_value(dict(cleaned_points)),
            "downgraded_value": _json_safe_value(dict(downgraded_points)),
            "context": _json_safe_value(dict(audit_context)),
        }
        SNIPER_POINT_DOWNGRADE_AUDIT_PATH.parent.mkdir(parents=True, exist_ok=True)
        with SNIPER_POINT_DOWNGRADE_AUDIT_PATH.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(record, ensure_ascii=False) + "\n")
    except Exception:
        # Audit failures must never block report generation.
        return


def _record_downgrade_audit(
    *,
    field: str,
    current: float,
    bearish: bool,
    limit_pct: float,
    price_values: Sequence[float],
    distance_pct_values: Sequence[float],
    source_value: Any,
    cleaned_value: str,
    downgraded_value: str | None,
    audit_context: Mapping[str, Any] | None,
) -> None:
    """Append a structured local audit record when a level is protectively downgraded."""
    if not audit_context:
        return
    try:
        record = {
            "created_at": datetime.now().isoformat(timespec="seconds"),
            "event": "sniper_point_protective_downgrade",
            "field": field,
            "current_price": round(float(current), 6),
            "bearish": bool(bearish),
            "limit_pct": float(limit_pct),
            "price_values": [round(float(item), 6) for item in price_values],
            "distance_pct_values": [round(float(item), 4) for item in distance_pct_values],
            "source_value": _json_safe_value(source_value),
            "cleaned_value": cleaned_value,
            "downgraded_value": downgraded_value,
            "context": _json_safe_value(dict(audit_context)),
        }
        SNIPER_POINT_DOWNGRADE_AUDIT_PATH.parent.mkdir(parents=True, exist_ok=True)
        with SNIPER_POINT_DOWNGRADE_AUDIT_PATH.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(record, ensure_ascii=False) + "\n")
    except Exception:
        # Audit failures must never block report generation.
        return


def _json_safe_value(value: Any) -> Any:
    try:
        json.dumps(value, ensure_ascii=False)
        return value
    except (TypeError, ValueError):
        return str(value)


def _deviation_fallback(field: str, *, current: float, bearish: bool) -> str:
    current_text = _format_price_number(current)
    if bearish:
        return {
            "ideal_buy": f"暂不接回；原点位偏离现价{current_text}元过大，待行情刷新后重算",
            "secondary_buy": f"暂不确认转强；原点位偏离现价{current_text}元过大，待行情刷新后重算",
            "stop_loss": f"{current_text}元附近先降风险；原止损点位偏离现价过大",
            "take_profit": f"不设反抽出局线；原点位偏离现价{current_text}元过大，待行情刷新后重算",
        }.get(field, f"暂不设点位；原点位偏离现价{current_text}元过大，待行情刷新后重算")
    return {
        "ideal_buy": f"暂不设买入区；原点位偏离现价{current_text}元过大，待行情刷新后重算",
        "secondary_buy": f"暂不设加仓区；原点位偏离现价{current_text}元过大，待行情刷新后重算",
        "stop_loss": f"暂不设止损线；原点位偏离现价{current_text}元过大，待行情刷新后重算",
        "take_profit": f"暂不设目标区；原点位偏离现价{current_text}元过大，待行情刷新后重算",
    }.get(field, f"暂不设点位；原点位偏离现价{current_text}元过大，待行情刷新后重算")


def _context_mismatch_fallback(field: str, *, bearish: bool) -> str:
    if bearish:
        return {
            "ideal_buy": "暂不接回；行情上下文不一致，待刷新后重算",
            "secondary_buy": "暂不确认转强；行情上下文不一致，待刷新后重算",
            "stop_loss": "暂不设防守线；行情上下文不一致，待刷新后重算",
            "take_profit": "不设反抽出局线；行情上下文不一致，待刷新后重算",
        }.get(field, "暂不设点位；行情上下文不一致，待刷新后重算")
    return {
        "ideal_buy": "暂不设买入区；行情上下文不一致，待刷新后重算",
        "secondary_buy": "暂不设加仓区；行情上下文不一致，待刷新后重算",
        "stop_loss": "暂不设止损线；行情上下文不一致，待刷新后重算",
        "take_profit": "暂不设目标区；行情上下文不一致，待刷新后重算",
    }.get(field, "暂不设点位；行情上下文不一致，待刷新后重算")


def _compact_bearish_stop_loss(text: str, current: float | None) -> str:
    """Keep bearish stop text readable in the four-line battle plan card."""
    noisy_tokens = ("现价", "当前价", "立即", "清仓", "截图", "若无法执行", "强制止损")
    if len(text) <= 42 and not any(token in text for token in noisy_tokens):
        return text

    prices = _extract_prices(text)
    exit_price = current if current is not None and any(token in text for token in noisy_tokens) else None
    if exit_price is None and prices:
        exit_price = prices[0]
    if exit_price is None:
        return text

    parts = [f"{_format_price_number(exit_price)}元附近离场"]
    hard_stop = next(
        (
            price
            for price in prices
            if price < exit_price
            and abs(price - exit_price) > max(exit_price * 0.003, 0.01)
        ),
        None,
    )
    if hard_stop is not None:
        parts.append(f"硬止损{_format_price_number(hard_stop)}元")
    return "；".join(parts)


def _clean_sniper_value(value: Any) -> str:
    if _is_placeholder(value):
        return "N/A"

    if isinstance(value, bool):
        return "N/A"

    if isinstance(value, (int, float)):
        return f"{_format_price_number(float(value))}元"

    if isinstance(value, Mapping):
        return _clean_mapping(value)

    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return _clean_sequence(value)

    text = _strip_label_prefix(str(value).strip())
    if _is_placeholder(text):
        return "N/A"

    parsed = _parse_jsonish(text)
    if parsed is not None and parsed is not value:
        return _clean_sniper_value(parsed)

    text = _collapse_spaces(text)
    range_text = _format_range_from_text(text)
    if range_text:
        return range_text

    if _looks_like_display_sentence(text):
        return _normalize_sentence_price_text(text)

    price = _extract_first_price(text)
    if price is not None:
        return f"{_format_price_number(price)}元"

    return _strip_raw_markers(text) or "N/A"


def _clean_mapping(value: Mapping[str, Any]) -> str:
    flattened = _flatten_mapping(value)

    reason = _first_text(flattened, _REASON_KEYS)
    low = _first_number(flattened, _LOW_KEYS)
    high = _first_number(flattened, _HIGH_KEYS)
    if low is not None and high is not None:
        if low > high:
            low, high = high, low
        base = f"{_format_price_number(low)}-{_format_price_number(high)}元"
        return _append_reason(base, reason)

    price = _first_number(flattened, _PRICE_KEYWORDS)
    if price is not None:
        return _append_reason(f"{_format_price_number(price)}元", reason)

    for key in (*_PRICE_KEYWORDS, *_REASON_KEYS):
        if key in flattened and not _is_placeholder(flattened[key]):
            cleaned = _clean_sniper_value(flattened[key])
            if cleaned != "N/A":
                return cleaned

    for item in flattened.values():
        cleaned = _clean_sniper_value(item)
        if cleaned != "N/A":
            return cleaned
    return "N/A"


def _clean_sequence(value: Sequence[Any]) -> str:
    cleaned: list[str] = []
    for item in value:
        text = _clean_sniper_value(item)
        if text != "N/A" and text not in cleaned:
            cleaned.append(text)
        if len(cleaned) >= 2:
            break
    if not cleaned:
        return "N/A"
    if len(cleaned) == 1:
        return cleaned[0]

    left = _extract_first_price(cleaned[0])
    right = _extract_first_price(cleaned[1])
    if left is not None and right is not None:
        if left > right:
            left, right = right, left
        return f"{_format_price_number(left)}-{_format_price_number(right)}元"
    return "；".join(cleaned)


def _flatten_mapping(value: Mapping[str, Any]) -> dict[str, Any]:
    flattened: dict[str, Any] = {}
    for key, item in value.items():
        key_text = str(key).strip()
        flattened[key_text] = item
        if isinstance(item, Mapping):
            nested = _flatten_mapping(item)
            for nested_key, nested_value in nested.items():
                flattened.setdefault(nested_key, nested_value)
    return flattened


def _is_placeholder(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return value.strip().lower() in _PLACEHOLDER_VALUES
    return False


def _strip_label_prefix(text: str) -> str:
    normalized = text.strip()
    for prefix in _LABEL_PREFIXES:
        pattern = rf"^\s*{re.escape(prefix)}\s*[:：]\s*"
        normalized = re.sub(pattern, "", normalized, flags=re.IGNORECASE)
    return normalized.strip()


def _parse_jsonish(text: str) -> Any | None:
    stripped = text.strip()
    if not ((stripped.startswith("{") and stripped.endswith("}")) or (stripped.startswith("[") and stripped.endswith("]"))):
        return None
    try:
        return json.loads(stripped)
    except (TypeError, ValueError, json.JSONDecodeError):
        return None


def _format_price_number(value: float) -> str:
    if abs(value) < 10 and round(value, 2) != round(value, 3):
        return f"{value:.3f}"
    return f"{value:.2f}"


def _format_range_from_text(text: str) -> str | None:
    number = r"-?\d+(?:\.\d+)?"
    match = re.fullmatch(rf"\s*({number})\s*([-~～至/])\s*({number})\s*(?:元)?\s*", text)
    if not match:
        return None
    left, sep, right = match.groups()
    left_number = abs(float(left))
    right_number = abs(float(right))
    if left_number > right_number:
        left_number, right_number = right_number, left_number
    display_sep = "-" if sep in {"~", "～", "至", "/"} else sep
    return f"{_format_price_number(left_number)}{display_sep}{_format_price_number(right_number)}元"


def _looks_like_display_sentence(text: str) -> bool:
    if any(marker in text for marker in ("元", "%", "日", "天", "周", "等待", "暂不", "不新开仓", "跌破", "突破", "回踩", "站回", "附近")):
        return True
    return text.startswith(("无", "暂无", "不宜", "不建议", "观望"))


def _extract_first_price(text: str) -> float | None:
    search_text = text
    for paren in ("(", "（"):
        pos = search_text.find(paren)
        if pos != -1:
            search_text = search_text[:pos]
            break

    for match in re.finditer(r"-?\d+(?:\.\d+)?", search_text.replace(",", "")):
        start = match.start()
        if start >= 2 and search_text[start - 2:start].upper() == "MA":
            continue
        try:
            number = abs(float(match.group()))
        except ValueError:
            continue
        if number > 0:
            return number
    return None


def _extract_prices(text: str) -> list[float]:
    search_text = text.replace(",", "")
    prices: list[float] = []
    for match in re.finditer(r"-?\d+(?:\.\d+)?", search_text):
        start = match.start()
        if start >= 2 and search_text[start - 2:start].upper() == "MA":
            continue
        try:
            number = abs(float(match.group()))
        except ValueError:
            continue
        if number > 0 and all(abs(number - item) > max(number * 0.001, 0.001) for item in prices):
            prices.append(number)
    return prices


def _extract_guard_prices(text: str) -> list[float]:
    """Extract price-like numbers while ignoring MA labels, dates, and percentages."""
    search_text = text.replace(",", "")
    prices: list[float] = []
    for match in re.finditer(r"-?\d+(?:\.\d+)?", search_text):
        start = match.start()
        if start >= 2 and search_text[start - 2:start].upper() == "MA":
            continue

        suffix = search_text[match.end(): match.end() + 3].lstrip()
        if suffix.startswith(("%", "日", "天", "周", "月", "年", "个")):
            continue

        try:
            number = abs(float(match.group()))
        except ValueError:
            continue
        if number > 0 and all(abs(number - item) > max(number * 0.001, 0.001) for item in prices):
            prices.append(number)
    return prices


def _normalize_sentence_price_text(text: str) -> str:
    if "元" in text:
        return text
    match = re.search(r"-?\d+(?:\.\d+)?", text.replace(",", ""))
    if not match:
        return text
    try:
        price_text = f"{_format_price_number(abs(float(match.group())))}元"
    except ValueError:
        return text
    return f"{text[:match.start()]}{price_text}{text[match.end():]}"


def _first_number(source: Mapping[str, Any], keys: Sequence[str]) -> float | None:
    for key in keys:
        value = _lookup_key(source, key)
        number = _coerce_number(value)
        if number is not None:
            return number
    return None


def _first_text(source: Mapping[str, Any], keys: Sequence[str]) -> str | None:
    for key in keys:
        value = _lookup_key(source, key)
        if _is_placeholder(value) or isinstance(value, (Mapping, list, tuple, set)):
            continue
        text = _strip_raw_markers(str(value).strip())
        if text:
            return text
    return None


def _lookup_key(source: Mapping[str, Any], key: str) -> Any:
    key_lower = key.lower()
    for candidate_key, candidate_value in source.items():
        if str(candidate_key).strip().lower() == key_lower:
            return candidate_value
    return None


def _coerce_number(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        number = abs(float(value))
        return number if number > 0 else None
    if isinstance(value, str):
        return _extract_first_price(value)
    return None


def _append_reason(base: str, reason: str | None) -> str:
    if not reason:
        return base
    reason = _truncate_display(_strip_label_prefix(reason), limit=36)
    if not reason or reason == "N/A":
        return base
    if reason.startswith(("(", "（")):
        return f"{base}{reason}"
    return f"{base}（{reason}）"


def _strip_raw_markers(text: str) -> str:
    return (
        text.replace("{", "")
        .replace("}", "")
        .replace("[", "")
        .replace("]", "")
        .replace('"', "")
        .replace("'", "")
        .strip()
    )


def _clean_label(value: Any) -> str:
    text = _strip_raw_markers(str(value).strip())
    return _truncate_display(text, limit=16)


def _collapse_spaces(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def _truncate_display(text: str, limit: int = 80) -> str:
    cleaned = _collapse_spaces(text)
    if len(cleaned) <= limit:
        return cleaned
    return cleaned[:limit].rstrip() + "..."
