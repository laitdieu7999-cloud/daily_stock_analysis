"""Trend-law helpers for research resonance models."""

from __future__ import annotations

import math
from datetime import datetime

MONTH_TURNING_POINT_WINDOWS = (
    (1, 3, "month_open"),
    (7, 10, "early_pivot"),
    (14, 16, "mid_pivot"),
    (21, 24, "late_pivot"),
    (27, 31, "month_end"),
)


def bollinger_bands(kline_data, period: int = 20, stddev: float = 2.0) -> list[dict]:
    """Return Bollinger band series for simple K-line records."""
    closes = [float(item["close"]) for item in kline_data]
    result = []
    for idx, item in enumerate(kline_data):
        if idx < period - 1:
            result.append(
                {
                    "date": item["date"],
                    "ma": None,
                    "upper": None,
                    "lower": None,
                    "bandwidth": None,
                    "bb_width_ratio": None,
                    "bb_position_pct": None,
                    "bb_width_roc_3d": None,
                    "price": float(item["close"]),
                }
            )
            continue

        window = closes[idx - period + 1 : idx + 1]
        mean = sum(window) / period
        variance = sum((value - mean) ** 2 for value in window) / period
        sigma = math.sqrt(variance)
        upper = mean + stddev * sigma
        lower = mean - stddev * sigma
        result.append(
            {
                "date": item["date"],
                "ma": round(mean, 2),
                "upper": round(upper, 2),
                "lower": round(lower, 2),
                "bandwidth": round(((upper - lower) / mean) * 100, 2) if mean else 0.0,
                "bb_width_ratio": round(((upper - lower) / mean), 6) if mean else 0.0,
                "bb_position_pct": (
                    round((float(item["close"]) - lower) / (upper - lower), 6)
                    if upper != lower
                    else 0.5
                ),
                "price": float(item["close"]),
            }
        )

    # Add 3-day bandwidth rate-of-change after the base series is built.
    valid_bandwidths = [item["bandwidth"] for item in result]
    for idx, item in enumerate(result):
        bandwidth = item["bandwidth"]
        previous_valid = valid_bandwidths[idx - 3] if idx >= 3 else None
        if bandwidth is None or previous_valid in (None, 0):
            item["bb_width_roc_3d"] = None
        else:
            item["bb_width_roc_3d"] = round((bandwidth - previous_valid) / previous_valid, 6)
    return result


def bollinger_state(bb_series) -> dict:
    """Evaluate the current Bollinger regime from the recent band series."""
    valid = [item for item in bb_series if item["bandwidth"] is not None]
    if not valid:
        return {"state": "neutral", "label": "数据不足", "note": "--", "bandwidth": 0.0}

    recent = valid[-20:]
    avg_bw = sum(float(item["bandwidth"] or 0) for item in recent) / len(recent)
    last = valid[-1]
    bw = float(last["bandwidth"] or 0)
    touching_upper = last["upper"] is not None and last["price"] > last["upper"] * 0.995
    touching_lower = last["lower"] is not None and last["price"] < last["lower"] * 1.005

    if touching_upper:
        return {
            "state": "riding_upper",
            "label": "上轨强势",
            "note": "多头动能充沛，趋势加速阶段",
            "bandwidth": bw,
        }
    if touching_lower:
        return {
            "state": "riding_lower",
            "label": "下轨弱势",
            "note": "空头动能主导，警惕趋势延续",
            "bandwidth": bw,
        }
    if bw < avg_bw * 0.7:
        return {
            "state": "squeeze",
            "label": "收口",
            "note": "波动率收缩，变盘窗口临近",
            "bandwidth": bw,
        }
    if bw > avg_bw * 1.3:
        return {
            "state": "expanding",
            "label": "开口放大",
            "note": "动能释放中，顺势操作",
            "bandwidth": bw,
        }
    return {
        "state": "neutral",
        "label": "中性震荡",
        "note": "多空均衡，等待方向选择",
        "bandwidth": bw,
    }


def is_bb_squeeze(bb_series, lookback: int = 20, quantile: float = 0.1) -> bool:
    """Return True when current width is below the recent low-bandwidth threshold."""
    valid = [item for item in bb_series if item["bb_width_ratio"] is not None]
    if len(valid) < lookback:
        return False
    recent = [float(item["bb_width_ratio"]) for item in valid[-lookback:]]
    current = recent[-1]
    sorted_recent = sorted(recent)
    threshold_index = min(max(int(len(sorted_recent) * quantile), 0), len(sorted_recent) - 1)
    threshold = sorted_recent[threshold_index]
    return current <= threshold


def is_bb_breakout(bb_series) -> bool:
    """Return True when price breaks the band with expanding width."""
    valid = [item for item in bb_series if item["upper"] is not None and item["lower"] is not None]
    if not valid:
        return False
    last = valid[-1]
    roc = last.get("bb_width_roc_3d")
    if roc is None or roc <= 0:
        return False
    return bool(last["price"] > last["upper"] or last["price"] < last["lower"])


def bb_breakout_strength(bb_series) -> float:
    """Return a continuous breakout strength score in [0, 1]."""
    valid = [item for item in bb_series if item["upper"] is not None and item["lower"] is not None]
    if not valid:
        return 0.0

    last = valid[-1]
    upper = float(last["upper"])
    lower = float(last["lower"])
    price = float(last["price"])
    band_range = max(upper - lower, 1e-9)
    roc = max(float(last.get("bb_width_roc_3d") or 0.0), 0.0)

    if lower <= price <= upper:
        return 0.0

    overshoot = max(price - upper, lower - price, 0.0) / band_range
    score = overshoot * 4.0 + roc
    return round(min(score, 1.0), 6)


def is_triple_resonance(
    bb_series,
    *,
    volume_ratio: float | None = None,
    require_breakout: bool = True,
    require_squeeze: bool = True,
    volume_ratio_threshold: float = 1.2,
) -> bool:
    """Return a lightweight triple-resonance trigger for research use.

    Current rule set:
    - volatility law: squeeze signal
    - space law: breakout signal
    - energy law: optional volume ratio threshold
    """
    if require_squeeze and not is_bb_squeeze(bb_series):
        return False
    if require_breakout and not is_bb_breakout(bb_series):
        return False
    if volume_ratio is not None and volume_ratio < volume_ratio_threshold:
        return False
    return True


def triple_resonance_score(
    bb_series,
    *,
    volume_ratio: float | None = None,
    volume_ratio_threshold: float = 1.2,
) -> float:
    """Return a continuous triple-resonance strength score in [0, 1]."""
    valid = [item for item in bb_series if item.get("bb_width_ratio") is not None]
    if not valid:
        return 0.0

    last = valid[-1]
    squeeze_component = 1.0 if is_bb_squeeze(bb_series) else 0.0
    breakout_component = bb_breakout_strength(bb_series)
    energy_component = 0.0
    if volume_ratio is not None:
        energy_component = max(float(volume_ratio) / volume_ratio_threshold - 1.0, 0.0)
        energy_component = min(energy_component, 1.0)
    roc_component = max(float(last.get("bb_width_roc_3d") or 0.0), 0.0)
    roc_component = min(roc_component, 1.0)

    score = (
        squeeze_component * 0.3
        + breakout_component * 0.35
        + energy_component * 0.2
        + roc_component * 0.15
    )
    return round(min(score, 1.0), 6)


def month_turning_point_score(dt) -> dict:
    """Return a simple month-turning-point template score for source-style timing."""
    if isinstance(dt, str):
        base = datetime.fromisoformat(dt)
    else:
        base = datetime(dt.year, dt.month, dt.day)

    day = base.day
    best_distance = 31
    best_label = "none"
    for start, end, label in MONTH_TURNING_POINT_WINDOWS:
        if start <= day <= end:
            best_distance = 0
            best_label = label
            break
        if day < start:
            distance = start - day
        else:
            distance = day - end
        if distance < best_distance:
            best_distance = distance
            best_label = label

    is_window = int(best_distance == 0)
    # score decays quickly outside the turning-point window
    score = max(0.0, 1.0 - best_distance / 5.0)
    return {
        "month_turning_point_score": round(score, 6),
        "is_month_turning_point_window": is_window,
        "month_turning_point_label": best_label,
        "month_turning_point_distance": int(best_distance),
    }


def batch_month_turning_points(dates) -> list[dict]:
    """Batch monthly turning-point features."""
    return [month_turning_point_score(dt) for dt in dates]


def volatility_plus_score(
    bb_series,
    *,
    dt=None,
    volume_ratio: float | None = None,
    volume_ratio_threshold: float = 1.2,
) -> float:
    """Return an author-style volatility-plus score in [0, 1].

    This blends:
    - Bollinger squeeze / breakout structure
    - recent width expansion
    - monthly turning-point timing window
    - optional volume confirmation
    """
    valid = [item for item in bb_series if item.get("bb_width_ratio") is not None]
    if not valid:
        return 0.0

    last = valid[-1]
    dt = dt or last.get("date")
    turning = month_turning_point_score(dt) if dt is not None else {
        "month_turning_point_score": 0.0
    }
    squeeze_component = 1.0 if is_bb_squeeze(bb_series) else 0.0
    breakout_component = bb_breakout_strength(bb_series)
    roc_component = max(float(last.get("bb_width_roc_3d") or 0.0), 0.0)
    roc_component = min(roc_component, 1.0)
    turning_component = float(turning["month_turning_point_score"])

    energy_component = 0.0
    if volume_ratio is not None:
        energy_component = max(float(volume_ratio) / volume_ratio_threshold - 1.0, 0.0)
        energy_component = min(energy_component, 1.0)

    score = (
        squeeze_component * 0.2
        + breakout_component * 0.3
        + roc_component * 0.15
        + turning_component * 0.2
        + energy_component * 0.15
    )
    return round(min(score, 1.0), 6)


def is_volatility_plus_signal(
    bb_series,
    *,
    dt=None,
    volume_ratio: float | None = None,
    threshold: float = 0.55,
) -> bool:
    """Return whether volatility-plus timing is active."""
    score = volatility_plus_score(
        bb_series,
        dt=dt,
        volume_ratio=volume_ratio,
    )
    return score >= threshold
