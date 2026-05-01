"""Gann-model helpers extracted from standalone quant research prototypes."""

from __future__ import annotations

from datetime import datetime, timedelta
from math import sqrt


def gann_square_of_9(base_price: float, angles=None) -> list[dict]:
    angles = angles or [45, 90, 135, 180, 225, 270, 360]
    root = sqrt(base_price)
    result = []
    for angle in angles:
        delta = angle / 360
        up = (root + delta) ** 2
        down = (root - delta) ** 2
        strength = (
            "strong" if angle in {180, 360}
            else "medium" if angle in {45, 90}
            else "weak"
        )
        result.append(
            {
                "angle": angle,
                "price": round(up, 2),
                "kind": "resistance",
                "strength": strength,
                "label": f"+{angle}° 阻力",
            }
        )
        result.append(
            {
                "angle": angle,
                "price": round(down, 2),
                "kind": "support",
                "strength": strength,
                "label": f"-{angle}° 支撑",
            }
        )
    return sorted(result, key=lambda item: item["price"])


def extract_key_gann_levels(levels: list[dict], base_price: float) -> dict:
    below = [item for item in levels if item["price"] < base_price]
    above = [item for item in levels if item["price"] > base_price]
    strong_down = [item for item in below if item["strength"] == "strong"]
    strong_up = [item for item in above if item["strength"] == "strong"]
    return {
        "terminal_support": strong_down[0] if strong_down else (below[0] if below else None),
        "primary_support": below[-1] if below else None,
        "primary_resistance": above[0] if above else None,
        "heavy_resistance": strong_up[-1] if strong_up else (above[-1] if above else None),
    }


def gann_time_square(start_date, cycles=None) -> list[dict]:
    cycles = cycles or [45, 60, 90, 120, 144, 180, 270, 360]
    base = datetime(start_date.year, start_date.month, start_date.day)
    result = []
    for cycle in cycles:
        event_date = base + timedelta(days=cycle)
        if cycle == 45:
            reason = "八分之一循环"
        elif cycle == 90:
            reason = "四分之一循环（强变盘）"
        elif cycle == 120:
            reason = "三分之一循环"
        elif cycle == 144:
            reason = "平方数 12² · 江恩核心数"
        elif cycle == 180:
            reason = "半循环（强变盘）"
        elif cycle == 270:
            reason = "四分之三循环"
        elif cycle == 360:
            reason = "完整循环（强变盘）"
        else:
            reason = "时间循环节点"
        result.append(
            {
                "date": event_date,
                "days_from_start": cycle,
                "reason": reason,
            }
        )
    return result
