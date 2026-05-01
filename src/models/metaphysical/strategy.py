"""Lightweight resonance strategy summary helpers for research use."""

from __future__ import annotations

from datetime import datetime

from .gann import extract_key_gann_levels, gann_square_of_9, gann_time_square
from .time_law import mars_events_in_range, solar_terms_in_range, ten_gods_for, year_ganzhi
from .trend_law import (
    bollinger_bands,
    bollinger_state,
    is_bb_breakout,
    is_bb_squeeze,
    is_triple_resonance,
    is_volatility_plus_signal,
    month_turning_point_score,
    volatility_plus_score,
)


def generate_resonance_strategy_summary(
    base_price: float,
    first_date,
    *,
    kline_data=None,
    now=None,
    horizon_months: int = 6,
) -> dict:
    """Generate a lightweight time/space resonance summary.

    This is intentionally research-oriented. It combines reusable time-law,
    Gann-law, and optional Bollinger trend-law context.
    """
    now = now or datetime.now()
    horizon_end = datetime(now.year, now.month, now.day)
    month = horizon_end.month - 1 + horizon_months
    year = horizon_end.year + month // 12
    month = month % 12 + 1
    horizon_end = horizon_end.replace(year=year, month=month)

    year_gz = year_ganzhi(now.year)
    ten_god = ten_gods_for(year_gz["stem"])
    solar_terms = solar_terms_in_range(now, horizon_end)
    mars_events = mars_events_in_range(now, horizon_end)
    critical_terms = [item for item in solar_terms if item["critical"]]
    strong_mars = [item for item in mars_events if item["level"] == "强"]

    gann_levels = gann_square_of_9(base_price)
    key_levels = extract_key_gann_levels(gann_levels, base_price)
    gann_times = [item for item in gann_time_square(first_date) if item["date"] > now]

    trend_summary = None
    stance = "震荡观望"
    near_support = False
    near_resistance = False
    entry = key_levels["primary_support"]["price"] if key_levels["primary_support"] else None
    stop_loss = key_levels["terminal_support"]["price"] if key_levels["terminal_support"] else None
    target1 = key_levels["primary_resistance"]["price"] if key_levels["primary_resistance"] else None
    target2 = key_levels["heavy_resistance"]["price"] if key_levels["heavy_resistance"] else None

    if kline_data:
        bb_series = bollinger_bands(kline_data, 20, 2)
        bb_state = bollinger_state(bb_series)
        last_bb = bb_series[-1] if bb_series else None
        trend_summary = {
            "bb_series": bb_series,
            "bb_state": bb_state,
            "is_bb_squeeze": is_bb_squeeze(bb_series),
            "is_bb_breakout": is_bb_breakout(bb_series),
            "is_triple_resonance": is_triple_resonance(bb_series),
            "month_turning_point": month_turning_point_score(now),
            "volatility_plus_score": volatility_plus_score(bb_series, dt=now),
            "is_volatility_plus_signal": is_volatility_plus_signal(bb_series, dt=now),
        }
        if last_bb:
            near_support = (
                last_bb["lower"] is not None and base_price < last_bb["lower"] * 1.02
            )
            near_resistance = (
                last_bb["upper"] is not None and base_price > last_bb["upper"] * 0.98
            )
        trend = bb_state["state"]
        if trend == "riding_upper" or (trend == "expanding" and not near_resistance):
            stance = "偏多"
        elif trend == "riding_lower":
            stance = "偏空"

    next_pivot = critical_terms[0] if critical_terms else (
        {
            "name": mars_events[0]["title"],
            "date": mars_events[0]["date"],
            "critical": False,
            "note": mars_events[0]["interpretation"],
        }
        if mars_events
        else None
    )

    return {
        "stance": stance,
        "entry": entry,
        "stop_loss": stop_loss,
        "target1": target1,
        "target2": target2,
        "near_support": near_support,
        "near_resistance": near_resistance,
        "time_law": {
            "year_ganzhi": year_gz,
            "ten_god": ten_god,
            "solar_terms": solar_terms,
            "mars_events": mars_events,
            "critical_terms": critical_terms,
            "strong_mars": strong_mars,
        },
        "space_law": {
            "gann_levels": gann_levels,
            "key_levels": key_levels,
            "gann_times": gann_times,
        },
        "trend_law": trend_summary,
        "next_pivot": next_pivot,
    }
