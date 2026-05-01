"""Ganzhi calendar helpers."""

from __future__ import annotations

import pandas as pd

from .constants import BRANCHES_CN, STEMS_CN
from .deps import get_from_solar


def get_ganzhi(dt):
    """Return (tiangan, dizhi, pair) for the given date-like value."""
    from_solar = get_from_solar()
    day = from_solar(dt.year, dt.month, dt.day)
    gz = day.getDayGZ()
    tg = STEMS_CN[gz.tg]
    dz = BRANCHES_CN[gz.dz]
    return tg, dz, tg + dz


def batch_ganzhi(dates):
    """Batch ganzhi calculation aligned to the input date index."""
    records = []
    for dt in dates:
        tg, dz, pair = get_ganzhi(dt)
        records.append({"tiangan": tg, "dizhi": dz, "tg_dz": pair})
    return pd.DataFrame(records, index=dates)
