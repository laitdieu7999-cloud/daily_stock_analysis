"""Astronomical longitude helpers for research features."""

from __future__ import annotations

import os
import pickle
from datetime import datetime

from .constants import PLANET_NAME_MAP, PLUTO_LON
from .deps import get_ephem


def _build_observer():
    ephem = get_ephem()
    observer = ephem.Observer()
    observer.lon, observer.lat = "116.4", "39.9"
    return observer


def get_body_helio_lon(body_name, dt):
    ephem = get_ephem()
    observer = _build_observer()
    observer.date = datetime(dt.year, dt.month, dt.day, 12, 0, 0)
    body = getattr(ephem, body_name)(observer)
    return float(ephem.degrees(body.hlon)) % 360


def get_sun_geo_lon(dt):
    ephem = get_ephem()
    observer = _build_observer()
    observer.date = datetime(dt.year, dt.month, dt.day, 12, 0, 0)
    sun = ephem.Sun(observer)
    ecl = ephem.Ecliptic(ephem.Equatorial(sun.ra, sun.dec))
    return float(ephem.degrees(ecl.lon)) % 360


def get_pluto_lon(dt):
    keys = sorted(PLUTO_LON.keys())
    prev_k = next_k = None
    for key in keys:
        if key <= (dt.year, dt.month):
            prev_k = key
        else:
            next_k = key
            break
    if prev_k is None:
        return PLUTO_LON[keys[0]]
    if next_k is None:
        return PLUTO_LON[prev_k]
    prev_day = prev_k[0] * 365 + prev_k[1] * 30
    next_day = next_k[0] * 365 + next_k[1] * 30
    cur_day = dt.year * 365 + dt.month * 30
    ratio = (cur_day - prev_day) / (next_day - prev_day) if next_day != prev_day else 0
    return PLUTO_LON[prev_k] + ratio * (PLUTO_LON[next_k] - PLUTO_LON[prev_k])


def batch_planet_lons(dates, cache_dir=None):
    cache_file = os.path.join(cache_dir, "astro_lons.pkl") if cache_dir else None
    if cache_file and os.path.exists(cache_file):
        with open(cache_file, "rb") as handle:
            return pickle.load(handle)

    result = {}
    for key, name in PLANET_NAME_MAP.items():
        result[key] = [get_body_helio_lon(name, dt) for dt in dates]
    result["pluto"] = [get_pluto_lon(dt) % 360 for dt in dates]
    result["sun"] = [get_sun_geo_lon(dt) for dt in dates]

    if cache_file:
        os.makedirs(cache_dir, exist_ok=True)
        with open(cache_file, "wb") as handle:
            pickle.dump(result, handle)

    return result
