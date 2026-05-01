"""Derived trigger signals for metaphysical research features."""

from __future__ import annotations

import numpy as np

from .constants import (
    FIRE_CONFLICT_DZ,
    GOLD_PURE_PAIRS,
    KUI_GANG,
    LUO_WANG_DZ,
    WATER_STEMS,
)


def is_hard_aspect(lon1, lon2, orb=3):
    diff = abs(lon1 - lon2) % 360
    if diff > 180:
        diff = 360 - diff
    for angle in [0, 90, 180]:
        if abs(diff - angle) < orb:
            return 1
    return 0


def is_soft_aspect(lon1, lon2, orb=3):
    diff = abs(lon1 - lon2) % 360
    if diff > 180:
        diff = 360 - diff
    for angle in [60, 120]:
        if abs(diff - angle) < orb:
            return 1
    return 0


def compute_triggers(df, planet_lons):
    df = df.copy()
    n = len(df)

    csi500_liquidity_crisis = np.zeros(n, dtype=int)
    for i in range(n):
        tg = df["tiangan"].iloc[i]
        dz = df["dizhi"].iloc[i]
        pair = df["tg_dz"].iloc[i]
        if tg in WATER_STEMS and dz in FIRE_CONFLICT_DZ:
            csi500_liquidity_crisis[i] = 1
        if pair in KUI_GANG:
            csi500_liquidity_crisis[i] = 1

    csi500_flash_crash = [
        is_hard_aspect(planet_lons["mars"][i], planet_lons["uranus"][i], orb=3)
        for i in range(n)
    ]

    csi500_capital_drain = np.zeros(n, dtype=int)
    for i in range(n):
        if is_hard_aspect(planet_lons["saturn"][i], planet_lons["pluto"][i], orb=4):
            csi500_capital_drain[i] = 1
        if is_hard_aspect(planet_lons["saturn"][i], planet_lons["neptune"][i], orb=4):
            csi500_capital_drain[i] = 1

    gold_panic_rush = np.zeros(n, dtype=int)
    for i in range(n):
        dz = df["dizhi"].iloc[i]
        pair = df["tg_dz"].iloc[i]
        if pair in GOLD_PURE_PAIRS:
            gold_panic_rush[i] = 1
        if dz in LUO_WANG_DZ:
            gold_panic_rush[i] = 1

    gold_macro_shock = [
        is_hard_aspect(planet_lons["jupiter"][i], planet_lons["pluto"][i], orb=3)
        for i in range(n)
    ]

    gold_currency_crisis = np.zeros(n, dtype=int)
    for i in range(n):
        if is_hard_aspect(planet_lons["uranus"][i], planet_lons["venus"][i], orb=2):
            gold_currency_crisis[i] = 1
        if is_hard_aspect(planet_lons["uranus"][i], planet_lons["sun"][i], orb=2):
            gold_currency_crisis[i] = 1

    df["csi500_liquidity_crisis"] = csi500_liquidity_crisis
    df["csi500_flash_crash"] = csi500_flash_crash
    df["csi500_capital_drain"] = csi500_capital_drain
    df["gold_panic_rush"] = gold_panic_rush
    df["gold_macro_shock"] = gold_macro_shock
    df["gold_currency_crisis"] = gold_currency_crisis
    return df
