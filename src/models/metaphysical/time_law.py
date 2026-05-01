"""Time-law helpers extracted from standalone quant research prototypes."""

from __future__ import annotations

import calendar
from dataclasses import dataclass
from datetime import datetime

from .constants import BRANCHES_CN, STEMS_CN
from .deps import get_ephem

STEM_ELEMENT = {
    "甲": "木", "乙": "木", "丙": "火", "丁": "火", "戊": "土",
    "己": "土", "庚": "金", "辛": "金", "壬": "水", "癸": "水",
}

BRANCH_ELEMENT = {
    "寅": "木", "卯": "木", "巳": "火", "午": "火", "辰": "土", "丑": "土",
    "戌": "土", "未": "土", "申": "金", "酉": "金", "亥": "水", "子": "水",
}

STEM_YIN_YANG = {
    "甲": "阳", "丙": "阳", "戊": "阳", "庚": "阳", "壬": "阳",
    "乙": "阴", "丁": "阴", "己": "阴", "辛": "阴", "癸": "阴",
}

ZODIAC = {
    "子": "鼠", "丑": "牛", "寅": "虎", "卯": "兔", "辰": "龙", "巳": "蛇",
    "午": "马", "未": "羊", "申": "猴", "酉": "鸡", "戌": "狗", "亥": "猪",
}

TEN_GODS_VS_METAL = {
    "甲": {"name": "偏财", "vibe": "外财", "impact": "题材资金活跃，短线机会增多"},
    "乙": {"name": "正财", "vibe": "实财", "impact": "蓝筹价值回归，现金流稳健"},
    "丙": {"name": "七杀", "vibe": "攻势", "impact": "趋势急进 / 加速，风险与机会并存"},
    "丁": {"name": "正官", "vibe": "规范", "impact": "政策与监管主导，金融秩序收紧"},
    "戊": {"name": "偏印", "vibe": "资源", "impact": "旧存量资金博弈，震荡筑底"},
    "己": {"name": "正印", "vibe": "庇护", "impact": "避险情绪上升，债券 / 贵金属受捧"},
    "庚": {"name": "比肩", "vibe": "同伴", "impact": "同业竞争，资金分散"},
    "辛": {"name": "劫财", "vibe": "消耗", "impact": "博弈加剧，强势股分化"},
    "壬": {"name": "食神", "vibe": "流通", "impact": "情绪宣泄，成交放量"},
    "癸": {"name": "伤官", "vibe": "创造", "impact": "创新主线，新产业轮动"},
}

SOLAR_TERMS_ORDER = [
    "小寒", "大寒", "立春", "雨水", "惊蛰", "春分",
    "清明", "谷雨", "立夏", "小满", "芒种", "夏至",
    "小暑", "大暑", "立秋", "处暑", "白露", "秋分",
    "寒露", "霜降", "立冬", "小雪", "大雪", "冬至",
]

TERM_BASE_DAY = [
    6.11, 20.84, 4.6295, 19.4599, 6.3826, 21.4155,
    5.59, 20.888, 6.318, 21.86, 6.5, 22.2,
    7.928, 23.65, 8.35, 23.95, 8.44, 23.822,
    9.098, 24.218, 8.218, 23.08, 7.9, 22.6,
]

CRITICAL_TERMS = {
    "立春", "春分", "清明", "立夏", "夏至",
    "白露", "立秋", "秋分", "立冬", "冬至",
}

TERM_NOTES = {
    "立春": "一年之气起点 · 趋势转折高概率窗口",
    "雨水": "资金解冻 · 流动性改善",
    "惊蛰": "情绪复苏 · 成交量抬升",
    "春分": "阴阳均衡 · 多空再平衡关键",
    "清明": "传统变盘节 · 方向选择窗口",
    "谷雨": "资金预期落地 · 震荡整理",
    "立夏": "趋势加速 · 动能释放",
    "小满": "短期顶部预警 · 避免追高",
    "芒种": "主升浪中继 · 择机加仓",
    "夏至": "阳极生阴 · 多头警戒",
    "小暑": "高位震荡 · 轮动加速",
    "大暑": "极端行情 · 波动率放大",
    "立秋": "阴气始生 · 趋势拐点",
    "处暑": "暑退秋凉 · 情绪降温",
    "白露": "传统秋季变盘节 · 方向选择",
    "秋分": "多空再平衡 · 月底结算关键",
    "寒露": "避险升温 · 资金流向防御",
    "霜降": "收敛行情 · 低位筑底",
    "立冬": "冬藏 · 趋势减速",
    "小雪": "缩量震荡 · 等待共振",
    "大雪": "年末躁动 · 做多窗口开启",
    "冬至": "一阳来复 · 黎明前的布局点",
    "小寒": "岁末博弈 · 波动放大",
    "大寒": "极冻反弹 · 春季躁动前奏",
}

MARS_EVENTS_2026_2027 = [
    {"date": "2026-04-22", "title": "火星入双子座", "level": "中", "interpretation": "信息与轮动加速，消息面主导短线，适合波段而非趋势"},
    {"date": "2026-06-17", "title": "火星入狮子座", "level": "强", "interpretation": "情绪扩张，题材主升浪窗口；注意节制杠杆"},
    {"date": "2026-08-14", "title": "火星合相木星（强扩张）", "level": "强", "interpretation": "资金风险偏好极速上升，易造就顶部或 FOMO 行情"},
    {"date": "2026-10-05", "title": "火星刑冥王星（剧烈冲突）", "level": "强", "interpretation": "系统性波动预警，警惕黑天鹅与深度回调"},
    {"date": "2026-11-23", "title": "火星入射手座", "level": "弱", "interpretation": "跨境资本再配置，海外资产相对活跃"},
    {"date": "2027-01-10", "title": "火星开始逆行", "level": "强", "interpretation": "动能衰竭 · 既有趋势进入反转倒计时，持仓减半为宜"},
    {"date": "2027-04-01", "title": "火星结束逆行", "level": "强", "interpretation": "新一轮趋势启动点，关注 4 月首个节气附近的开仓信号"},
]

SATURN_JUPITER_CONJUNCTION_DATES = [
    "1940-10-21",
    "1961-02-18",
    "1980-12-31",
    "2000-05-28",
    "2020-12-21",
    "2040-10-31",
]

URANUS_84Y_ANCHOR_DATES = [
    "1776-07-04",
    "1860-04-12",
    "1942-06-04",
    "2026-07-28",
]

URANUS_RETROGRADE_WINDOWS = [
    ("2016-07-29", "2016-12-29"),
    ("2017-08-03", "2018-01-02"),
    ("2018-08-07", "2019-01-06"),
    ("2019-08-12", "2020-01-10"),
    ("2020-08-15", "2021-01-14"),
    ("2021-08-19", "2022-01-18"),
    ("2022-08-24", "2023-01-22"),
    ("2023-08-29", "2024-01-27"),
    ("2024-09-01", "2025-01-30"),
    ("2025-09-06", "2026-02-04"),
    ("2026-09-09", "2027-02-07"),
    ("2027-09-13", "2028-02-10"),
]


def year_ganzhi(year: int) -> dict:
    """Approximate yearly ganzhi using the classic year-4 mapping."""
    n = year - 4
    stem = STEMS_CN[((n % 10) + 10) % 10]
    branch = BRANCHES_CN[((n % 12) + 12) % 12]
    return {
        "stem": stem,
        "branch": branch,
        "pillar": stem + branch,
        "element": STEM_ELEMENT[stem],
        "branch_element": BRANCH_ELEMENT.get(branch, "土"),
        "animal": ZODIAC[branch],
        "yin_yang": STEM_YIN_YANG[stem],
    }


def ten_gods_for(stem: str) -> dict:
    return TEN_GODS_VS_METAL.get(stem, {"name": "—", "vibe": "—", "impact": "—"})


def solar_term_date(year: int, term_index: int) -> datetime:
    y = (year - 1900) * 0.2422
    day_float = TERM_BASE_DAY[term_index] + y - int(y / 4)
    month = term_index // 2 + 1
    day = min(int(day_float), calendar.monthrange(year, month)[1])
    return datetime(year, month, day)


def solar_terms_in_range(from_date, to_date) -> list[dict]:
    start = datetime(from_date.year, from_date.month, from_date.day)
    end = datetime(to_date.year, to_date.month, to_date.day)
    result = []
    for year in range(start.year, end.year + 1):
        for idx, name in enumerate(SOLAR_TERMS_ORDER):
            term_date = solar_term_date(year, idx)
            if term_date < start or term_date > end:
                continue
            result.append(
                {
                    "name": name,
                    "date": term_date,
                    "critical": name in CRITICAL_TERMS,
                    "note": TERM_NOTES.get(name, ""),
                }
            )
    return sorted(result, key=lambda item: item["date"])


def mars_events_in_range(from_date, to_date) -> list[dict]:
    start = datetime(from_date.year, from_date.month, from_date.day)
    end = datetime(to_date.year, to_date.month, to_date.day)
    result = []
    for item in MARS_EVENTS_2026_2027:
        event_date = datetime.fromisoformat(item["date"])
        if start <= event_date <= end:
            result.append({**item, "date": event_date})
    return result


def nearest_lunar_phase_distance(dt) -> dict:
    """Return distances to the nearest new/full moon windows."""
    ephem = get_ephem()
    base = datetime(dt.year, dt.month, dt.day, 12, 0, 0)

    prev_new = ephem.previous_new_moon(base).datetime()
    next_new = ephem.next_new_moon(base).datetime()
    prev_full = ephem.previous_full_moon(base).datetime()
    next_full = ephem.next_full_moon(base).datetime()

    new_distance = min(abs((base - prev_new).days), abs((next_new - base).days))
    full_distance = min(abs((base - prev_full).days), abs((next_full - base).days))
    nearest_distance = min(new_distance, full_distance)
    nearest_phase = "new_moon" if new_distance <= full_distance else "full_moon"

    return {
        "new_moon_distance": int(new_distance),
        "full_moon_distance": int(full_distance),
        "nearest_lunar_phase_distance": int(nearest_distance),
        "nearest_lunar_phase": nearest_phase,
        "is_lunar_window": int(nearest_distance <= 3),
    }


def solar_term_distance(dt) -> dict:
    """Return the nearest solar-term distance and current term bucket."""
    base = datetime(dt.year, dt.month, dt.day)
    candidates = []
    for year in range(base.year - 1, base.year + 2):
        for idx, name in enumerate(SOLAR_TERMS_ORDER):
            term_date = solar_term_date(year, idx)
            candidates.append((name, term_date))

    nearest_name, nearest_date = min(candidates, key=lambda item: abs((item[1] - base).days))
    previous_candidates = [item for item in candidates if item[1] <= base]
    current_name, current_date = max(previous_candidates, key=lambda item: item[1]) if previous_candidates else min(candidates, key=lambda item: item[1])

    return {
        "nearest_solar_term": nearest_name,
        "nearest_solar_term_distance": int(abs((nearest_date - base).days)),
        "current_solar_term": current_name,
        "current_solar_term_distance": int((base - current_date).days),
        "is_critical_term_window": int(
            nearest_name in CRITICAL_TERMS and abs((nearest_date - base).days) <= 2
        ),
    }


def anniversary_cycle_distance(dt, anchor_dates) -> dict:
    """Return the nearest annual cycle distance to anchor dates."""
    base = datetime(dt.year, dt.month, dt.day)
    candidates = []
    for anchor in anchor_dates:
        anchor_dt = datetime(anchor.year, anchor.month, anchor.day)
        for year in (base.year - 1, base.year, base.year + 1):
            try:
                shifted = anchor_dt.replace(year=year)
            except ValueError:
                shifted = anchor_dt.replace(year=year, day=min(anchor_dt.day, 28))
            candidates.append(shifted)

    nearest = min(candidates, key=lambda item: abs((item - base).days))
    return {
        "anniversary_cycle_distance": int(abs((nearest - base).days)),
        "is_anniversary_window": int(abs((nearest - base).days) <= 5),
    }


def _nearest_anchor_distance(dt, anchor_dates, *, window_days: int) -> dict:
    base = datetime(dt.year, dt.month, dt.day)
    nearest = min(anchor_dates, key=lambda item: abs((item - base).days))
    distance = int(abs((nearest - base).days))
    return {
        "nearest_date": nearest,
        "distance_days": distance,
        "is_window": int(distance <= window_days),
    }


def saturn_jupiter_cycle_distance(dt) -> dict:
    """Distance to the nearest known Saturn-Jupiter conjunction window."""
    anchors = [datetime.fromisoformat(item) for item in SATURN_JUPITER_CONJUNCTION_DATES]
    nearest = _nearest_anchor_distance(dt, anchors, window_days=180)
    return {
        "saturn_jupiter_cycle_distance": nearest["distance_days"],
        "is_saturn_jupiter_cycle_window": nearest["is_window"],
        "nearest_saturn_jupiter_cycle_date": nearest["nearest_date"],
    }


def uranus_cycle_distance(dt) -> dict:
    """Distance to the nearest known 84-year Uranus-cycle anchor."""
    anchors = [datetime.fromisoformat(item) for item in URANUS_84Y_ANCHOR_DATES]
    nearest = _nearest_anchor_distance(dt, anchors, window_days=240)
    return {
        "uranus_cycle_84_distance": nearest["distance_days"],
        "is_uranus_cycle_window": nearest["is_window"],
        "nearest_uranus_cycle_date": nearest["nearest_date"],
    }


def uranus_retrograde_state(dt) -> dict:
    """Return whether the date falls inside an approximate Uranus retrograde window."""
    base = datetime(dt.year, dt.month, dt.day)
    parsed = [
        (datetime.fromisoformat(start), datetime.fromisoformat(end))
        for start, end in URANUS_RETROGRADE_WINDOWS
    ]
    in_window = 0
    nearest_boundary_distance = None
    for start, end in parsed:
        if start <= base <= end:
            in_window = 1
            nearest_boundary_distance = min((base - start).days, (end - base).days)
            break

    if nearest_boundary_distance is None:
        boundaries = [point for window in parsed for point in window]
        nearest_boundary_distance = min(abs((point - base).days) for point in boundaries)

    return {
        "uranus_retrograde_active": in_window,
        "uranus_retrograde_boundary_distance": int(nearest_boundary_distance),
    }


def batch_long_cycle_features(dates):
    """Batch long-cycle timing features for source-driven theory expansion."""
    records = []
    for dt in dates:
        saturn_jupiter = saturn_jupiter_cycle_distance(dt)
        uranus_cycle = uranus_cycle_distance(dt)
        uranus_retro = uranus_retrograde_state(dt)
        records.append(
            {
                "saturn_jupiter_cycle_distance": saturn_jupiter["saturn_jupiter_cycle_distance"],
                "is_saturn_jupiter_cycle_window": saturn_jupiter["is_saturn_jupiter_cycle_window"],
                "uranus_cycle_84_distance": uranus_cycle["uranus_cycle_84_distance"],
                "is_uranus_cycle_window": uranus_cycle["is_uranus_cycle_window"],
                "uranus_retrograde_active": uranus_retro["uranus_retrograde_active"],
                "uranus_retrograde_boundary_distance": uranus_retro["uranus_retrograde_boundary_distance"],
            }
        )
    return records
