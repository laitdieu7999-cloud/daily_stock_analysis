"""Lightweight intraday snapshot collector for local replay and review."""

from __future__ import annotations

import json
import logging
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Optional

from src.market_data_fetcher import MarketDataFetcher

logger = logging.getLogger(__name__)

INTRADAY_ARCHIVE_DIR = Path(__file__).resolve().parent.parent / "reports" / "intraday_archive"
SNAPSHOT_KEYWORDS = [
    "黄金", "白银", "中证500", "A股", "股指期货", "美债",
    "原油", "中东", "地缘", "关税", "非农", "CPI", "流动性", "避险",
]

ASCII_KEYWORD_PATTERNS = {
    "CPI": re.compile(r"\bCPI\b", re.IGNORECASE),
}

IC_SHADOW_SIGNAL_ZSCORE_THRESHOLD = 2.0
IC_SHADOW_SIGNAL_JUMP_THRESHOLD = 0.114
IC_SHADOW_SIGNAL_CUTOFF = "14:30:00"
IC_BASIS_ZSCORE_MIN_STD = 0.05
IC_TERM_FRONT_END_GAP_THRESHOLD = 2.05
IC_TERM_Q_ANCHOR_MEDIAN = -6.18
IC_TERM_Q_ANCHOR_BAND = 2.35
OPTION_PROXY_QVIX_ZSCORE_THRESHOLD = 2.0
OPTION_PROXY_QVIX_JUMP_THRESHOLD = 4.0
OPTION_PROXY_SKEW_RATIO_THRESHOLD = 0.55
OPTION_PROXY_VOLUME_RATIO_THRESHOLD = 1.80
IC_SHADOW_CONFIRMATION_STREAK = 3
IC_SHADOW_COOLDOWN_MINUTES = 30
IC_SHADOW_SILENT_WINDOWS = (
    ("09:25:00", "09:30:00"),
    ("14:57:00", "15:00:00"),
)


def _refresh_term_structure_shadow_monitoring() -> Optional[Dict[str, Any]]:
    try:
        from src.services.ic_term_structure_shadow_monitor import (
            PROJECT_ROOT,
            refresh_term_structure_shadow_monitoring_outputs,
        )
    except Exception:
        logger.exception("[IntradayCollector] 加载 M1-M2 shadow 刷新器失败")
        return None

    try:
        return refresh_term_structure_shadow_monitoring_outputs(
            intraday_archive_dir=INTRADAY_ARCHIVE_DIR,
            ledger_path=PROJECT_ROOT / "reports" / "ic_m1_m2_shadow_monitoring_events.jsonl",
            latest_summary_path=PROJECT_ROOT / "reports" / "backtests" / "latest_ic_m1_m2_shadow_monitoring.md",
            data_cache_dir=PROJECT_ROOT / ".cache" / "ic_basis_history",
            refresh_data_cache=False,
        )
    except Exception:
        logger.exception("[IntradayCollector] 刷新 M1-M2 shadow 账本失败")
        return None


def _jsonl_append(path: Path, row: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(row, ensure_ascii=False) + "\n")


def _load_jsonl_rows(path: Path) -> list[Dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[Dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            payload = json.loads(line)
        except Exception:
            continue
        if isinstance(payload, dict):
            rows.append(payload)
    return rows


def _load_json_payload(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _save_json_payload(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _resolve_ic_shadow_state_path() -> Path:
    return INTRADAY_ARCHIVE_DIR.parent / "ic_shadow_monitor_state.json"


def _is_in_silent_window(captured_time: Optional[str]) -> bool:
    if not captured_time:
        return False
    return any(start <= captured_time <= end for start, end in IC_SHADOW_SILENT_WINDOWS)


def _apply_shadow_signal_state_lock(
    signal: Optional[Dict[str, Any]],
    *,
    rule_key: str,
    state: Dict[str, Any],
    captured_at: Optional[str],
) -> Optional[Dict[str, Any]]:
    if not signal:
        return signal

    item = dict(signal)
    captured_ts = None
    if captured_at:
        try:
            captured_ts = datetime.fromisoformat(str(captured_at))
        except Exception:
            captured_ts = None
    captured_time = item.get("captured_time")
    today = captured_ts.date().isoformat() if captured_ts is not None else datetime.now().date().isoformat()

    if state.get("date") != today:
        state.clear()
        state.update({"date": today, "rules": {}})

    rules = state.setdefault("rules", {})
    rule_state = rules.setdefault(rule_key, {})
    streak = int(rule_state.get("streak") or 0)
    cooldown_until_text = str(rule_state.get("cooldown_until") or "")
    cooldown_until = None
    if cooldown_until_text:
        try:
            cooldown_until = datetime.fromisoformat(cooldown_until_text)
        except Exception:
            cooldown_until = None

    raw_candidate = bool(item.get("candidate"))
    silent_window_active = _is_in_silent_window(str(captured_time or ""))
    cooldown_active = bool(captured_ts and cooldown_until and captured_ts < cooldown_until)

    if silent_window_active:
        item["candidate"] = False
        item["raw_candidate"] = raw_candidate
        item["silent_window_active"] = True
        item["cooldown_active"] = cooldown_active
        item["confirmation_required"] = IC_SHADOW_CONFIRMATION_STREAK
        item["confirmation_count"] = streak
        item.setdefault("reasons", []).append("位于集合竞价/尾盘垃圾时间，静默跳过")
        rule_state["streak"] = 0
        return item

    next_streak = (streak + 1) if raw_candidate else 0
    confirmed = raw_candidate and not cooldown_active and next_streak >= IC_SHADOW_CONFIRMATION_STREAK

    item["raw_candidate"] = raw_candidate
    item["cooldown_active"] = cooldown_active
    item["silent_window_active"] = False
    item["confirmation_required"] = IC_SHADOW_CONFIRMATION_STREAK
    item["confirmation_count"] = next_streak if raw_candidate else 0
    if cooldown_until is not None:
        item["cooldown_until"] = cooldown_until.isoformat()

    if cooldown_active:
        item["candidate"] = False
        item.setdefault("reasons", []).append("命中冷却期，本轮仅记原始信号不升级为候选")
    elif confirmed:
        item["candidate"] = True
        cooldown_until = (captured_ts or datetime.now()) + timedelta(minutes=IC_SHADOW_COOLDOWN_MINUTES)
        item["cooldown_until"] = cooldown_until.isoformat()
        item["cooldown_minutes"] = IC_SHADOW_COOLDOWN_MINUTES
        item.setdefault("reasons", []).append(
            f"已通过连续{IC_SHADOW_CONFIRMATION_STREAK}个快照确认，升级为正式候选"
        )
    else:
        item["candidate"] = False
        if raw_candidate:
            item.setdefault("reasons", []).append(
                f"原始信号已命中，但连续确认仅 {next_streak}/{IC_SHADOW_CONFIRMATION_STREAK}"
            )

    rule_state["streak"] = 0 if confirmed else next_streak
    rule_state["cooldown_until"] = cooldown_until.isoformat() if cooldown_until is not None else ""
    rule_state["last_captured_at"] = str(captured_at or "")
    return item


def _load_recorded_event_ids(path: Path) -> set[str]:
    ids: set[str] = set()
    for payload in _load_jsonl_rows(path):
        event_key = _build_event_key(
            str(payload.get("id") or ""),
            str(payload.get("headline") or ""),
            str(payload.get("time") or ""),
        )
        if event_key:
            ids.add(event_key)
    return ids


def _build_event_key(event_id: str, headline: str, event_time: str) -> str:
    clean_id = (event_id or "").strip()
    if clean_id:
        return clean_id
    return f"{event_time.strip()}|{headline.strip()}"


def _find_matching_keywords(headline: str) -> list[str]:
    lowered = headline.lower()
    hit_keywords = [kw for kw in SNAPSHOT_KEYWORDS if kw.lower() in lowered]
    for keyword, pattern in ASCII_KEYWORD_PATTERNS.items():
        if pattern.search(headline):
            hit_keywords.append(keyword)
    return hit_keywords[:4]


def _build_price_snapshot(fetcher: MarketDataFetcher) -> Dict[str, Any]:
    quotes = fetcher.get_gold_silver_quotes()
    gold = quotes.get("XAUUSD")
    silver = quotes.get("XAGUSD")
    ic_basis = fetcher.get_ic_basis()
    ic_term_structure = fetcher.get_ic_term_structure()
    option_proxy = fetcher.get_500etf_option_proxy()
    golden_dragon = fetcher.get_nasdaq_golden_dragon_snapshot()

    payload: Dict[str, Any] = {
        "captured_at": datetime.now().isoformat(),
        "gold": None,
        "silver": None,
        "csi500_basis": None,
        "csi500_term_structure": None,
        "csi500_option_proxy": None,
        "golden_dragon": golden_dragon,
    }
    if gold:
        payload["gold"] = {
            "price": gold.price,
            "change_pct": gold.change_pct,
            "high": gold.high,
            "low": gold.low,
            "time": gold.time,
        }
    if silver:
        payload["silver"] = {
            "price": silver.price,
            "change_pct": silver.change_pct,
            "high": silver.high,
            "low": silver.low,
            "time": silver.time,
        }
    if ic_basis:
        payload["csi500_basis"] = {
            "spot_price": ic_basis.spot_price,
            "futures_price": ic_basis.futures_price,
            "basis": ic_basis.basis,
            "annualized_basis_pct": ic_basis.annualized_basis_pct,
            "contract_code": ic_basis.contract_code,
            "days_to_expiry": ic_basis.days_to_expiry,
        }
    if ic_term_structure:
        payload["csi500_term_structure"] = {
            "near_symbol": ic_term_structure.near_symbol,
            "near_price": ic_term_structure.near_price,
            "near_days": ic_term_structure.near_days,
            "next_symbol": ic_term_structure.next_symbol,
            "next_price": ic_term_structure.next_price,
            "next_days": ic_term_structure.next_days,
            "m1_m2_annualized_pct": ic_term_structure.m1_m2_annualized_pct,
            "q1_symbol": ic_term_structure.q1_symbol,
            "q1_price": ic_term_structure.q1_price,
            "q1_days": ic_term_structure.q1_days,
            "q2_symbol": ic_term_structure.q2_symbol,
            "q2_price": ic_term_structure.q2_price,
            "q2_days": ic_term_structure.q2_days,
            "q1_q2_annualized_pct": ic_term_structure.q1_q2_annualized_pct,
            "front_end_gap_pct": ic_term_structure.front_end_gap_pct,
        }
    if option_proxy:
        payload["csi500_option_proxy"] = {
            "board_timestamp": option_proxy.board_timestamp,
            "expiry_ym": option_proxy.expiry_ym,
            "expiry_style": option_proxy.expiry_style,
            "qvix_latest": option_proxy.qvix_latest,
            "qvix_prev": option_proxy.qvix_prev,
            "qvix_jump_pct": option_proxy.qvix_jump_pct,
            "qvix_zscore": option_proxy.qvix_zscore,
            "atm_strike": option_proxy.atm_strike,
            "atm_call_trade_code": option_proxy.atm_call_trade_code,
            "atm_call_price": option_proxy.atm_call_price,
            "atm_put_trade_code": option_proxy.atm_put_trade_code,
            "atm_put_price": option_proxy.atm_put_price,
            "atm_put_last_price": getattr(option_proxy, "atm_put_last_price", None),
            "atm_put_bid1": getattr(option_proxy, "atm_put_bid1", None),
            "atm_put_ask1": getattr(option_proxy, "atm_put_ask1", None),
            "atm_put_quote_time": getattr(option_proxy, "atm_put_quote_time", ""),
            "atm_put_days_to_expiry": getattr(option_proxy, "atm_put_days_to_expiry", None),
            "atm_put_price_source": getattr(option_proxy, "atm_put_price_source", "latest"),
            "otm_put_trade_code": option_proxy.otm_put_trade_code,
            "otm_put_strike": option_proxy.otm_put_strike,
            "otm_put_price": option_proxy.otm_put_price,
            "otm_put_last_price": getattr(option_proxy, "otm_put_last_price", None),
            "otm_put_bid1": getattr(option_proxy, "otm_put_bid1", None),
            "otm_put_ask1": getattr(option_proxy, "otm_put_ask1", None),
            "otm_put_quote_time": getattr(option_proxy, "otm_put_quote_time", ""),
            "otm_put_days_to_expiry": getattr(option_proxy, "otm_put_days_to_expiry", None),
            "otm_put_price_source": getattr(option_proxy, "otm_put_price_source", "latest"),
            "expiry_days_to_expiry": getattr(option_proxy, "expiry_days_to_expiry", None),
            "roll_window_shifted": getattr(option_proxy, "roll_window_shifted", False),
            "put_skew_ratio": option_proxy.put_skew_ratio,
            "atm_put_call_volume_ratio": option_proxy.atm_put_call_volume_ratio,
            "atm_put_volume": option_proxy.atm_put_volume,
            "atm_call_volume": option_proxy.atm_call_volume,
            "source": option_proxy.source,
        }
    return payload


def _build_basis_signal(snapshot: Dict[str, Any], prior_rows: list[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    current = snapshot.get("csi500_basis") or {}
    if not current or current.get("annualized_basis_pct") is None:
        return None

    history = [
        row.get("csi500_basis", {}).get("annualized_basis_pct")
        for row in prior_rows
        if isinstance(row, dict) and isinstance(row.get("csi500_basis"), dict)
    ]
    history = [float(value) for value in history if value is not None]
    current_annualized = float(current["annualized_basis_pct"])
    current_basis = float(current.get("basis") or 0.0)
    prev_value = history[-1] if history else None
    window = history[-12:]
    mean_value = sum(window) / len(window) if window else current_annualized
    variance = (
        sum((value - mean_value) ** 2 for value in window) / len(window)
        if len(window) >= 2
        else 0.0
    )
    std_value = variance ** 0.5
    delta_vs_prev = current_annualized - prev_value if prev_value is not None else 0.0
    effective_std = std_value if std_value >= IC_BASIS_ZSCORE_MIN_STD else 0.0
    zscore = (current_annualized - mean_value) / effective_std if effective_std > 0 else 0.0

    triggered = False
    severity = "normal"
    reasons: list[str] = []
    if len(window) >= 3:
        if current_annualized >= max(10.0, mean_value + max(2.0, 2 * std_value)):
            reasons.append("年化贴水显著高于盘中均值")
        if delta_vs_prev >= max(1.5, std_value):
            reasons.append("年化贴水单次跳升")
        if zscore >= 2.0:
            reasons.append("贴水zscore异常偏高")
        triggered = bool(reasons)
        if triggered:
            severity = "critical" if len(reasons) >= 2 else "warning"

    return {
        "annualized_basis_pct": round(current_annualized, 2),
        "basis": round(current_basis, 2),
        "previous_annualized_basis_pct": round(prev_value, 2) if prev_value is not None else None,
        "delta_vs_prev": round(delta_vs_prev, 2),
        "rolling_mean": round(mean_value, 2),
        "rolling_std": round(std_value, 2),
        "rolling_std_floor_applied": bool(std_value < IC_BASIS_ZSCORE_MIN_STD),
        "zscore": round(zscore, 2),
        "history_size": len(window),
        "severity": severity,
        "triggered": triggered,
        "reasons": reasons,
    }


def _build_shadow_basis_signal(snapshot: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    signal = snapshot.get("csi500_basis_signal") or {}
    if not signal:
        return None

    captured_at = str(snapshot.get("captured_at") or "")
    captured_time = None
    if captured_at:
        try:
            captured_time = datetime.fromisoformat(captured_at).strftime("%H:%M:%S")
        except Exception:
            captured_time = None

    zscore = float(signal.get("zscore") or 0.0)
    abs_jump = abs(float(signal.get("delta_vs_prev") or 0.0))
    before_cutoff = bool(captured_time and captured_time <= IC_SHADOW_SIGNAL_CUTOFF)
    candidate = before_cutoff and zscore >= IC_SHADOW_SIGNAL_ZSCORE_THRESHOLD and abs_jump >= IC_SHADOW_SIGNAL_JUMP_THRESHOLD

    reasons: list[str] = []
    if before_cutoff and zscore >= IC_SHADOW_SIGNAL_ZSCORE_THRESHOLD:
        reasons.append("盘中基差zscore达到影子阈值")
    if before_cutoff and abs_jump >= IC_SHADOW_SIGNAL_JUMP_THRESHOLD:
        reasons.append("盘中基差跳升达到影子阈值")
    if not before_cutoff:
        reasons.append("触发时间晚于14:30，仅记录不计入影子候选")

    return {
        "rule_name": "z>=2.0 & |jump|>=0.114 (14:30前)",
        "candidate": candidate,
        "captured_time": captured_time,
        "before_cutoff": before_cutoff,
        "zscore_threshold": IC_SHADOW_SIGNAL_ZSCORE_THRESHOLD,
        "jump_threshold": IC_SHADOW_SIGNAL_JUMP_THRESHOLD,
        "zscore": round(zscore, 2),
        "abs_jump": round(abs_jump, 3),
        "reasons": reasons,
    }


def _build_term_structure_shadow_signal(snapshot: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    structure = snapshot.get("csi500_term_structure") or {}
    if not structure or structure.get("front_end_gap_pct") is None:
        return None

    captured_at = str(snapshot.get("captured_at") or "")
    captured_time = None
    if captured_at:
        try:
            captured_time = datetime.fromisoformat(captured_at).strftime("%H:%M:%S")
        except Exception:
            captured_time = None

    front_end_gap_pct = float(structure.get("front_end_gap_pct") or 0.0)
    q_anchor = structure.get("q1_q2_annualized_pct")
    q_anchor_pct = float(q_anchor) if q_anchor is not None else None
    anchor_stable = (
        q_anchor_pct is not None
        and abs(q_anchor_pct - IC_TERM_Q_ANCHOR_MEDIAN) <= IC_TERM_Q_ANCHOR_BAND
    )
    before_cutoff = bool(captured_time and captured_time <= IC_SHADOW_SIGNAL_CUTOFF)
    candidate = before_cutoff and front_end_gap_pct >= IC_TERM_FRONT_END_GAP_THRESHOLD

    reasons: list[str] = []
    if before_cutoff and front_end_gap_pct >= IC_TERM_FRONT_END_GAP_THRESHOLD:
        reasons.append("M1-M2前端塌陷达到影子阈值")
    if q_anchor_pct is None:
        reasons.append("远季锚缺失，仅记录前端结构")
    elif anchor_stable:
        reasons.append("Q1-Q2远季锚保持稳定")
    else:
        reasons.append("Q1-Q2远季锚偏离常态")
    if not before_cutoff:
        reasons.append("触发时间晚于14:30，仅记录不计入影子候选")

    return {
        "rule_name": "M1-M2前端塌陷>=2.05% (14:30前)",
        "candidate": candidate,
        "captured_time": captured_time,
        "before_cutoff": before_cutoff,
        "front_end_gap_threshold": IC_TERM_FRONT_END_GAP_THRESHOLD,
        "front_end_gap_pct": round(front_end_gap_pct, 2),
        "q_anchor_median": IC_TERM_Q_ANCHOR_MEDIAN,
        "q_anchor_band": IC_TERM_Q_ANCHOR_BAND,
        "q1_q2_annualized_pct": round(q_anchor_pct, 2) if q_anchor_pct is not None else None,
        "anchor_stable": anchor_stable,
        "reasons": reasons,
    }


def _build_option_proxy_shadow_signal(snapshot: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    proxy = snapshot.get("csi500_option_proxy") or {}
    if not proxy:
        return None

    captured_at = str(snapshot.get("captured_at") or "")
    captured_time = None
    if captured_at:
        try:
            captured_time = datetime.fromisoformat(captured_at).strftime("%H:%M:%S")
        except Exception:
            captured_time = None

    qvix_zscore = float(proxy.get("qvix_zscore") or 0.0)
    qvix_jump_pct = float(proxy.get("qvix_jump_pct") or 0.0)
    put_skew_ratio = float(proxy.get("put_skew_ratio") or 0.0)
    volume_ratio_raw = proxy.get("atm_put_call_volume_ratio")
    volume_ratio = float(volume_ratio_raw) if volume_ratio_raw is not None else None

    qvix_flag = qvix_zscore >= OPTION_PROXY_QVIX_ZSCORE_THRESHOLD or qvix_jump_pct >= OPTION_PROXY_QVIX_JUMP_THRESHOLD
    skew_flag = put_skew_ratio >= OPTION_PROXY_SKEW_RATIO_THRESHOLD
    volume_flag = volume_ratio is not None and volume_ratio >= OPTION_PROXY_VOLUME_RATIO_THRESHOLD

    before_cutoff = bool(captured_time and captured_time <= IC_SHADOW_SIGNAL_CUTOFF)
    flag_count = int(qvix_flag) + int(skew_flag) + int(volume_flag)
    candidate = before_cutoff and (flag_count >= 2 or (qvix_flag and skew_flag))

    reasons: list[str] = []
    if qvix_flag:
        reasons.append("500ETF期权Qvix异常上冲")
    if skew_flag:
        reasons.append("虚平认沽价格比抬升")
    if volume_flag:
        reasons.append("ATM沽/购成交量比抬升")
    if not reasons:
        reasons.append("期权代理指标尚未形成共振")
    if not before_cutoff:
        reasons.append("触发时间晚于14:30，仅记录不计入影子候选")

    return {
        "rule_name": "500ETF期权代理共振 (14:30前)",
        "candidate": candidate,
        "captured_time": captured_time,
        "before_cutoff": before_cutoff,
        "qvix_zscore_threshold": OPTION_PROXY_QVIX_ZSCORE_THRESHOLD,
        "qvix_jump_threshold": OPTION_PROXY_QVIX_JUMP_THRESHOLD,
        "skew_ratio_threshold": OPTION_PROXY_SKEW_RATIO_THRESHOLD,
        "volume_ratio_threshold": OPTION_PROXY_VOLUME_RATIO_THRESHOLD,
        "qvix_zscore": round(qvix_zscore, 2),
        "qvix_jump_pct": round(qvix_jump_pct, 2),
        "put_skew_ratio": round(put_skew_ratio, 3),
        "atm_put_call_volume_ratio": round(volume_ratio, 2) if volume_ratio is not None else None,
        "flag_count": flag_count,
        "reasons": reasons,
    }


def _extract_major_flash_events(fetcher: MarketDataFetcher) -> list[Dict[str, Any]]:
    flash_list = fetcher.list_flash(limit=50)
    if not flash_list:
        return []
    matched: list[Dict[str, Any]] = []
    for item in flash_list:
        if not isinstance(item, dict):
            continue
        headline = str(item.get("title", "") or item.get("content", "") or "").strip()
        if not headline:
            continue
        hit_keywords = _find_matching_keywords(headline)
        if not hit_keywords:
            continue
        event_id = str(item.get("id", "") or item.get("data_id", "") or "").strip()
        event_time = str(item.get("time", "") or "").strip()
        matched.append(
            {
                "id": event_id,
                "event_key": _build_event_key(event_id, headline, event_time),
                "headline": headline,
                "time": event_time,
                "keywords": hit_keywords[:4],
                "captured_at": datetime.now().isoformat(),
            }
        )
        if len(matched) >= 10:
            break
    return matched


def collect_intraday_snapshots(
    *,
    jin10_api_key: str = "",
    jin10_x_token: str = "",
) -> Optional[Dict[str, Any]]:
    """Collect one lightweight local replay snapshot for intraday review."""
    fetcher = MarketDataFetcher(jin10_api_key, jin10_x_token)
    today = datetime.now().strftime("%Y-%m-%d")
    snapshot_path = INTRADAY_ARCHIVE_DIR / f"{today}_market_snapshots.jsonl"
    event_path = INTRADAY_ARCHIVE_DIR / f"{today}_jin10_events.jsonl"
    basis_signal_path = INTRADAY_ARCHIVE_DIR / f"{today}_ic_basis_signals.jsonl"
    shadow_signal_path = INTRADAY_ARCHIVE_DIR / f"{today}_ic_basis_shadow_signals.jsonl"
    term_shadow_signal_path = INTRADAY_ARCHIVE_DIR / f"{today}_ic_term_structure_shadow_signals.jsonl"
    option_shadow_signal_path = INTRADAY_ARCHIVE_DIR / f"{today}_ic_option_proxy_shadow_signals.jsonl"
    try:
        prior_rows = _load_jsonl_rows(snapshot_path)
        shadow_state_path = _resolve_ic_shadow_state_path()
        shadow_state = _load_json_payload(shadow_state_path)
        snapshot = _build_price_snapshot(fetcher)
        basis_signal = _build_basis_signal(snapshot, prior_rows)
        if basis_signal:
            snapshot["csi500_basis_signal"] = basis_signal
        shadow_signal = _build_shadow_basis_signal(snapshot)
        if shadow_signal:
            shadow_signal = _apply_shadow_signal_state_lock(
                shadow_signal,
                rule_key="basis_shadow",
                state=shadow_state,
                captured_at=snapshot.get("captured_at"),
            )
            snapshot["csi500_basis_shadow_signal"] = shadow_signal
        term_shadow_signal = _build_term_structure_shadow_signal(snapshot)
        if term_shadow_signal:
            term_shadow_signal = _apply_shadow_signal_state_lock(
                term_shadow_signal,
                rule_key="term_structure_shadow",
                state=shadow_state,
                captured_at=snapshot.get("captured_at"),
            )
            snapshot["csi500_term_structure_shadow_signal"] = term_shadow_signal
        option_shadow_signal = _build_option_proxy_shadow_signal(snapshot)
        if option_shadow_signal:
            option_shadow_signal = _apply_shadow_signal_state_lock(
                option_shadow_signal,
                rule_key="option_proxy_shadow",
                state=shadow_state,
                captured_at=snapshot.get("captured_at"),
            )
            snapshot["csi500_option_proxy_shadow_signal"] = option_shadow_signal
        _save_json_payload(shadow_state_path, shadow_state)
        _jsonl_append(snapshot_path, snapshot)

        new_events = 0
        new_basis_signal_count = 0
        new_shadow_signal_count = 0
        new_term_shadow_signal_count = 0
        new_option_shadow_signal_count = 0
        if jin10_api_key:
            recorded_ids = _load_recorded_event_ids(event_path)
            for event in _extract_major_flash_events(fetcher):
                event_key = _build_event_key(
                    str(event.get("id") or ""),
                    str(event.get("headline") or ""),
                    str(event.get("time") or ""),
                )
                if event_key in recorded_ids:
                    continue
                _jsonl_append(event_path, event)
                recorded_ids.add(event_key)
                new_events += 1

        if basis_signal and basis_signal.get("triggered"):
            _jsonl_append(
                basis_signal_path,
                {
                    "captured_at": snapshot.get("captured_at"),
                    "contract_code": (snapshot.get("csi500_basis") or {}).get("contract_code"),
                    "basis_signal": basis_signal,
                    "csi500_basis": snapshot.get("csi500_basis"),
                },
            )
            new_basis_signal_count = 1
        if shadow_signal and shadow_signal.get("candidate"):
            _jsonl_append(
                shadow_signal_path,
                {
                    "captured_at": snapshot.get("captured_at"),
                    "contract_code": (snapshot.get("csi500_basis") or {}).get("contract_code"),
                    "shadow_signal": shadow_signal,
                    "csi500_basis": snapshot.get("csi500_basis"),
                },
            )
            new_shadow_signal_count = 1
        if term_shadow_signal and term_shadow_signal.get("candidate"):
            _jsonl_append(
                term_shadow_signal_path,
                {
                    "captured_at": snapshot.get("captured_at"),
                    "contract_code": (snapshot.get("csi500_basis") or {}).get("contract_code"),
                    "term_structure_signal": term_shadow_signal,
                    "csi500_term_structure": snapshot.get("csi500_term_structure"),
                },
            )
            new_term_shadow_signal_count = 1
        if option_shadow_signal and option_shadow_signal.get("candidate"):
            _jsonl_append(
                option_shadow_signal_path,
                {
                    "captured_at": snapshot.get("captured_at"),
                    "contract_code": (snapshot.get("csi500_basis") or {}).get("contract_code"),
                    "option_proxy_signal": option_shadow_signal,
                    "csi500_option_proxy": snapshot.get("csi500_option_proxy"),
                },
            )
            new_option_shadow_signal_count = 1

        shadow_monitoring_payload = _refresh_term_structure_shadow_monitoring()

        return {
            "snapshot_path": str(snapshot_path),
            "event_path": str(event_path),
            "basis_signal_path": str(basis_signal_path),
            "shadow_signal_path": str(shadow_signal_path),
            "term_shadow_signal_path": str(term_shadow_signal_path),
            "option_shadow_signal_path": str(option_shadow_signal_path),
            "shadow_state_path": str(shadow_state_path),
            "snapshot": snapshot,
            "new_event_count": new_events,
            "new_basis_signal_count": new_basis_signal_count,
            "new_shadow_signal_count": new_shadow_signal_count,
            "new_term_shadow_signal_count": new_term_shadow_signal_count,
            "new_option_shadow_signal_count": new_option_shadow_signal_count,
            "shadow_monitoring_payload": shadow_monitoring_payload,
        }
    except Exception as exc:
        logger.exception("[IntradayCollector] 采集分钟级快照失败: %s", exc)
        return None
    finally:
        fetcher.close()
