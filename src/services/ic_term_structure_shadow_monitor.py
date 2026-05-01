from __future__ import annotations

import json
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SCRIPT_ROOT = PROJECT_ROOT / "scripts"
if str(SCRIPT_ROOT) not in sys.path:
    sys.path.insert(0, str(SCRIPT_ROOT))


@dataclass
class IcTermStructureShadowSummary:
    report_date: str
    snapshot_count: int
    term_snapshot_count: int
    candidate_count: int
    event_cluster_count: int
    paper_trade_count: int
    paper_trade_payout_count: int
    profitable_paper_trade_count: int
    dividend_season_candidate_count: int
    slow_bear_candidate_count: int
    option_proxy_linked_count: int
    positive_lead_count: int
    non_positive_lead_count: int
    missing_lead_count: int
    t5_drop_event_count: int
    net_defense_payoff_proxy: float | None
    latest_captured_at: str | None
    latest_front_end_gap_pct: float | None
    latest_q1_q2_annualized_pct: float | None


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            rows.append(json.loads(line))
        except Exception:
            continue
    return rows


def load_market_snapshots(intraday_archive_dir: str | Path) -> list[dict[str, Any]]:
    intraday_root = Path(intraday_archive_dir)
    rows: list[dict[str, Any]] = []
    for path in sorted(intraday_root.glob("*_market_snapshots.jsonl")):
        report_date = path.name.replace("_market_snapshots.jsonl", "")
        for row in _read_jsonl(path):
            row = dict(row)
            row["report_date"] = report_date
            rows.append(row)
    rows.sort(key=lambda item: str(item.get("captured_at") or ""))
    return rows


def build_term_structure_shadow_events(
    snapshots: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    for row in snapshots:
        term_signal = row.get("csi500_term_structure_shadow_signal") or {}
        if not isinstance(term_signal, dict) or not term_signal.get("candidate"):
            continue
        structure = row.get("csi500_term_structure") or {}
        option_proxy = row.get("csi500_option_proxy") or {}
        report_date = str(row.get("report_date") or "")
        month = int(report_date[5:7]) if len(report_date) >= 7 and report_date[5:7].isdigit() else None
        reference_price = option_proxy.get("otm_put_ask1")
        reference_price_source = option_proxy.get("otm_put_price_source") or "latest"
        if reference_price in (None, ""):
            reference_price = option_proxy.get("otm_put_price")
        event = {
            "event_key": f"{report_date}::{row.get('captured_at')}",
            "report_date": report_date,
            "captured_at": row.get("captured_at"),
            "shadow_stage": "shadow_monitoring",
            "rule_name": term_signal.get("rule_name"),
            "candidate": True,
            "before_cutoff": bool(term_signal.get("before_cutoff")),
            "front_end_gap_pct": term_signal.get("front_end_gap_pct"),
            "q1_q2_annualized_pct": term_signal.get("q1_q2_annualized_pct"),
            "anchor_stable": term_signal.get("anchor_stable"),
            "near_symbol": structure.get("near_symbol"),
            "next_symbol": structure.get("next_symbol"),
            "q1_symbol": structure.get("q1_symbol"),
            "q2_symbol": structure.get("q2_symbol"),
            "dividend_season_proxy": month in {5, 6, 7, 8},
            "paper_trade_action": "buy_next_month_otm_put_proxy",
            "paper_trade_reference_trade_code": option_proxy.get("otm_put_trade_code"),
            "paper_trade_reference_price": reference_price,
            "paper_trade_reference_strike": option_proxy.get("otm_put_strike"),
            "paper_trade_reference_expiry": option_proxy.get("expiry_ym"),
            "paper_trade_reference_days_to_expiry": option_proxy.get("otm_put_days_to_expiry"),
            "paper_trade_reference_quote_time": option_proxy.get("otm_put_quote_time"),
            "paper_trade_reference_bid1": option_proxy.get("otm_put_bid1"),
            "paper_trade_reference_ask1": option_proxy.get("otm_put_ask1"),
            "paper_trade_reference_last": option_proxy.get("otm_put_last_price"),
            "paper_trade_option_expiry_days_to_expiry": option_proxy.get("expiry_days_to_expiry"),
            "paper_trade_roll_window_shifted": bool(option_proxy.get("roll_window_shifted")),
            "paper_trade_proxy_source": option_proxy.get("source"),
            "paper_trade_cost_proxy": reference_price,
            "paper_trade_cost_proxy_type": f"otm_put_snapshot_{reference_price_source}",
            "paper_trade_max_payout_proxy": None,
            "paper_trade_payout_proxy_type": "future_snapshot_max_otm_put_price_5d",
            "notes": term_signal.get("reasons") or [],
        }
        events.append(event)
    events.sort(key=lambda item: str(item.get("captured_at") or ""))
    return events


def _build_snapshot_put_price_windows(snapshots: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in snapshots:
        option_proxy = row.get("csi500_option_proxy") or {}
        if not isinstance(option_proxy, dict):
            continue
        trade_code = option_proxy.get("otm_put_trade_code")
        price = option_proxy.get("otm_put_last_price")
        if price in (None, ""):
            price = option_proxy.get("otm_put_price")
        captured_at = row.get("captured_at")
        report_date = row.get("report_date")
        if not trade_code or price in (None, "") or not captured_at:
            continue
        try:
            ts = datetime.fromisoformat(str(captured_at))
            px = float(price)
        except Exception:
            continue
        rows.append(
            {
                "captured_at": ts,
                "report_date": str(report_date or ""),
                "trade_code": str(trade_code),
                "price": px,
            }
        )
    rows.sort(key=lambda item: item["captured_at"])
    return rows


def _build_event_clusters(events: list[dict[str, Any]], max_gap_days: int = 5) -> list[dict[str, Any]]:
    if not events:
        return []
    clustered: list[dict[str, Any]] = []
    cluster_id = 0
    previous_date: datetime | None = None
    for event in events:
        item = dict(event)
        try:
            event_dt = datetime.fromisoformat(str(item.get("captured_at")))
        except Exception:
            event_dt = None
        if previous_date is None or event_dt is None or (event_dt.date() - previous_date.date()).days > max_gap_days:
            cluster_id += 1
        item["event_cluster_id"] = f"m1m2-shadow-{cluster_id:04d}"
        clustered.append(item)
        if event_dt is not None:
            previous_date = event_dt
    return clustered


def build_daily_shadow_context_map(
    *,
    start: str,
    end: str,
    data_cache_dir: str | Path,
    refresh_data_cache: bool = False,
) -> tuple[dict[str, dict[str, Any]], list[str]]:
    from run_ic_full_return_label_validation import _build_tradable_frame
    from run_ic_second_confirmation_execution_validation import _build_second_confirmation_trigger

    frame = _build_tradable_frame(start, end, str(data_cache_dir), refresh_data_cache)
    trigger, _ = _build_second_confirmation_trigger(frame)
    frame = frame.copy()
    frame["date_key"] = pd.to_datetime(frame["date"]).dt.date.astype(str)
    frame["t1_spot_ret"] = frame["spot_close"].shift(-1) / frame["spot_close"] - 1.0
    frame["t3_spot_ret"] = frame["spot_close"].shift(-3) / frame["spot_close"] - 1.0
    frame["t5_spot_ret"] = frame["spot_close"].shift(-5) / frame["spot_close"] - 1.0
    frame["t1_carry_delta"] = frame["annualized_carry"].shift(-1) - frame["annualized_carry"]
    frame["t3_carry_delta"] = frame["annualized_carry"].shift(-3) - frame["annualized_carry"]
    frame["t5_carry_delta"] = frame["annualized_carry"].shift(-5) - frame["annualized_carry"]
    trailing_10d = frame["spot_close"] / frame["spot_close"].shift(10) - 1.0
    rolling_worst_10d = frame["spot_ret_1d"].rolling(10).min()
    frame["slow_bear_proxy"] = (
        (trailing_10d <= -0.03)
        & (trailing_10d >= -0.12)
        & (rolling_worst_10d > -0.02)
    )
    frame["second_confirmation"] = trigger.fillna(False)

    context: dict[str, dict[str, Any]] = {}
    for _, row in frame.iterrows():
        key = str(row["date_key"])
        context[key] = {
            "dividend_season": bool(row.get("dividend_season", 0)),
            "slow_bear_proxy": bool(row.get("slow_bear_proxy", False)),
            "trend_intact": int(row.get("trend_intact", 0) or 0),
            "annualized_carry": float(row.get("annualized_carry")) if pd.notna(row.get("annualized_carry")) else None,
            "t1_spot_ret": float(row.get("t1_spot_ret")) if pd.notna(row.get("t1_spot_ret")) else None,
            "t3_spot_ret": float(row.get("t3_spot_ret")) if pd.notna(row.get("t3_spot_ret")) else None,
            "t5_spot_ret": float(row.get("t5_spot_ret")) if pd.notna(row.get("t5_spot_ret")) else None,
            "t1_carry_delta": float(row.get("t1_carry_delta")) if pd.notna(row.get("t1_carry_delta")) else None,
            "t3_carry_delta": float(row.get("t3_carry_delta")) if pd.notna(row.get("t3_carry_delta")) else None,
            "t5_carry_delta": float(row.get("t5_carry_delta")) if pd.notna(row.get("t5_carry_delta")) else None,
            "second_confirmation": bool(row.get("second_confirmation", False)),
        }
    second_confirmation_dates = [key for key, value in context.items() if value.get("second_confirmation")]
    return context, second_confirmation_dates


def enrich_term_structure_shadow_events(
    events: list[dict[str, Any]],
    snapshots: list[dict[str, Any]],
    *,
    daily_context_by_date: dict[str, dict[str, Any]] | None = None,
    second_confirmation_dates: list[str] | None = None,
) -> list[dict[str, Any]]:
    clustered = _build_event_clusters(events)
    if not clustered:
        return []

    price_windows = _build_snapshot_put_price_windows(snapshots)
    confirmation_dates = sorted(second_confirmation_dates or [])
    trading_dates = sorted(daily_context_by_date.keys()) if daily_context_by_date else []
    trading_index = {d: idx for idx, d in enumerate(trading_dates)}

    enriched: list[dict[str, Any]] = []
    for event in clustered:
        item = dict(event)
        report_date = str(item.get("report_date") or "")
        event_ts = None
        try:
            event_ts = datetime.fromisoformat(str(item.get("captured_at")))
        except Exception:
            event_ts = None

        if event_ts is not None:
            max_window_end = event_ts + timedelta(days=5)
            trade_code = item.get("paper_trade_reference_trade_code")
            payout_candidates = [
                row["price"]
                for row in price_windows
                if row["captured_at"] > event_ts
                and row["captured_at"] <= max_window_end
                and (
                    (trade_code and row["trade_code"] == trade_code)
                    or (not trade_code and row["report_date"] >= report_date)
                )
            ]
            if payout_candidates:
                item["paper_trade_max_payout_proxy"] = max(payout_candidates)

        context = (daily_context_by_date or {}).get(report_date, {})
        item["dividend_season_proxy"] = bool(context.get("dividend_season", item.get("dividend_season_proxy", False)))
        item["slow_bear_proxy"] = bool(context.get("slow_bear_proxy", False))
        item["t1_spot_ret"] = context.get("t1_spot_ret")
        item["t3_spot_ret"] = context.get("t3_spot_ret")
        item["t5_spot_ret"] = context.get("t5_spot_ret")
        item["t1_carry_delta"] = context.get("t1_carry_delta")
        item["t3_carry_delta"] = context.get("t3_carry_delta")
        item["t5_carry_delta"] = context.get("t5_carry_delta")

        next_confirmation = next((d for d in confirmation_dates if d >= report_date), None)
        item["second_confirmation_date"] = next_confirmation
        if next_confirmation is not None and report_date in trading_index and next_confirmation in trading_index:
            item["lead_time_trading_days"] = int(trading_index[next_confirmation] - trading_index[report_date])
        else:
            item["lead_time_trading_days"] = None

        cost_proxy = item.get("paper_trade_cost_proxy")
        payout_proxy = item.get("paper_trade_max_payout_proxy")
        if cost_proxy not in (None, 0, "") and payout_proxy not in (None, ""):
            try:
                cost_value = float(cost_proxy)
                payout_value = float(payout_proxy)
            except Exception:
                cost_value = None
                payout_value = None
            if cost_value not in (None, 0) and payout_value is not None:
                item["paper_trade_pnl_proxy"] = payout_value - cost_value
                item["paper_trade_return_proxy"] = payout_value / cost_value - 1.0
            else:
                item["paper_trade_pnl_proxy"] = None
                item["paper_trade_return_proxy"] = None
        else:
            item["paper_trade_pnl_proxy"] = None
            item["paper_trade_return_proxy"] = None
        enriched.append(item)
    return enriched


def write_term_structure_shadow_events(path: str | Path, events: list[dict[str, Any]]) -> Path:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    lines = [json.dumps(item, ensure_ascii=False) for item in events]
    target.write_text(("\n".join(lines) + "\n") if lines else "", encoding="utf-8")
    return target


def build_ic_shadow_signal_event(event: dict[str, Any]):
    """Convert one IC term-structure shadow event into the unified P3 contract."""
    from src.services.signal_router import SignalEvent

    captured_at = str(event.get("captured_at") or "")
    report_date = str(event.get("report_date") or "")
    trade_code = event.get("paper_trade_reference_trade_code") or "无期权代理"
    cost = event.get("paper_trade_cost_proxy")
    payout = event.get("paper_trade_max_payout_proxy")
    lead = event.get("lead_time_trading_days")
    lines = [
        f"- 样本日: {report_date or '无'}",
        f"- 捕捉时间: {captured_at or '无'}",
        f"- 规则: {event.get('rule_name') or 'M1-M2前端塌陷 Shadow'}",
        f"- M1-M2前端差: {event.get('front_end_gap_pct')}",
        f"- Q1-Q2远季锚: {event.get('q1_q2_annualized_pct')}",
        f"- 纸面Put: {trade_code}",
        f"- 纸面成本代理: {cost if cost is not None else '无'}",
        f"- T+5最大赔付代理: {payout if payout is not None else '无'}",
        f"- Lead Time: {lead if lead is not None else '无'}",
        "",
        "路由口径: P3 Shadow，只归档，不提醒，不触发真实交易。",
    ]
    return SignalEvent(
        source="ic_term_structure_shadow",
        priority="P3",
        category="shadow",
        action="record",
        title="IC M1-M2 Shadow事件",
        content="\n".join(lines),
        reason=str(event.get("rule_name") or "m1_m2_front_end_collapse_shadow"),
        should_notify=False,
        channels=[],
        dedupe_key=str(event.get("event_key") or f"ic_shadow:{report_date}:{captured_at}"),
        created_at=captured_at or datetime.now().isoformat(timespec="seconds"),
        metadata={
            "report_date": report_date,
            "event_cluster_id": event.get("event_cluster_id"),
            "front_end_gap_pct": event.get("front_end_gap_pct"),
            "q1_q2_annualized_pct": event.get("q1_q2_annualized_pct"),
            "paper_trade_reference_trade_code": event.get("paper_trade_reference_trade_code"),
            "paper_trade_cost_proxy": event.get("paper_trade_cost_proxy"),
            "paper_trade_max_payout_proxy": event.get("paper_trade_max_payout_proxy"),
            "paper_trade_pnl_proxy": event.get("paper_trade_pnl_proxy"),
            "paper_trade_return_proxy": event.get("paper_trade_return_proxy"),
            "lead_time_trading_days": event.get("lead_time_trading_days"),
            "t5_spot_ret": event.get("t5_spot_ret"),
            "dividend_season_proxy": event.get("dividend_season_proxy"),
            "slow_bear_proxy": event.get("slow_bear_proxy"),
        },
    )


def write_ic_shadow_signal_events(path: str | Path, events: list[dict[str, Any]]) -> Path:
    """Write a deterministic routed P3 signal archive for IC Shadow events."""
    from src.services.signal_router import SignalRouter

    router = SignalRouter()
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    rows: list[str] = []
    for event in events:
        signal = build_ic_shadow_signal_event(event)
        rows.append(
            json.dumps(
                {
                    "archived_at": datetime.now().isoformat(timespec="seconds"),
                    "event": signal.to_dict(),
                    "decision": router.route(signal).to_dict(),
                },
                ensure_ascii=False,
                sort_keys=True,
            )
        )
    target.write_text(("\n".join(rows) + "\n") if rows else "", encoding="utf-8")
    return target


def summarize_term_structure_shadow_monitoring(
    snapshots: list[dict[str, Any]],
    events: list[dict[str, Any]],
) -> IcTermStructureShadowSummary:
    if snapshots:
        report_date = str(snapshots[-1].get("report_date") or "latest")
    else:
        report_date = "latest"
    term_rows = [row for row in snapshots if row.get("csi500_term_structure")]
    latest_term = term_rows[-1].get("csi500_term_structure") if term_rows else {}
    pnl_values = [float(item["paper_trade_pnl_proxy"]) for item in events if item.get("paper_trade_pnl_proxy") is not None]
    positive_pnl_sum = sum(value for value in pnl_values if value > 0)
    negative_pnl_sum = sum(-value for value in pnl_values if value < 0)
    profit_factor = None
    if positive_pnl_sum > 0 and negative_pnl_sum > 0:
        profit_factor = positive_pnl_sum / negative_pnl_sum
    elif positive_pnl_sum > 0 and negative_pnl_sum == 0:
        profit_factor = float("inf")

    return IcTermStructureShadowSummary(
        report_date=report_date,
        snapshot_count=len(snapshots),
        term_snapshot_count=len(term_rows),
        candidate_count=len(events),
        event_cluster_count=len({str(item.get("event_cluster_id") or "") for item in events if item.get("event_cluster_id")}),
        paper_trade_count=sum(1 for item in events if item.get("paper_trade_reference_trade_code")),
        paper_trade_payout_count=sum(1 for item in events if item.get("paper_trade_max_payout_proxy") is not None),
        profitable_paper_trade_count=sum(1 for item in events if (item.get("paper_trade_pnl_proxy") or 0.0) > 0),
        dividend_season_candidate_count=sum(1 for item in events if item.get("dividend_season_proxy")),
        slow_bear_candidate_count=sum(1 for item in events if item.get("slow_bear_proxy")),
        option_proxy_linked_count=sum(1 for item in events if item.get("paper_trade_proxy_source")),
        positive_lead_count=sum(1 for item in events if isinstance(item.get("lead_time_trading_days"), int) and int(item["lead_time_trading_days"]) > 0),
        non_positive_lead_count=sum(1 for item in events if isinstance(item.get("lead_time_trading_days"), int) and int(item["lead_time_trading_days"]) <= 0),
        missing_lead_count=sum(1 for item in events if item.get("lead_time_trading_days") is None),
        t5_drop_event_count=sum(1 for item in events if (item.get("t5_spot_ret") or 0.0) <= -0.02),
        net_defense_payoff_proxy=profit_factor,
        latest_captured_at=str(term_rows[-1].get("captured_at")) if term_rows else None,
        latest_front_end_gap_pct=latest_term.get("front_end_gap_pct") if latest_term else None,
        latest_q1_q2_annualized_pct=latest_term.get("q1_q2_annualized_pct") if latest_term else None,
    )


def render_term_structure_shadow_monitoring_summary(
    summary: IcTermStructureShadowSummary,
    events: list[dict[str, Any]],
) -> str:
    lines = [
        "# IC M1-M2 Shadow Monitoring 摘要",
        "",
        "- 当前状态: `shadow_monitoring`",
        "- 监控对象: `M1-M2前端塌陷>=2.05% (14:30前)`",
        "- 纸面动作: `若盘中亮灯，则记一笔“买入下月虚值 Put 保护”的纸面事件，不触发真实交易`",
        "",
        "## 今日概览",
        "",
        f"- 快照数: {summary.snapshot_count}",
        f"- 期限结构有效快照数: {summary.term_snapshot_count}",
        f"- 候选事件数: {summary.candidate_count}",
        f"- 事件簇数: {summary.event_cluster_count}",
        f"- 具备期权代理报价的纸面事件数: {summary.paper_trade_count}",
        f"- 具备后续纸面赔付代理的事件数: {summary.paper_trade_payout_count}",
        f"- 纸面 Put 代理盈利事件数: {summary.profitable_paper_trade_count}",
        f"- 位于分红季代理窗口的候选事件数: {summary.dividend_season_candidate_count}",
        f"- 位于慢熊代理窗口的候选事件数: {summary.slow_bear_candidate_count}",
        f"- 最新快照时间: {summary.latest_captured_at or '无'}",
        f"- 最新前端塌陷差值: {summary.latest_front_end_gap_pct if summary.latest_front_end_gap_pct is not None else '无'}",
        f"- 最新Q1-Q2远季锚: {summary.latest_q1_q2_annualized_pct if summary.latest_q1_q2_annualized_pct is not None else '无'}",
        "",
        "## Shadow 记分卡",
        "",
        f"- Lead Time 为正的事件数: {summary.positive_lead_count}",
        f"- Lead Time 非正的事件数: {summary.non_positive_lead_count}",
        f"- Lead Time 暂缺的事件数: {summary.missing_lead_count}",
        f"- T+5 现货下跌超过 2% 的事件数: {summary.t5_drop_event_count}",
        f"- 净防守赔率代理: {summary.net_defense_payoff_proxy if summary.net_defense_payoff_proxy is not None else '样本不足'}",
        "",
        "## 当前治理口径",
        "",
        "- 该规则已从 candidate 观察提升为 `shadow_monitoring` 监控层。",
        "- 当前只记录亮灯时点和纸面保护动作，不触发真实下单。",
        "- 当前北极星指标不再是 Sharpe，而是事件簇、防守赔率代理、Lead Time、分红季误报、慢熊误报。",
        "",
        "## 最近候选事件",
        "",
    ]
    if not events:
        lines.append("- 暂无盘中候选事件。")
    else:
        for event in events[-5:]:
            trade_code = event.get("paper_trade_reference_trade_code") or "无期权代理报价"
            price = event.get("paper_trade_reference_price")
            price_text = "无"
            if price is not None:
                price_text = f"{float(price):.4f}"
            payout = event.get("paper_trade_max_payout_proxy")
            payout_text = "无" if payout is None else f"{float(payout):.4f}"
            lead = event.get("lead_time_trading_days")
            lead_text = "无" if lead is None else str(int(lead))
            bid1 = event.get("paper_trade_reference_bid1")
            ask1 = event.get("paper_trade_reference_ask1")
            last_px = event.get("paper_trade_reference_last")
            bid1_text = "无" if bid1 is None else f"{float(bid1):.4f}"
            ask1_text = "无" if ask1 is None else f"{float(ask1):.4f}"
            last_text = "无" if last_px is None else f"{float(last_px):.4f}"
            dte = event.get("paper_trade_reference_days_to_expiry")
            dte_text = "无" if dte is None else str(int(dte))
            expiry_dte = event.get("paper_trade_option_expiry_days_to_expiry")
            expiry_dte_text = "无" if expiry_dte is None else str(int(expiry_dte))
            near_symbol = event.get("near_symbol") or "无"
            next_symbol = event.get("next_symbol") or "无"
            cost_type = event.get("paper_trade_cost_proxy_type") or "无"
            quote_time = event.get("paper_trade_reference_quote_time") or "无"
            roll_shift_text = "yes" if event.get("paper_trade_roll_window_shifted") else "no"
            lines.append(
                f"- {event.get('captured_at')} | front_end_gap={event.get('front_end_gap_pct')} | "
                f"anchor={event.get('q1_q2_annualized_pct')} | near/next={near_symbol}/{next_symbol} | "
                f"Put代理={trade_code} @ {price_text} ({cost_type}) -> {payout_text} | "
                f"ask1={ask1_text} bid1={bid1_text} last={last_text} dte={dte_text} expiry_dte={expiry_dte_text} roll_shift={roll_shift_text} quote={quote_time} | "
                f"LeadTime={lead_text} | T+5现货={event.get('t5_spot_ret')}"
            )
    lines.append("")
    return "\n".join(lines)


def refresh_term_structure_shadow_monitoring_outputs(
    *,
    intraday_archive_dir: str | Path,
    ledger_path: str | Path,
    latest_summary_path: str | Path,
    summary_path: str | Path | None = None,
    data_cache_dir: str | Path | None = None,
    refresh_data_cache: bool = False,
) -> dict[str, Any]:
    snapshots = load_market_snapshots(intraday_archive_dir)
    raw_events = build_term_structure_shadow_events(snapshots)

    if snapshots:
        first_date = datetime.fromisoformat(str(snapshots[0].get("report_date"))).date()
        last_date = datetime.fromisoformat(str(snapshots[-1].get("report_date"))).date()
        start = (first_date - timedelta(days=180)).isoformat()
        end = last_date.isoformat()
        if data_cache_dir is None:
            data_cache_dir = PROJECT_ROOT / ".cache" / "ic_basis_history"
        try:
            daily_context_by_date, second_confirmation_dates = build_daily_shadow_context_map(
                start=start,
                end=end,
                data_cache_dir=data_cache_dir,
                refresh_data_cache=refresh_data_cache,
            )
        except Exception:
            daily_context_by_date, second_confirmation_dates = {}, []
    else:
        daily_context_by_date, second_confirmation_dates = {}, []

    events = enrich_term_structure_shadow_events(
        raw_events,
        snapshots,
        daily_context_by_date=daily_context_by_date,
        second_confirmation_dates=second_confirmation_dates,
    )
    summary = summarize_term_structure_shadow_monitoring(snapshots, events)

    write_term_structure_shadow_events(ledger_path, events)
    signal_archive_path = PROJECT_ROOT / "reports" / "signal_events" / "ic_shadow_events.jsonl"
    write_ic_shadow_signal_events(signal_archive_path, events)

    resolved_summary_path = Path(summary_path) if summary_path else (
        PROJECT_ROOT / "reports" / "backtests" / f"{summary.report_date}_IC_M1-M2_shadow_monitoring摘要.md"
    )
    summary_text = render_term_structure_shadow_monitoring_summary(summary, events)
    resolved_summary_path.parent.mkdir(parents=True, exist_ok=True)
    resolved_summary_path.write_text(summary_text + "\n", encoding="utf-8")

    latest_path = Path(latest_summary_path)
    latest_path.parent.mkdir(parents=True, exist_ok=True)
    latest_path.write_text(summary_text + "\n", encoding="utf-8")

    return {
        "ledger_path": str(ledger_path),
        "summary_path": str(resolved_summary_path),
        "latest_summary_path": str(latest_path),
        "candidate_count": summary.candidate_count,
        "paper_trade_count": summary.paper_trade_count,
        "event_cluster_count": summary.event_cluster_count,
        "latest_front_end_gap_pct": summary.latest_front_end_gap_pct,
        "signal_archive_path": str(signal_archive_path),
        "signal_event_count": len(events),
    }
