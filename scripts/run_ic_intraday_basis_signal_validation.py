#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Summarize intraday IC basis anomaly signals and their next-day CSI500 behavior."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_DIR = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from run_ic_basis_overlay_validation import _load_spot_history  # noqa: E402

SHADOW_RULE_NAME = "z>=2.0 & |jump|>=0.114 (14:30前)"
SHADOW_ZSCORE_THRESHOLD = 2.0
SHADOW_JUMP_THRESHOLD = 0.114
SHADOW_CUTOFF = "14:30:00"
TERM_SHADOW_RULE_NAME = "M1-M2前端塌陷>=2.05% (14:30前)"
OPTION_SHADOW_RULE_NAME = "500ETF期权代理共振 (14:30前)"


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Validate intraday IC basis anomaly signals.")
    parser.add_argument(
        "--intraday-archive-dir",
        default=str(PROJECT_ROOT / "reports" / "intraday_archive"),
        help="Directory that stores intraday snapshot and IC basis signal archives.",
    )
    parser.add_argument(
        "--data-cache-dir",
        default=str(PROJECT_ROOT / ".cache" / "ic_basis_history"),
        help="Directory used to cache CSI500 spot history.",
    )
    parser.add_argument(
        "--output-dir",
        default=str(PROJECT_ROOT / "reports" / "backtests"),
        help="Directory where the markdown report will be written.",
    )
    return parser


def _load_intraday_signal_days(intraday_archive_dir: Path) -> pd.DataFrame:
    rows: list[dict] = []
    for path in sorted(intraday_archive_dir.glob("*_ic_basis_signals.jsonl")):
        report_date = path.name.replace("_ic_basis_signals.jsonl", "")
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            try:
                payload = json.loads(line)
            except Exception:
                continue
            signal = payload.get("basis_signal") or {}
            rows.append(
                {
                    "report_date": report_date,
                    "captured_at": payload.get("captured_at"),
                    "severity": signal.get("severity"),
                    "annualized_basis_pct": signal.get("annualized_basis_pct"),
                    "delta_vs_prev": signal.get("delta_vs_prev"),
                    "zscore": signal.get("zscore"),
                }
            )
    if not rows:
        return pd.DataFrame(
            columns=[
                "report_date",
                "captured_at",
                "severity",
                "annualized_basis_pct",
                "delta_vs_prev",
                "zscore",
            ]
        )
    frame = pd.DataFrame(rows)
    frame["report_date"] = pd.to_datetime(frame["report_date"])
    return frame.sort_values(["report_date", "captured_at"])


def _coerce_shadow_signal(payload: dict) -> dict:
    shadow = payload.get("csi500_basis_shadow_signal")
    if isinstance(shadow, dict) and shadow:
        return shadow

    signal = payload.get("csi500_basis_signal") or {}
    captured_at = str(payload.get("captured_at") or "")
    captured_time = None
    if captured_at:
        try:
            captured_time = pd.to_datetime(captured_at).strftime("%H:%M:%S")
        except Exception:
            captured_time = None
    zscore = float(signal.get("zscore") or 0.0)
    abs_jump = abs(float(signal.get("delta_vs_prev") or 0.0))
    before_cutoff = bool(captured_time and captured_time <= SHADOW_CUTOFF)
    candidate = before_cutoff and zscore >= SHADOW_ZSCORE_THRESHOLD and abs_jump >= SHADOW_JUMP_THRESHOLD
    return {
        "rule_name": SHADOW_RULE_NAME,
        "candidate": candidate,
        "captured_time": captured_time,
        "before_cutoff": before_cutoff,
        "zscore_threshold": SHADOW_ZSCORE_THRESHOLD,
        "jump_threshold": SHADOW_JUMP_THRESHOLD,
        "zscore": zscore,
        "abs_jump": abs_jump,
    }


def _coerce_term_structure_shadow_signal(payload: dict) -> dict:
    shadow = payload.get("csi500_term_structure_shadow_signal")
    if isinstance(shadow, dict) and shadow:
        return shadow
    return {}


def _coerce_option_shadow_signal(payload: dict) -> dict:
    shadow = payload.get("csi500_option_proxy_shadow_signal")
    if isinstance(shadow, dict) and shadow:
        return shadow
    return {}


def _load_intraday_snapshot_days(intraday_archive_dir: Path) -> pd.DataFrame:
    rows: list[dict] = []
    for path in sorted(intraday_archive_dir.glob("*_market_snapshots.jsonl")):
        report_date = path.name.replace("_market_snapshots.jsonl", "")
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            try:
                payload = json.loads(line)
            except Exception:
                continue
            basis = payload.get("csi500_basis") or {}
            signal = payload.get("csi500_basis_signal") or {}
            shadow = _coerce_shadow_signal(payload)
            term_shadow = _coerce_term_structure_shadow_signal(payload)
            option_shadow = _coerce_option_shadow_signal(payload)
            if not basis:
                continue
            rows.append(
                {
                    "report_date": report_date,
                    "captured_at": payload.get("captured_at"),
                    "annualized_basis_pct": basis.get("annualized_basis_pct"),
                    "basis": basis.get("basis"),
                    "severity": signal.get("severity"),
                    "triggered": bool(signal.get("triggered")),
                    "delta_vs_prev": signal.get("delta_vs_prev"),
                    "zscore": signal.get("zscore"),
                    "basis_zscore_floor_applied": bool(signal.get("rolling_std_floor_applied")),
                    "shadow_candidate": bool(shadow.get("candidate")),
                    "shadow_raw_candidate": bool(shadow.get("raw_candidate", shadow.get("candidate"))),
                    "shadow_abs_jump": shadow.get("abs_jump"),
                    "shadow_zscore": shadow.get("zscore"),
                    "shadow_confirmation_count": shadow.get("confirmation_count"),
                    "shadow_confirmation_required": shadow.get("confirmation_required"),
                    "shadow_cooldown_active": bool(shadow.get("cooldown_active")),
                    "shadow_silent_window_active": bool(shadow.get("silent_window_active")),
                    "shadow_rule_name": shadow.get("rule_name"),
                    "term_shadow_candidate": bool(term_shadow.get("candidate")),
                    "term_shadow_raw_candidate": bool(term_shadow.get("raw_candidate", term_shadow.get("candidate"))),
                    "term_front_end_gap_pct": term_shadow.get("front_end_gap_pct"),
                    "term_anchor_stable": term_shadow.get("anchor_stable"),
                    "term_confirmation_count": term_shadow.get("confirmation_count"),
                    "term_confirmation_required": term_shadow.get("confirmation_required"),
                    "term_cooldown_active": bool(term_shadow.get("cooldown_active")),
                    "term_silent_window_active": bool(term_shadow.get("silent_window_active")),
                    "term_shadow_rule_name": term_shadow.get("rule_name"),
                    "option_shadow_candidate": bool(option_shadow.get("candidate")),
                    "option_shadow_raw_candidate": bool(option_shadow.get("raw_candidate", option_shadow.get("candidate"))),
                    "option_qvix_zscore": option_shadow.get("qvix_zscore"),
                    "option_qvix_jump_pct": option_shadow.get("qvix_jump_pct"),
                    "option_put_skew_ratio": option_shadow.get("put_skew_ratio"),
                    "option_volume_ratio": option_shadow.get("atm_put_call_volume_ratio"),
                    "option_confirmation_count": option_shadow.get("confirmation_count"),
                    "option_confirmation_required": option_shadow.get("confirmation_required"),
                    "option_cooldown_active": bool(option_shadow.get("cooldown_active")),
                    "option_silent_window_active": bool(option_shadow.get("silent_window_active")),
                    "option_roll_window_shifted": bool((payload.get("csi500_option_proxy") or {}).get("roll_window_shifted")),
                    "option_shadow_rule_name": option_shadow.get("rule_name"),
                }
            )
    if not rows:
        return pd.DataFrame(
            columns=[
                "report_date",
                "captured_at",
                "annualized_basis_pct",
                "basis",
                "severity",
                "triggered",
                "delta_vs_prev",
                "zscore",
                "basis_zscore_floor_applied",
                "shadow_candidate",
                "shadow_raw_candidate",
                "shadow_abs_jump",
                "shadow_zscore",
                "shadow_confirmation_count",
                "shadow_confirmation_required",
                "shadow_cooldown_active",
                "shadow_silent_window_active",
                "shadow_rule_name",
                "term_shadow_candidate",
                "term_shadow_raw_candidate",
                "term_front_end_gap_pct",
                "term_anchor_stable",
                "term_confirmation_count",
                "term_confirmation_required",
                "term_cooldown_active",
                "term_silent_window_active",
                "term_shadow_rule_name",
                "option_shadow_candidate",
                "option_shadow_raw_candidate",
                "option_qvix_zscore",
                "option_qvix_jump_pct",
                "option_put_skew_ratio",
                "option_volume_ratio",
                "option_confirmation_count",
                "option_confirmation_required",
                "option_cooldown_active",
                "option_silent_window_active",
                "option_roll_window_shifted",
                "option_shadow_rule_name",
            ]
        )
    frame = pd.DataFrame(rows)
    frame["report_date"] = pd.to_datetime(frame["report_date"])
    return frame.sort_values(["report_date", "captured_at"])


def _build_daily_signal_summary(signal_rows: pd.DataFrame) -> pd.DataFrame:
    if signal_rows.empty:
        return pd.DataFrame(
            columns=[
                "report_date",
                "signal_count",
                "critical_count",
                "max_annualized_basis_pct",
                "max_delta_vs_prev",
                "max_zscore",
                "last_severity",
            ]
        )
    grouped = signal_rows.groupby("report_date", as_index=False).agg(
        signal_count=("severity", "size"),
        critical_count=("severity", lambda s: int((s == "critical").sum())),
        max_annualized_basis_pct=("annualized_basis_pct", "max"),
        max_delta_vs_prev=("delta_vs_prev", "max"),
        max_zscore=("zscore", "max"),
        last_severity=("severity", "last"),
    )
    return grouped


def _build_daily_snapshot_summary(snapshot_rows: pd.DataFrame) -> pd.DataFrame:
    if snapshot_rows.empty:
        return pd.DataFrame(
            columns=[
                "report_date",
                "snapshot_count",
                "basis_coverage_count",
                "max_basis_pct",
                "mean_basis_pct",
                "max_intraday_zscore",
                "invalid_intraday_zscore_count",
                "max_intraday_jump",
                "triggered_count",
                "shadow_candidate_count",
                "max_shadow_zscore",
                "max_shadow_jump",
                "term_shadow_candidate_count",
                "max_term_front_end_gap",
                "option_shadow_candidate_count",
                "max_option_qvix_zscore",
                "max_option_put_skew_ratio",
                "max_option_volume_ratio",
                "last_severity",
            ]
        )
    scoped = snapshot_rows.copy()
    scoped["intraday_zscore_invalid"] = (
        scoped["basis_zscore_floor_applied"].fillna(False)
        | (pd.to_numeric(scoped["zscore"], errors="coerce").abs() > 50)
    )
    scoped["display_intraday_zscore"] = pd.to_numeric(scoped["zscore"], errors="coerce").where(
        ~scoped["intraday_zscore_invalid"],
        other=pd.NA,
    )
    grouped = scoped.groupby("report_date", as_index=False).agg(
        snapshot_count=("captured_at", "size"),
        basis_coverage_count=("annualized_basis_pct", lambda s: int(s.notna().sum())),
        max_basis_pct=("annualized_basis_pct", "max"),
        mean_basis_pct=("annualized_basis_pct", "mean"),
        max_intraday_zscore=("display_intraday_zscore", "max"),
        invalid_intraday_zscore_count=("intraday_zscore_invalid", lambda s: int(pd.Series(s).fillna(False).sum())),
        max_intraday_jump=("delta_vs_prev", "max"),
        triggered_count=("triggered", lambda s: int(pd.Series(s).fillna(False).sum())),
        shadow_candidate_count=("shadow_candidate", lambda s: int(pd.Series(s).fillna(False).sum())),
        max_shadow_zscore=("shadow_zscore", "max"),
        max_shadow_jump=("shadow_abs_jump", "max"),
        term_shadow_candidate_count=("term_shadow_candidate", lambda s: int(pd.Series(s).fillna(False).sum())),
        max_term_front_end_gap=("term_front_end_gap_pct", "max"),
        option_shadow_candidate_count=("option_shadow_candidate", lambda s: int(pd.Series(s).fillna(False).sum())),
        max_option_qvix_zscore=("option_qvix_zscore", "max"),
        max_option_put_skew_ratio=("option_put_skew_ratio", "max"),
        max_option_volume_ratio=("option_volume_ratio", "max"),
        last_severity=("severity", "last"),
    )
    return grouped


def _build_daily_shadow_summary(snapshot_rows: pd.DataFrame) -> pd.DataFrame:
    if snapshot_rows.empty:
        return pd.DataFrame(
            columns=[
                "report_date",
                "shadow_signal_count",
                "shadow_raw_hit_count",
                "shadow_pending_confirmation_count",
                "shadow_cooldown_block_count",
                "shadow_silent_block_count",
                "max_shadow_zscore",
                "max_shadow_jump",
                "max_basis_pct",
                "shadow_rule_name",
            ]
        )
    scoped = snapshot_rows[
        (snapshot_rows["shadow_raw_candidate"] == True)
        | (snapshot_rows["shadow_candidate"] == True)
        | (snapshot_rows["shadow_cooldown_active"] == True)
        | (snapshot_rows["shadow_silent_window_active"] == True)
    ].copy()
    if scoped.empty:
        return pd.DataFrame(
            columns=[
                "report_date",
                "shadow_signal_count",
                "shadow_raw_hit_count",
                "shadow_pending_confirmation_count",
                "shadow_cooldown_block_count",
                "shadow_silent_block_count",
                "max_shadow_zscore",
                "max_shadow_jump",
                "max_basis_pct",
                "shadow_rule_name",
            ]
        )
    scoped["shadow_pending_confirmation"] = (
        scoped["shadow_raw_candidate"].fillna(False)
        & ~scoped["shadow_candidate"].fillna(False)
        & ~scoped["shadow_cooldown_active"].fillna(False)
        & ~scoped["shadow_silent_window_active"].fillna(False)
    )
    scoped["display_shadow_zscore"] = pd.to_numeric(scoped["shadow_zscore"], errors="coerce").where(
        pd.to_numeric(scoped["shadow_zscore"], errors="coerce").abs() <= 50,
        other=pd.NA,
    )
    grouped = scoped.groupby("report_date", as_index=False).agg(
        shadow_raw_hit_count=("shadow_raw_candidate", lambda s: int(pd.Series(s).fillna(False).sum())),
        shadow_pending_confirmation_count=("shadow_pending_confirmation", lambda s: int(pd.Series(s).fillna(False).sum())),
        shadow_cooldown_block_count=("shadow_cooldown_active", lambda s: int(pd.Series(s).fillna(False).sum())),
        shadow_silent_block_count=("shadow_silent_window_active", lambda s: int(pd.Series(s).fillna(False).sum())),
        max_shadow_zscore=("display_shadow_zscore", "max"),
        max_shadow_jump=("shadow_abs_jump", "max"),
        max_basis_pct=("annualized_basis_pct", "max"),
        shadow_rule_name=("shadow_rule_name", "last"),
    )
    grouped["shadow_signal_count"] = (
        scoped.groupby("report_date")["shadow_candidate"].sum().astype(int).reindex(grouped["report_date"]).to_numpy()
    )
    return grouped


def _build_daily_term_shadow_summary(snapshot_rows: pd.DataFrame) -> pd.DataFrame:
    if snapshot_rows.empty:
        return pd.DataFrame(
            columns=[
                "report_date",
                "term_shadow_signal_count",
                "term_shadow_raw_hit_count",
                "term_pending_confirmation_count",
                "term_cooldown_block_count",
                "term_silent_block_count",
                "max_term_front_end_gap",
                "anchor_stable_count",
                "term_shadow_rule_name",
            ]
        )
    scoped = snapshot_rows[
        (snapshot_rows["term_shadow_raw_candidate"] == True)
        | (snapshot_rows["term_shadow_candidate"] == True)
        | (snapshot_rows["term_cooldown_active"] == True)
        | (snapshot_rows["term_silent_window_active"] == True)
    ].copy()
    if scoped.empty:
        return pd.DataFrame(
            columns=[
                "report_date",
                "term_shadow_signal_count",
                "term_shadow_raw_hit_count",
                "term_pending_confirmation_count",
                "term_cooldown_block_count",
                "term_silent_block_count",
                "max_term_front_end_gap",
                "anchor_stable_count",
                "term_shadow_rule_name",
            ]
        )
    scoped["term_pending_confirmation"] = (
        scoped["term_shadow_raw_candidate"].fillna(False)
        & ~scoped["term_shadow_candidate"].fillna(False)
        & ~scoped["term_cooldown_active"].fillna(False)
        & ~scoped["term_silent_window_active"].fillna(False)
    )
    grouped = scoped.groupby("report_date", as_index=False).agg(
        term_shadow_raw_hit_count=("term_shadow_raw_candidate", lambda s: int(pd.Series(s).fillna(False).sum())),
        term_pending_confirmation_count=("term_pending_confirmation", lambda s: int(pd.Series(s).fillna(False).sum())),
        term_cooldown_block_count=("term_cooldown_active", lambda s: int(pd.Series(s).fillna(False).sum())),
        term_silent_block_count=("term_silent_window_active", lambda s: int(pd.Series(s).fillna(False).sum())),
        max_term_front_end_gap=("term_front_end_gap_pct", "max"),
        anchor_stable_count=("term_anchor_stable", lambda s: int(pd.Series(s).fillna(False).sum())),
        term_shadow_rule_name=("term_shadow_rule_name", "last"),
    )
    grouped["term_shadow_signal_count"] = (
        scoped.groupby("report_date")["term_shadow_candidate"].sum().astype(int).reindex(grouped["report_date"]).to_numpy()
    )
    return grouped


def _build_daily_option_shadow_summary(snapshot_rows: pd.DataFrame) -> pd.DataFrame:
    if snapshot_rows.empty:
        return pd.DataFrame(
            columns=[
                "report_date",
                "option_shadow_signal_count",
                "option_shadow_raw_hit_count",
                "option_pending_confirmation_count",
                "option_cooldown_block_count",
                "option_silent_block_count",
                "option_roll_window_shifted_count",
                "max_option_qvix_zscore",
                "max_option_qvix_jump_pct",
                "max_option_put_skew_ratio",
                "max_option_volume_ratio",
                "option_shadow_rule_name",
            ]
        )
    scoped = snapshot_rows[
        (snapshot_rows["option_shadow_raw_candidate"] == True)
        | (snapshot_rows["option_shadow_candidate"] == True)
        | (snapshot_rows["option_cooldown_active"] == True)
        | (snapshot_rows["option_silent_window_active"] == True)
        | (snapshot_rows["option_roll_window_shifted"] == True)
    ].copy()
    if scoped.empty:
        return pd.DataFrame(
            columns=[
                "report_date",
                "option_shadow_signal_count",
                "option_shadow_raw_hit_count",
                "option_pending_confirmation_count",
                "option_cooldown_block_count",
                "option_silent_block_count",
                "option_roll_window_shifted_count",
                "max_option_qvix_zscore",
                "max_option_qvix_jump_pct",
                "max_option_put_skew_ratio",
                "max_option_volume_ratio",
                "option_shadow_rule_name",
            ]
        )
    scoped["option_pending_confirmation"] = (
        scoped["option_shadow_raw_candidate"].fillna(False)
        & ~scoped["option_shadow_candidate"].fillna(False)
        & ~scoped["option_cooldown_active"].fillna(False)
        & ~scoped["option_silent_window_active"].fillna(False)
    )
    grouped = scoped.groupby("report_date", as_index=False).agg(
        option_shadow_raw_hit_count=("option_shadow_raw_candidate", lambda s: int(pd.Series(s).fillna(False).sum())),
        option_pending_confirmation_count=("option_pending_confirmation", lambda s: int(pd.Series(s).fillna(False).sum())),
        option_cooldown_block_count=("option_cooldown_active", lambda s: int(pd.Series(s).fillna(False).sum())),
        option_silent_block_count=("option_silent_window_active", lambda s: int(pd.Series(s).fillna(False).sum())),
        option_roll_window_shifted_count=("option_roll_window_shifted", lambda s: int(pd.Series(s).fillna(False).sum())),
        max_option_qvix_zscore=("option_qvix_zscore", "max"),
        max_option_qvix_jump_pct=("option_qvix_jump_pct", "max"),
        max_option_put_skew_ratio=("option_put_skew_ratio", "max"),
        max_option_volume_ratio=("option_volume_ratio", "max"),
        option_shadow_rule_name=("option_shadow_rule_name", "last"),
    )
    grouped["option_shadow_signal_count"] = (
        scoped.groupby("report_date")["option_shadow_candidate"].sum().astype(int).reindex(grouped["report_date"]).to_numpy()
    )
    return grouped


def _load_spot_returns(cache_dir: Path) -> pd.DataFrame:
    spot = _load_spot_history(cache_dir, refresh_cache=False)
    if spot is None or spot.empty:
        raise RuntimeError("未获取到中证500现货历史，无法评估盘中基差信号。")
    frame = spot.copy()
    frame["date"] = pd.to_datetime(frame["date"])
    frame = frame.rename(columns={"close": "spot_close"})
    frame = frame.sort_values("date")
    frame["t1_ret"] = frame["spot_close"].shift(-1) / frame["spot_close"] - 1.0
    frame["t3_ret"] = frame["spot_close"].shift(-3) / frame["spot_close"] - 1.0
    return frame[["date", "spot_close", "t1_ret", "t3_ret"]]


def _safe_eval_sample(frame: pd.DataFrame) -> pd.DataFrame:
    required_cols = {"t1_ret", "t3_ret"}
    if frame is None or frame.empty or not required_cols.issubset(frame.columns):
        return pd.DataFrame()
    return frame.dropna(subset=["t1_ret", "t3_ret"]).copy()


def _build_report(
    signal_days: pd.DataFrame,
    snapshot_days: pd.DataFrame,
    shadow_days: pd.DataFrame,
    term_shadow_days: pd.DataFrame,
    option_shadow_days: pd.DataFrame,
    signal_merged: pd.DataFrame,
    shadow_merged: pd.DataFrame,
    term_shadow_merged: pd.DataFrame,
    option_shadow_merged: pd.DataFrame,
) -> str:
    lines = [
        f"# {datetime.now().strftime('%Y-%m-%d')} IC盘中基差异动效果表",
        "",
        "- 目标: 评估盘中 IC 基差异常信号是否值得进入更早期的多头收缩框架",
        "- 信号来源: intraday_archive 下的 `_market_snapshots.jsonl` 和 `_ic_basis_signals.jsonl`",
        "- 收益评估: 次日和未来 3 日中证500现货收益（当前只做方向参考，不代表完整 IC 可交易收益）",
        "",
    ]

    if snapshot_days.empty:
        lines.extend(
            [
                "## 当前状态",
                "",
                "- 目前还没有积累到可用的盘中 IC 基差或500ETF期权代理快照。",
                "- 原因通常是：白天基差数据尚未采到，或运行时仍在旧版取数链路上。",
                "- 这份表已经就位，后续只要白天采到有效基差与期权代理，就会先形成覆盖统计，再逐步形成异常样本。",
            ]
        )
        return "\n".join(lines)

    lines.extend(
        [
            "## 快照覆盖概览",
            "",
            f"- 盘中基差覆盖日数: {len(snapshot_days)}",
            f"- 已触发异常日数: {len(signal_days)}",
            f"- 已触发影子候选日数: {len(shadow_days)}",
            f"- 已触发前端塌陷候选日数: {len(term_shadow_days)}",
            f"- 已触发期权代理候选日数: {len(option_shadow_days)}",
            "",
            "| 日期 | 快照数 | 有效基差数 | 最大年化贴水 | 平均年化贴水 | 最大zscore | 失真zscore数 | 最大跳升 | 硬异常触发数 | 基差影子数 | 前端塌陷影子数 | 期权代理影子数 | 最后级别 |",
            "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |",
        ]
    )
    for _, row in snapshot_days.tail(20).iterrows():
        lines.append(
            f"| {row['report_date'].strftime('%Y-%m-%d')} | {int(row['snapshot_count'])} | {int(row['basis_coverage_count'])} | "
            f"{(row['max_basis_pct'] or 0):.2f}% | {(row['mean_basis_pct'] or 0):.2f}% | "
            f"{(row['max_intraday_zscore'] or 0):.2f} | {int(row['invalid_intraday_zscore_count'])} | {(row['max_intraday_jump'] or 0):.2f}% | "
            f"{int(row['triggered_count'])} | {int(row['shadow_candidate_count'])} | {int(row['term_shadow_candidate_count'])} | "
            f"{int(row['option_shadow_candidate_count'])} | {row['last_severity'] or 'n/a'} |"
        )

    shadow_raw_days = shadow_days[shadow_days["shadow_raw_hit_count"] > 0] if not shadow_days.empty else pd.DataFrame()
    term_raw_days = (
        term_shadow_days[term_shadow_days["term_shadow_raw_hit_count"] > 0]
        if not term_shadow_days.empty
        else pd.DataFrame()
    )
    option_raw_days = (
        option_shadow_days[
            (option_shadow_days["option_shadow_raw_hit_count"] > 0)
            | (option_shadow_days["option_roll_window_shifted_count"] > 0)
        ]
        if not option_shadow_days.empty
        else pd.DataFrame()
    )
    if not shadow_raw_days.empty or not term_raw_days.empty or not option_raw_days.empty:
        lines.extend(
            [
                "",
                "## 影子QA验收账本",
                "",
                f"- 基差影子原始命中日数: {len(shadow_raw_days)}",
                f"- 基差影子被三连确认拦住日数: {int((shadow_days['shadow_pending_confirmation_count'] > 0).sum()) if not shadow_days.empty else 0}",
                f"- 基差影子被冷却压制日数: {int((shadow_days['shadow_cooldown_block_count'] > 0).sum()) if not shadow_days.empty else 0}",
                f"- 基差影子被静默窗口压制日数: {int((shadow_days['shadow_silent_block_count'] > 0).sum()) if not shadow_days.empty else 0}",
                f"- 前端塌陷原始命中日数: {len(term_raw_days)}",
                f"- 前端塌陷被三连确认拦住日数: {int((term_shadow_days['term_pending_confirmation_count'] > 0).sum()) if not term_shadow_days.empty else 0}",
                f"- 前端塌陷被冷却压制日数: {int((term_shadow_days['term_cooldown_block_count'] > 0).sum()) if not term_shadow_days.empty else 0}",
                f"- 前端塌陷被静默窗口压制日数: {int((term_shadow_days['term_silent_block_count'] > 0).sum()) if not term_shadow_days.empty else 0}",
                f"- 期权代理原始命中日数: {len(option_raw_days[option_raw_days['option_shadow_raw_hit_count'] > 0]) if not option_raw_days.empty else 0}",
                f"- 期权代理被三连确认拦住日数: {int((option_shadow_days['option_pending_confirmation_count'] > 0).sum()) if not option_shadow_days.empty else 0}",
                f"- 期权代理被冷却压制日数: {int((option_shadow_days['option_cooldown_block_count'] > 0).sum()) if not option_shadow_days.empty else 0}",
                f"- 期权代理被静默窗口压制日数: {int((option_shadow_days['option_silent_block_count'] > 0).sum()) if not option_shadow_days.empty else 0}",
                f"- 期权腿换月平移发生日数: {int((option_shadow_days['option_roll_window_shifted_count'] > 0).sum()) if not option_shadow_days.empty else 0}",
                "",
                "| 日期 | 基差原始/确认/待确认/冷却/静默 | 前端原始/确认/待确认/冷却/静默 | 期权原始/确认/待确认/冷却/静默/换月 |",
                "| --- | --- | --- | --- |",
            ]
        )
        qa_dates = sorted(
            {
                *([d for d in shadow_days["report_date"]] if not shadow_days.empty else []),
                *([d for d in term_shadow_days["report_date"]] if not term_shadow_days.empty else []),
                *([d for d in option_shadow_days["report_date"]] if not option_shadow_days.empty else []),
            }
        )
        for report_date in qa_dates[-20:]:
            shadow_row = shadow_days.loc[shadow_days["report_date"] == report_date] if not shadow_days.empty else pd.DataFrame()
            term_row = term_shadow_days.loc[term_shadow_days["report_date"] == report_date] if not term_shadow_days.empty else pd.DataFrame()
            option_row = option_shadow_days.loc[option_shadow_days["report_date"] == report_date] if not option_shadow_days.empty else pd.DataFrame()
            shadow_text = (
                f"{int(shadow_row.iloc[0]['shadow_raw_hit_count'])}/{int(shadow_row.iloc[0]['shadow_signal_count'])}/"
                f"{int(shadow_row.iloc[0]['shadow_pending_confirmation_count'])}/{int(shadow_row.iloc[0]['shadow_cooldown_block_count'])}/"
                f"{int(shadow_row.iloc[0]['shadow_silent_block_count'])}"
                if not shadow_row.empty
                else "0/0/0/0/0"
            )
            term_text = (
                f"{int(term_row.iloc[0]['term_shadow_raw_hit_count'])}/{int(term_row.iloc[0]['term_shadow_signal_count'])}/"
                f"{int(term_row.iloc[0]['term_pending_confirmation_count'])}/{int(term_row.iloc[0]['term_cooldown_block_count'])}/"
                f"{int(term_row.iloc[0]['term_silent_block_count'])}"
                if not term_row.empty
                else "0/0/0/0/0"
            )
            option_text = (
                f"{int(option_row.iloc[0]['option_shadow_raw_hit_count'])}/{int(option_row.iloc[0]['option_shadow_signal_count'])}/"
                f"{int(option_row.iloc[0]['option_pending_confirmation_count'])}/{int(option_row.iloc[0]['option_cooldown_block_count'])}/"
                f"{int(option_row.iloc[0]['option_silent_block_count'])}/{int(option_row.iloc[0]['option_roll_window_shifted_count'])}"
                if not option_row.empty
                else "0/0/0/0/0/0"
            )
            lines.append(f"| {report_date.strftime('%Y-%m-%d')} | {shadow_text} | {term_text} | {option_text} |")

    signal_sample = _safe_eval_sample(signal_merged) if not signal_days.empty else pd.DataFrame()
    shadow_sample = (
        _safe_eval_sample(shadow_merged[shadow_merged["shadow_signal_count"] > 0])
        if not shadow_merged.empty
        else pd.DataFrame()
    )
    if not shadow_sample.empty:
        shadow_t1_mean = float(shadow_sample["t1_ret"].mean())
        shadow_t3_mean = float(shadow_sample["t3_ret"].mean())
        shadow_t1_win = float((shadow_sample["t1_ret"] > 0).mean())
        shadow_t3_win = float((shadow_sample["t3_ret"] > 0).mean())
        lines.extend(
            [
                "",
                "## 影子候选表现",
                "",
                f"- 影子规则: `{SHADOW_RULE_NAME}`",
                f"- 可评估样本数: {len(shadow_sample)}",
                f"- 次日平均收益: {shadow_t1_mean * 100:.2f}%，胜率 {shadow_t1_win * 100:.1f}%",
                f"- 3日平均收益: {shadow_t3_mean * 100:.2f}%，胜率 {shadow_t3_win * 100:.1f}%",
            ]
        )
        lines.extend(
            [
                "",
                "| 日期 | 影子信号数 | 最大影子zscore | 最大影子跳升 | 最大年化贴水 | 次日收益 | 3日收益 |",
                "| --- | ---: | ---: | ---: | ---: | ---: | ---: |",
            ]
        )
        for _, row in shadow_sample.tail(20).iterrows():
            lines.append(
                f"| {row['report_date'].strftime('%Y-%m-%d')} | {int(row['shadow_signal_count'])} | "
                f"{(row['max_shadow_zscore'] or 0):.2f} | {(row['max_shadow_jump'] or 0):.3f}% | "
                f"{(row['max_basis_pct'] or 0):.2f}% | {row['t1_ret'] * 100:.2f}% | {row['t3_ret'] * 100:.2f}% |"
            )

    term_shadow_sample = (
        _safe_eval_sample(term_shadow_merged[term_shadow_merged["term_shadow_signal_count"] > 0])
        if not term_shadow_merged.empty
        else pd.DataFrame()
    )
    if not term_shadow_sample.empty:
        term_t1_mean = float(term_shadow_sample["t1_ret"].mean())
        term_t3_mean = float(term_shadow_sample["t3_ret"].mean())
        term_t1_win = float((term_shadow_sample["t1_ret"] > 0).mean())
        term_t3_win = float((term_shadow_sample["t3_ret"] > 0).mean())
        lines.extend(
            [
                "",
                "## 前端塌陷影子候选表现",
                "",
                f"- 影子规则: `{TERM_SHADOW_RULE_NAME}`",
                f"- 可评估样本数: {len(term_shadow_sample)}",
                f"- 次日平均收益: {term_t1_mean * 100:.2f}%，胜率 {term_t1_win * 100:.1f}%",
                f"- 3日平均收益: {term_t3_mean * 100:.2f}%，胜率 {term_t3_win * 100:.1f}%",
            ]
        )
        lines.extend(
            [
                "",
                "| 日期 | 前端塌陷信号数 | 最大前端塌陷差 | 远季锚稳定次数 | 次日收益 | 3日收益 |",
                "| --- | ---: | ---: | ---: | ---: | ---: |",
            ]
        )
        for _, row in term_shadow_sample.tail(20).iterrows():
            lines.append(
                f"| {row['report_date'].strftime('%Y-%m-%d')} | {int(row['term_shadow_signal_count'])} | "
                f"{(row['max_term_front_end_gap'] or 0):.2f}% | {int(row['anchor_stable_count'])} | "
                f"{row['t1_ret'] * 100:.2f}% | {row['t3_ret'] * 100:.2f}% |"
            )

    option_shadow_sample = (
        _safe_eval_sample(option_shadow_merged[option_shadow_merged["option_shadow_signal_count"] > 0])
        if not option_shadow_merged.empty
        else pd.DataFrame()
    )
    if not option_shadow_sample.empty:
        option_t1_mean = float(option_shadow_sample["t1_ret"].mean())
        option_t3_mean = float(option_shadow_sample["t3_ret"].mean())
        option_t1_win = float((option_shadow_sample["t1_ret"] > 0).mean())
        option_t3_win = float((option_shadow_sample["t3_ret"] > 0).mean())
        lines.extend(
            [
                "",
                "## 期权代理影子候选表现",
                "",
                f"- 影子规则: `{OPTION_SHADOW_RULE_NAME}`",
                f"- 可评估样本数: {len(option_shadow_sample)}",
                f"- 次日平均收益: {option_t1_mean * 100:.2f}%，胜率 {option_t1_win * 100:.1f}%",
                f"- 3日平均收益: {option_t3_mean * 100:.2f}%，胜率 {option_t3_win * 100:.1f}%",
            ]
        )
        lines.extend(
            [
                "",
                "| 日期 | 期权代理信号数 | 最大Qvix zscore | 最大Qvix跳升 | 最大虚平沽比 | 最大ATM沽/购量比 | 次日收益 | 3日收益 |",
                "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
            ]
        )
        for _, row in option_shadow_sample.tail(20).iterrows():
            lines.append(
                f"| {row['report_date'].strftime('%Y-%m-%d')} | {int(row['option_shadow_signal_count'])} | "
                f"{(row['max_option_qvix_zscore'] or 0):.2f} | {(row['max_option_qvix_jump_pct'] or 0):.2f}% | "
                f"{(row['max_option_put_skew_ratio'] or 0):.3f} | {(row['max_option_volume_ratio'] or 0):.2f} | "
                f"{row['t1_ret'] * 100:.2f}% | {row['t3_ret'] * 100:.2f}% |"
            )

    if signal_sample.empty:
        lines.extend(
            [
                "",
                "## 当前状态",
                "",
                "- 已经开始积累盘中基差、前端期限结构与500ETF期权代理快照，但硬异常样本还不够用于统计表现。",
                "- 下一步重点是继续让白天样本长出来，尤其是有跳升、高 zscore、前端塌陷或期权代理共振的日子，并继续比较影子候选与硬异常谁更早、更稳。",
            ]
        )
        return "\n".join(lines)

    lines.extend(
        [
            "## 样本概览",
            "",
            f"- 盘中异常日数: {len(signal_days)}",
            f"- 可评估样本数: {len(signal_sample)}",
            "",
            "| 日期 | 信号数 | critical数 | 最大年化贴水 | 最大跳升 | 最大zscore | 次日收益 | 3日收益 |",
            "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for _, row in signal_sample.tail(20).iterrows():
        lines.append(
            f"| {row['report_date'].strftime('%Y-%m-%d')} | {int(row['signal_count'])} | {int(row['critical_count'])} | "
            f"{row['max_annualized_basis_pct']:.2f}% | {row['max_delta_vs_prev']:.2f}% | {row['max_zscore']:.2f} | "
            f"{row['t1_ret'] * 100:.2f}% | {row['t3_ret'] * 100:.2f}% |"
        )

    if not signal_sample.empty:
        t1_mean = float(signal_sample["t1_ret"].mean())
        t3_mean = float(signal_sample["t3_ret"].mean())
        t1_win = float((signal_sample["t1_ret"] > 0).mean())
        t3_win = float((signal_sample["t3_ret"] > 0).mean())
        lines.extend(
            [
                "",
                "## 初步结论",
                "",
                f"- 异常日次日平均收益: {t1_mean * 100:.2f}%，胜率 {t1_win * 100:.1f}%",
                f"- 异常日3日平均收益: {t3_mean * 100:.2f}%，胜率 {t3_win * 100:.1f}%",
                "- 当前仍以样本积累为主，后续更适合和 IC 全收益标签、盘中认沽保护联动一起看。",
            ]
        )
    return "\n".join(lines)


def main() -> int:
    args = _build_parser().parse_args()
    intraday_archive_dir = Path(args.intraday_archive_dir)
    cache_dir = Path(args.data_cache_dir)
    signal_rows = _load_intraday_signal_days(intraday_archive_dir)
    signal_days = _build_daily_signal_summary(signal_rows)
    snapshot_rows = _load_intraday_snapshot_days(intraday_archive_dir)
    snapshot_days = _build_daily_snapshot_summary(snapshot_rows)
    shadow_days = _build_daily_shadow_summary(snapshot_rows)
    term_shadow_days = _build_daily_term_shadow_summary(snapshot_rows)
    option_shadow_days = _build_daily_option_shadow_summary(snapshot_rows)
    spot = _load_spot_returns(cache_dir)
    if not signal_days.empty:
        signal_merged = signal_days.merge(spot, left_on="report_date", right_on="date", how="left").drop(columns=["date"])
    else:
        signal_merged = signal_days.copy()
    if not shadow_days.empty:
        shadow_merged = shadow_days.merge(spot, left_on="report_date", right_on="date", how="left").drop(columns=["date"])
    else:
        shadow_merged = shadow_days.copy()
    if not term_shadow_days.empty:
        term_shadow_merged = term_shadow_days.merge(spot, left_on="report_date", right_on="date", how="left").drop(columns=["date"])
    else:
        term_shadow_merged = term_shadow_days.copy()
    if not option_shadow_days.empty:
        option_shadow_merged = option_shadow_days.merge(spot, left_on="report_date", right_on="date", how="left").drop(columns=["date"])
    else:
        option_shadow_merged = option_shadow_days.copy()

    report = _build_report(
        signal_days,
        snapshot_days,
        shadow_days,
        term_shadow_days,
        option_shadow_days,
        signal_merged,
        shadow_merged,
        term_shadow_merged,
        option_shadow_merged,
    )
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    report_date = datetime.now().strftime("%Y-%m-%d")
    report_path = output_dir / f"{report_date}_IC盘中基差异动效果表.md"
    latest_path = output_dir / "latest_ic_intraday_basis_signal_validation.md"
    report_path.write_text(report, encoding="utf-8")
    latest_path.write_text(report, encoding="utf-8")

    print("=" * 72)
    print("IC盘中基差异动效果表生成完成")
    print(f"报告: {report_path}")
    print("=" * 72)
    print(report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
