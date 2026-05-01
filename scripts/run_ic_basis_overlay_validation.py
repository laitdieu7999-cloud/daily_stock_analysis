#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Validate IC basis-aware overlays on the CSI500 / IC line.

This script keeps the current weighted candidate model unchanged and asks a
more practical question for the user's real workflow:

- can a basis-aware overlay improve the CSI500 proxy line
- when should deep IC discount be treated as support instead of pure risk
- when does a widening discount become a basis trap that deserves defense

The preferred path is to use:
- 中证500现货历史: ak.stock_zh_index_daily("sh000905")
- IC 主连历史: ak.futures_zh_daily_sina("IC0")

If that real continuous basis series is unavailable in the environment, the
script falls back to the repo's older proxy `ic_discount` logic.
"""

from __future__ import annotations

import argparse
import sys
import calendar
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_DIR = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from backtest_next_production_metaphysical_model import (  # noqa: E402
    _build_candidate_frame,
    _build_target,
    _compute_quant_and_resonance_features,
    _generate_walk_forward_probabilities,
)
from src.models.metaphysical import NEXT_PRODUCTION_WEIGHTED_CANDIDATE_FEATURES  # noqa: E402
from src.services.local_etf_history import load_cached_etf_daily_ohlcv  # noqa: E402


def _default_start_end() -> tuple[str, str]:
    end = datetime.now().date()
    start = end - timedelta(days=365 * 5)
    return start.isoformat(), end.isoformat()


def _build_parser() -> argparse.ArgumentParser:
    default_start, default_end = _default_start_end()
    parser = argparse.ArgumentParser(description="Validate IC basis-aware overlays on the CSI500 proxy line.")
    parser.add_argument("--start", default=default_start)
    parser.add_argument("--end", default=default_end)
    parser.add_argument(
        "--cache-dir",
        default=str(PROJECT_ROOT / ".cache" / "xgb_cache"),
        help="Cache directory for metaphysical feature calculations.",
    )
    parser.add_argument(
        "--probability-cache-dir",
        default=str(PROJECT_ROOT / ".cache" / "metaphysical_probabilities"),
        help="Directory used to cache walk-forward probability frames.",
    )
    parser.add_argument(
        "--refresh-probability-cache",
        action="store_true",
        help="Rebuild the walk-forward probability cache before validation.",
    )
    parser.add_argument(
        "--output-dir",
        default=str(PROJECT_ROOT / "reports" / "backtests"),
        help="Directory where the markdown report will be written.",
    )
    parser.add_argument(
        "--data-cache-dir",
        default=str(PROJECT_ROOT / ".cache" / "ic_basis_history"),
        help="Directory used to cache CSI500 spot and CFFEX IC panel history.",
    )
    parser.add_argument(
        "--refresh-data-cache",
        action="store_true",
        help="Ignore local IC basis history cache and rebuild it from data sources.",
    )
    return parser


def _load_csi500_proxy(start: str, end: str) -> pd.DataFrame:
    local_frame = load_cached_etf_daily_ohlcv("510500")
    if local_frame is not None and not local_frame.empty:
        frame = local_frame.copy()
        frame["date"] = pd.to_datetime(frame["date"])
        frame = frame[(frame["date"] >= pd.Timestamp(start)) & (frame["date"] <= pd.Timestamp(end))].copy()
        if not frame.empty:
            return frame[["date", "open", "close", "high", "low", "volume"]].sort_values("date").reset_index(drop=True)

    import yfinance as yf

    ticker = yf.Ticker("510500.SS")
    frame = ticker.history(start=start, end=end, auto_adjust=True)
    if frame is None or frame.empty:
        raise RuntimeError("未获取到 510500.SS 的日线数据")
    frame = frame.reset_index().rename(
        columns={
            "Date": "date",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Volume": "volume",
        }
    )
    frame = frame[["date", "open", "close", "high", "low", "volume"]].copy()
    frame["date"] = pd.to_datetime(frame["date"]).dt.tz_localize(None)
    return frame.sort_values("date").reset_index(drop=True)


def _generate_ic_discount(date_index: pd.Series, close_prices: pd.Series) -> pd.Series:
    """Reuse the historical proxy logic from the resonance backtest line."""
    np.random.seed(123)
    n = len(date_index)
    base_discount = 0.06 + np.random.randn(n) * 0.02
    daily_returns = close_prices.pct_change().fillna(0.0)
    for i in range(n):
        if daily_returns.iloc[i] < -0.02:
            base_discount[i] += 0.05
        if daily_returns.iloc[i] < -0.03:
            base_discount[i] += 0.05
    for i in range(1, n):
        base_discount[i] = 0.7 * base_discount[i] + 0.3 * base_discount[i - 1]
    return pd.Series(base_discount, index=date_index.index, name="ic_discount")


def _prepare_frame(start: str, end: str, cache_dir: str, data_cache_dir: str, refresh_data_cache: bool) -> pd.DataFrame:
    frame = _load_csi500_proxy(start, end)
    feature_df = _compute_quant_and_resonance_features(frame)
    feature_df = _build_target(feature_df)
    candidate_df = _build_candidate_frame(feature_df, cache_dir=cache_dir)
    candidate_df = candidate_df.loc[:, ~candidate_df.columns.duplicated()].copy()
    for feature in NEXT_PRODUCTION_WEIGHTED_CANDIDATE_FEATURES:
        if feature not in candidate_df.columns:
            candidate_df[feature] = 0.0

    real_basis = _load_real_ic_discount(candidate_df, data_cache_dir=data_cache_dir, refresh_cache=refresh_data_cache)
    candidate_df["ic_discount"] = real_basis["ic_discount"].values
    candidate_df["ic_annualized_carry"] = real_basis["ic_annualized_carry"].values
    candidate_df["ic_discount_source"] = real_basis["ic_discount_source"].values
    if candidate_df["ic_discount"].isna().all():
        candidate_df["ic_discount"] = _generate_ic_discount(candidate_df["date"], candidate_df["close"]).values
        candidate_df["ic_discount_source"] = "proxy"
        candidate_df["ic_annualized_carry"] = np.nan
    else:
        fallback = _generate_ic_discount(candidate_df["date"], candidate_df["close"]).values
        candidate_df["ic_discount"] = candidate_df["ic_discount"].fillna(pd.Series(fallback, index=candidate_df.index))
        candidate_df.loc[candidate_df["ic_discount_source"] != "real_continuous", "ic_discount_source"] = "proxy"

    candidate_df["ic_discount_change_5d"] = candidate_df["ic_discount"] - candidate_df["ic_discount"].shift(5)
    candidate_df["ic_annualized_carry_change_5d"] = (
        candidate_df["ic_annualized_carry"] - candidate_df["ic_annualized_carry"].shift(5)
    )
    candidate_df["ma_20"] = candidate_df["close"].rolling(20).mean()
    candidate_df["trend_broken"] = (
        (candidate_df["close"] < candidate_df["ma_20"])
        & (candidate_df["quant_return_5d"] < 0)
        & (candidate_df["bb_position_pct"].fillna(0.5) < 0.35)
    ).astype(int)
    candidate_df["panic_break"] = (
        (candidate_df["quant_return_1d"] < -0.025)
        | (candidate_df["quant_return_5d"] < -0.06)
    ).astype(int)
    support_carry_threshold = candidate_df["ic_annualized_carry"].dropna().quantile(0.75)
    trap_carry_change_threshold = candidate_df["ic_annualized_carry_change_5d"].dropna().quantile(0.80)
    trap_discount_change_threshold = candidate_df["ic_discount_change_5d"].dropna().quantile(0.80)
    candidate_df["basis_support_window"] = (
        (candidate_df["ic_annualized_carry"].fillna(0.0) >= support_carry_threshold)
        & (candidate_df["trend_broken"] == 0)
    ).astype(int)
    candidate_df["basis_trap_window"] = (
        (
            (candidate_df["ic_annualized_carry_change_5d"].fillna(0.0) >= trap_carry_change_threshold)
            | (candidate_df["ic_discount_change_5d"] >= trap_discount_change_threshold)
        )
        & (candidate_df["trend_broken"] == 1)
    ).astype(int)
    candidate_df["next_3d_return"] = candidate_df["close"].shift(-3) / candidate_df["close"] - 1.0
    candidate_df["basis_support_threshold"] = support_carry_threshold
    candidate_df["basis_trap_carry_change_threshold"] = trap_carry_change_threshold
    candidate_df["basis_trap_discount_change_threshold"] = trap_discount_change_threshold
    return candidate_df


def _third_friday(year: int, month: int) -> datetime:
    first_weekday, _ = calendar.monthrange(year, month)
    first_friday = 1 + (4 - first_weekday) % 7
    return datetime(year, month, first_friday + 14)


def _extract_expiry(symbol: str) -> datetime | None:
    if not symbol.startswith("IC") or len(symbol) != 6:
        return None
    try:
        year = 2000 + int(symbol[2:4])
        month = int(symbol[4:6])
        return _third_friday(year, month)
    except Exception:
        return None


def _load_spot_history(cache_dir: Path, *, refresh_cache: bool) -> pd.DataFrame | None:
    cache_path = cache_dir / "csi500_spot_daily.pkl"
    if cache_path.exists() and not refresh_cache:
        try:
            return pd.read_pickle(cache_path)
        except Exception:
            pass
    try:
        import akshare as ak
        spot = ak.stock_zh_index_daily(symbol="sh000905")
    except Exception:
        return None
    if spot is None or spot.empty:
        return None
    spot.to_pickle(cache_path)
    return spot


def _load_cffex_ic_panel(cache_dir: Path, start_year: int, end_year: int, *, refresh_cache: bool) -> list[pd.DataFrame]:
    try:
        import akshare as ak
    except Exception:
        return []
    futures_frames: list[pd.DataFrame] = []
    for year in range(start_year, end_year + 1):
        cache_path = cache_dir / f"cffex_ic_panel_{year}.pkl"
        panel = None
        if cache_path.exists() and not refresh_cache:
            try:
                panel = pd.read_pickle(cache_path)
            except Exception:
                panel = None
        if panel is None:
            quarterly_frames: list[pd.DataFrame] = []
            for q_start, q_end in [
                (f"{year}0101", f"{year}0331"),
                (f"{year}0401", f"{year}0630"),
                (f"{year}0701", f"{year}0930"),
                (f"{year}1001", f"{year}1231"),
            ]:
                quarter_cache_path = cache_dir / f"cffex_ic_panel_{year}_{q_start[4:]}_{q_end[4:]}.pkl"
                quarter_panel = None
                if quarter_cache_path.exists() and not refresh_cache:
                    try:
                        quarter_panel = pd.read_pickle(quarter_cache_path)
                    except Exception:
                        quarter_panel = None
                if quarter_panel is None:
                    try:
                        quarter_panel = ak.get_futures_daily(start_date=q_start, end_date=q_end, market="CFFEX")
                    except Exception:
                        quarter_panel = None
                    if quarter_panel is not None and not quarter_panel.empty:
                        quarter_panel.to_pickle(quarter_cache_path)
                if quarter_panel is not None and not quarter_panel.empty:
                    quarterly_frames.append(quarter_panel)
            panel = pd.concat(quarterly_frames, ignore_index=True) if quarterly_frames else None
            if panel is not None and not panel.empty:
                panel.to_pickle(cache_path)
        if panel is None or panel.empty:
            continue
        panel = panel[panel["variety"] == "IC"].copy()
        if panel.empty:
            continue
        futures_frames.append(panel)
    return futures_frames


def _load_real_ic_discount(candidate_df: pd.DataFrame, *, data_cache_dir: str, refresh_cache: bool) -> pd.DataFrame:
    """Build a more realistic dominant-contract discount/carry series."""
    cache_dir = Path(data_cache_dir)
    cache_dir.mkdir(parents=True, exist_ok=True)
    spot = _load_spot_history(cache_dir, refresh_cache=refresh_cache)

    if spot is None or spot.empty:
        return pd.DataFrame(
            {
                "ic_discount": np.nan,
                "ic_annualized_carry": np.nan,
                "ic_discount_source": "proxy",
            },
            index=candidate_df.index,
        )

    start = pd.to_datetime(candidate_df["date"].min()).strftime("%Y%m%d")
    end = pd.to_datetime(candidate_df["date"].max()).strftime("%Y%m%d")
    start_year = int(start[:4])
    end_year = int(end[:4])
    futures_frames = _load_cffex_ic_panel(cache_dir, start_year, end_year, refresh_cache=refresh_cache)

    if not futures_frames:
        return pd.DataFrame(
            {
                "ic_discount": np.nan,
                "ic_annualized_carry": np.nan,
                "ic_discount_source": "proxy",
            },
            index=candidate_df.index,
        )

    spot = spot.copy()
    spot["date"] = pd.to_datetime(spot["date"])
    spot = spot[(spot["date"] >= pd.to_datetime(candidate_df["date"].min())) & (spot["date"] <= pd.to_datetime(candidate_df["date"].max()))]

    future = pd.concat(futures_frames, ignore_index=True)
    future["date"] = pd.to_datetime(future["date"])
    future = future.sort_values(["date", "open_interest", "volume", "symbol"], ascending=[True, False, False, True])
    dominant = future.groupby("date").head(1).copy()
    dominant["expiry"] = dominant["symbol"].apply(_extract_expiry)
    dominant["days_to_expiry"] = (dominant["expiry"] - dominant["date"]).dt.days.clip(lower=1)

    merged = spot[["date", "close"]].rename(columns={"close": "spot_close"}).merge(
        dominant[["date", "symbol", "close", "days_to_expiry"]].rename(columns={"close": "future_close"}),
        on="date",
        how="inner",
    )
    if merged.empty:
        return pd.DataFrame(
            {
                "ic_discount": np.nan,
                "ic_annualized_carry": np.nan,
                "ic_discount_source": "proxy",
            },
            index=candidate_df.index,
        )

    merged["ic_discount_real"] = (merged["spot_close"] - merged["future_close"]) / merged["future_close"]
    merged["ic_annualized_carry"] = merged["ic_discount_real"] / (merged["days_to_expiry"] / 365.0)
    aligned = candidate_df[["date"]].merge(
        merged[["date", "ic_discount_real", "ic_annualized_carry"]],
        on="date",
        how="left",
    )
    return pd.DataFrame(
        {
            "ic_discount": aligned["ic_discount_real"].values,
            "ic_annualized_carry": aligned["ic_annualized_carry"].values,
            "ic_discount_source": np.where(aligned["ic_discount_real"].notna(), "real_continuous", "proxy"),
        },
        index=candidate_df.index,
    )


def _probability_cache_path(base_dir: Path, start: str, end: str, min_train_days: int, retrain_every: int) -> Path:
    return base_dir / f"csi500_basis_overlay_{start}_{end}_min{min_train_days}_retrain{retrain_every}.pkl"


def _max_drawdown(equity_curve: pd.Series) -> float:
    rolling_peak = equity_curve.cummax()
    drawdown = equity_curve / rolling_peak - 1.0
    return float(drawdown.min())


def _annualized_sharpe(returns: pd.Series) -> float:
    valid = returns.dropna()
    if len(valid) < 2 or valid.std() == 0:
        return 0.0
    return float(valid.mean() / valid.std() * np.sqrt(252))


def _apply_baseline_positions(df: pd.DataFrame, caution: float = 0.45, risk_off: float = 0.60) -> pd.Series:
    position = pd.Series(1.0, index=df.index)
    position[df["tail_risk_probability"] >= caution] = 0.5
    position[df["tail_risk_probability"] >= risk_off] = 0.0
    return position


def _evaluate_variant(usable: pd.DataFrame, *, label: str, mode: str) -> dict[str, object]:
    result = usable.copy()
    position = _apply_baseline_positions(result)

    if mode == "basis_support":
        support_mask = (
            (result["basis_support_window"] == 1)
            & (result["panic_break"] == 0)
            & (result["tail_risk_probability"] < 0.72)
        )
        position[support_mask] = position[support_mask].clip(lower=0.5)
    elif mode == "basis_guard":
        trap_mask = (
            (result["basis_trap_window"] == 1)
            & (result["tail_risk_probability"] >= 0.35)
        )
        position[trap_mask] = 0.0
    elif mode == "combined":
        trap_mask = (
            (result["basis_trap_window"] == 1)
            & (result["tail_risk_probability"] >= 0.35)
        )
        position[trap_mask] = 0.0
        support_mask = (
            (result["basis_support_window"] == 1)
            & (result["panic_break"] == 0)
            & (result["tail_risk_probability"] < 0.72)
            & (result["basis_trap_window"] == 0)
        )
        position[support_mask] = position[support_mask].clip(lower=0.5)
    elif mode != "baseline":
        raise ValueError(f"unknown mode: {mode}")

    result["position"] = position
    result["strategy_return"] = result["position"] * result["next_day_return"]
    result["buy_hold_return"] = result["next_day_return"]
    result["strategy_equity"] = (1.0 + result["strategy_return"]).cumprod()
    result["buy_hold_equity"] = (1.0 + result["buy_hold_return"]).cumprod()

    support_slice = result[result["basis_support_window"] == 1]
    trap_slice = result[result["basis_trap_window"] == 1]
    return {
        "label": label,
        "sample_count": int(len(result)),
        "strategy_total_return": float(result["strategy_equity"].iloc[-1] - 1.0),
        "buy_hold_total_return": float(result["buy_hold_equity"].iloc[-1] - 1.0),
        "strategy_max_drawdown": _max_drawdown(result["strategy_equity"]),
        "strategy_sharpe": _annualized_sharpe(result["strategy_return"]),
        "avg_position": float(result["position"].mean()),
        "support_days": int((result["basis_support_window"] == 1).sum()),
        "trap_days": int((result["basis_trap_window"] == 1).sum()),
        "support_next_day": float(support_slice["next_day_return"].mean()) if not support_slice.empty else 0.0,
        "support_next_3d": float(support_slice["next_3d_return"].mean()) if not support_slice.empty else 0.0,
        "trap_next_day": float(trap_slice["next_day_return"].mean()) if not trap_slice.empty else 0.0,
        "trap_next_3d": float(trap_slice["next_3d_return"].mean()) if not trap_slice.empty else 0.0,
    }


def _build_report(results: list[dict[str, object]], *, start: str, end: str) -> str:
    ordered = sorted(results, key=lambda item: float(item["strategy_sharpe"]), reverse=True)
    top = ordered[0]
    baseline = next(item for item in ordered if item["label"] == "基线阈值")
    source_summary = results[0].get("discount_source_summary", "")
    support_threshold = results[0].get("basis_support_threshold", 0.0)
    trap_carry_threshold = results[0].get("basis_trap_carry_change_threshold", 0.0)
    trap_discount_threshold = results[0].get("basis_trap_discount_change_threshold", 0.0)
    lines = [
        f"# {datetime.now().strftime('%Y-%m-%d')} IC贴水专属验证报告",
        "",
        f"- 验证区间: {start} 至 {end}",
        "- 标的代理: 本地ETF缓存优先读取 510500，缺失时回退 510500.SS",
        "- 目标: 验证 IC 贴水环境是否值得作为中证500/IC 这条线的专属覆盖层",
        f"- 贴水来源: {source_summary}",
        f"- 当前支撑阈值: 年化贴水 >= {support_threshold:.2%}",
        f"- 当前陷阱阈值: 年化贴水5日变化 >= {trap_carry_threshold:.2%} 或 贴水率5日变化 >= {trap_discount_threshold:.2%}",
        "",
        "## 覆盖方案结果",
        "",
        "| 方案 | 样本数 | 策略收益 | 买入持有收益 | 最大回撤 | Sharpe | 平均仓位 | 支撑窗口天数 | 陷阱窗口天数 |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for item in ordered:
        lines.append(
            f"| {item['label']} | {item['sample_count']} | {item['strategy_total_return'] * 100:.2f}% | "
            f"{item['buy_hold_total_return'] * 100:.2f}% | {item['strategy_max_drawdown'] * 100:.2f}% | "
            f"{item['strategy_sharpe']:.2f} | {item['avg_position']:.2f} | {item['support_days']} | {item['trap_days']} |"
        )

    lines.extend([
        "",
        "## 贴水环境统计",
        "",
        "| 方案 | 支撑窗口次日均值 | 支撑窗口3日均值 | 陷阱窗口次日均值 | 陷阱窗口3日均值 |",
        "| --- | ---: | ---: | ---: | ---: |",
    ])
    for item in ordered:
        lines.append(
            f"| {item['label']} | {item['support_next_day'] * 100:.2f}% | {item['support_next_3d'] * 100:.2f}% | "
            f"{item['trap_next_day'] * 100:.2f}% | {item['trap_next_3d'] * 100:.2f}% |"
        )

    lines.extend(["", "## 结论", ""])
    lines.append(f"- 当前最优方案是 `{top['label']}`，Sharpe `{top['strategy_sharpe']:.2f}`。")
    if float(top["strategy_sharpe"]) > float(baseline["strategy_sharpe"]):
        lines.append("- 说明 IC 贴水环境作为专属覆盖层是有增益的，值得继续朝更交易化的主力合约标签深化。")
    else:
        lines.append("- 说明这版真实贴水覆盖层暂时没有显著跑赢基线，问题更像是交易标签还不够贴近你的真实移仓动作。")

    if float(baseline["support_next_3d"]) > 0 and float(baseline["trap_next_3d"]) < 0:
        lines.append("- 从条件统计看，深贴水且未破位更像支撑窗口，而贴水加深叠加破位更像基差陷阱。")
    else:
        lines.append("- 当前条件统计还不够干净，支撑窗口和陷阱窗口的分离度一般。")

    if top["label"] != "基线阈值":
        lines.append("- 下一步更值得做的是把主力合约切换、到期前移仓和基差收敛收益，真正写进交易标签。")
    else:
        lines.append("- 下一步不建议急着并入主模型，先把主力合约切换、到期前移仓和基差收敛收益，真正写进交易标签。")
    return "\n".join(lines)


def main() -> int:
    args = _build_parser().parse_args()
    frame = _prepare_frame(
        args.start,
        args.end,
        args.cache_dir,
        args.data_cache_dir,
        args.refresh_data_cache,
    )
    cache_dir = Path(args.probability_cache_dir)
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_path = _probability_cache_path(cache_dir, args.start, args.end, 756, 42)
    usable = _generate_walk_forward_probabilities(
        frame,
        min_train_days=756,
        retrain_every=42,
        cache_path=cache_path,
        refresh_cache=args.refresh_probability_cache,
    )
    if usable.empty and not args.refresh_probability_cache:
        usable = _generate_walk_forward_probabilities(
            frame,
            min_train_days=756,
            retrain_every=42,
            cache_path=cache_path,
            refresh_cache=True,
        )
    if usable.empty:
        raise RuntimeError("walk-forward 概率为空，无法做 IC 贴水专属验证")

    source_counts = usable["ic_discount_source"].value_counts(dropna=False).to_dict()
    source_summary = "真实连续贴水优先"
    if source_counts.get("real_continuous", 0) == 0:
        source_summary = "仅代理贴水"
    elif source_counts.get("proxy", 0):
        source_summary = (
            f"真实连续贴水 {source_counts.get('real_continuous', 0)} 天"
            f" + 代理补足 {source_counts.get('proxy', 0)} 天"
        )
    else:
        source_summary = f"真实连续贴水 {source_counts.get('real_continuous', 0)} 天"

    results = [
        _evaluate_variant(usable, label="基线阈值", mode="baseline"),
        _evaluate_variant(usable, label="贴水支撑覆盖", mode="basis_support"),
        _evaluate_variant(usable, label="基差陷阱防守", mode="basis_guard"),
        _evaluate_variant(usable, label="贴水支撑+陷阱防守", mode="combined"),
    ]
    for item in results:
        item["discount_source_summary"] = source_summary
        item["basis_support_threshold"] = float(frame["basis_support_threshold"].dropna().iloc[0]) if frame["basis_support_threshold"].dropna().size else 0.0
        item["basis_trap_carry_change_threshold"] = float(frame["basis_trap_carry_change_threshold"].dropna().iloc[0]) if frame["basis_trap_carry_change_threshold"].dropna().size else 0.0
        item["basis_trap_discount_change_threshold"] = float(frame["basis_trap_discount_change_threshold"].dropna().iloc[0]) if frame["basis_trap_discount_change_threshold"].dropna().size else 0.0

    report = _build_report(results, start=args.start, end=args.end)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    report_date = datetime.now().strftime("%Y-%m-%d")
    report_path = output_dir / f"{report_date}_IC贴水专属验证报告.md"
    latest_path = output_dir / "latest_ic_basis_overlay_validation.md"
    report_path.write_text(report, encoding="utf-8")
    latest_path.write_text(report, encoding="utf-8")

    print("=" * 72)
    print("IC贴水专属验证完成")
    print(f"报告: {report_path}")
    print("=" * 72)
    print(report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
