"""Shared helpers for the next-production metaphysical model backtest path."""

from __future__ import annotations

import re
from pathlib import Path

import pandas as pd

from .model_defaults import NEXT_PRODUCTION_MODEL_DEFAULTS
from .report_ingest import compare_tactical_report_to_model_signal, parse_tactical_report_text
from .report_ingest import assess_tactical_report_freshness


def resolve_next_production_model_params(**overrides) -> dict[str, float | int]:
    """Return merged model/backtest defaults with explicit overrides."""
    params = dict(NEXT_PRODUCTION_MODEL_DEFAULTS)
    params.update({key: value for key, value in overrides.items() if value is not None})
    return params


def apply_next_production_position_sizing(
    frame: pd.DataFrame,
    *,
    probability_col: str = "tail_risk_probability",
    caution_threshold: float | None = None,
    risk_off_threshold: float | None = None,
    copy: bool = True,
) -> pd.DataFrame:
    """Map tail-risk probabilities to the shared 1.0 / 0.5 / 0.0 exposure regime."""
    params = resolve_next_production_model_params(
        caution_threshold=caution_threshold,
        risk_off_threshold=risk_off_threshold,
    )
    result = frame.copy() if copy else frame
    prob = pd.to_numeric(result[probability_col], errors="coerce")
    result["position"] = 1.0
    result.loc[prob >= float(params["caution_threshold"]), "position"] = 0.5
    result.loc[prob >= float(params["risk_off_threshold"]), "position"] = 0.0
    result["position_regime"] = "full_risk"
    result.loc[result["position"] == 0.5, "position_regime"] = "caution"
    result.loc[result["position"] == 0.0, "position_regime"] = "risk_off"
    return result


def summarize_next_production_position_sizing(frame: pd.DataFrame) -> dict[str, float | int]:
    """Summarize current shared position-sizing output for diagnostics."""
    position = pd.to_numeric(frame.get("position"), errors="coerce")
    return {
        "avg_position": float(position.mean()),
        "risk_off_days": int((position == 0.0).sum()),
        "caution_days": int((position == 0.5).sum()),
        "full_risk_days": int((position == 1.0).sum()),
    }


def build_next_production_signal_frame(
    frame: pd.DataFrame,
    *,
    probability_col: str = "tail_risk_probability",
    date_col: str = "date",
    caution_threshold: float | None = None,
    risk_off_threshold: float | None = None,
    copy: bool = True,
) -> pd.DataFrame:
    """Build a compact daily signal frame from tail-risk probabilities."""
    result = apply_next_production_position_sizing(
        frame,
        probability_col=probability_col,
        caution_threshold=caution_threshold,
        risk_off_threshold=risk_off_threshold,
        copy=copy,
    )
    output = pd.DataFrame(index=result.index)
    if date_col in result.columns:
        output["signal_date"] = pd.to_datetime(result[date_col], errors="coerce")
    else:
        output["signal_date"] = pd.to_datetime(result.index, errors="coerce")
    output["tail_risk_probability"] = pd.to_numeric(result[probability_col], errors="coerce")
    output["position"] = pd.to_numeric(result["position"], errors="coerce")
    output["position_regime"] = result["position_regime"].astype(str)
    action_map = {
        "risk_off": "risk_off",
        "caution": "reduce",
        "full_risk": "hold_or_add",
    }
    output["action"] = output["position_regime"].map(action_map).fillna("hold")
    return output


def latest_next_production_signal(
    frame: pd.DataFrame,
    *,
    probability_col: str = "tail_risk_probability",
    date_col: str = "date",
    caution_threshold: float | None = None,
    risk_off_threshold: float | None = None,
) -> dict[str, object]:
    """Return the latest daily next-production signal as a plain dict."""
    signals = build_next_production_signal_frame(
        frame,
        probability_col=probability_col,
        date_col=date_col,
        caution_threshold=caution_threshold,
        risk_off_threshold=risk_off_threshold,
        copy=True,
    )
    latest = signals.dropna(subset=["tail_risk_probability"]).iloc[-1]
    return {
        "signal_date": pd.Timestamp(latest["signal_date"]).date().isoformat(),
        "tail_risk_probability": float(latest["tail_risk_probability"]),
        "position": float(latest["position"]),
        "position_regime": str(latest["position_regime"]),
        "action": str(latest["action"]),
    }


def apply_tactical_report_signal_overlay(
    model_signal: dict[str, object],
    *,
    report_summary: dict[str, object] | None = None,
    report_text: str | None = None,
    report_freshness: dict[str, object] | None = None,
    caution_risk_score: int = 3,
    high_risk_score: int = 5,
) -> dict[str, object]:
    """Overlay the latest signal with tactical-report risk when the text is strongly defensive."""
    if report_summary is None:
        report_summary = parse_tactical_report_text(report_text or "")

    comparison = compare_tactical_report_to_model_signal(report_summary, model_signal)
    risk_score = int(comparison["report_risk_score"])
    severe_pair_active = (
        int(report_summary.get("black_swan_warning", 0) or 0) == 1
        and int(report_summary.get("physical_blockade", 0) or 0) == 1
    )

    raw_position = float(model_signal.get("position", 0.0) or 0.0)
    raw_regime = str(model_signal.get("position_regime") or "unknown")
    raw_action = str(model_signal.get("action") or "hold")

    position = raw_position
    regime = raw_regime
    action = raw_action
    overlay_active = False
    overlay_reason = None
    overlay_skipped = False
    overlay_skip_reason = None

    if report_freshness is not None and not bool(report_freshness.get("is_fresh")):
        overlay_skipped = True
        overlay_skip_reason = str(report_freshness.get("freshness_reason") or "").strip() or "外部归档过期，今天不启用文本纠偏。"
    elif risk_score >= high_risk_score and severe_pair_active:
        if raw_regime != "risk_off":
            position = 0.0
            regime = "risk_off"
            action = "risk_off"
            overlay_active = True
            overlay_reason = "日报高危风险标签集中出现，且黑天鹅预警与物理封锁同时生效，强制切到 risk_off。"
    elif risk_score >= caution_risk_score and raw_regime == "full_risk":
        position = 0.5
        regime = "caution"
        action = "reduce"
        overlay_active = True
        overlay_reason = "日报风险叙事显著偏空，先把 full_risk 下调一档到 caution。"

    result = dict(model_signal)
    result.update(
        {
            "raw_position": raw_position,
            "raw_position_regime": raw_regime,
            "raw_action": raw_action,
            "position": position,
            "position_regime": regime,
            "action": action,
            "overlay_active": overlay_active,
            "overlay_reason": overlay_reason,
            "report_overlay_skipped": overlay_skipped,
            "report_overlay_skip_reason": overlay_skip_reason,
            "report_risk_score": risk_score,
            "report_alignment": "stale_report" if overlay_skipped else comparison["alignment"],
            "report_core_stance": report_summary.get("core_stance"),
            "report_is_fresh": bool(report_freshness.get("is_fresh")) if report_freshness is not None else True,
            "report_date_iso": report_freshness.get("report_date_iso") if report_freshness is not None else None,
            "expected_report_date_iso": report_freshness.get("expected_report_date_iso") if report_freshness is not None else None,
        }
    )
    return result


def latest_next_production_signal_with_report_overlay(
    frame: pd.DataFrame,
    *,
    report_summary: dict[str, object] | None = None,
    report_text: str | None = None,
    expected_report_date: str | None = None,
    probability_col: str = "tail_risk_probability",
    date_col: str = "date",
    caution_threshold: float | None = None,
    risk_off_threshold: float | None = None,
    caution_risk_score: int = 3,
    high_risk_score: int = 5,
) -> dict[str, object]:
    """Return the latest signal after applying the tactical-report overlay."""
    report_freshness = assess_tactical_report_freshness(
        report_text or "",
        expected_date_iso=expected_report_date,
    ) if report_text else None
    raw_signal = latest_next_production_signal(
        frame,
        probability_col=probability_col,
        date_col=date_col,
        caution_threshold=caution_threshold,
        risk_off_threshold=risk_off_threshold,
    )
    return apply_tactical_report_signal_overlay(
        raw_signal,
        report_summary=report_summary,
        report_text=report_text,
        report_freshness=report_freshness,
        caution_risk_score=caution_risk_score,
        high_risk_score=high_risk_score,
    )


def next_production_probability_cache_path(
    cache_dir: str | Path,
    *,
    symbol: str,
    start: str,
    end: str,
    min_train_days: int,
    retrain_every: int,
) -> Path:
    """Return the canonical cached probability frame path."""
    def _sanitize_token(value: str) -> str:
        return re.sub(r"[^A-Za-z0-9._-]+", "_", value)

    base = Path(cache_dir)
    filename = (
        f"{_sanitize_token(symbol)}_{_sanitize_token(start)}_{_sanitize_token(end)}"
        f"_min{min_train_days}_retrain{retrain_every}.pkl"
    )
    return base / filename


def latest_cached_next_production_signal(
    *,
    cache_dir: str | Path,
    symbol: str,
    start: str,
    end: str,
    min_train_days: int | None = None,
    retrain_every: int | None = None,
    caution_threshold: float | None = None,
    risk_off_threshold: float | None = None,
    report_summary: dict[str, object] | None = None,
    report_text: str | None = None,
    expected_report_date: str | None = None,
) -> dict[str, object]:
    """Load the cached probability frame and return the latest raw or overlay-adjusted signal."""
    params = resolve_next_production_model_params(
        min_train_days=min_train_days,
        retrain_every=retrain_every,
        caution_threshold=caution_threshold,
        risk_off_threshold=risk_off_threshold,
    )
    cache_path = next_production_probability_cache_path(
        cache_dir,
        symbol=symbol,
        start=start,
        end=end,
        min_train_days=int(params["min_train_days"]),
        retrain_every=int(params["retrain_every"]),
    )
    if not cache_path.exists():
        cache_base = Path(cache_dir)
        pattern = (
            f"{re.sub(r'[^A-Za-z0-9._-]+', '_', symbol)}_"
            f"{re.sub(r'[^A-Za-z0-9._-]+', '_', start)}_*"
            f"_min{int(params['min_train_days'])}_retrain{int(params['retrain_every'])}.pkl"
        )
        fallback_candidates = sorted(cache_base.glob(pattern), key=lambda item: item.stat().st_mtime)
        if fallback_candidates:
            cache_path = fallback_candidates[-1]
    frame = pd.read_pickle(cache_path)
    if report_summary is not None or report_text:
        result = latest_next_production_signal_with_report_overlay(
            frame,
            report_summary=report_summary,
            report_text=report_text,
            expected_report_date=expected_report_date,
            caution_threshold=float(params["caution_threshold"]),
            risk_off_threshold=float(params["risk_off_threshold"]),
        )
    else:
        result = latest_next_production_signal(
            frame,
            caution_threshold=float(params["caution_threshold"]),
            risk_off_threshold=float(params["risk_off_threshold"]),
        )
    result["cache_path"] = str(cache_path)
    return result
