"""Persistence helpers for daily learning snapshots and training run records."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

from .report_ingest import archive_tactical_report_text, build_tactical_report_optimization_notes
from .warehouse import LEDGER_DIR, ensure_metaphysical_warehouse
from .model_defaults import NEXT_PRODUCTION_MODEL_DEFAULTS


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for raw_line in handle:
            line = raw_line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")
    return path


def _upsert_jsonl(path: Path, row: dict[str, Any], *, keys: tuple[str, ...]) -> Path:
    rows = _read_jsonl(path)
    key_tuple = tuple(row.get(key) for key in keys)
    filtered = [item for item in rows if tuple(item.get(key) for key in keys) != key_tuple]
    filtered.append(row)
    return _write_jsonl(path, filtered)


def _mirror_ledger(path: Path) -> Path:
    ensure_metaphysical_warehouse()
    mirror_path = LEDGER_DIR / path.name
    if path.exists():
        mirror_path.write_text(path.read_text(encoding="utf-8"), encoding="utf-8")
    return mirror_path


def _learning_maturity_status(outcomes: dict[str, Any] | None) -> str:
    values = dict(outcomes or {})
    if values.get("next_10d_return") is not None:
        return "matured_10d"
    if values.get("next_5d_return") is not None:
        return "matured_5d"
    if values.get("next_3d_return") is not None:
        return "matured_3d"
    if values.get("next_1d_return") is not None:
        return "matured_1d"
    return "observing"


def build_daily_learning_snapshot(
    *,
    report_text: str,
    final_signal: dict[str, Any],
    symbol: str = "510500.SS",
    source_label: str = "gemini_google_doc_archive",
    report_path: str | None = None,
) -> dict[str, Any]:
    """Build a structured daily learning snapshot from the report and final signal."""
    notes = build_tactical_report_optimization_notes(
        report_text,
        model_signal={
            "position_regime": final_signal.get("raw_position_regime", final_signal.get("position_regime")),
            "position": final_signal.get("raw_position", final_signal.get("position")),
            "action": final_signal.get("raw_action", final_signal.get("action")),
        },
        archive_dir=Path(__file__).resolve().parents[3] / "reports" / "gemini_daily_archive",
    )
    summary = notes["report_summary"]
    return {
        "report_date": summary.get("report_date_text"),
        "symbol": symbol,
        "source_label": source_label,
        "report_path": report_path,
        "generated_at": datetime.now().isoformat(),
        "core_stance": summary.get("core_stance"),
        "candidate_feature_flags": notes.get("candidate_feature_flags") or [],
        "optimization_priority": notes.get("optimization_priority"),
        "report_risk_score": final_signal.get("report_risk_score"),
        "report_alignment": final_signal.get("report_alignment"),
        "tail_risk_probability": final_signal.get("tail_risk_probability"),
        "raw_position_regime": final_signal.get("raw_position_regime", final_signal.get("position_regime")),
        "final_position_regime": final_signal.get("position_regime"),
        "overlay_active": bool(final_signal.get("overlay_active", False)),
        "overlay_reason": final_signal.get("overlay_reason"),
        "cache_path": final_signal.get("cache_path"),
        "report_summary": summary,
        "duplicate_check": notes.get("duplicate_check") or {},
        "future_outcomes": {
            "next_1d_return": None,
            "next_3d_return": None,
            "next_5d_return": None,
            "next_10d_return": None,
            "max_drawdown_10d": None,
        },
        "maturity_status": "observing",
    }


def record_daily_learning_snapshot(
    path: str | Path,
    *,
    report_text: str,
    final_signal: dict[str, Any],
    symbol: str = "510500.SS",
    source_label: str = "gemini_google_doc_archive",
    report_path: str | None = None,
) -> Path:
    """Upsert one daily learning snapshot keyed by report_date + symbol."""
    snapshot = build_daily_learning_snapshot(
        report_text=report_text,
        final_signal=final_signal,
        symbol=symbol,
        source_label=source_label,
        report_path=report_path,
    )
    archive_tactical_report_text(
        report_text,
        report_date_text=snapshot.get("report_date"),
        archive_dir=Path(__file__).resolve().parents[3] / "reports" / "gemini_daily_archive",
    )
    target = _upsert_jsonl(Path(path), snapshot, keys=("report_date", "symbol"))
    _mirror_ledger(target)
    return target


def build_training_run_record(
    *,
    symbol: str,
    start: str,
    end: str,
    sample_count: int,
    feature_count: int,
    auc: float,
    ap: float,
    feature_pool: str = "NEXT_PRODUCTION_WEIGHTED_CANDIDATE_FEATURES",
    model_name: str = "RandomForestClassifier",
    params: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build a structured training run record for future model comparison."""
    return {
        "run_timestamp": datetime.now().isoformat(),
        "symbol": symbol,
        "start": start,
        "end": end,
        "sample_count": int(sample_count),
        "feature_count": int(feature_count),
        "auc": float(auc),
        "ap": float(ap),
        "feature_pool": feature_pool,
        "model_name": model_name,
        "params": params or {},
    }


def record_training_run(
    path: str | Path,
    *,
    symbol: str,
    start: str,
    end: str,
    sample_count: int,
    feature_count: int,
    auc: float,
    ap: float,
    feature_pool: str = "NEXT_PRODUCTION_WEIGHTED_CANDIDATE_FEATURES",
    model_name: str = "RandomForestClassifier",
    params: dict[str, Any] | None = None,
) -> Path:
    """Append one immutable training run record to JSONL."""
    row = build_training_run_record(
        symbol=symbol,
        start=start,
        end=end,
        sample_count=sample_count,
        feature_count=feature_count,
        auc=auc,
        ap=ap,
        feature_pool=feature_pool,
        model_name=model_name,
        params=params,
    )
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(row, ensure_ascii=False) + "\n")
    _mirror_ledger(target)
    return target


def _parse_report_date(value: str | None) -> pd.Timestamp | None:
    if not value:
        return None
    normalized = (
        str(value)
        .replace("年", "-")
        .replace("月", "-")
        .replace("日", "")
        .strip()
    )
    dt = pd.to_datetime(normalized, errors="coerce")
    if pd.isna(dt):
        return None
    return pd.Timestamp(dt).normalize()


def _next_trading_index(prices: pd.DataFrame, report_date: pd.Timestamp) -> int | None:
    candidates = prices.index[prices["date"] >= report_date]
    if len(candidates) == 0:
        return None
    return int(candidates[0])


def compute_learning_outcomes_from_prices(
    report_date: str | None,
    prices: pd.DataFrame,
) -> dict[str, float | None]:
    """Compute forward returns and max drawdown from a daily close series."""
    dt = _parse_report_date(report_date)
    if dt is None:
        return {
            "next_1d_return": None,
            "next_3d_return": None,
            "next_5d_return": None,
            "next_10d_return": None,
            "max_drawdown_10d": None,
        }

    frame = prices.copy()
    frame["date"] = pd.to_datetime(frame["date"]).dt.normalize()
    frame = frame.sort_values("date").reset_index(drop=True)
    start_idx = _next_trading_index(frame, dt)
    if start_idx is None:
        return {
            "next_1d_return": None,
            "next_3d_return": None,
            "next_5d_return": None,
            "next_10d_return": None,
            "max_drawdown_10d": None,
        }

    start_close = float(frame.loc[start_idx, "close"])

    def _forward_return(offset: int) -> float | None:
        target_idx = start_idx + offset
        if target_idx >= len(frame):
            return None
        target_close = float(frame.loc[target_idx, "close"])
        return float(target_close / start_close - 1.0)

    horizon_end = min(start_idx + 10, len(frame) - 1)
    window = frame.loc[start_idx:horizon_end, "close"].astype(float)
    running_peak = window.cummax()
    drawdown = window / running_peak - 1.0

    return {
        "next_1d_return": _forward_return(1),
        "next_3d_return": _forward_return(3),
        "next_5d_return": _forward_return(5),
        "next_10d_return": _forward_return(10),
        "max_drawdown_10d": float(drawdown.min()) if len(drawdown) > 0 else None,
    }


def backfill_learning_snapshot_outcomes(
    path: str | Path,
    prices: pd.DataFrame,
) -> Path:
    """Backfill future outcome fields in the daily learning snapshot ledger."""
    target = Path(path)
    rows = _read_jsonl(target)
    updated_rows: list[dict[str, Any]] = []
    for row in rows:
        outcomes = compute_learning_outcomes_from_prices(row.get("report_date"), prices)
        current_outcomes = dict(row.get("future_outcomes") or {})
        current_outcomes.update(outcomes)
        row["future_outcomes"] = current_outcomes
        row["maturity_status"] = _learning_maturity_status(current_outcomes)
        updated_rows.append(row)
    output = _write_jsonl(target, updated_rows)
    _mirror_ledger(output)
    return output


def select_matured_learning_samples(
    path: str | Path,
    *,
    required_status: str = "matured_10d",
) -> list[dict[str, Any]]:
    """Load only matured learning samples from the JSONL ledger."""
    rows = _read_jsonl(Path(path))
    return [row for row in rows if str(row.get("maturity_status") or "") == required_status]


def summarize_training_run_window(
    path: str | Path,
    *,
    recent_n: int = 3,
) -> dict[str, Any]:
    """Summarize the most recent training run window for promotion checks."""
    rows = _read_jsonl(Path(path))
    if not rows:
        return {
            "run_count": 0,
            "recent_n": recent_n,
            "mean_auc": None,
            "mean_ap": None,
            "latest_auc": None,
            "latest_ap": None,
        }

    recent = rows[-recent_n:]
    auc_values = [float(item["auc"]) for item in recent if item.get("auc") is not None]
    ap_values = [float(item["ap"]) for item in recent if item.get("ap") is not None]
    latest = recent[-1]
    return {
        "run_count": len(rows),
        "recent_n": len(recent),
        "mean_auc": float(sum(auc_values) / len(auc_values)) if auc_values else None,
        "mean_ap": float(sum(ap_values) / len(ap_values)) if ap_values else None,
        "latest_auc": float(latest["auc"]) if latest.get("auc") is not None else None,
        "latest_ap": float(latest["ap"]) if latest.get("ap") is not None else None,
    }


def evaluate_candidate_promotion_readiness(
    path: str | Path,
    *,
    recent_n: int = 3,
    min_runs: int = 2,
    auc_floor: float = 0.60,
    ap_floor: float = 0.18,
) -> dict[str, Any]:
    """Evaluate whether the current candidate looks ready for promotion."""
    summary = summarize_training_run_window(path, recent_n=recent_n)
    reasons: list[str] = []
    ready = True

    if int(summary["run_count"] or 0) < min_runs:
        ready = False
        reasons.append("训练记录数量不足")
    if summary["mean_auc"] is None or float(summary["mean_auc"]) < auc_floor:
        ready = False
        reasons.append("近期平均 AUC 未达门槛")
    if summary["mean_ap"] is None or float(summary["mean_ap"]) < ap_floor:
        ready = False
        reasons.append("近期平均 AP 未达门槛")

    if ready:
        reasons.append("近期训练窗口达到候选升版门槛")

    return {
        **summary,
        "promotion_ready": ready,
        "reasons": reasons,
        "thresholds": {
            "recent_n": recent_n,
            "min_runs": min_runs,
            "auc_floor": auc_floor,
            "ap_floor": ap_floor,
        },
    }


def evaluate_stage_promotion_readiness(
    path: str | Path,
    *,
    stage: str,
    recent_n: int = 3,
    min_runs: int = 2,
    auc_floor: float = 0.60,
    ap_floor: float = 0.18,
) -> dict[str, Any]:
    """Evaluate readiness against one named governance stage."""
    readiness = evaluate_candidate_promotion_readiness(
        path,
        recent_n=recent_n,
        min_runs=min_runs,
        auc_floor=auc_floor,
        ap_floor=ap_floor,
    )
    return {
        **readiness,
        "stage": stage,
    }


def evaluate_governance_stage_flow(
    snapshot_path: str | Path,
    training_path: str | Path,
    *,
    current_stage: str = "candidate",
    required_status: str = "matured_10d",
    min_matured_samples_for_candidate: int = 2,
    min_matured_samples_for_shadow: int = 5,
    min_matured_samples_for_production: int = 12,
    recent_n: int = 3,
    min_runs: int = 2,
    auc_floor: float = 0.60,
    ap_floor: float = 0.18,
    shadow_auc_floor: float = 0.62,
    shadow_ap_floor: float = 0.20,
    production_auc_floor: float = 0.64,
    production_ap_floor: float = 0.22,
    production_min_runs: int = 3,
) -> dict[str, Any]:
    """Evaluate multi-stage governance flow from research to production."""
    matured = select_matured_learning_samples(snapshot_path, required_status=required_status)
    matured_count = len(matured)

    current_stage = str(current_stage or "candidate").strip().lower()
    if current_stage not in {"research", "candidate", "shadow", "production"}:
        current_stage = "candidate"

    candidate_readiness = evaluate_stage_promotion_readiness(
        training_path,
        stage="candidate",
        recent_n=recent_n,
        min_runs=min_runs,
        auc_floor=auc_floor,
        ap_floor=ap_floor,
    )
    shadow_readiness = evaluate_stage_promotion_readiness(
        training_path,
        stage="shadow",
        recent_n=recent_n,
        min_runs=min_runs,
        auc_floor=shadow_auc_floor,
        ap_floor=shadow_ap_floor,
    )
    production_readiness = evaluate_stage_promotion_readiness(
        training_path,
        stage="production",
        recent_n=recent_n,
        min_runs=max(min_runs, production_min_runs),
        auc_floor=production_auc_floor,
        ap_floor=production_ap_floor,
    )

    action = "continue_observing"
    target_stage = current_stage
    reason = "继续观察。"

    if current_stage == "research":
        if not candidate_readiness["promotion_ready"]:
            action = "continue_observing"
            target_stage = "research"
            reason = "研究层训练质量门槛未满足，继续留在 research。"
        elif matured_count < min_matured_samples_for_candidate:
            action = "continue_observing"
            target_stage = "research"
            reason = "研究层训练质量达标，但成熟样本不足，继续留在 research。"
        else:
            action = "promote_to_candidate"
            target_stage = "candidate"
            reason = "研究层训练质量达标且成熟样本达到门槛，可进入 candidate。"
    elif current_stage == "candidate":
        if not candidate_readiness["promotion_ready"]:
            action = "keep_candidate"
            target_stage = "candidate"
            reason = "candidate 训练质量门槛未满足，继续保留在 candidate。"
        elif matured_count < min_matured_samples_for_shadow:
            action = "keep_candidate"
            target_stage = "candidate"
            reason = "训练质量达标，但成熟样本不足，先保留在 candidate。"
        else:
            action = "promote_to_shadow"
            target_stage = "shadow"
            reason = "训练质量达标且成熟样本达到门槛，可进入 shadow。"
    elif current_stage == "shadow":
        if not shadow_readiness["promotion_ready"]:
            action = "keep_shadow"
            target_stage = "shadow"
            reason = "shadow 阶段训练质量门槛未满足，继续留在 shadow。"
        elif matured_count < min_matured_samples_for_production:
            action = "keep_shadow"
            target_stage = "shadow"
            reason = "shadow 阶段训练质量达标，但成熟样本不足，继续留在 shadow。"
        elif not production_readiness["promotion_ready"]:
            action = "keep_shadow"
            target_stage = "shadow"
            reason = "shadow 阶段样本充足，但 production 门槛尚未满足，继续留在 shadow。"
        else:
            action = "promote_to_production"
            target_stage = "production"
            reason = "shadow 阶段训练质量与成熟样本均达标，可进入 production。"
    else:
        action = "keep_production"
        target_stage = "production"
        if production_readiness["promotion_ready"]:
            reason = "production 继续满足门槛，维持 production。"
        else:
            reason = "production 暂未满足更高门槛，但当前逻辑不自动降级，维持 production。"

    return {
        "action": action,
        "reason": reason,
        "current_stage": current_stage,
        "target_stage": target_stage,
        "matured_sample_count": matured_count,
        "required_status": required_status,
        "stage_thresholds": {
            "candidate": {
                "min_matured_samples": min_matured_samples_for_candidate,
                "recent_n": recent_n,
                "min_runs": min_runs,
                "auc_floor": auc_floor,
                "ap_floor": ap_floor,
            },
            "shadow": {
                "min_matured_samples": min_matured_samples_for_shadow,
                "recent_n": recent_n,
                "min_runs": min_runs,
                "auc_floor": shadow_auc_floor,
                "ap_floor": shadow_ap_floor,
            },
            "production": {
                "min_matured_samples": min_matured_samples_for_production,
                "recent_n": recent_n,
                "min_runs": max(min_runs, production_min_runs),
                "auc_floor": production_auc_floor,
                "ap_floor": production_ap_floor,
            },
        },
        "promotion_readiness": candidate_readiness,
        "stage_readiness": {
            "candidate": candidate_readiness,
            "shadow": shadow_readiness,
            "production": production_readiness,
        },
    }


def evaluate_governance_action(
    snapshot_path: str | Path,
    training_path: str | Path,
    *,
    current_stage: str = "candidate",
    required_status: str = "matured_10d",
    min_matured_samples_for_candidate: int = 2,
    min_matured_samples_for_shadow: int = 5,
    min_matured_samples_for_production: int = 12,
    recent_n: int = 3,
    min_runs: int = 2,
    auc_floor: float = 0.60,
    ap_floor: float = 0.18,
    shadow_auc_floor: float = 0.62,
    shadow_ap_floor: float = 0.20,
    production_auc_floor: float = 0.64,
    production_ap_floor: float = 0.22,
    production_min_runs: int = 3,
) -> dict[str, Any]:
    """Translate sample maturity and training quality into a governance action."""
    governance = evaluate_governance_stage_flow(
        snapshot_path,
        training_path,
        current_stage=current_stage,
        required_status=required_status,
        min_matured_samples_for_candidate=min_matured_samples_for_candidate,
        min_matured_samples_for_shadow=min_matured_samples_for_shadow,
        min_matured_samples_for_production=min_matured_samples_for_production,
        recent_n=recent_n,
        min_runs=min_runs,
        auc_floor=auc_floor,
        ap_floor=ap_floor,
        shadow_auc_floor=shadow_auc_floor,
        shadow_ap_floor=shadow_ap_floor,
        production_auc_floor=production_auc_floor,
        production_ap_floor=production_ap_floor,
        production_min_runs=production_min_runs,
    )
    governance["min_matured_samples_for_shadow"] = min_matured_samples_for_shadow
    return governance


def build_governance_run_record(
    *,
    governance: dict[str, Any],
    source_label: str = "weekly_evaluation",
) -> dict[str, Any]:
    """Build a durable governance decision record for later stage reviews."""
    readiness = dict(governance.get("promotion_readiness") or {})
    return {
        "run_timestamp": datetime.now().isoformat(),
        "source_label": source_label,
        "current_stage": governance.get("current_stage"),
        "target_stage": governance.get("target_stage"),
        "action": governance.get("action"),
        "reason": governance.get("reason"),
        "matured_sample_count": int(governance.get("matured_sample_count") or 0),
        "required_status": governance.get("required_status"),
        "latest_auc": readiness.get("latest_auc"),
        "latest_ap": readiness.get("latest_ap"),
        "promotion_ready": bool(readiness.get("promotion_ready", False)),
        "stage_thresholds": governance.get("stage_thresholds") or {},
    }


def record_governance_run(
    path: str | Path,
    *,
    governance: dict[str, Any],
    source_label: str = "weekly_evaluation",
) -> Path:
    """Append one immutable governance decision record to JSONL."""
    row = build_governance_run_record(governance=governance, source_label=source_label)
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(row, ensure_ascii=False) + "\n")
    _mirror_ledger(target)
    return target


def latest_governance_run(path: str | Path) -> dict[str, Any] | None:
    """Return the latest governance decision record if available."""
    rows = _read_jsonl(Path(path))
    if not rows:
        return None
    return rows[-1]


def build_stage_performance_record(
    *,
    stage: str,
    symbol: str,
    start: str,
    end: str,
    metrics: dict[str, Any],
    source_label: str = "walk_forward_backtest",
) -> dict[str, Any]:
    """Build one stage-performance record from walk-forward metrics."""
    strategy_total_return = metrics.get("strategy_total_return")
    buy_hold_total_return = metrics.get("buy_hold_total_return")
    excess_return = None
    if strategy_total_return is not None and buy_hold_total_return is not None:
        excess_return = float(strategy_total_return) - float(buy_hold_total_return)

    strategy_max_drawdown = metrics.get("strategy_max_drawdown")
    buy_hold_max_drawdown = metrics.get("buy_hold_max_drawdown")
    drawdown_gap = None
    if strategy_max_drawdown is not None and buy_hold_max_drawdown is not None:
        drawdown_gap = float(strategy_max_drawdown) - float(buy_hold_max_drawdown)

    return {
        "run_timestamp": datetime.now().isoformat(),
        "source_label": source_label,
        "stage": str(stage),
        "symbol": symbol,
        "start": start,
        "end": end,
        "sample_count": int(metrics.get("sample_count") or 0),
        "auc": None if metrics.get("auc") is None else float(metrics["auc"]),
        "ap": None if metrics.get("ap") is None else float(metrics["ap"]),
        "strategy_total_return": None
        if strategy_total_return is None
        else float(strategy_total_return),
        "buy_hold_total_return": None
        if buy_hold_total_return is None
        else float(buy_hold_total_return),
        "excess_return": excess_return,
        "strategy_max_drawdown": None
        if strategy_max_drawdown is None
        else float(strategy_max_drawdown),
        "buy_hold_max_drawdown": None
        if buy_hold_max_drawdown is None
        else float(buy_hold_max_drawdown),
        "drawdown_gap": drawdown_gap,
        "strategy_sharpe": None
        if metrics.get("strategy_sharpe") is None
        else float(metrics["strategy_sharpe"]),
        "buy_hold_sharpe": None
        if metrics.get("buy_hold_sharpe") is None
        else float(metrics["buy_hold_sharpe"]),
        "avg_position": None if metrics.get("avg_position") is None else float(metrics["avg_position"]),
        "risk_off_threshold": None
        if metrics.get("risk_off_threshold") is None
        else float(metrics["risk_off_threshold"]),
        "caution_threshold": None
        if metrics.get("caution_threshold") is None
        else float(metrics["caution_threshold"]),
        "risk_off_days": int(metrics.get("risk_off_days") or 0),
        "caution_days": int(metrics.get("caution_days") or 0),
        "full_risk_days": int(metrics.get("full_risk_days") or 0),
    }


def record_stage_performance_run(
    path: str | Path,
    *,
    stage: str,
    symbol: str,
    start: str,
    end: str,
    metrics: dict[str, Any],
    source_label: str = "walk_forward_backtest",
) -> Path:
    """Append one immutable stage-performance record to JSONL."""
    row = build_stage_performance_record(
        stage=stage,
        symbol=symbol,
        start=start,
        end=end,
        metrics=metrics,
        source_label=source_label,
    )
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(row, ensure_ascii=False) + "\n")
    _mirror_ledger(target)
    return target


def latest_stage_performance_run(
    path: str | Path,
    *,
    stage: str | None = None,
) -> dict[str, Any] | None:
    """Return the latest stage-performance record, optionally filtered by stage."""
    rows = _read_jsonl(Path(path))
    if stage is not None:
        rows = [row for row in rows if str(row.get("stage") or "") == str(stage)]
    if not rows:
        return None
    return rows[-1]


def summarize_stage_performance_window(
    path: str | Path,
    *,
    stage: str,
    recent_n: int = 3,
) -> dict[str, Any]:
    """Summarize recent stage-performance runs for guardrail checks."""
    rows = [
        row
        for row in _read_jsonl(Path(path))
        if str(row.get("stage") or "") == str(stage)
    ]
    if not rows:
        return {
            "stage": stage,
            "run_count": 0,
            "recent_n": recent_n,
            "mean_auc": None,
            "mean_ap": None,
            "mean_strategy_sharpe": None,
            "mean_excess_return": None,
            "mean_drawdown_gap": None,
            "latest_strategy_sharpe": None,
            "latest_excess_return": None,
        }

    recent = rows[-recent_n:]

    def _mean(key: str) -> float | None:
        values = [float(item[key]) for item in recent if item.get(key) is not None]
        return float(sum(values) / len(values)) if values else None

    latest = recent[-1]
    return {
        "stage": stage,
        "run_count": len(rows),
        "recent_n": len(recent),
        "mean_auc": _mean("auc"),
        "mean_ap": _mean("ap"),
        "mean_strategy_sharpe": _mean("strategy_sharpe"),
        "mean_excess_return": _mean("excess_return"),
        "mean_drawdown_gap": _mean("drawdown_gap"),
        "latest_strategy_sharpe": None
        if latest.get("strategy_sharpe") is None
        else float(latest["strategy_sharpe"]),
        "latest_excess_return": None
        if latest.get("excess_return") is None
        else float(latest["excess_return"]),
    }


def evaluate_stage_guardrail(
    path: str | Path,
    *,
    stage: str,
    recent_n: int = 3,
    min_runs: int = 2,
    min_strategy_sharpe: float = 0.20,
    min_excess_return: float = -0.05,
    max_drawdown_gap: float = 0.05,
) -> dict[str, Any]:
    """Evaluate whether a stage should be maintained or downgraded."""
    summary = summarize_stage_performance_window(path, stage=stage, recent_n=recent_n)
    reasons: list[str] = []
    healthy = True

    if int(summary["run_count"] or 0) < min_runs:
        healthy = False
        reasons.append("阶段表现记录数量不足")
    if summary["mean_strategy_sharpe"] is None or float(summary["mean_strategy_sharpe"]) < min_strategy_sharpe:
        healthy = False
        reasons.append("近期平均 strategy sharpe 未达门槛")
    if summary["mean_excess_return"] is None or float(summary["mean_excess_return"]) < min_excess_return:
        healthy = False
        reasons.append("近期平均超额收益未达门槛")
    if summary["mean_drawdown_gap"] is None or float(summary["mean_drawdown_gap"]) > max_drawdown_gap:
        healthy = False
        reasons.append("近期回撤优势未达门槛")

    stage = str(stage)
    if healthy:
        action = f"keep_{stage}"
        reasons.append(f"{stage} 阶段近期真实表现达标")
    elif stage == "production":
        action = "degrade_to_shadow"
    elif stage == "shadow":
        action = "keep_shadow_under_review"
    else:
        action = f"keep_{stage}_under_review"

    return {
        **summary,
        "stage": stage,
        "healthy": healthy,
        "action": action,
        "reasons": reasons,
        "thresholds": {
            "recent_n": recent_n,
            "min_runs": min_runs,
            "min_strategy_sharpe": min_strategy_sharpe,
            "min_excess_return": min_excess_return,
            "max_drawdown_gap": max_drawdown_gap,
        },
    }


def evaluate_release_lifecycle(
    snapshot_path: str | Path,
    training_path: str | Path,
    stage_performance_path: str | Path,
    *,
    current_stage: str = "candidate",
    required_status: str = "matured_10d",
    min_matured_samples_for_candidate: int = 2,
    min_matured_samples_for_shadow: int = 5,
    min_matured_samples_for_production: int = 12,
    recent_n: int = 3,
    min_runs: int = 2,
    auc_floor: float = 0.60,
    ap_floor: float = 0.18,
    shadow_auc_floor: float = 0.62,
    shadow_ap_floor: float = 0.20,
    production_auc_floor: float = 0.64,
    production_ap_floor: float = 0.22,
    production_min_runs: int = 3,
    guardrail_min_strategy_sharpe: float = 0.20,
    guardrail_min_excess_return: float = -0.05,
    guardrail_max_drawdown_gap: float = 0.05,
) -> dict[str, Any]:
    """Combine promotion readiness with stage guardrails into one lifecycle action."""
    governance = evaluate_governance_stage_flow(
        snapshot_path,
        training_path,
        current_stage=current_stage,
        required_status=required_status,
        min_matured_samples_for_candidate=min_matured_samples_for_candidate,
        min_matured_samples_for_shadow=min_matured_samples_for_shadow,
        min_matured_samples_for_production=min_matured_samples_for_production,
        recent_n=recent_n,
        min_runs=min_runs,
        auc_floor=auc_floor,
        ap_floor=ap_floor,
        shadow_auc_floor=shadow_auc_floor,
        shadow_ap_floor=shadow_ap_floor,
        production_auc_floor=production_auc_floor,
        production_ap_floor=production_ap_floor,
        production_min_runs=production_min_runs,
    )

    stage_for_guardrail = str(current_stage or "candidate").lower()
    if stage_for_guardrail in {"research", "candidate"}:
        stage_for_guardrail = "candidate"

    guardrail = evaluate_stage_guardrail(
        stage_performance_path,
        stage=stage_for_guardrail,
        recent_n=recent_n,
        min_runs=1 if stage_for_guardrail == "candidate" else min_runs,
        min_strategy_sharpe=guardrail_min_strategy_sharpe,
        min_excess_return=guardrail_min_excess_return,
        max_drawdown_gap=guardrail_max_drawdown_gap,
    )

    lifecycle_action = str(governance["action"])
    lifecycle_target_stage = str(governance["target_stage"])
    reasons: list[str] = [str(governance["reason"])]

    if current_stage == "production" and guardrail["action"] == "degrade_to_shadow":
        lifecycle_action = "degrade_to_shadow"
        lifecycle_target_stage = "shadow"
        reasons.extend(guardrail["reasons"])
    elif current_stage == "shadow" and not guardrail["healthy"]:
        lifecycle_action = "keep_shadow_under_review"
        lifecycle_target_stage = "shadow"
        reasons.extend(guardrail["reasons"])
    elif current_stage in {"candidate", "research"} and not guardrail["healthy"]:
        if governance["action"].startswith("promote_to_"):
            lifecycle_action = "keep_candidate_under_review" if current_stage == "candidate" else "continue_observing"
            lifecycle_target_stage = "candidate" if current_stage == "candidate" else "research"
            reasons.append("阶段真实表现 guardrail 尚未满足，暂不执行升版。")
            reasons.extend(guardrail["reasons"])
        else:
            reasons.extend(guardrail["reasons"])
    elif guardrail["healthy"]:
        reasons.extend(guardrail["reasons"])

    return {
        "current_stage": current_stage,
        "lifecycle_action": lifecycle_action,
        "lifecycle_target_stage": lifecycle_target_stage,
        "reason": "；".join(dict.fromkeys(reasons)),
        "governance": governance,
        "guardrail": guardrail,
    }


def build_lifecycle_run_record(
    *,
    lifecycle: dict[str, Any],
    source_label: str = "weekly_evaluation",
) -> dict[str, Any]:
    """Build a durable lifecycle decision record for release governance."""
    governance = dict(lifecycle.get("governance") or {})
    guardrail = dict(lifecycle.get("guardrail") or {})
    return {
        "run_timestamp": datetime.now().isoformat(),
        "source_label": source_label,
        "current_stage": lifecycle.get("current_stage"),
        "lifecycle_action": lifecycle.get("lifecycle_action"),
        "lifecycle_target_stage": lifecycle.get("lifecycle_target_stage"),
        "reason": lifecycle.get("reason"),
        "governance_action": governance.get("action"),
        "governance_target_stage": governance.get("target_stage"),
        "guardrail_action": guardrail.get("action"),
        "guardrail_healthy": bool(guardrail.get("healthy", False)),
        "guardrail_stage": guardrail.get("stage"),
    }


def record_lifecycle_run(
    path: str | Path,
    *,
    lifecycle: dict[str, Any],
    source_label: str = "weekly_evaluation",
) -> Path:
    """Append one immutable lifecycle decision record to JSONL."""
    row = build_lifecycle_run_record(lifecycle=lifecycle, source_label=source_label)
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(row, ensure_ascii=False) + "\n")
    _mirror_ledger(target)
    return target


def latest_lifecycle_run(path: str | Path) -> dict[str, Any] | None:
    """Return the latest lifecycle decision record if available."""
    rows = _read_jsonl(Path(path))
    if not rows:
        return None
    return rows[-1]


def build_version_switch_proposal(
    *,
    lifecycle: dict[str, Any],
    current_profile: str = "next_production_candidate",
    defaults: dict[str, Any] | None = None,
    source_label: str = "weekly_evaluation",
) -> dict[str, Any]:
    """Build a structured version-switch proposal from one lifecycle decision."""
    lifecycle_action = str(lifecycle.get("lifecycle_action") or "unknown")
    target_stage = str(lifecycle.get("lifecycle_target_stage") or lifecycle.get("current_stage") or "candidate")
    should_switch = lifecycle_action in {
        "promote_to_candidate",
        "promote_to_shadow",
        "promote_to_production",
        "degrade_to_shadow",
    }
    proposal_status = "pending_review" if should_switch else "no_change"

    if lifecycle_action == "promote_to_candidate":
        proposed_profile = "next_production_candidate"
    elif lifecycle_action == "promote_to_shadow":
        proposed_profile = "next_production_shadow"
    elif lifecycle_action == "promote_to_production":
        proposed_profile = "next_production_production"
    elif lifecycle_action == "degrade_to_shadow":
        proposed_profile = "next_production_shadow"
    else:
        proposed_profile = current_profile

    return {
        "run_timestamp": datetime.now().isoformat(),
        "source_label": source_label,
        "proposal_status": proposal_status,
        "proposal_action": lifecycle_action,
        "current_stage": lifecycle.get("current_stage"),
        "target_stage": target_stage,
        "current_profile": current_profile,
        "proposed_profile": proposed_profile,
        "should_switch": should_switch,
        "reason": lifecycle.get("reason"),
        "recommended_defaults": dict(defaults or NEXT_PRODUCTION_MODEL_DEFAULTS),
    }


def record_version_switch_proposal(
    path: str | Path,
    *,
    lifecycle: dict[str, Any],
    current_profile: str = "next_production_candidate",
    defaults: dict[str, Any] | None = None,
    source_label: str = "weekly_evaluation",
) -> Path:
    """Append one immutable version-switch proposal to JSONL."""
    if "proposal_action" in lifecycle:
        row = dict(lifecycle)
        row.setdefault("source_label", source_label)
        row.setdefault("recommended_defaults", dict(defaults or NEXT_PRODUCTION_MODEL_DEFAULTS))
    else:
        row = build_version_switch_proposal(
            lifecycle=lifecycle,
            current_profile=current_profile,
            defaults=defaults,
            source_label=source_label,
        )
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(row, ensure_ascii=False) + "\n")
    _mirror_ledger(target)
    return target


def latest_version_switch_proposal(path: str | Path) -> dict[str, Any] | None:
    """Return the latest version-switch proposal if available."""
    rows = _read_jsonl(Path(path))
    if not rows:
        return None
    return rows[-1]


def build_version_switch_execution_plan(
    *,
    proposal: dict[str, Any],
) -> dict[str, Any]:
    """Expand one proposal into a human/action-oriented execution draft."""
    current_profile = str(proposal.get("current_profile") or "unknown")
    proposed_profile = str(proposal.get("proposed_profile") or current_profile)
    should_switch = bool(proposal.get("should_switch", False))
    proposal_action = str(proposal.get("proposal_action") or "unknown")

    affected_entries = [
        {
            "path": "src/models/metaphysical/model_defaults.py",
            "role": "共享默认参数",
        },
        {
            "path": "scripts/evaluate_metaphysical_promotion.py",
            "role": "周评估与切换草案生成",
        },
        {
            "path": "scripts/backtest_next_production_metaphysical_model.py",
            "role": "回测默认配置与阶段表现记录",
        },
        {
            "path": "scripts/generate_next_production_signal.py",
            "role": "轻量最新信号入口",
        },
        {
            "path": "main.py",
            "role": "主日报摘要展示",
        },
    ]

    review_checks = [
        "确认 lifecycle_action 与 switch proposal 一致",
        "确认 recommended_defaults 仍是当前打算沿用的参数",
        "确认 stage performance 与 governance ledger 没有互相冲突",
        "确认主日报与自动化输出口径同步",
    ]

    if should_switch:
        review_checks.insert(0, f"确认是否从 {current_profile} 切换到 {proposed_profile}")

    return {
        "generated_at": datetime.now().isoformat(),
        "proposal_action": proposal_action,
        "proposal_status": proposal.get("proposal_status"),
        "current_profile": current_profile,
        "proposed_profile": proposed_profile,
        "should_switch": should_switch,
        "recommended_defaults": proposal.get("recommended_defaults") or dict(NEXT_PRODUCTION_MODEL_DEFAULTS),
        "affected_entries": affected_entries,
        "review_checks": review_checks,
        "operator_note": "待人工确认后再执行配置切换，不自动落生产。",
    }


def build_version_switch_confirmation_draft(
    *,
    proposal: dict[str, Any],
    execution_plan: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build a structured confirmation draft for a pending version switch."""
    plan = execution_plan or build_version_switch_execution_plan(proposal=proposal)
    current_profile = str(proposal.get("current_profile") or "unknown")
    proposed_profile = str(proposal.get("proposed_profile") or current_profile)
    should_switch = bool(proposal.get("should_switch", False))
    confirmation_state = "awaiting_confirmation" if should_switch else "not_required"

    summary_lines = [
        f"当前 profile: {current_profile}",
        f"建议切换到: {proposed_profile}",
        f"proposal_action: {proposal.get('proposal_action')}",
        f"proposal_status: {proposal.get('proposal_status')}",
    ]
    if proposal.get("reason"):
        summary_lines.append(f"原因: {proposal.get('reason')}")

    return {
        "generated_at": datetime.now().isoformat(),
        "confirmation_state": confirmation_state,
        "current_profile": current_profile,
        "proposed_profile": proposed_profile,
        "proposal_action": proposal.get("proposal_action"),
        "proposal_status": proposal.get("proposal_status"),
        "summary": " | ".join(summary_lines),
        "recommended_defaults": plan.get("recommended_defaults") or dict(NEXT_PRODUCTION_MODEL_DEFAULTS),
        "affected_entries": plan.get("affected_entries") or [],
        "review_checks": plan.get("review_checks") or [],
        "operator_note": "确认后再执行切换，当前只生成草案。",
    }


def build_version_switch_change_request(
    *,
    proposal: dict[str, Any],
    execution_plan: dict[str, Any] | None = None,
    confirmation_draft: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build a change-request style template for a pending profile switch."""
    plan = execution_plan or build_version_switch_execution_plan(proposal=proposal)
    draft = confirmation_draft or build_version_switch_confirmation_draft(
        proposal=proposal,
        execution_plan=plan,
    )
    current_profile = str(proposal.get("current_profile") or "unknown")
    proposed_profile = str(proposal.get("proposed_profile") or current_profile)
    should_switch = bool(proposal.get("should_switch", False))
    request_state = "draft_ready" if should_switch else "standby"

    rollback_points = [
        "恢复 current_profile 标识",
        "恢复 recommended_defaults 到切换前快照",
        "重新生成主日报并确认展示口径回退",
        "重新运行周评估，确认 lifecycle 与 proposal 恢复一致",
    ]

    return {
        "generated_at": datetime.now().isoformat(),
        "request_state": request_state,
        "title": f"{current_profile} -> {proposed_profile} 版本切换变更单",
        "current_profile": current_profile,
        "proposed_profile": proposed_profile,
        "proposal_action": proposal.get("proposal_action"),
        "summary": draft.get("summary"),
        "affected_entries": plan.get("affected_entries") or [],
        "recommended_defaults": draft.get("recommended_defaults") or dict(NEXT_PRODUCTION_MODEL_DEFAULTS),
        "review_checks": draft.get("review_checks") or [],
        "rollback_points": rollback_points,
        "operator_note": "仅生成变更单模板，待人工确认后执行。",
    }


def build_weekly_governance_summary(
    *,
    snapshot_path: str | Path,
    training_path: str | Path,
    governance_path: str | Path,
    lifecycle_path: str | Path,
    stage_performance_path: str | Path,
    switch_proposal_path: str | Path,
    current_stage: str = "candidate",
    recent_n: int = 3,
) -> dict[str, Any]:
    """Build a one-page weekly governance summary from the persisted ledgers."""
    matured = select_matured_learning_samples(snapshot_path)
    training_summary = summarize_training_run_window(training_path, recent_n=recent_n)
    governance = latest_governance_run(governance_path)
    lifecycle = latest_lifecycle_run(lifecycle_path)
    proposal = latest_version_switch_proposal(switch_proposal_path)

    lifecycle_stage = str((lifecycle or {}).get("current_stage") or current_stage or "candidate")
    guardrail_stage = "candidate" if lifecycle_stage == "research" else lifecycle_stage
    stage_health = evaluate_stage_guardrail(
        stage_performance_path,
        stage=guardrail_stage,
        recent_n=recent_n,
        min_runs=1 if guardrail_stage == "candidate" else 2,
    )
    confirmation = (
        build_version_switch_confirmation_draft(proposal=proposal)
        if proposal is not None
        else None
    )
    change_request = (
        build_version_switch_change_request(
            proposal=proposal,
            confirmation_draft=confirmation,
        )
        if proposal is not None
        else None
    )

    return {
        "generated_at": datetime.now().isoformat(),
        "matured_sample_count": len(matured),
        "training_summary": training_summary,
        "latest_governance": governance,
        "latest_lifecycle": lifecycle,
        "stage_health": stage_health,
        "latest_switch_proposal": proposal,
        "switch_confirmation_draft": confirmation,
        "switch_change_request": change_request,
    }


def render_weekly_governance_summary(summary: dict[str, Any]) -> str:
    """Render a compact human-readable weekly governance summary."""
    training = dict(summary.get("training_summary") or {})
    governance = dict(summary.get("latest_governance") or {})
    lifecycle = dict(summary.get("latest_lifecycle") or {})
    stage_health = dict(summary.get("stage_health") or {})
    proposal = dict(summary.get("latest_switch_proposal") or {})
    confirmation = dict(summary.get("switch_confirmation_draft") or {})
    change_request = dict(summary.get("switch_change_request") or {})

    lines = [
        "## 玄学模型周治理摘要",
        "",
        f"- 成熟样本数: {summary.get('matured_sample_count', 0)}",
        f"- 训练窗口: runs={training.get('run_count')} | mean_auc={training.get('mean_auc')} | mean_ap={training.get('mean_ap')}",
        (
            f"- 治理动作: {governance.get('current_stage')} -> {governance.get('target_stage')} | "
            f"{governance.get('action')} | {governance.get('reason')}"
            if governance
            else "- 治理动作: 暂无记录"
        ),
        (
            f"- 生命周期: {lifecycle.get('current_stage')} -> {lifecycle.get('lifecycle_target_stage')} | "
            f"{lifecycle.get('lifecycle_action')} | {lifecycle.get('reason')}"
            if lifecycle
            else "- 生命周期: 暂无记录"
        ),
        (
            f"- 阶段健康: {stage_health.get('stage')} | {stage_health.get('action')} | "
            f"{'；'.join(stage_health.get('reasons') or [])}"
            if stage_health
            else "- 阶段健康: 暂无记录"
        ),
        (
            f"- 切换草案: {proposal.get('current_profile')} -> {proposal.get('proposed_profile')} | "
            f"{proposal.get('proposal_status')} | {proposal.get('proposal_action')}"
            if proposal
            else "- 切换草案: 暂无记录"
        ),
        (
            f"- 确认稿: {confirmation.get('confirmation_state')} | {confirmation.get('summary')}"
            if confirmation
            else "- 确认稿: 暂无记录"
        ),
        (
            f"- 变更单: {change_request.get('request_state')} | {change_request.get('title')}"
            if change_request
            else "- 变更单: 暂无记录"
        ),
    ]
    return "\n".join(lines)


def _mean_or_none(values: list[float]) -> float | None:
    return float(sum(values) / len(values)) if values else None


def _pct_hit_rate(values: list[bool]) -> float | None:
    return float(sum(1 for item in values if item) / len(values)) if values else None


def _summarize_learning_signal_rows(
    rows: list[dict[str, Any]],
    *,
    expected_direction: str | None = None,
) -> dict[str, Any]:
    horizon_stats: dict[str, Any] = {}
    for horizon in (1, 3, 5, 10):
        key = f"next_{horizon}d_return"
        values = [
            float(row.get("future_outcomes", {}).get(key))
            for row in rows
            if row.get("future_outcomes", {}).get(key) is not None
        ]
        hit_flags: list[bool] = []
        if expected_direction == "defensive":
            hit_flags = [value <= 0 for value in values]
        elif expected_direction == "risk_on":
            hit_flags = [value > 0 for value in values]
        horizon_stats[key] = {
            "available_count": len(values),
            "mean_return": _mean_or_none(values),
            "hit_rate": _pct_hit_rate(hit_flags) if hit_flags else None,
        }

    drawdowns = [
        float(row.get("future_outcomes", {}).get("max_drawdown_10d"))
        for row in rows
        if row.get("future_outcomes", {}).get("max_drawdown_10d") is not None
    ]
    return {
        "sample_count": len(rows),
        "mean_max_drawdown_10d": _mean_or_none(drawdowns),
        "horizons": horizon_stats,
    }


def build_metaphysical_accuracy_dashboard(
    snapshot_path: str | Path,
    *,
    top_k_tags: int = 5,
) -> dict[str, Any]:
    """Build a compact validation dashboard from persisted learning snapshots."""
    rows = _read_jsonl(Path(snapshot_path))
    maturity_counts = {
        "observing": 0,
        "matured_1d": 0,
        "matured_3d": 0,
        "matured_5d": 0,
        "matured_10d": 0,
    }
    tag_counts: dict[str, int] = {}
    for row in rows:
        status = str(row.get("maturity_status") or "observing")
        maturity_counts[status] = maturity_counts.get(status, 0) + 1
        for tag in row.get("candidate_feature_flags") or []:
            tag_counts[str(tag)] = tag_counts.get(str(tag), 0) + 1

    latest = rows[-1] if rows else None
    risk_off_rows = [row for row in rows if str(row.get("final_position_regime") or "") == "risk_off"]
    full_risk_rows = [row for row in rows if str(row.get("final_position_regime") or "") == "full_risk"]
    overlay_rows = [
        row
        for row in rows
        if bool(row.get("overlay_active")) and row.get("raw_position_regime") != row.get("final_position_regime")
    ]
    conflict_rows = [row for row in rows if str(row.get("report_alignment") or "") == "conflict"]

    top_tags = sorted(tag_counts.items(), key=lambda item: (-int(item[1]), str(item[0])))[:top_k_tags]

    return {
        "generated_at": datetime.now().isoformat(),
        "sample_count": len(rows),
        "maturity_counts": maturity_counts,
        "latest_sample": {
            "report_date": latest.get("report_date") if latest else None,
            "raw_position_regime": latest.get("raw_position_regime") if latest else None,
            "final_position_regime": latest.get("final_position_regime") if latest else None,
            "report_alignment": latest.get("report_alignment") if latest else None,
        },
        "risk_off_summary": _summarize_learning_signal_rows(
            risk_off_rows,
            expected_direction="defensive",
        ),
        "full_risk_summary": _summarize_learning_signal_rows(
            full_risk_rows,
            expected_direction="risk_on",
        ),
        "overlay_correction_summary": _summarize_learning_signal_rows(
            overlay_rows,
            expected_direction="defensive",
        ),
        "conflict_summary": _summarize_learning_signal_rows(
            conflict_rows,
            expected_direction="defensive",
        ),
        "top_risk_tags": [{"tag": tag, "count": count} for tag, count in top_tags],
    }


def render_metaphysical_accuracy_dashboard(summary: dict[str, Any]) -> str:
    """Render a compact human-readable accuracy dashboard."""

    def _fmt_pct(value: float | None) -> str:
        return "暂无" if value is None else f"{value * 100:.1f}%"

    def _fmt_ret(value: float | None) -> str:
        return "暂无" if value is None else f"{value * 100:.2f}%"

    def _render_signal_line(label: str, payload: dict[str, Any]) -> str:
        horizons = dict(payload.get("horizons") or {})
        next_5d = dict(horizons.get("next_5d_return") or {})
        next_10d = dict(horizons.get("next_10d_return") or {})
        return (
            f"- {label}: samples={payload.get('sample_count', 0)} | "
            f"5d命中率={_fmt_pct(next_5d.get('hit_rate'))} | "
            f"10d命中率={_fmt_pct(next_10d.get('hit_rate'))} | "
            f"5d均值={_fmt_ret(next_5d.get('mean_return'))} | "
            f"10d均值={_fmt_ret(next_10d.get('mean_return'))} | "
            f"平均10d回撤={_fmt_ret(payload.get('mean_max_drawdown_10d'))}"
        )

    maturity = dict(summary.get("maturity_counts") or {})
    latest = dict(summary.get("latest_sample") or {})
    top_tags = summary.get("top_risk_tags") or []

    lines = [
        "## 玄学模型命中率看板",
        "",
        f"- 样本总数: {summary.get('sample_count', 0)}",
        (
            f"- 成熟进度: 1d={maturity.get('matured_1d', 0)} | 3d={maturity.get('matured_3d', 0)} | "
            f"5d={maturity.get('matured_5d', 0)} | 10d={maturity.get('matured_10d', 0)}"
        ),
        (
            f"- 最新样本: {latest.get('report_date')} | "
            f"{latest.get('raw_position_regime')} -> {latest.get('final_position_regime')} | "
            f"{latest.get('report_alignment')}"
            if latest
            else "- 最新样本: 暂无记录"
        ),
    ]

    matured_total = sum(
        int(maturity.get(key, 0) or 0)
        for key in ("matured_1d", "matured_3d", "matured_5d", "matured_10d")
    )
    if matured_total == 0:
        lines.append("- 当前验证状态: 暂无成熟样本，先继续积累。")
    else:
        lines.extend(
            [
                _render_signal_line("防守建议（risk_off）", dict(summary.get("risk_off_summary") or {})),
                _render_signal_line("进攻建议（full_risk）", dict(summary.get("full_risk_summary") or {})),
                _render_signal_line("纠偏建议（raw!=final）", dict(summary.get("overlay_correction_summary") or {})),
                _render_signal_line("冲突样本", dict(summary.get("conflict_summary") or {})),
            ]
        )

    if top_tags:
        lines.append(
            "- 高频风险标签: " + "，".join(f"{item['tag']}({item['count']})" for item in top_tags)
        )

    return "\n".join(lines)


def _extract_iso_date(value: str | None) -> str | None:
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    if "T" in text:
        text = text.split("T", 1)[0]
    dt = _parse_report_date(text)
    return dt.date().isoformat() if dt is not None else None


def build_feishu_push_accuracy_dashboard(
    audit_path: str | Path,
    snapshot_path: str | Path,
    *,
    max_recent_rows: int = 5,
) -> dict[str, Any]:
    """Match successful Feishu metaphysical pushes to learning outcomes."""
    audit_rows = _read_jsonl(Path(audit_path))
    snapshot_rows = _read_jsonl(Path(snapshot_path))

    successful_pushes = [
        row for row in audit_rows
        if bool(row.get("success")) and str(row.get("channel") or "") == "feishu"
    ]
    metaphysical_pushes = [
        row for row in successful_pushes
        if str(row.get("push_kind") or "") == "metaphysical_daily"
    ]

    latest_push_by_day: dict[str, dict[str, Any]] = {}
    for row in metaphysical_pushes:
        push_date_iso = _extract_iso_date(row.get("sent_at"))
        if not push_date_iso:
            continue
        latest_push_by_day[push_date_iso] = row

    snapshot_by_day: dict[str, dict[str, Any]] = {}
    for row in snapshot_rows:
        report_date_iso = _extract_iso_date(row.get("report_date"))
        if report_date_iso:
            snapshot_by_day[report_date_iso] = row

    matched_rows: list[dict[str, Any]] = []
    matched_pairs: list[dict[str, Any]] = []
    unmatched_push_dates: list[str] = []
    for push_date in sorted(latest_push_by_day):
        push = latest_push_by_day[push_date]
        sample = snapshot_by_day.get(push_date)
        if sample is None:
            unmatched_push_dates.append(push_date)
            continue
        matched_rows.append(sample)
        matched_pairs.append(
            {
                "push_date": push_date,
                "final_position_regime": sample.get("final_position_regime"),
                "raw_position_regime": sample.get("raw_position_regime"),
                "report_alignment": sample.get("report_alignment"),
                "tail_risk_probability": sample.get("tail_risk_probability"),
                "next_1d_return": sample.get("future_outcomes", {}).get("next_1d_return"),
                "next_3d_return": sample.get("future_outcomes", {}).get("next_3d_return"),
                "next_5d_return": sample.get("future_outcomes", {}).get("next_5d_return"),
                "next_10d_return": sample.get("future_outcomes", {}).get("next_10d_return"),
                "max_drawdown_10d": sample.get("future_outcomes", {}).get("max_drawdown_10d"),
                "content_preview": push.get("content_preview"),
                "archive_path": push.get("archive_path"),
            }
        )

    latest_push = None
    if latest_push_by_day:
        latest_date = sorted(latest_push_by_day)[-1]
        latest_push = dict(latest_push_by_day[latest_date])
        latest_push["push_date"] = latest_date
        latest_push["matched_learning_sample"] = bool(snapshot_by_day.get(latest_date))

    return {
        "generated_at": datetime.now().isoformat(),
        "successful_feishu_push_count": len(successful_pushes),
        "metaphysical_feishu_push_day_count": len(latest_push_by_day),
        "matched_learning_sample_count": len(matched_rows),
        "unmatched_push_dates": unmatched_push_dates,
        "latest_push": latest_push,
        "matched_push_summary": _summarize_learning_signal_rows(matched_rows),
        "matched_push_rows": matched_pairs[-max_recent_rows:],
    }


def render_feishu_push_accuracy_dashboard(summary: dict[str, Any]) -> str:
    """Render a compact dashboard linking real Feishu pushes to later outcomes."""

    def _fmt_ret(value: float | None) -> str:
        return "暂无" if value is None else f"{float(value) * 100:.2f}%"

    latest_push = dict(summary.get("latest_push") or {})
    matched_summary = dict(summary.get("matched_push_summary") or {})
    horizons = dict(matched_summary.get("horizons") or {})
    next_5d = dict(horizons.get("next_5d_return") or {})
    next_10d = dict(horizons.get("next_10d_return") or {})
    recent_rows = summary.get("matched_push_rows") or []

    lines = [
        "## 飞书推送建议验证看板",
        "",
        f"- 飞书成功推送总数: {summary.get('successful_feishu_push_count', 0)}",
        f"- 玄学日报推送天数: {summary.get('metaphysical_feishu_push_day_count', 0)}",
        f"- 已匹配验证样本: {summary.get('matched_learning_sample_count', 0)}",
    ]

    unmatched = summary.get("unmatched_push_dates") or []
    if unmatched:
        lines.append(f"- 还没匹配到验证样本的推送日期: {', '.join(unmatched[:8])}")

    if latest_push:
        lines.append(
            f"- 最新飞书推送: {latest_push.get('push_date')} | "
            f"{'已进入验证' if latest_push.get('matched_learning_sample') else '待进入验证'} | "
            f"{latest_push.get('content_preview') or '无摘要'}"
        )
    else:
        lines.append("- 最新飞书推送: 暂无记录")

    if int(summary.get("matched_learning_sample_count", 0) or 0) == 0:
        lines.append("- 当前验证状态: 还没有把飞书推送和成熟样本对应起来，先继续积累。")
        return "\n".join(lines)

    lines.extend(
        [
            (
                f"- 汇总表现: samples={matched_summary.get('sample_count', 0)} | "
                f"5d均值={_fmt_ret(next_5d.get('mean_return'))} | "
                f"10d均值={_fmt_ret(next_10d.get('mean_return'))} | "
                f"平均10d回撤={_fmt_ret(matched_summary.get('mean_max_drawdown_10d'))}"
            )
        ]
    )

    if recent_rows:
        lines.append("- 最近已验证推送:")
        for row in recent_rows:
            lines.append(
                "  "
                + f"{row.get('push_date')} | {row.get('raw_position_regime')} -> {row.get('final_position_regime')} | "
                + f"1d={_fmt_ret(row.get('next_1d_return'))} | 3d={_fmt_ret(row.get('next_3d_return'))} | "
                + f"5d={_fmt_ret(row.get('next_5d_return'))} | 10d={_fmt_ret(row.get('next_10d_return'))}"
            )

    return "\n".join(lines)


def build_daily_governance_summary(
    *,
    cache_dir: str | Path,
    symbol: str,
    start: str,
    end: str,
    governance_path: str | Path,
    lifecycle_path: str | Path,
    stage_performance_path: str | Path,
    switch_proposal_path: str | Path,
    report_text: str | None = None,
    report_sync_status: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build a compact daily governance summary from live signal plus ledgers."""
    from .model_backtest import latest_cached_next_production_signal

    signal = latest_cached_next_production_signal(
        cache_dir=cache_dir,
        symbol=symbol,
        start=start,
        end=end,
        report_text=report_text,
        expected_report_date=datetime.now().date().isoformat(),
    )
    governance = latest_governance_run(governance_path)
    lifecycle = latest_lifecycle_run(lifecycle_path)
    proposal = latest_version_switch_proposal(switch_proposal_path)

    lifecycle_stage = str((lifecycle or {}).get("current_stage") or "candidate")
    guardrail_stage = "candidate" if lifecycle_stage == "research" else lifecycle_stage
    stage_health = evaluate_stage_guardrail(
        stage_performance_path,
        stage=guardrail_stage,
        recent_n=3,
        min_runs=1 if guardrail_stage == "candidate" else 2,
    )
    confirmation = (
        build_version_switch_confirmation_draft(proposal=proposal)
        if proposal is not None
        else None
    )
    change_request = (
        build_version_switch_change_request(
            proposal=proposal,
            confirmation_draft=confirmation,
        )
        if proposal is not None
        else None
    )

    return {
        "generated_at": datetime.now().isoformat(),
        "latest_signal": signal,
        "latest_governance": governance,
        "latest_lifecycle": lifecycle,
        "stage_health": stage_health,
        "latest_switch_proposal": proposal,
        "switch_confirmation_draft": confirmation,
        "switch_change_request": change_request,
        "latest_report_sync": report_sync_status or {},
    }


def render_daily_governance_summary(summary: dict[str, Any]) -> str:
    """Render a standalone daily metaphysical governance report."""
    signal = dict(summary.get("latest_signal") or {})
    report_sync = dict(summary.get("latest_report_sync") or {})
    governance = dict(summary.get("latest_governance") or {})
    lifecycle = dict(summary.get("latest_lifecycle") or {})
    stage_health = dict(summary.get("stage_health") or {})
    proposal = dict(summary.get("latest_switch_proposal") or {})
    confirmation = dict(summary.get("switch_confirmation_draft") or {})
    change_request = dict(summary.get("switch_change_request") or {})

    def _humanize_internal_terms(text: str) -> str:
        replacements = {
            "candidate": "候选版本",
            "shadow": "影子观察版本",
            "production": "正式版本",
            "profile": "版本",
        }
        result = text
        for old, new in replacements.items():
            result = result.replace(old, new)
        return result

    probability = signal.get("tail_risk_probability")
    prob_text = f"{float(probability):.4f}" if isinstance(probability, (float, int)) else "unknown"

    regime_map = {
        "full_risk": "正常持有/可继续进攻",
        "caution": "谨慎，控制节奏",
        "risk_off": "偏防守，减少风险暴露",
    }
    action_map = {
        "hold_or_add": "继续持有，必要时再加一点",
        "hold": "继续持有",
        "trim": "适当减仓",
        "reduce": "降低仓位",
        "exit_or_hedge": "离场或做保护",
        "risk_off": "先降到防守仓位",
    }
    governance_action_map = {
        "continue_observing": "继续观察",
        "keep_candidate": "维持候选版本，先不升级",
        "keep_candidate_under_review": "先保留，但继续盯着",
        "promote_to_shadow": "可以进入影子观察",
        "promote_to_production": "可以考虑转正式版本",
        "degrade_to_shadow": "建议从正式版降回影子观察",
    }
    proposal_status_map = {
        "no_change": "当前不需要切换版本",
        "pending_review": "出现切换建议，等你确认",
        "awaiting_confirmation": "已进入待确认阶段",
    }
    confirmation_state_map = {
        "not_required": "今天不需要你确认",
        "pending_review": "需要你人工确认",
        "awaiting_confirmation": "等你拍板",
    }
    change_request_state_map = {
        "standby": "变更单暂时不用执行",
        "draft_ready": "变更单草案已备好",
        "ready_to_apply": "可以按变更单执行",
    }

    final_regime = signal.get("position_regime")
    raw_regime = signal.get("raw_position_regime", final_regime)
    regime_changed = bool(signal) and raw_regime != final_regime
    action_hint = action_map.get(signal.get("action"), signal.get("action"))
    lifecycle_action = lifecycle.get("lifecycle_action")
    proposal_status = proposal.get("proposal_status")
    confirmation_state = confirmation.get("confirmation_state")
    change_request_state = change_request.get("request_state")
    governance_reason = _humanize_internal_terms(str(governance.get("reason") or "").strip())
    lifecycle_reason = _humanize_internal_terms(str(lifecycle.get("reason") or "").strip())
    stage_reasons = [
        _humanize_internal_terms(str(item).strip())
        for item in (stage_health.get("reasons") or [])
        if str(item).strip()
    ]
    sync_freshness = dict(report_sync.get("freshness") or {})
    sync_selection = dict(report_sync.get("selection") or {})

    reason_parts: list[str] = []
    sync_reason = str(sync_freshness.get("freshness_reason") or "").strip()
    if sync_reason:
        reason_parts.append(sync_reason)
    selection_reason = str(sync_selection.get("selection_reason") or "").strip()
    if selection_reason and int(sync_selection.get("same_day_candidate_count") or 0) > 1:
        reason_parts.append(selection_reason)
    elif signal.get("report_overlay_skipped") and not sync_reason:
        reason_parts.append(str(signal.get("report_overlay_skip_reason") or "").strip())
    elif regime_changed:
        reason_parts.append(
            f"外部日报把判断从“{regime_map.get(raw_regime, raw_regime)}”调整成“{regime_map.get(final_regime, final_regime)}”"
        )
    if governance_reason:
        reason_parts.append(governance_reason)
    elif lifecycle_reason:
        reason_parts.append(lifecycle_reason)
    elif stage_reasons:
        reason_parts.append("；".join(stage_reasons))
    short_reason = "；".join(reason_parts[:2]) if reason_parts else "当前没有额外异常，继续按既定节奏观察。"

    lines = [
        "## 玄学治理日报",
        "",
        (
            f"- 结论: {regime_map.get(final_regime, final_regime)}"
            if signal
            else "- 结论: 暂无记录"
        ),
        (
            f"- 建议动作: {action_hint or '先观察'}"
            if signal
            else "- 建议动作: 暂无记录"
        ),
        (
            f"- 是否需要你介入: {confirmation_state_map.get(confirmation_state, confirmation_state)}"
            if confirmation
            else "- 是否需要你介入: 暂无记录"
        ),
        (
            f"- 主要原因: {short_reason}"
            if summary
            else "- 主要原因: 暂无记录"
        ),
    ]
    return "\n".join(lines)
