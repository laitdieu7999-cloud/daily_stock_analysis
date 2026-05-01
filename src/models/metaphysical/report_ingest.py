"""Parsers for daily tactical reports used in metaphysical model research."""

from __future__ import annotations

import hashlib
import re
from collections import Counter
from datetime import date
from difflib import SequenceMatcher
from pathlib import Path


def _normalize_report_text(text: str) -> str:
    return "\n".join(line.strip() for line in (text or "").splitlines() if line.strip())


def _report_date_text_to_iso(value: str | None) -> str | None:
    if not value:
        return None
    match = re.search(r"([0-9]{4})年([0-9]{1,2})月([0-9]{1,2})日", value)
    if match:
        year, month, day = match.groups()
        return f"{int(year):04d}-{int(month):02d}-{int(day):02d}"
    iso_match = re.search(r"([0-9]{4})-([0-9]{1,2})-([0-9]{1,2})", value)
    if not iso_match:
        return None
    year, month, day = iso_match.groups()
    return f"{int(year):04d}-{int(month):02d}-{int(day):02d}"


def extract_tactical_report_date_iso(text: str) -> str | None:
    """Extract one report date from free-form tactical text."""
    normalized = text or ""
    patterns = (
        r"日期[:：]\s*([0-9]{4}年[0-9]{1,2}月[0-9]{1,2}日)",
        r"报告日期[:：]\s*([0-9]{4}年[0-9]{1,2}月[0-9]{1,2}日)",
        r"([0-9]{4})年([0-9]{1,2})月([0-9]{1,2})日",
        r"([0-9]{4})-([0-9]{1,2})-([0-9]{1,2})",
    )
    for pattern in patterns:
        match = re.search(pattern, normalized)
        if not match:
            continue
        groups = match.groups()
        if len(groups) == 1:
            parsed = _report_date_text_to_iso(groups[0])
            if parsed:
                return parsed
        elif len(groups) == 3:
            year, month, day_value = groups
            return f"{int(year):04d}-{int(month):02d}-{int(day_value):02d}"
    return None


def assess_tactical_report_freshness(
    text: str,
    *,
    expected_date_iso: str | None = None,
) -> dict[str, object]:
    """Determine whether a tactical report is fresh enough to drive signal overlay."""
    report_date_iso = extract_tactical_report_date_iso(text)
    expected_iso = expected_date_iso or date.today().isoformat()
    if not report_date_iso:
        return {
            "report_date_iso": None,
            "expected_report_date_iso": expected_iso,
            "is_fresh": False,
            "is_stale": True,
            "freshness_reason": "外部归档缺少可识别日期，今天不启用文本纠偏。",
        }
    is_fresh = report_date_iso == expected_iso
    return {
        "report_date_iso": report_date_iso,
        "expected_report_date_iso": expected_iso,
        "is_fresh": is_fresh,
        "is_stale": not is_fresh,
        "freshness_reason": (
            "外部归档日期与今天一致，可启用文本纠偏。"
            if is_fresh
            else f"外部归档日期停留在 {report_date_iso}，今天不启用文本纠偏。"
        ),
    }


def tactical_report_text_hash(text: str) -> str:
    """Build a stable content hash for one tactical report."""
    normalized = _normalize_report_text(text)
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def detect_tactical_report_duplicates(text: str) -> dict[str, object]:
    """Detect obvious duplicate content in one tactical report text."""
    non_empty_lines = [line.strip() for line in (text or "").splitlines() if line.strip()]
    counts = Counter(non_empty_lines)
    duplicated_lines = [
        {"line": line, "count": count}
        for line, count in counts.items()
        if count > 1
    ]
    duplicated_lines.sort(key=lambda item: (-int(item["count"]), str(item["line"])))
    return {
        "has_duplicates": bool(duplicated_lines),
        "non_empty_line_count": len(non_empty_lines),
        "duplicate_line_count": len(duplicated_lines),
        "duplicated_lines": duplicated_lines,
    }


def detect_tactical_report_archive_duplicates(
    text: str,
    *,
    archive_dir: str | Path | None = None,
    report_date_text: str | None = None,
    similarity_threshold: float = 0.9,
) -> dict[str, object]:
    """Detect same-day duplicates and high-similarity re-archives against history."""
    archive_path = Path(archive_dir) if archive_dir else None
    if archive_path is None or not archive_path.exists():
        return {
            "same_day_archive_count": 0,
            "same_day_archive_files": [],
            "high_similarity_detected": False,
            "highest_similarity": 0.0,
            "highest_similarity_file": None,
            "similarity_threshold": float(similarity_threshold),
        }

    normalized = _normalize_report_text(text)
    report_date_iso = _report_date_text_to_iso(report_date_text)
    same_day_files: list[str] = []
    highest_similarity = 0.0
    highest_similarity_file: str | None = None

    for candidate in sorted(archive_path.glob("*.md")):
        candidate_text = _normalize_report_text(candidate.read_text(encoding="utf-8"))
        if report_date_iso and candidate.name.startswith(f"{report_date_iso}_"):
            same_day_files.append(candidate.name)
        if candidate_text:
            ratio = SequenceMatcher(None, normalized, candidate_text).ratio()
            if ratio > highest_similarity:
                highest_similarity = ratio
                highest_similarity_file = candidate.name

    return {
        "same_day_archive_count": len(same_day_files),
        "same_day_archive_files": same_day_files,
        "high_similarity_detected": highest_similarity >= float(similarity_threshold),
        "highest_similarity": float(round(highest_similarity, 4)),
        "highest_similarity_file": highest_similarity_file,
        "similarity_threshold": float(similarity_threshold),
    }


def archive_tactical_report_text(
    text: str,
    *,
    report_date_text: str | None,
    archive_dir: str | Path,
    filename_label: str = "gemini_daily",
) -> Path:
    """Persist one normalized tactical report snapshot for later similarity checks."""
    archive_path = Path(archive_dir)
    archive_path.mkdir(parents=True, exist_ok=True)
    report_date_iso = _report_date_text_to_iso(report_date_text) or "unknown-date"
    target = archive_path / f"{report_date_iso}_{filename_label}.md"
    target.write_text(_normalize_report_text(text), encoding="utf-8")
    return target


def sync_tactical_report_cache(
    *,
    source_path: str | Path,
    target_path: str | Path,
    archive_dir: str | Path | None = None,
    filename_label: str = "gemini_daily",
    expected_date_iso: str | None = None,
) -> dict[str, object]:
    """Copy a Google Drive synced file into the local cache and archive it."""
    source = Path(source_path)
    target = Path(target_path)
    if not source.exists():
        return {
            "status": "missing_source",
            "source_path": str(source),
            "target_path": str(target),
            "synced": False,
            "freshness": {
                "report_date_iso": None,
                "expected_report_date_iso": expected_date_iso or date.today().isoformat(),
                "is_fresh": False,
                "is_stale": True,
                "freshness_reason": "Google Drive 同步文件不存在，今天不启用文本纠偏。",
            },
        }
    text = source.read_text(encoding="utf-8").strip()
    normalized = _normalize_report_text(text)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(normalized, encoding="utf-8")
    freshness = assess_tactical_report_freshness(normalized, expected_date_iso=expected_date_iso)
    archive_path = None
    if archive_dir:
        archive_path = archive_tactical_report_text(
            normalized,
            report_date_text=freshness.get("report_date_iso"),
            archive_dir=archive_dir,
            filename_label=filename_label,
        )
    return {
        "status": "synced",
        "source_path": str(source),
        "target_path": str(target),
        "archive_path": str(archive_path) if archive_path else None,
        "synced": True,
        "freshness": freshness,
    }


def sync_tactical_report_text(
    *,
    text: str,
    source_label: str,
    target_path: str | Path,
    archive_dir: str | Path | None = None,
    filename_label: str = "gemini_daily",
    expected_date_iso: str | None = None,
) -> dict[str, object]:
    """Write one tactical report text snapshot into cache and archive it."""
    target = Path(target_path)
    normalized = _normalize_report_text(text)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(normalized, encoding="utf-8")
    freshness = assess_tactical_report_freshness(normalized, expected_date_iso=expected_date_iso)
    archive_path = None
    if archive_dir:
        archive_path = archive_tactical_report_text(
            normalized,
            report_date_text=freshness.get("report_date_iso"),
            archive_dir=archive_dir,
            filename_label=filename_label,
        )
    return {
        "status": "synced",
        "source_path": str(source_label),
        "target_path": str(target),
        "archive_path": str(archive_path) if archive_path else None,
        "synced": True,
        "freshness": freshness,
    }


def parse_tactical_report_text(text: str) -> dict[str, object]:
    """Extract a compact structured summary from a daily tactical report."""
    normalized = text or ""

    report_date_match = re.search(r"报告日期[:：]\s*([0-9]{4}年[0-9]{1,2}月[0-9]{1,2}日)", normalized)
    core_stance_match = re.search(r"核心态势[:：]\s*(.+)", normalized)
    csi500_spot_match = re.search(r"现货点位[:：]\s*([0-9.]+)\s*\(([-+0-9.]+%)\)", normalized)
    vwap_match = re.search(r"受压于\s*([0-9.]+)\s*附近", normalized)
    gann_anchor_match = re.search(r"核心锚点\s*([0-9.]+)", normalized)

    keyword_flags = {
        "liquidity_risk_resonance": int("流动性风险共振" in normalized),
        "black_swan_warning": int("黑天鹅预警" in normalized),
        "physical_blockade": int("物理封锁" in normalized),
        "usd_cash_flight": int("美元现金归笼" in normalized),
        "deleveraging_phase": int("去杠杆" in normalized),
        "institutional_outflow": int("净流出" in normalized),
        "gamma_hedge_active": int("Gamma" in normalized or "gamma" in normalized),
        "deep_discount_wait": int("宁可踏空" in normalized and "绝不套牢" in normalized),
        "high_growth_pause_add": int("暂停加仓" in normalized),
        "gold_defense_core": int("黄金 ETF" in normalized or "黄金ETF" in normalized),
    }

    return {
        "report_date_text": report_date_match.group(1) if report_date_match else None,
        "core_stance": core_stance_match.group(1).strip() if core_stance_match else None,
        "csi500_spot": float(csi500_spot_match.group(1)) if csi500_spot_match else None,
        "csi500_spot_change_pct": csi500_spot_match.group(2) if csi500_spot_match else None,
        "vwap_pressure_level": float(vwap_match.group(1)) if vwap_match else None,
        "gann_anchor_level": float(gann_anchor_match.group(1)) if gann_anchor_match else None,
        **keyword_flags,
    }


def compare_tactical_report_to_model_signal(
    report_summary: dict[str, object],
    model_signal: dict[str, object],
) -> dict[str, object]:
    """Compare parsed daily report stance with the latest model signal."""
    risk_score = sum(
        int(report_summary.get(key, 0) or 0)
        for key in (
            "liquidity_risk_resonance",
            "black_swan_warning",
            "physical_blockade",
            "usd_cash_flight",
            "deleveraging_phase",
            "institutional_outflow",
        )
    )
    regime = str(model_signal.get("position_regime") or "")

    if risk_score >= 3 and regime == "full_risk":
        alignment = "conflict"
        implication = "日报风险叙事明显偏空，但模型仍维持满风险，优先检查风险阈值与宏观特征缺口。"
    elif risk_score >= 3 and regime in {"caution", "risk_off"}:
        alignment = "aligned_defensive"
        implication = "日报与模型都偏防守，可继续观察是否需要把日报文本信号转成结构化特征。"
    elif risk_score <= 1 and regime == "full_risk":
        alignment = "aligned_risk_on"
        implication = "日报与模型都偏进攻，当前无需额外人工纠偏。"
    else:
        alignment = "mixed"
        implication = "日报与模型部分一致，建议继续跟踪并记录后续验证结果。"

    return {
        "alignment": alignment,
        "report_risk_score": int(risk_score),
        "model_position_regime": regime,
        "implication": implication,
    }


def build_tactical_report_optimization_notes(
    text: str,
    *,
    model_signal: dict[str, object] | None = None,
    archive_dir: str | Path | None = None,
) -> dict[str, object]:
    """Turn daily tactical text into optimization-ready structured notes."""
    summary = parse_tactical_report_text(text)
    candidate_features = [
        key
        for key in (
            "liquidity_risk_resonance",
            "black_swan_warning",
            "physical_blockade",
            "usd_cash_flight",
            "deleveraging_phase",
            "institutional_outflow",
            "gamma_hedge_active",
            "deep_discount_wait",
            "high_growth_pause_add",
            "gold_defense_core",
        )
        if int(summary.get(key, 0) or 0) == 1
    ]
    result = {
        "report_summary": summary,
        "candidate_feature_flags": candidate_features,
        "optimization_priority": "high" if len(candidate_features) >= 4 else "medium",
        "duplicate_check": {
            **detect_tactical_report_duplicates(text),
            **detect_tactical_report_archive_duplicates(
                text,
                archive_dir=archive_dir,
                report_date_text=summary.get("report_date_text"),
            ),
            "report_text_hash": tactical_report_text_hash(text),
        },
    }
    if model_signal is not None:
        result["signal_comparison"] = compare_tactical_report_to_model_signal(summary, model_signal)
    return result
