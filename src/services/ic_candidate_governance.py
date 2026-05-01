from __future__ import annotations

import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any

_BASELINE_RE = re.compile(
    r"- 原始高贴水满仓/普通半仓: 收益 ([0-9.+-]+)% \| 最大回撤 ([0-9.+-]+)% \| Sharpe ([0-9.+-]+)"
)
_DISTRIBUTION_RE = re.compile(r"- 触发时基础仓位分布: 满仓 (\d+) 天 / 半仓 (\d+) 天")


def _parse_pct(value: str) -> float:
    return float(str(value).replace("%", "").strip())


def _parse_table_row(text: str, label: str) -> list[str]:
    needle = f"| {label} |"
    for line in text.splitlines():
        if line.startswith(needle):
            return [part.strip() for part in line.strip().strip("|").split("|")]
    raise ValueError(f"未找到表格行: {label}")


def _extract_baseline_metrics(text: str) -> dict[str, float]:
    match = _BASELINE_RE.search(text)
    if not match:
        raise ValueError("未找到基线指标。")
    return {
        "strategy_return_pct": _parse_pct(match.group(1)),
        "max_drawdown_pct": _parse_pct(match.group(2)),
        "sharpe": float(match.group(3)),
    }


def _extract_base_position_distribution(text: str) -> dict[str, int]:
    match = _DISTRIBUTION_RE.search(text)
    if not match:
        raise ValueError("未找到基础仓位分布。")
    return {
        "full_days": int(match.group(1)),
        "half_days": int(match.group(2)),
    }


def _extract_execution_metrics(text: str) -> dict[str, float]:
    row = _parse_table_row(text, "第二确认空仓两日")
    return {
        "trigger_days": int(row[1]),
        "trigger_day_target_position": float(row[2]),
        "strategy_return_pct": _parse_pct(row[3]),
        "max_drawdown_pct": _parse_pct(row[4]),
        "sharpe": float(row[5]),
        "average_position": float(row[6]),
        "avg_contract_return_5d_pct": _parse_pct(row[7]),
        "avg_excess_return_5d_pct": _parse_pct(row[8]),
    }


def _extract_reentry_metrics(text: str) -> dict[str, float]:
    row = _parse_table_row(text, "空仓2日")
    return {
        "trigger_days": int(row[1]),
        "strategy_return_pct": _parse_pct(row[2]),
        "max_drawdown_pct": _parse_pct(row[3]),
        "sharpe": float(row[4]),
        "average_position": float(row[5]),
        "avg_contract_return_5d_pct": _parse_pct(row[6]),
        "avg_excess_return_5d_pct": _parse_pct(row[7]),
    }


def build_ic_candidate_execution_record(
    *,
    execution_report_path: str | Path,
    reentry_report_path: str | Path,
    strategy_path: str | Path,
    docs_path: str | Path,
    current_stage: str = "candidate",
    review_status: str = "pending_review",
    source_label: str = "ic_second_confirmation_execution_governance",
) -> dict[str, Any]:
    execution_path = Path(execution_report_path)
    reentry_path = Path(reentry_report_path)
    strategy_file = Path(strategy_path)
    docs_file = Path(docs_path)

    execution_text = execution_path.read_text(encoding="utf-8")
    reentry_text = reentry_path.read_text(encoding="utf-8")

    baseline = _extract_baseline_metrics(execution_text)
    distribution = _extract_base_position_distribution(execution_text)
    execution_metrics = _extract_execution_metrics(execution_text)
    reentry_metrics = _extract_reentry_metrics(reentry_text)

    return {
        "run_timestamp": datetime.now().isoformat(),
        "source_label": source_label,
        "candidate_key": "ic_second_confirmation_flat_2d",
        "candidate_display_name": "IC第二确认后空仓2日",
        "current_stage": current_stage,
        "review_status": review_status,
        "trigger_definition": "趋势破坏 + 单日弱势",
        "scope_note": "仅在第二确认触发时基础仓位已处于半仓的前提下适用。",
        "candidate_action": "第二确认后将 IC 仓位降到 0，连续空仓 2 个交易日，再回到原框架观察。",
        "base_position_distribution": distribution,
        "baseline_metrics": baseline,
        "execution_report_metrics": execution_metrics,
        "reentry_report_metrics": reentry_metrics,
        "evidence_paths": [
            str(execution_path),
            str(reentry_path),
        ],
        "strategy_path": str(strategy_file),
        "docs_path": str(docs_file),
        "governance_summary": (
            "当前仅列为候选执行规则 / 待审执行层；"
            "证据显示其优于立刻清零后立即恢复，也优于过长冷静期。"
        ),
        "promotion_gate": (
            "在更多真实盘中样本或更成熟的第一层预警规则出现前，"
            "只允许以 candidate 口径并行观察，不直接升为生产默认动作。"
        ),
    }


def append_ic_candidate_execution_record(path: str | Path, record: dict[str, Any]) -> Path:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, ensure_ascii=False) + "\n")
    return target


def latest_ic_candidate_execution_record(path: str | Path) -> dict[str, Any] | None:
    target = Path(path)
    if not target.exists():
        return None
    rows = [
        json.loads(line)
        for line in target.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    if not rows:
        return None
    return rows[-1]


def render_ic_candidate_execution_summary(record: dict[str, Any]) -> str:
    baseline = dict(record.get("baseline_metrics") or {})
    execution = dict(record.get("execution_report_metrics") or {})
    reentry = dict(record.get("reentry_report_metrics") or {})
    distribution = dict(record.get("base_position_distribution") or {})
    evidence_paths = list(record.get("evidence_paths") or [])

    lines = [
        "# IC候选执行治理摘要",
        "",
        f"- 候选规则: `{record.get('candidate_display_name')}`",
        f"- 当前阶段: `{record.get('current_stage')}`",
        f"- 审核状态: `{record.get('review_status')}`",
        f"- 第二确认定义: `{record.get('trigger_definition')}`",
        f"- 适用范围: {record.get('scope_note')}",
        f"- 候选动作: {record.get('candidate_action')}",
        "",
        "## 基线与证据",
        "",
        (
            f"- 基线策略: 收益 {baseline.get('strategy_return_pct', 0.0):.2f}% | "
            f"最大回撤 {baseline.get('max_drawdown_pct', 0.0):.2f}% | "
            f"Sharpe {baseline.get('sharpe', 0.0):.2f}"
        ),
        (
            f"- 执行细节验证最佳项: 收益 {execution.get('strategy_return_pct', 0.0):.2f}% | "
            f"最大回撤 {execution.get('max_drawdown_pct', 0.0):.2f}% | "
            f"Sharpe {execution.get('sharpe', 0.0):.2f}"
        ),
        (
            f"- 冷静期与回补验证最佳项: 收益 {reentry.get('strategy_return_pct', 0.0):.2f}% | "
            f"最大回撤 {reentry.get('max_drawdown_pct', 0.0):.2f}% | "
            f"Sharpe {reentry.get('sharpe', 0.0):.2f}"
        ),
        (
            f"- 触发时基础仓位分布: 满仓 {distribution.get('full_days', 0)} 天 / "
            f"半仓 {distribution.get('half_days', 0)} 天"
        ),
        "",
        "## 当前治理口径",
        "",
        f"- {record.get('governance_summary')}",
        f"- {record.get('promotion_gate')}",
        "",
        "## 证据文件",
        "",
    ]
    for path in evidence_paths:
        lines.append(f"- `{path}`")
    lines.extend(
        [
            f"- `{record.get('strategy_path')}`",
            f"- `{record.get('docs_path')}`",
            "",
        ]
    )
    return "\n".join(lines)
