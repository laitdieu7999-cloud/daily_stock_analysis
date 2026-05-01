from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from src.services.ic_candidate_governance import (
    append_ic_candidate_execution_record,
    build_ic_candidate_execution_record,
    latest_ic_candidate_execution_record,
    render_ic_candidate_execution_summary,
)


EXECUTION_REPORT = """# 2026-04-26 IC第二确认执行细节验证报告

- 触发时基础仓位分布: 满仓 0 天 / 半仓 187 天

## 基线

- 原始高贴水满仓/普通半仓: 收益 49.00% | 最大回撤 -17.24% | Sharpe 0.73

## 执行细节比较

| 方案 | 触发天数 | 触发日目标仓位 | 策略收益 | 最大回撤 | Sharpe | 平均仓位 | 触发后5日合约均值 | 触发后5日超额均值 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 第二确认空仓两日 | 187 | 0.00 | 54.86% | -10.29% | 0.98 | 0.42 | 0.52% | 0.20% |
"""

REENTRY_REPORT = """# 2026-04-26 IC第二确认冷静期与回补搜索报告

## 基线

- 原始高贴水满仓/普通半仓: 收益 49.00% | 最大回撤 -17.24% | Sharpe 0.73

## 冷静期与回补方案比较

| 方案 | 触发天数 | 策略收益 | 最大回撤 | Sharpe | 平均仓位 | 触发后5日合约均值 | 触发后5日超额均值 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 空仓2日 | 187 | 54.86% | -10.29% | 0.98 | 0.42 | 0.52% | 0.20% |
"""


class IcCandidateGovernanceTestCase(unittest.TestCase):
    def test_build_record_extracts_expected_metrics(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            execution_path = tmp / "execution.md"
            reentry_path = tmp / "reentry.md"
            strategy_path = tmp / "ic_basis_roll_framework.yaml"
            docs_path = tmp / "CUSTOM_EXTENSIONS.md"
            execution_path.write_text(EXECUTION_REPORT, encoding="utf-8")
            reentry_path.write_text(REENTRY_REPORT, encoding="utf-8")
            strategy_path.write_text("name: ic_basis_roll_framework\n", encoding="utf-8")
            docs_path.write_text("# docs\n", encoding="utf-8")

            record = build_ic_candidate_execution_record(
                execution_report_path=execution_path,
                reentry_report_path=reentry_path,
                strategy_path=strategy_path,
                docs_path=docs_path,
            )

        self.assertEqual(record["candidate_key"], "ic_second_confirmation_flat_2d")
        self.assertEqual(record["base_position_distribution"]["half_days"], 187)
        self.assertAlmostEqual(record["baseline_metrics"]["sharpe"], 0.73)
        self.assertAlmostEqual(record["execution_report_metrics"]["sharpe"], 0.98)
        self.assertAlmostEqual(record["reentry_report_metrics"]["max_drawdown_pct"], -10.29)

    def test_append_and_load_latest_record(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            target = Path(tmpdir) / "ledger.jsonl"
            record = {
                "candidate_key": "ic_second_confirmation_flat_2d",
                "run_timestamp": "2026-04-26T21:00:00",
            }
            append_ic_candidate_execution_record(target, record)
            latest = latest_ic_candidate_execution_record(target)

        self.assertEqual(latest, record)

    def test_render_summary_mentions_candidate_and_metrics(self) -> None:
        record = {
            "candidate_display_name": "IC第二确认后空仓2日",
            "current_stage": "candidate",
            "review_status": "pending_review",
            "trigger_definition": "趋势破坏 + 单日弱势",
            "scope_note": "仅在半仓时适用。",
            "candidate_action": "空仓2日后回原框架。",
            "baseline_metrics": {
                "strategy_return_pct": 49.0,
                "max_drawdown_pct": -17.24,
                "sharpe": 0.73,
            },
            "execution_report_metrics": {
                "strategy_return_pct": 54.86,
                "max_drawdown_pct": -10.29,
                "sharpe": 0.98,
            },
            "reentry_report_metrics": {
                "strategy_return_pct": 54.86,
                "max_drawdown_pct": -10.29,
                "sharpe": 0.98,
            },
            "base_position_distribution": {"full_days": 0, "half_days": 187},
            "governance_summary": "当前仅列为候选执行规则 / 待审执行层。",
            "promotion_gate": "不直接升为生产默认动作。",
            "evidence_paths": ["/tmp/a.md", "/tmp/b.md"],
            "strategy_path": "/tmp/strategy.yaml",
            "docs_path": "/tmp/docs.md",
        }
        summary = render_ic_candidate_execution_summary(record)
        self.assertIn("IC第二确认后空仓2日", summary)
        self.assertIn("Sharpe 0.98", summary)
        self.assertIn("候选执行规则 / 待审执行层", summary)


if __name__ == "__main__":
    unittest.main()
