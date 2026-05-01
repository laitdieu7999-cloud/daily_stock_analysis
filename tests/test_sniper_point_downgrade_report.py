# -*- coding: utf-8 -*-
"""Tests for sniper point downgrade summary reports."""

from __future__ import annotations

import json
from pathlib import Path

from src.services.sniper_point_downgrade_report import (
    build_sniper_point_downgrade_summary,
    render_sniper_point_downgrade_summary,
    write_sniper_point_downgrade_summary,
)


def _append_jsonl(path: Path, row: dict) -> None:
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(row, ensure_ascii=False) + "\n")


def test_build_sniper_point_downgrade_summary_groups_recent_events(tmp_path: Path) -> None:
    audit_path = tmp_path / "audit.jsonl"
    _append_jsonl(
        audit_path,
        {
            "created_at": "2026-04-30T15:00:00",
            "event": "sniper_point_protective_downgrade",
            "field": "ideal_buy",
            "current_price": 27.48,
            "price_values": [33.64],
            "distance_pct_values": [22.4],
            "downgraded_value": "暂不设买入区；原点位偏离现价27.48元过大，待行情刷新后重算",
            "context": {"source": "analysis_response", "code": "002580", "name": "圣阳股份"},
        },
    )
    _append_jsonl(
        audit_path,
        {
            "created_at": "2026-04-30T15:01:00",
            "event": "sniper_point_protective_downgrade",
            "field": "take_profit",
            "current_price": 27.48,
            "price_values": [40.0],
            "distance_pct_values": [45.6],
            "downgraded_value": "暂不设目标区；原点位偏离现价27.48元过大，待行情刷新后重算",
            "context": {"source": "history_markdown", "code": "002580", "name": "圣阳股份"},
        },
    )
    audit_path.write_text(audit_path.read_text(encoding="utf-8") + "{bad json\n", encoding="utf-8")

    summary = build_sniper_point_downgrade_summary(audit_path, max_recent_rows=1)

    assert summary["total_events"] == 2
    assert summary["unique_symbols"] == 1
    assert summary["latest_created_at"] == "2026-04-30T15:01:00"
    assert summary["by_field"][0]["count"] == 1
    assert summary["by_symbol"][0]["label"] == "圣阳股份(002580)"
    assert len(summary["recent_events"]) == 1
    assert summary["recent_events"][0]["field_label"] == "目标/反抽线"
    assert summary["recent_events"][0]["max_distance_pct"] == "45.6%"


def test_render_sniper_point_downgrade_summary_handles_missing_audit(tmp_path: Path) -> None:
    summary = build_sniper_point_downgrade_summary(tmp_path / "missing.jsonl")
    rendered = render_sniper_point_downgrade_summary(summary)

    assert "总拦截次数: 0" in rendered
    assert "当前还没有审计文件" in rendered


def test_summary_includes_current_price_context_mismatch_events(tmp_path: Path) -> None:
    audit_path = tmp_path / "audit.jsonl"
    _append_jsonl(
        audit_path,
        {
            "created_at": "2026-04-30T15:02:00",
            "event": "sniper_point_context_mismatch",
            "field": "all",
            "current_price": 27.48,
            "mismatch_pct": 4921.8,
            "current_price_candidates": [
                {"source": "result.current_price", "value": 27.48},
                {"source": "dashboard.price_position.current_price", "value": 1380.0},
            ],
            "downgraded_value": {
                "ideal_buy": "暂不设买入区；行情上下文不一致，待刷新后重算",
                "take_profit": "暂不设目标区；行情上下文不一致，待刷新后重算",
            },
            "context": {"source": "analysis_response", "code": "002580", "name": "圣阳股份"},
        },
    )

    summary = build_sniper_point_downgrade_summary(audit_path)
    rendered = render_sniper_point_downgrade_summary(summary)

    assert summary["total_events"] == 1
    assert summary["by_event"][0]["label"] == "行情上下文不一致"
    assert summary["by_field"][0]["label"] == "整组点位"
    assert "result.current_price: 27.48" in rendered
    assert "dashboard.price_position.current_price: 1380" in rendered
    assert "暂不设买入区" in rendered


def test_write_sniper_point_downgrade_summary_creates_markdown(tmp_path: Path) -> None:
    audit_path = tmp_path / "audit.jsonl"
    output_path = tmp_path / "summary.md"
    _append_jsonl(
        audit_path,
        {
            "created_at": "2026-04-30T15:00:00",
            "event": "sniper_point_protective_downgrade",
            "field": "stop_loss",
            "current_price": 10,
            "price_values": [6],
            "distance_pct_values": [40],
            "downgraded_value": "暂不设止损线；原点位偏离现价10元过大，待行情刷新后重算",
            "context": {"source": "notification_report", "code": "600000"},
        },
    )

    target = write_sniper_point_downgrade_summary(audit_path, output_path)

    assert target == output_path
    rendered = output_path.read_text(encoding="utf-8")
    assert "狙击点位保护降级摘要" in rendered
    assert "止损/防守线" in rendered
    assert "600000" in rendered
