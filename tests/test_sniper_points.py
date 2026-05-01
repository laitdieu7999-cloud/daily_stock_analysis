# -*- coding: utf-8 -*-
"""Tests for stock sniper point display normalization."""

import json

from src.services import sniper_points as sniper_points_module
from src.services.sniper_points import (
    clean_sniper_items,
    clean_sniper_points,
    clean_sniper_value,
    refine_sniper_points_for_context,
)


def test_clean_sniper_value_formats_mapping_without_raw_payload() -> None:
    value = {
        "price": 20.1,
        "reason": "MA5附近，等待站稳",
        "raw": {"debug": [1, 2, 3]},
    }

    assert clean_sniper_value(value) == "20.10元（MA5附近，等待站稳）"


def test_clean_sniper_value_formats_json_range_string() -> None:
    value = '{"low": 8.3, "high": 8.33, "condition": "回踩确认"}'

    assert clean_sniper_value(value) == "8.30-8.33元（回踩确认）"


def test_clean_sniper_value_formats_plain_range() -> None:
    assert clean_sniper_value("8.30-8.33") == "8.30-8.33元"


def test_clean_sniper_value_preserves_nearby_condition() -> None:
    assert clean_sniper_value("3.70附近") == "3.70元附近"


def test_clean_sniper_value_filters_complex_sequence_to_actionable_level() -> None:
    value = [{"debug": "无"}, {"price": 0.986, "basis": "前低支撑"}]

    assert clean_sniper_value(value) == "0.986元（前低支撑）"


def test_clean_sniper_points_keeps_fixed_field_contract() -> None:
    points = {"ideal_buy": {"price": 112}, "stop_loss": "跌破 108 元止损"}

    cleaned = clean_sniper_points(points)

    assert cleaned["ideal_buy"] == "112.00元"
    assert cleaned["secondary_buy"] == "N/A"
    assert cleaned["stop_loss"] == "跌破 108 元止损"
    assert cleaned["take_profit"] == "N/A"


def test_clean_sniper_items_supports_nonstandard_labels_for_summary_reports() -> None:
    points = {
        "首仓区": '{"low": 3.8, "high": 3.85, "condition": "回踩"}',
        "加仓区": {"price": 3.7, "reason": "支撑附近"},
        "debug": {"raw": ["无"]},
    }

    assert clean_sniper_items(points) == [
        ("首仓区", "3.80-3.85元（回踩）"),
        ("加仓区", "3.70元（支撑附近）"),
    ]


def test_refine_sniper_points_rewrites_bearish_far_target_to_near_pressure() -> None:
    points = {
        "ideal_buy": "暂不新开仓；重新站回MA5附近 28.42元 后再评估",
        "secondary_buy": "保守等待回踩MA10附近 28.91元 且止跌后再看",
        "stop_loss": "立即止损：以现价27.48元或更优价格清仓。若无法执行，强制止损位设于26.00元。",
        "take_profit": "目标位：33.64元（压力位或约8%风险回报目标）",
    }

    refined = refine_sniper_points_for_context(
        points,
        current_price=27.48,
        decision_type="sell",
        operation_advice="卖出",
        trend_prediction="强烈看空",
        dashboard={
            "data_perspective": {
                "price_position": {
                    "current_price": 27.48,
                    "ma5": 28.42,
                    "ma10": 28.91,
                    "resistance_level": 28.42,
                }
            }
        },
    )

    assert refined["ideal_buy"] == "暂不接回；重新站回28.42元后再评估（较现价+3.4%）"
    assert refined["secondary_buy"] == "确认转强：站稳28.91元且止跌后再看"
    assert refined["stop_loss"] == "27.48元附近离场；硬止损26.00元"
    assert refined["take_profit"] == "反抽出局线：28.42元附近（不是止盈目标）"
    assert "33.64" not in refined["take_profit"]


def test_refine_sniper_points_does_not_call_upper_bounce_level_hard_stop() -> None:
    points = {
        "ideal_buy": "暂不新开仓；重新站回MA5附近 99.42元 后再评估",
        "secondary_buy": "保守等待回踩MA10附近 100.33元 且止跌后再看",
        "stop_loss": "立即止损：以现价97.08元附近离场，若反抽至101.81元再重新评估。",
        "take_profit": "目标位：108.00元（压力位）",
    }

    refined = refine_sniper_points_for_context(
        points,
        current_price=97.08,
        decision_type="sell",
        operation_advice="减仓",
        trend_prediction="看空",
        dashboard={
            "data_perspective": {
                "price_position": {
                    "current_price": 97.08,
                    "ma5": 99.42,
                    "ma10": 100.33,
                }
            }
        },
    )

    assert refined["stop_loss"] == "97.08元附近离场"
    assert "硬止损101.81" not in refined["stop_loss"]
    assert refined["take_profit"] == "反抽出局线：99.42元附近（不是止盈目标）"


def test_refine_sniper_points_downgrades_far_levels_against_current_price() -> None:
    points = {
        "ideal_buy": "12.00",
        "secondary_buy": "13.00",
        "stop_loss": "10.00",
        "take_profit": "36.00",
    }

    refined = refine_sniper_points_for_context(
        points,
        current_price=27.48,
        decision_type="buy",
        operation_advice="买入",
        trend_prediction="看多",
    )

    assert "偏离现价27.48元过大" in refined["ideal_buy"]
    assert "偏离现价27.48元过大" in refined["secondary_buy"]
    assert "偏离现价27.48元过大" in refined["stop_loss"]
    assert "偏离现价27.48元过大" in refined["take_profit"]
    assert "36.00" not in refined["take_profit"]


def test_refine_sniper_points_downgrades_all_levels_when_current_price_context_conflicts(
    tmp_path,
    monkeypatch,
) -> None:
    audit_path = tmp_path / "sniper_point_downgrade_audit.jsonl"
    monkeypatch.setattr(sniper_points_module, "SNIPER_POINT_DOWNGRADE_AUDIT_PATH", audit_path)
    points = {
        "ideal_buy": "28.00",
        "secondary_buy": "29.00",
        "stop_loss": "26.50",
        "take_profit": "31.00",
    }

    refined = refine_sniper_points_for_context(
        points,
        current_price=27.48,
        decision_type="buy",
        operation_advice="买入",
        trend_prediction="看多",
        dashboard={"data_perspective": {"price_position": {"current_price": 1380.0}}},
        market_snapshot={"price": 27.49},
        audit_context={"source": "unit_test", "code": "002580", "name": "圣阳股份"},
    )

    assert refined["ideal_buy"] == "暂不设买入区；行情上下文不一致，待刷新后重算"
    assert refined["secondary_buy"] == "暂不设加仓区；行情上下文不一致，待刷新后重算"
    assert refined["stop_loss"] == "暂不设止损线；行情上下文不一致，待刷新后重算"
    assert refined["take_profit"] == "暂不设目标区；行情上下文不一致，待刷新后重算"

    rows = [
        json.loads(line)
        for line in audit_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert len(rows) == 1
    assert rows[0]["event"] == "sniper_point_context_mismatch"
    assert rows[0]["field"] == "all"
    assert rows[0]["context"]["code"] == "002580"
    assert rows[0]["mismatch_pct"] > 3.0
    assert {item["source"] for item in rows[0]["current_price_candidates"]} == {
        "result.current_price",
        "dashboard.price_position.current_price",
    }


def test_refine_sniper_points_records_downgrade_audit_when_context_is_available(tmp_path, monkeypatch) -> None:
    audit_path = tmp_path / "sniper_point_downgrade_audit.jsonl"
    monkeypatch.setattr(sniper_points_module, "SNIPER_POINT_DOWNGRADE_AUDIT_PATH", audit_path)
    points = {
        "ideal_buy": {"price": 12.0, "reason": "旧点位"},
        "secondary_buy": "13.00",
        "stop_loss": "10.00",
        "take_profit": "36.00",
    }

    refine_sniper_points_for_context(
        points,
        current_price=27.48,
        decision_type="buy",
        operation_advice="买入",
        trend_prediction="看多",
        audit_context={"source": "unit_test", "code": "002580", "name": "圣阳股份"},
    )

    rows = [
        json.loads(line)
        for line in audit_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]

    assert len(rows) == 4
    assert {row["field"] for row in rows} == {"ideal_buy", "secondary_buy", "stop_loss", "take_profit"}
    assert rows[0]["event"] == "sniper_point_protective_downgrade"
    assert rows[0]["context"]["source"] == "unit_test"
    assert rows[0]["context"]["code"] == "002580"
    assert rows[0]["current_price"] == 27.48
    assert rows[0]["limit_pct"] == 20.0
    assert "偏离现价27.48元过大" in rows[0]["downgraded_value"]


def test_refine_sniper_points_does_not_write_audit_without_context(tmp_path, monkeypatch) -> None:
    audit_path = tmp_path / "sniper_point_downgrade_audit.jsonl"
    monkeypatch.setattr(sniper_points_module, "SNIPER_POINT_DOWNGRADE_AUDIT_PATH", audit_path)

    refine_sniper_points_for_context(
        {"ideal_buy": "12.00"},
        current_price=27.48,
        decision_type="buy",
        operation_advice="买入",
        trend_prediction="看多",
    )

    assert not audit_path.exists()


def test_refine_sniper_points_keeps_nearby_levels_and_ignores_percent_text() -> None:
    points = {
        "ideal_buy": "暂不接回；重新站回28.42元后再评估（较现价+3.4%）",
        "secondary_buy": "确认转强：站稳28.91元且止跌后再看",
        "stop_loss": "27.48元附近离场；硬止损26.00元",
        "take_profit": "反抽出局线：28.42元附近（不是止盈目标）",
    }

    refined = refine_sniper_points_for_context(
        points,
        current_price=27.48,
        decision_type="sell",
        operation_advice="卖出",
        trend_prediction="强烈看空",
    )

    assert refined["ideal_buy"] == "暂不接回；重新站回28.42元后再评估（较现价+3.4%）"
    assert refined["secondary_buy"] == "确认转强：站稳28.91元且止跌后再看"
    assert refined["stop_loss"] == "27.48元附近离场；硬止损26.00元"
    assert refined["take_profit"] == "反抽出局线：28.42元附近（不是止盈目标）"
