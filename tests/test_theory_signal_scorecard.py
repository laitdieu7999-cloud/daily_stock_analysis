from __future__ import annotations

import importlib.util
import sqlite3
import sys
import tempfile
from datetime import date, timedelta
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = PROJECT_ROOT / "scripts" / "run_theory_signal_scorecard.py"
SPEC = importlib.util.spec_from_file_location("run_theory_signal_scorecard", SCRIPT_PATH)
scorecard = importlib.util.module_from_spec(SPEC)
assert SPEC and SPEC.loader
sys.modules["run_theory_signal_scorecard"] = scorecard
SPEC.loader.exec_module(scorecard)


def test_payoff_ratio_handles_asymmetric_returns() -> None:
    ratio = scorecard._payoff_ratio(scorecard.pd.Series([2.0, 4.0, -1.0, -2.0]))
    assert round(ratio, 6) == 2.0


def test_dynamic_forward_label_filter_rejects_corrupt_labels() -> None:
    row = scorecard.pd.Series({"ATR60_PCT": 2.0, "ABS_RET_P99_120": 4.0})
    assert scorecard._valid_forward_label(row, 5, 20.0, 10.0, -8.0)
    assert not scorecard._valid_forward_label(row, 5, 500.0)


def test_defensive_grade_requires_negative_future_return_and_payoff() -> None:
    noisy = scorecard.pd.Series(
        {
            "sample_count": 100,
            "avg_avoided_drawdown_pct": 3.0,
            "avg_return_pct": 0.2,
            "payoff_ratio": 1.2,
            "false_rebound_rate_pct": 30.0,
            "down_hit_rate_pct": 60.0,
        }
    )
    qualified = noisy.copy()
    qualified["avg_return_pct"] = -0.5

    assert scorecard._grade_defensive(noisy, 50) == "继续观察"
    assert scorecard._grade_defensive(qualified, 50) == "可进Shadow"


def test_summarize_equity_records_splits_offense_and_defense() -> None:
    records = []
    for idx in range(60):
        records.append(
            {
                "module": "自选股买入",
                "direction_type": "offensive",
                "rule": "test-buy",
                "description": "test",
                "market_regime": "震荡",
                "window": 5,
                "future_return_pct": 2.0 if idx % 2 == 0 else -1.0,
                "mfe_pct": 3.0,
                "mae_pct": -1.0,
                "direction_correct": idx % 2 == 0,
                "avoided_drawdown_pct": None,
                "false_rebound": None,
            }
        )
        records.append(
            {
                "module": "持仓风控",
                "direction_type": "defensive",
                "rule": "test-risk",
                "description": "test",
                "market_regime": "震荡",
                "window": 5,
                "future_return_pct": -2.0 if idx % 2 == 0 else 1.0,
                "mfe_pct": 1.0,
                "mae_pct": -4.0,
                "direction_correct": idx % 2 == 0,
                "avoided_drawdown_pct": 4.0,
                "false_rebound": False,
            }
        )

    summary = scorecard._summarize_equity_records(records, min_samples=50)
    buy = summary[summary["rule"] == "test-buy"].iloc[0]
    risk = summary[summary["rule"] == "test-risk"].iloc[0]

    assert buy["direction_type"] == "offensive"
    assert buy["payoff_ratio"] == 2.0
    assert risk["direction_type"] == "defensive"
    assert risk["avg_avoided_drawdown_pct"] == 4.0

    regime_summary = scorecard._summarize_equity_records(records, min_samples=50, by_regime=True)
    assert set(regime_summary["market_regime"]) == {"震荡"}


def test_market_regime_classification_prioritizes_extremes() -> None:
    assert scorecard._classify_market_regime(scorecard.pd.Series({"RET_1": -3.0, "RET_5": 0, "RETURN_20": 0})) == "急跌"
    assert scorecard._classify_market_regime(scorecard.pd.Series({"RET_1": 0, "RET_5": 0, "RETURN_20": -9.0})) == "阴跌"
    assert scorecard._classify_market_regime(scorecard.pd.Series({"RET_1": 3.0, "RET_5": 0, "RETURN_20": 0})) == "急涨"
    assert (
        scorecard._classify_market_regime(
            scorecard.pd.Series({"RET_1": 0, "RET_5": 0, "RETURN_20": 4.0, "close": 11.0, "MA20": 10.0})
        )
        == "上行"
    )


def test_walk_forward_promotes_on_train_and_checks_next_year() -> None:
    records = []
    for year in [2019, 2020, 2021]:
        for idx in range(12):
            if year == 2021:
                future_return = 1.0 if idx % 2 == 0 else -0.4
            else:
                future_return = 2.0 if idx % 2 == 0 else -1.0
            records.append(
                {
                    "module": "日线技术信号",
                    "direction_type": "offensive",
                    "rule": "wf-buy",
                    "description": "test",
                    "market_regime": "震荡",
                    "window": 5,
                    "date": f"{year}-01-{idx + 1:02d}",
                    "future_return_pct": future_return,
                    "mfe_pct": 2.0,
                    "mae_pct": -1.0,
                    "direction_correct": future_return > 0,
                    "avoided_drawdown_pct": None,
                    "false_rebound": None,
                }
            )

    result = scorecard._evaluate_walk_forward(
        records,
        primary_window=5,
        min_train_samples=10,
        train_years=2,
    )

    assert result["status"] == "ok"
    assert result["summary"]["valid_tests"] == 1
    assert result["summary"]["pass_count"] == 1
    assert result["rows"][0]["test_year"] == 2021
    assert result["rows"][0]["oos_status"] == "通过"


def test_parameter_plateau_status_marks_stable_and_single_point() -> None:
    assert scorecard._plateau_status(2, 3) == "稳定高原"
    assert scorecard._plateau_status(1, 3) == "单点敏感"
    assert scorecard._plateau_status(0, 3) == "不稳定"


def test_parameter_plateau_evaluates_rule_neighborhoods() -> None:
    rows = []
    start = date(2025, 1, 1)
    for idx in range(80):
        close = 100.0 + idx * 0.2
        rows.append(
            {
                "code": "600519",
                "date": scorecard.pd.Timestamp(start + timedelta(days=idx)),
                "open": close,
                "high": close + 1.5,
                "low": close - 0.3,
                "close": close,
                "volume": 10000.0,
                "amount": 100000.0,
                "MA5": close + 0.1,
                "MA10": close,
                "MA20": close * 0.985,
                "RET_1": 0.2,
                "RET_5": 1.0,
                "RETURN_20": 4.0,
                "PREV_CLOSE": close - 0.2,
                "PREV_MA20": (close - 0.2) * 0.985,
                "VOLUME_MA20": 10000.0,
                "ATR60_PCT": 6.0,
                "ABS_RET_P99_120": 6.0,
                "market_regime": "上行",
            }
        )
    indicator_df = scorecard.pd.DataFrame(rows)

    result = scorecard._evaluate_parameter_plateau(indicator_df, primary_window=5, min_samples=10)

    assert result["status"] == "ok"
    family_names = {row["parameter_family"] for row in result["families"]}
    assert "趋势回踩MA5/MA10" in family_names
    assert any(row["plateau_status"] == "稳定高原" for row in result["families"])


def test_permutation_status_uses_p_value_thresholds() -> None:
    assert scorecard._permutation_status(0.03, sample_count=80, min_samples=50) == "显著优于随机"
    assert scorecard._permutation_status(0.12, sample_count=80, min_samples=50) == "略优于随机"
    assert scorecard._permutation_status(0.50, sample_count=80, min_samples=50) == "不显著"
    assert scorecard._permutation_status(0.03, sample_count=10, min_samples=50) == "样本不足"


def test_random_baseline_frame_builds_forward_labels() -> None:
    rows = []
    start = date(2025, 1, 1)
    for idx in range(8):
        close = 10.0 + idx
        rows.append(
            {
                "code": "600519",
                "date": scorecard.pd.Timestamp(start + timedelta(days=idx)),
                "open": close,
                "high": close + 0.5,
                "low": close - 2.0,
                "close": close,
                "volume": 1000.0,
                "amount": 10000.0,
                "ATR60_PCT": 10.0,
                "ABS_RET_P99_120": 10.0,
                "market_regime": "震荡",
            }
        )
    frame = scorecard.pd.DataFrame(rows)

    baseline = scorecard._build_random_baseline_frame(frame, window=3)

    assert len(baseline) == 5
    assert baseline["future_return_pct"].iloc[0] > 0
    assert baseline["avoided_drawdown_pct"].iloc[0] > 0


def test_cost_stress_subtracts_round_trip_cost() -> None:
    records = []
    for idx in range(60):
        records.append(
            {
                "module": "日线技术信号",
                "direction_type": "offensive",
                "rule": "cost-buy",
                "description": "test",
                "window": 5,
                "future_return_pct": 1.0 if idx % 2 == 0 else -0.2,
            }
        )

    result = scorecard._evaluate_cost_stress(records, primary_window=5, min_samples=50, cost_bps_values=[20.0])

    assert result["status"] == "ok"
    assert round(result["rows"][0]["net_avg_return_pct"], 6) == 0.2
    assert result["rows"][0]["grade"] == "成本后通过"


def test_symbol_attribution_flags_concentration_for_shadow_candidates() -> None:
    records = []
    for code, ret in [("600519", 2.0), ("000858", 1.0), ("510300", -0.2)]:
        for idx in range(30):
            records.append(
                {
                    "module": "日线技术信号",
                    "direction_type": "offensive",
                    "rule": "attr-buy",
                    "code": code,
                    "window": 5,
                    "future_return_pct": ret,
                }
            )
    result = scorecard._evaluate_symbol_attribution(
        records,
        graduation_scorecard={
            "rows": [
                {
                    "module": "日线技术信号",
                    "direction_type": "offensive",
                    "rule": "attr-buy",
                    "final_decision": "可进Shadow",
                }
            ]
        },
        primary_window=5,
        min_symbol_samples=10,
    )

    assert result["status"] == "ok"
    assert result["summary"][0]["code_count"] == 3
    assert result["summary"][0]["concentration_status"] in {"偏集中", "高度集中"}
    assert result["details"][0]["code"] == "600519"


def test_graduation_scorecard_combines_gates() -> None:
    summary = scorecard.pd.DataFrame(
        [
            {
                "module": "自选股买入",
                "direction_type": "offensive",
                "rule": "趋势回踩MA5/MA10",
                "description": "test",
                "window": 5,
                "sample_count": 80,
                "avg_return_pct": 1.2,
                "avg_avoided_drawdown_pct": None,
                "payoff_ratio": 1.5,
                "grade": "可进Shadow",
            }
        ]
    )
    result = scorecard._build_graduation_scorecard(
        summary=summary,
        walk_forward={"by_rule": [{"module": "自选股买入", "direction_type": "offensive", "rule": "趋势回踩MA5/MA10", "oos_pass_rate_pct": 100.0}]},
        parameter_plateau={"families": [{"parameter_family": "趋势回踩MA5/MA10", "plateau_status": "稳定高原"}]},
        permutation_baseline={"rows": [{"module": "自选股买入", "direction_type": "offensive", "rule": "趋势回踩MA5/MA10", "status": "显著优于随机", "p_value": 0.01}]},
        cost_stress={"rows": [{"module": "自选股买入", "direction_type": "offensive", "rule": "趋势回踩MA5/MA10", "cost_bps": 20.0, "grade": "成本后通过", "net_avg_return_pct": 0.8}]},
        primary_window=5,
        min_samples=50,
        standard_cost_bps=20.0,
    )

    assert result["status"] == "ok"
    assert result["rows"][0]["final_decision"] == "可进Shadow"


def test_load_theory_daily_frame_reads_sqlite_focus_rows_without_external_caches() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        conn = sqlite3.connect(db_path)
        conn.execute(
            """
            CREATE TABLE stock_daily (
                id INTEGER PRIMARY KEY,
                code VARCHAR(10) NOT NULL,
                date DATE NOT NULL,
                open FLOAT,
                high FLOAT,
                low FLOAT,
                close FLOAT,
                volume FLOAT,
                amount FLOAT,
                pct_chg FLOAT,
                ma5 FLOAT,
                ma10 FLOAT,
                ma20 FLOAT,
                volume_ratio FLOAT,
                data_source VARCHAR(50),
                created_at DATETIME,
                updated_at DATETIME
            )
            """
        )
        start = date(2026, 1, 1)
        for idx in range(5):
            day = start + timedelta(days=idx)
            conn.execute(
                """
                INSERT INTO stock_daily
                (code, date, open, high, low, close, volume, amount)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                ("600519", day.isoformat(), 10 + idx, 11 + idx, 9 + idx, 10.5 + idx, 1000 + idx, 10000 + idx),
            )
        conn.commit()
        conn.close()

        daily, meta = scorecard.load_theory_daily_frame(
            db_path=db_path,
            focus_codes=["600519"],
            use_qlib=False,
            use_etf_cache=False,
        )

    assert len(daily) == 5
    assert meta["code_count"] == 1
    assert meta["source_rows"][0]["source"] == "sqlite_stock_daily"
