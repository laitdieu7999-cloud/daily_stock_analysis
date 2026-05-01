from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from src.services.ic_term_structure_shadow_monitor import (
    build_ic_shadow_signal_event,
    build_term_structure_shadow_events,
    enrich_term_structure_shadow_events,
    load_market_snapshots,
    render_term_structure_shadow_monitoring_summary,
    summarize_term_structure_shadow_monitoring,
    write_ic_shadow_signal_events,
    write_term_structure_shadow_events,
)


class IcTermStructureShadowMonitorTestCase(unittest.TestCase):
    def test_build_term_structure_shadow_events_keeps_only_candidates(self) -> None:
        snapshots = [
            {
                "report_date": "2026-04-27",
                "captured_at": "2026-04-27T14:10:00",
                "csi500_term_structure": {
                    "near_symbol": "IC2606",
                    "next_symbol": "IC2609",
                    "q1_symbol": "IC2606",
                    "q2_symbol": "IC2609",
                },
                "csi500_term_structure_shadow_signal": {
                    "rule_name": "M1-M2前端塌陷>=2.05% (14:30前)",
                    "candidate": True,
                    "before_cutoff": True,
                    "front_end_gap_pct": 2.4,
                    "q1_q2_annualized_pct": -6.0,
                    "anchor_stable": True,
                    "reasons": ["M1-M2前端塌陷达到影子阈值"],
                },
                "csi500_option_proxy": {
                    "otm_put_trade_code": "510500P2606M07750",
                    "otm_put_price": 0.1234,
                    "otm_put_ask1": 0.1266,
                    "otm_put_bid1": 0.1210,
                    "otm_put_last_price": 0.1234,
                    "otm_put_quote_time": "2026-04-27 14:09:58",
                    "otm_put_days_to_expiry": 31,
                    "expiry_days_to_expiry": 31,
                    "roll_window_shifted": True,
                    "otm_put_price_source": "ask1",
                    "otm_put_strike": 7.75,
                    "expiry_ym": "2606",
                    "source": "akshare_public",
                },
            },
            {
                "report_date": "2026-04-27",
                "captured_at": "2026-04-27T14:40:00",
                "csi500_term_structure": {},
                "csi500_term_structure_shadow_signal": {
                    "rule_name": "M1-M2前端塌陷>=2.05% (14:30前)",
                    "candidate": False,
                },
            },
        ]
        events = build_term_structure_shadow_events(snapshots)
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["paper_trade_reference_trade_code"], "510500P2606M07750")
        self.assertEqual(events[0]["paper_trade_reference_price"], 0.1266)
        self.assertEqual(events[0]["paper_trade_cost_proxy_type"], "otm_put_snapshot_ask1")
        self.assertTrue(events[0]["paper_trade_roll_window_shifted"])
        self.assertFalse(events[0]["dividend_season_proxy"])

    def test_enrich_term_structure_shadow_events_adds_clusters_payout_and_lead_time(self) -> None:
        snapshots = [
            {
                "report_date": "2026-04-27",
                "captured_at": "2026-04-27T14:10:00",
                "csi500_term_structure_shadow_signal": {
                    "rule_name": "M1-M2前端塌陷>=2.05% (14:30前)",
                    "candidate": True,
                    "before_cutoff": True,
                    "front_end_gap_pct": 2.4,
                    "q1_q2_annualized_pct": -6.0,
                    "anchor_stable": True,
                    "reasons": ["hit"],
                },
                "csi500_term_structure": {
                    "near_symbol": "IC2606",
                    "next_symbol": "IC2609",
                    "q1_symbol": "IC2606",
                    "q2_symbol": "IC2609",
                },
                "csi500_option_proxy": {
                    "otm_put_trade_code": "510500P2606M07750",
                    "otm_put_price": 0.12,
                    "otm_put_strike": 7.75,
                    "expiry_ym": "2606",
                    "source": "akshare_public",
                },
            },
            {
                "report_date": "2026-04-29",
                "captured_at": "2026-04-29T10:00:00",
                "csi500_option_proxy": {
                    "otm_put_trade_code": "510500P2606M07750",
                    "otm_put_price": 0.32,
                },
            },
        ]
        events = build_term_structure_shadow_events(snapshots)
        enriched = enrich_term_structure_shadow_events(
            events,
            snapshots,
            daily_context_by_date={
                "2026-04-27": {
                    "dividend_season": False,
                    "slow_bear_proxy": True,
                    "t1_spot_ret": -0.01,
                    "t3_spot_ret": -0.03,
                    "t5_spot_ret": -0.05,
                    "t1_carry_delta": 0.01,
                    "t3_carry_delta": 0.02,
                    "t5_carry_delta": 0.03,
                },
                "2026-04-28": {"second_confirmation": False},
                "2026-04-29": {"second_confirmation": True},
            },
            second_confirmation_dates=["2026-04-29"],
        )
        self.assertEqual(len(enriched), 1)
        event = enriched[0]
        self.assertEqual(event["event_cluster_id"], "m1m2-shadow-0001")
        self.assertEqual(event["paper_trade_max_payout_proxy"], 0.32)
        self.assertAlmostEqual(event["paper_trade_pnl_proxy"], 0.20, places=6)
        self.assertAlmostEqual(event["paper_trade_return_proxy"], (0.32 / 0.12) - 1.0, places=6)
        self.assertEqual(event["lead_time_trading_days"], 2)
        self.assertTrue(event["slow_bear_proxy"])
        self.assertAlmostEqual(event["t5_spot_ret"], -0.05, places=6)

    def test_load_write_and_render_summary(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            archive = root / "intraday"
            archive.mkdir()
            snapshot_path = archive / "2026-04-27_market_snapshots.jsonl"
            rows = [
                {
                    "captured_at": "2026-04-27T14:10:00",
                    "csi500_term_structure": {
                        "front_end_gap_pct": 2.4,
                        "q1_q2_annualized_pct": -6.0,
                    },
                    "csi500_term_structure_shadow_signal": {
                        "candidate": True,
                        "rule_name": "M1-M2前端塌陷>=2.05% (14:30前)",
                        "before_cutoff": True,
                        "front_end_gap_pct": 2.4,
                        "q1_q2_annualized_pct": -6.0,
                        "anchor_stable": True,
                        "reasons": ["hit"],
                    },
                }
            ]
            snapshot_path.write_text("\n".join(json.dumps(row, ensure_ascii=False) for row in rows) + "\n", encoding="utf-8")
            snapshots = load_market_snapshots(archive)
            events = enrich_term_structure_shadow_events(
                build_term_structure_shadow_events(snapshots),
                snapshots,
                daily_context_by_date={"2026-04-27": {"t5_spot_ret": -0.03}},
                second_confirmation_dates=[],
            )
            summary = summarize_term_structure_shadow_monitoring(snapshots, events)
            text = render_term_structure_shadow_monitoring_summary(summary, events)
            ledger_path = root / "ledger.jsonl"
            write_term_structure_shadow_events(ledger_path, events)

            self.assertEqual(summary.candidate_count, 1)
            self.assertEqual(summary.event_cluster_count, 1)
            self.assertIn("shadow_monitoring", text)
            self.assertIn("Lead Time", text)
            self.assertIn("ask1=", text)
            self.assertIn("bid1=", text)
            self.assertIn("dte=", text)
            self.assertIn("roll_shift=", text)
            self.assertTrue(ledger_path.exists())

    def test_ic_shadow_events_are_written_as_p3_routed_signals(self) -> None:
        event = {
            "event_key": "2026-04-27::2026-04-27T14:10:00",
            "report_date": "2026-04-27",
            "captured_at": "2026-04-27T14:10:00",
            "rule_name": "M1-M2前端塌陷>=2.05% (14:30前)",
            "front_end_gap_pct": 2.4,
            "q1_q2_annualized_pct": -6.0,
            "paper_trade_reference_trade_code": "510500P2606M07750",
            "paper_trade_cost_proxy": 0.12,
            "paper_trade_max_payout_proxy": 0.32,
            "lead_time_trading_days": 2,
            "t5_spot_ret": -0.05,
        }

        signal = build_ic_shadow_signal_event(event)

        self.assertEqual(signal.priority, "P3")
        self.assertEqual(signal.category, "shadow")
        self.assertFalse(signal.should_notify)
        self.assertEqual(signal.dedupe_key, "2026-04-27::2026-04-27T14:10:00")

        with tempfile.TemporaryDirectory() as tmpdir:
            target = Path(tmpdir) / "ic_shadow_events.jsonl"
            write_ic_shadow_signal_events(target, [event])
            payload = json.loads(target.read_text(encoding="utf-8").splitlines()[0])

        self.assertEqual(payload["event"]["priority"], "P3")
        self.assertFalse(payload["decision"]["should_notify"])
        self.assertTrue(payload["decision"]["should_archive"])


if __name__ == "__main__":
    unittest.main()
