from __future__ import annotations

import unittest
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory

from src.services.signal_router import SignalEvent, SignalRouter, append_signal_event_archive


class _FakeNotifier:
    def __init__(self) -> None:
        self.messages: list[str] = []

    def send(self, content: str) -> bool:
        self.messages.append(content)
        return True


class SignalRouterTestCase(unittest.TestCase):
    def test_signal_event_round_trip_keeps_contract_fields(self) -> None:
        event = SignalEvent(
            source="holding_risk",
            symbol="600519",
            name="贵州茅台",
            priority="P1",
            category="holding",
            action="risk_alert",
            title="持仓风控触发",
            content="跌破MA20且放量走弱",
            reason="break_ma20",
            should_notify=True,
            channels=["feishu", "desktop"],
            dedupe_key="holding_risk:600519:break_ma20",
            created_at="2026-04-28T14:35:00",
        )

        restored = SignalEvent.from_dict(event.to_dict())

        self.assertEqual(restored.priority, "P1")
        self.assertEqual(restored.symbol, "600519")
        self.assertEqual(restored.channels, ["feishu", "desktop"])
        self.assertEqual(restored.dedupe_key, "holding_risk:600519:break_ma20")

    def test_p1_holding_risk_must_notify_even_if_source_forgets_flag(self) -> None:
        event = SignalEvent(
            source="holding_risk",
            priority="P1",
            category="holding",
            action="risk_alert",
            title="持仓风控触发",
            content="跌破止损线",
        )

        decision = SignalRouter().route(event)

        self.assertTrue(decision.should_notify)
        self.assertEqual(decision.alert_type, "warning")
        self.assertEqual(decision.channels, ["feishu", "desktop"])

    def test_watchlist_p2_only_notifies_buy_signal(self) -> None:
        router = SignalRouter()
        buy = SignalEvent(
            source="stock_intraday",
            priority="P2",
            category="watchlist",
            action="buy",
            title="自选股买入提醒",
            content="进入尾盘击球区",
        )
        hold = SignalEvent(
            source="stock_intraday",
            priority="P2",
            category="watchlist",
            action="hold",
            title="自选股观察",
            content="仅观察",
        )

        self.assertTrue(router.route(buy).should_notify)
        self.assertFalse(router.route(hold).should_notify)

    def test_shadow_and_external_views_archive_without_interrupting(self) -> None:
        event = SignalEvent(
            source="ic_term_structure_shadow",
            priority="P3",
            category="shadow",
            action="record",
            title="M1-M2 Shadow记录",
            content="仅记录不提醒",
        )

        decision = SignalRouter().route(event)

        self.assertFalse(decision.should_notify)
        self.assertTrue(decision.should_archive)

    def test_dispatch_sends_only_when_route_allows(self) -> None:
        router = SignalRouter()
        notifier = _FakeNotifier()
        notify_event = SignalEvent(
            source="stock_intraday",
            priority="P2",
            category="watchlist",
            action="buy",
            title="自选股买入提醒",
            content="进入尾盘击球区",
        )
        silent_event = SignalEvent(
            source="gemini_compare",
            priority="P4",
            category="external_view",
            action="archive",
            title="Gemini观点归档",
            content="只归档",
        )

        notify_result = router.dispatch(notify_event, notifier)
        silent_result = router.dispatch(silent_event, notifier)

        self.assertTrue(notify_result["sent"])
        self.assertFalse(silent_result["sent"])
        self.assertEqual(len(notifier.messages), 1)
        self.assertIn("自选股买入提醒", notifier.messages[0])

    def test_append_signal_event_archive_writes_routed_jsonl(self) -> None:
        event = SignalEvent(
            source="gemini_compare",
            priority="P4",
            category="external_view",
            action="archive",
            title="Gemini观点归档",
            content="只归档",
        )

        with TemporaryDirectory() as tmpdir:
            archive_path = Path(tmpdir) / "signals.jsonl"
            result = append_signal_event_archive(event, archive_path=archive_path)

            self.assertEqual(result["archive_path"], str(archive_path))
            self.assertTrue(archive_path.exists())
            payload = archive_path.read_text(encoding="utf-8")
            self.assertIn("gemini_compare", payload)
            self.assertIn('"should_notify": false', payload)

    def test_p1_delivery_policy_cools_down_same_scope(self) -> None:
        with TemporaryDirectory() as tmpdir:
            state_path = Path(tmpdir) / "route_state.json"
            notifier = _FakeNotifier()
            event = SignalEvent(
                source="stock_intraday_reminder",
                priority="P1",
                category="holding",
                action="risk_alert",
                title="持仓风控触发",
                content="多只持仓触发风险",
                metadata={"scope": "stock_list_buy_sell"},
            )

            first = SignalRouter(
                state_path=state_path,
                now=datetime(2026, 4, 27, 10, 0),
                p1_cooldown_minutes=30,
            ).dispatch(event, notifier)
            second = SignalRouter(
                state_path=state_path,
                now=datetime(2026, 4, 27, 10, 10),
                p1_cooldown_minutes=30,
            ).dispatch(event, notifier)

            self.assertTrue(first["sent"])
            self.assertFalse(second["sent"])
            self.assertIn("cooldown", second["decision"]["reason"])
            self.assertEqual(len(notifier.messages), 1)

    def test_p2_delivery_policy_limits_same_symbol_once_per_day(self) -> None:
        with TemporaryDirectory() as tmpdir:
            state_path = Path(tmpdir) / "route_state.json"
            notifier = _FakeNotifier()
            event = SignalEvent(
                source="stock_intraday_reminder",
                symbol="600529",
                priority="P2",
                category="watchlist",
                action="buy",
                title="自选股买入提醒",
                content="进入尾盘击球区",
            )

            first = SignalRouter(
                state_path=state_path,
                now=datetime(2026, 4, 27, 14, 35),
                p2_daily_limit=1,
            ).dispatch(event, notifier)
            second = SignalRouter(
                state_path=state_path,
                now=datetime(2026, 4, 27, 14, 50),
                p2_daily_limit=1,
            ).dispatch(event, notifier)

            self.assertTrue(first["sent"])
            self.assertFalse(second["sent"])
            self.assertIn("daily limit", second["decision"]["reason"])
            self.assertEqual(len(notifier.messages), 1)


if __name__ == "__main__":
    unittest.main()
