import unittest

import pandas as pd

from scripts.run_ic_second_confirmation_reentry_search import _apply_reentry_variant


class ICSecondConfirmationReentrySearchTest(unittest.TestCase):
    def test_fixed_two_day_cooldown(self) -> None:
        base_signal = pd.Series([0.5, 0.5, 0.5, 0.5], index=pd.RangeIndex(4))
        trigger = pd.Series([False, True, False, False], index=base_signal.index)
        trend_intact = pd.Series([True, False, True, True], index=base_signal.index)
        high_carry = pd.Series([True, True, True, True], index=base_signal.index)

        signal = _apply_reentry_variant(base_signal, trigger, trend_intact, high_carry, "空仓2日")

        self.assertEqual(signal.tolist(), [0.5, 0.0, 0.0, 0.5])

    def test_waits_for_trend_repair(self) -> None:
        base_signal = pd.Series([0.5, 0.5, 0.5, 0.5, 0.5], index=pd.RangeIndex(5))
        trigger = pd.Series([False, True, False, False, False], index=base_signal.index)
        trend_intact = pd.Series([True, False, False, True, True], index=base_signal.index)
        high_carry = pd.Series([True, True, True, True, True], index=base_signal.index)

        signal = _apply_reentry_variant(base_signal, trigger, trend_intact, high_carry, "空仓至趋势修复")

        self.assertEqual(signal.tolist(), [0.5, 0.0, 0.0, 0.0, 0.5])

    def test_waits_for_trend_and_carry_after_cooldown(self) -> None:
        base_signal = pd.Series([0.5] * 6, index=pd.RangeIndex(6))
        trigger = pd.Series([False, True, False, False, False, False], index=base_signal.index)
        trend_intact = pd.Series([True, False, True, True, True, True], index=base_signal.index)
        high_carry = pd.Series([True, True, False, False, True, True], index=base_signal.index)

        signal = _apply_reentry_variant(
            base_signal,
            trigger,
            trend_intact,
            high_carry,
            "空仓2日后需趋势修复且高贴水",
        )

        self.assertEqual(signal.tolist(), [0.5, 0.0, 0.0, 0.0, 0.0, 0.5])
