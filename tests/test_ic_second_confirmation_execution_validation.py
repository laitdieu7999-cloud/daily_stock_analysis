import unittest

import pandas as pd

from scripts.run_ic_second_confirmation_execution_validation import _apply_execution_variant


class ICSecondConfirmationExecutionValidationTest(unittest.TestCase):
    def test_apply_execution_variant_keeps_small_floor(self) -> None:
        base_signal = pd.Series([0.5, 0.5, 0.5], index=pd.RangeIndex(3))
        trigger = pd.Series([False, True, False], index=base_signal.index)

        signal = _apply_execution_variant(base_signal, trigger, "第二确认保留0.10底仓")

        self.assertEqual(signal.tolist(), [0.5, 0.1, 0.5])

    def test_apply_execution_variant_cooldown_zero_two_days(self) -> None:
        base_signal = pd.Series([0.5, 0.5, 0.5, 0.5], index=pd.RangeIndex(4))
        trigger = pd.Series([False, True, False, False], index=base_signal.index)

        signal = _apply_execution_variant(base_signal, trigger, "第二确认空仓两日")

        self.assertEqual(signal.tolist(), [0.5, 0.0, 0.0, 0.5])

    def test_apply_execution_variant_staged_then_zero_on_consecutive_trigger(self) -> None:
        base_signal = pd.Series([0.5, 0.5, 0.5, 0.5], index=pd.RangeIndex(4))
        trigger = pd.Series([False, True, True, False], index=base_signal.index)

        signal = _apply_execution_variant(base_signal, trigger, "第二确认首日0.25/连续再清零")

        self.assertEqual(signal.tolist(), [0.5, 0.25, 0.0, 0.5])
