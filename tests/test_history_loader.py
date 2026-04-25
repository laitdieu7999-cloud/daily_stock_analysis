from datetime import date
from types import SimpleNamespace

import pandas as pd

from src.services.history_loader import (
    get_frozen_target_date,
    load_history_df,
    reset_frozen_target_date,
    set_frozen_target_date,
)


def test_frozen_target_date_roundtrip():
    token = set_frozen_target_date(date(2026, 4, 25))
    try:
        assert get_frozen_target_date() == date(2026, 4, 25)
    finally:
        reset_frozen_target_date(token)
    assert get_frozen_target_date() is None


def test_load_history_df_prefers_db(monkeypatch):
    bars = [
        SimpleNamespace(
            date=date(2026, 4, 25),
            to_dict=lambda: {"date": "2026-04-25", "close": 10.0, "code": "600519"}
        )
        for _ in range(30)
    ]

    class _FakeDb:
        def get_data_range(self, code, start, end):
            assert code == "600519"
            return bars

    monkeypatch.setattr("src.storage.get_db", lambda: _FakeDb())
    monkeypatch.setattr(
        "src.services.history_loader._get_fetcher_manager",
        lambda: (_ for _ in ()).throw(AssertionError("fetcher should not be used")),
    )

    df, source = load_history_df("600519", days=60, target_date=date(2026, 4, 25))

    assert source == "db_cache"
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 30
