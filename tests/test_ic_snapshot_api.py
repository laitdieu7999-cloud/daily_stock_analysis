# -*- coding: utf-8 -*-

from __future__ import annotations

import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from fastapi.testclient import TestClient

try:
    import litellm  # noqa: F401
except ModuleNotFoundError:
    sys.modules["litellm"] = MagicMock()

import src.auth as auth
from api.app import create_app
from src.config import Config
from src.market_data_fetcher import ETFOptionProxyData, ICContractSnapshot, ICMarketSnapshotData
from src.storage import DatabaseManager


def _reset_auth_globals() -> None:
    auth._auth_enabled = None
    auth._session_secret = None
    auth._password_hash_salt = None
    auth._password_hash_stored = None
    auth._rate_limit = {}


class ICSnapshotApiTestCase(unittest.TestCase):
    def setUp(self) -> None:
        _reset_auth_globals()
        self.temp_dir = tempfile.TemporaryDirectory()
        self.data_dir = Path(self.temp_dir.name)
        self.env_path = self.data_dir / ".env"
        self.db_path = self.data_dir / "ic_api_test.db"
        self.env_path.write_text(
            "\n".join(
                [
                    "STOCK_LIST=600519",
                    "ADMIN_AUTH_ENABLED=false",
                    f"DATABASE_PATH={self.db_path}",
                ]
            )
            + "\n",
            encoding="utf-8",
        )

        os.environ["ENV_FILE"] = str(self.env_path)
        os.environ["DATABASE_PATH"] = str(self.db_path)
        Config.reset_instance()
        DatabaseManager.reset_instance()
        self.client = TestClient(create_app(static_dir=self.data_dir / "empty-static"))

    def tearDown(self) -> None:
        DatabaseManager.reset_instance()
        Config.reset_instance()
        os.environ.pop("ENV_FILE", None)
        os.environ.pop("DATABASE_PATH", None)
        self.temp_dir.cleanup()

    def test_ic_snapshot_returns_contract_rows(self) -> None:
        payload = ICMarketSnapshotData(
            spot_price=5800.0,
            main_contract_code="IC2505",
            fetched_at="2026-04-27T14:00:00",
            option_proxy=ETFOptionProxyData(
                board_timestamp="2026-04-27",
                expiry_ym="2506",
                expiry_style="M",
                qvix_latest=21.23,
                qvix_prev=20.5,
                qvix_jump_pct=3.56,
                qvix_zscore=1.2,
                atm_strike=5.8,
                atm_call_trade_code="MO2506-C-5800",
                atm_call_price=0.18,
                atm_put_trade_code="MO2506-P-5800",
                atm_put_price=0.12,
                otm_put_trade_code="MO2506-P-5500",
                otm_put_strike=5.5,
                otm_put_price=0.05,
                put_skew_ratio=0.42,
                atm_put_call_volume_ratio=1.58,
                expiry_days_to_expiry=44,
            ),
            contracts=[
                ICContractSnapshot(
                    symbol="IC2505",
                    price=5700.0,
                    expiry_date="2026-05-15",
                    days_to_expiry=30,
                    term_gap_days=0,
                    basis=100.0,
                    annualized_basis_pct=21.35,
                    is_main=True,
                ),
                ICContractSnapshot(
                    symbol="IC2506",
                    price=5680.0,
                    expiry_date="2026-06-19",
                    days_to_expiry=65,
                    term_gap_days=35,
                    basis=120.0,
                    annualized_basis_pct=11.85,
                    is_main=False,
                ),
            ],
        )

        with patch(
            "api.v1.endpoints.ic.MarketDataFetcher.get_ic_market_snapshot",
            return_value=payload,
        ):
            response = self.client.get("/api/v1/ic/snapshot")

        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(body["spot_price"], 5800.0)
        self.assertEqual(body["main_contract_code"], "IC2505")
        self.assertEqual(len(body["contracts"]), 2)
        self.assertEqual(body["contracts"][0]["symbol"], "IC2505")
        self.assertEqual(body["contracts"][1]["term_gap_days"], 35)
        self.assertEqual(body["option_proxy"]["expiry_ym"], "2506")
        self.assertEqual(body["option_proxy"]["qvix_latest"], 21.23)
        self.assertEqual(body["option_proxy"]["atm_put_call_volume_ratio"], 1.58)

    def test_ic_snapshot_returns_503_when_fetch_fails(self) -> None:
        with patch(
            "api.v1.endpoints.ic.MarketDataFetcher.get_ic_market_snapshot",
            return_value=None,
        ):
            response = self.client.get("/api/v1/ic/snapshot")

        self.assertEqual(response.status_code, 503)
        body = response.json()
        self.assertEqual(body.get("error"), "http_error")
