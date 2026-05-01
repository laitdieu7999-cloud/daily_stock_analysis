# -*- coding: utf-8 -*-
from unittest.mock import patch

from src.core.pipeline import StockAnalysisPipeline


def test_build_portfolio_position_context_matches_code_suffix_and_aggregates() -> None:
    pipeline = StockAnalysisPipeline.__new__(StockAnalysisPipeline)
    snapshot = {
        "as_of": "2026-04-28T10:00:00",
        "cost_method": "fifo",
        "currency": "CNY",
        "total_equity": 2000.0,
        "accounts": [
            {
                "account_id": 1,
                "account_name": "主账户",
                "base_currency": "CNY",
                "positions": [
                    {
                        "symbol": "159326",
                        "quantity": 100,
                        "avg_cost": 1.2,
                        "last_price": 1.5,
                        "market_value_base": 150.0,
                        "unrealized_pnl_base": 30.0,
                        "valuation_currency": "CNY",
                    },
                    {
                        "symbol": "000001",
                        "quantity": 50,
                        "avg_cost": 10.0,
                        "last_price": 9.0,
                        "market_value_base": 450.0,
                        "unrealized_pnl_base": -50.0,
                    },
                ],
            },
            {
                "account_id": 2,
                "account_name": "备用账户",
                "base_currency": "CNY",
                "positions": [
                    {
                        "symbol": "SZ159326",
                        "quantity": 200,
                        "avg_cost": 1.1,
                        "last_price": 1.5,
                        "market_value_base": 300.0,
                        "unrealized_pnl_base": 80.0,
                    },
                ],
            },
        ],
    }

    with patch("src.services.portfolio_service.PortfolioService") as service_cls:
        service_cls.return_value.get_portfolio_snapshot.return_value = snapshot
        context = pipeline._build_portfolio_position_context("159326.SZ")

    assert context is not None
    assert context["has_position"] is True
    assert context["stock_code"] == "159326"
    assert context["quantity"] == 300
    assert context["avg_cost"] == 1.1333
    assert context["market_value"] == 450
    assert context["unrealized_pnl"] == 110
    assert context["unrealized_pnl_pct"] == 32.35
    assert context["weight_pct"] == 22.5
    assert context["account_count"] == 2


def test_build_portfolio_position_context_returns_none_without_match() -> None:
    pipeline = StockAnalysisPipeline.__new__(StockAnalysisPipeline)
    snapshot = {
        "total_equity": 1000.0,
        "accounts": [
            {
                "positions": [
                    {
                        "symbol": "000001",
                        "quantity": 100,
                        "market_value_base": 1000,
                        "unrealized_pnl_base": 0,
                    }
                ]
            }
        ],
    }

    with patch("src.services.portfolio_service.PortfolioService") as service_cls:
        service_cls.return_value.get_portfolio_snapshot.return_value = snapshot
        assert pipeline._build_portfolio_position_context("159326") is None


def test_build_portfolio_position_context_fails_open_on_portfolio_error() -> None:
    pipeline = StockAnalysisPipeline.__new__(StockAnalysisPipeline)

    with patch("src.services.portfolio_service.PortfolioService") as service_cls:
        service_cls.return_value.get_portfolio_snapshot.side_effect = RuntimeError("db locked")
        assert pipeline._build_portfolio_position_context("159326") is None
