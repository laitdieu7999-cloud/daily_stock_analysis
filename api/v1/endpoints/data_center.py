# -*- coding: utf-8 -*-
"""Data center endpoints."""

from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, HTTPException

from api.deps import get_database_manager
from api.v1.schemas.common import ErrorResponse
from api.v1.schemas.data_center import (
    DataCenterCleanupRequest,
    DataCenterCleanupResponse,
    DataCenterMarketDataRefreshRequest,
    DataCenterMarketDataRefreshResponse,
    DataCenterOverviewResponse,
    DataCenterPortfolioBacktestRequest,
    DataCenterPortfolioBacktestResponse,
    DataCenterPortfolioReviewRequest,
    DataCenterPortfolioReviewResponse,
    DataCenterPortfolioRiskRadarResponse,
)
from src.services.data_center_service import DataCenterService
from src.services.market_data_warehouse_service import MarketDataWarehouseService
from src.services.portfolio_daily_review_service import PortfolioDailyReviewService
from src.services.workstation_cleanup_service import WorkstationCleanupService
from src.storage import DatabaseManager

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get(
    "/overview",
    response_model=DataCenterOverviewResponse,
    responses={
        200: {"description": "本地金融数据中心概览"},
        500: {"description": "服务器错误", "model": ErrorResponse},
    },
    summary="获取本地金融数据中心概览",
)
def get_data_center_overview(
    db_manager: DatabaseManager = Depends(get_database_manager),
) -> DataCenterOverviewResponse:
    try:
        service = DataCenterService(db_manager)
        return DataCenterOverviewResponse(**service.build_overview())
    except Exception as exc:
        logger.error("获取本地数据中心概览失败: %s", exc, exc_info=True)
        raise HTTPException(
            status_code=500,
            detail={"error": "internal_error", "message": f"获取本地数据中心概览失败: {exc}"},
        )


@router.post(
    "/market-data-refresh",
    response_model=DataCenterMarketDataRefreshResponse,
    responses={
        200: {"description": "持仓和自选行情刷新完成"},
        500: {"description": "服务器错误", "model": ErrorResponse},
    },
    summary="补齐持仓和自选历史行情",
)
def refresh_market_data(
    request: DataCenterMarketDataRefreshRequest,
    db_manager: DatabaseManager = Depends(get_database_manager),
) -> DataCenterMarketDataRefreshResponse:
    service = None
    try:
        service = MarketDataWarehouseService(db_manager)
        return DataCenterMarketDataRefreshResponse(
            **service.run_refresh(
                force=request.force,
                lookback_days=request.lookback_days,
                refresh_overlap_days=request.refresh_overlap_days,
                max_symbols=request.max_symbols,
            )
        )
    except Exception as exc:
        logger.error("本地行情数据补齐失败: %s", exc, exc_info=True)
        raise HTTPException(
            status_code=500,
            detail={"error": "internal_error", "message": f"本地行情数据补齐失败: {exc}"},
        )
    finally:
        if service is not None:
            service.close()


@router.post(
    "/portfolio-backtest",
    response_model=DataCenterPortfolioBacktestResponse,
    responses={
        200: {"description": "持仓股回测执行完成"},
        500: {"description": "服务器错误", "model": ErrorResponse},
    },
    summary="对当前持仓股触发回测",
)
def run_portfolio_backtests(
    request: DataCenterPortfolioBacktestRequest,
    db_manager: DatabaseManager = Depends(get_database_manager),
) -> DataCenterPortfolioBacktestResponse:
    try:
        service = DataCenterService(db_manager)
        return DataCenterPortfolioBacktestResponse(
            **service.run_portfolio_backtests(
                force=request.force,
                eval_window_days=request.eval_window_days,
                min_age_days=request.min_age_days,
                limit_per_symbol=request.limit_per_symbol,
            )
        )
    except Exception as exc:
        logger.error("持仓股回测失败: %s", exc, exc_info=True)
        raise HTTPException(
            status_code=500,
            detail={"error": "internal_error", "message": f"持仓股回测失败: {exc}"},
        )


@router.post(
    "/portfolio-daily-review",
    response_model=DataCenterPortfolioReviewResponse,
    responses={
        200: {"description": "每日持仓复盘生成完成"},
        500: {"description": "服务器错误", "model": ErrorResponse},
    },
    summary="生成每日持仓复盘",
)
def run_portfolio_daily_review(
    request: DataCenterPortfolioReviewRequest,
    db_manager: DatabaseManager = Depends(get_database_manager),
) -> DataCenterPortfolioReviewResponse:
    try:
        service = PortfolioDailyReviewService(db_manager)
        return DataCenterPortfolioReviewResponse(
            **service.run(
                run_backtests=request.run_backtests,
                send_notification=request.send_notification,
            )
        )
    except Exception as exc:
        logger.error("每日持仓复盘生成失败: %s", exc, exc_info=True)
        raise HTTPException(
            status_code=500,
            detail={"error": "internal_error", "message": f"每日持仓复盘生成失败: {exc}"},
        )


@router.post(
    "/maintenance-cleanup",
    response_model=DataCenterCleanupResponse,
    responses={
        200: {"description": "本地日志和缓存清理完成"},
        500: {"description": "服务器错误", "model": ErrorResponse},
    },
    summary="清理旧日志和临时缓存",
)
def run_maintenance_cleanup(
    request: DataCenterCleanupRequest,
    db_manager: DatabaseManager = Depends(get_database_manager),
) -> DataCenterCleanupResponse:
    try:
        project_root = DataCenterService(db_manager)._project_root()
        service = WorkstationCleanupService(project_root=project_root)
        return DataCenterCleanupResponse(**service.run(dry_run=request.dry_run))
    except Exception as exc:
        logger.error("本地日志和缓存清理失败: %s", exc, exc_info=True)
        raise HTTPException(
            status_code=500,
            detail={"error": "internal_error", "message": f"本地日志和缓存清理失败: {exc}"},
        )


@router.get(
    "/portfolio-risk-radar",
    response_model=DataCenterPortfolioRiskRadarResponse,
    responses={
        200: {"description": "当前持仓风险雷达"},
        500: {"description": "服务器错误", "model": ErrorResponse},
    },
    summary="获取当前持仓风险雷达",
)
def get_portfolio_risk_radar(
    db_manager: DatabaseManager = Depends(get_database_manager),
) -> DataCenterPortfolioRiskRadarResponse:
    try:
        service = DataCenterService(db_manager)
        return DataCenterPortfolioRiskRadarResponse(**service.get_portfolio_risk_radar())
    except Exception as exc:
        logger.error("获取持仓风险雷达失败: %s", exc, exc_info=True)
        raise HTTPException(
            status_code=500,
            detail={"error": "internal_error", "message": f"获取持仓风险雷达失败: {exc}"},
        )
