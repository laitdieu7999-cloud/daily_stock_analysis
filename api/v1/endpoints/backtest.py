# -*- coding: utf-8 -*-
"""Backtest endpoints."""

from __future__ import annotations

import logging
from datetime import date
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query

from api.deps import get_database_manager
from api.v1.schemas.backtest import (
    BacktestScanConclusion,
    BacktestRunRequest,
    BacktestRunResponse,
    BacktestScanItem,
    BacktestScanRequest,
    BacktestScanResponse,
    BacktestResultItem,
    BacktestResultsResponse,
    PerformanceMetrics,
    ShadowDashboardResponse,
)
from api.v1.schemas.common import ErrorResponse
from src.services.backtest_service import BacktestService
from src.services.shadow_dashboard_service import ShadowDashboardService
from src.storage import DatabaseManager

logger = logging.getLogger(__name__)

router = APIRouter()


def _validate_analysis_date_range(
    analysis_date_from: Optional[date],
    analysis_date_to: Optional[date],
) -> None:
    if analysis_date_from and analysis_date_to and analysis_date_from > analysis_date_to:
        raise HTTPException(
            status_code=400,
            detail={
                "error": "invalid_params",
                "message": "analysis_date_from cannot be after analysis_date_to",
            },
        )


@router.post(
    "/run",
    response_model=BacktestRunResponse,
    responses={
        200: {"description": "回测执行完成"},
        500: {"description": "服务器错误", "model": ErrorResponse},
    },
    summary="触发回测",
    description="对历史分析记录进行回测评估，并写入 backtest_results/backtest_summaries",
)
def run_backtest(
    request: BacktestRunRequest,
    db_manager: DatabaseManager = Depends(get_database_manager),
) -> BacktestRunResponse:
    try:
        service = BacktestService(db_manager)
        stats = service.run_backtest(
            code=request.code,
            force=request.force,
            eval_window_days=request.eval_window_days,
            min_age_days=request.min_age_days,
            limit=request.limit,
            score_threshold=request.score_threshold,
            top_n=request.top_n,
        )
        return BacktestRunResponse(**stats)
    except Exception as exc:
        logger.error(f"回测执行失败: {exc}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail={"error": "internal_error", "message": f"回测执行失败: {str(exc)}"},
        )


@router.post(
    "/scan",
    response_model=BacktestScanResponse,
    responses={
        200: {"description": "参数扫描完成"},
        500: {"description": "服务器错误", "model": ErrorResponse},
    },
    summary="扫描回测参数组合",
    description="对 score_threshold / top_n / eval_window_days 组合做横向比较，不写入 backtest_results/backtest_summaries。",
)
def scan_backtest_grid(
    request: BacktestScanRequest,
    db_manager: DatabaseManager = Depends(get_database_manager),
) -> BacktestScanResponse:
    try:
        service = BacktestService(db_manager)
        data = service.scan_parameter_grid(
            code=request.code,
            min_age_days=request.min_age_days,
            limit=request.limit,
            local_data_only=request.local_data_only,
            eval_window_days_options=request.eval_window_days_options,
            score_threshold_options=request.score_threshold_options,
            top_n_options=request.top_n_options,
        )
        return BacktestScanResponse(
            raw_candidate_count=int(data.get("raw_candidate_count", 0)),
            ranked_candidate_count=int(data.get("ranked_candidate_count", 0)),
            local_data_only=bool(data.get("local_data_only", False)),
            best_by_return=BacktestScanItem(**data["best_by_return"]) if data.get("best_by_return") else None,
            best_by_win_rate=BacktestScanItem(**data["best_by_win_rate"]) if data.get("best_by_win_rate") else None,
            conclusion=BacktestScanConclusion(**data["conclusion"]) if data.get("conclusion") else None,
            scans=[BacktestScanItem(**item) for item in data.get("scans", [])],
        )
    except Exception as exc:
        logger.error(f"回测参数扫描失败: {exc}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail={"error": "internal_error", "message": f"回测参数扫描失败: {str(exc)}"},
        )


@router.get(
    "/results",
    response_model=BacktestResultsResponse,
    responses={
        200: {"description": "回测结果列表"},
        500: {"description": "服务器错误", "model": ErrorResponse},
    },
    summary="获取回测结果",
    description="分页获取回测结果，支持按股票代码过滤",
)
def get_backtest_results(
    code: Optional[str] = Query(None, description="股票代码筛选"),
    eval_window_days: Optional[int] = Query(None, ge=1, le=120, description="评估窗口过滤"),
    analysis_date_from: Optional[date] = Query(None, description="分析日期起始（含）"),
    analysis_date_to: Optional[date] = Query(None, description="分析日期结束（含）"),
    page: int = Query(1, ge=1, description="页码"),
    limit: int = Query(20, ge=1, le=200, description="每页数量"),
    db_manager: DatabaseManager = Depends(get_database_manager),
) -> BacktestResultsResponse:
    try:
        _validate_analysis_date_range(analysis_date_from, analysis_date_to)
        service = BacktestService(db_manager)
        data = service.get_recent_evaluations(
            code=code,
            eval_window_days=eval_window_days,
            limit=limit,
            page=page,
            analysis_date_from=analysis_date_from,
            analysis_date_to=analysis_date_to,
        )
        items = [BacktestResultItem(**item) for item in data.get("items", [])]
        return BacktestResultsResponse(
            total=int(data.get("total", 0)),
            page=page,
            limit=limit,
            items=items,
        )
    except HTTPException:
        raise
    except Exception as exc:
        logger.error(f"查询回测结果失败: {exc}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail={"error": "internal_error", "message": f"查询回测结果失败: {str(exc)}"},
        )


@router.get(
    "/performance",
    response_model=PerformanceMetrics,
    responses={
        200: {"description": "整体回测表现"},
        404: {"description": "无回测汇总", "model": ErrorResponse},
        500: {"description": "服务器错误", "model": ErrorResponse},
    },
    summary="获取整体回测表现",
)
def get_overall_performance(
    eval_window_days: Optional[int] = Query(None, ge=1, le=120, description="评估窗口过滤"),
    analysis_date_from: Optional[date] = Query(None, description="分析日期起始（含）"),
    analysis_date_to: Optional[date] = Query(None, description="分析日期结束（含）"),
    db_manager: DatabaseManager = Depends(get_database_manager),
) -> PerformanceMetrics:
    try:
        _validate_analysis_date_range(analysis_date_from, analysis_date_to)
        service = BacktestService(db_manager)
        summary = service.get_summary(
            scope="overall",
            code=None,
            eval_window_days=eval_window_days,
            analysis_date_from=analysis_date_from,
            analysis_date_to=analysis_date_to,
        )
        if summary is None:
            raise HTTPException(
                status_code=404,
                detail={"error": "not_found", "message": "未找到整体回测汇总"},
            )
        return PerformanceMetrics(**summary)
    except ValueError as exc:
        raise HTTPException(
            status_code=400,
            detail={"error": "invalid_params", "message": str(exc)},
        )
    except HTTPException:
        raise
    except Exception as exc:
        logger.error(f"查询整体表现失败: {exc}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail={"error": "internal_error", "message": f"查询整体表现失败: {str(exc)}"},
        )


@router.get(
    "/performance/{code}",
    response_model=PerformanceMetrics,
    responses={
        200: {"description": "单股回测表现"},
        404: {"description": "无回测汇总", "model": ErrorResponse},
        500: {"description": "服务器错误", "model": ErrorResponse},
    },
    summary="获取单股回测表现",
)
def get_stock_performance(
    code: str,
    eval_window_days: Optional[int] = Query(None, ge=1, le=120, description="评估窗口过滤"),
    analysis_date_from: Optional[date] = Query(None, description="分析日期起始（含）"),
    analysis_date_to: Optional[date] = Query(None, description="分析日期结束（含）"),
    db_manager: DatabaseManager = Depends(get_database_manager),
) -> PerformanceMetrics:
    try:
        _validate_analysis_date_range(analysis_date_from, analysis_date_to)
        service = BacktestService(db_manager)
        summary = service.get_summary(
            scope="stock",
            code=code,
            eval_window_days=eval_window_days,
            analysis_date_from=analysis_date_from,
            analysis_date_to=analysis_date_to,
        )
        if summary is None:
            raise HTTPException(
                status_code=404,
                detail={"error": "not_found", "message": f"未找到 {code} 的回测汇总"},
            )
        return PerformanceMetrics(**summary)
    except ValueError as exc:
        raise HTTPException(
            status_code=400,
            detail={"error": "invalid_params", "message": str(exc)},
        )
    except HTTPException:
        raise
    except Exception as exc:
        logger.error(f"查询单股表现失败: {exc}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail={"error": "internal_error", "message": f"查询单股表现失败: {str(exc)}"},
        )


@router.get(
    "/shadow-dashboard",
    response_model=ShadowDashboardResponse,
    responses={
        200: {"description": "Shadow 纸面信号看板"},
        500: {"description": "服务器错误", "model": ErrorResponse},
    },
    summary="获取 Shadow 纸面信号看板",
    description="读取本地理论评分表和 Shadow 纸面账本文件；不触发回测、不推送、不下单。",
)
def get_shadow_dashboard(
    limit: int = Query(50, ge=1, le=200, description="最多返回多少条纸面交易记录"),
) -> ShadowDashboardResponse:
    try:
        payload = ShadowDashboardService().get_dashboard(limit=limit)
        return ShadowDashboardResponse(**payload)
    except Exception as exc:
        logger.error("读取 Shadow 看板失败: %s", exc, exc_info=True)
        raise HTTPException(
            status_code=500,
            detail={"error": "internal_error", "message": f"读取 Shadow 看板失败: {str(exc)}"},
        )
