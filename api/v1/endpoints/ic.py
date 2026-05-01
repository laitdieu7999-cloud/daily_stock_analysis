# -*- coding: utf-8 -*-
"""
===================================
IC 行情快照接口
===================================

职责：
1. 提供 IC 现货 + 多合约期限结构快照
2. 为前端 IC 页面提供自动化数据源
"""

from dataclasses import asdict

from fastapi import APIRouter, HTTPException

from src.config import Config
from src.market_data_fetcher import MarketDataFetcher

router = APIRouter()


@router.get("/snapshot")
async def get_ic_snapshot() -> dict:
    """返回 IC 现货与多合约快照。"""
    config = Config.get_instance()
    fetcher = MarketDataFetcher(
        getattr(config, "jin10_api_key", ""),
        getattr(config, "jin10_x_token", ""),
    )
    snapshot = fetcher.get_ic_market_snapshot()
    if snapshot is None:
        raise HTTPException(status_code=503, detail="IC 实时快照获取失败")
    return asdict(snapshot)
