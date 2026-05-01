# -*- coding: utf-8 -*-
"""
EventMonitor — lightweight event-driven alert system.

Monitors a set of stocks for threshold events and triggers
notifications when conditions are met.  Designed to run as a
background task (e.g. via ``--schedule`` or a dedicated loop).

Currently supported runtime events:
- Price crossing threshold (above / below)
- Volume spike (> N× average)

Other alert types remain defined as enum placeholders for future
extension, but config validation rejects them until the monitor can
actually evaluate them.

Usage::

    from src.agent.events import EventMonitor, PriceAlert
    monitor = EventMonitor()
    monitor.add_alert(PriceAlert(stock_code="600519", direction="above", price=1800.0))
    triggered = await monitor.check_all()

AI boundary note:
- This file now serves both the original generic alert framework and the local
  extension alerts for gold / silver / IC / Jin10-driven monitoring.
- If a change is specific to gold, silver, IC, or Jin10 news flow, check
  ``src/market_data_fetcher.py`` and ``tests/test_custom_extensions.py`` first.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import time
from dataclasses import dataclass, field
from datetime import date, datetime, time as datetime_time
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

logger = logging.getLogger(__name__)

CN_INTRADAY_MARKET_SESSION = "cn_intraday"


def _is_cn_intraday_session(now: Optional[datetime] = None) -> bool:
    """Return True only during regular A-share continuous trading windows."""
    current_time = now or datetime.now()
    try:
        from src.core.trading_calendar import is_market_open

        if not is_market_open("cn", current_time.date()):
            return False
    except Exception as exc:
        logger.debug("[EventMonitor] CN market calendar check failed: %s", exc)
        return False

    current = current_time.time()
    morning = datetime_time(9, 30) <= current <= datetime_time(11, 30)
    afternoon = datetime_time(13, 0) <= current <= datetime_time(15, 0)
    return morning or afternoon


def _should_skip_for_market_session(rule: "AlertRule") -> bool:
    metadata = rule.metadata if isinstance(rule.metadata, dict) else {}
    market_session = str(metadata.get("market_session") or "").strip().lower()
    if market_session != CN_INTRADAY_MARKET_SESSION:
        return False
    if _is_cn_intraday_session():
        return False
    logger.debug("[EventMonitor] Skip non-session alert: %s", rule.description)
    return True


class AlertType(str, Enum):
    PRICE_CROSS = "price_cross"
    VOLUME_SPIKE = "volume_spike"
    BASIS_SPIKE = "basis_spike"          # IC期货贴水突然加深
    PRICE_SPIKE = "price_spike"          # 金银价格异动（急涨急跌）
    NEWS_IMPACT = "news_impact"          # 重大事件影响持仓/金银/IC
    SENTIMENT_SHIFT = "sentiment_shift"
    RISK_FLAG = "risk_flag"
    CUSTOM = "custom"


class AlertStatus(str, Enum):
    ACTIVE = "active"
    TRIGGERED = "triggered"
    EXPIRED = "expired"
    DISMISSED = "dismissed"


_RUNTIME_SUPPORTED_ALERT_TYPES = frozenset({
    AlertType.PRICE_CROSS,
    AlertType.VOLUME_SPIKE,
    AlertType.BASIS_SPIKE,
    AlertType.PRICE_SPIKE,
    AlertType.NEWS_IMPACT,
})


def _supported_alert_type_names() -> str:
    return ", ".join(sorted(alert_type.value for alert_type in _RUNTIME_SUPPORTED_ALERT_TYPES))


def _ensure_runtime_supported_alert_type(alert_type: AlertType) -> None:
    if alert_type not in _RUNTIME_SUPPORTED_ALERT_TYPES:
        raise ValueError(
            f"unsupported alert_type for current EventMonitor runtime: {alert_type.value} "
            f"(supported: {_supported_alert_type_names()})"
        )


@dataclass
class AlertRule:
    """Base alert rule definition."""
    stock_code: str
    alert_type: AlertType
    description: str = ""
    status: AlertStatus = AlertStatus.ACTIVE
    created_at: float = field(default_factory=time.time)
    triggered_at: Optional[float] = None
    ttl_hours: float = 24.0  # auto-expire after this many hours
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class PriceAlert(AlertRule):
    """Alert when price crosses a threshold."""
    alert_type: AlertType = AlertType.PRICE_CROSS
    direction: str = "above"  # "above" or "below"
    price: float = 0.0

    def __post_init__(self):
        if not self.description:
            self.description = f"{self.stock_code} price {self.direction} {self.price}"


@dataclass
class VolumeAlert(AlertRule):
    """Alert when volume exceeds N× average."""
    alert_type: AlertType = AlertType.VOLUME_SPIKE
    multiplier: float = 2.0  # trigger when volume > multiplier × avg

    def __post_init__(self):
        if not self.description:
            self.description = f"{self.stock_code} volume > {self.multiplier}× average"


@dataclass
class ICBasisAlert(AlertRule):
    """Alert when IC futures annualized basis yield spikes (deep contango).

    Monitors IC futures contracts for:
    - Deep contango: annualized basis yield > deep_threshold (default 10%)
    - Sudden deepening: intraday change > change_threshold (default 2%)

    The annualized basis yield is calculated as:
        (spot_price - futures_price) / futures_price / (days_to_expiry / 365) * 100
    """
    alert_type: AlertType = AlertType.BASIS_SPIKE
    deep_threshold: float = 10.0       # 年化贴水收益率 > 此值触发"深度贴水"
    change_threshold: float = 2.0      # 日内变化 > 此值触发"突然加深"
    contract: str = ""                 # 监控的合约代码，空=主力合约
    # 内部状态：上次检查的年化贴水收益率
    _prev_annualized: float = field(default=0.0, repr=False)

    def __post_init__(self):
        if not self.description:
            self.description = (
                f"IC basis spike: deep>{self.deep_threshold}%, "
                f"change>{self.change_threshold}%"
            )


@dataclass
class PriceSpikeAlert(AlertRule):
    """Alert when gold/silver price moves sharply within a short window.

    Monitors precious metals for rapid price changes that may indicate
    significant market events or trading opportunities.
    """
    alert_type: AlertType = AlertType.PRICE_SPIKE
    change_pct: float = 1.0            # 价格变化百分比阈值（默认1%）
    direction: str = "both"            # "up", "down", "both"
    window_minutes: int = 30           # 检测窗口（分钟）
    # 内部状态
    _prev_price: float = field(default=0.0, repr=False)
    _prev_check_time: float = field(default=0.0, repr=False)

    def __post_init__(self):
        if not self.description:
            dir_label = {"up": "上涨", "down": "下跌", "both": "异动"}.get(self.direction, self.direction)
            self.description = (
                f"{self.stock_code} {dir_label}>{self.change_pct}% "
                f"({self.window_minutes}分钟内)"
            )


@dataclass
class NewsImpactAlert(AlertRule):
    """Alert when a major news event impacts monitored assets.

    Uses Jin10 flash news to detect events related to gold, silver, IC futures,
    or held stocks. Keywords are matched against news headlines.
    """
    alert_type: AlertType = AlertType.NEWS_IMPACT
    keywords: List[str] = field(default_factory=lambda: [
        # 金银相关
        "黄金", "白银", "金价", "银价", "XAUUSD", "XAGUSD",
        "美联储", "Fed", "利率", "降息", "加息", "通胀", "CPI", "PCE",
        "避险", "地缘", "战争", "冲突", "制裁",
        # IC/中证500相关
        "中证500", "IC期货", "股指期货", "A股", "沪深300",
        "央行", "MLF", "LPR", "降准", "印花税",
        # 通用重大事件
        "非农", "就业", "GDP", "PMI", "贸易战", "关税",
    ])
    # 内部状态：上次检查的最新快讯ID
    _last_flash_id: Optional[str] = field(default=None, repr=False)

    def __post_init__(self):
        if not self.description:
            self.description = f"重大事件监控: {len(self.keywords)} 个关键词"


@dataclass
class SentimentAlert(AlertRule):
    """Alert on sentiment direction change."""
    alert_type: AlertType = AlertType.SENTIMENT_SHIFT
    from_sentiment: str = "positive"  # "positive", "negative", "neutral"
    to_sentiment: str = "negative"

    def __post_init__(self):
        if not self.description:
            self.description = f"{self.stock_code} sentiment shift: {self.from_sentiment} → {self.to_sentiment}"


@dataclass
class TriggeredAlert:
    """An alert that was triggered, ready for notification."""
    rule: AlertRule
    triggered_at: float = field(default_factory=time.time)
    current_value: Any = None
    message: str = ""


class EventMonitor:
    """Monitor stocks for event-driven alerts.

    This class manages a list of :class:`AlertRule` objects and checks
    them against current market data.  Triggered alerts are collected
    and can be forwarded to the notification system.
    """

    def __init__(self, *, jin10_api_key: str = ""):
        self.rules: List[AlertRule] = []
        self._callbacks: List[Callable[[TriggeredAlert], None]] = []
        self._jin10_api_key = (jin10_api_key or "").strip()
        self._cycle_quote_cache: Dict[str, Any] = {}

    def add_alert(self, rule: AlertRule) -> None:
        """Register a new alert rule."""
        _ensure_runtime_supported_alert_type(rule.alert_type)
        self.rules.append(rule)
        logger.debug("[EventMonitor] Added alert: %s", rule.description)

    def remove_expired(self) -> int:
        """Remove alerts that have expired based on TTL.

        Returns:
            Number of expired alerts removed.
        """
        now = time.time()
        before = len(self.rules)
        self.rules = [
            r for r in self.rules
            if r.status != AlertStatus.EXPIRED
            and (now - r.created_at) < r.ttl_hours * 3600
        ]
        removed = before - len(self.rules)
        if removed:
            logger.info("[EventMonitor] Removed %d expired alerts", removed)
        return removed

    def on_trigger(self, callback: Callable[[TriggeredAlert], None]) -> None:
        """Register a callback for when an alert triggers."""
        self._callbacks.append(callback)

    async def check_all(self) -> List[TriggeredAlert]:
        """Check all active rules against current market data.

        Returns:
            List of triggered alerts.
        """
        self.remove_expired()
        triggered: List[TriggeredAlert] = []
        self._cycle_quote_cache = {}

        for rule in self.rules:
            self._refresh_rule_runtime_state(rule)
            if rule.status != AlertStatus.ACTIVE:
                continue
            if _should_skip_for_market_session(rule):
                continue

            try:
                result = await self._check_rule(rule)
                if result:
                    triggered.append(result)
                    rule.status = AlertStatus.TRIGGERED
                    rule.triggered_at = time.time()
                    # Notify callbacks (offload slow/sync ones to thread)
                    for cb in self._callbacks:
                        try:
                            if asyncio.iscoroutinefunction(cb):
                                await cb(result)
                            else:
                                await asyncio.to_thread(cb, result)
                        except Exception as exc:
                            logger.warning("[EventMonitor] Callback error: %s", exc)
            except Exception as exc:
                logger.debug("[EventMonitor] Check failed for %s: %s", rule.description, exc)

        return triggered

    async def _check_rule(self, rule: AlertRule) -> Optional[TriggeredAlert]:
        """Check a single rule.  Returns TriggeredAlert if condition met."""
        if isinstance(rule, PriceAlert):
            return await self._check_price(rule)
        elif isinstance(rule, VolumeAlert):
            return await self._check_volume(rule)
        elif isinstance(rule, ICBasisAlert):
            return await self._check_ic_basis(rule)
        elif isinstance(rule, PriceSpikeAlert):
            return await self._check_price_spike(rule)
        elif isinstance(rule, NewsImpactAlert):
            return await self._check_news_impact(rule)
        # SentimentAlert and custom alerts require more context —
        # implemented as hooks for future extension
        return None

    def _refresh_rule_runtime_state(self, rule: AlertRule) -> None:
        metadata = rule.metadata if isinstance(rule.metadata, dict) else None
        if not metadata or not metadata.get("daily_reset"):
            return

        today_iso = date.today().isoformat()
        if metadata.get("monitor_date") == today_iso:
            return

        metadata["monitor_date"] = today_iso
        rule.status = AlertStatus.ACTIVE
        rule.triggered_at = None
        for key in (
            "armed",
            "armed_at",
            "armed_price",
            "armed_time_label",
            "max_price_after_arm",
        ):
            metadata.pop(key, None)

    def _check_holding_reversal_fail(
        self,
        rule: PriceAlert,
        current_price: float,
    ) -> Optional[TriggeredAlert]:
        metadata = rule.metadata if isinstance(rule.metadata, dict) else {}
        symbol_name = str(metadata.get("symbol_name") or rule.stock_code)
        trigger_level = float(metadata.get("reversal_trigger_price") or rule.price or 0.0)
        if trigger_level <= 0:
            return None

        now_ts = time.time()
        now_dt = datetime.now()
        now_minutes = now_dt.hour * 60 + now_dt.minute
        arm_after_minutes = int(metadata.get("arm_after_minutes", 14 * 60))
        max_reversal_window_minutes = int(metadata.get("max_reversal_window_minutes", 90))
        min_peak_buffer_pct = float(metadata.get("reversal_min_peak_buffer_pct", 0.002))

        if not metadata.get("armed"):
            if now_minutes < arm_after_minutes:
                return None
            if current_price >= trigger_level:
                metadata["armed"] = True
                metadata["armed_at"] = now_ts
                metadata["armed_price"] = current_price
                metadata["armed_time_label"] = now_dt.strftime("%H:%M")
                metadata["max_price_after_arm"] = current_price
                logger.info(
                    "[EventMonitor] Armed reversal-fail watch for %s at %.4f (trigger %.4f)",
                    rule.stock_code,
                    current_price,
                    trigger_level,
                )
            return None

        armed_at = float(metadata.get("armed_at") or now_ts)
        if now_ts - armed_at > max_reversal_window_minutes * 60:
            for key in ("armed", "armed_at", "armed_price", "armed_time_label", "max_price_after_arm"):
                metadata.pop(key, None)
            logger.info("[EventMonitor] Reset stale reversal-fail watch for %s", rule.stock_code)
            return None

        peak_price = max(float(metadata.get("max_price_after_arm") or current_price), current_price)
        metadata["max_price_after_arm"] = peak_price

        required_peak = trigger_level * (1.0 + min_peak_buffer_pct)
        if current_price <= trigger_level and peak_price >= required_peak:
            trigger_level_text = _format_price_level(trigger_level)
            current_value_text = _format_price_level(current_price)
            peak_value_text = _format_price_level(peak_price)
            armed_time_label = str(metadata.get("armed_time_label") or "")
            prefix = f"{symbol_name} 尾盘冲高触及 {trigger_level_text} 后回落失守"
            if armed_time_label:
                prefix += f"（{armed_time_label} 已上冲）"
            return TriggeredAlert(
                rule=rule,
                current_value=current_price,
                message=f"{prefix}，当前 {current_value_text}，盘中高点 {peak_value_text}。",
            )

        return None

    async def _check_price(self, rule: PriceAlert) -> Optional[TriggeredAlert]:
        """Check price alert against realtime quote."""
        try:
            cached_quote = self._cycle_quote_cache.get(rule.stock_code)

            def _fetch_quote():
                from data_provider import DataFetcherManager

                fm = DataFetcherManager()
                return fm.get_realtime_quote(rule.stock_code)

            quote = cached_quote if cached_quote is not None else await asyncio.to_thread(_fetch_quote)
            if quote is None:
                return None
            self._cycle_quote_cache[rule.stock_code] = quote

            current_price = float(getattr(quote, "price", 0) or 0)
            if current_price <= 0:
                return None

            triggered = False
            metadata = rule.metadata if isinstance(rule.metadata, dict) else {}
            kind = str(metadata.get("kind") or "").strip().lower()
            if kind == "holding_reversal_fail":
                return self._check_holding_reversal_fail(rule, current_price)

            if rule.direction == "above" and current_price >= rule.price:
                triggered = True
            elif rule.direction == "below" and current_price <= rule.price:
                triggered = True

            if triggered:
                symbol_name = str(metadata.get("symbol_name") or rule.stock_code)
                threshold = _format_price_level(rule.price)
                current_value = _format_price_level(current_price)
                if kind == "holding_stop_loss":
                    message = (
                        f"{symbol_name} 跌破持仓防守位 {threshold}，当前 {current_value}。"
                    )
                elif kind == "holding_trim_level":
                    message = (
                        f"{symbol_name} 触及反弹减仓位 {threshold}，当前 {current_value}。"
                    )
                elif kind == "holding_reversal_fail":
                    message = (
                        f"{symbol_name} 冲高触及 {threshold} 后回落失守，当前 {current_value}。"
                    )
                else:
                    message = (
                        f"{rule.stock_code} price {rule.direction} {rule.price}: "
                        f"current = {current_price}"
                    )
                return TriggeredAlert(
                    rule=rule,
                    current_value=current_price,
                    message=message,
                )
        except Exception as exc:
            logger.debug("[EventMonitor] _check_price error: %s", exc)
        return None

    async def _check_volume(self, rule: VolumeAlert) -> Optional[TriggeredAlert]:
        """Check volume spike against recent average."""
        try:
            def _fetch_daily_data():
                from data_provider import DataFetcherManager

                fm = DataFetcherManager()
                return fm.get_daily_data(rule.stock_code, days=20)

            result = await asyncio.to_thread(_fetch_daily_data)
            # get_daily_data returns (df, source) tuple or None
            if result is None:
                return None
            df, _source = result
            if df is None or df.empty:
                return None

            avg_vol = df["volume"].mean()
            latest_vol = df["volume"].iloc[-1]

            if avg_vol > 0 and latest_vol > avg_vol * rule.multiplier:
                return TriggeredAlert(
                    rule=rule,
                    current_value=latest_vol,
                    message=f"📊 {rule.stock_code} volume spike: "
                            f"{latest_vol:,.0f} ({latest_vol / avg_vol:.1f}× avg)",
                )
        except Exception as exc:
            logger.debug("[EventMonitor] _check_volume error: %s", exc)
        return None

    async def _check_ic_basis(self, rule: ICBasisAlert) -> Optional[TriggeredAlert]:
        """Check IC futures basis spike against thresholds.

        Fetches IC contract data via akshare, calculates annualized basis
        yield, and triggers if:
        1. Annualized yield > deep_threshold (deep contango), OR
        2. Intraday change > change_threshold (sudden deepening)
        """
        try:
            def _fetch_ic_basis():
                import akshare as ak
                from datetime import date
                import calendar

                # 获取中证500现货价格
                try:
                    spot_df = ak.stock_zh_index_spot_em()
                    spot_row = spot_df[spot_df['代码'] == '000905']
                    spot_price = float(spot_row['最新价'].values[0])
                except Exception:
                    # Fallback: 新浪接口
                    import requests
                    url = "https://hq.sinajs.cn/list=sh000905"
                    headers = {"Referer": "https://finance.sina.com.cn"}
                    resp = requests.get(url, headers=headers, timeout=10)
                    resp.encoding = 'gbk'
                    data = resp.text.strip().split('"')[1].split(',')
                    spot_price = float(data[2])  # 今开作为参考

                if spot_price <= 0:
                    return None

                # 确定监控合约
                contract = rule.contract
                if not contract:
                    # 默认监控主力合约：尝试 IC当月+下月，取持仓量最大的
                    from datetime import datetime
                    now = datetime.now()
                    candidates = []
                    for m_offset in range(6):
                        year = now.year
                        month = now.month + m_offset
                        while month > 12:
                            month -= 12
                            year += 1
                        code = f"IC{year % 100:02d}{month:02d}"
                        try:
                            df = ak.futures_main_sina(symbol=code)
                            if df is not None and len(df) > 0:
                                latest = df.iloc[-1]
                                oi = int(latest.get('持仓量', 0))
                                close = float(latest.get('收盘价', 0))
                                if close > 0 and oi > 0:
                                    candidates.append((code, close, oi))
                        except Exception:
                            continue
                    if not candidates:
                        return None
                    # 选持仓量最大的作为主力
                    candidates.sort(key=lambda x: x[2], reverse=True)
                    contract, futures_price, open_interest = candidates[0]
                else:
                    try:
                        df = ak.futures_main_sina(symbol=contract)
                        if df is None or len(df) == 0:
                            return None
                        futures_price = float(df.iloc[-1]['收盘价'])
                        open_interest = int(df.iloc[-1].get('持仓量', 0))
                    except Exception:
                        return None

                if futures_price <= 0:
                    return None

                # 计算到期日（每月第3个周五）
                year = int('20' + contract[2:4])
                month = int(contract[4:6])
                first_day_weekday = date(year, month, 1).weekday()
                first_friday = 1 + (4 - first_day_weekday) % 7
                third_fri = first_friday + 14
                expiry = date(year, month, third_fri)
                today = date.today()
                days_to_expiry = max((expiry - today).days, 1)

                # 年化贴水收益率（正数 = 多头收益）
                basis = spot_price - futures_price
                annualized = (basis / futures_price) / (days_to_expiry / 365) * 100

                return {
                    "contract": contract,
                    "spot_price": spot_price,
                    "futures_price": futures_price,
                    "basis": basis,
                    "annualized": annualized,
                    "days_to_expiry": days_to_expiry,
                    "open_interest": open_interest,
                }

            result = await asyncio.to_thread(_fetch_ic_basis)
            if result is None:
                return None

            annualized = result["annualized"]
            prev = rule._prev_annualized
            change = annualized - prev if prev > 0 else 0

            # 更新内部状态
            rule._prev_annualized = annualized

            # 判断触发条件
            triggered = False
            trigger_reason = ""

            if annualized > rule.deep_threshold:
                triggered = True
                trigger_reason = f"深度贴水: 年化{annualized:.2f}% > {rule.deep_threshold}%"
            elif change > rule.change_threshold:
                triggered = True
                trigger_reason = f"贴水突然加深: 日内变化{change:+.2f}% > {rule.change_threshold}%"

            if triggered:
                return TriggeredAlert(
                    rule=rule,
                    current_value=annualized,
                    message=(
                        f"**{result['contract']}** | "
                        f"年化贴水 **{annualized:.2f}%**\n\n"
                        f"> 中证500现货　{result['spot_price']:.2f}\n"
                        f"> {result['contract']}期货　{result['futures_price']:.2f}\n"
                        f"> 基差　　　　{result['basis']:.2f}\n"
                        f"> 剩余天数　{result['days_to_expiry']}天\n"
                        f"> 持仓量　　{result['open_interest']:,}\n\n"
                        f"**触发**: {trigger_reason}\n\n"
                        f"**建议**: 贴水加深是多头加仓/移仓良机，关注移仓到贴水更深的远月合约"
                    ),
                )
        except Exception as exc:
            logger.debug("[EventMonitor] _check_ic_basis error: %s", exc)
        return None

    async def _check_price_spike(self, rule: PriceSpikeAlert) -> Optional[TriggeredAlert]:
        """Check gold/silver price for sharp moves within the detection window."""
        try:
            def _fetch_price():
                import requests
                from src.market_data_fetcher import MarketDataFetcher

                # Jin10 quote codes: XAUUSD, XAGUSD
                code_map = {
                    "XAUUSD": "XAUUSD", "xauusd": "XAUUSD",
                    "XAGUSD": "XAGUSD", "xagusd": "XAGUSD",
                    "黄金": "XAUUSD", "白银": "XAGUSD",
                    "gold": "XAUUSD", "silver": "XAGUSD",
                }
                jin10_code = code_map.get(rule.stock_code, rule.stock_code)

                if self._jin10_api_key:
                    fetcher = MarketDataFetcher(self._jin10_api_key)
                    try:
                        quote = fetcher.get_quote(jin10_code)
                        if quote and quote.price > 0:
                            return float(quote.price)
                    finally:
                        fetcher.close()

                # Fallback: 新浪接口
                sina_map = {"XAUUSD": "hf_GC", "XAGUSD": "hf_SI"}
                sina_code = sina_map.get(jin10_code)
                if sina_code:
                    url = f"https://hq.sinajs.cn/list={sina_code}"
                    headers = {"Referer": "https://finance.sina.com.cn"}
                    resp = requests.get(url, headers=headers, timeout=10)
                    resp.encoding = 'gbk'
                    line = resp.text.strip()
                    if '=""' not in line and len(line) > 20:
                        fields = line.split('"')[1].split(',')
                        return float(fields[0])  # 最新价
                return None

            current_price = await asyncio.to_thread(_fetch_price)
            if current_price is None or current_price <= 0:
                return None

            now = time.time()
            prev_price = rule._prev_price
            prev_time = rule._prev_check_time

            # 更新状态
            rule._prev_price = current_price
            rule._prev_check_time = now

            # 首次检查，仅记录基准
            if prev_price <= 0 or prev_time <= 0:
                logger.debug("[EventMonitor] PriceSpike 首次记录 %s: %.2f", rule.stock_code, current_price)
                return None

            # 检查是否在窗口内
            elapsed_minutes = (now - prev_time) / 60
            if elapsed_minutes > rule.window_minutes:
                # 超出窗口，仅更新基准不触发
                logger.debug("[EventMonitor] PriceSpike 窗口超时 %s: %.1f分钟", rule.stock_code, elapsed_minutes)
                return None

            # 计算变化百分比
            change_pct = ((current_price - prev_price) / prev_price) * 100

            # 判断方向
            triggered = False
            if rule.direction == "both":
                triggered = abs(change_pct) >= rule.change_pct
            elif rule.direction == "up":
                triggered = change_pct >= rule.change_pct
            elif rule.direction == "down":
                triggered = change_pct <= -rule.change_pct

            if triggered:
                direction_label = "暴涨" if change_pct > 0 else "暴跌"
                return TriggeredAlert(
                    rule=rule,
                    current_value=current_price,
                    message=(
                        f"**{rule.stock_code}** {direction_label} "
                        f"**{change_pct:+.2f}%**\n\n"
                        f"> 当前价格　{current_price:.2f}\n"
                        f"> 上次价格　{prev_price:.2f}\n"
                        f"> 变化幅度　{change_pct:+.2f}%\n"
                        f"> 检测窗口　{elapsed_minutes:.1f}分钟\n\n"
                        f"**建议**: 关注是否为入场/止盈/止损时机"
                    ),
                )
        except Exception as exc:
            logger.debug("[EventMonitor] _check_price_spike error: %s", exc)
        return None

    async def _check_news_impact(self, rule: NewsImpactAlert) -> Optional[TriggeredAlert]:
        """Check Jin10 flash news for events impacting monitored assets."""
        try:
            def _is_new_flash_id(current_id: str, last_id: Optional[str]) -> bool:
                if not current_id:
                    return False
                if not last_id:
                    return True
                try:
                    return int(current_id) > int(last_id)
                except (TypeError, ValueError):
                    return current_id > last_id

            def _fetch_flash():
                if not self._jin10_api_key:
                    return []

                from src.market_data_fetcher import MarketDataFetcher

                fetcher = MarketDataFetcher(self._jin10_api_key)
                try:
                    return fetcher.list_flash(limit=50)
                finally:
                    fetcher.close()

            flash_list = await asyncio.to_thread(_fetch_flash)
            if not flash_list:
                return None

            # 筛选匹配关键词的快讯
            matched = []
            for item in flash_list:
                if not isinstance(item, dict):
                    continue
                headline = str(item.get("title", "") or item.get("content", "") or "")
                flash_id = str(item.get("id", "") or item.get("data_id", ""))

                if not flash_id:
                    continue
                # 跳过已处理过或更旧的快讯
                if not _is_new_flash_id(flash_id, rule._last_flash_id):
                    continue

                # 关键词匹配（不区分大小写）
                headline_lower = headline.lower()
                hit_keywords = [kw for kw in rule.keywords if kw.lower() in headline_lower]

                if hit_keywords:
                    matched.append({
                        "id": flash_id,
                        "headline": headline,
                        "keywords": hit_keywords,
                        "time": item.get("time", ""),
                    })

            if not matched:
                return None

            # 更新最新快讯ID（取最大ID，防止重复推送）
            max_id = max(m["id"] for m in matched)
            if _is_new_flash_id(max_id, rule._last_flash_id):
                rule._last_flash_id = max_id

            # 构建推送消息（最多聚合5条）
            display_items = matched[:5]
            news_lines = []
            for i, item in enumerate(display_items, 1):
                news_lines.append(
                    f"**{i}.** {item['headline']}\n"
                    f"> 匹配: {', '.join(item['keywords'])} | {item.get('time', '')}"
                )

            extra = f"\n> ... 另有 **{len(matched) - 5}** 条相关快讯" if len(matched) > 5 else ""

            return TriggeredAlert(
                rule=rule,
                current_value=len(matched),
                message=(
                    f"匹配到 **{len(matched)}** 条影响持仓/金银/IC的重大快讯\n\n"
                    + "\n\n".join(news_lines)
                    + extra
                    + f"\n\n**建议**: 评估事件对持仓品种的影响，必要时调整仓位"
                ),
            )
        except Exception as exc:
            logger.debug("[EventMonitor] _check_news_impact error: %s", exc)
        return None

    # -----------------------------------------------------------------
    # Persistence helpers
    # -----------------------------------------------------------------

    def to_dict_list(self) -> List[Dict[str, Any]]:
        """Serialize all rules for persistence."""
        results = []
        for rule in self.rules:
            entry: Dict[str, Any] = {
                "stock_code": rule.stock_code,
                "alert_type": rule.alert_type.value,
                "description": rule.description,
                "status": rule.status.value,
                "created_at": rule.created_at,
                "ttl_hours": rule.ttl_hours,
            }
            if isinstance(rule.metadata, dict) and rule.metadata:
                entry["metadata"] = dict(rule.metadata)
            if isinstance(rule, PriceAlert):
                entry["direction"] = rule.direction
                entry["price"] = rule.price
            elif isinstance(rule, VolumeAlert):
                entry["multiplier"] = rule.multiplier
            elif isinstance(rule, ICBasisAlert):
                entry["deep_threshold"] = rule.deep_threshold
                entry["change_threshold"] = rule.change_threshold
                entry["contract"] = rule.contract
            elif isinstance(rule, PriceSpikeAlert):
                entry["change_pct"] = rule.change_pct
                entry["direction"] = rule.direction
                entry["window_minutes"] = rule.window_minutes
            elif isinstance(rule, NewsImpactAlert):
                entry["keywords"] = rule.keywords
            results.append(entry)
        return results

    @classmethod
    def from_dict_list(
        cls,
        data: List[Dict[str, Any]],
        *,
        jin10_api_key: str = "",
    ) -> "EventMonitor":
        """Restore an EventMonitor from serialized data."""
        monitor = cls(jin10_api_key=jin10_api_key)
        for index, entry in enumerate(data, start=1):
            try:
                validate_event_alert_rule(entry)

                alert_type = entry.get("alert_type", "custom")
                stock_code = entry.get("stock_code", "")
                if alert_type == AlertType.PRICE_CROSS.value:
                    rule = PriceAlert(
                        stock_code=stock_code,
                        direction=entry.get("direction", "above").lower(),
                        price=float(entry.get("price", 0.0)),
                    )
                elif alert_type == AlertType.VOLUME_SPIKE.value:
                    rule = VolumeAlert(
                        stock_code=stock_code,
                        multiplier=float(entry.get("multiplier", 2.0)),
                    )
                elif alert_type == AlertType.BASIS_SPIKE.value:
                    rule = ICBasisAlert(
                        stock_code=stock_code,
                        deep_threshold=float(entry.get("deep_threshold", 10.0)),
                        change_threshold=float(entry.get("change_threshold", 2.0)),
                        contract=str(entry.get("contract", "")),
                    )
                elif alert_type == AlertType.PRICE_SPIKE.value:
                    rule = PriceSpikeAlert(
                        stock_code=stock_code,
                        change_pct=float(entry.get("change_pct", 1.0)),
                        direction=str(entry.get("direction", "both")),
                        window_minutes=int(entry.get("window_minutes", 30)),
                    )
                elif alert_type == AlertType.NEWS_IMPACT.value:
                    raw_kw = entry.get("keywords", [])
                    if isinstance(raw_kw, str):
                        raw_kw = [k.strip() for k in raw_kw.split(",") if k.strip()]
                    rule = NewsImpactAlert(
                        stock_code=stock_code,
                        keywords=raw_kw if isinstance(raw_kw, list) else [],
                    )
                else:
                    raise ValueError(f"unsupported alert_type: {alert_type}")
                rule.status = AlertStatus(entry.get("status", "active"))
                raw_metadata = entry.get("metadata")
                if isinstance(raw_metadata, dict):
                    rule.metadata = dict(raw_metadata)
                raw_created = entry.get("created_at")
                try:
                    rule.created_at = float(raw_created) if raw_created is not None else time.time()
                except (TypeError, ValueError):
                    rule.created_at = time.time()
                rule.ttl_hours = float(entry.get("ttl_hours", 24.0))
                monitor.add_alert(rule)
            except Exception as exc:
                logger.warning("[EventMonitor] Skip invalid rule #%d: %s", index, exc)
        return monitor


def parse_event_alert_rules(raw_rules: Any) -> List[Dict[str, Any]]:
    """Parse event alert rules from config JSON or already-loaded objects."""
    if raw_rules is None:
        return []

    parsed = raw_rules
    if isinstance(raw_rules, str):
        cleaned = raw_rules.strip()
        if not cleaned:
            return []
        parsed = json.loads(cleaned)

    if isinstance(parsed, dict):
        parsed = parsed.get("rules", [])

    if not isinstance(parsed, list):
        raise ValueError("Event alert rules must be a JSON array")

    invalid_indices = [idx for idx, entry in enumerate(parsed) if not isinstance(entry, dict)]
    if invalid_indices:
        raise ValueError(
            "Event alert rules list must contain only objects; "
            f"invalid entries at positions: {invalid_indices}"
        )

    return parsed


def validate_event_alert_rule(rule: Dict[str, Any]) -> None:
    """Validate one serialized EventMonitor rule."""
    if not isinstance(rule, dict):
        raise ValueError("Event alert rule must be an object")

    stock_code = str(rule.get("stock_code") or "").strip()
    if not stock_code:
        raise ValueError("stock_code is required")

    try:
        alert_type = AlertType(rule.get("alert_type", ""))
    except ValueError as exc:
        raise ValueError(f"invalid alert_type: {rule.get('alert_type')}") from exc
    _ensure_runtime_supported_alert_type(alert_type)

    status = rule.get("status")
    if status is not None:
        try:
            AlertStatus(status)
        except ValueError as exc:
            raise ValueError(f"invalid status: {status}") from exc

    ttl_hours = rule.get("ttl_hours")
    if ttl_hours is not None:
        try:
            ttl_value = float(ttl_hours)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"invalid ttl_hours: {ttl_hours}") from exc
        if ttl_value <= 0:
            raise ValueError("ttl_hours must be > 0")

    if alert_type == AlertType.PRICE_CROSS:
        direction = str(rule.get("direction", "above")).lower()
        if direction not in {"above", "below"}:
            raise ValueError(f"invalid direction: {direction}")
        try:
            price = float(rule.get("price"))
        except (TypeError, ValueError) as exc:
            raise ValueError(f"invalid price: {rule.get('price')}") from exc
        if price <= 0:
            raise ValueError("price must be > 0")
    elif alert_type == AlertType.VOLUME_SPIKE:
        try:
            multiplier = float(rule.get("multiplier", 2.0))
        except (TypeError, ValueError) as exc:
            raise ValueError(f"invalid multiplier: {rule.get('multiplier')}") from exc
        if multiplier <= 0:
            raise ValueError("multiplier must be > 0")
    elif alert_type == AlertType.BASIS_SPIKE:
        for field_name in ("deep_threshold", "change_threshold"):
            try:
                val = float(rule.get(field_name, 10.0 if "deep" in field_name else 2.0))
            except (TypeError, ValueError) as exc:
                raise ValueError(f"invalid {field_name}: {rule.get(field_name)}") from exc
            if val <= 0:
                raise ValueError(f"{field_name} must be > 0")
    elif alert_type == AlertType.PRICE_SPIKE:
        try:
            change_pct = float(rule.get("change_pct", 1.0))
        except (TypeError, ValueError) as exc:
            raise ValueError(f"invalid change_pct: {rule.get('change_pct')}") from exc
        if change_pct <= 0:
            raise ValueError("change_pct must be > 0")
        direction = str(rule.get("direction", "both")).lower()
        if direction not in {"up", "down", "both"}:
            raise ValueError(f"invalid direction: {direction}")
    elif alert_type == AlertType.NEWS_IMPACT:
        keywords = rule.get("keywords", [])
        if isinstance(keywords, str):
            keywords = [k.strip() for k in keywords.split(",") if k.strip()]
        if not isinstance(keywords, list) or len(keywords) == 0:
            raise ValueError("keywords must be a non-empty list")


def _format_price_level(value: Any) -> str:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return str(value)
    if numeric >= 100:
        return f"{numeric:.0f}元"
    if numeric >= 10:
        return f"{numeric:.2f}元"
    return f"{numeric:.3f}元"


def _extract_first_numeric_price(text: Any) -> Optional[float]:
    values = _extract_numeric_prices(text)
    return values[0] if values else None


def _extract_numeric_prices(text: Any) -> List[float]:
    if not text:
        return []
    values: List[float] = []
    matches = re.findall(r"(?<![A-Za-z])(\d+(?:\.\d+)?)", str(text))
    for item in matches:
        try:
            value = float(item)
        except (TypeError, ValueError):
            continue
        if value > 0:
            values.append(value)
    return values


def _dedupe_serialized_event_rules(rules: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    deduped: List[Dict[str, Any]] = []
    seen = set()
    for rule in rules:
        metadata = rule.get("metadata") if isinstance(rule.get("metadata"), dict) else {}
        key = (
            str(rule.get("stock_code") or "").strip(),
            str(rule.get("alert_type") or "").strip(),
            str(rule.get("direction") or "").strip(),
            str(rule.get("price") or "").strip(),
            str(rule.get("multiplier") or "").strip(),
            str(metadata.get("kind") or "").strip(),
        )
        if key in seen:
            continue
        seen.add(key)
        deduped.append(rule)
    return deduped


def _resolve_event_monitor_state_path(config: Any) -> Optional[Path]:
    raw_path = (
        getattr(config, "agent_event_monitor_state_path", None)
        or os.getenv("AGENT_EVENT_MONITOR_STATE_PATH")
    )
    if raw_path:
        return Path(str(raw_path)).expanduser()

    log_dir = getattr(config, "log_dir", None)
    if log_dir:
        return Path(str(log_dir)).expanduser() / "event_monitor_alert_state.json"
    return None


def _load_event_monitor_sent_state(path: Optional[Path]) -> Dict[str, Any]:
    if path is None or not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _save_event_monitor_sent_state(path: Optional[Path], payload: Dict[str, Any]) -> None:
    if path is None:
        return
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    except Exception as exc:
        logger.debug("[EventMonitor] Failed to save alert state: %s", exc)


def _event_monitor_alert_key(triggered: TriggeredAlert) -> str:
    rule = triggered.rule
    metadata = rule.metadata if isinstance(rule.metadata, dict) else {}
    parts = [
        str(rule.stock_code),
        str(rule.alert_type.value if isinstance(rule.alert_type, AlertType) else rule.alert_type),
        str(getattr(rule, "direction", "")),
        str(getattr(rule, "price", "")),
        str(getattr(rule, "contract", "")),
        str(metadata.get("kind") or ""),
    ]
    if rule.alert_type == AlertType.NEWS_IMPACT:
        parts.append(str(triggered.message or ""))
    return "|".join(parts)


def _build_portfolio_intraday_alert_rules(config=None) -> List[Dict[str, Any]]:
    if config is not None and not getattr(config, "agent_event_auto_portfolio_rules_enabled", True):
        return []

    max_positions = int(getattr(config, "agent_event_auto_portfolio_max_positions", 16) or 0)
    if max_positions <= 0:
        return []

    try:
        from src.repositories.analysis_repo import AnalysisRepository
        from src.services.portfolio_service import PortfolioService
    except Exception as exc:
        logger.warning("[EventMonitor] Failed to import portfolio auto-rule dependencies: %s", exc)
        return []

    try:
        snapshot = PortfolioService().get_portfolio_snapshot(
            account_id=None,
            as_of=date.today(),
            cost_method="fifo",
        )
    except Exception as exc:
        logger.warning("[EventMonitor] Failed to load portfolio snapshot for auto rules: %s", exc)
        return []

    positions_by_symbol: Dict[str, Dict[str, Any]] = {}
    for account in snapshot.get("accounts", []):
        account_id = account.get("account_id")
        for position in account.get("positions", []):
            symbol = str(position.get("symbol") or "").strip()
            if not symbol:
                continue
            quantity = float(position.get("quantity") or 0.0)
            market_value = float(position.get("market_value_base") or 0.0)
            last_price = float(position.get("last_price") or 0.0)
            avg_cost = float(position.get("avg_cost") or 0.0)
            if quantity <= 0 or market_value <= 0:
                continue
            bucket = positions_by_symbol.setdefault(
                symbol,
                {
                    "symbol": symbol,
                    "quantity": 0.0,
                    "market_value": 0.0,
                    "last_price": last_price,
                    "avg_cost": avg_cost,
                    "account_ids": [],
                },
            )
            bucket["quantity"] += quantity
            bucket["market_value"] += market_value
            if last_price > 0:
                bucket["last_price"] = last_price
            if avg_cost > 0:
                bucket["avg_cost"] = avg_cost
            if account_id is not None and account_id not in bucket["account_ids"]:
                bucket["account_ids"].append(account_id)

    if not positions_by_symbol:
        return []

    analysis_repo = AnalysisRepository()
    ranked_positions = sorted(
        positions_by_symbol.values(),
        key=lambda item: float(item.get("market_value") or 0.0),
        reverse=True,
    )[:max_positions]

    rules: List[Dict[str, Any]] = []
    for holding in ranked_positions:
        symbol = str(holding.get("symbol") or "")
        current_price = float(holding.get("last_price") or 0.0)
        avg_cost = float(holding.get("avg_cost") or 0.0)
        market_value = float(holding.get("market_value") or 0.0)
        account_ids = list(holding.get("account_ids") or [])

        history = analysis_repo.get_list(code=symbol, days=180, limit=1)
        latest = history[0] if history else None
        symbol_name = getattr(latest, "name", None) or symbol
        stop_loss = float(getattr(latest, "stop_loss", 0.0) or 0.0) if latest else 0.0
        take_profit = float(getattr(latest, "take_profit", 0.0) or 0.0) if latest else 0.0
        operation_advice = getattr(latest, "operation_advice", "") if latest else ""
        trend_prediction = getattr(latest, "trend_prediction", "") if latest else ""
        created_at = getattr(latest, "created_at", None) if latest else None

        has_position_text = ""
        take_profit_text = ""
        resistance_level = 0.0
        if latest and getattr(latest, "raw_result", None):
            try:
                raw_result = json.loads(latest.raw_result)
            except Exception:
                raw_result = {}
            dashboard = raw_result.get("dashboard") or {}
            core = dashboard.get("core_conclusion") or {}
            position_advice = core.get("position_advice") or {}
            battle_plan = dashboard.get("battle_plan") or {}
            sniper_points = battle_plan.get("sniper_points") or {}
            data_perspective = dashboard.get("data_perspective") or {}
            price_position = data_perspective.get("price_position") or {}
            has_position_text = str(position_advice.get("has_position") or "")
            take_profit_text = str(sniper_points.get("take_profit") or "")
            try:
                resistance_level = float(price_position.get("resistance_level") or 0.0)
            except (TypeError, ValueError):
                resistance_level = 0.0

        trim_target = None
        if has_position_text and "减仓" in has_position_text:
            trim_target = _extract_first_numeric_price(has_position_text)
        if trim_target is None:
            trim_target = take_profit if take_profit > 0 else _extract_first_numeric_price(take_profit_text)

        reversal_candidates: List[float] = []
        if current_price > 0:
            take_profit_candidates = _extract_numeric_prices(take_profit_text)
            for value in (resistance_level, trim_target, *take_profit_candidates):
                if value and value > current_price:
                    gap_pct = (float(value) - current_price) / current_price
                    if gap_pct <= 0.08:
                        reversal_candidates.append(float(value))
        reversal_trigger = min(reversal_candidates) if reversal_candidates else None

        if stop_loss <= 0 and current_price > 0:
            stop_loss = round(current_price * 0.97, 4)

        base_metadata = {
            "symbol_name": symbol_name,
            "market_value_base": round(market_value, 6),
            "account_ids": account_ids,
            "analysis_created_at": str(created_at) if created_at else "",
            "operation_advice": operation_advice,
            "trend_prediction": trend_prediction,
            "market_session": CN_INTRADAY_MARKET_SESSION,
        }

        if stop_loss > 0:
            rules.append(
                {
                    "stock_code": symbol,
                    "alert_type": AlertType.PRICE_CROSS.value,
                    "direction": "below",
                    "price": round(stop_loss, 4),
                    "description": f"{symbol_name} 跌破持仓止损位 {_format_price_level(stop_loss)}",
                    "ttl_hours": 24.0,
                    "metadata": {
                        **base_metadata,
                        "kind": "holding_stop_loss",
                        "current_price": round(current_price, 4),
                        "fallback": bool(not latest),
                        "daily_reset": True,
                    },
                }
            )

        if trim_target and trim_target > 0 and current_price > 0 and current_price < trim_target:
            rules.append(
                {
                    "stock_code": symbol,
                    "alert_type": AlertType.PRICE_CROSS.value,
                    "direction": "above",
                    "price": round(trim_target, 4),
                    "description": f"{symbol_name} 触及减仓/压力位 {_format_price_level(trim_target)}",
                    "ttl_hours": 24.0,
                    "metadata": {
                        **base_metadata,
                        "kind": "holding_trim_level",
                        "current_price": round(current_price, 4),
                        "fallback": bool(not latest),
                        "daily_reset": True,
                    },
                }
            )

        if reversal_trigger and current_price > 0:
            rules.append(
                {
                    "stock_code": symbol,
                    "alert_type": AlertType.PRICE_CROSS.value,
                    "direction": "below",
                    "price": round(reversal_trigger, 4),
                    "description": f"{symbol_name} 尾盘冲高回落失守 {_format_price_level(reversal_trigger)}",
                    "ttl_hours": 72.0,
                    "metadata": {
                        **base_metadata,
                        "kind": "holding_reversal_fail",
                        "current_price": round(current_price, 4),
                        "fallback": bool(not latest),
                        "daily_reset": True,
                        "reversal_trigger_price": round(reversal_trigger, 4),
                        "arm_after_minutes": 14 * 60,
                        "max_reversal_window_minutes": 120,
                        "reversal_min_peak_buffer_pct": 0.002,
                        "reversal_source": (
                            "resistance_level"
                            if resistance_level and abs(reversal_trigger - resistance_level) < 1e-6
                            else "trim_target"
                        ),
                    },
                }
            )

    deduped = _dedupe_serialized_event_rules(rules)
    if deduped:
        logger.info("[EventMonitor] Auto-built %d portfolio alert rule(s)", len(deduped))
    return deduped


def build_event_monitor_from_config(config=None, notifier=None) -> Optional[EventMonitor]:
    """Build an EventMonitor from runtime config and attach notification callbacks."""
    if config is None:
        from src.config import get_config
        config = get_config()

    if not getattr(config, "agent_event_monitor_enabled", False):
        return None

    rules: List[Dict[str, Any]] = []
    raw_rules = getattr(config, "agent_event_alert_rules_json", "")
    try:
        rules.extend(parse_event_alert_rules(raw_rules))
    except Exception as exc:
        logger.warning("[EventMonitor] Failed to parse configured alert rules: %s", exc)
    try:
        rules.extend(_build_portfolio_intraday_alert_rules(config))
    except Exception as exc:
        logger.warning("[EventMonitor] Failed to build portfolio auto rules: %s", exc)
    rules = _dedupe_serialized_event_rules(rules)
    if not rules:
        logger.info("[EventMonitor] Enabled but no alert rules configured")
        return None

    monitor = EventMonitor.from_dict_list(
        rules,
        jin10_api_key=getattr(config, "jin10_api_key", ""),
    )
    if not monitor.rules:
        return None

    from src.notification import NotificationBuilder, NotificationService

    notification_service = notifier or NotificationService()
    alert_state_path = _resolve_event_monitor_state_path(config)
    alert_state = _load_event_monitor_sent_state(alert_state_path)
    today_iso = date.today().isoformat()
    if alert_state.get("date") != today_iso:
        alert_state = {"date": today_iso, "sent_keys": []}
    sent_keys = set(alert_state.get("sent_keys") or [])

    def _format_brief_alert(triggered: TriggeredAlert) -> str:
        metadata = triggered.rule.metadata if isinstance(triggered.rule.metadata, dict) else {}
        kind = str(metadata.get("kind") or "").strip().lower()
        symbol_name = str(metadata.get("symbol_name") or triggered.rule.stock_code)
        threshold = _format_price_level(getattr(triggered.rule, "price", None))
        if kind == "holding_stop_loss":
            return "\n".join(
                [
                    f"触发事件: {symbol_name} 跌破持仓止损位 {threshold}",
                    "影响策略: 当前持仓风控 / 减仓止损",
                    "当前倾向: 先按既定防守位执行，不等收盘确认",
                    "建议动作: 立即检查仓位与成交，优先按计划减仓或止损。",
                    "是否立即关注: 是",
                ]
            )
        if kind == "holding_trim_level":
            return "\n".join(
                [
                    f"触发事件: {symbol_name} 触及减仓/压力位 {threshold}",
                    "影响策略: 当前持仓节奏 / 反弹减仓",
                    "当前倾向: 优先看是否放量突破，未突破则按计划减仓",
                    "建议动作: 立即复核量能与盘口，按持仓计划逢高减仓或做T。",
                    "是否立即关注: 是",
                ]
            )
        if kind == "holding_reversal_fail":
            return "\n".join(
                [
                    f"触发事件: {symbol_name} 尾盘冲高后跌回压力位 {threshold} 下方",
                    "影响策略: 当前持仓节奏 / 尾盘失败减仓",
                    "当前倾向: 上冲未站稳，优先按失败突破处理，不追高等待收盘",
                    "建议动作: 立即检查量能与尾盘回落幅度，优先减仓或撤销追高计划。",
                    "是否立即关注: 是",
                ]
            )

        strategy_map = {
            AlertType.BASIS_SPIKE: "IC贴水策略 / 认沽保护",
            AlertType.PRICE_SPIKE: "黄金ETF / 白银期货 / 商品ETF",
            AlertType.NEWS_IMPACT: "黄金ETF / 白银期货 / IC贴水 / 认沽保护",
            AlertType.PRICE_CROSS: "相关持仓或观察标的",
            AlertType.VOLUME_SPIKE: "相关持仓或观察标的",
        }
        bias_map = {
            AlertType.BASIS_SPIKE: "先防基差继续走阔",
            AlertType.PRICE_SPIKE: "先看异动是否延续，不追单边",
            AlertType.NEWS_IMPACT: "先看是否升级为影响持仓的实质事件",
            AlertType.PRICE_CROSS: "观察关键位是否站稳/失守",
            AlertType.VOLUME_SPIKE: "观察量能异常是否带来趋势确认",
        }
        action_map = {
            AlertType.BASIS_SPIKE: "先盯IC贴水与认沽保护，必要时收缩进攻仓位。",
            AlertType.PRICE_SPIKE: "先盯黄金、白银或股指异动主线，再决定是否跟进。",
            AlertType.NEWS_IMPACT: "先核实事件是否升级，再决定是否切换到防守。",
            AlertType.PRICE_CROSS: "先看关键位是否站稳/失守，再决定是否调整节奏。",
            AlertType.VOLUME_SPIKE: "先看量能是否确认趋势，再决定是否扩大关注。",
        }
        trigger_event = triggered.message or triggered.rule.description or "事件触发"
        strategy = strategy_map.get(triggered.rule.alert_type, "相关策略")
        bias = bias_map.get(triggered.rule.alert_type, "先观察后决策")
        action = action_map.get(triggered.rule.alert_type, "先聚焦受影响持仓，确认是否需要动作。")
        immediate_attention = triggered.rule.alert_type in {
            AlertType.BASIS_SPIKE,
            AlertType.PRICE_SPIKE,
            AlertType.NEWS_IMPACT,
        }
        return "\n".join(
            [
                f"触发事件: {trigger_event}",
                f"影响策略: {strategy}",
                f"当前倾向: {bias}",
                f"建议动作: {action}",
                f"是否立即关注: {'是' if immediate_attention else '否'}",
            ]
        )

    def _notify(triggered: TriggeredAlert) -> None:
        # 根据告警类型选择图标和样式
        metadata = triggered.rule.metadata if isinstance(triggered.rule.metadata, dict) else {}
        kind = str(metadata.get("kind") or "").strip().lower()
        symbol_name = str(metadata.get("symbol_name") or triggered.rule.stock_code)
        alert_key = _event_monitor_alert_key(triggered)
        if alert_key in sent_keys:
            logger.info("[EventMonitor] Suppressed duplicate same-day alert: %s", triggered.rule.description)
            return

        if kind == "holding_stop_loss":
            label, alert_type = ("持仓止损预警", "error")
        elif kind == "holding_trim_level":
            label, alert_type = ("持仓减仓提醒", "warning")
        elif kind == "holding_reversal_fail":
            label, alert_type = ("持仓冲高回落提醒", "warning")
        else:
            # 根据告警类型选择图标和样式
            _ALERT_META = {
                AlertType.BASIS_SPIKE: ("IC贴水预警", "warning"),
                AlertType.PRICE_SPIKE: ("价格异动预警", "error"),
                AlertType.NEWS_IMPACT: ("重大事件预警", "warning"),
                AlertType.PRICE_CROSS: ("价格穿越提醒", "info"),
                AlertType.VOLUME_SPIKE: ("成交量异常", "info"),
            }
            label, alert_type = _ALERT_META.get(
                triggered.rule.alert_type, ("事件提醒", "info")
            )
        title = f"{label} | {symbol_name}({triggered.rule.stock_code})"
        content = _format_brief_alert(triggered)
        alert_text = NotificationBuilder.build_simple_alert(
            title=title, content=content, alert_type=alert_type
        )
        sent = notification_service.send(alert_text)
        if not sent:
            logger.info("[EventMonitor] No notification channel available for alert: %s", title)
            return
        sent_keys.add(alert_key)
        alert_state["date"] = today_iso
        alert_state["sent_keys"] = sorted(sent_keys)
        alert_state["last_sent_at"] = datetime.now().isoformat()
        _save_event_monitor_sent_state(alert_state_path, alert_state)

    monitor.on_trigger(_notify)
    logger.info("[EventMonitor] Loaded %d configured alert rule(s)", len(monitor.rules))
    return monitor


def run_event_monitor_once(monitor: EventMonitor) -> List[TriggeredAlert]:
    """Run one synchronous monitor cycle."""
    return asyncio.run(monitor.check_all())
