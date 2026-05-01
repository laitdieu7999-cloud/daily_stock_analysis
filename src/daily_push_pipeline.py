"""
每日推送流水线 - 按策略优先级推送：黄金 -> 白银 -> 中证500 -> 持仓 -> 精选入场

在每日个股分析推送之前，先推送金银/中证500 的详细技术分析和AI预测。
每个品种包含：实时报价 + 技术指标分析 + 综合评分 + AI预测建议。

AI boundary note:
- This file is the local extension push layer for gold / silver / CSI500.
- It runs before the stock-analysis main flow, but it does not replace the
  core stock pipeline in ``main.py`` / ``src/core/pipeline.py``.
"""
import logging
import json
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)

VIP_WATCH_SIGNAL_HISTORY_PATH = (
    Path(__file__).resolve().parent.parent / "reports" / "vip_watch_signal_history.jsonl"
)

# 品种配置：(代码, 数据源, 名称, 单位)
MARKET_ASSETS = [
    {"code": "AU0", "source": "futures_sina", "name": "黄金期货", "unit": "元/克",
     "quote_code": "XAUUSD", "quote_unit": "$"},
    {"code": "AG0", "source": "futures_sina", "name": "白银期货", "unit": "元/千克",
     "quote_code": "XAGUSD", "quote_unit": "$"},
    {"code": "000905", "source": "index_zh_a", "name": "中证500指数", "unit": "点",
     "quote_code": None, "quote_unit": "", "fallback_code": "IC0", "fallback_source": "futures_sina",
     "fallback_name": "IC期货(中证500替代)"},
]

FLASH_KEYWORDS = [
    "黄金", "白银", "中证500", "IC", "股指期货", "A股", "美联储",
    "降息", "加息", "非农", "通胀", "CPI", "美元", "美债", "地缘",
    "俄乌", "中东", "关税", "原油", "避险",
]
OVERNIGHT_A_SHARE_KEYWORDS = [
    "纳斯达克中国金龙", "金龙指数", "中概股", "美股", "A股", "股指期货",
    "美联储", "降息", "加息", "非农", "CPI", "美元", "美债", "原油",
    "中东", "地缘", "关税", "离岸人民币", "流动性", "避险",
]

CALENDAR_TAGS = {
    "黄金期货": ["黄金", "白银", "美元"],
    "白银期货": ["白银", "黄金", "美元"],
    "中证500指数": ["A股", "股指", "美元"],
}

VIP_WATCH_CODES = {
    "黄金期货": "XAUUSD.GOODS",
    "白银期货": "XAGUSD.GOODS",
}

ASSET_BIAS_RULES = {
    "黄金期货": {
        "bullish": {
            "强利多": 4,
            "弱利多": 2,
            "资金偏多": 3,
            "趋势偏强": 2,
            "均线多头": 2,
            "临近支撑": 2,
            "避险升温": 3,
            "利多黄金": 3,
            "黄金走强": 2,
            "降息": 2,
            "美元走弱": 2,
            "利多白银": 1,
        },
        "bearish": {
            "强利空": 4,
            "弱利空": 2,
            "资金偏空": 3,
            "趋势偏弱": 2,
            "均线空头": 2,
            "临近压力": 2,
            "压力侧更近": 1,
            "利空黄金": 3,
            "黄金承压": 2,
            "加息": 2,
            "美元走强": 2,
        },
    },
    "白银期货": {
        "bullish": {
            "强利多": 4,
            "弱利多": 2,
            "资金偏多": 3,
            "趋势偏强": 2,
            "均线多头": 2,
            "临近支撑": 2,
            "利多白银": 3,
            "白银走强": 2,
            "工业金属回暖": 2,
            "降息": 2,
            "美元走弱": 2,
            "利多黄金": 1,
        },
        "bearish": {
            "强利空": 4,
            "弱利空": 2,
            "资金偏空": 3,
            "趋势偏弱": 2,
            "均线空头": 2,
            "临近压力": 2,
            "压力侧更近": 1,
            "利空白银": 3,
            "白银承压": 2,
            "加息": 2,
            "美元走强": 2,
        },
    },
    "中证500指数": {
        "bullish": {
            "利多A股": 3,
            "利多股市": 3,
            "政策支持": 3,
            "风险偏好回升": 2,
            "降息": 2,
            "金龙上涨": 3,
            "金龙走强": 3,
            "中概股走强": 2,
            "隔夜外盘情绪回暖": 2,
        },
        "bearish": {
            "利空A股": 3,
            "利空股市": 3,
            "关税": 2,
            "地缘冲突": 2,
            "风险偏好回落": 2,
            "美元走强": 2,
            "金龙下跌": 3,
            "金龙走弱": 3,
            "中概股承压": 2,
            "隔夜外盘风险偏好回落": 2,
        },
    },
}


class DailyPushPipeline:
    """每日推送流水线

    负责在个股分析推送之前，按策略优先级推送市场品种的详细分析。
    推送顺序：黄金 -> 白银 -> 中证500 -> 持仓分析 -> 精选入场
    当前实现前三个品种的详细分析，持仓分析和精选入场由主分析流程负责。
    """

    def __init__(
        self,
        notifier,
        jin10_api_key: str = "",
        jin10_x_token: str = "",
        ai_enabled: bool = True,
    ):
        """
        Args:
            notifier: NotificationService 实例（用于发送飞书消息）
            jin10_api_key: 金十数据 API Key
            jin10_x_token: Jin10 Web x-token（用于会员盯盘接口）
            ai_enabled: 是否启用 AI 短评
        """
        self._notifier = notifier
        self._jin10_key = jin10_api_key
        self._jin10_x_token = jin10_x_token
        self._ai_enabled = ai_enabled

    def _analyze_asset(self, fetcher, asset_cfg: dict) -> Optional[str]:
        """对单个品种进行完整分析（实时报价 + 技术分析 + AI预测）

        Returns:
            格式化后的 Markdown 分析文本，失败返回 None
        """
        from src.stock_analyzer import StockTrendAnalyzer

        code = asset_cfg["code"]
        source = asset_cfg["source"]
        name = asset_cfg["name"]
        unit = asset_cfg["unit"]

        # 1. 获取实时报价（仅金银有 Jin10 报价）
        quote_section = ""
        quote_code = asset_cfg.get("quote_code")
        if quote_code and self._jin10_key:
            quote = fetcher.get_quote(quote_code)
            if quote:
                qu = asset_cfg.get("quote_unit", "")
                arrow = "📈" if quote.change_pct >= 0 else "📉"
                quote_section = (
                    f"> {arrow} 现货最新价: **{qu}{quote.price:,.2f}** | "
                    f"涨跌幅: {'+' if quote.change_pct >= 0 else ''}{quote.change_pct:.2f}%\n"
                    f"> 今日区间: {qu}{quote.low:,.2f} ~ {qu}{quote.high:,.2f}\n"
                )

        # 2. 获取历史K线（支持备用数据源）
        df = fetcher.get_historical_kline(code, source=source)
        actual_name = name
        if (df is None or len(df) < 20) and asset_cfg.get("fallback_code"):
            fb_code = asset_cfg["fallback_code"]
            fb_source = asset_cfg["fallback_source"]
            logger.info(f"[DailyPush] {name} 主数据源失败，尝试备用: {fb_code}")
            df = fetcher.get_historical_kline(fb_code, source=fb_source)
            if df is not None and len(df) >= 20:
                actual_name = asset_cfg.get("fallback_name", name)
        if df is None or len(df) < 20:
            logger.warning(f"[DailyPush] {name} 历史K线数据不足，跳过技术分析")
            if quote_section:
                return f"**{name} ({code})**\n{quote_section}> ⚠️ 历史数据不足，无法进行技术分析"
            return None

        # 3. 技术分析
        trend_analyzer = StockTrendAnalyzer()
        trend_result = trend_analyzer.analyze(df, code)
        if trend_result is None:
            logger.warning(f"[DailyPush] {name} 技术分析失败")
            return None

        # 4. 构建分析报告
        latest = df.iloc[-1]
        price = float(latest['close'])
        change = float(latest['close'] - latest['open'])
        change_pct = (change / float(latest['open'])) * 100 if float(latest['open']) > 0 else 0
        arrow = "📈" if change_pct >= 0 else "📉"

        # 信号映射
        signal_map = {
            "STRONG_BUY": "🟢 强烈买入", "BUY": "🟢 买入",
            "HOLD": "🟡 持有", "WAIT": "⚪ 观望",
            "SELL": "🔴 卖出", "STRONG_SELL": "🔴 强烈卖出",
        }
        signal_text = signal_map.get(trend_result.buy_signal.value, str(trend_result.buy_signal.value))

        lines = [
            f"**{actual_name} ({code})**",
            f"> {arrow} 最新价: **{price:,.2f} {unit}** | "
            f"涨跌: {'+' if change_pct >= 0 else ''}{change_pct:.2f}%",
        ]

        # 实时报价（金银现货）
        if quote_section:
            lines.append(quote_section.rstrip())

        # 均线系统
        lines.append(
            f"> **均线**: MA5={trend_result.ma5:.2f} MA10={trend_result.ma10:.2f} "
            f"MA20={trend_result.ma20:.2f} MA60={trend_result.ma60:.2f}"
        )
        lines.append(f"> **均线排列**: {trend_result.ma_alignment}")

        # MACD
        if trend_result.macd_signal:
            lines.append(f"> **MACD**: DIF={trend_result.macd_dif:.2f} DEA={trend_result.macd_dea:.2f} | {trend_result.macd_signal}")

        # 布林带
        if trend_result.boll_signal:
            lines.append(
                f"> **BOLL**: 上轨={trend_result.boll_upper:.2f} 中轨={trend_result.boll_mid:.2f} "
                f"下轨={trend_result.boll_lower:.2f} | {trend_result.boll_signal}"
            )

        # KDJ
        if trend_result.kdj_signal:
            lines.append(f"> **KDJ**: K={trend_result.kdj_k:.1f} D={trend_result.kdj_d:.1f} J={trend_result.kdj_j:.1f} | {trend_result.kdj_signal}")

        # RSI
        if trend_result.rsi_signal:
            lines.append(f"> **RSI**: RSI6={trend_result.rsi_6:.1f} RSI12={trend_result.rsi_12:.1f} | {trend_result.rsi_signal}")

        # ATR 波动率
        if trend_result.atr_percent > 0:
            lines.append(f"> **ATR**: {trend_result.atr_14:.2f} (波动率 {trend_result.atr_percent:.2f}%)")

        # OBV
        if trend_result.obv_signal:
            lines.append(f"> **OBV**: {trend_result.obv_signal}")

        # RSRS 择时
        if trend_result.rsrs_signal:
            lines.append(f"> **RSRS**: β={trend_result.rsrs_beta:.3f} Z={trend_result.rsrs_zscore:.3f} R²={trend_result.rsrs_r2:.3f} | {trend_result.rsrs_signal}")

        # 量能
        lines.append(f"> **量能**: {trend_result.volume_trend}")

        # 综合信号
        lines.append(f"> **综合评分**: {trend_result.signal_score}/100 | **信号**: {signal_text}")

        # 买入理由
        if trend_result.signal_reasons:
            reasons = "；".join(trend_result.signal_reasons[:3])
            lines.append(f"> **看多理由**: {reasons}")

        # 风险因素
        if trend_result.risk_factors:
            risks = "；".join(trend_result.risk_factors[:3])
            lines.append(f"> **风险提示**: {risks}")

        return "\n".join(lines)

    def _ai_predict(self, asset_name: str, analysis_text: str, jin10_context: Optional[str] = None) -> Optional[str]:
        """调用 AI 对品种进行分析预测

        Returns:
            AI 预测文本，失败返回 None
        """
        try:
            import litellm
            from src.config import get_config

            config = get_config()
            api_key = config.openai_api_key or ""
            api_base = config.openai_base_url or None
            model = config.litellm_model or "openai/gpt-4o-mini"

            extra_context = ""
            if jin10_context:
                extra_context = f"\n金十参考（仅作为辅助判断，不要逐条复述）：\n{jin10_context}\n"

            prompt = f"""你是首席执行交易员，请基于以下技术分析数据，对{asset_name}给出极简、直接、可执行的分析预测。

要求：
1. 只给结论，不复述原始数据
2. 用2-3句话给出当前结构判断、短中期方向、关键动作
3. 必须给出关键支撑位和压力位
4. 操作建议必须明确（做多/做空/观望/等待回踩）
5. 严格控制篇幅，不超过120字
6. 如金十参考与技术面明显共振，可在结论中简要吸收；若无关则忽略，不要硬套
7. 禁止使用“可能”“或许”“建议关注”等空话

技术分析数据：
{analysis_text}
{extra_context}

请直接输出分析预测，不要重复数据。"""

            kwargs = {
                "model": model,
                "messages": [
                    {"role": "system", "content": "你是量化基金首席执行交易员。回复必须极简、直接、可执行，只给结论，不复述原始数据。"},
                    {"role": "user", "content": prompt},
                ],
                "temperature": 0.1,
                "max_tokens": 220,
            }
            if api_key:
                kwargs["api_key"] = api_key
            if api_base:
                kwargs["api_base"] = api_base

            response = litellm.completion(**kwargs)
            text = response.choices[0].message.content.strip()
            return text if text else None
        except Exception as e:
            logger.warning(f"[DailyPush] {asset_name} AI预测失败: {e}")
        return None

    def _build_jin10_flash_context(self, fetcher, asset_name: Optional[str] = None) -> Optional[str]:
        """构建供 AI 使用的 Jin10 快讯上下文，不直接展示给用户。"""
        if not self._jin10_key:
            return None

        try:
            flash_list = fetcher.list_flash(limit=30)
            if not flash_list:
                return None

            asset_keywords = []
            if asset_name:
                if "黄金" in asset_name:
                    asset_keywords.extend(["黄金", "美元", "美联储", "降息", "通胀", "避险"])
                elif "白银" in asset_name:
                    asset_keywords.extend(["白银", "黄金", "美元", "美联储", "工业金属", "避险"])
                elif "中证500" in asset_name or "IC" in asset_name:
                    asset_keywords.extend(["中证500", "IC", "A股", "股指期货", "政策", "关税"])

            effective_keywords = asset_keywords + [kw for kw in FLASH_KEYWORDS if kw not in asset_keywords]
            matched = []
            for item in flash_list:
                if not isinstance(item, dict):
                    continue
                headline = str(item.get("title", "") or item.get("content", "") or "").strip()
                if not headline:
                    continue
                headline_lower = headline.lower()
                hit_keywords = [kw for kw in effective_keywords if kw.lower() in headline_lower]
                if not hit_keywords:
                    continue
                matched.append(
                    {
                        "headline": headline,
                        "time": str(item.get("time", "") or ""),
                        "keywords": hit_keywords[:3],
                    }
                )
                if len(matched) >= 5:
                    break

            if not matched:
                return None

            lines = []
            for index, item in enumerate(matched, 1):
                time_text = f" | {item['time']}" if item["time"] else ""
                keyword_text = f" | 关键词: {', '.join(item['keywords'])}" if item["keywords"] else ""
                lines.append(f"{index}. {item['headline']}{keyword_text}{time_text}")
            return "\n".join(lines)
        except Exception as e:
            logger.warning(f"[DailyPush] 获取 Jin10 快讯上下文失败: {e}")
        return None

    @staticmethod
    def _is_overnight_flash_time(time_text: str, now: Optional[datetime] = None) -> bool:
        if not time_text:
            return False
        now = now or datetime.now()
        try:
            parts = time_text.strip().split(":")
            hour = int(parts[0])
            minute = int(parts[1]) if len(parts) > 1 else 0
        except Exception:
            return False

        current_hhmm = now.hour * 100 + now.minute
        flash_hhmm = hour * 100 + minute
        if now.hour < 12:
            return flash_hhmm >= 2000 or flash_hhmm <= current_hhmm
        return False

    def _build_overnight_jin10_a_share_context(
        self,
        fetcher,
        asset_name: Optional[str] = None,
    ) -> Optional[str]:
        """提取夜间可能影响次日 A 股的重大金十事件。"""
        if not asset_name or "中证500" not in asset_name:
            return None
        if not self._jin10_key:
            return None

        try:
            flash_list = fetcher.list_flash(limit=50)
            if not flash_list:
                return None

            matched = []
            now = datetime.now()
            for item in flash_list:
                if not isinstance(item, dict):
                    continue
                headline = str(item.get("title", "") or item.get("content", "") or "").strip()
                time_text = str(item.get("time", "") or "").strip()
                if not headline or not self._is_overnight_flash_time(time_text, now=now):
                    continue
                headline_lower = headline.lower()
                hit_keywords = [kw for kw in OVERNIGHT_A_SHARE_KEYWORDS if kw.lower() in headline_lower]
                if not hit_keywords:
                    continue
                matched.append(
                    {
                        "headline": headline,
                        "time": time_text,
                        "keywords": hit_keywords[:3],
                    }
                )
                if len(matched) >= 5:
                    break

            if not matched:
                return None

            lines = []
            for index, item in enumerate(matched, 1):
                keyword_text = f" | 关键词: {', '.join(item['keywords'])}" if item["keywords"] else ""
                lines.append(f"{index}. {item['headline']}{keyword_text} | 时间: {item['time']}")
            return "隔夜金十事件:\n" + "\n".join(lines)
        except Exception as e:
            logger.warning(f"[DailyPush] 获取隔夜金十事件失败: {e}")
            return None

    def _build_nasdaq_golden_dragon_context(
        self,
        snapshot: Optional[Dict[str, Any]],
        asset_name: Optional[str] = None,
    ) -> Optional[str]:
        """构建隔夜中概金龙指数上下文，仅服务次日 A 股情绪判断。"""
        if not asset_name or "中证500" not in asset_name:
            return None

        try:
            if not snapshot:
                return None

            pct = snapshot.get("change_pct")
            if pct is None:
                return None
            if pct >= 1.0:
                bias_text = "金龙上涨，隔夜外盘情绪回暖"
            elif pct <= -1.0:
                bias_text = "金龙下跌，隔夜外盘风险偏好回落"
            elif pct > 0:
                bias_text = "金龙走强"
            elif pct < 0:
                bias_text = "金龙走弱"
            else:
                bias_text = "金龙震荡"

            parts = [
                f"隔夜中概金龙指数: {snapshot.get('last', 0):,.2f}",
                f"涨跌幅: {pct:+.2f}%",
                bias_text,
            ]
            if snapshot.get("change") is not None:
                parts.append(f"净变动: {snapshot['change']:+.2f}")
            return " | ".join(parts)
        except Exception as e:
            logger.warning(f"[DailyPush] 获取隔夜中概金龙指数失败: {e}")
            return None

    def _build_nasdaq_golden_dragon_bias_item(
        self,
        snapshot: Optional[Dict[str, Any]],
    ) -> Optional[Dict[str, Any]]:
        """将隔夜金龙指数压成结构化偏向，用于归档和次日A股辅助判断。"""
        if not snapshot or snapshot.get("change_pct") is None:
            return None

        pct = float(snapshot["change_pct"])
        if pct >= 1.0:
            label = "利多"
            strength = "中"
            reasons = ["金龙上涨", "隔夜外盘情绪回暖"]
        elif pct <= -1.0:
            label = "利空"
            strength = "中"
            reasons = ["金龙下跌", "隔夜外盘风险偏好回落"]
        elif pct > 0:
            label = "利多"
            strength = "弱"
            reasons = ["金龙走强"]
        elif pct < 0:
            label = "利空"
            strength = "弱"
            reasons = ["金龙走弱"]
        else:
            label = "中性"
            strength = "弱"
            reasons = ["金龙震荡"]

        return {
            "asset_name": "纳斯达克中国金龙指数",
            "label": label,
            "strength": strength,
            "score": pct,
            "reasons": reasons,
            "trend_note": "",
            "summary": f"纳斯达克中国金龙指数参考倾向: {label} | 依据: {', '.join(reasons)}",
        }

    def _build_jin10_calendar_context(self, fetcher, asset_name: Optional[str] = None) -> Optional[str]:
        """构建供 AI 使用的 Jin10 财经日历上下文，不直接展示给用户。"""
        try:
            calendar_rows = fetcher.list_calendar(limit=30)
            indicator_rows = fetcher.list_calendar_indicators()
            if not calendar_rows or not indicator_rows:
                return None

            indicator_map = {str(item.get("id")): item for item in indicator_rows}
            asset_tags = CALENDAR_TAGS.get(asset_name or "", [])
            if not asset_tags:
                return None

            matched = []
            for item in calendar_rows:
                indicator_id = str(item.get("indicator_id", "") or "")
                indicator_meta = indicator_map.get(indicator_id, {})
                tags = indicator_meta.get("tags") or []
                if not any(tag in tags for tag in asset_tags):
                    continue

                interpretation = fetcher.get_calendar_interpretation(item.get("data_id"))
                impact_text = ""
                if interpretation:
                    impact_text = str(
                        interpretation.get("impact")
                        or interpretation.get("concern")
                        or ""
                    ).strip()

                matched.append(
                    {
                        "title": f"{item.get('country', '')}{item.get('indicator_name', '')}",
                        "time": str(item.get("pub_time", "") or ""),
                        "actual": item.get("actual"),
                        "consensus": item.get("consensus"),
                        "previous": item.get("previous"),
                        "tags": tags[:3],
                        "impact": impact_text,
                    }
                )
                if len(matched) >= 3:
                    break

            if not matched:
                return None

            lines = []
            for index, item in enumerate(matched, 1):
                value_text = (
                    f"公布={item['actual']} / 预期={item['consensus']} / 前值={item['previous']}"
                )
                impact_text = f" | 解读: {item['impact']}" if item["impact"] else ""
                tag_text = f" | 标签: {', '.join(item['tags'])}" if item["tags"] else ""
                lines.append(
                    f"{index}. {item['title']} | {item['time']} | {value_text}{tag_text}{impact_text}"
                )
            return "\n".join(lines)
        except Exception as e:
            logger.warning(f"[DailyPush] 获取 Jin10 财经日历上下文失败: {e}")
            return None

    def _build_jin10_vip_watch_context(self, fetcher, asset_name: Optional[str] = None) -> Optional[str]:
        """构建供 AI 使用的 Jin10 会员盯盘上下文，不直接展示给用户。"""
        if not self._jin10_x_token or not asset_name:
            return None

        watch_code = VIP_WATCH_CODES.get(asset_name)
        if not watch_code:
            return None

        try:
            parts = []
            resonance = fetcher.get_vip_watch_indicator_resonance(watch_code)
            bomb_events = fetcher.list_vip_watch_events(limit=2, type="bomb", code=watch_code)

            vip_signal_payload = self._build_vip_watch_signal_payload(resonance, bomb_events)
            vip_signal_summary = self._summarize_vip_watch_signal_from_payload(vip_signal_payload)
            if vip_signal_summary:
                parts.append(vip_signal_summary)
            recent_history_summary = self._summarize_recent_vip_watch_history(
                asset_name,
                current_signal_payload=vip_signal_payload,
            )
            if recent_history_summary:
                parts.append(recent_history_summary)

            if resonance:
                resonance_parts = []
                price = self._safe_float(resonance.get("price"))
                if price is not None:
                    resonance_parts.append(f"现价={self._fmt_num(price)}")

                hl_summary = self._summarize_vip_hl(resonance.get("hl"), price)
                if hl_summary:
                    resonance_parts.append(hl_summary)

                pivot_summary = self._summarize_vip_pivots(
                    resonance.get("classPivotPoint"),
                    resonance.get("woodiePivotPoint"),
                    price,
                )
                if pivot_summary:
                    resonance_parts.append(pivot_summary)

                cycle_summary = self._summarize_vip_cycle_range(resonance.get("cycleRange"), price)
                if cycle_summary:
                    resonance_parts.append(cycle_summary)

                fib_summary = self._summarize_vip_fibonacci(resonance.get("fibonacci"), price)
                if fib_summary:
                    resonance_parts.append(fib_summary)

                boll_summary = self._summarize_vip_boll(resonance.get("boll"), price)
                if boll_summary:
                    resonance_parts.append(boll_summary)

                vpc_summary = self._summarize_vip_vpc(resonance.get("vpc"), price)
                if vpc_summary:
                    resonance_parts.append(vpc_summary)

                average_summary = self._summarize_vip_averages(resonance.get("averages"), price)
                if average_summary:
                    resonance_parts.append(average_summary)

                option_summary = self._summarize_vip_option_key(resonance.get("optionKey"), price)
                if option_summary:
                    resonance_parts.append(option_summary)

                if resonance_parts:
                    parts.append("指标共振: " + " | ".join(resonance_parts))

            if bomb_events:
                event_lines = []
                for event in bomb_events:
                    data = event.get("data", {}) if isinstance(event, dict) else {}
                    title = data.get("title") or ""
                    content = data.get("content") or ""
                    extra = data.get("extra", {}) if isinstance(data, dict) else {}
                    long_order = extra.get("longOrder")
                    short_order = extra.get("shortOrder")
                    bias = ""
                    if long_order is not None and short_order is not None:
                        bias = f" 多单{long_order}%/空单{short_order}%"
                    text = title or content
                    if text:
                        event_lines.append(f"{text}{bias}".strip())
                if event_lines:
                    parts.append("资金炸弹: " + "；".join(event_lines))

            if not parts:
                return None
            return "\n".join(parts)
        except Exception as e:
            logger.warning(f"[DailyPush] 获取 Jin10 会员盯盘上下文失败: {e}")
            return None

    @staticmethod
    def _safe_float(value: Any) -> Optional[float]:
        try:
            if value is None or value == "":
                return None
            return float(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _fmt_num(value: Optional[float]) -> str:
        if value is None:
            return "-"
        if abs(value) >= 100:
            return f"{value:.2f}"
        if abs(value) >= 10:
            return f"{value:.3f}".rstrip("0").rstrip(".")
        return f"{value:.4f}".rstrip("0").rstrip(".")

    @staticmethod
    def _closest_distance_text(price: Optional[float], levels: list[float]) -> Optional[str]:
        if price is None or not levels:
            return None
        nearest = min(levels, key=lambda item: abs(item - price))
        gap_pct = ((nearest - price) / price) * 100 if price else 0.0
        direction = "上方" if nearest >= price else "下方"
        return f"{direction}{abs(gap_pct):.2f}%({nearest:.2f})"

    @staticmethod
    def _period_label(raw_type: Any) -> str:
        mapping = {
            "4": "4H",
            "5": "日线",
            "6": "周线",
            "7": "月线",
            "20": "短周期",
            "21": "中周期",
        }
        key = str(raw_type or "")
        return mapping.get(key, key or "未知周期")

    def _summarize_vip_hl(self, rows: Any, price: Optional[float]) -> Optional[str]:
        if not isinstance(rows, list) or not rows:
            return None
        focus = sorted(
            [item for item in rows if isinstance(item, dict)],
            key=lambda item: self._safe_float(item.get("weights")) or 0,
            reverse=True,
        )[:2]
        pieces = []
        for item in focus:
            low = self._safe_float(item.get("low"))
            high = self._safe_float(item.get("high"))
            if low is None or high is None:
                continue
            pos = ""
            if price is not None and high > low:
                ratio = (price - low) / (high - low)
                if ratio >= 0.7:
                    pos = "偏上沿"
                elif ratio <= 0.3:
                    pos = "偏下沿"
                else:
                    pos = "区间中部"
            pieces.append(
                f"{self._period_label(item.get('type'))}高低区={self._fmt_num(low)}~{self._fmt_num(high)}"
                + (f"({pos})" if pos else "")
            )
        if not pieces:
            return None
        return "高低区: " + "；".join(pieces)

    def _summarize_vip_pivots(
        self,
        classic_rows: Any,
        woodie_rows: Any,
        price: Optional[float],
    ) -> Optional[str]:
        pieces = []
        for label, rows in [("经典枢轴", classic_rows), ("Woodie枢轴", woodie_rows)]:
            if not isinstance(rows, list) or not rows:
                continue
            row = None
            for item in rows:
                if isinstance(item, dict) and str(item.get("type")) == "5":
                    row = item
                    break
            if row is None:
                row = rows[0] if isinstance(rows[0], dict) else None
            if row is None:
                continue
            pivot = self._safe_float(row.get("value"))
            support = self._safe_float(row.get("support1"))
            resistance = self._safe_float(row.get("resistance1"))
            distance_text = self._closest_distance_text(
                price,
                [item for item in [pivot, support, resistance] if item is not None],
            )
            pieces.append(
                f"{label}: 枢轴={self._fmt_num(pivot)} S1={self._fmt_num(support)} R1={self._fmt_num(resistance)}"
                + (f" 最近位={distance_text}" if distance_text else "")
            )
        if not pieces:
            return None
        return "枢轴带: " + "；".join(pieces)

    def _summarize_vip_cycle_range(self, rows: Any, price: Optional[float]) -> Optional[str]:
        if not isinstance(rows, list) or not rows:
            return None
        row = rows[0] if isinstance(rows[0], dict) else None
        if row is None:
            return None
        low = self._safe_float(row.get("min"))
        high = self._safe_float(row.get("max"))
        if low is None or high is None:
            return None
        pos = ""
        if price is not None and high > low:
            ratio = (price - low) / (high - low)
            pos = f"位置={ratio * 100:.1f}%"
        return (
            f"周期区间: {self._period_label(row.get('type'))}={self._fmt_num(low)}~{self._fmt_num(high)}"
            + (f" | {pos}" if pos else "")
        )

    def _summarize_vip_fibonacci(self, rows: Any, price: Optional[float]) -> Optional[str]:
        if not isinstance(rows, list) or not rows:
            return None
        row = rows[0] if isinstance(rows[0], dict) else None
        if row is None:
            return None
        levels = [self._safe_float(item) for item in row.get("levels") or []]
        levels = [item for item in levels if item is not None]
        if not levels:
            return None
        nearest = self._closest_distance_text(price, levels)
        return (
            f"斐波那契: {self._period_label(row.get('type'))} 共{len(levels)}层"
            + (f" | 最近位={nearest}" if nearest else "")
        )

    def _summarize_vip_boll(self, rows: Any, price: Optional[float]) -> Optional[str]:
        if not isinstance(rows, list) or not rows:
            return None
        row = rows[0] if isinstance(rows[0], dict) else None
        if row is None:
            return None
        middle = self._safe_float(row.get("middleBand"))
        upper = self._safe_float(row.get("upperBand"))
        lower = self._safe_float(row.get("lowerBand"))
        if middle is None or upper is None or lower is None:
            return None
        width_pct = ((upper - lower) / middle * 100) if middle else None
        state = ""
        if price is not None:
            if price >= upper:
                state = "触及上轨"
            elif price <= lower:
                state = "触及下轨"
            elif price >= middle:
                state = "中轨上方"
            else:
                state = "中轨下方"
        return (
            f"布林带: 中轨={self._fmt_num(middle)} 上轨={self._fmt_num(upper)} 下轨={self._fmt_num(lower)}"
            + (f" | 带宽={width_pct:.2f}%" if width_pct is not None else "")
            + (f" | {state}" if state else "")
        )

    def _summarize_vip_vpc(self, rows: Any, price: Optional[float]) -> Optional[str]:
        if not isinstance(rows, list) or not rows:
            return None
        row = rows[0] if isinstance(rows[0], dict) else None
        if row is None:
            return None
        values = [self._safe_float(item) for item in row.get("value") or []]
        values = [item for item in values if item is not None]
        if not values:
            return None
        nearest = self._closest_distance_text(price, values)
        return (
            f"VPC筹码: {self._period_label(row.get('type'))} 共{len(values)}层"
            + (f" | 最近密集位={nearest}" if nearest else "")
        )

    def _summarize_vip_averages(self, rows: Any, price: Optional[float]) -> Optional[str]:
        if not isinstance(rows, list) or not rows:
            return None
        row = rows[0] if isinstance(rows[0], dict) else None
        if row is None:
            return None
        values = [self._safe_float(item) for item in row.get("value") or []]
        values = [item for item in values if item is not None]
        if len(values) < 3:
            return None
        ma_short = values[0]
        ma_mid = values[2]
        ma_long = values[-1]
        trend = "多头" if ma_short >= ma_mid >= ma_long else "空头" if ma_short <= ma_mid <= ma_long else "分化"
        nearest = self._closest_distance_text(price, values[:4])
        return (
            f"均线簇: 短={self._fmt_num(ma_short)} 中={self._fmt_num(ma_mid)} 长={self._fmt_num(ma_long)} | 排列={trend}"
            + (f" | 最近均线={nearest}" if nearest else "")
        )

    def _summarize_vip_option_key(self, rows: Any, price: Optional[float]) -> Optional[str]:
        if not isinstance(rows, list) or not rows:
            return None
        levels = []
        labels = []
        for item in rows[:3]:
            if not isinstance(item, dict):
                continue
            max_price = self._safe_float(item.get("maxPrice"))
            min_price = self._safe_float(item.get("minPrice"))
            rank = str(item.get("rankWriting") or "关键位")
            if max_price is not None and min_price is not None:
                labels.append(f"{rank}:{self._fmt_num(min_price)}~{self._fmt_num(max_price)}")
                levels.extend([min_price, max_price])
            elif min_price is not None:
                labels.append(f"{rank}:{self._fmt_num(min_price)}")
                levels.append(min_price)
            elif max_price is not None:
                labels.append(f"{rank}:{self._fmt_num(max_price)}")
                levels.append(max_price)
        if not labels:
            return None
        nearest = self._closest_distance_text(price, levels)
        return "期权关键位: " + "；".join(labels) + (f" | 最近位={nearest}" if nearest else "")

    def _summarize_vip_watch_signal(self, resonance: Any, bomb_events: Any) -> Optional[str]:
        signal_payload = self._build_vip_watch_signal_payload(resonance, bomb_events)
        return self._summarize_vip_watch_signal_from_payload(signal_payload)

    def _summarize_vip_watch_signal_from_payload(
        self,
        signal_payload: Optional[Dict[str, Any]],
    ) -> Optional[str]:
        if not signal_payload:
            return None
        score = signal_payload["score"]
        strength = signal_payload["strength"]
        signals = signal_payload["signals"]
        return f"会员盯盘信号: {strength}({score:+.1f}) | " + " | ".join(signals[:5])

    def _build_vip_watch_signal_payload(self, resonance: Any, bomb_events: Any) -> Optional[Dict[str, Any]]:
        if not isinstance(resonance, dict):
            return None

        price = self._safe_float(resonance.get("price"))
        if price is None:
            return None

        signal_items = []

        trend_signal = self._infer_average_trend_signal(rows=resonance.get("averages"), price=price)
        if trend_signal:
            signal_items.append(trend_signal)

        support_resistance_signal = self._infer_support_resistance_signal(resonance, price)
        if support_resistance_signal:
            signal_items.append(support_resistance_signal)

        volatility_signal = self._infer_volatility_signal(resonance.get("boll"))
        if volatility_signal:
            signal_items.append(volatility_signal)

        option_signal = self._infer_option_key_signal(resonance.get("optionKey"), price)
        if option_signal:
            signal_items.append(option_signal)

        flow_signal = self._infer_bomb_flow_signal(bomb_events)
        if flow_signal:
            signal_items.append(flow_signal)

        if not signal_items:
            return None

        score = sum(item["score"] for item in signal_items)
        score = max(-2.0, min(2.0, score))
        if score >= 1.0:
            strength = "强利多"
        elif score >= 0.3:
            strength = "弱利多"
        elif score <= -1.0:
            strength = "强利空"
        elif score <= -0.3:
            strength = "弱利空"
        else:
            strength = "中性"

        return {
            "score": score,
            "strength": strength,
            "signals": [item["text"] for item in signal_items],
        }

    def _record_vip_watch_signal_history(self, asset_name: str, signal_payload: Optional[Dict[str, Any]]) -> None:
        if not signal_payload:
            return
        try:
            VIP_WATCH_SIGNAL_HISTORY_PATH.parent.mkdir(parents=True, exist_ok=True)
            record = {
                "timestamp": datetime.now().isoformat(timespec="seconds"),
                "asset_name": asset_name,
                "score": signal_payload.get("score"),
                "strength": signal_payload.get("strength"),
                "signals": signal_payload.get("signals") or [],
            }
            with VIP_WATCH_SIGNAL_HISTORY_PATH.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(record, ensure_ascii=False) + "\n")
        except Exception as exc:
            logger.warning("[DailyPush] 记录会员盯盘信号历史失败: %s", exc)

    def _load_recent_vip_watch_signal_history(self, asset_name: str, limit: int = 5) -> list[Dict[str, Any]]:
        if not VIP_WATCH_SIGNAL_HISTORY_PATH.exists():
            return []
        records: list[Dict[str, Any]] = []
        try:
            with VIP_WATCH_SIGNAL_HISTORY_PATH.open("r", encoding="utf-8") as handle:
                for line in handle:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        record = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if record.get("asset_name") != asset_name:
                        continue
                    records.append(record)
        except Exception as exc:
            logger.warning("[DailyPush] 读取会员盯盘信号历史失败: %s", exc)
            return []
        return records[-limit:]

    def _summarize_recent_vip_watch_history(
        self,
        asset_name: str,
        current_signal_payload: Optional[Dict[str, Any]] = None,
    ) -> Optional[str]:
        history = self._load_recent_vip_watch_signal_history(asset_name, limit=3)
        sequence = [
            {
                "strength": str(item.get("strength") or "中性"),
                "score": self._safe_float(item.get("score")) or 0.0,
            }
            for item in history
        ]
        if current_signal_payload:
            sequence.append(
                {
                    "strength": str(current_signal_payload.get("strength") or "中性"),
                    "score": self._safe_float(current_signal_payload.get("score")) or 0.0,
                }
            )
        if len(sequence) < 2:
            return None

        recent = sequence[-3:]
        chain = " -> ".join(f"{item['strength']}({item['score']:+.1f})" for item in recent)
        latest_score = recent[-1]["score"]
        prev_score = recent[-2]["score"]
        if latest_score > prev_score + 0.2:
            change = "最新较前次转强"
        elif latest_score < prev_score - 0.2:
            change = "最新较前次转弱"
        else:
            change = "最新与前次接近"

        if len(recent) >= 3:
            scores = [item["score"] for item in recent]
            if scores[0] < scores[1] < scores[2]:
                change += "，连续走强"
            elif scores[0] > scores[1] > scores[2]:
                change += "，连续走弱"

        return f"历史序列: 近{len(recent)}次={chain} | {change}"

    def _build_recent_vip_watch_trend_note(
        self,
        asset_name: str,
        current_signal_payload: Optional[Dict[str, Any]] = None,
    ) -> Optional[str]:
        history = self._load_recent_vip_watch_signal_history(asset_name, limit=3)
        sequence = [
            self._safe_float(item.get("score")) or 0.0
            for item in history
        ]
        if current_signal_payload:
            sequence.append(self._safe_float(current_signal_payload.get("score")) or 0.0)
        if len(sequence) < 2:
            return None

        recent = sequence[-3:]
        latest_score = recent[-1]
        prev_score = recent[-2]
        notes = []
        if latest_score > prev_score + 0.2:
            notes.append("最新转强")
        elif latest_score < prev_score - 0.2:
            notes.append("最新转弱")

        if len(recent) >= 3:
            if recent[0] < recent[1] < recent[2]:
                notes.append("连续走强")
            elif recent[0] > recent[1] > recent[2]:
                notes.append("连续走弱")
        if not notes:
            return None
        return "，".join(notes)

    def _build_vip_watch_action_note(
        self,
        asset_name: str,
        signal_payload: Optional[Dict[str, Any]],
        trend_note: Optional[str] = None,
    ) -> Optional[str]:
        if asset_name not in VIP_WATCH_CODES or not signal_payload:
            return None
        score = self._safe_float(signal_payload.get("score")) or 0.0
        strength = str(signal_payload.get("strength") or "中性")
        trend_note = trend_note or ""

        if score >= 1.0:
            action = "顺势为主，强势回踩再看低吸"
        elif score >= 0.3:
            action = "偏多观察，回踩支撑再考虑跟随"
        elif score <= -1.0:
            action = "先控节奏，急拉不追，等待压力释放"
        elif score <= -0.3:
            action = "以观察为主，反弹先看压力位反馈"
        else:
            action = "多空暂未拉开，先观察关键位"

        if "连续走强" in trend_note and score > 0:
            action += "；短线偏向顺势"
        elif "连续走弱" in trend_note and score < 0:
            action += "；短线偏向防守"
        elif "最新转强" in trend_note and score >= 0:
            action += "；留意节奏转暖"
        elif "最新转弱" in trend_note and score <= 0:
            action += "；留意动能回落"

        return f"会员盯盘建议: {strength} | {action}"

    def _infer_average_trend_signal(self, rows: Any, price: Optional[float]) -> Optional[Dict[str, Any]]:
        if price is None or not isinstance(rows, list) or not rows or not isinstance(rows[0], dict):
            return None
        values = [self._safe_float(item) for item in rows[0].get("value") or []]
        values = [item for item in values if item is not None]
        if len(values) < 3:
            return None
        short = values[0]
        mid = values[2]
        long = values[-1]
        if price >= short >= mid:
            return {"text": "趋势偏强", "score": 0.7}
        if price <= short <= mid:
            return {"text": "趋势偏弱", "score": -0.7}
        if short >= mid >= long:
            return {"text": "均线多头", "score": 0.5}
        if short <= mid <= long:
            return {"text": "均线空头", "score": -0.5}
        return {"text": "均线分化", "score": 0.0}

    def _infer_support_resistance_signal(self, resonance: Any, price: float) -> Optional[Dict[str, Any]]:
        support_levels: list[float] = []
        resistance_levels: list[float] = []

        for row in (resonance.get("classPivotPoint") or [])[:2]:
            if not isinstance(row, dict):
                continue
            for key in ["support1", "support2"]:
                value = self._safe_float(row.get(key))
                if value is not None and value < price:
                    support_levels.append(value)
            for key in ["resistance1", "resistance2"]:
                value = self._safe_float(row.get(key))
                if value is not None and value > price:
                    resistance_levels.append(value)

        boll_rows = resonance.get("boll") or []
        if boll_rows and isinstance(boll_rows[0], dict):
            lower = self._safe_float(boll_rows[0].get("lowerBand"))
            upper = self._safe_float(boll_rows[0].get("upperBand"))
            if lower is not None and lower < price:
                support_levels.append(lower)
            if upper is not None and upper > price:
                resistance_levels.append(upper)

        for row in (resonance.get("optionKey") or [])[:3]:
            if not isinstance(row, dict):
                continue
            for key in ["minPrice", "maxPrice"]:
                value = self._safe_float(row.get(key))
                if value is None:
                    continue
                if value < price:
                    support_levels.append(value)
                elif value > price:
                    resistance_levels.append(value)

        nearest_support = max(support_levels) if support_levels else None
        nearest_resistance = min(resistance_levels) if resistance_levels else None
        support_gap = ((price - nearest_support) / price * 100) if nearest_support is not None else None
        resistance_gap = ((nearest_resistance - price) / price * 100) if nearest_resistance is not None else None

        if support_gap is not None and support_gap <= 0.6:
            return {"text": f"临近支撑{self._fmt_num(nearest_support)}", "score": 0.5}
        if resistance_gap is not None and resistance_gap <= 0.6:
            return {"text": f"临近压力{self._fmt_num(nearest_resistance)}", "score": -0.5}
        if support_gap is not None and resistance_gap is not None and resistance_gap < support_gap:
            return {"text": "压力侧更近", "score": -0.3}
        if support_gap is not None and resistance_gap is not None and support_gap < resistance_gap:
            return {"text": "支撑侧更近", "score": 0.3}
        return None

    def _infer_volatility_signal(self, rows: Any) -> Optional[Dict[str, Any]]:
        if not isinstance(rows, list) or not rows or not isinstance(rows[0], dict):
            return None
        middle = self._safe_float(rows[0].get("middleBand"))
        upper = self._safe_float(rows[0].get("upperBand"))
        lower = self._safe_float(rows[0].get("lowerBand"))
        if middle is None or upper is None or lower is None or middle == 0:
            return None
        width_pct = (upper - lower) / middle * 100
        if width_pct >= 2.0:
            return {"text": f"波动放大({width_pct:.2f}%)", "score": -0.2}
        if width_pct <= 0.8:
            return {"text": f"波动收敛({width_pct:.2f}%)", "score": 0.2}
        return {"text": f"波动中性({width_pct:.2f}%)", "score": 0.0}

    def _infer_option_key_signal(self, rows: Any, price: float) -> Optional[Dict[str, Any]]:
        if not isinstance(rows, list) or not rows:
            return None
        levels = []
        for row in rows[:3]:
            if not isinstance(row, dict):
                continue
            for key in ["minPrice", "maxPrice"]:
                value = self._safe_float(row.get(key))
                if value is not None:
                    levels.append(value)
        if not levels:
            return None
        nearest = min(levels, key=lambda item: abs(item - price))
        gap_pct = abs(nearest - price) / price * 100 if price else 0.0
        if gap_pct <= 1.0:
            direction = "上方" if nearest >= price else "下方"
            score = -0.3 if nearest >= price else 0.3
            return {"text": f"期权关键位{direction}贴近({self._fmt_num(nearest)})", "score": score}
        return None

    def _infer_bomb_flow_signal(self, bomb_events: Any) -> Optional[Dict[str, Any]]:
        if not isinstance(bomb_events, list) or not bomb_events:
            return None
        long_scores = []
        short_scores = []
        for event in bomb_events:
            data = event.get("data", {}) if isinstance(event, dict) else {}
            extra = data.get("extra", {}) if isinstance(data, dict) else {}
            long_order = self._safe_float(extra.get("longOrder"))
            short_order = self._safe_float(extra.get("shortOrder"))
            if long_order is not None:
                long_scores.append(long_order)
            if short_order is not None:
                short_scores.append(short_order)
        if not long_scores or not short_scores:
            return None
        avg_long = sum(long_scores) / len(long_scores)
        avg_short = sum(short_scores) / len(short_scores)
        diff = avg_long - avg_short
        if diff >= 10:
            return {"text": f"资金偏多({avg_long:.1f}%/{avg_short:.1f}%)", "score": 0.8}
        if diff <= -10:
            return {"text": f"资金偏空({avg_long:.1f}%/{avg_short:.1f}%)", "score": -0.8}
        return {"text": f"资金均衡({avg_long:.1f}%/{avg_short:.1f}%)", "score": 0.0}

    @staticmethod
    def _score_bias_keywords(text: str, weighted_keywords: Dict[str, int]) -> tuple[list[str], int]:
        hits = [keyword for keyword in weighted_keywords if keyword in text]
        score = sum(weighted_keywords[keyword] for keyword in hits)
        return hits, score

    @staticmethod
    def _score_vip_trend_note(vip_trend_note: Optional[str]) -> tuple[int, list[str]]:
        """近期盯盘趋势比旧新闻关键词更及时，用来校正利多/利空方向。"""
        if not vip_trend_note:
            return 0, []

        score = 0
        hits: list[str] = []
        trend_weights = {
            "连续走强": 7,
            "连续走弱": -7,
            "最新转强": 3,
            "最新转弱": -3,
        }
        for keyword, weight in trend_weights.items():
            if keyword in vip_trend_note:
                score += weight
                hits.append(keyword)
        return score, hits

    def _summarize_macro_bias(
        self,
        asset_name: str,
        *,
        flash_context: Optional[str] = None,
        calendar_context: Optional[str] = None,
        vip_watch_context: Optional[str] = None,
        vip_trend_note: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """将 Jin10 参考压缩成利多/利空/中性的内部摘要。"""
        rules = ASSET_BIAS_RULES.get(asset_name)
        if not rules:
            return None

        text = "\n".join(
            part for part in [flash_context, calendar_context, vip_watch_context] if part
        ).strip()
        if not text:
            return None

        bullish_hits, bullish_score = self._score_bias_keywords(text, rules["bullish"])
        bearish_hits, bearish_score = self._score_bias_keywords(text, rules["bearish"])
        keyword_score = bullish_score - bearish_score
        trend_score_adjustment, trend_hits = self._score_vip_trend_note(vip_trend_note)
        score = keyword_score + trend_score_adjustment
        if score > 0:
            label = "利多"
        elif score < 0:
            label = "利空"
        else:
            label = "中性"

        if abs(score) >= 4:
            strength = "强"
        elif abs(score) >= 2:
            strength = "中"
        else:
            strength = "弱"

        directional_trend_hits = [
            hit for hit in trend_hits
            if (score >= 0 and "强" in hit) or (score < 0 and "弱" in hit)
        ]
        keyword_reasons = bullish_hits if score >= 0 else bearish_hits
        reasons = (directional_trend_hits + keyword_reasons)[:4]
        summary = f"{asset_name}参考倾向: {label}"
        if reasons:
            summary += f" | 依据: {', '.join(reasons)}"
        return {
            "asset_name": asset_name,
            "label": label,
            "strength": strength,
            "score": score,
            "keyword_score": keyword_score,
            "trend_score_adjustment": trend_score_adjustment,
            "reasons": reasons,
            "trend_note": vip_trend_note,
            "summary": summary,
        }

    @staticmethod
    def _format_macro_bias_section(bias_items: list[dict]) -> Optional[str]:
        """将多个资产的利多/利空判断格式化为推送首段。"""
        if not bias_items:
            return None

        lines = ["**宏观利多利空摘要**"]
        for item in bias_items:
            reasons = f" | 依据: {', '.join(item['reasons'])}" if item.get("reasons") else ""
            strength = f"({item['strength']})" if item.get("strength") and item.get("label") != "中性" else ""
            trend_note = f" | 趋势: {item['trend_note']}" if item.get("trend_note") else ""
            lines.append(f"> **{item['asset_name']}**: {item['label']}{strength}{reasons}{trend_note}")
        return "\n".join(lines)

    def build_market_summary_payload(self) -> Optional[Dict[str, Any]]:
        """生成市场品种推送正文与结构化宏观摘要。"""
        from src.market_data_fetcher import MarketDataFetcher

        fetcher = MarketDataFetcher(self._jin10_key, self._jin10_x_token)
        try:
            sections = []
            bias_items = []
            golden_dragon_snapshot = fetcher.get_nasdaq_golden_dragon_snapshot()

            for asset_cfg in MARKET_ASSETS:
                name = asset_cfg["name"]
                logger.info(f"[DailyPush] 开始分析 {name}...")

                analysis = self._analyze_asset(fetcher, asset_cfg)
                if analysis:
                    # 尝试 AI 预测
                    flash_context = self._build_jin10_flash_context(fetcher, name)
                    calendar_context = self._build_jin10_calendar_context(fetcher, name)
                    vip_watch_context = self._build_jin10_vip_watch_context(fetcher, name)
                    overnight_flash_context = self._build_overnight_jin10_a_share_context(fetcher, name)
                    golden_dragon_context = self._build_nasdaq_golden_dragon_context(
                        golden_dragon_snapshot, name
                    )
                    vip_signal_payload = None
                    vip_trend_note = None
                    vip_action_note = None
                    watch_code = VIP_WATCH_CODES.get(name)
                    if watch_code and self._jin10_x_token:
                        vip_signal_payload = self._build_vip_watch_signal_payload(
                            fetcher.get_vip_watch_indicator_resonance(watch_code),
                            fetcher.list_vip_watch_events(limit=2, type="bomb", code=watch_code),
                        )
                        vip_trend_note = self._build_recent_vip_watch_trend_note(
                            name,
                            current_signal_payload=vip_signal_payload,
                        )
                        vip_action_note = self._build_vip_watch_action_note(
                            name,
                            vip_signal_payload,
                            trend_note=vip_trend_note,
                        )
                        self._record_vip_watch_signal_history(name, vip_signal_payload)
                    bias_context = self._summarize_macro_bias(
                        name,
                        flash_context=flash_context,
                        calendar_context="\n".join(
                            part for part in [calendar_context, overnight_flash_context, golden_dragon_context] if part
                        ) or None,
                        vip_watch_context=vip_watch_context,
                        vip_trend_note=vip_trend_note,
                    )
                    if bias_context:
                        bias_items.append(bias_context)
                    if name == "中证500指数":
                        golden_dragon_bias = self._build_nasdaq_golden_dragon_bias_item(
                            golden_dragon_snapshot
                        )
                        if golden_dragon_bias:
                            bias_items.append(golden_dragon_bias)
                    if vip_action_note:
                        analysis += f"\n> **会员盯盘建议**: {vip_action_note}"
                    jin10_parts = [
                        part for part in [
                            bias_context["summary"] if bias_context else None,
                            flash_context,
                            calendar_context,
                            overnight_flash_context,
                            golden_dragon_context,
                            vip_watch_context,
                        ] if part
                    ]
                    jin10_context = "\n".join(jin10_parts) if jin10_parts else None
                    ai_text = self._ai_predict(name, analysis, jin10_context=jin10_context) if self._ai_enabled else None
                    if ai_text:
                        analysis += f"\n> **AI预测**: {ai_text}"
                    sections.append(analysis)
                else:
                    logger.warning(f"[DailyPush] {name} 分析失败，跳过")

            # 金银比
            if len(sections) >= 2:
                try:
                    import akshare as ak
                    au_df = ak.futures_main_sina(symbol="AU0")
                    ag_df = ak.futures_main_sina(symbol="AG0")
                    if au_df is not None and ag_df is not None and len(au_df) > 0 and len(ag_df) > 0:
                        au_close = float(au_df.iloc[-1]["收盘价"])
                        ag_close = float(ag_df.iloc[-1]["收盘价"])
                        # 金银比 = 黄金价格/白银价格（同单位）
                        ratio = (au_close * 1000) / ag_close if ag_close > 0 else 0
                        sections.append(
                            f"**金银比**\n"
                            f"> 黄金: {au_close:.2f} 元/克 | 白银: {ag_close:.0f} 元/千克\n"
                            f"> 金银比: **{ratio:.1f}**"
                        )
                except Exception:
                    pass

            if sections:
                header_suffix = "技术指标 + AI预测" if self._ai_enabled else "技术指标摘要"
                header = (
                    f"**📊 每日市场品种详细分析**\n"
                    f"> {datetime.now().strftime('%Y-%m-%d %H:%M')} | "
                    f"{header_suffix}\n"
                )
                macro_bias_section = self._format_macro_bias_section(bias_items)
                ordered_sections = [part for part in [macro_bias_section, *sections] if part]
                return {
                    "content": header + "\n\n---\n\n".join(ordered_sections),
                    "macro_bias_items": bias_items,
                }

            logger.warning("市场品种分析全部失败，跳过推送")
            return None

        except Exception as e:
            logger.error(f"市场品种分析推送失败: {e}")
            return None
        finally:
            fetcher.close()

    def build_market_summary(self) -> Optional[str]:
        """生成市场品种详细分析正文，不直接发送。"""
        payload = self.build_market_summary_payload()
        if not payload:
            return None
        return payload["content"]

    def push_market_summary(self):
        """推送市场品种详细分析（黄金+白银+中证500）"""
        content = self.build_market_summary()
        if content:
            self._notifier.send(content)
            logger.info("市场品种详细分析推送成功")
