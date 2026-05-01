# -*- coding: utf-8 -*-
"""
===================================
趋势交易分析器 - 基于用户交易理念
===================================

交易理念核心原则：
1. 严进策略 - 不追高，追求每笔交易成功率
2. 趋势交易 - MA5>MA10>MA20 多头排列，顺势而为
3. 效率优先 - 关注筹码结构好的股票
4. 买点偏好 - 在 MA5/MA10 附近回踩买入

技术标准：
- 多头排列：MA5 > MA10 > MA20
- 乖离率：(Close - MA5) / MA5 < 5%（不追高）
- 量能形态：缩量回调优先
"""

import logging
from dataclasses import dataclass, field
from typing import Dict, Any, List
from enum import Enum

import pandas as pd
import numpy as np

from src.config import get_config

logger = logging.getLogger(__name__)


class TrendStatus(Enum):
    """趋势状态枚举"""
    STRONG_BULL = "强势多头"      # MA5 > MA10 > MA20，且间距扩大
    BULL = "多头排列"             # MA5 > MA10 > MA20
    WEAK_BULL = "弱势多头"        # MA5 > MA10，但 MA10 < MA20
    CONSOLIDATION = "盘整"        # 均线缠绕
    WEAK_BEAR = "弱势空头"        # MA5 < MA10，但 MA10 > MA20
    BEAR = "空头排列"             # MA5 < MA10 < MA20
    STRONG_BEAR = "强势空头"      # MA5 < MA10 < MA20，且间距扩大


class VolumeStatus(Enum):
    """量能状态枚举"""
    HEAVY_VOLUME_UP = "放量上涨"       # 量价齐升
    HEAVY_VOLUME_DOWN = "放量下跌"     # 放量杀跌
    SHRINK_VOLUME_UP = "缩量上涨"      # 无量上涨
    SHRINK_VOLUME_DOWN = "缩量回调"    # 缩量回调（好）
    NORMAL = "量能正常"


class BuySignal(Enum):
    """买入信号枚举"""
    STRONG_BUY = "强烈买入"       # 多条件满足
    BUY = "买入"                  # 基本条件满足
    HOLD = "持有"                 # 已持有可继续
    WAIT = "观望"                 # 等待更好时机
    SELL = "卖出"                 # 趋势转弱
    STRONG_SELL = "强烈卖出"      # 趋势破坏


class MACDStatus(Enum):
    """MACD状态枚举"""
    GOLDEN_CROSS_ZERO = "零轴上金叉"      # DIF上穿DEA，且在零轴上方
    GOLDEN_CROSS = "金叉"                # DIF上穿DEA
    BULLISH = "多头"                    # DIF>DEA>0
    CROSSING_UP = "上穿零轴"             # DIF上穿零轴
    CROSSING_DOWN = "下穿零轴"           # DIF下穿零轴
    BEARISH = "空头"                    # DIF<DEA<0
    DEATH_CROSS = "死叉"                # DIF下穿DEA


class RSIStatus(Enum):
    """RSI状态枚举"""
    OVERBOUGHT = "超买"        # RSI > 70
    STRONG_BUY = "强势买入"    # 50 < RSI < 70
    NEUTRAL = "中性"          # 40 <= RSI <= 60
    WEAK = "弱势"             # 30 < RSI < 40
    OVERSOLD = "超卖"         # RSI < 30


class BollStatus(Enum):
    """布林带状态枚举"""
    ABOVE_UPPER = "突破上轨"       # 价格突破上轨，可能超买
    NEAR_UPPER = "接近上轨"         # 接近上轨
    MID_UPPER = "中上轨之间"        # 在中轨和上轨之间
    NEAR_LOWER = "接近下轨"         # 接近下轨
    BELOW_LOWER = "跌破下轨"        # 价格跌破下轨，可能超卖
    SQUEEZE = "布林收窄"            # 带宽收窄，可能变盘

class KDJStatus(Enum):
    """KDJ状态枚举"""
    GOLDEN_CROSS = "金叉"           # K上穿D
    DEATH_CROSS = "死叉"            # K下穿D
    OVERBOUGHT = "超买"             # K>80, D>80
    OVERSOLD = "超卖"               # K<20, D<20
    STRONG = "强势"                 # K>D>50
    WEAK = "弱势"                   # K<D<50
    NEUTRAL = "中性"

class OBVStatus(Enum):
    """OBV状态枚举"""
    BULLISH_DIVERGENCE = "底背离"   # 价格新低但OBV未新低
    BEARISH_DIVERGENCE = "顶背离"   # 价格新高但OBV未新高
    RISING = "上升"                 # OBV趋势上升
    FALLING = "下降"                # OBV趋势下降
    FLAT = "平稳"


class RSRSStatus(Enum):
    """RSRS择时状态枚举"""
    STRONG_BUY = "强烈买入"       # 右偏标准分极低，底部信号
    BUY = "买入"                  # 右偏标准分较低
    HOLD = "持有"                 # 中性区间
    SELL = "卖出"                 # 右偏标准分较高
    STRONG_SELL = "强烈卖出"      # 右偏标准分极高，顶部信号


@dataclass
class TrendAnalysisResult:
    """趋势分析结果"""
    code: str
    
    # 趋势判断
    trend_status: TrendStatus = TrendStatus.CONSOLIDATION
    ma_alignment: str = ""           # 均线排列描述
    trend_strength: float = 0.0      # 趋势强度 0-100
    
    # 均线数据
    ma5: float = 0.0
    ma10: float = 0.0
    ma20: float = 0.0
    ma60: float = 0.0
    current_price: float = 0.0
    
    # 乖离率（与 MA5 的偏离度）
    bias_ma5: float = 0.0            # (Close - MA5) / MA5 * 100
    bias_ma10: float = 0.0
    bias_ma20: float = 0.0
    
    # 量能分析
    volume_status: VolumeStatus = VolumeStatus.NORMAL
    volume_ratio_5d: float = 0.0     # 当日成交量/5日均量
    volume_trend: str = ""           # 量能趋势描述
    
    # 支撑压力
    support_ma5: bool = False        # MA5 是否构成支撑
    support_ma10: bool = False       # MA10 是否构成支撑
    resistance_levels: List[float] = field(default_factory=list)
    support_levels: List[float] = field(default_factory=list)

    # MACD 指标
    macd_dif: float = 0.0          # DIF 快线
    macd_dea: float = 0.0          # DEA 慢线
    macd_bar: float = 0.0           # MACD 柱状图
    macd_status: MACDStatus = MACDStatus.BULLISH
    macd_signal: str = ""            # MACD 信号描述

    # RSI 指标
    rsi_6: float = 0.0              # RSI(6) 短期
    rsi_12: float = 0.0             # RSI(12) 中期
    rsi_24: float = 0.0             # RSI(24) 长期
    rsi_status: RSIStatus = RSIStatus.NEUTRAL
    rsi_signal: str = ""              # RSI 信号描述

    # 布林带指标
    boll_upper: float = 0.0          # 布林带上轨
    boll_mid: float = 0.0            # 布林带中轨（MA20）
    boll_lower: float = 0.0          # 布林带下轨
    boll_width: float = 0.0          # 布林带宽度（上轨-下轨）/中轨
    boll_pctb: float = 0.0           # %B指标 (close-lower)/(upper-lower)
    boll_status: BollStatus = BollStatus.NEAR_UPPER
    boll_signal: str = ""

    # KDJ 指标
    kdj_k: float = 0.0              # K值
    kdj_d: float = 0.0              # D值
    kdj_j: float = 0.0              # J值
    kdj_status: KDJStatus = KDJStatus.NEUTRAL
    kdj_signal: str = ""

    # ATR 指标
    atr_14: float = 0.0             # 14日平均真实波幅
    atr_percent: float = 0.0        # ATR占价格百分比

    # OBV 指标
    obv: float = 0.0                 # OBV值
    obv_ma20: float = 0.0           # OBV的20日均线
    obv_status: OBVStatus = OBVStatus.FLAT
    obv_signal: str = ""

    # RSRS 择时指标
    rsrs_beta: float = 0.0            # RSRS 斜率（β值）
    rsrs_zscore: float = 0.0          # 标准化分
    rsrs_r2_weighted: float = 0.0     # R²加权标准分
    rsrs_r2: float = 0.0              # R²决定系数
    rsrs_status: RSRSStatus = RSRSStatus.HOLD
    rsrs_signal: str = ""

    # 扩展指标：趋势强度、资金流、突破与过热过滤
    adx_14: float = 0.0
    plus_di_14: float = 0.0
    minus_di_14: float = 0.0
    adx_signal: str = ""
    mfi_14: float = 50.0
    mfi_signal: str = ""
    cci_20: float = 0.0
    cci_signal: str = ""
    roc_12: float = 0.0
    roc_signal: str = ""
    donchian_upper_20: float = 0.0
    donchian_lower_20: float = 0.0
    donchian_mid_20: float = 0.0
    donchian_signal: str = ""
    williams_r_14: float = -50.0
    williams_signal: str = ""
    stoch_rsi_14: float = 50.0
    stoch_rsi_signal: str = ""
    cmf_20: float = 0.0
    cmf_signal: str = ""
    vwap_20: float = 0.0
    vwap_distance_pct: float = 0.0
    vwap_signal: str = ""

    # 多指标共振摘要：仅做解释与风控过滤，不直接覆盖原始评分
    indicator_consensus_score: float = 0.0
    indicator_consensus_signal: str = ""
    indicator_bullish_count: int = 0
    indicator_bearish_count: int = 0
    indicator_neutral_count: int = 0
    indicator_conflict_level: str = ""
    indicator_consensus_details: List[str] = field(default_factory=list)

    # 买入信号
    buy_signal: BuySignal = BuySignal.WAIT
    signal_score: int = 0            # 综合评分 0-100
    signal_reasons: List[str] = field(default_factory=list)
    risk_factors: List[str] = field(default_factory=list)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'code': self.code,
            'trend_status': self.trend_status.value,
            'ma_alignment': self.ma_alignment,
            'trend_strength': self.trend_strength,
            'ma5': self.ma5,
            'ma10': self.ma10,
            'ma20': self.ma20,
            'ma60': self.ma60,
            'current_price': self.current_price,
            'bias_ma5': self.bias_ma5,
            'bias_ma10': self.bias_ma10,
            'bias_ma20': self.bias_ma20,
            'volume_status': self.volume_status.value,
            'volume_ratio_5d': self.volume_ratio_5d,
            'volume_trend': self.volume_trend,
            'support_ma5': self.support_ma5,
            'support_ma10': self.support_ma10,
            'buy_signal': self.buy_signal.value,
            'signal_score': self.signal_score,
            'signal_reasons': self.signal_reasons,
            'risk_factors': self.risk_factors,
            'macd_dif': self.macd_dif,
            'macd_dea': self.macd_dea,
            'macd_bar': self.macd_bar,
            'macd_status': self.macd_status.value,
            'macd_signal': self.macd_signal,
            'rsi_6': self.rsi_6,
            'rsi_12': self.rsi_12,
            'rsi_24': self.rsi_24,
            'rsi_status': self.rsi_status.value,
            'rsi_signal': self.rsi_signal,
            'boll_upper': self.boll_upper,
            'boll_mid': self.boll_mid,
            'boll_lower': self.boll_lower,
            'boll_width': self.boll_width,
            'boll_pctb': self.boll_pctb,
            'boll_status': self.boll_status.value,
            'boll_signal': self.boll_signal,
            'kdj_k': self.kdj_k,
            'kdj_d': self.kdj_d,
            'kdj_j': self.kdj_j,
            'kdj_status': self.kdj_status.value,
            'kdj_signal': self.kdj_signal,
            'atr_14': self.atr_14,
            'atr_percent': self.atr_percent,
            'obv': self.obv,
            'obv_ma20': self.obv_ma20,
            'obv_status': self.obv_status.value,
            'obv_signal': self.obv_signal,
            'rsrs_beta': self.rsrs_beta,
            'rsrs_zscore': self.rsrs_zscore,
            'rsrs_r2_weighted': self.rsrs_r2_weighted,
            'rsrs_r2': self.rsrs_r2,
            'rsrs_status': self.rsrs_status.value,
            'rsrs_signal': self.rsrs_signal,
            'adx_14': self.adx_14,
            'plus_di_14': self.plus_di_14,
            'minus_di_14': self.minus_di_14,
            'adx_signal': self.adx_signal,
            'mfi_14': self.mfi_14,
            'mfi_signal': self.mfi_signal,
            'cci_20': self.cci_20,
            'cci_signal': self.cci_signal,
            'roc_12': self.roc_12,
            'roc_signal': self.roc_signal,
            'donchian_upper_20': self.donchian_upper_20,
            'donchian_lower_20': self.donchian_lower_20,
            'donchian_mid_20': self.donchian_mid_20,
            'donchian_signal': self.donchian_signal,
            'williams_r_14': self.williams_r_14,
            'williams_signal': self.williams_signal,
            'stoch_rsi_14': self.stoch_rsi_14,
            'stoch_rsi_signal': self.stoch_rsi_signal,
            'cmf_20': self.cmf_20,
            'cmf_signal': self.cmf_signal,
            'vwap_20': self.vwap_20,
            'vwap_distance_pct': self.vwap_distance_pct,
            'vwap_signal': self.vwap_signal,
            'indicator_consensus_score': self.indicator_consensus_score,
            'indicator_consensus_signal': self.indicator_consensus_signal,
            'indicator_bullish_count': self.indicator_bullish_count,
            'indicator_bearish_count': self.indicator_bearish_count,
            'indicator_neutral_count': self.indicator_neutral_count,
            'indicator_conflict_level': self.indicator_conflict_level,
            'indicator_consensus_details': self.indicator_consensus_details,
        }


class StockTrendAnalyzer:
    """
    股票趋势分析器

    基于用户交易理念实现：
    1. 趋势判断 - MA5>MA10>MA20 多头排列
    2. 乖离率检测 - 不追高，偏离 MA5 超过 5% 不买
    3. 量能分析 - 偏好缩量回调
    4. 买点识别 - 回踩 MA5/MA10 支撑
    5. MACD 指标 - 趋势确认和金叉死叉信号
    6. RSI 指标 - 超买超卖判断
    """
    
    # 交易参数配置（BIAS_THRESHOLD 从 Config 读取，见 _generate_signal）
    VOLUME_SHRINK_RATIO = 0.7   # 缩量判断阈值（当日量/5日均量）
    VOLUME_HEAVY_RATIO = 1.5    # 放量判断阈值
    MA_SUPPORT_TOLERANCE = 0.02  # MA 支撑判断容忍度（2%）

    # MACD 参数（标准12/26/9）
    MACD_FAST = 12              # 快线周期
    MACD_SLOW = 26             # 慢线周期
    MACD_SIGNAL = 9             # 信号线周期

    # RSI 参数
    RSI_SHORT = 6               # 短期RSI周期
    RSI_MID = 12               # 中期RSI周期
    RSI_LONG = 24              # 长期RSI周期
    RSI_OVERBOUGHT = 70        # 超买阈值
    RSI_OVERSOLD = 30          # 超卖阈值

    # 布林带参数
    BOLL_PERIOD = 20             # 布林带周期
    BOLL_STD = 2                 # 标准差倍数
    BOLL_SQUEEZE_RATIO = 0.10    # 收窄判断阈值（带宽/中轨 < 10%）

    # KDJ 参数
    KDJ_N = 9                    # KDJ周期
    KDJ_M1 = 3                   # K值平滑系数
    KDJ_M2 = 3                   # D值平滑系数
    KDJ_OVERBOUGHT = 80          # 超买阈值
    KDJ_OVERSOLD = 20            # 超卖阈值

    # ATR 参数
    ATR_PERIOD = 14              # ATR周期

    # OBV 参数
    OBV_MA_PERIOD = 20           # OBV均线周期

    # RSRS 参数（对齐聚宽原版策略）
    RSRS_WINDOW = 18              # RSRS回归窗口（N）
    RSRS_ZSCORE_WINDOW = 1100     # 标准分计算窗口（M），约4.4年交易日
    RSRS_BUY_THRESHOLD = 0.7      # 买入阈值（加权分 > 0.7 表示市场安全）
    RSRS_SELL_THRESHOLD = -0.7    # 卖出阈值（加权分 < -0.7 表示风险过大）

    # 扩展指标参数
    ADX_PERIOD = 14
    MFI_PERIOD = 14
    CCI_PERIOD = 20
    ROC_PERIOD = 12
    DONCHIAN_PERIOD = 20
    WILLIAMS_PERIOD = 14
    STOCH_RSI_PERIOD = 14
    CMF_PERIOD = 20
    VWAP_PERIOD = 20
    
    def __init__(self):
        """初始化分析器"""
        pass

    def prepare_indicator_frame(self, df: pd.DataFrame) -> pd.DataFrame:
        """Return OHLCV data enriched with all supported technical indicators."""
        df = df.sort_values('date').reset_index(drop=True).copy()
        df = self._calculate_mas(df)
        df = self._calculate_macd(df)
        df = self._calculate_rsi(df)
        df = self._calculate_boll(df)
        df = self._calculate_kdj(df)
        df = self._calculate_atr(df)
        df = self._calculate_obv(df)
        df = self._calculate_rsrs(df)
        df = self._calculate_adx_dmi(df)
        df = self._calculate_mfi(df)
        df = self._calculate_cci(df)
        df = self._calculate_roc(df)
        df = self._calculate_donchian(df)
        df = self._calculate_williams_r(df)
        df = self._calculate_stoch_rsi(df)
        df = self._calculate_cmf(df)
        df = self._calculate_vwap(df)
        return df
    
    def analyze(self, df: pd.DataFrame, code: str) -> TrendAnalysisResult:
        """
        分析股票趋势
        
        Args:
            df: 包含 OHLCV 数据的 DataFrame
            code: 股票代码
            
        Returns:
            TrendAnalysisResult 分析结果
        """
        result = TrendAnalysisResult(code=code)
        
        if df is None or df.empty or len(df) < 20:
            logger.warning(f"{code} 数据不足，无法进行趋势分析")
            result.risk_factors.append("数据不足，无法完成分析")
            return result
        
        # 确保数据按日期排序并计算完整指标集
        df = self.prepare_indicator_frame(df)

        # 获取最新数据
        latest = df.iloc[-1]
        result.current_price = float(latest['close'])
        result.ma5 = float(latest['MA5'])
        result.ma10 = float(latest['MA10'])
        result.ma20 = float(latest['MA20'])
        result.ma60 = float(latest.get('MA60', 0))

        # 1. 趋势判断
        self._analyze_trend(df, result)

        # 2. 乖离率计算
        self._calculate_bias(result)

        # 3. 量能分析
        self._analyze_volume(df, result)

        # 4. 支撑压力分析
        self._analyze_support_resistance(df, result)

        # 5. MACD 分析
        self._analyze_macd(df, result)

        # 6. RSI 分析
        self._analyze_rsi(df, result)

        # 7. 布林带分析
        self._analyze_boll(df, result)

        # 8. KDJ 分析
        self._analyze_kdj(df, result)

        # 9. ATR 分析
        self._analyze_atr(df, result)

        # 10. OBV 分析
        self._analyze_obv(df, result)

        # 11. RSRS 择时分析
        self._analyze_rsrs(df, result)

        # 12. 生成买入信号
        self._generate_signal(result)

        # 13. 扩展指标分析（作为过滤与解释，不直接改变评分）
        self._analyze_extra_indicators(df, result)

        # 14. 多指标共振摘要（作为给人和 AI 的总览，不直接改变评分）
        self._analyze_indicator_consensus(result)

        return result

    @staticmethod
    def _safe_float(value: Any, default: float = 0.0) -> float:
        """Convert pandas/numpy values to finite float values."""
        try:
            parsed = float(value)
        except (TypeError, ValueError):
            return default
        if not np.isfinite(parsed):
            return default
        return parsed
    
    def _calculate_mas(self, df: pd.DataFrame) -> pd.DataFrame:
        """计算均线"""
        df = df.copy()
        df['MA5'] = df['close'].rolling(window=5).mean()
        df['MA10'] = df['close'].rolling(window=10).mean()
        df['MA20'] = df['close'].rolling(window=20).mean()
        if len(df) >= 60:
            df['MA60'] = df['close'].rolling(window=60).mean()
        else:
            df['MA60'] = df['MA20']  # 数据不足时使用 MA20 替代
        return df

    def _calculate_macd(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        计算 MACD 指标

        公式：
        - EMA(12)：12日指数移动平均
        - EMA(26)：26日指数移动平均
        - DIF = EMA(12) - EMA(26)
        - DEA = EMA(DIF, 9)
        - MACD = (DIF - DEA) * 2
        """
        df = df.copy()

        # 计算快慢线 EMA
        ema_fast = df['close'].ewm(span=self.MACD_FAST, adjust=False).mean()
        ema_slow = df['close'].ewm(span=self.MACD_SLOW, adjust=False).mean()

        # 计算快线 DIF
        df['MACD_DIF'] = ema_fast - ema_slow

        # 计算信号线 DEA
        df['MACD_DEA'] = df['MACD_DIF'].ewm(span=self.MACD_SIGNAL, adjust=False).mean()

        # 计算柱状图
        df['MACD_BAR'] = (df['MACD_DIF'] - df['MACD_DEA']) * 2

        return df

    def _calculate_rsi(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        计算 RSI 指标

        公式：
        - RS = 平均上涨幅度 / 平均下跌幅度
        - RSI = 100 - (100 / (1 + RS))
        """
        df = df.copy()

        for period in [self.RSI_SHORT, self.RSI_MID, self.RSI_LONG]:
            # 计算价格变化
            delta = df['close'].diff()

            # 分离上涨和下跌
            gain = delta.where(delta > 0, 0)
            loss = -delta.where(delta < 0, 0)

            # 计算平均涨跌幅
            avg_gain = gain.rolling(window=period).mean()
            avg_loss = loss.rolling(window=period).mean()

            # 计算 RS 和 RSI
            rs = avg_gain / avg_loss
            rsi = 100 - (100 / (1 + rs))

            # 填充 NaN 值
            rsi = rsi.fillna(50)  # 默认中性值

            # 添加到 DataFrame
            col_name = f'RSI_{period}'
            df[col_name] = rsi

        return df
    
    def _analyze_trend(self, df: pd.DataFrame, result: TrendAnalysisResult) -> None:
        """
        分析趋势状态
        
        核心逻辑：判断均线排列和趋势强度
        """
        ma5, ma10, ma20 = result.ma5, result.ma10, result.ma20
        
        # 判断均线排列
        if ma5 > ma10 > ma20:
            # 检查间距是否在扩大（强势）
            prev = df.iloc[-5] if len(df) >= 5 else df.iloc[-1]
            prev_spread = (prev['MA5'] - prev['MA20']) / prev['MA20'] * 100 if prev['MA20'] > 0 else 0
            curr_spread = (ma5 - ma20) / ma20 * 100 if ma20 > 0 else 0
            
            if curr_spread > prev_spread and curr_spread > 5:
                result.trend_status = TrendStatus.STRONG_BULL
                result.ma_alignment = "强势多头排列，均线发散上行"
                result.trend_strength = 90
            else:
                result.trend_status = TrendStatus.BULL
                result.ma_alignment = "多头排列 MA5>MA10>MA20"
                result.trend_strength = 75
                
        elif ma5 > ma10 and ma10 <= ma20:
            result.trend_status = TrendStatus.WEAK_BULL
            result.ma_alignment = "弱势多头，MA5>MA10 但 MA10≤MA20"
            result.trend_strength = 55
            
        elif ma5 < ma10 < ma20:
            prev = df.iloc[-5] if len(df) >= 5 else df.iloc[-1]
            prev_spread = (prev['MA20'] - prev['MA5']) / prev['MA5'] * 100 if prev['MA5'] > 0 else 0
            curr_spread = (ma20 - ma5) / ma5 * 100 if ma5 > 0 else 0
            
            if curr_spread > prev_spread and curr_spread > 5:
                result.trend_status = TrendStatus.STRONG_BEAR
                result.ma_alignment = "强势空头排列，均线发散下行"
                result.trend_strength = 10
            else:
                result.trend_status = TrendStatus.BEAR
                result.ma_alignment = "空头排列 MA5<MA10<MA20"
                result.trend_strength = 25
                
        elif ma5 < ma10 and ma10 >= ma20:
            result.trend_status = TrendStatus.WEAK_BEAR
            result.ma_alignment = "弱势空头，MA5<MA10 但 MA10≥MA20"
            result.trend_strength = 40
            
        else:
            result.trend_status = TrendStatus.CONSOLIDATION
            result.ma_alignment = "均线缠绕，趋势不明"
            result.trend_strength = 50
    
    def _calculate_bias(self, result: TrendAnalysisResult) -> None:
        """
        计算乖离率
        
        乖离率 = (现价 - 均线) / 均线 * 100%
        
        严进策略：乖离率超过 5% 不追高
        """
        price = result.current_price
        
        if result.ma5 > 0:
            result.bias_ma5 = (price - result.ma5) / result.ma5 * 100
        if result.ma10 > 0:
            result.bias_ma10 = (price - result.ma10) / result.ma10 * 100
        if result.ma20 > 0:
            result.bias_ma20 = (price - result.ma20) / result.ma20 * 100
    
    def _analyze_volume(self, df: pd.DataFrame, result: TrendAnalysisResult) -> None:
        """
        分析量能
        
        偏好：缩量回调 > 放量上涨 > 缩量上涨 > 放量下跌
        """
        if len(df) < 5:
            return
        
        latest = df.iloc[-1]
        vol_5d_avg = df['volume'].iloc[-6:-1].mean()
        
        if vol_5d_avg > 0:
            result.volume_ratio_5d = float(latest['volume']) / vol_5d_avg
        
        # 判断价格变化
        prev_close = df.iloc[-2]['close']
        price_change = (latest['close'] - prev_close) / prev_close * 100
        
        # 量能状态判断
        if result.volume_ratio_5d >= self.VOLUME_HEAVY_RATIO:
            if price_change > 0:
                result.volume_status = VolumeStatus.HEAVY_VOLUME_UP
                result.volume_trend = "放量上涨，多头力量强劲"
            else:
                result.volume_status = VolumeStatus.HEAVY_VOLUME_DOWN
                result.volume_trend = "放量下跌，注意风险"
        elif result.volume_ratio_5d <= self.VOLUME_SHRINK_RATIO:
            if price_change > 0:
                result.volume_status = VolumeStatus.SHRINK_VOLUME_UP
                result.volume_trend = "缩量上涨，上攻动能不足"
            else:
                result.volume_status = VolumeStatus.SHRINK_VOLUME_DOWN
                result.volume_trend = "缩量回调，洗盘特征明显（好）"
        else:
            result.volume_status = VolumeStatus.NORMAL
            result.volume_trend = "量能正常"
    
    def _analyze_support_resistance(self, df: pd.DataFrame, result: TrendAnalysisResult) -> None:
        """
        分析支撑压力位
        
        买点偏好：回踩 MA5/MA10 获得支撑
        """
        price = result.current_price
        
        # 检查是否在 MA5 附近获得支撑
        if result.ma5 > 0:
            ma5_distance = abs(price - result.ma5) / result.ma5
            if ma5_distance <= self.MA_SUPPORT_TOLERANCE and price >= result.ma5:
                result.support_ma5 = True
                result.support_levels.append(result.ma5)
        
        # 检查是否在 MA10 附近获得支撑
        if result.ma10 > 0:
            ma10_distance = abs(price - result.ma10) / result.ma10
            if ma10_distance <= self.MA_SUPPORT_TOLERANCE and price >= result.ma10:
                result.support_ma10 = True
                if result.ma10 not in result.support_levels:
                    result.support_levels.append(result.ma10)
        
        # MA20 作为重要支撑
        if result.ma20 > 0 and price >= result.ma20:
            result.support_levels.append(result.ma20)
        
        # 近期高点作为压力
        if len(df) >= 20:
            recent_high = df['high'].iloc[-20:].max()
            if recent_high > price:
                result.resistance_levels.append(recent_high)

    def _analyze_macd(self, df: pd.DataFrame, result: TrendAnalysisResult) -> None:
        """
        分析 MACD 指标

        核心信号：
        - 零轴上金叉：最强买入信号
        - 金叉：DIF 上穿 DEA
        - 死叉：DIF 下穿 DEA
        """
        if len(df) < self.MACD_SLOW:
            result.macd_signal = "数据不足"
            return

        latest = df.iloc[-1]
        prev = df.iloc[-2]

        # 获取 MACD 数据
        result.macd_dif = float(latest['MACD_DIF'])
        result.macd_dea = float(latest['MACD_DEA'])
        result.macd_bar = float(latest['MACD_BAR'])

        # 判断金叉死叉
        prev_dif_dea = prev['MACD_DIF'] - prev['MACD_DEA']
        curr_dif_dea = result.macd_dif - result.macd_dea

        # 金叉：DIF 上穿 DEA
        is_golden_cross = prev_dif_dea <= 0 and curr_dif_dea > 0

        # 死叉：DIF 下穿 DEA
        is_death_cross = prev_dif_dea >= 0 and curr_dif_dea < 0

        # 零轴穿越
        prev_zero = prev['MACD_DIF']
        curr_zero = result.macd_dif
        is_crossing_up = prev_zero <= 0 and curr_zero > 0
        is_crossing_down = prev_zero >= 0 and curr_zero < 0

        # 判断 MACD 状态
        if is_golden_cross and curr_zero > 0:
            result.macd_status = MACDStatus.GOLDEN_CROSS_ZERO
            result.macd_signal = "⭐ 零轴上金叉，强烈买入信号！"
        elif is_crossing_up:
            result.macd_status = MACDStatus.CROSSING_UP
            result.macd_signal = "⚡ DIF上穿零轴，趋势转强"
        elif is_golden_cross:
            result.macd_status = MACDStatus.GOLDEN_CROSS
            result.macd_signal = "✅ 金叉，趋势向上"
        elif is_death_cross:
            result.macd_status = MACDStatus.DEATH_CROSS
            result.macd_signal = "❌ 死叉，趋势向下"
        elif is_crossing_down:
            result.macd_status = MACDStatus.CROSSING_DOWN
            result.macd_signal = "⚠️ DIF下穿零轴，趋势转弱"
        elif result.macd_dif > 0 and result.macd_dea > 0:
            result.macd_status = MACDStatus.BULLISH
            result.macd_signal = "✓ 多头排列，持续上涨"
        elif result.macd_dif < 0 and result.macd_dea < 0:
            result.macd_status = MACDStatus.BEARISH
            result.macd_signal = "⚠ 空头排列，持续下跌"
        else:
            result.macd_status = MACDStatus.BULLISH
            result.macd_signal = " MACD 中性区域"

    def _analyze_rsi(self, df: pd.DataFrame, result: TrendAnalysisResult) -> None:
        """
        分析 RSI 指标

        核心判断：
        - RSI > 70：超买，谨慎追高
        - RSI < 30：超卖，关注反弹
        - 40-60：中性区域
        """
        if len(df) < self.RSI_LONG:
            result.rsi_signal = "数据不足"
            return

        latest = df.iloc[-1]

        # 获取 RSI 数据
        result.rsi_6 = float(latest[f'RSI_{self.RSI_SHORT}'])
        result.rsi_12 = float(latest[f'RSI_{self.RSI_MID}'])
        result.rsi_24 = float(latest[f'RSI_{self.RSI_LONG}'])

        # 以中期 RSI(12) 为主进行判断
        rsi_mid = result.rsi_12

        # 判断 RSI 状态
        if rsi_mid > self.RSI_OVERBOUGHT:
            result.rsi_status = RSIStatus.OVERBOUGHT
            result.rsi_signal = f"⚠️ RSI超买({rsi_mid:.1f}>70)，短期回调风险高"
        elif rsi_mid > 60:
            result.rsi_status = RSIStatus.STRONG_BUY
            result.rsi_signal = f"✅ RSI强势({rsi_mid:.1f})，多头力量充足"
        elif rsi_mid >= 40:
            result.rsi_status = RSIStatus.NEUTRAL
            result.rsi_signal = f" RSI中性({rsi_mid:.1f})，震荡整理中"
        elif rsi_mid >= self.RSI_OVERSOLD:
            result.rsi_status = RSIStatus.WEAK
            result.rsi_signal = f"⚡ RSI弱势({rsi_mid:.1f})，关注反弹"
        else:
            result.rsi_status = RSIStatus.OVERSOLD
            result.rsi_signal = f"⭐ RSI超卖({rsi_mid:.1f}<30)，反弹机会大"

    def _calculate_boll(self, df: pd.DataFrame) -> pd.DataFrame:
        """计算布林带指标"""
        df = df.copy()
        df['BOLL_MID'] = df['close'].rolling(window=self.BOLL_PERIOD).mean()
        std = df['close'].rolling(window=self.BOLL_PERIOD).std()
        df['BOLL_UPPER'] = df['BOLL_MID'] + self.BOLL_STD * std
        df['BOLL_LOWER'] = df['BOLL_MID'] - self.BOLL_STD * std
        df['BOLL_WIDTH'] = (df['BOLL_UPPER'] - df['BOLL_LOWER']) / df['BOLL_MID']
        df['BOLL_PCTB'] = (df['close'] - df['BOLL_LOWER']) / (df['BOLL_UPPER'] - df['BOLL_LOWER'])
        return df

    def _analyze_boll(self, df: pd.DataFrame, result: TrendAnalysisResult):
        """分析布林带"""
        latest = df.iloc[-1]
        prev = df.iloc[-2] if len(df) >= 2 else latest

        result.boll_upper = float(latest.get('BOLL_UPPER', 0))
        result.boll_mid = float(latest.get('BOLL_MID', 0))
        result.boll_lower = float(latest.get('BOLL_LOWER', 0))
        result.boll_width = float(latest.get('BOLL_WIDTH', 0))
        result.boll_pctb = float(latest.get('BOLL_PCTB', 0))

        price = result.current_price

        if result.boll_width < self.BOLL_SQUEEZE_RATIO:
            result.boll_status = BollStatus.SQUEEZE
            result.boll_signal = "布林带收窄，注意变盘方向"
        elif price >= result.boll_upper:
            result.boll_status = BollStatus.ABOVE_UPPER
            result.boll_signal = "突破布林上轨，短期可能超买"
            result.risk_factors.append("价格突破布林上轨，注意回调风险")
        elif price >= result.boll_mid + 0.7 * (result.boll_upper - result.boll_mid):
            result.boll_status = BollStatus.NEAR_UPPER
            result.boll_signal = "接近布林上轨，上方压力增大"
        elif price <= result.boll_lower:
            result.boll_status = BollStatus.BELOW_LOWER
            result.boll_signal = "跌破布林下轨，短期可能超卖"
        elif price <= result.boll_mid + 0.3 * (result.boll_upper - result.boll_mid):
            result.boll_status = BollStatus.NEAR_LOWER
            result.boll_signal = "接近布林下轨，关注支撑"
        else:
            result.boll_status = BollStatus.MID_UPPER
            result.boll_signal = "运行于布林带中轨附近"

    def _calculate_kdj(self, df: pd.DataFrame) -> pd.DataFrame:
        """计算 KDJ 指标"""
        df = df.copy()
        low_list = df['low'].rolling(window=self.KDJ_N, min_periods=1).min()
        high_list = df['high'].rolling(window=self.KDJ_N, min_periods=1).max()

        rsv = (df['close'] - low_list) / (high_list - low_list) * 100
        rsv = rsv.fillna(50)

        df['KDJ_K'] = rsv.ewm(com=self.KDJ_M1 - 1, adjust=False).mean()
        df['KDJ_D'] = df['KDJ_K'].ewm(com=self.KDJ_M2 - 1, adjust=False).mean()
        df['KDJ_J'] = 3 * df['KDJ_K'] - 2 * df['KDJ_D']
        return df

    def _analyze_kdj(self, df: pd.DataFrame, result: TrendAnalysisResult):
        """分析 KDJ 指标"""
        if len(df) < 2:
            return

        latest = df.iloc[-1]
        prev = df.iloc[-2]

        result.kdj_k = round(float(latest.get('KDJ_K', 50)), 2)
        result.kdj_d = round(float(latest.get('KDJ_D', 50)), 2)
        result.kdj_j = round(float(latest.get('KDJ_J', 50)), 2)

        k, d = result.kdj_k, result.kdj_d
        prev_k, prev_d = float(prev.get('KDJ_K', 50)), float(prev.get('KDJ_D', 50))

        # 金叉/死叉判断
        golden_cross = prev_k <= prev_d and k > d
        death_cross = prev_k >= prev_d and k < d

        if golden_cross and k < self.KDJ_OVERBOUGHT:
            result.kdj_status = KDJStatus.GOLDEN_CROSS
            result.kdj_signal = f"KDJ金叉 (K={k:.1f}, D={d:.1f})，短期看多信号"
        elif death_cross and k > self.KDJ_OVERSOLD:
            result.kdj_status = KDJStatus.DEATH_CROSS
            result.kdj_signal = f"KDJ死叉 (K={k:.1f}, D={d:.1f})，短期看空信号"
            result.risk_factors.append(f"KDJ死叉，短期可能调整")
        elif k > self.KDJ_OVERBOUGHT and d > self.KDJ_OVERBOUGHT:
            result.kdj_status = KDJStatus.OVERBOUGHT
            result.kdj_signal = f"KDJ超买区 (K={k:.1f}, D={d:.1f})"
            result.risk_factors.append("KDJ进入超买区")
        elif k < self.KDJ_OVERSOLD and d < self.KDJ_OVERSOLD:
            result.kdj_status = KDJStatus.OVERSOLD
            result.kdj_signal = f"KDJ超卖区 (K={k:.1f}, D={d:.1f})"
        elif k > d and k > 50:
            result.kdj_status = KDJStatus.STRONG
            result.kdj_signal = f"KDJ强势 (K={k:.1f}, D={d:.1f})"
        elif k < d and k < 50:
            result.kdj_status = KDJStatus.WEAK
            result.kdj_signal = f"KDJ弱势 (K={k:.1f}, D={d:.1f})"
        else:
            result.kdj_status = KDJStatus.NEUTRAL
            result.kdj_signal = f"KDJ中性 (K={k:.1f}, D={d:.1f})"

    def _calculate_atr(self, df: pd.DataFrame) -> pd.DataFrame:
        """计算 ATR（平均真实波幅）"""
        df = df.copy()
        high = df['high']
        low = df['low']
        close = df['close']
        prev_close = close.shift(1)

        tr1 = high - low
        tr2 = (high - prev_close).abs()
        tr3 = (low - prev_close).abs()
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

        df['ATR'] = tr.rolling(window=self.ATR_PERIOD).mean()
        df['ATR_PCT'] = df['ATR'] / close * 100
        return df

    def _analyze_atr(self, df: pd.DataFrame, result: TrendAnalysisResult):
        """分析 ATR 指标"""
        latest = df.iloc[-1]
        result.atr_14 = round(float(latest.get('ATR', 0)), 4)
        result.atr_percent = round(float(latest.get('ATR_PCT', 0)), 2)

        if result.atr_percent > 5:
            result.risk_factors.append(f"ATR波动率较高({result.atr_percent:.1f}%)，注意风险")
        elif result.atr_percent < 1:
            pass  # 低波动率，不特别提示

    def _calculate_obv(self, df: pd.DataFrame) -> pd.DataFrame:
        """计算 OBV（能量潮）指标"""
        df = df.copy()
        direction = df['close'].diff().apply(lambda x: 1 if x > 0 else (-1 if x < 0 else 0))
        df['OBV'] = (direction * df['volume']).cumsum()
        df['OBV_MA'] = df['OBV'].rolling(window=self.OBV_MA_PERIOD).mean()
        return df

    def _analyze_obv(self, df: pd.DataFrame, result: TrendAnalysisResult):
        """分析 OBV 指标"""
        if len(df) < 21:
            return

        latest = df.iloc[-1]
        result.obv = float(latest.get('OBV', 0))
        result.obv_ma20 = float(latest.get('OBV_MA', 0))

        # OBV 趋势判断
        recent_obv = df['OBV'].iloc[-5:]
        obv_trend = recent_obv.iloc[-1] - recent_obv.iloc[0]

        # 量价背离判断（简化版：比较近5日价格和OBV趋势）
        recent_price = df['close'].iloc[-5:]
        price_trend = recent_price.iloc[-1] - recent_price.iloc[0]

        if result.obv > result.obv_ma20:
            if obv_trend > 0:
                result.obv_status = OBVStatus.RISING
                result.obv_signal = "OBV上升且高于均线，资金持续流入"
            else:
                result.obv_status = OBVStatus.RISING
                result.obv_signal = "OBV高于均线但动能减弱"
        else:
            if obv_trend < 0:
                result.obv_status = OBVStatus.FALLING
                result.obv_signal = "OBV下降且低于均线，资金持续流出"
                result.risk_factors.append("OBV持续下降，资金流出")
            else:
                result.obv_status = OBVStatus.FALLING
                result.obv_signal = "OBV低于均线但有回升迹象"

        # 背离判断
        if price_trend < 0 and obv_trend > 0:
            result.obv_status = OBVStatus.BULLISH_DIVERGENCE
            result.obv_signal = "⚠️ 量价底背离：价格下跌但OBV上升，可能见底"
        elif price_trend > 0 and obv_trend < 0:
            result.obv_status = OBVStatus.BEARISH_DIVERGENCE
            result.obv_signal = "⚠️ 量价顶背离：价格上涨但OBV下降，注意风险"
            result.risk_factors.append("量价顶背离，上涨动能不足")

    def _calculate_rsrs(self, df: pd.DataFrame) -> pd.DataFrame:
        """计算 RSRS（阻力支撑相对强弱）指标

        原理：用最高价对最低价做OLS线性回归，取β斜率作为支撑阻力相对强度。
        优化：将β值标准化(z-score)并用R²加权，得到右偏标准分用于择时。
        """
        df = df.copy()
        window = self.RSRS_WINDOW
        n = len(df)

        beta_list = []
        r2_list = []

        for i in range(window, n):
            y = df['high'].iloc[i - window:i].values.astype(float)
            x = df['low'].iloc[i - window:i].values.astype(float)

            # OLS 回归: high = α + β * low
            x_mean = np.mean(x)
            y_mean = np.mean(y)
            ss_xy = np.sum((x - x_mean) * (y - y_mean))
            ss_xx = np.sum((x - x_mean) ** 2)
            ss_yy = np.sum((y - y_mean) ** 2)

            if ss_xx == 0:
                beta_list.append(np.nan)
                r2_list.append(0.0)
                continue

            beta = ss_xy / ss_xx
            # R² = (SS_xy)² / (SS_xx * SS_yy)
            if ss_yy == 0:
                r2 = 0.0
            else:
                r2 = (ss_xy ** 2) / (ss_xx * ss_yy)

            beta_list.append(beta)
            r2_list.append(r2)

        # 前面 window 个位置填充 NaN（第 window 个位置开始有值）
        beta_arr = np.array([np.nan] * window + beta_list)
        r2_arr = np.array([0.0] * window + r2_list)

        df['RSRS_BETA'] = beta_arr
        df['RSRS_R2'] = r2_arr

        # 标准化：z-score = (β - mean) / std
        zscore_window = min(self.RSRS_ZSCORE_WINDOW, len(beta_list))
        if zscore_window > 1:
            recent_betas = np.array(beta_list[-zscore_window:])
            mean_beta = np.nanmean(recent_betas)
            std_beta = np.nanstd(recent_betas)
            if std_beta > 0:
                df['RSRS_ZSCORE'] = (df['RSRS_BETA'] - mean_beta) / std_beta
            else:
                df['RSRS_ZSCORE'] = 0.0
        else:
            df['RSRS_ZSCORE'] = 0.0

        # R² 加权标准分 = zscore * β * R²（对齐聚宽原版公式）
        df['RSRS_R2_WEIGHTED'] = df['RSRS_ZSCORE'] * df['RSRS_BETA'] * df['RSRS_R2']

        return df

    def _analyze_rsrs(self, df: pd.DataFrame, result: TrendAnalysisResult):
        """分析 RSRS 择时指标"""
        if len(df) < self.RSRS_WINDOW + 1:
            return

        latest = df.iloc[-1]
        result.rsrs_beta = round(float(latest.get('RSRS_BETA', 0)), 4)
        result.rsrs_zscore = round(float(latest.get('RSRS_ZSCORE', 0)), 4)
        result.rsrs_r2_weighted = round(float(latest.get('RSRS_R2_WEIGHTED', 0)), 4)
        result.rsrs_r2 = round(float(latest.get('RSRS_R2', 0)), 4)

        rw = result.rsrs_r2_weighted

        # 对齐聚宽原版：加权分 > 0.7 市场安全可买入，< -0.7 风险过大应卖出
        if rw >= self.RSRS_BUY_THRESHOLD * 1.5:
            result.rsrs_status = RSRSStatus.STRONG_BUY
            result.rsrs_signal = f"RSRS强烈买入 (β={result.rsrs_beta:.3f}, 加权分={rw:.3f})，市场风险极低，支撑强劲"
        elif rw >= self.RSRS_BUY_THRESHOLD:
            result.rsrs_status = RSRSStatus.BUY
            result.rsrs_signal = f"RSRS买入 (β={result.rsrs_beta:.3f}, 加权分={rw:.3f})，市场风险在合理范围"
        elif rw <= self.RSRS_SELL_THRESHOLD * 1.5:
            result.rsrs_status = RSRSStatus.STRONG_SELL
            result.rsrs_signal = f"RSRS强烈卖出 (β={result.rsrs_beta:.3f}, 加权分={rw:.3f})，市场风险极大，保持空仓"
            result.risk_factors.append(f"RSRS择时显示强烈卖出信号(加权分={rw:.2f})，市场风险过大")
        elif rw <= self.RSRS_SELL_THRESHOLD:
            result.rsrs_status = RSRSStatus.SELL
            result.rsrs_signal = f"RSRS卖出 (β={result.rsrs_beta:.3f}, 加权分={rw:.3f})，市场风险较大"
            result.risk_factors.append(f"RSRS择时显示卖出信号(加权分={rw:.2f})")
        else:
            result.rsrs_status = RSRSStatus.HOLD
            result.rsrs_signal = f"RSRS中性 (β={result.rsrs_beta:.3f}, 加权分={rw:.3f})"

    def _calculate_adx_dmi(self, df: pd.DataFrame) -> pd.DataFrame:
        """计算 ADX/DMI，用于判断趋势强度和多空占优方向。"""
        df = df.copy()
        high = df['high']
        low = df['low']
        close = df['close']
        prev_high = high.shift(1)
        prev_low = low.shift(1)
        prev_close = close.shift(1)

        plus_dm = (high - prev_high).where((high - prev_high) > (prev_low - low), 0.0)
        plus_dm = plus_dm.where(plus_dm > 0, 0.0)
        minus_dm = (prev_low - low).where((prev_low - low) > (high - prev_high), 0.0)
        minus_dm = minus_dm.where(minus_dm > 0, 0.0)

        tr = pd.concat(
            [
                high - low,
                (high - prev_close).abs(),
                (low - prev_close).abs(),
            ],
            axis=1,
        ).max(axis=1)
        tr_sum = tr.rolling(window=self.ADX_PERIOD).sum()
        plus_di = 100 * plus_dm.rolling(window=self.ADX_PERIOD).sum() / tr_sum
        minus_di = 100 * minus_dm.rolling(window=self.ADX_PERIOD).sum() / tr_sum
        dx = ((plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, np.nan)) * 100

        df['PLUS_DI'] = plus_di.replace([np.inf, -np.inf], np.nan).fillna(0)
        df['MINUS_DI'] = minus_di.replace([np.inf, -np.inf], np.nan).fillna(0)
        df['ADX'] = dx.rolling(window=self.ADX_PERIOD).mean().replace([np.inf, -np.inf], np.nan).fillna(0)
        return df

    def _calculate_mfi(self, df: pd.DataFrame) -> pd.DataFrame:
        """计算 MFI，作为加入成交量的 RSI 类资金强弱指标。"""
        df = df.copy()
        typical_price = (df['high'] + df['low'] + df['close']) / 3
        money_flow = typical_price * df['volume']
        delta = typical_price.diff()
        positive_flow = money_flow.where(delta > 0, 0.0)
        negative_flow = money_flow.where(delta < 0, 0.0)

        positive_sum = positive_flow.rolling(window=self.MFI_PERIOD).sum()
        negative_sum = negative_flow.rolling(window=self.MFI_PERIOD).sum()
        money_ratio = positive_sum / negative_sum.replace(0, np.nan)
        df['MFI'] = (100 - (100 / (1 + money_ratio))).replace([np.inf, -np.inf], np.nan).fillna(50)
        return df

    def _calculate_cci(self, df: pd.DataFrame) -> pd.DataFrame:
        """计算 CCI，用于衡量价格偏离常态区间的程度。"""
        df = df.copy()
        typical_price = (df['high'] + df['low'] + df['close']) / 3
        tp_ma = typical_price.rolling(window=self.CCI_PERIOD).mean()
        mean_deviation = typical_price.rolling(window=self.CCI_PERIOD).apply(
            lambda values: float(np.mean(np.abs(values - np.mean(values)))),
            raw=True,
        )
        df['CCI'] = ((typical_price - tp_ma) / (0.015 * mean_deviation)).replace(
            [np.inf, -np.inf],
            np.nan,
        ).fillna(0)
        return df

    def _calculate_roc(self, df: pd.DataFrame) -> pd.DataFrame:
        """计算 ROC，衡量中短期价格动量。"""
        df = df.copy()
        df['ROC'] = df['close'].pct_change(periods=self.ROC_PERIOD).replace(
            [np.inf, -np.inf],
            np.nan,
        ).fillna(0) * 100
        return df

    def _calculate_donchian(self, df: pd.DataFrame) -> pd.DataFrame:
        """计算唐奇安通道，用于识别价格突破。"""
        df = df.copy()
        upper = df['high'].rolling(window=self.DONCHIAN_PERIOD).max()
        lower = df['low'].rolling(window=self.DONCHIAN_PERIOD).min()
        df['DONCHIAN_UPPER'] = upper
        df['DONCHIAN_LOWER'] = lower
        df['DONCHIAN_MID'] = (upper + lower) / 2
        df['DONCHIAN_UPPER_PREV'] = upper.shift(1)
        df['DONCHIAN_LOWER_PREV'] = lower.shift(1)
        return df

    def _calculate_williams_r(self, df: pd.DataFrame) -> pd.DataFrame:
        """计算 Williams %R，作为短线超买超卖过滤器。"""
        df = df.copy()
        highest_high = df['high'].rolling(window=self.WILLIAMS_PERIOD).max()
        lowest_low = df['low'].rolling(window=self.WILLIAMS_PERIOD).min()
        denominator = (highest_high - lowest_low).replace(0, np.nan)
        df['WILLIAMS_R'] = ((highest_high - df['close']) / denominator * -100).fillna(-50)
        return df

    def _calculate_stoch_rsi(self, df: pd.DataFrame) -> pd.DataFrame:
        """计算 StochRSI，比普通 RSI 更敏感，只作为短线过滤器。"""
        df = df.copy()
        delta = df['close'].diff()
        gain = delta.where(delta > 0, 0)
        loss = -delta.where(delta < 0, 0)
        avg_gain = gain.rolling(window=self.STOCH_RSI_PERIOD).mean()
        avg_loss = loss.rolling(window=self.STOCH_RSI_PERIOD).mean()
        rs = avg_gain / avg_loss.replace(0, np.nan)
        rsi = (100 - (100 / (1 + rs))).replace([np.inf, -np.inf], np.nan).fillna(50)
        rsi_min = rsi.rolling(window=self.STOCH_RSI_PERIOD).min()
        rsi_max = rsi.rolling(window=self.STOCH_RSI_PERIOD).max()
        df['STOCH_RSI'] = ((rsi - rsi_min) / (rsi_max - rsi_min).replace(0, np.nan) * 100).fillna(50)
        return df

    def _calculate_cmf(self, df: pd.DataFrame) -> pd.DataFrame:
        """计算 CMF，衡量一段时间内的资金流入/流出压力。"""
        df = df.copy()
        high_low_range = (df['high'] - df['low']).replace(0, np.nan)
        money_flow_multiplier = ((df['close'] - df['low']) - (df['high'] - df['close'])) / high_low_range
        money_flow_volume = money_flow_multiplier.fillna(0) * df['volume']
        df['CMF'] = (
            money_flow_volume.rolling(window=self.CMF_PERIOD).sum()
            / df['volume'].rolling(window=self.CMF_PERIOD).sum().replace(0, np.nan)
        ).replace([np.inf, -np.inf], np.nan).fillna(0)
        return df

    def _calculate_vwap(self, df: pd.DataFrame) -> pd.DataFrame:
        """计算滚动 VWAP，用于观察当前价格相对成交成本的位置。"""
        df = df.copy()
        typical_price = (df['high'] + df['low'] + df['close']) / 3
        volume_sum = df['volume'].rolling(window=self.VWAP_PERIOD).sum().replace(0, np.nan)
        df['VWAP'] = (typical_price * df['volume']).rolling(window=self.VWAP_PERIOD).sum() / volume_sum
        df['VWAP_DISTANCE_PCT'] = ((df['close'] - df['VWAP']) / df['VWAP'] * 100).replace(
            [np.inf, -np.inf],
            np.nan,
        ).fillna(0)
        return df

    def _analyze_extra_indicators(self, df: pd.DataFrame, result: TrendAnalysisResult) -> None:
        """分析扩展指标。它们先作为辅助解释与风险过滤，不直接改评分。"""
        latest = df.iloc[-1]
        price = result.current_price

        result.adx_14 = round(self._safe_float(latest.get('ADX')), 2)
        result.plus_di_14 = round(self._safe_float(latest.get('PLUS_DI')), 2)
        result.minus_di_14 = round(self._safe_float(latest.get('MINUS_DI')), 2)
        if result.adx_14 >= 25 and result.plus_di_14 > result.minus_di_14:
            result.adx_signal = f"ADX {result.adx_14:.1f}，+DI占优，趋势强度支持多头"
        elif result.adx_14 >= 25 and result.minus_di_14 > result.plus_di_14:
            result.adx_signal = f"ADX {result.adx_14:.1f}，-DI占优，趋势强但偏空"
            result.risk_factors.append("ADX/DMI 显示空方趋势占优")
        elif result.adx_14 < 18:
            result.adx_signal = f"ADX {result.adx_14:.1f}，趋势强度不足，容易震荡反复"
        else:
            result.adx_signal = f"ADX {result.adx_14:.1f}，趋势强度中性"

        result.mfi_14 = round(self._safe_float(latest.get('MFI'), 50), 2)
        if result.mfi_14 >= 80:
            result.mfi_signal = f"MFI {result.mfi_14:.1f}，资金指标过热"
            result.risk_factors.append("MFI 过热，短线追高风险上升")
        elif result.mfi_14 <= 20:
            result.mfi_signal = f"MFI {result.mfi_14:.1f}，资金指标超卖，关注修复"
        elif result.mfi_14 >= 55:
            result.mfi_signal = f"MFI {result.mfi_14:.1f}，资金流偏强"
        else:
            result.mfi_signal = f"MFI {result.mfi_14:.1f}，资金流中性或偏弱"

        result.cci_20 = round(self._safe_float(latest.get('CCI')), 2)
        if result.cci_20 >= 100:
            result.cci_signal = f"CCI {result.cci_20:.1f}，价格偏热"
        elif result.cci_20 <= -100:
            result.cci_signal = f"CCI {result.cci_20:.1f}，价格偏冷，可能有修复机会"
        else:
            result.cci_signal = f"CCI {result.cci_20:.1f}，处于常态区间"

        result.roc_12 = round(self._safe_float(latest.get('ROC')), 2)
        if result.roc_12 >= 5:
            result.roc_signal = f"ROC {result.roc_12:+.1f}%，动量加速"
        elif result.roc_12 <= -5:
            result.roc_signal = f"ROC {result.roc_12:+.1f}%，动量走弱"
            result.risk_factors.append("ROC 动量明显转弱")
        else:
            result.roc_signal = f"ROC {result.roc_12:+.1f}%，动量中性"

        result.donchian_upper_20 = round(self._safe_float(latest.get('DONCHIAN_UPPER')), 4)
        result.donchian_lower_20 = round(self._safe_float(latest.get('DONCHIAN_LOWER')), 4)
        result.donchian_mid_20 = round(self._safe_float(latest.get('DONCHIAN_MID')), 4)
        upper_prev = self._safe_float(latest.get('DONCHIAN_UPPER_PREV'))
        lower_prev = self._safe_float(latest.get('DONCHIAN_LOWER_PREV'))
        if upper_prev > 0 and price >= upper_prev:
            result.donchian_signal = "突破20日唐奇安上轨，趋势启动信号增强"
        elif lower_prev > 0 and price <= lower_prev:
            result.donchian_signal = "跌破20日唐奇安下轨，趋势破位风险"
            result.risk_factors.append("价格跌破唐奇安下轨")
        else:
            result.donchian_signal = "价格仍在20日唐奇安通道内"

        result.williams_r_14 = round(self._safe_float(latest.get('WILLIAMS_R'), -50), 2)
        if result.williams_r_14 >= -20:
            result.williams_signal = f"Williams %R {result.williams_r_14:.1f}，短线超买"
        elif result.williams_r_14 <= -80:
            result.williams_signal = f"Williams %R {result.williams_r_14:.1f}，短线超卖"
        else:
            result.williams_signal = f"Williams %R {result.williams_r_14:.1f}，短线中性"

        result.stoch_rsi_14 = round(self._safe_float(latest.get('STOCH_RSI'), 50), 2)
        if result.stoch_rsi_14 >= 80:
            result.stoch_rsi_signal = f"StochRSI {result.stoch_rsi_14:.1f}，短线过热"
        elif result.stoch_rsi_14 <= 20:
            result.stoch_rsi_signal = f"StochRSI {result.stoch_rsi_14:.1f}，短线超卖"
        else:
            result.stoch_rsi_signal = f"StochRSI {result.stoch_rsi_14:.1f}，短线中性"

        result.cmf_20 = round(self._safe_float(latest.get('CMF')), 4)
        if result.cmf_20 >= 0.05:
            result.cmf_signal = f"CMF {result.cmf_20:.3f}，资金流入占优"
        elif result.cmf_20 <= -0.05:
            result.cmf_signal = f"CMF {result.cmf_20:.3f}，资金流出占优"
            result.risk_factors.append("CMF 显示资金流出压力")
        else:
            result.cmf_signal = f"CMF {result.cmf_20:.3f}，资金流中性"

        result.vwap_20 = round(self._safe_float(latest.get('VWAP')), 4)
        result.vwap_distance_pct = round(self._safe_float(latest.get('VWAP_DISTANCE_PCT')), 2)
        if result.vwap_20 > 0 and result.vwap_distance_pct >= 5:
            result.vwap_signal = f"价格高于20日VWAP {result.vwap_distance_pct:+.1f}%，成本乖离偏高"
            result.risk_factors.append("价格相对 VWAP 乖离偏高")
        elif result.vwap_20 > 0 and result.vwap_distance_pct > 0:
            result.vwap_signal = f"价格高于20日VWAP {result.vwap_distance_pct:+.1f}%，成本线支撑尚可"
        elif result.vwap_20 > 0:
            result.vwap_signal = f"价格低于20日VWAP {result.vwap_distance_pct:+.1f}%，成本线压制"
        else:
            result.vwap_signal = "VWAP 数据不足"

    def _analyze_indicator_consensus(self, result: TrendAnalysisResult) -> None:
        """汇总所有技术指标的方向，形成一条易读的辅助结论。"""
        votes: List[Dict[str, Any]] = []

        def add_vote(name: str, score: float, weight: float, note: str) -> None:
            capped_score = max(-weight, min(weight, score))
            votes.append(
                {
                    "name": name,
                    "score": capped_score,
                    "weight": weight,
                    "note": note,
                }
            )

        # 趋势与传统核心指标
        trend_scores = {
            TrendStatus.STRONG_BULL: (2.0, "强势多头"),
            TrendStatus.BULL: (1.5, "多头排列"),
            TrendStatus.WEAK_BULL: (0.75, "弱势多头"),
            TrendStatus.CONSOLIDATION: (0.0, "趋势不明"),
            TrendStatus.WEAK_BEAR: (-0.75, "弱势空头"),
            TrendStatus.BEAR: (-1.5, "空头排列"),
            TrendStatus.STRONG_BEAR: (-2.0, "强势空头"),
        }
        trend_score, trend_note = trend_scores.get(result.trend_status, (0.0, "趋势不明"))
        add_vote("MA趋势", trend_score, 2.0, trend_note)

        macd_scores = {
            MACDStatus.GOLDEN_CROSS_ZERO: (1.5, "零轴上金叉"),
            MACDStatus.GOLDEN_CROSS: (1.0, "金叉"),
            MACDStatus.BULLISH: (1.0, "MACD多头"),
            MACDStatus.CROSSING_UP: (1.0, "DIF上穿零轴"),
            MACDStatus.CROSSING_DOWN: (-1.0, "DIF下穿零轴"),
            MACDStatus.BEARISH: (-1.0, "MACD空头"),
            MACDStatus.DEATH_CROSS: (-1.0, "死叉"),
        }
        macd_score, macd_note = macd_scores.get(result.macd_status, (0.0, "MACD中性"))
        add_vote("MACD", macd_score, 1.5, macd_note)

        rsi_scores = {
            RSIStatus.OVERBOUGHT: (-0.75, "超买"),
            RSIStatus.STRONG_BUY: (0.75, "强势"),
            RSIStatus.NEUTRAL: (0.0, "中性"),
            RSIStatus.WEAK: (-0.25, "偏弱"),
            RSIStatus.OVERSOLD: (0.75, "超卖修复"),
        }
        rsi_score, rsi_note = rsi_scores.get(result.rsi_status, (0.0, "中性"))
        add_vote("RSI", rsi_score, 1.0, rsi_note)

        boll_scores = {
            BollStatus.ABOVE_UPPER: (-0.75, "突破上轨偏热"),
            BollStatus.NEAR_UPPER: (-0.4, "接近上轨"),
            BollStatus.MID_UPPER: (0.0, "中轨附近"),
            BollStatus.NEAR_LOWER: (0.4, "接近下轨"),
            BollStatus.BELOW_LOWER: (0.75, "跌破下轨修复"),
            BollStatus.SQUEEZE: (0.0, "收窄待变盘"),
        }
        boll_score, boll_note = boll_scores.get(result.boll_status, (0.0, "中性"))
        add_vote("BOLL", boll_score, 1.0, boll_note)

        kdj_scores = {
            KDJStatus.GOLDEN_CROSS: (0.75, "金叉"),
            KDJStatus.DEATH_CROSS: (-0.75, "死叉"),
            KDJStatus.OVERBOUGHT: (-0.4, "超买"),
            KDJStatus.OVERSOLD: (0.4, "超卖"),
            KDJStatus.STRONG: (0.75, "强势"),
            KDJStatus.WEAK: (-0.75, "弱势"),
            KDJStatus.NEUTRAL: (0.0, "中性"),
        }
        kdj_score, kdj_note = kdj_scores.get(result.kdj_status, (0.0, "中性"))
        add_vote("KDJ", kdj_score, 1.0, kdj_note)

        obv_scores = {
            OBVStatus.BULLISH_DIVERGENCE: (0.75, "底背离"),
            OBVStatus.RISING: (0.75, "资金上升"),
            OBVStatus.FLAT: (0.0, "平稳"),
            OBVStatus.FALLING: (-0.75, "资金下降"),
            OBVStatus.BEARISH_DIVERGENCE: (-0.75, "顶背离"),
        }
        obv_score, obv_note = obv_scores.get(result.obv_status, (0.0, "平稳"))
        add_vote("OBV", obv_score, 1.0, obv_note)

        rsrs_scores = {
            RSRSStatus.STRONG_BUY: (1.5, "强买"),
            RSRSStatus.BUY: (1.0, "买入"),
            RSRSStatus.HOLD: (0.0, "中性"),
            RSRSStatus.SELL: (-1.0, "卖出"),
            RSRSStatus.STRONG_SELL: (-1.5, "强卖"),
        }
        rsrs_score, rsrs_note = rsrs_scores.get(result.rsrs_status, (0.0, "中性"))
        add_vote("RSRS", rsrs_score, 1.5, rsrs_note)

        # 新增扩展指标
        if result.adx_14 >= 25 and result.plus_di_14 > result.minus_di_14:
            add_vote("ADX/DMI", 1.25, 1.25, "强趋势偏多")
        elif result.adx_14 >= 25 and result.minus_di_14 > result.plus_di_14:
            add_vote("ADX/DMI", -1.25, 1.25, "强趋势偏空")
        elif result.adx_14 >= 18 and result.plus_di_14 > result.minus_di_14:
            add_vote("ADX/DMI", 0.4, 1.25, "弱趋势偏多")
        elif result.adx_14 >= 18 and result.minus_di_14 > result.plus_di_14:
            add_vote("ADX/DMI", -0.4, 1.25, "弱趋势偏空")
        else:
            add_vote("ADX/DMI", 0.0, 1.25, "趋势强度不足")

        if result.mfi_14 >= 80:
            add_vote("MFI", -0.75, 0.75, "资金过热")
        elif result.mfi_14 <= 20:
            add_vote("MFI", 0.75, 0.75, "资金超卖")
        elif result.mfi_14 >= 55:
            add_vote("MFI", 0.4, 0.75, "资金偏强")
        elif result.mfi_14 <= 45:
            add_vote("MFI", -0.25, 0.75, "资金偏弱")
        else:
            add_vote("MFI", 0.0, 0.75, "中性")

        if result.cci_20 >= 100:
            add_vote("CCI", -0.75, 0.75, "偏热")
        elif result.cci_20 <= -100:
            add_vote("CCI", 0.75, 0.75, "偏冷修复")
        else:
            add_vote("CCI", 0.0, 0.75, "常态")

        if result.roc_12 >= 5:
            add_vote("ROC", 1.0, 1.0, "动量加速")
        elif result.roc_12 <= -5:
            add_vote("ROC", -1.0, 1.0, "动量走弱")
        else:
            add_vote("ROC", 0.0, 1.0, "动量中性")

        if "突破20日唐奇安上轨" in result.donchian_signal:
            add_vote("唐奇安", 1.0, 1.0, "上轨突破")
        elif "跌破20日唐奇安下轨" in result.donchian_signal:
            add_vote("唐奇安", -1.0, 1.0, "下轨破位")
        else:
            add_vote("唐奇安", 0.0, 1.0, "通道内")

        if result.williams_r_14 >= -20:
            add_vote("Williams %R", -0.5, 0.5, "超买")
        elif result.williams_r_14 <= -80:
            add_vote("Williams %R", 0.5, 0.5, "超卖")
        else:
            add_vote("Williams %R", 0.0, 0.5, "中性")

        if result.stoch_rsi_14 >= 80:
            add_vote("StochRSI", -0.5, 0.5, "过热")
        elif result.stoch_rsi_14 <= 20:
            add_vote("StochRSI", 0.5, 0.5, "超卖")
        else:
            add_vote("StochRSI", 0.0, 0.5, "中性")

        if result.cmf_20 >= 0.05:
            add_vote("CMF", 1.0, 1.0, "资金流入")
        elif result.cmf_20 <= -0.05:
            add_vote("CMF", -1.0, 1.0, "资金流出")
        else:
            add_vote("CMF", 0.0, 1.0, "资金中性")

        if result.vwap_20 <= 0:
            add_vote("VWAP", 0.0, 0.75, "数据不足")
        elif result.vwap_distance_pct >= 5:
            add_vote("VWAP", -0.5, 0.75, "高于成本过多")
        elif result.vwap_distance_pct > 0:
            add_vote("VWAP", 0.35, 0.75, "成本线上方")
        elif result.vwap_distance_pct <= -5:
            add_vote("VWAP", -0.5, 0.75, "成本线压制")
        else:
            add_vote("VWAP", -0.2, 0.75, "略低于成本线")

        total_weight = sum(vote["weight"] for vote in votes) or 1.0
        raw_score = sum(vote["score"] for vote in votes)
        consensus_score = raw_score / total_weight * 100
        result.indicator_consensus_score = round(consensus_score, 2)

        bullish = [vote for vote in votes if vote["score"] > 0.2]
        bearish = [vote for vote in votes if vote["score"] < -0.2]
        neutral = [vote for vote in votes if -0.2 <= vote["score"] <= 0.2]
        result.indicator_bullish_count = len(bullish)
        result.indicator_bearish_count = len(bearish)
        result.indicator_neutral_count = len(neutral)

        if bullish and bearish and min(len(bullish), len(bearish)) >= 4 and abs(consensus_score) < 20:
            result.indicator_conflict_level = "高"
        elif bullish and bearish:
            result.indicator_conflict_level = "中"
        else:
            result.indicator_conflict_level = "低"

        if consensus_score >= 35:
            result.indicator_consensus_signal = "多指标强共振偏多"
        elif consensus_score >= 15:
            result.indicator_consensus_signal = "多指标温和偏多"
        elif consensus_score <= -35:
            result.indicator_consensus_signal = "多指标强共振偏空"
        elif consensus_score <= -15:
            result.indicator_consensus_signal = "多指标温和偏空"
        else:
            result.indicator_consensus_signal = "多指标分歧或中性"

        def summarize_votes(items: List[Dict[str, Any]], include_note: bool = True) -> str:
            if not items:
                return "无"
            if include_note:
                text = "、".join(f"{vote['name']}({vote['note']})" for vote in items[:6])
            else:
                text = "、".join(vote["name"] for vote in items[:6])
            if len(items) > 6:
                text += "等"
            return text

        bullish_text = summarize_votes(bullish)
        bearish_text = summarize_votes(bearish)
        neutral_text = summarize_votes(neutral, include_note=False)
        result.indicator_consensus_details = [
            f"偏多指标 {len(bullish)} 个：{bullish_text}",
            f"偏空指标 {len(bearish)} 个：{bearish_text}",
            f"中性指标 {len(neutral)} 个：{neutral_text}",
            f"冲突程度：{result.indicator_conflict_level}",
        ]

        if consensus_score >= 35:
            result.signal_reasons.append(
                f"{result.indicator_consensus_signal}，共振分 {result.indicator_consensus_score:+.1f}"
            )
        elif consensus_score <= -35:
            result.risk_factors.append(
                f"{result.indicator_consensus_signal}，共振分 {result.indicator_consensus_score:+.1f}"
            )
        elif result.indicator_conflict_level == "高":
            result.risk_factors.append("多指标分歧较大，单一指标信号不宜直接执行")

    def _generate_signal(self, result: TrendAnalysisResult) -> None:
        """
        生成买入信号

        综合评分系统：
        - 趋势（30分）：多头排列得分高
        - 乖离率（20分）：接近 MA5 得分高
        - 量能（15分）：缩量回调得分高
        - 支撑（10分）：获得均线支撑得分高
        - MACD（15分）：金叉和多头得分高
        - RSI（10分）：超卖和强势得分高
        """
        score = 0
        reasons = []
        risks = []

        # === 趋势评分（30分）===
        trend_scores = {
            TrendStatus.STRONG_BULL: 30,
            TrendStatus.BULL: 26,
            TrendStatus.WEAK_BULL: 18,
            TrendStatus.CONSOLIDATION: 12,
            TrendStatus.WEAK_BEAR: 8,
            TrendStatus.BEAR: 4,
            TrendStatus.STRONG_BEAR: 0,
        }
        trend_score = trend_scores.get(result.trend_status, 12)
        score += trend_score

        if result.trend_status in [TrendStatus.STRONG_BULL, TrendStatus.BULL]:
            reasons.append(f"✅ {result.trend_status.value}，顺势做多")
        elif result.trend_status in [TrendStatus.BEAR, TrendStatus.STRONG_BEAR]:
            risks.append(f"⚠️ {result.trend_status.value}，不宜做多")

        # === 乖离率评分（20分，强势趋势补偿）===
        bias = result.bias_ma5
        if bias != bias or bias is None:  # NaN or None defense
            bias = 0.0
        base_threshold = get_config().bias_threshold

        # Strong trend compensation: relax threshold for STRONG_BULL with high strength
        trend_strength = result.trend_strength if result.trend_strength == result.trend_strength else 0.0
        if result.trend_status == TrendStatus.STRONG_BULL and (trend_strength or 0) >= 70:
            effective_threshold = base_threshold * 1.5
            is_strong_trend = True
        else:
            effective_threshold = base_threshold
            is_strong_trend = False

        if bias < 0:
            # Price below MA5 (pullback)
            if bias > -3:
                score += 20
                reasons.append(f"✅ 价格略低于MA5({bias:.1f}%)，回踩买点")
            elif bias > -5:
                score += 16
                reasons.append(f"✅ 价格回踩MA5({bias:.1f}%)，观察支撑")
            else:
                score += 8
                risks.append(f"⚠️ 乖离率过大({bias:.1f}%)，可能破位")
        elif bias < 2:
            score += 18
            reasons.append(f"✅ 价格贴近MA5({bias:.1f}%)，介入好时机")
        elif bias < base_threshold:
            score += 14
            reasons.append(f"⚡ 价格略高于MA5({bias:.1f}%)，可小仓介入")
        elif bias > effective_threshold:
            score += 4
            risks.append(
                f"❌ 乖离率过高({bias:.1f}%>{effective_threshold:.1f}%)，严禁追高！"
            )
        elif bias > base_threshold and is_strong_trend:
            score += 10
            reasons.append(
                f"⚡ 强势趋势中乖离率偏高({bias:.1f}%)，可轻仓追踪"
            )
        else:
            score += 4
            risks.append(
                f"❌ 乖离率过高({bias:.1f}%>{base_threshold:.1f}%)，严禁追高！"
            )

        # === 量能评分（15分）===
        volume_scores = {
            VolumeStatus.SHRINK_VOLUME_DOWN: 15,  # 缩量回调最佳
            VolumeStatus.HEAVY_VOLUME_UP: 12,     # 放量上涨次之
            VolumeStatus.NORMAL: 10,
            VolumeStatus.SHRINK_VOLUME_UP: 6,     # 无量上涨较差
            VolumeStatus.HEAVY_VOLUME_DOWN: 0,    # 放量下跌最差
        }
        vol_score = volume_scores.get(result.volume_status, 8)
        score += vol_score

        if result.volume_status == VolumeStatus.SHRINK_VOLUME_DOWN:
            reasons.append("✅ 缩量回调，主力洗盘")
        elif result.volume_status == VolumeStatus.HEAVY_VOLUME_DOWN:
            risks.append("⚠️ 放量下跌，注意风险")

        # === 支撑评分（10分）===
        if result.support_ma5:
            score += 5
            reasons.append("✅ MA5支撑有效")
        if result.support_ma10:
            score += 5
            reasons.append("✅ MA10支撑有效")

        # === MACD 评分（15分）===
        macd_scores = {
            MACDStatus.GOLDEN_CROSS_ZERO: 15,  # 零轴上金叉最强
            MACDStatus.GOLDEN_CROSS: 12,      # 金叉
            MACDStatus.CROSSING_UP: 10,       # 上穿零轴
            MACDStatus.BULLISH: 8,            # 多头
            MACDStatus.BEARISH: 2,            # 空头
            MACDStatus.CROSSING_DOWN: 0,       # 下穿零轴
            MACDStatus.DEATH_CROSS: 0,        # 死叉
        }
        macd_score = macd_scores.get(result.macd_status, 5)
        score += macd_score

        if result.macd_status in [MACDStatus.GOLDEN_CROSS_ZERO, MACDStatus.GOLDEN_CROSS]:
            reasons.append(f"✅ {result.macd_signal}")
        elif result.macd_status in [MACDStatus.DEATH_CROSS, MACDStatus.CROSSING_DOWN]:
            risks.append(f"⚠️ {result.macd_signal}")
        else:
            reasons.append(result.macd_signal)

        # === RSI 评分（10分）===
        rsi_scores = {
            RSIStatus.OVERSOLD: 10,       # 超卖最佳
            RSIStatus.STRONG_BUY: 8,     # 强势
            RSIStatus.NEUTRAL: 5,        # 中性
            RSIStatus.WEAK: 3,            # 弱势
            RSIStatus.OVERBOUGHT: 0,       # 超买最差
        }
        rsi_score = rsi_scores.get(result.rsi_status, 5)
        score += rsi_score

        if result.rsi_status in [RSIStatus.OVERSOLD, RSIStatus.STRONG_BUY]:
            reasons.append(f"✅ {result.rsi_signal}")
        elif result.rsi_status == RSIStatus.OVERBOUGHT:
            risks.append(f"⚠️ {result.rsi_signal}")
        else:
            reasons.append(result.rsi_signal)

        # === 综合判断 ===
        result.signal_score = score
        result.signal_reasons = reasons
        result.risk_factors = risks

        # 生成买入信号（调整阈值以适应新的100分制）
        if score >= 75 and result.trend_status in [TrendStatus.STRONG_BULL, TrendStatus.BULL]:
            result.buy_signal = BuySignal.STRONG_BUY
        elif score >= 60 and result.trend_status in [TrendStatus.STRONG_BULL, TrendStatus.BULL, TrendStatus.WEAK_BULL]:
            result.buy_signal = BuySignal.BUY
        elif score >= 45:
            result.buy_signal = BuySignal.HOLD
        elif score >= 30:
            result.buy_signal = BuySignal.WAIT
        elif result.trend_status in [TrendStatus.BEAR, TrendStatus.STRONG_BEAR]:
            result.buy_signal = BuySignal.STRONG_SELL
        else:
            result.buy_signal = BuySignal.SELL
    
    def format_analysis(self, result: TrendAnalysisResult) -> str:
        """
        格式化分析结果为文本

        Args:
            result: 分析结果

        Returns:
            格式化的分析文本
        """
        lines = [
            f"=== {result.code} 趋势分析 ===",
            f"",
            f"📊 趋势判断: {result.trend_status.value}",
            f"   均线排列: {result.ma_alignment}",
            f"   趋势强度: {result.trend_strength}/100",
            f"",
            f"📈 均线数据:",
            f"   现价: {result.current_price:.2f}",
            f"   MA5:  {result.ma5:.2f} (乖离 {result.bias_ma5:+.2f}%)",
            f"   MA10: {result.ma10:.2f} (乖离 {result.bias_ma10:+.2f}%)",
            f"   MA20: {result.ma20:.2f} (乖离 {result.bias_ma20:+.2f}%)",
            f"",
            f"📊 量能分析: {result.volume_status.value}",
            f"   量比(vs5日): {result.volume_ratio_5d:.2f}",
            f"   量能趋势: {result.volume_trend}",
            f"",
            f"📈 MACD指标: {result.macd_status.value}",
            f"   DIF: {result.macd_dif:.4f}",
            f"   DEA: {result.macd_dea:.4f}",
            f"   MACD: {result.macd_bar:.4f}",
            f"   信号: {result.macd_signal}",
            f"",
            f"📊 RSI指标: {result.rsi_status.value}",
            f"   RSI(6): {result.rsi_6:.1f}",
            f"   RSI(12): {result.rsi_12:.1f}",
            f"   RSI(24): {result.rsi_24:.1f}",
            f"   信号: {result.rsi_signal}",
            f"",
            f"🎯 操作建议: {result.buy_signal.value}",
            f"   综合评分: {result.signal_score}/100",
        ]

        if result.signal_reasons:
            lines.append(f"")
            lines.append(f"✅ 买入理由:")
            for reason in result.signal_reasons:
                lines.append(f"   {reason}")

        if result.risk_factors:
            lines.append(f"")
            lines.append(f"⚠️ 风险因素:")
            for risk in result.risk_factors:
                lines.append(f"   {risk}")

        return "\n".join(lines)


def analyze_stock(df: pd.DataFrame, code: str) -> TrendAnalysisResult:
    """
    便捷函数：分析单只股票
    
    Args:
        df: 包含 OHLCV 数据的 DataFrame
        code: 股票代码
        
    Returns:
        TrendAnalysisResult 分析结果
    """
    analyzer = StockTrendAnalyzer()
    return analyzer.analyze(df, code)


if __name__ == "__main__":
    # 测试代码
    logging.basicConfig(level=logging.INFO)
    
    # 模拟数据测试
    import numpy as np
    
    dates = pd.date_range(start='2025-01-01', periods=60, freq='D')
    np.random.seed(42)
    
    # 模拟多头排列的数据
    base_price = 10.0
    prices = [base_price]
    for i in range(59):
        change = np.random.randn() * 0.02 + 0.003  # 轻微上涨趋势
        prices.append(prices[-1] * (1 + change))
    
    df = pd.DataFrame({
        'date': dates,
        'open': prices,
        'high': [p * (1 + np.random.uniform(0, 0.02)) for p in prices],
        'low': [p * (1 - np.random.uniform(0, 0.02)) for p in prices],
        'close': prices,
        'volume': [np.random.randint(1000000, 5000000) for _ in prices],
    })
    
    analyzer = StockTrendAnalyzer()
    result = analyzer.analyze(df, '000001')
    print(analyzer.format_analysis(result))
