"""Structured theory coverage mapping for metaphysical research sources.

This module turns qualitative source material into a reusable comparison layer:
- what a source is claiming
- where the current codebase already has support
- where the gap is still narrative-only
- which gaps are worth converting into features next
"""

from __future__ import annotations

from dataclasses import asdict, dataclass


@dataclass(frozen=True)
class TheoryCoverage:
    """A single source-theory item mapped against current code support."""

    theory_id: str
    category: str
    title: str
    summary: str
    source_video_ids: tuple[str, ...]
    current_support: str
    support_level: str
    code_refs: tuple[str, ...]
    missing_capabilities: tuple[str, ...]
    next_step: str

    def to_dict(self) -> dict:
        return asdict(self)


AUTHOR_THEORY_CATALOG: tuple[TheoryCoverage, ...] = (
    TheoryCoverage(
        theory_id="macro-kondratiev-juglar",
        category="macro_cycle",
        title="康波/长波叙事与历史节点",
        summary=(
            "作者频繁使用康波、货币周期、历史性制度节点来解释 2026 前后的大变局，"
            "核心是把市场放到超长期社会-货币周期里。"
        ),
        source_video_ids=("7619177154948926735", "7338205211552451881", "7624009172861897990"),
        current_support="已有宏观周期文本策略，但未进入共享玄学特征层。",
        support_level="partial",
        code_refs=("strategies/macro_cycle_positioning.yaml",),
        missing_capabilities=(
            "可量化的宏观周期状态机",
            "周期阶段置信度评分",
            "宏观周期与市场特征共振接口",
        ),
        next_step="先把宏观周期从 YAML 叙事升级为结构化 regime 特征，再接入多因子分析。",
    ),
    TheoryCoverage(
        theory_id="astro-geopolitical-narrative",
        category="astro_story",
        title="国家/地缘事件的星盘叙事",
        summary=(
            "作者会把美伊冲突、战争与货币体系变化写成星盘剧本，重心不只是相位，"
            "而是特定主体与事件的叙事映射。"
        ),
        source_video_ids=("7631048490960932138", "7621017683856854272", "7619907500166466850"),
        current_support="现有代码只支持通用行星经度和通用硬相位，不支持主体化命盘或事件盘。",
        support_level="weak",
        code_refs=("src/models/metaphysical/astro.py", "src/models/metaphysical/signals.py"),
        missing_capabilities=(
            "国家/资产/事件 natal chart 数据层",
            "主体关系盘/过境盘",
            "叙事型事件模板与证据链",
        ),
        next_step="先定义事件盘数据结构，不急着做自动预测，先支持研究记录与对照验证。",
    ),
    TheoryCoverage(
        theory_id="gann-price-time-square",
        category="gann_geometry",
        title="江恩时间价格平方",
        summary=(
            "作者明显依赖江恩时间-价格对称、时间价格平方、关键循环天数来判断变盘窗。"
        ),
        source_video_ids=("7618030650313936154", "7331586441765473577", "7331213561160650023"),
        current_support="已有基础版平方九和时间平方，但仍是通用工具，不含作者式判盘流程。",
        support_level="strong",
        code_refs=("src/models/metaphysical/gann.py", "src/models/metaphysical/strategy.py"),
        missing_capabilities=(
            "时间价格联立评分",
            "关键价位与关键日期的共振打分",
            "更细的角度/倍数/锚点选择机制",
        ),
        next_step="把现有 Gann 工具从静态列表升级为 price-time resonance scorer。",
    ),
    TheoryCoverage(
        theory_id="longitude-to-price",
        category="gann_geometry",
        title="经度转换价格/星盘价格映射",
        summary=(
            "作者不止讲普通江恩位，还在尝试把经度、星盘角度直接映射到价格，"
            "属于几何金融的更激进版本。"
        ),
        source_video_ids=("7331213561160650023", "7208309131751558404", "7205757957131357496"),
        current_support="代码里有行星经度，但没有经度到价格轴的映射层。",
        support_level="missing",
        code_refs=("src/models/metaphysical/astro.py",),
        missing_capabilities=(
            "longitude-to-price transform",
            "资产价格尺度归一化规则",
            "价格映射后的回测验证",
        ),
        next_step="这是值得单开实验分支验证的高风险理论，不建议直接进生产候选。",
    ),
    TheoryCoverage(
        theory_id="uranus-84y-and-retrograde",
        category="astro_cycle",
        title="天王星 84 年周期与逆行窗口",
        summary=(
            "作者把天王星周期当作美国级别的历史节点，并反复使用逆行/冲突窗口解释大波动。"
        ),
        source_video_ids=("7337898311615073548", "7619907500166466850", "7619177154948926735"),
        current_support="已有 Uranus 参与的短周期触发器，但没有 84 年长期循环和逆行特征。",
        support_level="partial",
        code_refs=("src/models/metaphysical/signals.py", "resonance_backtest.py"),
        missing_capabilities=(
            "Uranus retrograde feature",
            "84-year recurrence distance",
            "国家级锚点周年对照",
        ),
        next_step="优先补长期循环距离特征，比直接做国家命盘更容易量化。",
    ),
    TheoryCoverage(
        theory_id="saturn-jupiter-20y-cycle",
        category="astro_cycle",
        title="土木 20 年循环",
        summary=(
            "作者明确使用土木循环讲长期金融秩序与指数结构，接近传统金融占星的骨架理论。"
        ),
        source_video_ids=("7331586441765473577",),
        current_support="当前没有土木合相/循环距离的专门特征。",
        support_level="missing",
        code_refs=("src/models/metaphysical/signals.py",),
        missing_capabilities=(
            "Saturn-Jupiter conjunction cycle distance",
            "20-year macro pivot windows",
            "土木循环与指数 regime 对照",
        ),
        next_step="这是最适合新增的长期周期特征之一，定义清晰、回测窗口也足够长。",
    ),
    TheoryCoverage(
        theory_id="volatility-plus-turning-points",
        category="market_timing",
        title="波动率 Plus / 月度转折点",
        summary=(
            "作者常用波动率低点、月度关键转折、某月何去何从这类 timing 叙事，"
            "本质是时间窗与波动压缩的联合判断。"
        ),
        source_video_ids=("7620287744396365056", "7352694389040336169", "7341920663831072039", "7340881412154756364"),
        current_support="保力加/三重共振部分已覆盖波动压缩，但没有作者式月份转折模板。",
        support_level="partial",
        code_refs=("src/models/metaphysical/trend_law.py", "src/models/metaphysical/strategy.py"),
        missing_capabilities=(
            "month-turning-point templates",
            "volatility-plus derived score",
            "时间窗与波动压缩联动信号",
        ),
        next_step="可先从 month-turning-point 特征做起，和现有 Bollinger 特征天然兼容。",
    ),
    TheoryCoverage(
        theory_id="geometry-pythagorean-market",
        category="gann_geometry",
        title="几何/勾股定理解释市场",
        summary=(
            "作者把几何、勾股定理、价格波动率统一到一个几何金融叙事里，"
            "强调图形关系而不只是一组固定角度。"
        ),
        source_video_ids=("7208309131751558404", "7205757957131357496", "7205423964116520247"),
        current_support="已有基础 Gann 角度工具，但没有几何模式识别与几何相似度。",
        support_level="partial",
        code_refs=("src/models/metaphysical/gann.py",),
        missing_capabilities=(
            "几何模式库",
            "price-path shape similarity",
            "几何叙事到特征的可解释桥接",
        ),
        next_step="先不要神化几何，优先做可验证的 shape feature，再回头吸收话术层。",
    ),
)


def get_author_theory_catalog() -> list[dict]:
    """Return the structured theory catalog as plain dictionaries."""
    return [item.to_dict() for item in AUTHOR_THEORY_CATALOG]


def summarize_author_theory_coverage() -> dict:
    """Return a compact coverage summary for the current source catalog."""
    items = get_author_theory_catalog()
    counts = {"strong": 0, "partial": 0, "weak": 0, "missing": 0}
    for item in items:
        counts[item["support_level"]] += 1

    priority_gaps = [
        item["theory_id"]
        for item in items
        if item["support_level"] in {"partial", "missing"}
    ]

    return {
        "total_theories": len(items),
        "coverage_counts": counts,
        "priority_gap_ids": priority_gaps,
        "best_next_targets": [
            "saturn-jupiter-20y-cycle",
            "uranus-84y-and-retrograde",
            "volatility-plus-turning-points",
            "macro-kondratiev-juglar",
        ],
    }

