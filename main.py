# -*- coding: utf-8 -*-
"""
===================================
A股自选股智能分析系统 - 主调度程序
===================================

职责：
1. 协调各模块完成股票分析流程
2. 实现低并发的线程池调度
3. 全局异常处理，确保单股失败不影响整体
4. 提供命令行入口

使用方式：
    python main.py              # 正常运行
    python main.py --debug      # 调试模式
    python main.py --dry-run    # 仅获取数据不分析

交易理念（已融入分析）：
- 严进策略：不追高，乖离率 > 5% 不买入
- 趋势交易：只做 MA5>MA10>MA20 多头排列
- 效率优先：关注筹码集中度好的股票
- 买点偏好：缩量回踩 MA5/MA10 支撑
"""
import os
import json
import atexit
import re
from pathlib import Path
from typing import Any, Dict, Optional

from dotenv import dotenv_values
from src.config import setup_env

_INITIAL_PROCESS_ENV = dict(os.environ)
setup_env()


def _ensure_litellm_log_defaults() -> None:
    """Keep LiteLLM quiet unless the user explicitly asks for debug logging."""
    os.environ.setdefault("LITELLM_LOG", "INFO")


_ensure_litellm_log_defaults()

# 代理配置 - 通过 USE_PROXY 环境变量控制，默认关闭
# GitHub Actions 环境自动跳过代理配置
if os.getenv("GITHUB_ACTIONS") != "true" and os.getenv("USE_PROXY", "false").lower() == "true":
    # 本地开发环境，启用代理（可在 .env 中配置 PROXY_HOST 和 PROXY_PORT）
    proxy_host = os.getenv("PROXY_HOST", "127.0.0.1")
    proxy_port = os.getenv("PROXY_PORT", "10809")
    proxy_url = f"http://{proxy_host}:{proxy_port}"
    os.environ["http_proxy"] = proxy_url
    os.environ["https_proxy"] = proxy_url

import argparse
import logging
import multiprocessing as mp
import sys
import time
import uuid
from datetime import datetime, timezone, timedelta, date, time as datetime_time
from typing import List, Tuple

from data_provider.base import canonical_stock_code
from src.webui_frontend import prepare_webui_frontend_assets
from src.config import get_config, Config
from src.logging_config import setup_logging
from src.services.sniper_points import clean_sniper_items


logger = logging.getLogger(__name__)
_RUNTIME_ENV_FILE_KEYS = set()
_SCHEDULE_GUARD_PATH: Optional[Path] = None


def _get_active_env_path() -> Path:
    env_file = os.getenv("ENV_FILE")
    if env_file:
        return Path(env_file)
    return Path(__file__).resolve().parent / ".env"


def _read_active_env_values() -> Optional[Dict[str, str]]:
    env_path = _get_active_env_path()
    if not env_path.exists():
        return {}

    try:
        values = dotenv_values(env_path)
    except Exception as exc:  # pragma: no cover - defensive branch
        logger.warning("读取配置文件 %s 失败，继续沿用当前环境变量: %s", env_path, exc)
        return None

    return {
        str(key): "" if value is None else str(value)
        for key, value in values.items()
        if key is not None
    }


_ACTIVE_ENV_FILE_VALUES = _read_active_env_values() or {}
_RUNTIME_ENV_FILE_KEYS = {
    key for key in _ACTIVE_ENV_FILE_VALUES
    if key not in _INITIAL_PROCESS_ENV
}

# setup_env() already ran at import time above.
_env_bootstrapped = True


def _bootstrap_environment() -> None:
    """Load .env and apply optional local proxy settings.

    Guarded to be idempotent so it can safely be called from lazy-import
    paths used by API / bot consumers.
    """
    global _env_bootstrapped
    if _env_bootstrapped:
        return

    from src.config import setup_env

    setup_env()
    _ensure_litellm_log_defaults()

    if os.getenv("GITHUB_ACTIONS") != "true" and os.getenv("USE_PROXY", "false").lower() == "true":
        proxy_host = os.getenv("PROXY_HOST", "127.0.0.1")
        proxy_port = os.getenv("PROXY_PORT", "10809")
        proxy_url = f"http://{proxy_host}:{proxy_port}"
        os.environ["http_proxy"] = proxy_url
        os.environ["https_proxy"] = proxy_url

    _env_bootstrapped = True


def _setup_bootstrap_logging(debug: bool = False) -> None:
    """Initialize stderr-only logging before config is loaded.

    File handlers are deferred until ``config.log_dir`` is known (via the
    subsequent ``setup_logging()`` call) so that healthy runs never create
    log files in a hard-coded directory.
    """
    level = logging.DEBUG if debug else logging.INFO
    root = logging.getLogger()
    root.setLevel(level)
    if not any(
        isinstance(h, logging.StreamHandler) and getattr(h, "stream", None) is sys.stderr
        for h in root.handlers
    ):
        handler = logging.StreamHandler(sys.stderr)
        handler.setLevel(level)
        handler.setFormatter(
            logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
        )
        root.addHandler(handler)


def _get_stock_analysis_pipeline():
    """Lazily import StockAnalysisPipeline for external consumers.

    Also ensures env/proxy bootstrap has run so that API / bot consumers
    that never call ``main()`` still get ``USE_PROXY`` applied.
    """
    _bootstrap_environment()
    from src.core.pipeline import StockAnalysisPipeline as _Pipeline

    return _Pipeline


class _LazyPipelineDescriptor:
    """Descriptor that resolves StockAnalysisPipeline on first attribute access."""

    _resolved = None

    def __set_name__(self, owner, name):
        self._name = name

    def __get__(self, obj, objtype=None):
        if self._resolved is None:
            self._resolved = _get_stock_analysis_pipeline()
        return self._resolved


class _ModuleExports:
    StockAnalysisPipeline = _LazyPipelineDescriptor()


_exports = _ModuleExports()


def __getattr__(name: str):
    if name == "StockAnalysisPipeline":
        return _exports.StockAnalysisPipeline
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def _reload_env_file_values_preserving_overrides() -> None:
    """Refresh `.env`-managed env vars without clobbering process env overrides."""
    global _RUNTIME_ENV_FILE_KEYS

    latest_values = _read_active_env_values()
    if latest_values is None:
        return

    managed_keys = {
        key for key in latest_values
        if key not in _INITIAL_PROCESS_ENV
    }

    for key in _RUNTIME_ENV_FILE_KEYS - managed_keys:
        os.environ.pop(key, None)

    for key in managed_keys:
        os.environ[key] = latest_values[key]

    _RUNTIME_ENV_FILE_KEYS = managed_keys


def _is_process_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def _release_schedule_singleton_guard() -> None:
    global _SCHEDULE_GUARD_PATH
    if _SCHEDULE_GUARD_PATH is None:
        return
    try:
        if _SCHEDULE_GUARD_PATH.exists():
            content = _SCHEDULE_GUARD_PATH.read_text(encoding="utf-8").strip()
            if content == str(os.getpid()):
                _SCHEDULE_GUARD_PATH.unlink()
    except OSError:
        pass
    finally:
        _SCHEDULE_GUARD_PATH = None


def _acquire_schedule_singleton_guard(lock_path: Optional[Path] = None) -> bool:
    """Ensure only one local schedule process stays active."""
    global _SCHEDULE_GUARD_PATH

    lock_path = lock_path or (Path.home() / ".dsa_schedule.lock")
    current_pid = os.getpid()
    existing_pid = None
    if lock_path.exists():
        try:
            existing_pid = int(lock_path.read_text(encoding="utf-8").strip() or "0")
        except Exception:
            existing_pid = None

    if existing_pid and existing_pid != current_pid and _is_process_alive(existing_pid):
        logger.warning("检测到已有定时调度进程在运行(pid=%s)，当前实例将退出以避免重复推送。", existing_pid)
        return False

    lock_path.write_text(str(current_pid), encoding="utf-8")
    _SCHEDULE_GUARD_PATH = lock_path
    atexit.register(_release_schedule_singleton_guard)
    return True


def _should_run_in_schedule_mode(args: argparse.Namespace, config: Any) -> bool:
    """Decide whether this invocation should behave as the long-lived scheduler."""
    if getattr(args, "schedule", False):
        return True

    if any(
        getattr(args, flag, False)
        for flag in ("serve", "serve_only", "webui", "webui_only")
    ):
        return False

    if not getattr(config, "schedule_enabled", False):
        return False

    return not _has_explicit_one_shot_request(args)


def _is_cn_intraday_monitoring_session(now: Optional[datetime] = None) -> bool:
    """Return True only during regular A-share intraday monitoring windows."""
    current_time = now or datetime.now()
    try:
        from src.core.trading_calendar import is_market_open

        if not is_market_open("cn", current_time.date()):
            return False
    except Exception as exc:
        logger.debug("A股交易日历检查失败，盘中监控本轮跳过: %s", exc)
        return False

    current = current_time.time()
    morning = datetime_time(9, 30) <= current <= datetime_time(11, 30)
    afternoon = datetime_time(13, 0) <= current <= datetime_time(15, 0)
    return morning or afternoon


def _has_explicit_one_shot_request(args: argparse.Namespace) -> bool:
    """Whether CLI flags indicate a manual one-shot run instead of background service mode."""
    return any(
        [
            bool(getattr(args, "stocks", None)),
            bool(getattr(args, "market_review", False)),
            bool(getattr(args, "backtest", False)),
            bool(getattr(args, "dry_run", False)),
            bool(getattr(args, "force_run", False)),
        ]
    )


def _resolve_manual_report_filenames(
    args: argparse.Namespace,
    stock_codes: Optional[List[str]] = None,
) -> Optional[Dict[str, str]]:
    """Return dedicated filenames for ad-hoc CLI stock tests so they don't overwrite daily artifacts."""
    if getattr(args, "schedule", False):
        return None

    explicit_stocks = getattr(args, "stocks", None)
    if not explicit_stocks:
        return None

    codes = [code for code in (stock_codes or []) if code]
    date_str = datetime.now().strftime("%Y%m%d")

    if len(codes) == 1:
        code_suffix = codes[0]
        return {
            "dashboard_filename": f"report_{date_str}_{code_suffix}.md",
            "market_review_filename": f"market_review_{date_str}_{code_suffix}.md",
            "daily_push_filename": f"{datetime.now().strftime('%Y-%m-%d')}_{code_suffix}_盘前总报告.md",
        }

    return {
        "dashboard_filename": f"report_{date_str}_manual.md",
        "market_review_filename": f"market_review_{date_str}_manual.md",
        "daily_push_filename": f"{datetime.now().strftime('%Y-%m-%d')}_manual_盘前总报告.md",
    }


def parse_arguments() -> argparse.Namespace:
    """解析命令行参数"""
    parser = argparse.ArgumentParser(
        description='A股自选股智能分析系统',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
示例:
  python main.py                    # 正常运行
  python main.py --debug            # 调试模式
  python main.py --dry-run          # 仅获取数据，不进行 AI 分析
  python main.py --stocks 600519,000001  # 指定分析特定股票
  python main.py --no-notify        # 不发送推送通知
  python main.py --single-notify    # 启用单股推送模式（每分析完一只立即推送）
  python main.py --schedule         # 启用定时任务模式
  python main.py --market-review    # 仅运行大盘复盘
        '''
    )

    parser.add_argument(
        '--debug',
        action='store_true',
        help='启用调试模式，输出详细日志'
    )

    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='仅获取数据，不进行 AI 分析'
    )

    parser.add_argument(
        '--stocks',
        type=str,
        help='指定要分析的股票代码，逗号分隔（覆盖配置文件）'
    )

    parser.add_argument(
        '--no-notify',
        action='store_true',
        help='不发送推送通知'
    )

    parser.add_argument(
        '--single-notify',
        action='store_true',
        help='启用单股推送模式：每分析完一只股票立即推送，而不是汇总推送'
    )

    parser.add_argument(
        '--workers',
        type=int,
        default=None,
        help='并发线程数（默认使用配置值）'
    )

    parser.add_argument(
        '--schedule',
        action='store_true',
        help='启用定时任务模式，每日定时执行'
    )

    parser.add_argument(
        '--no-run-immediately',
        action='store_true',
        help='定时任务启动时不立即执行一次'
    )

    parser.add_argument(
        '--market-review',
        action='store_true',
        help='仅运行大盘复盘分析'
    )

    parser.add_argument(
        '--no-market-review',
        action='store_true',
        help='跳过大盘复盘分析'
    )

    parser.add_argument(
        '--force-run',
        action='store_true',
        help='跳过交易日检查，强制执行全量分析（Issue #373）'
    )

    parser.add_argument(
        '--webui',
        action='store_true',
        help='启动 Web 管理界面'
    )

    parser.add_argument(
        '--webui-only',
        action='store_true',
        help='仅启动 Web 服务，不执行自动分析'
    )

    parser.add_argument(
        '--serve',
        action='store_true',
        help='启动 FastAPI 后端服务（同时执行分析任务）'
    )

    parser.add_argument(
        '--serve-only',
        action='store_true',
        help='仅启动 FastAPI 后端服务，不自动执行分析'
    )

    parser.add_argument(
        '--port',
        type=int,
        default=8000,
        help='FastAPI 服务端口（默认 8000）'
    )

    parser.add_argument(
        '--host',
        type=str,
        default='0.0.0.0',
        help='FastAPI 服务监听地址（默认 0.0.0.0）'
    )

    parser.add_argument(
        '--no-context-snapshot',
        action='store_true',
        help='不保存分析上下文快照'
    )

    # === Backtest ===
    parser.add_argument(
        '--backtest',
        action='store_true',
        help='运行回测（对历史分析结果进行评估）'
    )

    parser.add_argument(
        '--backtest-code',
        type=str,
        default=None,
        help='仅回测指定股票代码'
    )

    parser.add_argument(
        '--backtest-days',
        type=int,
        default=None,
        help='回测评估窗口（交易日数，默认使用配置）'
    )

    parser.add_argument(
        '--backtest-force',
        action='store_true',
        help='强制回测（即使已有回测结果也重新计算）'
    )

    return parser.parse_args()


def _compute_trading_day_filter(
    config: Config,
    args: argparse.Namespace,
    stock_codes: List[str],
) -> Tuple[List[str], Optional[str], bool]:
    """
    Compute filtered stock list and effective market review region (Issue #373).

    Returns:
        (filtered_codes, effective_region, should_skip_all)
        - effective_region None = use config default (check disabled)
        - effective_region '' = all relevant markets closed, skip market review
        - should_skip_all: skip entire run when no stocks and no market review to run
    """
    force_run = getattr(args, 'force_run', False)
    if force_run or not getattr(config, 'trading_day_check_enabled', True):
        return (stock_codes, None, False)

    from src.core.trading_calendar import (
        get_market_for_stock,
        get_open_markets_today,
        compute_effective_region,
    )

    open_markets = get_open_markets_today()
    filtered_codes = []
    for code in stock_codes:
        mkt = get_market_for_stock(code)
        if mkt in open_markets or mkt is None:
            filtered_codes.append(code)

    if config.market_review_enabled and not getattr(args, 'no_market_review', False):
        effective_region = compute_effective_region(
            getattr(config, 'market_review_region', 'cn') or 'cn', open_markets
        )
    else:
        effective_region = None

    should_skip_all = (not filtered_codes) and (effective_region or '') == ''
    return (filtered_codes, effective_region, should_skip_all)


def _should_merge_daily_push(config: Config, args: argparse.Namespace) -> bool:
    """Whether this run should send one combined daily push."""
    if not _should_send_runtime_notifications(config, args):
        return False

    if getattr(config, "single_stock_notify", False):
        return False

    if getattr(config, "market_daily_push_enabled", True):
        return True

    return (
        getattr(config, 'merge_email_notification', False)
        and config.market_review_enabled
        and not getattr(args, 'no_market_review', False)
    )


def _should_send_runtime_notifications(config: Config, args: argparse.Namespace) -> bool:
    """Gate push notifications so ad-hoc local runs stay quiet by default."""
    if getattr(args, "no_notify", False):
        return False

    if getattr(args, "single_notify", False) or getattr(config, "single_stock_notify", False):
        return True

    if getattr(args, "_scheduled_invocation", False):
        return True

    # Only explicit scheduled runs should push by default.
    return False


def _is_preposition_candidate(result: Any) -> bool:
    advice = str(getattr(result, "operation_advice", "") or "").lower()
    decision_type = str(getattr(result, "decision_type", "") or "").lower()
    score = int(getattr(result, "sentiment_score", 0) or 0)
    bullish_tokens = ("买", "加仓", "建仓", "低吸", "buy", "accumulate", "add")

    if decision_type == "buy":
        return score >= 60

    return score >= 65 and any(token in advice for token in bullish_tokens)


def _infer_preposition_style(
    *,
    is_etf: bool,
    catalysts: List[str],
    sniper: Dict[str, Any],
    macro_reasons: List[str],
) -> str:
    """Infer a concise positioning style label for the recommendation."""
    if macro_reasons and catalysts:
        return "事件驱动"
    if sniper:
        return "左侧埋伏"
    if is_etf:
        return "趋势跟随"
    if catalysts:
        return "趋势跟随"
    return "观察跟踪"


def _infer_recommendation_tags(
    *,
    is_etf: bool,
    catalysts: List[str],
    risks: List[str],
    macro_reasons: List[str],
) -> List[str]:
    """Build short tags so the push can be scanned quickly."""
    tags = ["ETF优先" if is_etf else "个股弹性"]
    if macro_reasons:
        tags.append("宏观共振")
    if catalysts:
        tags.append("消息催化")
    if risks:
        tags.append("控制节奏")
    return tags[:3]


def _collect_macro_reasons_for_candidate(
    *,
    is_etf: bool,
    etf_subtype: str,
    commodity_targets: Optional[List[str]],
    macro_bias_map: Dict[str, Dict[str, Any]],
) -> Tuple[int, List[str]]:
    macro_bonus = 0
    macro_reasons: List[str] = []

    csi500_bias = macro_bias_map.get("中证500指数")
    if is_etf and csi500_bias and etf_subtype in {"宽基ETF", "行业ETF", "其他ETF"}:
        if csi500_bias.get("label") == "利多":
            macro_bonus += 3
            macro_reasons.append(f"中证500指数{csi500_bias['label']}")
        elif csi500_bias.get("label") == "利空":
            macro_bonus -= 3
            macro_reasons.append(f"中证500指数{csi500_bias['label']}")

    if is_etf and etf_subtype == "商品ETF" and commodity_targets:
        for asset_name in commodity_targets:
            asset_bias = macro_bias_map.get(asset_name)
            if not asset_bias:
                continue
            label = asset_bias.get("label")
            trend_note = str(asset_bias.get("trend_note") or "")
            if label == "利多":
                macro_bonus += 2
                macro_reasons.append(f"{asset_name}{label}")
            elif label == "利空":
                macro_bonus -= 2
                macro_reasons.append(f"{asset_name}{label}")

            if trend_note:
                if "连续走强" in trend_note:
                    macro_bonus += 1
                    macro_reasons.append(f"{asset_name}{trend_note}")
                elif "连续走弱" in trend_note:
                    macro_bonus -= 1
                    macro_reasons.append(f"{asset_name}{trend_note}")
                elif "最新转强" in trend_note:
                    macro_bonus += 1
                    macro_reasons.append(f"{asset_name}{trend_note}")
                elif "最新转弱" in trend_note:
                    macro_bonus -= 1
                    macro_reasons.append(f"{asset_name}{trend_note}")

    return macro_bonus, macro_reasons[:3]


def _infer_commodity_macro_targets(result: Any) -> List[str]:
    """Map commodity ETFs to the specific macro lines they should follow."""
    name = str(getattr(result, "name", "") or "").upper()
    code = str(getattr(result, "code", "") or "").upper()
    text = f"{name} {code}"

    targets: List[str] = []
    if any(marker in text for marker in ["黄金", "GOLD", "518880", "518800", "159934"]):
        targets.append("黄金期货")
    if any(marker in text for marker in ["白银", "SILVER"]):
        targets.append("白银期货")
    return targets


def _classify_etf_subtype(result: Any) -> str:
    """Roughly classify ETF recommendations for push grouping."""
    name = str(getattr(result, "name", "") or "").upper()
    code = str(getattr(result, "code", "") or "").upper()
    text = f"{name} {code}"

    broad_markers = ["300", "500", "1000", "A50", "沪深", "中证", "创业板", "科创", "红利", "宽基"]
    commodity_markers = ["黄金", "白银", "有色", "原油", "豆粕", "商品", "纳指商品", "黄金ETF", "黄金LOF"]
    sector_markers = ["证券", "银行", "芯片", "半导体", "医药", "消费", "军工", "新能源", "光伏", "人工智能", "机器人", "煤炭"]

    if any(marker in text for marker in commodity_markers):
        return "商品ETF"
    if any(marker in text for marker in sector_markers):
        return "行业ETF"
    if any(marker in text for marker in broad_markers):
        return "宽基ETF"
    return "其他ETF"


def _build_prepositioning_section(
    results: List[Any],
    *,
    macro_bias_items: Optional[List[Dict[str, Any]]] = None,
    max_items: int = 5,
) -> Optional[str]:
    """Build a concise stock / ETF recommendation section for early positioning."""
    macro_bias_map = {
        item.get("asset_name", ""): item
        for item in (macro_bias_items or [])
        if isinstance(item, dict)
    }
    candidates = []
    for result in sorted(results, key=lambda item: getattr(item, "sentiment_score", 0), reverse=True):
        if not _is_preposition_candidate(result):
            continue

        dashboard = getattr(result, "dashboard", {}) or {}
        intel = dashboard.get("intelligence", {}) if isinstance(dashboard, dict) else {}
        catalysts = list(intel.get("positive_catalysts", []) or [])
        risks = list(intel.get("risk_alerts", []) or [])
        sniper = result.get_sniper_points() if hasattr(result, "get_sniper_points") else {}
        sniper_items = clean_sniper_items(sniper, max_items=4)
        no_position_advice = (
            result.get_position_advice(False)
            if hasattr(result, "get_position_advice")
            else getattr(result, "operation_advice", "")
        )
        display_name = getattr(result, "name", result.code)
        is_etf = "ETF" in str(display_name).upper() or "ETF" in str(getattr(result, "code", "")).upper()
        etf_subtype = _classify_etf_subtype(result) if is_etf else ""
        commodity_targets = _infer_commodity_macro_targets(result) if etf_subtype == "商品ETF" else []
        macro_bonus, macro_reasons = _collect_macro_reasons_for_candidate(
            is_etf=is_etf,
            etf_subtype=etf_subtype,
            commodity_targets=commodity_targets,
            macro_bias_map=macro_bias_map,
        )

        style = _infer_preposition_style(
            is_etf=is_etf,
            catalysts=catalysts,
            sniper=dict(sniper_items),
            macro_reasons=macro_reasons,
        )
        tags = _infer_recommendation_tags(
            is_etf=is_etf,
            catalysts=catalysts,
            risks=risks,
            macro_reasons=macro_reasons,
        )

        priority = (
            int(getattr(result, "sentiment_score", 0) or 0)
            + min(len(catalysts), 3) * 2
            + (2 if sniper_items else 0)
            - min(len(risks), 2) * 2
            + macro_bonus
        )
        candidates.append(
            {
                "result": result,
                "priority": priority,
                "catalysts": catalysts[:2],
                "risks": risks[:2],
                "sniper": sniper_items,
                "no_position_advice": no_position_advice,
                "macro_reasons": macro_reasons,
                "style": style,
                "tags": tags,
                "etf_subtype": etf_subtype,
            }
        )

    if not candidates:
        return None

    ordered = sorted(candidates, key=lambda item: item["priority"], reverse=True)[:max_items]
    macro_headline = []
    for asset_name in ["黄金期货", "白银期货", "中证500指数"]:
        item = macro_bias_map.get(asset_name)
        if item:
            macro_headline.append(f"{asset_name}{item['label']}")

    lines = ["结合当日宏观利多利空、消息催化与技术位置，优先关注以下可提前布局标的："]
    if macro_headline:
        lines.append(f"当前宏观线索: {'；'.join(macro_headline)}")
    lines.append("")

    etf_items = [
        item for item in ordered
        if "ETF" in str(getattr(item["result"], "name", item["result"].code)).upper()
        or "ETF" in str(getattr(item["result"], "code", "")).upper()
    ]
    stock_items = [
        item for item in ordered
        if item not in etf_items
    ]

    def _append_group(group_name: str, items: List[Dict[str, Any]]) -> None:
        if not items:
            return

        lines.append(f"## {group_name}优先观察")
        lines.append("")
        for index, item in enumerate(items, 1):
            result = item["result"]
            display_name = getattr(result, "name", result.code)
            tag_text = f" | 标签: {', '.join(item['tags'])}" if item["tags"] else ""
            lines.append(
                f"{index}. **{display_name}({result.code})** | 类型: {group_name} | 风格: {item['style']} | 建议: {item['no_position_advice']} | 评分: {result.sentiment_score}{tag_text}"
            )

            core_conclusion = (
                result.get_core_conclusion()
                if hasattr(result, "get_core_conclusion")
                else getattr(result, "analysis_summary", "")
            )
            if core_conclusion:
                lines.append(f"> 核心逻辑: {core_conclusion}")
            if item["catalysts"]:
                lines.append(f"> 催化因素: {'；'.join(item['catalysts'])}")
            if item["macro_reasons"]:
                lines.append(f"> 宏观映射: {'；'.join(item['macro_reasons'])}")
            if item["sniper"]:
                sniper_text = "；".join(
                    f"{key}:{value}"
                    for key, value in item["sniper"]
                    if value
                )
                if sniper_text:
                    lines.append(f"> 参考建仓: {sniper_text}")
            if item["risks"]:
                lines.append(f"> 风险提醒: {'；'.join(item['risks'])}")
            lines.append("")

    etf_group_map = {
        "宽基ETF": [item for item in etf_items if item.get("etf_subtype") == "宽基ETF"],
        "行业ETF": [item for item in etf_items if item.get("etf_subtype") == "行业ETF"],
        "商品ETF": [item for item in etf_items if item.get("etf_subtype") == "商品ETF"],
        "其他ETF": [item for item in etf_items if item.get("etf_subtype") == "其他ETF"],
    }
    for subgroup_name in ["宽基ETF", "行业ETF", "商品ETF", "其他ETF"]:
        _append_group(subgroup_name, etf_group_map[subgroup_name])
    _append_group("股票", stock_items)

    return "\n".join(lines).strip()


def _macro_item_map(macro_bias_items: Optional[List[Dict[str, Any]]]) -> Dict[str, Dict[str, Any]]:
    return {
        item.get("asset_name", ""): item
        for item in (macro_bias_items or [])
        if isinstance(item, dict)
    }


def _format_strategy_effect_line(label: str, impact: str, reason: str) -> str:
    return f"- **{label}**: {impact} | {reason}"


def _build_ic_candidate_execution_note(csi500_label: str, csi500_trend: str) -> str:
    """Return a short candidate execution note for the IC line."""
    if csi500_label == "利空" or "走弱" in csi500_trend:
        return "候选执行层: 若后续走到“趋势破坏+单日弱势”，当前待审节奏是先空仓2日，再回到原框架观察。"
    return "候选执行层: 先观察，不提前启用“空仓2日”节奏。"


def _build_strategy_impact_section(
    macro_bias_items: Optional[List[Dict[str, Any]]] = None,
) -> Optional[str]:
    """Summarize how today's macro lines affect the user's actual strategy set."""
    macro_bias_map = _macro_item_map(macro_bias_items)
    gold = macro_bias_map.get("黄金期货", {})
    silver = macro_bias_map.get("白银期货", {})
    csi500 = macro_bias_map.get("中证500指数", {})

    def _label(item: Dict[str, Any]) -> str:
        return str(item.get("label") or "中性")

    def _trend(item: Dict[str, Any]) -> str:
        return str(item.get("trend_note") or "")

    gold_label = _label(gold)
    silver_label = _label(silver)
    csi500_label = _label(csi500)
    gold_trend = _trend(gold)
    silver_trend = _trend(silver)
    csi500_trend = _trend(csi500)

    growth_impact = "中性"
    growth_reason = "暂未看到明显估值压缩或风险偏好修复信号"
    if csi500_label == "利空":
        growth_impact = "利空"
        growth_reason = "中证500承压，成长风格更容易受情绪和流动性拖累"
    elif csi500_label == "利多":
        growth_impact = "利多"
        growth_reason = "中证500走强，成长板块风险偏好边际修复"

    cashflow_impact = "中性"
    cashflow_reason = "当前更适合作为组合压舱石继续观察"
    if csi500_label == "利空":
        cashflow_reason = "市场偏弱时，自由现金流风格相对更抗波动"
    elif csi500_label == "利多":
        cashflow_reason = "若市场修复，自由现金流风格仍适合稳步持有"

    put_impact = "中性"
    put_reason = "暂不需要明显强化保护"
    if csi500_label == "利空" or "走弱" in csi500_trend:
        put_impact = "利多"
        put_reason = "指数风险偏弱，认沽保护的必要性上升"
    elif csi500_label == "利多":
        put_reason = "指数若偏强，保护仓位可以先控节奏"

    lines = [
        "## 策略影响",
        "",
        _format_strategy_effect_line(
            "黄金ETF（做多）",
            "利多" if gold_label == "利多" else "利空" if gold_label == "利空" else "中性",
            "黄金主线转强，继续偏向做多思路" if gold_label == "利多"
            else "黄金主线转弱，追高要更谨慎" if gold_label == "利空"
            else "黄金方向暂未形成明显单边优势",
        ),
        _format_strategy_effect_line(
            "白银期货",
            "利多" if silver_label == "利多" else "利空" if silver_label == "利空" else "中性",
            "白银期货偏强，白银方向按利多处理" if silver_label == "利多"
            else "白银期货偏弱，白银方向按利空处理" if silver_label == "利空"
            else "白银期货暂未给出明显单边信号",
        ),
        _format_strategy_effect_line(
            "IC贴水策略",
            "利多" if csi500_label == "利多" else "利空" if csi500_label == "利空" else "中性",
            "中证500偏强，更有利于贴水修复" if csi500_label == "利多"
            else (
                "中证500承压，需防基差陷阱和回撤；"
                f"{_build_ic_candidate_execution_note(csi500_label, csi500_trend)}"
            ) if csi500_label == "利空"
            else "IC主线暂偏震荡，先看基差变化再决策",
        ),
        _format_strategy_effect_line(
            "认沽期权保护",
            put_impact,
            put_reason,
        ),
        _format_strategy_effect_line(
            "自由现金流ETF",
            cashflow_impact,
            cashflow_reason,
        ),
        _format_strategy_effect_line(
            "高增长行业ETF",
            growth_impact,
            growth_reason,
        ),
    ]
    return "\n".join(lines)


def _build_black_swan_playbook_section(
    macro_bias_items: Optional[List[Dict[str, Any]]] = None,
) -> Optional[str]:
    """Show a concise defense/attack playbook only on relatively high-risk days."""
    macro_bias_map = _macro_item_map(macro_bias_items)
    gold = macro_bias_map.get("黄金期货", {})
    silver = macro_bias_map.get("白银期货", {})
    csi500 = macro_bias_map.get("中证500指数", {})

    risk_score = 0
    if csi500.get("label") == "利空":
        risk_score += 2
    if str(csi500.get("trend_note") or "").find("走弱") >= 0:
        risk_score += 1
    if gold.get("label") == "利多":
        risk_score += 1
    if silver.get("label") == "利空":
        risk_score += 1

    if risk_score < 3:
        return None

    lines = [
        "## 高风险日应对沙盘",
        "",
        "**偏防守方案**",
        "- 适用场景: 中证500继续转弱、贴水恶化，且黄金维持强势避险。",
        "- 期指思路: 控节奏处理IC敞口，先防基差继续走阔。",
        "- 候选执行层: 若后续走到“趋势破坏+单日弱势”，当前待审节奏是先空仓2日，再回到原框架观察。",
        "- 期权思路: 优先考虑中证500认沽保护，避免尾部波动放大。",
        "- 风险点: 若市场快速修复，保护仓位会回撤时间价值。",
        "",
        "**偏进攻方案**",
        "- 适用场景: 白银继续偏弱、黄金偏强，商品主线分化延续。",
        "- 期指思路: IC只适合等情绪企稳后再看贴水修复，不宜硬扛。",
        "- 期权思路: 以保护优先，进攻只适合围绕白银弱势和黄金相对强势做轻仓博弈。",
        "- 风险点: 地缘或政策预期突然缓和，强弱切换会很快。",
    ]
    return "\n".join(lines)


def _load_external_tactical_report(config: Config) -> Optional[str]:
    """Load one or more optional locally cached tactical reports."""
    raw_paths = str(getattr(config, "external_tactical_report_path", "") or "").strip()
    if not raw_paths:
        return None

    contents: List[str] = []
    for raw_path in [item.strip() for item in raw_paths.split(",") if item.strip()]:
        report_path = Path(raw_path)
        if not report_path.is_absolute():
            report_path = Path.cwd() / report_path
        try:
            if not report_path.exists() or not report_path.is_file():
                continue
            content = report_path.read_text(encoding="utf-8").strip()
            if content:
                contents.append(content)
        except Exception as exc:
            logger.warning("读取外部战术报告失败(%s): %s", report_path, exc)
    if not contents:
        return None
    return "\n\n---\n\n".join(contents)


def _summarize_external_tactical_report(content: Optional[str]) -> List[str]:
    """Extract a few high-signal lines from synced tactical reports."""
    if not content:
        return []
    lines: List[str] = []
    heading_like_pattern = re.compile(
        r"^(?:[一二三四五六七八九十0-9]+[、.．]\s*)?"
        r"(?:核心态势|黑天鹅触发状态|核心结论|今日必须|今日黑天鹅)$"
    )
    for raw_line in content.splitlines():
        line = raw_line.strip().lstrip("-").strip()
        if not line:
            continue
        if line.startswith("#"):
            continue
        normalized = line.strip("*").strip()
        if heading_like_pattern.match(normalized):
            continue
        if any(
            keyword in normalized
            for keyword in ("核心态势", "黑天鹅触发状态", "核心结论", "今日必须", "今日黑天鹅")
        ):
            lines.append(normalized)
        if len(lines) >= 3:
            break
    return lines[:3]


def _classify_external_reference_line(line: str) -> str:
    """Classify external tactical lines by how directly they can be trusted."""
    text = (line or "").strip()
    if not text:
        return "仅作辅助"

    high_confidence_keywords = (
        "贴水", "基差", "年化", "收益率", "涨跌", "大跌", "大涨", "%",
        "美元指数", "美债", "原油", "布伦特", "霍尔木兹", "期权", "认沽",
        "cpi", "非农", "利率决议",
    )
    low_confidence_keywords = (
        "干支", "十神", "火星", "天象", "木火", "财星", "相位",
        "增强警惕", "模型反馈", "玄学", "预示",
    )
    medium_confidence_keywords = (
        "核心态势", "黑天鹅", "今日必须", "核心结论", "市场恐慌", "流动性风险",
    )

    lowered = text.lower()
    if any(keyword in text for keyword in high_confidence_keywords) or any(
        keyword in lowered for keyword in ("vix", "ic", "mo put")
    ):
        return "可直接参考"
    if any(keyword in text for keyword in low_confidence_keywords):
        return "仅作辅助"
    if any(keyword in text for keyword in medium_confidence_keywords):
        return "需二次验证"
    return "需二次验证"


def _summarize_external_tactical_report_with_confidence(
    content: Optional[str],
) -> List[Dict[str, str]]:
    lines = _summarize_external_tactical_report(content)
    return [
        {"line": line, "confidence": _classify_external_reference_line(line)}
        for line in lines
    ]


def _reverse_bias_label(label: str) -> str:
    if label == "利多":
        return "利空"
    if label == "利空":
        return "利多"
    return label


def _infer_external_asset_biases_from_text(content: Optional[str]) -> Dict[str, str]:
    """Extract Gemini/external asset directions and normalize strategy wording."""
    if not content:
        return {}

    text = content.replace("（", "(").replace("）", ")")
    extracted: Dict[str, str] = {}
    direct_patterns = {
        "黄金期货": [r"黄金期货[:：\s(]+(利多|利空|中性)"],
        "白银期货": [r"白银期货[:：\s(]+(利多|利空|中性)"],
        "中证500指数": [r"中证\s*500(?:指数)?[:：\s(]+(利多|利空|中性)"],
    }
    strategy_patterns = {
        "黄金期货": [(r"黄金\s*ETF[:：\s(]+(利多|利空|中性)", False)],
        # Gemini 历史报告常写“白银空头”，这里统一换算成白银期货本体方向。
        "白银期货": [(r"白银空头[:：\s(]+(利多|利空|中性)", True)],
        "中证500指数": [(r"IC\s*贴水策略[:：\s(]+(利多|利空|中性)", False)],
    }

    for asset_name, patterns in direct_patterns.items():
        for pattern in patterns:
            match = re.search(pattern, text, flags=re.IGNORECASE)
            if match:
                extracted[asset_name] = match.group(1)
                break

    for asset_name, patterns in strategy_patterns.items():
        if asset_name in extracted:
            continue
        for pattern, should_reverse in patterns:
            match = re.search(pattern, text, flags=re.IGNORECASE)
            if not match:
                continue
            label = match.group(1)
            extracted[asset_name] = _reverse_bias_label(label) if should_reverse else label
            break

    return extracted


def _build_external_direction_conflict_section(
    *,
    macro_bias_items: Optional[List[Dict[str, Any]]] = None,
    external_tactical_report: Optional[str] = None,
) -> Optional[str]:
    """Build a concise section when local macro direction conflicts with Gemini."""
    local_map = _macro_item_map(macro_bias_items)
    external_map = _infer_external_asset_biases_from_text(external_tactical_report)
    if not local_map or not external_map:
        return None

    conflict_lines: List[str] = []
    for asset_name in ["黄金期货", "白银期货", "中证500指数"]:
        local_label = str((local_map.get(asset_name) or {}).get("label") or "")
        external_label = str(external_map.get(asset_name) or "")
        if local_label not in {"利多", "利空"} or external_label not in {"利多", "利空"}:
            continue
        if local_label == external_label:
            continue
        conflict_lines.append(
            f"- **{asset_name}**: 本地={local_label}，Gemini={external_label} | "
            "处理: 不按单一观点行动，先看开盘/盘中关键位与量价确认。"
        )

    if not conflict_lines:
        return None
    return "\n".join(["## 本地 vs Gemini 方向冲突", "", *conflict_lines])


def _build_metaphysical_signal_summary(
    *,
    external_tactical_report: Optional[str] = None,
    expected_report_date: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """Load the cached next-production signal and optionally apply tactical-report overlay."""
    try:
        cache_dir = Path.cwd() / ".cache" / "metaphysical_probabilities"
        if not any(cache_dir.glob("510500.SS_2016-01-01_*_min756_retrain42.pkl")):
            return None

        from src.models.metaphysical import latest_cached_next_production_signal

        result = latest_cached_next_production_signal(
            cache_dir=cache_dir,
            symbol="510500.SS",
            start="2016-01-01",
            end=datetime.now().strftime("%Y-%m-%d"),
            report_text=external_tactical_report,
            expected_report_date=expected_report_date,
        )
        return result
    except Exception as exc:
        logger.warning("加载玄学模型最终信号失败: %s", exc)
        return None


def _load_metaphysical_governance_summary() -> Optional[Dict[str, Any]]:
    """Load the latest metaphysical governance decision if available."""
    try:
        from src.models.metaphysical import latest_governance_run

        return latest_governance_run(Path.cwd() / "reports" / "metaphysical_governance_runs.jsonl")
    except Exception as exc:
        logger.warning("加载玄学模型治理记录失败: %s", exc)
        return None


def _load_metaphysical_stage_health_summary() -> Optional[Dict[str, Any]]:
    """Load the latest stage-performance record and derive a quick guardrail view."""
    try:
        from src.models.metaphysical import evaluate_stage_guardrail, latest_stage_performance_run

        ledger = Path.cwd() / "reports" / "metaphysical_stage_performance_runs.jsonl"
        latest = latest_stage_performance_run(ledger)
        if not latest:
            return None
        stage = str(latest.get("stage") or "candidate")
        guardrail = evaluate_stage_guardrail(
            ledger,
            stage=stage,
            recent_n=3,
            min_runs=1,
        )
        return {
            "latest": latest,
            "guardrail": guardrail,
        }
    except Exception as exc:
        logger.warning("加载玄学模型阶段健康记录失败: %s", exc)
        return None


def _load_metaphysical_lifecycle_summary() -> Optional[Dict[str, Any]]:
    """Load the latest metaphysical lifecycle decision if available."""
    try:
        from src.models.metaphysical import latest_lifecycle_run

        return latest_lifecycle_run(Path.cwd() / "reports" / "metaphysical_lifecycle_runs.jsonl")
    except Exception as exc:
        logger.warning("加载玄学模型生命周期记录失败: %s", exc)
        return None


def _load_metaphysical_switch_proposal_summary() -> Optional[Dict[str, Any]]:
    """Load the latest metaphysical version-switch proposal if available."""
    try:
        from src.models.metaphysical import latest_version_switch_proposal

        return latest_version_switch_proposal(
            Path.cwd() / "reports" / "metaphysical_version_switch_proposals.jsonl"
        )
    except Exception as exc:
        logger.warning("加载玄学模型版本切换草案失败: %s", exc)
        return None


def _build_metaphysical_daily_report_content(
    *,
    external_tactical_report: Optional[str] = None,
) -> Optional[str]:
    """Build the standalone metaphysical governance daily report if available."""
    try:
        project_root = Path(__file__).resolve().parent
        from src.models.metaphysical import (
            build_daily_governance_summary,
            render_daily_governance_summary,
        )

        summary = build_daily_governance_summary(
            cache_dir=project_root / ".cache" / "metaphysical_probabilities",
            symbol="510500.SS",
            start="2016-01-01",
            end="2026-04-20",
            governance_path=project_root / "reports" / "metaphysical_governance_runs.jsonl",
            lifecycle_path=project_root / "reports" / "metaphysical_lifecycle_runs.jsonl",
            stage_performance_path=project_root / "reports" / "metaphysical_stage_performance_runs.jsonl",
            switch_proposal_path=project_root / "reports" / "metaphysical_version_switch_proposals.jsonl",
            report_text=external_tactical_report,
        )
        return render_daily_governance_summary(summary)
    except Exception as exc:
        logger.warning("生成独立玄学治理日报失败: %s", exc)
        return None


def _build_daily_conclusion_section(
    *,
    report_date: str,
    macro_bias_items: Optional[List[Dict[str, Any]]] = None,
    external_tactical_report: Optional[str] = None,
) -> str:
    macro_bias_map = _macro_item_map(macro_bias_items)
    lines = [f"## 今日结论", ""]
    for asset_name in ["黄金期货", "白银期货", "中证500指数"]:
        item = macro_bias_map.get(asset_name)
        if not item:
            continue
        trend_note = f" | {item['trend_note']}" if item.get("trend_note") else ""
        reason = f" | 依据: {', '.join(item.get('reasons') or [])}" if item.get("reasons") else ""
        lines.append(f"- {asset_name}: {item.get('label', '中性')}{trend_note}{reason}")
    metaphysical_signal = _build_metaphysical_signal_summary(
        external_tactical_report=external_tactical_report,
        expected_report_date=report_date,
    )
    if metaphysical_signal:
        raw_regime = str(
            metaphysical_signal.get("raw_position_regime")
            or metaphysical_signal.get("position_regime")
            or "unknown"
        )
        final_regime = str(metaphysical_signal.get("position_regime") or "unknown")
        action = str(metaphysical_signal.get("action") or "hold")
        overlay_note = ""
        if metaphysical_signal.get("overlay_active"):
            reason_text = str(metaphysical_signal.get("overlay_reason") or "").strip()
            overlay_note = f" | 外部覆盖: {reason_text}" if reason_text else " | 外部覆盖: 已触发"
        lines.append(f"- 玄学模型: {raw_regime} -> {final_regime} | 动作: {action}{overlay_note}")
    tactical_lines = _summarize_external_tactical_report_with_confidence(external_tactical_report)
    for item in tactical_lines:
        lines.append(f"- 外部参考[{item['confidence']}]: {item['line']}")
    if len(lines) == 2:
        lines.append(f"- {report_date} 暂无明确单边结论，按既有计划观察。")
    return "\n".join(lines)


def _build_focus_targets_section(results: List[Any], max_items: int = 5) -> Optional[str]:
    if not results:
        return None
    ordered = sorted(results, key=lambda item: getattr(item, "sentiment_score", 0), reverse=True)[:max_items]
    lines = ["## 重点标的", ""]
    for result in ordered:
        core = (
            result.get_core_conclusion()
            if hasattr(result, "get_core_conclusion")
            else getattr(result, "analysis_summary", "")
        )
        core = (core or "").replace("\n", " ").strip()
        if len(core) > 40:
            core = core[:40].rstrip() + "..."
        lines.append(
            f"- {getattr(result, 'name', result.code)}({result.code}): "
            f"{getattr(result, 'operation_advice', '观望')} | 评分 {getattr(result, 'sentiment_score', 0)}"
            + (f" | {core}" if core else "")
        )
    return "\n".join(lines)


def _build_daily_push_summary(
    *,
    report_date: str,
    results: List[Any],
    macro_bias_items: Optional[List[Dict[str, Any]]] = None,
    external_tactical_report: Optional[str] = None,
) -> str:
    strategy_impact_section = _build_strategy_impact_section(macro_bias_items)
    external_conflict_section = _build_external_direction_conflict_section(
        macro_bias_items=macro_bias_items,
        external_tactical_report=external_tactical_report,
    )
    focus_targets_section = _build_focus_targets_section(results)
    playbook_section = _build_black_swan_playbook_section(macro_bias_items)

    sections = [
        f"# 📌 {report_date} 盘前总报告",
        _build_daily_conclusion_section(
            report_date=report_date,
            macro_bias_items=macro_bias_items,
            external_tactical_report=external_tactical_report,
        ),
        strategy_impact_section,
        external_conflict_section,
        focus_targets_section,
        playbook_section,
    ]
    return "\n\n---\n\n".join(section for section in sections if section)


def _resolve_next_cn_trading_date(now: Optional[datetime] = None) -> date:
    """Resolve the next A-share trading date for an evening outlook."""
    now = now or datetime.now()
    current = now.date()
    try:
        from src.core.trading_calendar import is_market_open

        probe = current + timedelta(days=1)
        for _ in range(14):
            if is_market_open("cn", probe):
                return probe
            probe += timedelta(days=1)
    except Exception as exc:
        logger.warning("解析下一交易日失败，回退到工作日规则: %s", exc)

    probe = current + timedelta(days=1)
    while probe.weekday() >= 5:
        probe += timedelta(days=1)
    return probe


def _macro_bias_signed_score(item: Optional[Dict[str, Any]]) -> float:
    """Convert a macro-bias item into a compact A-share directional score."""
    if not item:
        return 0.0

    label = str(item.get("label") or "中性")
    direction = 1.0 if label == "利多" else -1.0 if label == "利空" else 0.0
    strength = str(item.get("strength") or "弱")
    strength_scale = {"强": 1.45, "中": 1.0, "弱": 0.6}.get(strength, 0.6)
    score = direction * strength_scale

    raw_score = item.get("score")
    try:
        numeric_score = float(raw_score)
    except (TypeError, ValueError):
        numeric_score = 0.0
    if numeric_score:
        numeric_component = max(min(numeric_score / 4.0, 1.8), -1.8)
        if direction == 0:
            score = numeric_component
        elif (numeric_component > 0) == (score > 0):
            score = (score + numeric_component) / 2.0

    trend_note = str(item.get("trend_note") or "")
    if "连续走强" in trend_note or "最新转强" in trend_note:
        score += 0.3
    elif "连续走弱" in trend_note or "最新转弱" in trend_note:
        score -= 0.3
    return score


def _calculate_next_day_market_direction(
    macro_bias_items: Optional[List[Dict[str, Any]]] = None,
    external_tactical_report: Optional[str] = None,
) -> Dict[str, Any]:
    """Build a deterministic next-day A-share directional view."""
    macro_bias_map = _macro_item_map(macro_bias_items)
    drivers: List[str] = []
    total_score = 0.0

    def _append_driver(asset_name: str, score: float, weight: float, note: str = "") -> None:
        nonlocal total_score
        if not score:
            return
        total_score += score * weight
        item = macro_bias_map.get(asset_name, {})
        label = item.get("label", "中性")
        strength = item.get("strength", "")
        suffix = f"({strength})" if strength and label != "中性" else ""
        drivers.append(f"{asset_name}{label}{suffix}{note}")

    csi500_score = _macro_bias_signed_score(macro_bias_map.get("中证500指数"))
    golden_dragon_score = _macro_bias_signed_score(macro_bias_map.get("纳斯达克中国金龙指数"))
    silver_score = _macro_bias_signed_score(macro_bias_map.get("白银期货"))
    gold_score = _macro_bias_signed_score(macro_bias_map.get("黄金期货"))

    _append_driver("中证500指数", csi500_score, 1.25)
    _append_driver("纳斯达克中国金龙指数", golden_dragon_score, 0.8, "，隔夜风险偏好线索")

    gold = macro_bias_map.get("黄金期货", {})
    gold_text = " ".join(
        [
            str(gold.get("label") or ""),
            str(gold.get("trend_note") or ""),
            " ".join(str(item) for item in (gold.get("reasons") or [])),
            str(gold.get("summary") or ""),
        ]
    )
    risk_off_keywords = ("避险", "地缘", "冲突", "中东", "俄乌", "关税", "风险偏好回落")
    if gold_score > 0 and any(keyword in gold_text for keyword in risk_off_keywords):
        total_score -= min(abs(gold_score), 1.2) * 0.45
        drivers.append("黄金避险线走强，压制A股风险偏好")
    elif gold_score < 0:
        total_score += min(abs(gold_score), 1.0) * 0.15
        drivers.append("黄金避险线降温，对A股压力边际减弱")

    if silver_score > 0:
        total_score += min(abs(silver_score), 1.2) * 0.25
        drivers.append("白银偏强，风险偏好略有支撑")
    elif silver_score < 0:
        total_score -= min(abs(silver_score), 1.2) * 0.25
        drivers.append("白银偏弱，周期风险偏好承压")

    for item in _summarize_external_tactical_report_with_confidence(external_tactical_report):
        line = item["line"]
        confidence = item["confidence"]
        if confidence == "仅作辅助":
            continue
        bearish_hit = any(keyword in line for keyword in ("高风险", "清仓", "认沽", "贴水", "基差", "塌陷", "黑天鹅"))
        bullish_hit = any(keyword in line for keyword in ("做多A股", "风险偏好回升", "低吸", "加仓", "修复"))
        if bearish_hit:
            total_score -= 0.45 if confidence == "可直接参考" else 0.25
            drivers.append(f"外部战术参考偏防守: {line}")
        elif bullish_hit:
            total_score += 0.35 if confidence == "可直接参考" else 0.2
            drivers.append(f"外部战术参考偏进攻: {line}")

    if total_score >= 1.6:
        direction = "偏强"
        probability = "偏强55% / 震荡30% / 偏弱15%"
    elif total_score >= 0.45:
        direction = "震荡偏强"
        probability = "偏强45% / 震荡35% / 偏弱20%"
    elif total_score <= -1.6:
        direction = "高风险回避"
        probability = "偏弱60% / 震荡25% / 偏强15%"
    elif total_score <= -0.45:
        direction = "偏弱"
        probability = "偏弱50% / 震荡35% / 偏强15%"
    else:
        direction = "震荡"
        probability = "震荡50% / 偏强25% / 偏弱25%"

    if direction in {"偏强", "震荡偏强"}:
        holding_action = "持仓不因隔夜预测主动减仓，明早看开盘承接和量能确认。"
        watchlist_action = "自选股只保留尾盘击球区买入提醒，早盘假突破不追。"
        ic_action = "IC底仓可继续吃贴水，但不在高开时追增敞口。"
        put_action = "认沽保护先观察，除非盘中出现M1-M2塌陷或期权恐慌代理异动。"
    elif direction == "震荡":
        holding_action = "维持原计划，个股只按既定止损和MA支撑处理。"
        watchlist_action = "自选股只在回踩支撑且止损距离可控时提醒。"
        ic_action = "IC维持底仓，重点看基差是否平滑。"
        put_action = "认沽保护不提前加码，等盘中衍生品信号确认。"
    elif direction == "偏弱":
        holding_action = "持仓进入防守检查，破位标的优先减风险，不新增高波动敞口。"
        watchlist_action = "自选股买入提醒降级，除非尾盘强势缩量回踩。"
        ic_action = "IC不新增仓位，盘中盯M1-M2和基差跳变。"
        put_action = "若盘中触发衍生品第一层预警，优先买认沽上保护。"
    else:
        holding_action = "明早优先检查持仓止损、保证金和可减仓名单。"
        watchlist_action = "自选股买入提醒全局锁死，避免逆势接飞刀。"
        ic_action = "IC只保留防守底仓思路，若基差/期限结构恶化则准备降档。"
        put_action = "认沽保护优先级提高，等待盘中低成本窗口执行。"

    return {
        "direction": direction,
        "probability": probability,
        "score": round(total_score, 2),
        "drivers": drivers[:4] or ["市场品种没有形成一致方向，按震荡预案处理"],
        "holding_action": holding_action,
        "watchlist_action": watchlist_action,
        "ic_action": ic_action,
        "put_action": put_action,
    }


def _extract_index_level_hint(market_summary_content: str) -> str:
    """Extract a compact CSI500 key-level hint from the detailed market text."""
    content = market_summary_content or ""
    idx = content.find("中证500指数")
    section = content[idx: idx + 1200] if idx >= 0 else content[:1200]

    def _first_number(patterns: List[str]) -> Optional[str]:
        for pattern in patterns:
            match = re.search(pattern, section, flags=re.IGNORECASE)
            if match:
                return match.group(1)
        return None

    price = _first_number([r"(?:现价|当前价格|最新价)[:： ]+([0-9]+(?:\.[0-9]+)?)"])
    ma5 = _first_number([r"MA5[()（）均线]*[:：= ]+([0-9]+(?:\.[0-9]+)?)"])
    ma20 = _first_number([r"MA20[()（）均线]*[:：= ]+([0-9]+(?:\.[0-9]+)?)"])

    parts = []
    if price:
        parts.append(f"现价 {price}")
    if ma5:
        parts.append(f"MA5({ma5})")
    if ma20:
        parts.append(f"MA20({ma20})")
    if parts:
        return "中证500: " + "，".join(parts) + "；明早以能否站稳MA5/MA20作为第一确认。"
    return "中证500: 晚间只给方向预案；明早开盘后以MA5/MA20、昨日低点和IC基差变化确认。"


def _build_next_day_market_outlook_content(
    *,
    report_date: str,
    target_date: date,
    market_payload: Optional[Dict[str, Any]] = None,
    external_tactical_report: Optional[str] = None,
    generated_at: Optional[datetime] = None,
) -> str:
    """Render the concise next-day market outlook card."""
    market_payload = market_payload or {}
    macro_bias_items = market_payload.get("macro_bias_items", []) or []
    market_summary_content = market_payload.get("content", "") or ""
    decision = _calculate_next_day_market_direction(
        macro_bias_items=macro_bias_items,
        external_tactical_report=external_tactical_report,
    )
    generated_at = generated_at or datetime.now()
    drivers = "\n".join(f"- {item}" for item in decision["drivers"])

    return "\n".join(
        [
            f"# {target_date.strftime('%Y-%m-%d')} 明日大盘预判",
            "",
            f"> 生成时间: {generated_at.strftime('%Y-%m-%d %H:%M')} | 样本日: {report_date}",
            "",
            "## 终极判断",
            "",
            f"- **方向判断**: {decision['direction']}",
            f"- **概率分布**: {decision['probability']}",
            f"- **内部评分**: {decision['score']:+.2f}",
            "",
            "## 核心驱动",
            "",
            drivers,
            "",
            "## 关键观察位",
            "",
            f"- {_extract_index_level_hint(market_summary_content)}",
            "- 盘中若出现IC近次月前端塌陷、认沽成交量PCR突刺、或尾盘放量下破，晚间判断自动降级。",
            "",
            "## 明日执行",
            "",
            f"- **持仓**: {decision['holding_action']}",
            f"- **自选股**: {decision['watchlist_action']}",
            f"- **IC贴水**: {decision['ic_action']}",
            f"- **认沽保护**: {decision['put_action']}",
            "",
            "## 一票否决",
            "",
            "- 隔夜美股、中概股、离岸人民币、美元/美债或重大政策与本判断明显反向，明早开盘前必须重评。",
            "- 若开盘后前30分钟放量反向突破关键位，以盘中真实价格行为覆盖本预测。",
        ]
    )


def _date_text_variants(value: date) -> List[str]:
    return [
        value.isoformat(),
        value.strftime("%Y%m%d"),
        f"{value.year}年{value.month}月{value.day}日",
    ]


def _load_fresh_gemini_tactical_reports(
    config: Config,
    *,
    report_date: str,
    target_date: date,
) -> Optional[str]:
    """Load today's synced Gemini tactical reports from the existing two-doc pipeline."""
    raw_paths = str(getattr(config, "external_tactical_report_path", "") or "").strip()
    if not raw_paths:
        return None

    allowed_dates = {report_date, target_date.isoformat()}
    target_variants = set(_date_text_variants(target_date))
    report_variants = set(_date_text_variants(datetime.fromisoformat(report_date).date()))
    contents: List[str] = []

    for raw_path in [item.strip() for item in raw_paths.split(",") if item.strip()]:
        path = Path(raw_path)
        if not path.is_absolute():
            path = Path.cwd() / path
        try:
            if not path.exists() or not path.is_file():
                continue
            content = path.read_text(encoding="utf-8").strip()
            if not content:
                continue
            mtime_date = datetime.fromtimestamp(path.stat().st_mtime).date().isoformat()
            has_current_date = any(item in content for item in (target_variants | report_variants))
            if mtime_date not in allowed_dates and not has_current_date:
                logger.info("[NightlyOutlook] Gemini 战术报告非当日同步，跳过对比输入: %s", path)
                continue
            contents.append(content)
        except Exception as exc:
            logger.info(
                "[NightlyOutlook] 读取 Gemini 战术报告失败(%s): %s",
                path,
                exc,
            )
    if not contents:
        return None
    return "\n\n---\n\n".join(contents)


def _persist_archive_markdown_report(
    *,
    report_date: str,
    content: str,
    archive_dir: Path,
    filename_suffix: str,
) -> Dict[str, str]:
    if not content.strip():
        return {}
    archive_dir.mkdir(parents=True, exist_ok=True)
    archive_path = archive_dir / f"{report_date}_{filename_suffix}.md"
    archive_path.write_text(content, encoding="utf-8")
    return {"archive_path": str(archive_path)}


def _infer_market_direction_from_text(text: str) -> str:
    normalized = (text or "").replace(" ", "")
    ordered_hits = [
        ("高风险回避", ("高风险回避", "清仓规避", "强防守", "空仓观望", "大幅看空")),
        ("震荡偏强", ("震荡偏强", "小幅看多", "温和看多")),
        ("偏强", ("偏强", "看多", "做多", "上行", "上涨")),
        ("偏弱", ("偏弱", "看空", "下行", "下跌", "防守", "回避")),
        ("震荡", ("震荡", "中性", "观望")),
    ]
    for direction, keywords in ordered_hits:
        if any(keyword in normalized for keyword in keywords):
            return direction
    return "未知"


def _market_direction_side(direction: str) -> str:
    if direction in {"偏强", "震荡偏强"}:
        return "偏多"
    if direction in {"偏弱", "高风险回避"}:
        return "偏空"
    if direction == "震荡":
        return "中性"
    return "未知"


def _build_gemini_market_outlook_comparison(
    *,
    report_date: str,
    target_date: date,
    local_content: str,
    gemini_content: str,
) -> str:
    local_direction = _infer_market_direction_from_text(local_content)
    gemini_direction = _infer_market_direction_from_text(gemini_content)
    local_side = _market_direction_side(local_direction)
    gemini_side = _market_direction_side(gemini_direction)
    if local_direction == gemini_direction:
        agreement = "完全一致"
    elif local_side == gemini_side and local_side != "未知":
        agreement = "方向一致，强弱不同"
    elif "未知" in {local_side, gemini_side}:
        agreement = "无法自动判定，需要人工复核"
    else:
        agreement = "明显分歧"

    if agreement == "明显分歧":
        action = "明早不按单一模型行动，先看开盘30分钟价格行为、IC基差与认沽代理是否共振。"
    elif agreement == "完全一致":
        action = "可按共同方向做盘前预案，但仍以开盘后的关键位确认覆盖预测。"
    else:
        action = "按共同方向设预案，仓位和提醒等级取更保守的一侧。"

    gemini_lines = [
        line.strip()
        for line in gemini_content.splitlines()
        if line.strip() and not line.strip().startswith("#")
    ][:6]
    gemini_excerpt = "\n".join(f"- {line}" for line in gemini_lines) or "- Gemini 原文为空或格式无法提取。"

    return "\n".join(
        [
            f"# {target_date.strftime('%Y-%m-%d')} 明日大盘预判对比",
            "",
            f"> 样本日: {report_date} | 对比对象: 本机规则/数据预判 vs Gemini 外部预判",
            "",
            "## 方向对比",
            "",
            f"- **本机预判**: {local_direction}",
            f"- **Gemini 预判**: {gemini_direction}",
            f"- **一致性**: {agreement}",
            "",
            "## 执行处理",
            "",
            f"- {action}",
            "",
            "## Gemini 摘要",
            "",
            gemini_excerpt,
        ]
    )


def _archive_gemini_market_outlook_comparison(
    *,
    report_date: str,
    target_date: date,
    local_content: str,
    gemini_content: Optional[str],
) -> Dict[str, str]:
    if not gemini_content:
        return {}

    comparison_content = _build_gemini_market_outlook_comparison(
        report_date=report_date,
        target_date=target_date,
        local_content=local_content,
        gemini_content=gemini_content,
    )
    comparison_paths = _persist_archive_markdown_report(
        report_date=report_date,
        content=comparison_content,
        archive_dir=Path(__file__).resolve().parent / "reports" / "nightly_market_outlook_comparison",
        filename_suffix="明日大盘预判对比",
    )
    try:
        from src.services.signal_router import SignalEvent, append_signal_event_archive

        event = SignalEvent(
            source="gemini_market_outlook",
            priority="P4",
            category="external_view",
            action="archive",
            title="Gemini外部观点对比归档",
            content=comparison_content,
            reason="nightly_market_outlook_comparison",
            should_notify=False,
            channels=[],
            dedupe_key=f"gemini_market_outlook:{report_date}:{target_date.isoformat()}",
            created_at=datetime.now().isoformat(timespec="seconds"),
            metadata={
                "report_date": report_date,
                "target_date": target_date.isoformat(),
                "comparison_path": comparison_paths.get("archive_path", ""),
            },
        )
        signal_paths = append_signal_event_archive(
            event,
            archive_path=Path(__file__).resolve().parent / "reports" / "signal_events" / "gemini_external_views.jsonl",
        )
    except Exception as exc:
        logger.warning("[NightlyOutlook] Gemini P4 信号归档失败: %s", exc)
        signal_paths = {}
    return {
        "comparison_path": comparison_paths.get("archive_path", ""),
        "comparison_signal_path": signal_paths.get("archive_path", ""),
    }


def _extract_dated_tactical_section(content: str, report_date: str) -> str:
    """Extract a dated tactical section without letting old historical entries trigger alerts."""
    text = content or ""
    if not text.strip():
        return ""

    pattern = re.compile(
        rf"(?:^|\n)(【{re.escape(report_date)}】[^\n]*(?:\n.*?)*?)(?=\n【\d{{4}}-\d{{2}}-\d{{2}}】|\Z)",
        re.MULTILINE,
    )
    match = pattern.search(text)
    if match:
        return match.group(1).strip()

    has_dated_blocks = re.search(r"【\d{4}-\d{2}-\d{2}】", text) is not None
    if not has_dated_blocks and any(variant in text for variant in _date_text_variants(datetime.fromisoformat(report_date).date())):
        return text.strip()
    return ""


def _black_swan_status_line(section: str) -> str:
    for raw_line in (section or "").splitlines():
        line = raw_line.strip().strip("-").strip()
        if "黑天鹅触发状态" in line:
            return line
    return ""


def _build_black_swan_signal_event_from_report(
    *,
    report_date: str,
    report_content: Optional[str],
) -> Optional[Any]:
    section = _extract_dated_tactical_section(report_content or "", report_date)
    if not section:
        return None

    status_line = _black_swan_status_line(section)
    if not status_line:
        return None
    normalized = status_line.replace(" ", "")
    if "未触发" in normalized or "已触发" not in normalized:
        return None

    summary_lines = [
        line.strip()
        for line in section.splitlines()
        if line.strip()
    ][:8]
    content = "\n".join(
        [
            f"- 样本日: {report_date}",
            f"- 状态: {status_line}",
            "- 路由: P0 黑天鹅强提醒",
            "",
            "## Gemini黑天鹅摘要",
            *[f"- {line}" for line in summary_lines],
        ]
    )

    from src.services.signal_router import SignalEvent

    return SignalEvent(
        source="gemini_black_swan",
        priority="P0",
        category="black_swan",
        action="alert",
        title="黑天鹅监控触发",
        content=content,
        reason=status_line,
        should_notify=True,
        channels=["feishu", "desktop"],
        dedupe_key=f"gemini_black_swan:{report_date}:{status_line}",
        created_at=datetime.now().isoformat(timespec="seconds"),
        metadata={
            "report_date": report_date,
            "status_line": status_line,
        },
    )


def _load_json_state(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _save_json_state(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _dispatch_black_swan_signal_if_needed(
    *,
    report_date: str,
    report_content: Optional[str],
    notifier: Any,
    state_path: Optional[Path] = None,
    archive_path: Optional[Path] = None,
) -> Dict[str, Any]:
    event = _build_black_swan_signal_event_from_report(
        report_date=report_date,
        report_content=report_content,
    )
    if event is None:
        return {"black_swan_signal": "none"}

    from src.services.signal_router import SignalRouter, append_signal_event_archive

    root = Path(__file__).resolve().parent / "reports"
    state_path = state_path or root / "black_swan_signal_state.json"
    archive_path = archive_path or root / "signal_events" / "black_swan_events.jsonl"

    archive_result = append_signal_event_archive(event, archive_path=archive_path)
    state = _load_json_state(state_path)
    today_keys = state.get("sent_keys") if state.get("date") == report_date else []
    sent_keys = set(today_keys if isinstance(today_keys, list) else [])
    if event.dedupe_key in sent_keys:
        return {
            "black_swan_signal": "suppressed_duplicate",
            "black_swan_signal_path": archive_result.get("archive_path", ""),
            "black_swan_dedupe_key": event.dedupe_key,
        }

    dispatch_result = SignalRouter().dispatch(event, notifier)
    if dispatch_result.get("sent"):
        sent_keys.add(event.dedupe_key)
        _save_json_state(
            state_path,
            {
                "date": report_date,
                "sent_keys": sorted(sent_keys),
                "last_sent_at": datetime.now().isoformat(timespec="seconds"),
                "last_dedupe_key": event.dedupe_key,
            },
        )

    return {
        "black_swan_signal": "sent" if dispatch_result.get("sent") else "send_failed",
        "black_swan_signal_path": archive_result.get("archive_path", ""),
        "black_swan_dedupe_key": event.dedupe_key,
    }


def _nightly_market_payload_worker(
    connection: Any,
    jin10_api_key: str,
    jin10_x_token: str,
    ai_enabled: bool,
) -> None:
    """Build market payload in a child process so stuck data sources can be killed."""
    try:
        from src.daily_push_pipeline import DailyPushPipeline

        market_push = DailyPushPipeline(
            notifier=None,
            jin10_api_key=jin10_api_key,
            jin10_x_token=jin10_x_token,
            ai_enabled=ai_enabled,
        )
        connection.send({"payload": market_push.build_market_summary_payload() or {}})
    except Exception as exc:
        connection.send({"error": str(exc)})
    finally:
        try:
            connection.close()
        except Exception:
            pass


def _build_nightly_market_payload_with_timeout(
    *,
    config: Config,
    timeout_seconds: int = 45,
) -> Dict[str, Any]:
    """Return market payload, or an empty dict if data collection times out."""
    timeout_seconds = max(5, int(timeout_seconds or 45))
    try:
        ctx = mp.get_context("fork")
    except ValueError:  # pragma: no cover - non-Unix fallback
        ctx = mp.get_context("spawn")

    parent_conn, child_conn = ctx.Pipe(duplex=False)
    process = ctx.Process(
        target=_nightly_market_payload_worker,
        args=(
            child_conn,
            getattr(config, "jin10_api_key", ""),
            getattr(config, "jin10_x_token", ""),
            getattr(config, "nightly_market_outlook_ai_enabled", True),
        ),
        daemon=True,
    )
    process.start()
    child_conn.close()

    result: Optional[Dict[str, Any]] = None
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        if parent_conn.poll(0.2):
            try:
                result = parent_conn.recv()
            except EOFError:
                result = None
            break
        if not process.is_alive():
            break
        process.join(0.2)

    if result is None and parent_conn.poll():
        try:
            result = parent_conn.recv()
        except EOFError:
            result = None

    timed_out = process.is_alive() and result is None
    if timed_out:
        process.terminate()
    process.join(3)
    if process.is_alive():
        process.terminate()
        process.join(1)
    parent_conn.close()

    if timed_out:
        logger.warning("[NightlyOutlook] 市场数据采集超过 %s 秒，已降级生成预测卡", timeout_seconds)
        return {}

    if result is None:
        logger.warning("[NightlyOutlook] 市场数据采集无返回，已降级生成预测卡")
        return {}

    if result.get("error"):
        logger.warning("[NightlyOutlook] 市场数据采集失败，已降级生成预测卡: %s", result["error"])
        return {}
    return result.get("payload") or {}


def run_nightly_market_outlook(
    config: Config,
    *,
    now: Optional[datetime] = None,
) -> Dict[str, str]:
    """Generate, persist, and push the next-day market outlook."""
    now = now or datetime.now()
    report_date = now.strftime("%Y-%m-%d")
    target_date = _resolve_next_cn_trading_date(now)

    from src.notification import NotificationService

    notification_service = NotificationService()
    market_payload = _build_nightly_market_payload_with_timeout(
        config=config,
        timeout_seconds=getattr(config, "nightly_market_outlook_timeout_seconds", 45),
    )

    external_tactical_report = _load_fresh_gemini_tactical_reports(
        config,
        report_date=report_date,
        target_date=target_date,
    )
    content = _build_next_day_market_outlook_content(
        report_date=report_date,
        target_date=target_date,
        market_payload=market_payload,
        external_tactical_report=external_tactical_report,
        generated_at=now,
    )
    paths = _persist_standalone_markdown_report(
        report_date=report_date,
        content=content,
        desktop_dir=Path.home() / "Desktop" / "每日分析报告" / "明日大盘预判",
        archive_dir=Path(__file__).resolve().parent / "reports" / "nightly_market_outlook_archive",
        filename_suffix="明日大盘预判",
        desktop_keep_days=3,
    )
    comparison_paths = _archive_gemini_market_outlook_comparison(
        report_date=report_date,
        target_date=target_date,
        local_content=content,
        gemini_content=external_tactical_report,
    )
    paths.update(comparison_paths)
    black_swan_signal_paths = _dispatch_black_swan_signal_if_needed(
        report_date=report_date,
        report_content=external_tactical_report,
        notifier=notification_service,
    )
    paths.update({key: str(value) for key, value in black_swan_signal_paths.items() if value})

    if notification_service.is_available():
        if notification_service.send(content, email_send_to_all=True):
            logger.info("[NightlyOutlook] 明日大盘预判已推送")
        else:
            logger.warning("[NightlyOutlook] 明日大盘预判推送失败")
    else:
        logger.info("[NightlyOutlook] 通知服务不可用，仅保存本地报告")

    logger.info(
        "[NightlyOutlook] 已保存明日大盘预判: desktop=%s archive=%s comparison=%s",
        paths.get("desktop_path", ""),
        paths.get("archive_path", ""),
        paths.get("comparison_path", ""),
    )
    return paths


def _persist_daily_push_report(
    *,
    report_date: str,
    content: str,
    desktop_dir: Optional[Path] = None,
    archive_dir: Optional[Path] = None,
    desktop_keep_days: int = 3,
) -> Dict[str, str]:
    """Save the daily summary to desktop and a long-term archive folder."""
    if not content.strip():
        return {}

    desktop_dir = desktop_dir or (Path.home() / "Desktop" / "每日分析报告")
    archive_dir = archive_dir or _default_daily_push_archive_dir()
    desktop_root_dir = desktop_dir.parent
    desktop_dir.mkdir(parents=True, exist_ok=True)
    archive_dir.mkdir(parents=True, exist_ok=True)

    filename = f"{report_date}_盘前总报告.md"
    desktop_path = desktop_root_dir / filename
    archive_path = archive_dir / filename
    history_path = desktop_dir / filename

    target_content = content
    preserve_existing_report = False
    for existing_path in (archive_path, desktop_path):
        if not existing_path.exists():
            continue
        try:
            existing_content = existing_path.read_text(encoding="utf-8")
        except OSError:
            existing_content = ""
        if _should_preserve_existing_daily_push_report(existing_content, content):
            target_content = existing_content
            preserve_existing_report = True
            logger.info("检测到同日有效盘前总报告已存在，跳过空白兜底版覆盖: %s", existing_path)
            break

    desktop_path.write_text(target_content, encoding="utf-8")
    archive_path.write_text(target_content if preserve_existing_report else content, encoding="utf-8")
    if history_path.exists():
        try:
            history_path.unlink()
        except OSError as exc:
            logger.warning("删除桌面历史日报重复项失败(%s): %s", history_path, exc)

    desktop_reports = sorted(
        desktop_root_dir.glob("*_盘前总报告.md"),
        key=lambda item: item.stat().st_mtime,
        reverse=True,
    )
    current_name = filename
    for stale in desktop_reports:
        if stale.name == current_name:
            continue
        try:
            stale.replace(desktop_dir / stale.name)
        except OSError as exc:
            logger.warning("移动旧桌面日报到历史文件夹失败(%s): %s", stale, exc)

    history_reports = sorted(
        desktop_dir.glob("*_盘前总报告.md"),
        key=lambda item: item.stat().st_mtime,
        reverse=True,
    )
    for stale in history_reports[max(desktop_keep_days - 1, 0):]:
        try:
            stale.unlink()
        except OSError as exc:
            logger.warning("删除旧桌面历史日报失败(%s): %s", stale, exc)

    return {
        "desktop_path": str(desktop_path),
        "archive_path": str(archive_path),
    }


def _is_fallback_daily_push_content(content: str) -> bool:
    """Detect an empty fallback daily report that should not replace a richer same-day report."""
    text = content or ""
    if "暂无明确单边结论" not in text:
        return False
    if "## 重点标的" in text:
        return False
    neutral_tokens = [
        "黄金方向暂未形成明显单边优势",
        "白银期货暂未给出明显单边信号",
        "IC主线暂偏震荡",
        "暂不需要明显强化保护",
    ]
    return sum(1 for token in neutral_tokens if token in text) >= 2


def _should_preserve_existing_daily_push_report(existing_content: str, incoming_content: str) -> bool:
    """Keep the richer archived report when a later fallback run has no actionable content."""
    if not existing_content.strip():
        return False
    if not _is_fallback_daily_push_content(incoming_content):
        return False
    return not _is_fallback_daily_push_content(existing_content)


def _default_daily_push_archive_dir() -> Path:
    """Use the durable user report archive outside packaged app bundles."""
    env_reports_dir = os.getenv("DSA_REPORTS_DIR") or os.getenv("DAILY_STOCK_ANALYSIS_REPORTS_DIR")
    if env_reports_dir:
        return Path(env_reports_dir).expanduser() / "daily_push_archive"

    user_reports_dir = Path.home() / "Reports" / "projects" / "daily_stock_analysis"
    if user_reports_dir.exists():
        return user_reports_dir / "daily_push_archive"

    return Path.cwd() / "reports" / "daily_push_archive"


def _persist_standalone_markdown_report(
    *,
    report_date: str,
    content: str,
    desktop_dir: Path,
    archive_dir: Path,
    filename_suffix: str,
    desktop_keep_days: int = 3,
) -> Dict[str, str]:
    """Persist an auxiliary markdown report to desktop and archive storage."""
    if not content.strip():
        return {}

    desktop_dir.mkdir(parents=True, exist_ok=True)
    archive_dir.mkdir(parents=True, exist_ok=True)

    filename = f"{report_date}_{filename_suffix}.md"
    desktop_path = desktop_dir / filename
    archive_path = archive_dir / filename

    desktop_path.write_text(content, encoding="utf-8")
    archive_path.write_text(content, encoding="utf-8")

    desktop_reports = sorted(
        desktop_dir.glob(f"*_{filename_suffix}.md"),
        key=lambda item: item.stat().st_mtime,
        reverse=True,
    )
    for stale in desktop_reports[desktop_keep_days:]:
        try:
            stale.unlink()
        except OSError as exc:
            logger.warning("删除旧桌面独立报告失败(%s): %s", stale, exc)

    return {
        "desktop_path": str(desktop_path),
        "archive_path": str(archive_path),
    }


def _mirror_existing_markdown_to_desktop(
    *,
    report_date: str,
    source_path: Path,
    desktop_dir: Path,
    filename_suffix: str,
    desktop_keep_days: int = 3,
) -> Dict[str, str]:
    """Mirror an existing markdown report to the desktop without touching backend archives."""
    if not source_path.exists():
        return {}

    try:
        content = source_path.read_text(encoding="utf-8")
    except OSError as exc:
        logger.warning("读取待镜像报告失败(%s): %s", source_path, exc)
        return {}

    if not content.strip():
        return {}

    desktop_root_dir = desktop_dir.parent
    desktop_dir.mkdir(parents=True, exist_ok=True)
    filename = f"{report_date}_{filename_suffix}.md"
    desktop_path = desktop_root_dir / filename
    history_path = desktop_dir / filename
    desktop_path.write_text(content, encoding="utf-8")

    if history_path.exists():
        try:
            history_path.unlink()
        except OSError as exc:
            logger.warning("删除桌面历史镜像报告重复项失败(%s): %s", history_path, exc)

    desktop_reports = sorted(
        desktop_root_dir.glob(f"*_{filename_suffix}.md"),
        key=lambda item: item.stat().st_mtime,
        reverse=True,
    )
    current_name = filename
    for stale in desktop_reports:
        if stale.name == current_name:
            continue
        try:
            stale.replace(desktop_dir / stale.name)
        except OSError as exc:
            logger.warning("移动旧桌面镜像报告到历史文件夹失败(%s): %s", stale, exc)

    history_reports = sorted(
        desktop_dir.glob(f"*_{filename_suffix}.md"),
        key=lambda item: item.stat().st_mtime,
        reverse=True,
    )
    for stale in history_reports[max(desktop_keep_days - 1, 0):]:
        try:
            stale.unlink()
        except OSError as exc:
            logger.warning("删除旧桌面历史镜像报告失败(%s): %s", stale, exc)

    return {
        "desktop_path": str(desktop_path),
        "source_path": str(source_path),
    }


def _build_daily_push_index_record(
    *,
    report_date: str,
    content: str,
    archive_path: str,
    macro_bias_items: Optional[List[Dict[str, Any]]] = None,
    results: Optional[List[Any]] = None,
) -> Dict[str, Any]:
    macro_bias_map = _macro_item_map(macro_bias_items)
    top_results = sorted(
        results or [],
        key=lambda item: getattr(item, "sentiment_score", 0),
        reverse=True,
    )[:5]
    return {
        "report_date": report_date,
        "generated_at": datetime.now().isoformat(),
        "archive_path": archive_path,
        "macro_bias": {
            asset_name: {
                "label": item.get("label", "中性"),
                "trend_note": item.get("trend_note", ""),
                "reasons": item.get("reasons", []) or [],
            }
            for asset_name, item in macro_bias_map.items()
        },
        "top_targets": [
            {
                "code": getattr(item, "code", ""),
                "name": getattr(item, "name", ""),
                "score": int(getattr(item, "sentiment_score", 0) or 0),
                "operation_advice": getattr(item, "operation_advice", ""),
                "trend_prediction": getattr(item, "trend_prediction", ""),
            }
            for item in top_results
        ],
        "has_high_risk_playbook": "## 高风险日应对沙盘" in content,
        "content_preview": content[:300],
    }


def _append_daily_push_index_record(
    *,
    report_date: str,
    content: str,
    archive_dir: Path,
    archive_path: str,
    macro_bias_items: Optional[List[Dict[str, Any]]] = None,
    results: Optional[List[Any]] = None,
) -> str:
    archive_dir.mkdir(parents=True, exist_ok=True)
    index_path = archive_dir / "daily_push_index.jsonl"
    record = _build_daily_push_index_record(
        report_date=report_date,
        content=content,
        archive_path=archive_path,
        macro_bias_items=macro_bias_items,
        results=results,
    )
    existing_lines: List[str] = []
    if index_path.exists():
        existing_lines = [
            line for line in index_path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
        existing_lines = [
            line for line in existing_lines
            if json.loads(line).get("report_date") != report_date
        ]
    existing_lines.append(json.dumps(record, ensure_ascii=False))
    index_path.write_text("\n".join(existing_lines) + "\n", encoding="utf-8")
    return str(index_path)


def _coerce_iso_date(value: str) -> Optional[date]:
    try:
        return datetime.strptime(str(value).strip(), "%Y-%m-%d").date()
    except Exception:
        return None


def _compute_forward_return_metrics(
    *,
    stock_code: str,
    report_date: str,
    as_of_date: date,
    fetch_history,
    horizons: Tuple[int, ...] = (1, 3, 5),
) -> Optional[Dict[str, Any]]:
    report_dt = _coerce_iso_date(report_date)
    if report_dt is None or as_of_date <= report_dt:
        return None

    end_date = as_of_date.strftime("%Y-%m-%d")
    start_date = (report_dt - timedelta(days=20)).strftime("%Y-%m-%d")
    try:
        df = fetch_history(stock_code, start_date, end_date)
    except Exception as exc:
        logger.debug("评估收益率抓取失败(%s): %s", stock_code, exc)
        return None
    if df is None or getattr(df, "empty", True):
        return None

    series = df.copy()
    if "date" not in series.columns or "close" not in series.columns:
        return None
    series["date"] = series["date"].astype(str).str[:10]
    series = series.sort_values("date").reset_index(drop=True)
    entry_idx = None
    for idx, row in series.iterrows():
        row_dt = _coerce_iso_date(row["date"])
        if row_dt and row_dt >= report_dt:
            entry_idx = idx
            break
    if entry_idx is None:
        return None
    try:
        entry_price = float(series.iloc[entry_idx]["close"])
    except Exception:
        return None
    if entry_price <= 0:
        return None

    metrics: Dict[str, Any] = {
        "entry_date": str(series.iloc[entry_idx]["date"]),
        "entry_close": round(entry_price, 4),
    }
    for horizon in horizons:
        target_idx = entry_idx + horizon
        key = f"t_plus_{horizon}"
        if target_idx < len(series):
            exit_row = series.iloc[target_idx]
            exit_price = float(exit_row["close"])
            metrics[key] = {
                "date": str(exit_row["date"]),
                "close": round(exit_price, 4),
                "return_pct": round((exit_price / entry_price - 1.0) * 100, 2),
            }
        else:
            metrics[key] = None
    return metrics


def _refresh_daily_push_index_outcomes(
    index_path: Path,
    *,
    as_of_date: Optional[date] = None,
) -> int:
    """Update archived daily push index with forward T+1/T+3/T+5 results."""
    if not index_path.exists():
        return 0

    as_of_date = as_of_date or datetime.now().date()
    lines = [line for line in index_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    if not lines:
        return 0

    from data_provider.base import DataFetcherManager

    manager = DataFetcherManager()

    def _fetch_history(stock_code: str, start_date: str, end_date: str):
        df, _ = manager.get_daily_data(stock_code, start_date=start_date, end_date=end_date, days=30)
        return df

    updated = 0
    rewritten: List[str] = []
    try:
        for line in lines:
            record = json.loads(line)
            targets = record.get("top_targets") or []
            evaluations = record.get("forward_eval") or {}
            changed = False
            for target in targets:
                code = str(target.get("code") or "").strip()
                if not code:
                    continue
                if code in evaluations and evaluations.get(code):
                    continue
                metrics = _compute_forward_return_metrics(
                    stock_code=code,
                    report_date=record.get("report_date", ""),
                    as_of_date=as_of_date,
                    fetch_history=_fetch_history,
                )
                if metrics:
                    evaluations[code] = metrics
                    changed = True
            if changed:
                record["forward_eval"] = evaluations
                updated += 1
            rewritten.append(json.dumps(record, ensure_ascii=False))
    finally:
        try:
            manager.close()
        except Exception:
            pass

    if updated:
        index_path.write_text("\n".join(rewritten) + "\n", encoding="utf-8")
    return updated


def _build_weekly_review_from_index(
    index_path: Path,
    *,
    as_of_date: Optional[date] = None,
    lookback_days: int = 7,
) -> Optional[str]:
    """Summarize recent indexed reports into a weekly hit-rate review."""
    if not index_path.exists():
        return None

    as_of_date = as_of_date or datetime.now().date()
    lines = [line for line in index_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    if not lines:
        return None

    start_date = as_of_date - timedelta(days=max(lookback_days - 1, 0))
    horizon_stats: Dict[str, Dict[str, float]] = {
        "t_plus_1": {"count": 0, "wins": 0, "sum": 0.0},
        "t_plus_3": {"count": 0, "wins": 0, "sum": 0.0},
        "t_plus_5": {"count": 0, "wins": 0, "sum": 0.0},
    }
    records_in_window: List[Dict[str, Any]] = []
    best_case: Optional[Dict[str, Any]] = None
    worst_case: Optional[Dict[str, Any]] = None

    for line in lines:
        record = json.loads(line)
        report_dt = _coerce_iso_date(record.get("report_date", ""))
        if report_dt is None or report_dt < start_date or report_dt > as_of_date:
            continue
        records_in_window.append(record)
        target_map = {
            str(item.get("code") or ""): item
            for item in (record.get("top_targets") or [])
        }
        for code, metrics in (record.get("forward_eval") or {}).items():
            target = target_map.get(code, {})
            for horizon_key, stats in horizon_stats.items():
                payload = metrics.get(horizon_key) if isinstance(metrics, dict) else None
                if not payload:
                    continue
                return_pct = payload.get("return_pct")
                if return_pct is None:
                    continue
                value = float(return_pct)
                stats["count"] += 1
                stats["sum"] += value
                if value > 0:
                    stats["wins"] += 1
                if horizon_key == "t_plus_3":
                    candidate = {
                        "code": code,
                        "name": target.get("name") or code,
                        "report_date": record.get("report_date", ""),
                        "return_pct": round(value, 2),
                    }
                    if best_case is None or value > best_case["return_pct"]:
                        best_case = candidate
                    if worst_case is None or value < worst_case["return_pct"]:
                        worst_case = candidate

    if not records_in_window:
        return None

    lines_out = [
        f"# {as_of_date.strftime('%Y-%m-%d')} 周度效果复盘",
        "",
        "## 样本概览",
        "",
        f"- 统计窗口: {start_date.strftime('%Y-%m-%d')} 至 {as_of_date.strftime('%Y-%m-%d')}",
        f"- 日报样本数: {len(records_in_window)}",
        f"- 已回填标的数: {sum(stats['count'] for stats in horizon_stats.values())}",
        "",
        "## 前瞻表现统计",
        "",
    ]

    for horizon_key, label in (("t_plus_1", "T+1"), ("t_plus_3", "T+3"), ("t_plus_5", "T+5")):
        stats = horizon_stats[horizon_key]
        if stats["count"] == 0:
            lines_out.append(f"- {label}: 暂无足够样本")
            continue
        avg_return = stats["sum"] / stats["count"]
        win_rate = stats["wins"] / stats["count"] * 100
        lines_out.append(
            f"- {label}: 样本 {int(stats['count'])} | 胜率 {win_rate:.1f}% | 平均收益 {avg_return:.2f}%"
        )

    lines_out.extend(["", "## 代表样本", ""])
    if best_case:
        lines_out.append(
            f"- 最优(T+3): {best_case['name']}({best_case['code']}) "
            f"| 报告日 {best_case['report_date']} | 收益 {best_case['return_pct']:.2f}%"
        )
    if worst_case:
        lines_out.append(
            f"- 最弱(T+3): {worst_case['name']}({worst_case['code']}) "
            f"| 报告日 {worst_case['report_date']} | 收益 {worst_case['return_pct']:.2f}%"
        )
    if not best_case and not worst_case:
        lines_out.append("- 暂无可展示的代表样本")

    lines_out.extend(["", "## 观察结论", ""])
    t3_count = horizon_stats["t_plus_3"]["count"]
    if t3_count:
        t3_avg = horizon_stats["t_plus_3"]["sum"] / t3_count
        if t3_avg > 0:
            lines_out.append("- 当前近7天样本在 T+3 维度整体仍有正收益，说明主推逻辑暂未失真。")
        elif t3_avg < 0:
            lines_out.append("- 当前近7天样本在 T+3 维度转弱，后续更应收紧推荐数量和进攻性表达。")
        else:
            lines_out.append("- 当前近7天样本在 T+3 维度接近平衡，建议继续观察后再调整主逻辑。")
    else:
        lines_out.append("- 当前近7天样本尚不足以形成稳定结论。")

    return "\n".join(lines_out)


def _build_weekly_dashboard_from_index(
    index_path: Path,
    *,
    as_of_date: Optional[date] = None,
    lookback_days: int = 7,
) -> Optional[str]:
    """Build a compact weekly scoreboard for fast reading."""
    if not index_path.exists():
        return None

    as_of_date = as_of_date or datetime.now().date()
    lines = [line for line in index_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    if not lines:
        return None

    start_date = as_of_date - timedelta(days=max(lookback_days - 1, 0))
    counts = {"reports": 0, "targets": 0}
    t3_returns: List[float] = []
    risk_days = 0
    bullish_gold = 0
    bearish_csi500 = 0

    for line in lines:
        record = json.loads(line)
        report_dt = _coerce_iso_date(record.get("report_date", ""))
        if report_dt is None or report_dt < start_date or report_dt > as_of_date:
            continue
        counts["reports"] += 1
        counts["targets"] += len(record.get("top_targets") or [])
        if record.get("has_high_risk_playbook"):
            risk_days += 1
        macro_bias = record.get("macro_bias") or {}
        if (macro_bias.get("黄金期货") or {}).get("label") == "利多":
            bullish_gold += 1
        if (macro_bias.get("中证500指数") or {}).get("label") == "利空":
            bearish_csi500 += 1
        for metrics in (record.get("forward_eval") or {}).values():
            payload = metrics.get("t_plus_3") if isinstance(metrics, dict) else None
            if payload and payload.get("return_pct") is not None:
                t3_returns.append(float(payload["return_pct"]))

    if counts["reports"] == 0:
        return None

    t3_win_rate = (
        sum(1 for value in t3_returns if value > 0) / len(t3_returns) * 100
        if t3_returns else None
    )
    t3_avg = sum(t3_returns) / len(t3_returns) if t3_returns else None

    if t3_avg is None:
        suggestion = "样本仍少，继续积累，不急着改主逻辑。"
    elif t3_avg > 1:
        suggestion = "近期主逻辑有效，可保持当前推荐节奏。"
    elif t3_avg > 0:
        suggestion = "近期仍为正收益，但优势不大，控制推荐数量。"
    else:
        suggestion = "近期样本转弱，后续宜收紧进攻表达，优先防守。"

    lines_out = [
        f"# {as_of_date.strftime('%Y-%m-%d')} 周度命中率看板",
        "",
        f"- 统计窗口: {start_date.strftime('%Y-%m-%d')} 至 {as_of_date.strftime('%Y-%m-%d')}",
        f"- 日报数: {counts['reports']}",
        f"- 标的样本数: {counts['targets']}",
        (
            f"- T+3 胜率: {t3_win_rate:.1f}%"
            if t3_win_rate is not None else "- T+3 胜率: 暂无足够样本"
        ),
        (
            f"- T+3 平均收益: {t3_avg:.2f}%"
            if t3_avg is not None else "- T+3 平均收益: 暂无足够样本"
        ),
        f"- 高风险日占比: {risk_days}/{counts['reports']}",
        f"- 黄金利多天数: {bullish_gold}/{counts['reports']}",
        f"- 中证500利空天数: {bearish_csi500}/{counts['reports']}",
        f"- 当前建议: {suggestion}",
    ]
    return "\n".join(lines_out)


def _persist_weekly_review(
    *,
    report_date: str,
    content: str,
    archive_dir: Path,
) -> Optional[Dict[str, str]]:
    if not content.strip():
        return None

    archive_dir.mkdir(parents=True, exist_ok=True)
    weekly_path = archive_dir / f"{report_date}_周度效果复盘.md"
    latest_path = archive_dir / "weekly_review_latest.md"
    weekly_path.write_text(content, encoding="utf-8")
    latest_path.write_text(content, encoding="utf-8")
    return {
        "weekly_path": str(weekly_path),
        "latest_path": str(latest_path),
    }


def _persist_weekly_dashboard(
    *,
    report_date: str,
    content: str,
    archive_dir: Path,
) -> Optional[Dict[str, str]]:
    if not content.strip():
        return None

    archive_dir.mkdir(parents=True, exist_ok=True)
    dashboard_path = archive_dir / f"{report_date}_周度命中率看板.md"
    latest_path = archive_dir / "weekly_dashboard_latest.md"
    dashboard_path.write_text(content, encoding="utf-8")
    latest_path.write_text(content, encoding="utf-8")
    return {
        "dashboard_path": str(dashboard_path),
        "latest_path": str(latest_path),
    }


def _build_monthly_review_from_index(
    index_path: Path,
    *,
    as_of_date: Optional[date] = None,
    lookback_days: int = 30,
) -> Optional[str]:
    weekly_like = _build_weekly_review_from_index(
        index_path,
        as_of_date=as_of_date,
        lookback_days=lookback_days,
    )
    if not weekly_like:
        return None
    return (
        weekly_like
        .replace("周度效果复盘", "月度稳定性复盘")
        .replace("当前近7天样本", "当前近30天样本")
    )


def _build_monthly_dashboard_from_index(
    index_path: Path,
    *,
    as_of_date: Optional[date] = None,
    lookback_days: int = 30,
) -> Optional[str]:
    weekly_like = _build_weekly_dashboard_from_index(
        index_path,
        as_of_date=as_of_date,
        lookback_days=lookback_days,
    )
    if not weekly_like:
        return None
    return weekly_like.replace("周度命中率看板", "月度稳定性看板")


def _classify_target_group(record: Dict[str, Any], target: Dict[str, Any]) -> str:
    code = str(target.get("code") or "").strip()
    name = str(target.get("name") or "").strip()
    macro_bias = record.get("macro_bias") or {}
    name_text = f"{code} {name}".lower()
    if any(keyword in name_text for keyword in ("黄金", "gold", "518880", "159934")):
        return "黄金相关"
    if any(keyword in name_text for keyword in ("白银", "silver")):
        return "白银相关"
    if any(keyword in name_text for keyword in ("500", "ic", "中证500", "510500", "159922")):
        return "中证500相关"
    if "etf" in name_text:
        return "ETF推荐"
    if "黄金期货" in macro_bias or "白银期货" in macro_bias or "中证500指数" in macro_bias:
        return "ETF推荐"
    return "其他标的"


def _build_strategy_group_performance_table(
    index_path: Path,
    *,
    as_of_date: Optional[date] = None,
    lookback_days: int = 30,
) -> Optional[str]:
    if not index_path.exists():
        return None

    as_of_date = as_of_date or datetime.now().date()
    lines = [line for line in index_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    if not lines:
        return None

    start_date = as_of_date - timedelta(days=max(lookback_days - 1, 0))
    stats: Dict[str, Dict[str, float]] = {}

    for line in lines:
        record = json.loads(line)
        report_dt = _coerce_iso_date(record.get("report_date", ""))
        if report_dt is None or report_dt < start_date or report_dt > as_of_date:
            continue
        target_map = {
            str(item.get("code") or ""): item
            for item in (record.get("top_targets") or [])
        }
        for code, metrics in (record.get("forward_eval") or {}).items():
            payload = metrics.get("t_plus_3") if isinstance(metrics, dict) else None
            if not payload or payload.get("return_pct") is None:
                continue
            target = target_map.get(code) or {"code": code}
            group = _classify_target_group(record, target)
            bucket = stats.setdefault(group, {"count": 0, "wins": 0, "sum": 0.0})
            value = float(payload["return_pct"])
            bucket["count"] += 1
            bucket["sum"] += value
            if value > 0:
                bucket["wins"] += 1

    if not stats:
        return None

    ordered_groups = ["黄金相关", "白银相关", "中证500相关", "ETF推荐", "其他标的"]
    lines_out = [
        f"# {as_of_date.strftime('%Y-%m-%d')} 策略分组表现表",
        "",
        f"- 统计窗口: {start_date.strftime('%Y-%m-%d')} 至 {as_of_date.strftime('%Y-%m-%d')}",
        "",
        "| 分组 | 样本数 | T+3 胜率 | T+3 平均收益 |",
        "| --- | ---: | ---: | ---: |",
    ]

    for group in ordered_groups:
        bucket = stats.get(group)
        if not bucket:
            continue
        avg_return = bucket["sum"] / bucket["count"]
        win_rate = bucket["wins"] / bucket["count"] * 100
        lines_out.append(
            f"| {group} | {int(bucket['count'])} | {win_rate:.1f}% | {avg_return:.2f}% |"
        )

    return "\n".join(lines_out)


def _build_golden_dragon_effectiveness_table(
    index_path: Path,
    *,
    as_of_date: Optional[date] = None,
    lookback_days: int = 30,
) -> Optional[str]:
    """Evaluate whether overnight Golden Dragon bias helped next-day CSI500 decisions."""
    if not index_path.exists():
        return None

    as_of_date = as_of_date or datetime.now().date()
    lines = [line for line in index_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    if not lines:
        return None

    start_date = as_of_date - timedelta(days=max(lookback_days - 1, 0))
    stats: Dict[str, Dict[str, float]] = {}

    for line in lines:
        record = json.loads(line)
        report_dt = _coerce_iso_date(record.get("report_date", ""))
        if report_dt is None or report_dt < start_date or report_dt > as_of_date:
            continue

        macro_bias = record.get("macro_bias") or {}
        dragon_bias = (macro_bias.get("纳斯达克中国金龙指数") or {}).get("label")
        if dragon_bias not in {"利多", "利空", "中性"}:
            continue

        target_map = {
            str(item.get("code") or ""): item
            for item in (record.get("top_targets") or [])
        }
        bucket = stats.setdefault(
            dragon_bias,
            {
                "count": 0,
                "t1_sum": 0.0,
                "t1_wins": 0,
                "t3_sum": 0.0,
                "t3_wins": 0,
            },
        )
        for code, metrics in (record.get("forward_eval") or {}).items():
            target = target_map.get(code) or {"code": code}
            if _classify_target_group(record, target) != "中证500相关":
                continue
            if not isinstance(metrics, dict):
                continue
            t1 = metrics.get("t_plus_1") or {}
            t3 = metrics.get("t_plus_3") or {}
            t1_ret = t1.get("return_pct")
            t3_ret = t3.get("return_pct")
            if t1_ret is None and t3_ret is None:
                continue
            bucket["count"] += 1
            if t1_ret is not None:
                value = float(t1_ret)
                bucket["t1_sum"] += value
                if value > 0:
                    bucket["t1_wins"] += 1
            if t3_ret is not None:
                value = float(t3_ret)
                bucket["t3_sum"] += value
                if value > 0:
                    bucket["t3_wins"] += 1

    if not stats:
        return None

    lines_out = [
        f"# {as_of_date.strftime('%Y-%m-%d')} 金龙指数参考效果表",
        "",
        f"- 统计窗口: {start_date.strftime('%Y-%m-%d')} 至 {as_of_date.strftime('%Y-%m-%d')}",
        "- 评估对象: 隔夜纳斯达克中国金龙指数方向，对次日中证500相关推荐样本的辅助效果。",
        "",
        "| 金龙参考 | 样本数 | T+1 胜率 | T+1 平均收益 | T+3 胜率 | T+3 平均收益 |",
        "| --- | ---: | ---: | ---: | ---: | ---: |",
    ]
    for label in ["利多", "利空", "中性"]:
        bucket = stats.get(label)
        if not bucket:
            continue
        count = max(int(bucket["count"]), 1)
        t1_win = bucket["t1_wins"] / count * 100
        t1_avg = bucket["t1_sum"] / count
        t3_win = bucket["t3_wins"] / count * 100
        t3_avg = bucket["t3_sum"] / count
        lines_out.append(
            f"| {label} | {int(bucket['count'])} | {t1_win:.1f}% | {t1_avg:.2f}% | {t3_win:.1f}% | {t3_avg:.2f}% |"
        )

    lines_out.extend(["", "## 观察结论", ""])
    bullish = stats.get("利多")
    bearish = stats.get("利空")
    if bullish and bearish and bullish["count"] > 0 and bearish["count"] > 0:
        bullish_avg = bullish["t1_sum"] / bullish["count"]
        bearish_avg = bearish["t1_sum"] / bearish["count"]
        if bullish_avg > bearish_avg:
            lines_out.append("- 当前样本里，金龙偏强时对中证500次日表现的辅助解释更好，可以继续保留为隔夜参考。")
        elif bullish_avg < bearish_avg:
            lines_out.append("- 当前样本里，金龙偏弱的风险提示更有效，后续更适合作为防守信号使用。")
        else:
            lines_out.append("- 当前金龙指数对中证500样本的区分度还不明显，继续积累样本。")
    else:
        lines_out.append("- 当前样本仍少，先把金龙指数作为辅助观察，不宜赋予过高权重。")

    return "\n".join(lines_out)


def _load_overnight_signal_contexts(
    intraday_archive_dir: Path,
    *,
    cutoff_time: str = "09:40",
) -> Dict[str, Dict[str, Any]]:
    """Load overnight snapshot/event context keyed by report date."""
    if not intraday_archive_dir.exists():
        return {}

    contexts: Dict[str, Dict[str, Any]] = {}
    cutoff_hour, cutoff_minute = [int(part) for part in cutoff_time.split(":", 1)]

    for snapshot_path in intraday_archive_dir.glob("*_market_snapshots.jsonl"):
        report_date = snapshot_path.name.replace("_market_snapshots.jsonl", "")
        latest_snapshot: Optional[Dict[str, Any]] = None
        for line in snapshot_path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            try:
                payload = json.loads(line)
            except Exception:
                continue
            captured_at = payload.get("captured_at")
            try:
                captured_dt = datetime.fromisoformat(str(captured_at))
            except Exception:
                continue
            if (captured_dt.hour, captured_dt.minute) > (cutoff_hour, cutoff_minute):
                continue
            latest_snapshot = payload

        event_path = intraday_archive_dir / f"{report_date}_jin10_events.jsonl"
        event_count = 0
        if event_path.exists():
            event_count = sum(
                1 for line in event_path.read_text(encoding="utf-8").splitlines() if line.strip()
            )

        if latest_snapshot or event_count:
            golden_dragon = (latest_snapshot or {}).get("golden_dragon") or {}
            contexts[report_date] = {
                "snapshot": latest_snapshot,
                "event_count": event_count,
                "golden_dragon_change_pct": golden_dragon.get("change_pct"),
            }
    return contexts


def _classify_overnight_signal_label(context: Dict[str, Any]) -> str:
    change_pct = context.get("golden_dragon_change_pct")
    event_count = int(context.get("event_count") or 0)

    if change_pct is None:
        dragon_label = "金龙缺失"
    elif float(change_pct) >= 1.0:
        dragon_label = "金龙走强"
    elif float(change_pct) <= -1.0:
        dragon_label = "金龙走弱"
    else:
        dragon_label = "金龙震荡"

    if event_count >= 1:
        return f"{dragon_label}+夜间事件"
    return dragon_label


def _build_overnight_signal_effectiveness_table(
    index_path: Path,
    *,
    intraday_archive_dir: Optional[Path] = None,
    as_of_date: Optional[date] = None,
    lookback_days: int = 30,
) -> Optional[str]:
    """Evaluate overnight signal usefulness for next-day CSI500/IC related targets."""
    if not index_path.exists():
        return None

    archive_dir = intraday_archive_dir or (index_path.parent.parent / "intraday_archive")
    contexts = _load_overnight_signal_contexts(archive_dir)
    if not contexts:
        return None

    as_of_date = as_of_date or datetime.now().date()
    lines = [line for line in index_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    if not lines:
        return None

    start_date = as_of_date - timedelta(days=max(lookback_days - 1, 0))
    stats: Dict[str, Dict[str, float]] = {}

    for line in lines:
        record = json.loads(line)
        report_date = str(record.get("report_date") or "").strip()
        report_dt = _coerce_iso_date(report_date)
        if report_dt is None or report_dt < start_date or report_dt > as_of_date:
            continue

        context = contexts.get(report_date)
        if not context:
            continue

        label = _classify_overnight_signal_label(context)
        bucket = stats.setdefault(
            label,
            {
                "count": 0,
                "event_days": 0,
                "dragon_sum": 0.0,
                "dragon_count": 0,
                "t1_sum": 0.0,
                "t1_wins": 0,
                "t3_sum": 0.0,
                "t3_wins": 0,
            },
        )
        event_count = int(context.get("event_count") or 0)
        if event_count:
            bucket["event_days"] += 1
        dragon_change = context.get("golden_dragon_change_pct")
        if dragon_change is not None:
            bucket["dragon_sum"] += float(dragon_change)
            bucket["dragon_count"] += 1

        target_map = {
            str(item.get("code") or ""): item
            for item in (record.get("top_targets") or [])
        }
        for code, metrics in (record.get("forward_eval") or {}).items():
            target = target_map.get(code) or {"code": code}
            if _classify_target_group(record, target) != "中证500相关":
                continue
            if not isinstance(metrics, dict):
                continue
            t1 = metrics.get("t_plus_1") or {}
            t3 = metrics.get("t_plus_3") or {}
            t1_ret = t1.get("return_pct")
            t3_ret = t3.get("return_pct")
            if t1_ret is None and t3_ret is None:
                continue
            bucket["count"] += 1
            if t1_ret is not None:
                value = float(t1_ret)
                bucket["t1_sum"] += value
                if value > 0:
                    bucket["t1_wins"] += 1
            if t3_ret is not None:
                value = float(t3_ret)
                bucket["t3_sum"] += value
                if value > 0:
                    bucket["t3_wins"] += 1

    if not stats:
        return None

    lines_out = [
        f"# {as_of_date.strftime('%Y-%m-%d')} 隔夜信号效果表",
        "",
        f"- 统计窗口: {start_date.strftime('%Y-%m-%d')} 至 {as_of_date.strftime('%Y-%m-%d')}",
        "- 评估对象: 夜间金龙指数方向 + 夜间 Jin10 重大事件，对次日中证500/IC 相关样本的辅助效果。",
        "",
        "| 隔夜信号 | 样本数 | 平均金龙涨跌幅 | 夜间事件天数 | T+1 胜率 | T+1 平均收益 | T+3 胜率 | T+3 平均收益 |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]

    ordered_labels = [
        "金龙走强",
        "金龙走强+夜间事件",
        "金龙震荡",
        "金龙震荡+夜间事件",
        "金龙走弱",
        "金龙走弱+夜间事件",
        "金龙缺失+夜间事件",
    ]
    for label in ordered_labels:
        bucket = stats.get(label)
        if not bucket:
            continue
        count = max(int(bucket["count"]), 1)
        dragon_avg = (
            bucket["dragon_sum"] / bucket["dragon_count"]
            if bucket["dragon_count"] else 0.0
        )
        lines_out.append(
            f"| {label} | {int(bucket['count'])} | {dragon_avg:.2f}% | {int(bucket['event_days'])} | "
            f"{bucket['t1_wins'] / count * 100:.1f}% | {bucket['t1_sum'] / count:.2f}% | "
            f"{bucket['t3_wins'] / count * 100:.1f}% | {bucket['t3_sum'] / count:.2f}% |"
        )

    lines_out.extend(["", "## 观察结论", ""])
    strong = stats.get("金龙走强") or stats.get("金龙走强+夜间事件")
    weak = stats.get("金龙走弱") or stats.get("金龙走弱+夜间事件")
    if strong and weak and strong["count"] > 0 and weak["count"] > 0:
        strong_avg = strong["t1_sum"] / strong["count"]
        weak_avg = weak["t1_sum"] / weak["count"]
        if strong_avg > weak_avg:
            lines_out.append("- 当前样本里，隔夜偏强信号对次日中证500/IC 更有正向辅助价值，可继续保留。")
        elif strong_avg < weak_avg:
            lines_out.append("- 当前样本里，隔夜偏弱或带事件的防守提示更有效，后续更适合作为风险收紧信号。")
        else:
            lines_out.append("- 当前隔夜信号对次日中证500/IC 的区分度还不明显，继续积累样本。")
    else:
        lines_out.append("- 当前样本仍少，先把隔夜信号作为辅助观察，不宜赋予过高权重。")

    return "\n".join(lines_out)


def _classify_recommendation_scenario(record: Dict[str, Any], target: Dict[str, Any]) -> List[str]:
    scenarios: List[str] = []
    advice = str(target.get("operation_advice") or "").strip()
    if any(keyword in advice for keyword in ("买", "加仓", "低吸", "关注")):
        scenarios.append("买入类")
    elif any(keyword in advice for keyword in ("观望", "等待", "跟踪")):
        scenarios.append("观望类")
    else:
        scenarios.append("其他建议")

    if record.get("has_high_risk_playbook"):
        scenarios.append("高风险日")
    else:
        scenarios.append("普通日")
    return scenarios


def _build_recommendation_scenario_performance_table(
    index_path: Path,
    *,
    as_of_date: Optional[date] = None,
    lookback_days: int = 30,
) -> Optional[str]:
    if not index_path.exists():
        return None

    as_of_date = as_of_date or datetime.now().date()
    lines = [line for line in index_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    if not lines:
        return None

    start_date = as_of_date - timedelta(days=max(lookback_days - 1, 0))
    stats: Dict[str, Dict[str, float]] = {}

    for line in lines:
        record = json.loads(line)
        report_dt = _coerce_iso_date(record.get("report_date", ""))
        if report_dt is None or report_dt < start_date or report_dt > as_of_date:
            continue
        target_map = {
            str(item.get("code") or ""): item
            for item in (record.get("top_targets") or [])
        }
        for code, metrics in (record.get("forward_eval") or {}).items():
            payload = metrics.get("t_plus_3") if isinstance(metrics, dict) else None
            if not payload or payload.get("return_pct") is None:
                continue
            target = target_map.get(code) or {"code": code}
            scenarios = _classify_recommendation_scenario(record, target)
            value = float(payload["return_pct"])
            for scenario in scenarios:
                bucket = stats.setdefault(scenario, {"count": 0, "wins": 0, "sum": 0.0})
                bucket["count"] += 1
                bucket["sum"] += value
                if value > 0:
                    bucket["wins"] += 1

    if not stats:
        return None

    ordered = ["买入类", "观望类", "高风险日", "普通日", "其他建议"]
    lines_out = [
        f"# {as_of_date.strftime('%Y-%m-%d')} 推荐场景表现表",
        "",
        f"- 统计窗口: {start_date.strftime('%Y-%m-%d')} 至 {as_of_date.strftime('%Y-%m-%d')}",
        "",
        "| 场景 | 样本数 | T+3 胜率 | T+3 平均收益 |",
        "| --- | ---: | ---: | ---: |",
    ]
    for scenario in ordered:
        bucket = stats.get(scenario)
        if not bucket:
            continue
        avg_return = bucket["sum"] / bucket["count"]
        win_rate = bucket["wins"] / bucket["count"] * 100
        lines_out.append(
            f"| {scenario} | {int(bucket['count'])} | {win_rate:.1f}% | {avg_return:.2f}% |"
        )
    return "\n".join(lines_out)


def _collect_t3_stats_by_group(
    index_path: Path,
    *,
    as_of_date: Optional[date] = None,
    lookback_days: int = 30,
    classifier=None,
) -> Dict[str, Dict[str, float]]:
    if not index_path.exists():
        return {}

    as_of_date = as_of_date or datetime.now().date()
    lines = [line for line in index_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    if not lines:
        return {}

    start_date = as_of_date - timedelta(days=max(lookback_days - 1, 0))
    stats: Dict[str, Dict[str, float]] = {}
    for line in lines:
        record = json.loads(line)
        report_dt = _coerce_iso_date(record.get("report_date", ""))
        if report_dt is None or report_dt < start_date or report_dt > as_of_date:
            continue
        target_map = {
            str(item.get("code") or ""): item
            for item in (record.get("top_targets") or [])
        }
        for code, metrics in (record.get("forward_eval") or {}).items():
            payload = metrics.get("t_plus_3") if isinstance(metrics, dict) else None
            if not payload or payload.get("return_pct") is None:
                continue
            target = target_map.get(code) or {"code": code}
            groups = classifier(record, target) if classifier else []
            if isinstance(groups, str):
                groups = [groups]
            value = float(payload["return_pct"])
            for group in groups:
                bucket = stats.setdefault(group, {"count": 0, "wins": 0, "sum": 0.0})
                bucket["count"] += 1
                bucket["sum"] += value
                if value > 0:
                    bucket["wins"] += 1
    return stats


def _build_recommendation_adjustment_notes(
    index_path: Path,
    *,
    as_of_date: Optional[date] = None,
    lookback_days: int = 30,
    min_samples: int = 3,
) -> Optional[str]:
    scenario_stats = _collect_t3_stats_by_group(
        index_path,
        as_of_date=as_of_date,
        lookback_days=lookback_days,
        classifier=_classify_recommendation_scenario,
    )
    strategy_stats = _collect_t3_stats_by_group(
        index_path,
        as_of_date=as_of_date,
        lookback_days=lookback_days,
        classifier=lambda record, target: [_classify_target_group(record, target)],
    )
    if not scenario_stats and not strategy_stats:
        return None

    as_of_date = as_of_date or datetime.now().date()
    start_date = as_of_date - timedelta(days=max(lookback_days - 1, 0))
    lines_out = [
        f"# {as_of_date.strftime('%Y-%m-%d')} 推荐调整建议",
        "",
        f"- 统计窗口: {start_date.strftime('%Y-%m-%d')} 至 {as_of_date.strftime('%Y-%m-%d')}",
        "",
        "## 建议保留",
        "",
    ]

    kept: List[str] = []
    shrunk: List[str] = []

    for label, stats in {**scenario_stats, **strategy_stats}.items():
        if stats["count"] < min_samples:
            continue
        avg_return = stats["sum"] / stats["count"]
        win_rate = stats["wins"] / stats["count"] * 100
        sentence = f"{label}: 样本 {int(stats['count'])} | 胜率 {win_rate:.1f}% | 平均收益 {avg_return:.2f}%"
        if avg_return > 0 and win_rate >= 50:
            kept.append(sentence)
        elif avg_return < 0:
            shrunk.append(sentence)

    if kept:
        for item in kept:
            lines_out.append(f"- {item}")
    else:
        lines_out.append("- 当前暂无达到保留阈值的稳定场景，继续积累样本。")

    lines_out.extend(["", "## 建议收缩", ""])
    if shrunk:
        for item in shrunk:
            lines_out.append(f"- {item}")
    else:
        lines_out.append("- 当前暂无明确需要收缩的场景。")

    lines_out.extend(["", "## 当前结论", ""])
    if kept or shrunk:
        lines_out.append("- 后续优先保留正收益且样本达到阈值的场景，对持续负收益场景减少表达强度。")
    else:
        lines_out.append("- 当前样本仍不足以自动给出强调整结论，继续观察。")
    return "\n".join(lines_out)


def _build_first_review_readiness(
    index_path: Path,
    *,
    as_of_date: Optional[date] = None,
    lookback_days: int = 30,
    min_total_samples: int = 12,
    min_per_bucket_samples: int = 3,
) -> Optional[str]:
    scenario_stats = _collect_t3_stats_by_group(
        index_path,
        as_of_date=as_of_date,
        lookback_days=lookback_days,
        classifier=_classify_recommendation_scenario,
    )
    strategy_stats = _collect_t3_stats_by_group(
        index_path,
        as_of_date=as_of_date,
        lookback_days=lookback_days,
        classifier=lambda record, target: [_classify_target_group(record, target)],
    )
    if not scenario_stats and not strategy_stats:
        return None

    as_of_date = as_of_date or datetime.now().date()
    start_date = as_of_date - timedelta(days=max(lookback_days - 1, 0))
    total_samples = sum(int(item["count"]) for item in scenario_stats.values())
    ready_scenarios = [name for name, item in scenario_stats.items() if item["count"] >= min_per_bucket_samples]
    ready_groups = [name for name, item in strategy_stats.items() if item["count"] >= min_per_bucket_samples]
    ready = total_samples >= min_total_samples and bool(ready_scenarios) and bool(ready_groups)

    lines_out = [
        f"# {as_of_date.strftime('%Y-%m-%d')} 真复盘就绪判断",
        "",
        f"- 统计窗口: {start_date.strftime('%Y-%m-%d')} 至 {as_of_date.strftime('%Y-%m-%d')}",
        f"- 当前场景样本总数: {total_samples}",
        f"- 场景阈值: 单项至少 {min_per_bucket_samples} 条，总样本至少 {min_total_samples} 条",
        f"- 已达场景: {', '.join(sorted(ready_scenarios)) if ready_scenarios else '暂无'}",
        f"- 已达分组: {', '.join(sorted(ready_groups)) if ready_groups else '暂无'}",
        "",
        "## 当前结论",
        "",
    ]
    if ready:
        lines_out.append("- 已达到第一次真复盘条件，可以正式做“保留什么、收缩什么”的样本复盘。")
    else:
        lines_out.append("- 还未达到第一次真复盘条件，先继续积累样本，避免过早下结论。")
    return "\n".join(lines_out)


def _persist_monthly_review(
    *,
    report_date: str,
    content: str,
    archive_dir: Path,
) -> Optional[Dict[str, str]]:
    if not content.strip():
        return None

    archive_dir.mkdir(parents=True, exist_ok=True)
    review_path = archive_dir / f"{report_date}_月度稳定性复盘.md"
    latest_path = archive_dir / "monthly_review_latest.md"
    review_path.write_text(content, encoding="utf-8")
    latest_path.write_text(content, encoding="utf-8")
    return {
        "review_path": str(review_path),
        "latest_path": str(latest_path),
    }


def _persist_monthly_dashboard(
    *,
    report_date: str,
    content: str,
    archive_dir: Path,
) -> Optional[Dict[str, str]]:
    if not content.strip():
        return None

    archive_dir.mkdir(parents=True, exist_ok=True)
    dashboard_path = archive_dir / f"{report_date}_月度稳定性看板.md"
    latest_path = archive_dir / "monthly_dashboard_latest.md"
    dashboard_path.write_text(content, encoding="utf-8")
    latest_path.write_text(content, encoding="utf-8")
    return {
        "dashboard_path": str(dashboard_path),
        "latest_path": str(latest_path),
    }


def _persist_strategy_group_performance(
    *,
    report_date: str,
    content: str,
    archive_dir: Path,
) -> Optional[Dict[str, str]]:
    if not content.strip():
        return None

    archive_dir.mkdir(parents=True, exist_ok=True)
    table_path = archive_dir / f"{report_date}_策略分组表现表.md"
    latest_path = archive_dir / "strategy_group_performance_latest.md"
    table_path.write_text(content, encoding="utf-8")
    latest_path.write_text(content, encoding="utf-8")
    return {
        "table_path": str(table_path),
        "latest_path": str(latest_path),
    }


def _persist_golden_dragon_effectiveness(
    *,
    report_date: str,
    content: str,
    archive_dir: Path,
) -> Optional[Dict[str, str]]:
    if not content.strip():
        return None

    archive_dir.mkdir(parents=True, exist_ok=True)
    table_path = archive_dir / f"{report_date}_金龙指数参考效果表.md"
    latest_path = archive_dir / "golden_dragon_effectiveness_latest.md"
    table_path.write_text(content, encoding="utf-8")
    latest_path.write_text(content, encoding="utf-8")
    return {
        "table_path": str(table_path),
        "latest_path": str(latest_path),
    }


def _persist_overnight_signal_effectiveness(
    *,
    report_date: str,
    content: str,
    archive_dir: Path,
) -> Optional[Dict[str, str]]:
    if not content.strip():
        return None

    archive_dir.mkdir(parents=True, exist_ok=True)
    table_path = archive_dir / f"{report_date}_隔夜信号效果表.md"
    latest_path = archive_dir / "overnight_signal_effectiveness_latest.md"
    table_path.write_text(content, encoding="utf-8")
    latest_path.write_text(content, encoding="utf-8")
    return {
        "table_path": str(table_path),
        "latest_path": str(latest_path),
    }


def _persist_recommendation_scenario_performance(
    *,
    report_date: str,
    content: str,
    archive_dir: Path,
) -> Optional[Dict[str, str]]:
    if not content.strip():
        return None

    archive_dir.mkdir(parents=True, exist_ok=True)
    table_path = archive_dir / f"{report_date}_推荐场景表现表.md"
    latest_path = archive_dir / "recommendation_scenario_performance_latest.md"
    table_path.write_text(content, encoding="utf-8")
    latest_path.write_text(content, encoding="utf-8")
    return {
        "table_path": str(table_path),
        "latest_path": str(latest_path),
    }


def _persist_recommendation_adjustment_notes(
    *,
    report_date: str,
    content: str,
    archive_dir: Path,
) -> Optional[Dict[str, str]]:
    if not content.strip():
        return None

    archive_dir.mkdir(parents=True, exist_ok=True)
    notes_path = archive_dir / f"{report_date}_推荐调整建议.md"
    latest_path = archive_dir / "recommendation_adjustment_latest.md"
    notes_path.write_text(content, encoding="utf-8")
    latest_path.write_text(content, encoding="utf-8")
    return {
        "notes_path": str(notes_path),
        "latest_path": str(latest_path),
    }


def _persist_first_review_readiness(
    *,
    report_date: str,
    content: str,
    archive_dir: Path,
) -> Optional[Dict[str, str]]:
    if not content.strip():
        return None

    archive_dir.mkdir(parents=True, exist_ok=True)
    readiness_path = archive_dir / f"{report_date}_真复盘就绪判断.md"
    latest_path = archive_dir / "first_review_readiness_latest.md"
    readiness_path.write_text(content, encoding="utf-8")
    latest_path.write_text(content, encoding="utf-8")
    return {
        "readiness_path": str(readiness_path),
        "latest_path": str(latest_path),
    }


def _build_brief_monitor_alert(
    *,
    trigger_event: str,
    strategy: str,
    bias: str,
    immediate_attention: bool,
    action: Optional[str] = None,
) -> str:
    if not action:
        strategy_text = strategy.lower()
        if "ic" in strategy_text or "认沽" in strategy_text:
            action = "先看IC贴水与保护仓位，必要时优先检查认沽对冲。"
        elif "黄金" in strategy_text or "白银" in strategy_text:
            action = "先盯黄金白银主线强弱，再决定是否联动商品ETF。"
        else:
            action = "先聚焦受影响持仓，确认是否需要立即动作。"
    return "\n".join(
        [
            f"触发事件: {trigger_event}",
            f"影响策略: {strategy}",
            f"当前倾向: {bias}",
            f"建议动作: {action}",
            f"是否立即关注: {'是' if immediate_attention else '否'}",
        ]
    )


def _persist_runtime_placeholder_reports(
    *,
    run_started_at: Optional[datetime] = None,
    stock_codes: Optional[List[str]] = None,
    merge_notification: bool = False,
    reports_dir: Optional[Path] = None,
    dashboard_filename: Optional[str] = None,
    market_review_filename: Optional[str] = None,
) -> Dict[str, str]:
    """Write in-progress placeholders so stale reports do not masquerade as fresh runs."""
    run_started_at = run_started_at or datetime.now()
    reports_dir = reports_dir or (Path.cwd() / "reports")
    reports_dir.mkdir(parents=True, exist_ok=True)

    stock_codes = stock_codes or []
    date_label = run_started_at.strftime("%Y-%m-%d")
    filename_date = run_started_at.strftime("%Y%m%d")
    started_label = run_started_at.strftime("%Y-%m-%d %H:%M:%S")

    dashboard_content = "\n".join(
        [
            f"# 🎯 {date_label} 决策仪表盘",
            "",
            "> 当前这轮完整分析仍在执行中，以下为运行状态占位内容。",
            "",
            "## ⏳ 当前状态",
            "",
            f"- 启动时间: {started_label}",
            f"- 计划分析标的数: {len(stock_codes)}",
            f"- 合并推送模式: {'是' if merge_notification else '否'}",
            "- 执行阶段: 正在依次生成市场段、个股分析、归档摘要和复盘文件。",
            "",
            "*说明: 本文件会在本轮分析完成后被正式结果覆盖。*",
            "",
        ]
    )

    market_review_content = "\n".join(
        [
            "# 🎯 大盘复盘",
            "",
            f"## {date_label} 大盘复盘",
            "",
            "> 当前这轮大盘复盘仍在生成中，以下为运行状态占位内容。",
            "",
            "### 当前状态",
            "",
            f"- 启动时间: {started_label}",
            "- 执行阶段: 等待个股分析与大盘复盘链路完成后写入正式内容。",
            "",
            "*说明: 本文件会在本轮分析完成后被正式结果覆盖。*",
            "",
        ]
    )

    dashboard_path = reports_dir / (dashboard_filename or f"report_{filename_date}.md")
    market_review_path = reports_dir / (market_review_filename or f"market_review_{filename_date}.md")
    dashboard_path.write_text(dashboard_content, encoding="utf-8")
    market_review_path.write_text(market_review_content, encoding="utf-8")
    logger.info(
        "已写入本轮占位报告: dashboard=%s market_review=%s",
        dashboard_path,
        market_review_path,
    )
    return {
        "dashboard_path": str(dashboard_path),
        "market_review_path": str(market_review_path),
    }


def run_full_analysis(
    config: Config,
    args: argparse.Namespace,
    stock_codes: Optional[List[str]] = None
):
    """
    执行完整的分析流程（个股 + 大盘复盘）

    这是定时任务调用的主函数
    """
    # Import pipeline modules outside the broad try/except so that import-time
    # failures propagate to the caller instead of being silently swallowed.
    from src.core.market_review import run_market_review
    from src.core.pipeline import StockAnalysisPipeline

    try:
        send_runtime_notifications = _should_send_runtime_notifications(config, args)
        manual_report_filenames = _resolve_manual_report_filenames(args, stock_codes)

        # Issue #529: Hot-reload STOCK_LIST from .env on each scheduled run
        if stock_codes is None:
            config.refresh_stock_list()

        # Issue #373: Trading day filter (per-stock, per-market)
        effective_codes = stock_codes if stock_codes is not None else _get_analysis_stock_codes(config)
        filtered_codes, effective_region, should_skip = _compute_trading_day_filter(
            config, args, effective_codes
        )
        if should_skip:
            logger.info(
                "今日所有相关市场均为非交易日，跳过执行。可使用 --force-run 强制执行。"
            )
            return
        if set(filtered_codes) != set(effective_codes):
            skipped = set(effective_codes) - set(filtered_codes)
            logger.info("今日休市股票已跳过: %s", skipped)
        stock_codes = filtered_codes

        # 命令行参数 --single-notify 覆盖配置（#55）
        if getattr(args, 'single_notify', False):
            config.single_stock_notify = True

        # 统一总推送：市场 -> 个股 -> 提前建仓推荐
        merge_notification = _should_merge_daily_push(config, args)

        if not getattr(args, "dry_run", False):
            _persist_runtime_placeholder_reports(
                run_started_at=datetime.now(),
                stock_codes=stock_codes,
                merge_notification=merge_notification,
                dashboard_filename=(manual_report_filenames or {}).get("dashboard_filename"),
                market_review_filename=(manual_report_filenames or {}).get("market_review_filename"),
            )

        # 创建调度器
        save_context_snapshot = None
        if getattr(args, 'no_context_snapshot', False):
            save_context_snapshot = False
        query_id = uuid.uuid4().hex
        pipeline = StockAnalysisPipeline(
            config=config,
            max_workers=args.workers,
            query_id=query_id,
            query_source="cli",
            save_context_snapshot=save_context_snapshot,
            local_report_filename=(manual_report_filenames or {}).get("dashboard_filename"),
        )

        # 每日市场品种详细分析（黄金+白银+中证500）
        market_summary_content = ""
        market_macro_bias_items: List[Dict[str, Any]] = []
        if getattr(config, "market_daily_push_enabled", True):
            try:
                from src.daily_push_pipeline import DailyPushPipeline
                market_push = DailyPushPipeline(
                    notifier=pipeline.notifier,
                    jin10_api_key=getattr(config, 'jin10_api_key', ''),
                    jin10_x_token=getattr(config, 'jin10_x_token', ''),
                    ai_enabled=getattr(config, "market_daily_push_ai_enabled", True),
                )
                if merge_notification or not send_runtime_notifications:
                    market_summary_payload = market_push.build_market_summary_payload() or {}
                    market_summary_content = market_summary_payload.get("content", "") or ""
                    market_macro_bias_items = market_summary_payload.get("macro_bias_items", []) or []
                elif send_runtime_notifications:
                    market_push.push_market_summary()
                else:
                    logger.info("静默运行模式：跳过市场品种推送，仅保留后续本地报告产物")
            except Exception as e:
                logger.warning(f"市场品种分析推送失败: {e}")

        # 1. 运行个股分析
        results = pipeline.run(
            stock_codes=stock_codes,
            dry_run=args.dry_run,
            send_notification=send_runtime_notifications,
            merge_notification=merge_notification
        )

        # Issue #128: 分析间隔 - 在个股分析和大盘分析之间添加延迟
        analysis_delay = getattr(config, 'analysis_delay', 0)
        if (
            analysis_delay > 0
            and config.market_review_enabled
            and not args.no_market_review
            and effective_region != ''
        ):
            logger.info(f"等待 {analysis_delay} 秒后执行大盘复盘（避免API限流）...")
            time.sleep(analysis_delay)

        # 2. 运行大盘复盘（如果启用且不是仅个股模式）
        market_report = ""
        if (
            config.market_review_enabled
            and not args.no_market_review
            and effective_region != ''
        ):
            review_result = run_market_review(
                notifier=pipeline.notifier,
                analyzer=pipeline.analyzer,
                search_service=pipeline.search_service,
                send_notification=send_runtime_notifications,
                merge_notification=merge_notification,
                override_region=effective_region,
                report_filename=(manual_report_filenames or {}).get("market_review_filename"),
            )
            # 如果有结果，赋值给 market_report 用于后续飞书文档生成
            if review_result:
                market_report = review_result

        # 合并推送：市场总览 + 个股分析 + 提前建仓推荐
        if market_summary_content or results or market_report:
            report_date = datetime.now().strftime('%Y-%m-%d')
            external_tactical_report = _load_external_tactical_report(config)
            summary_content = _build_daily_push_summary(
                report_date=report_date,
                results=results or [],
                macro_bias_items=market_macro_bias_items,
                external_tactical_report=external_tactical_report,
            )
            saved_paths = None
            desktop_dashboard_paths = None
            desktop_market_review_paths = None
            if manual_report_filenames:
                logger.info(
                    "检测到手动定向测试，本轮总摘要不覆盖主归档，跳过标准盘前总报告落盘: %s",
                    manual_report_filenames.get("daily_push_filename"),
                )
            else:
                saved_paths = _persist_daily_push_report(
                    report_date=report_date,
                    content=summary_content,
                )
            if saved_paths:
                archive_dir = Path(saved_paths["archive_path"]).parent
                desktop_dir = Path(saved_paths["desktop_path"]).parent / "每日分析报告"
                reports_dir = Path.cwd() / "reports"
                filename_date = datetime.now().strftime('%Y%m%d')
                dashboard_source = reports_dir / (
                    (manual_report_filenames or {}).get("dashboard_filename")
                    or f"report_{filename_date}.md"
                )
                market_review_source = reports_dir / (
                    (manual_report_filenames or {}).get("market_review_filename")
                    or f"market_review_{filename_date}.md"
                )
                desktop_dashboard_paths = _mirror_existing_markdown_to_desktop(
                    report_date=report_date,
                    source_path=dashboard_source,
                    desktop_dir=desktop_dir,
                    filename_suffix="详细版决策仪表盘",
                    desktop_keep_days=3,
                )
                desktop_market_review_paths = _mirror_existing_markdown_to_desktop(
                    report_date=report_date,
                    source_path=market_review_source,
                    desktop_dir=desktop_dir,
                    filename_suffix="大盘复盘报告",
                    desktop_keep_days=3,
                )
                archive_index_path = _append_daily_push_index_record(
                    report_date=report_date,
                    content=summary_content,
                    archive_dir=archive_dir,
                    archive_path=saved_paths["archive_path"],
                    macro_bias_items=market_macro_bias_items,
                    results=results or [],
                )
                refreshed_count = _refresh_daily_push_index_outcomes(Path(archive_index_path))
                weekly_review = _build_weekly_review_from_index(Path(archive_index_path))
                weekly_paths = None
                weekly_dashboard_paths = None
                monthly_review_paths = None
                monthly_dashboard_paths = None
                strategy_group_paths = None
                golden_dragon_effectiveness_paths = None
                overnight_signal_effectiveness_paths = None
                recommendation_scenario_paths = None
                recommendation_adjustment_paths = None
                first_review_readiness_paths = None
                metaphysical_daily_paths = None
                if weekly_review:
                    weekly_paths = _persist_weekly_review(
                        report_date=report_date,
                        content=weekly_review,
                        archive_dir=archive_dir,
                    )
                weekly_dashboard = _build_weekly_dashboard_from_index(Path(archive_index_path))
                if weekly_dashboard:
                    weekly_dashboard_paths = _persist_weekly_dashboard(
                        report_date=report_date,
                        content=weekly_dashboard,
                        archive_dir=archive_dir,
                    )
                monthly_review = _build_monthly_review_from_index(Path(archive_index_path))
                if monthly_review:
                    monthly_review_paths = _persist_monthly_review(
                        report_date=report_date,
                        content=monthly_review,
                        archive_dir=archive_dir,
                    )
                monthly_dashboard = _build_monthly_dashboard_from_index(Path(archive_index_path))
                if monthly_dashboard:
                    monthly_dashboard_paths = _persist_monthly_dashboard(
                        report_date=report_date,
                        content=monthly_dashboard,
                        archive_dir=archive_dir,
                    )
                strategy_group_table = _build_strategy_group_performance_table(
                    Path(archive_index_path)
                )
                if strategy_group_table:
                    strategy_group_paths = _persist_strategy_group_performance(
                        report_date=report_date,
                        content=strategy_group_table,
                        archive_dir=archive_dir,
                    )
                golden_dragon_effectiveness = _build_golden_dragon_effectiveness_table(
                    Path(archive_index_path)
                )
                if golden_dragon_effectiveness:
                    golden_dragon_effectiveness_paths = _persist_golden_dragon_effectiveness(
                        report_date=report_date,
                        content=golden_dragon_effectiveness,
                        archive_dir=archive_dir,
                    )
                overnight_signal_effectiveness = _build_overnight_signal_effectiveness_table(
                    Path(archive_index_path)
                )
                if overnight_signal_effectiveness:
                    overnight_signal_effectiveness_paths = _persist_overnight_signal_effectiveness(
                        report_date=report_date,
                        content=overnight_signal_effectiveness,
                        archive_dir=archive_dir,
                    )
                recommendation_scenario_table = _build_recommendation_scenario_performance_table(
                    Path(archive_index_path)
                )
                if recommendation_scenario_table:
                    recommendation_scenario_paths = _persist_recommendation_scenario_performance(
                        report_date=report_date,
                        content=recommendation_scenario_table,
                        archive_dir=archive_dir,
                    )
                recommendation_adjustment = _build_recommendation_adjustment_notes(
                    Path(archive_index_path)
                )
                if recommendation_adjustment:
                    recommendation_adjustment_paths = _persist_recommendation_adjustment_notes(
                        report_date=report_date,
                        content=recommendation_adjustment,
                        archive_dir=archive_dir,
                    )
                first_review_readiness = _build_first_review_readiness(Path(archive_index_path))
                if first_review_readiness:
                    first_review_readiness_paths = _persist_first_review_readiness(
                        report_date=report_date,
                        content=first_review_readiness,
                        archive_dir=archive_dir,
                    )
                metaphysical_daily_report = _build_metaphysical_daily_report_content(
                    external_tactical_report=external_tactical_report,
                )
                if metaphysical_daily_report:
                    metaphysical_daily_paths = _persist_standalone_markdown_report(
                        report_date=report_date,
                        content=metaphysical_daily_report,
                        desktop_dir=Path.home() / "Desktop" / "每日分析报告" / "玄学治理日报",
                        archive_dir=Path.cwd() / "reports" / "metaphysical_daily_archive",
                        filename_suffix="玄学治理日报",
                        desktop_keep_days=3,
                    )
                logger.info(
                    "已保存每日摘要报告: desktop=%s archive=%s index=%s refreshed=%s weekly=%s dashboard=%s monthly_review=%s monthly_dashboard=%s grouped=%s golden_dragon=%s overnight=%s scenario=%s adjustment=%s readiness=%s metaphysical_desktop=%s metaphysical_archive=%s",
                    saved_paths.get("desktop_path", ""),
                    saved_paths.get("archive_path", ""),
                    archive_index_path,
                    refreshed_count,
                    (weekly_paths or {}).get("weekly_path", ""),
                    (weekly_dashboard_paths or {}).get("dashboard_path", ""),
                    (monthly_review_paths or {}).get("review_path", ""),
                    (monthly_dashboard_paths or {}).get("dashboard_path", ""),
                    (strategy_group_paths or {}).get("table_path", ""),
                    (golden_dragon_effectiveness_paths or {}).get("table_path", ""),
                    (overnight_signal_effectiveness_paths or {}).get("table_path", ""),
                    (recommendation_scenario_paths or {}).get("table_path", ""),
                    (recommendation_adjustment_paths or {}).get("notes_path", ""),
                    (first_review_readiness_paths or {}).get("readiness_path", ""),
                    (metaphysical_daily_paths or {}).get("desktop_path", ""),
                    (metaphysical_daily_paths or {}).get("archive_path", ""),
                )
                if desktop_dashboard_paths or desktop_market_review_paths:
                    logger.info(
                        "已同步桌面详细报告: dashboard=%s market_review=%s",
                        (desktop_dashboard_paths or {}).get("desktop_path", ""),
                        (desktop_market_review_paths or {}).get("desktop_path", ""),
                    )
            if merge_notification and send_runtime_notifications and pipeline.notifier.is_available():
                if pipeline.notifier.send(summary_content, email_send_to_all=True):
                    logger.info("已发送短摘要版合并推送")
                else:
                    logger.warning("短摘要版合并推送失败")

        # 输出摘要
        if results:
            logger.info("\n===== 分析结果摘要 =====")
            for r in sorted(results, key=lambda x: x.sentiment_score, reverse=True):
                emoji = r.get_emoji() if hasattr(r, "get_emoji") else "📌"
                logger.info(
                    f"{emoji} {r.name}({r.code}): {r.operation_advice} | "
                    f"评分 {r.sentiment_score} | {r.trend_prediction}"
                )

        logger.info("\n任务执行完成")

        # === 新增：生成飞书云文档 ===
        try:
            from src.feishu_doc import FeishuDocManager

            feishu_doc = FeishuDocManager()
            if feishu_doc.is_configured() and (results or market_report):
                logger.info("正在创建飞书云文档...")

                # 1. 准备标题 "01-01 13:01大盘复盘"
                tz_cn = timezone(timedelta(hours=8))
                now = datetime.now(tz_cn)
                doc_title = f"{now.strftime('%Y-%m-%d %H:%M')} 大盘复盘"

                # 2. 准备内容 (拼接个股分析和大盘复盘)
                full_content = ""

                # 添加大盘复盘内容（如果有）
                if market_report:
                    full_content += f"# 📈 大盘复盘\n\n{market_report}\n\n---\n\n"

                # 添加个股决策仪表盘（使用 NotificationService 生成，按 report_type 分支）
                if results:
                    dashboard_content = pipeline.notifier.generate_aggregate_report(
                        results,
                        getattr(config, 'report_type', 'simple'),
                    )
                    full_content += f"# 🚀 个股决策仪表盘\n\n{dashboard_content}"

                # 3. 创建文档
                doc_url = feishu_doc.create_daily_doc(doc_title, full_content)
                if doc_url:
                    logger.info(f"飞书云文档创建成功: {doc_url}")
                    # 可选：将文档链接也推送到群里
                    if send_runtime_notifications:
                        pipeline.notifier.send(f"[{now.strftime('%Y-%m-%d %H:%M')}] 复盘文档创建成功: {doc_url}")

        except Exception as e:
            logger.error(f"飞书文档生成失败: {e}")

        # === Auto backtest ===
        try:
            if getattr(config, 'backtest_enabled', False):
                from src.services.backtest_service import BacktestService

                logger.info("开始自动回测...")
                service = BacktestService()
                stats = service.run_backtest(
                    force=False,
                    eval_window_days=getattr(config, 'backtest_eval_window_days', 10),
                    min_age_days=getattr(config, 'backtest_min_age_days', 14),
                    limit=200,
                )
                logger.info(
                    f"自动回测完成: processed={stats.get('processed')} saved={stats.get('saved')} "
                    f"completed={stats.get('completed')} insufficient={stats.get('insufficient')} errors={stats.get('errors')}"
                )
        except Exception as e:
            logger.warning(f"自动回测失败（已忽略）: {e}")

    except Exception as e:
        logger.exception(f"分析流程执行失败: {e}")


def start_api_server(host: str, port: int, config: Config) -> None:
    """
    在后台线程启动 FastAPI 服务

    Args:
        host: 监听地址
        port: 监听端口
        config: 配置对象
    """
    import threading
    import uvicorn

    def run_server():
        level_name = (config.log_level or "INFO").lower()
        uvicorn.run(
            "api.app:app",
            host=host,
            port=port,
            log_level=level_name,
            log_config=None,
        )

    thread = threading.Thread(target=run_server, daemon=True)
    thread.start()
    logger.info(f"FastAPI 服务已启动: http://{host}:{port}")


def _is_truthy_env(var_name: str, default: str = "true") -> bool:
    """Parse common truthy / falsy environment values."""
    value = os.getenv(var_name, default).strip().lower()
    return value not in {"0", "false", "no", "off"}


def start_bot_stream_clients(config: Config) -> None:
    """Start bot stream clients when enabled in config."""
    # 启动钉钉 Stream 客户端
    if config.dingtalk_stream_enabled:
        try:
            from bot.platforms import start_dingtalk_stream_background, DINGTALK_STREAM_AVAILABLE
            if DINGTALK_STREAM_AVAILABLE:
                if start_dingtalk_stream_background():
                    logger.info("[Main] Dingtalk Stream client started in background.")
                else:
                    logger.warning("[Main] Dingtalk Stream client failed to start.")
            else:
                logger.warning("[Main] Dingtalk Stream enabled but SDK is missing.")
                logger.warning("[Main] Run: pip install dingtalk-stream")
        except Exception as exc:
            logger.error(f"[Main] Failed to start Dingtalk Stream client: {exc}")

    # 启动飞书 Stream 客户端
    if getattr(config, 'feishu_stream_enabled', False):
        try:
            from bot.platforms import start_feishu_stream_background, FEISHU_SDK_AVAILABLE
            if FEISHU_SDK_AVAILABLE:
                if start_feishu_stream_background():
                    logger.info("[Main] Feishu Stream client started in background.")
                else:
                    logger.warning("[Main] Feishu Stream client failed to start.")
            else:
                logger.warning("[Main] Feishu Stream enabled but SDK is missing.")
                logger.warning("[Main] Run: pip install lark-oapi")
        except Exception as exc:
            logger.error(f"[Main] Failed to start Feishu Stream client: {exc}")


def _resolve_scheduled_stock_codes(stock_codes: Optional[List[str]]) -> Optional[List[str]]:
    """Scheduled runs should always read the latest persisted watchlist."""
    if stock_codes is not None:
        logger.warning(
            "定时模式下检测到 --stocks 参数；计划执行将忽略启动时股票快照，并在每次运行前重新读取最新的 STOCK_LIST。"
        )
    return None


def _get_analysis_stock_codes(config: Config) -> List[str]:
    if hasattr(config, "get_analysis_stock_list"):
        return config.get_analysis_stock_list()
    merged: List[str] = []
    seen = set()
    for code in list(getattr(config, "stock_list", []) or []) + list(getattr(config, "watchlist_stock_list", []) or []):
        normalized = (code or "").strip().upper()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        merged.append(normalized)
    return merged


def _reload_runtime_config() -> Config:
    """Reload config from the latest persisted `.env` values for scheduled runs."""
    _reload_env_file_values_preserving_overrides()
    Config.reset_instance()
    return get_config()


def _build_schedule_time_provider(default_schedule_time: str):
    """Read the latest schedule time directly from the active config file.

    Fallback order:
    1. Process-level env override (set before launch) → honour it.
    2. Persisted config file value (written by WebUI) → use it.
    3. Documented system default ``"18:00"`` → always fall back here so
       that clearing SCHEDULE_TIME in WebUI correctly resets the schedule.
    """
    from src.core.config_manager import ConfigManager

    _SYSTEM_DEFAULT_SCHEDULE_TIME = "18:00"
    manager = ConfigManager()

    def _provider() -> str:
        if "SCHEDULE_TIME" in _INITIAL_PROCESS_ENV:
            return os.getenv("SCHEDULE_TIME", default_schedule_time)

        config_map = manager.read_config_map()
        schedule_time = (config_map.get("SCHEDULE_TIME", "") or "").strip()
        if schedule_time:
            return schedule_time
        return _SYSTEM_DEFAULT_SCHEDULE_TIME

    return _provider


def _should_catch_up_missed_daily_report(
    schedule_time: str,
    *,
    now: Optional[datetime] = None,
    archive_dir: Optional[Path] = None,
) -> bool:
    """Return True when today's scheduled report time passed but no archive exists."""
    candidate = (schedule_time or "").strip()
    match = re.fullmatch(r"(\d{2}):(\d{2})", candidate)
    if not match:
        return False

    hour = int(match.group(1))
    minute = int(match.group(2))
    if hour > 23 or minute > 59:
        return False

    current_time = now or datetime.now()
    scheduled_at = current_time.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if current_time < scheduled_at:
        return False

    report_date = current_time.strftime("%Y-%m-%d")
    target_archive_dir = archive_dir or (Path.cwd() / "reports" / "daily_push_archive")
    return not (target_archive_dir / f"{report_date}_盘前总报告.md").exists()


def main() -> int:
    """
    主入口函数

    Returns:
        退出码（0 表示成功）
    """
    # 解析命令行参数
    args = parse_arguments()

    # 在配置加载前先初始化 bootstrap 日志，确保早期失败也能落盘
    try:
        _setup_bootstrap_logging(debug=args.debug)
    except Exception as exc:
        logging.basicConfig(
            level=logging.DEBUG if getattr(args, "debug", False) else logging.INFO,
            format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            stream=sys.stderr,
        )
        logger.warning("Bootstrap 日志初始化失败，已回退到 stderr: %s", exc)

    # 加载配置（在 bootstrap logging 之后执行，确保异常有日志）
    try:
        config = get_config()
    except Exception as exc:
        logger.exception("加载配置失败: %s", exc)
        return 1

    # 配置日志（输出到控制台和文件）
    try:
        setup_logging(log_prefix="stock_analysis", debug=args.debug, log_dir=config.log_dir)
    except Exception as exc:
        logger.exception("切换到配置日志目录失败: %s", exc)
        return 1

    logger.info("=" * 60)
    logger.info("A股自选股智能分析系统 启动")
    logger.info(f"运行时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 60)

    # 验证配置
    warnings = config.validate()
    for warning in warnings:
        logger.warning(warning)

    # 解析股票列表（统一为大写 Issue #355）
    stock_codes = None
    if args.stocks:
        stock_codes = [canonical_stock_code(c) for c in args.stocks.split(',') if (c or "").strip()]
        logger.info(f"使用命令行指定的股票列表: {stock_codes}")

    # === 处理 --webui / --webui-only 参数，映射到 --serve / --serve-only ===
    if args.webui:
        args.serve = True
    if args.webui_only:
        args.serve_only = True

    schedule_mode = _should_run_in_schedule_mode(args, config)
    explicit_one_shot = _has_explicit_one_shot_request(args)

    # 兼容旧版 WEBUI_ENABLED 环境变量
    if config.webui_enabled and not explicit_one_shot and not (args.serve or args.serve_only):
        args.serve = True

    if schedule_mode:
        if not _acquire_schedule_singleton_guard():
            return 0

    # === 启动 Web 服务 (如果启用) ===
    start_serve = (args.serve or args.serve_only) and os.getenv("GITHUB_ACTIONS") != "true"

    # 兼容旧版 WEBUI_HOST/WEBUI_PORT：如果用户未通过 --host/--port 指定，则使用旧变量
    if start_serve:
        if args.host == '0.0.0.0' and os.getenv('WEBUI_HOST'):
            args.host = os.getenv('WEBUI_HOST')
        if args.port == 8000 and os.getenv('WEBUI_PORT'):
            args.port = int(os.getenv('WEBUI_PORT'))

    bot_clients_started = False
    if start_serve:
        if not prepare_webui_frontend_assets():
            logger.warning("前端静态资源未就绪，继续启动 FastAPI 服务（Web 页面可能不可用）")
        try:
            start_api_server(host=args.host, port=args.port, config=config)
            bot_clients_started = True
        except Exception as e:
            logger.error(f"启动 FastAPI 服务失败: {e}")

    if bot_clients_started:
        start_bot_stream_clients(config)

    # === 仅 Web 服务模式：不自动执行分析 ===
    if args.serve_only:
        logger.info("模式: 仅 Web 服务")
        logger.info(f"Web 服务运行中: http://{args.host}:{args.port}")
        logger.info("通过 /api/v1/analysis/analyze 接口触发分析")
        logger.info(f"API 文档: http://{args.host}:{args.port}/docs")
        logger.info("按 Ctrl+C 退出...")
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("\n用户中断，程序退出")
        return 0

    try:
        # 模式0: 回测
        if getattr(args, 'backtest', False):
            logger.info("模式: 回测")
            from src.services.backtest_service import BacktestService

            service = BacktestService()
            stats = service.run_backtest(
                code=getattr(args, 'backtest_code', None),
                force=getattr(args, 'backtest_force', False),
                eval_window_days=getattr(args, 'backtest_days', None),
            )
            logger.info(
                f"回测完成: processed={stats.get('processed')} saved={stats.get('saved')} "
                f"completed={stats.get('completed')} insufficient={stats.get('insufficient')} errors={stats.get('errors')}"
            )
            return 0

        # 模式1: 仅大盘复盘
        if args.market_review:
            from src.analyzer import GeminiAnalyzer
            from src.core.market_review import run_market_review
            from src.notification import NotificationService
            from src.search_service import SearchService

            # Issue #373: Trading day check for market-review-only mode.
            # Do NOT use _compute_trading_day_filter here: that helper checks
            # config.market_review_enabled, which would wrongly block an
            # explicit --market-review invocation when the flag is disabled.
            effective_region = None
            if not getattr(args, 'force_run', False) and getattr(config, 'trading_day_check_enabled', True):
                from src.core.trading_calendar import get_open_markets_today, compute_effective_region as _compute_region
                open_markets = get_open_markets_today()
                effective_region = _compute_region(
                    getattr(config, 'market_review_region', 'cn') or 'cn', open_markets
                )
                if effective_region == '':
                    logger.info("今日大盘复盘相关市场均为非交易日，跳过执行。可使用 --force-run 强制执行。")
                    return 0

            logger.info("模式: 仅大盘复盘")
            notifier = NotificationService()

            # 初始化搜索服务和分析器（如果有配置）
            search_service = None
            analyzer = None

            if config.has_search_capability_enabled():
                search_service = SearchService(
                    bocha_keys=config.bocha_api_keys,
                    tavily_keys=config.tavily_api_keys,
                    anspire_keys=config.anspire_api_keys,
                    brave_keys=config.brave_api_keys,
                    serpapi_keys=config.serpapi_keys,
                    minimax_keys=config.minimax_api_keys,
                    searxng_base_urls=config.searxng_base_urls,
                    searxng_public_instances_enabled=config.searxng_public_instances_enabled,
                    news_max_age_days=config.news_max_age_days,
                    news_strategy_profile=getattr(config, "news_strategy_profile", "short"),
                )

            if config.gemini_api_key or config.openai_api_key:
                analyzer = GeminiAnalyzer(api_key=config.gemini_api_key)
                if not analyzer.is_available():
                    logger.warning("AI 分析器初始化后不可用，请检查 API Key 配置")
                    analyzer = None
            else:
                logger.warning("未检测到 API Key (Gemini/OpenAI)，将仅使用模板生成报告")

            run_market_review(
                notifier=notifier,
                analyzer=analyzer,
                search_service=search_service,
                send_notification=_should_send_runtime_notifications(config, args),
                override_region=effective_region,
            )
            return 0

        # 模式2: 定时任务模式
        if schedule_mode:
            logger.info("模式: 定时任务")
            logger.info(f"每日执行时间: {config.schedule_time}")

            # Determine whether to run immediately:
            # Command line arg --no-run-immediately overrides config if present.
            # Otherwise use config (defaults to True).
            should_run_immediately = config.schedule_run_immediately
            if getattr(args, 'no_run_immediately', False):
                should_run_immediately = False

            logger.info(f"启动时立即执行: {should_run_immediately}")

            from src.scheduler import run_with_schedule
            scheduled_stock_codes = _resolve_scheduled_stock_codes(stock_codes)
            schedule_time_provider = _build_schedule_time_provider(config.schedule_time)

            def scheduled_task():
                runtime_config = _reload_runtime_config()
                scheduled_args = argparse.Namespace(**vars(args))
                setattr(scheduled_args, "_scheduled_invocation", True)
                run_full_analysis(runtime_config, scheduled_args, scheduled_stock_codes)

            if not should_run_immediately:
                catch_up_schedule_time = schedule_time_provider()
                if _should_catch_up_missed_daily_report(catch_up_schedule_time):
                    logger.info(
                        "检测到今日 %s 主任务已错过且盘前总报告未生成，启动一次补跑",
                        catch_up_schedule_time,
                    )
                    scheduled_task()

            background_tasks = []
            extra_daily_tasks = []

            # 收盘提醒任务（每日 15:10）
            close_reminder_time = getattr(config, 'close_reminder_time', '15:10')
            close_reminder_enabled = getattr(config, 'close_reminder_enabled', False)

            if close_reminder_enabled:
                from src.notification import NotificationBuilder, NotificationService

                def close_reminder_task():
                    try:
                        notification_service = NotificationService()
                        reminder = _build_brief_monitor_alert(
                            trigger_event="收盘检查窗口开启",
                            strategy="IC贴水 / 认沽保护 / 现金管理",
                            bias="优先检查保证金、逆回购和持仓更新",
                            immediate_attention=True,
                        )
                        alert_text = NotificationBuilder.build_simple_alert(
                            title="收盘提醒",
                            content=reminder,
                            alert_type="warning",
                        )
                        notification_service.send(alert_text)
                        logger.info("[CloseReminder] 收盘提醒已发送")
                    except Exception as exc:
                        logger.exception("[CloseReminder] 收盘提醒发送失败: %s", exc)

                extra_daily_tasks.append({
                    "task": close_reminder_task,
                    "schedule_time": close_reminder_time,
                    "name": "close_reminder",
                })
                logger.info("已注册收盘提醒任务，执行时间: %s", close_reminder_time)

            premarket_health_check_enabled = getattr(config, 'premarket_health_check_enabled', False)
            premarket_health_check_time = getattr(config, 'premarket_health_check_time', '08:50')
            if premarket_health_check_enabled:
                def premarket_health_check_task():
                    try:
                        runtime_config = _reload_runtime_config()
                        if not getattr(runtime_config, 'premarket_health_check_enabled', False):
                            logger.info("[PremarketHealth] 当前运行配置已关闭，跳过本轮盘前健康自检")
                            return
                        from src.services.premarket_health_check import run_premarket_health_check

                        payload = run_premarket_health_check(
                            config=runtime_config,
                            project_root=Path(__file__).resolve().parent,
                        )
                        logger.info(
                            "[PremarketHealth] 盘前健康自检完成: status=%s report=%s sent=%s",
                            payload.get("status"),
                            payload.get("report_path", ""),
                            payload.get("notification_sent", False),
                        )
                    except Exception as exc:
                        logger.exception("[PremarketHealth] 盘前健康自检失败: %s", exc)

                extra_daily_tasks.append({
                    "task": premarket_health_check_task,
                    "schedule_time": premarket_health_check_time,
                    "name": "premarket_health_check",
                })
                logger.info("已注册盘前健康自检任务，执行时间: %s", premarket_health_check_time)

            nightly_market_outlook_enabled = getattr(config, 'nightly_market_outlook_enabled', False)
            nightly_market_outlook_time = getattr(config, 'nightly_market_outlook_time', '22:30')
            if nightly_market_outlook_enabled:
                def nightly_market_outlook_task():
                    try:
                        runtime_config = _reload_runtime_config()
                        if not getattr(runtime_config, 'nightly_market_outlook_enabled', False):
                            logger.info("[NightlyOutlook] 当前运行配置已关闭，跳过本轮明日大盘预测")
                            return
                        run_nightly_market_outlook(runtime_config)
                    except Exception as exc:
                        logger.exception("[NightlyOutlook] 明日大盘预测任务失败: %s", exc)

                extra_daily_tasks.append({
                    "task": nightly_market_outlook_task,
                    "schedule_time": nightly_market_outlook_time,
                    "name": "nightly_market_outlook",
                })
                logger.info("已注册明日大盘预测任务，执行时间: %s", nightly_market_outlook_time)

            market_data_warehouse_enabled = getattr(config, 'market_data_warehouse_enabled', False)
            market_data_warehouse_time = getattr(config, 'market_data_warehouse_time', '15:45')
            if market_data_warehouse_enabled:
                def market_data_warehouse_task():
                    service = None
                    try:
                        runtime_config = _reload_runtime_config()
                        if not getattr(runtime_config, 'market_data_warehouse_enabled', False):
                            logger.info("[MarketDataWarehouse] 当前运行配置已关闭，跳过本轮数据沉淀")
                            return
                        from src.services.market_data_warehouse_service import MarketDataWarehouseService

                        service = MarketDataWarehouseService()
                        payload = service.run_refresh(config=runtime_config)
                        totals = payload.get("totals") or {}
                        logger.info(
                            "[MarketDataWarehouse] 本轮完成: status=%s targets=%s succeeded=%s failed=%s rows_inserted=%s ledger=%s",
                            payload.get("status"),
                            totals.get("target_count", 0),
                            totals.get("succeeded", 0),
                            totals.get("failed", 0),
                            totals.get("rows_inserted", 0),
                            payload.get("ledger_path", ""),
                        )
                    except Exception as exc:
                        logger.exception("[MarketDataWarehouse] 数据沉淀任务失败: %s", exc)
                    finally:
                        if service is not None:
                            service.close()

                extra_daily_tasks.append({
                    "task": market_data_warehouse_task,
                    "schedule_time": market_data_warehouse_time,
                    "name": "market_data_warehouse",
                })
                logger.info("已注册本地行情数据沉淀任务，执行时间: %s", market_data_warehouse_time)

            post_close_shadow_refresh_enabled = getattr(config, 'post_close_shadow_refresh_enabled', False)
            post_close_shadow_refresh_time = getattr(config, 'post_close_shadow_refresh_time', '16:20')
            if post_close_shadow_refresh_enabled:
                def post_close_shadow_refresh_task():
                    try:
                        runtime_config = _reload_runtime_config()
                        if not getattr(runtime_config, 'post_close_shadow_refresh_enabled', False):
                            logger.info("[PostCloseShadowRefresh] 当前运行配置已关闭，跳过本轮理论回测刷新")
                            return
                        from scripts.run_post_close_shadow_refresh import run_post_close_shadow_refresh

                        payload = run_post_close_shadow_refresh(
                            output_dir=Path(__file__).resolve().parent / "reports" / "backtests",
                            timeout_seconds=getattr(runtime_config, 'post_close_shadow_refresh_timeout_seconds', 900),
                            rebuild_ledger=getattr(runtime_config, 'post_close_shadow_refresh_rebuild_ledger', False),
                        )
                        logger.info(
                            "[PostCloseShadowRefresh] 理论评分表与 Shadow 账本刷新完成: summary=%s json=%s",
                            payload.get("summary_path", ""),
                            payload.get("json_path", ""),
                        )
                    except Exception as exc:
                        logger.exception("[PostCloseShadowRefresh] 理论评分表与 Shadow 账本刷新失败: %s", exc)

                extra_daily_tasks.append({
                    "task": post_close_shadow_refresh_task,
                    "schedule_time": post_close_shadow_refresh_time,
                    "name": "post_close_shadow_refresh",
                })
                logger.info("已注册收盘后理论回测刷新任务，执行时间: %s", post_close_shadow_refresh_time)

            portfolio_daily_review_enabled = getattr(config, 'portfolio_daily_review_enabled', False)
            portfolio_daily_review_time = getattr(config, 'portfolio_daily_review_time', '16:05')
            if portfolio_daily_review_enabled:
                def portfolio_daily_review_task():
                    try:
                        runtime_config = _reload_runtime_config()
                        if not getattr(runtime_config, 'portfolio_daily_review_enabled', False):
                            logger.info("[PortfolioDailyReview] 当前运行配置已关闭，跳过本轮持仓复盘")
                            return
                        from src.services.portfolio_daily_review_service import PortfolioDailyReviewService

                        payload = PortfolioDailyReviewService(config=runtime_config).run(
                            run_backtests=True,
                            send_notification=getattr(runtime_config, 'portfolio_daily_review_notify_enabled', False),
                        )
                        radar = payload.get("radar") or {}
                        logger.info(
                            "[PortfolioDailyReview] 每日持仓复盘完成: holdings=%s report=%s notified=%s",
                            radar.get("holding_count", 0),
                            payload.get("markdown_path", ""),
                            payload.get("notification_sent", False),
                        )
                    except Exception as exc:
                        logger.exception("[PortfolioDailyReview] 每日持仓复盘失败: %s", exc)

                extra_daily_tasks.append({
                    "task": portfolio_daily_review_task,
                    "schedule_time": portfolio_daily_review_time,
                    "name": "portfolio_daily_review",
                })
                logger.info("已注册每日持仓复盘任务，执行时间: %s", portfolio_daily_review_time)

            workstation_cleanup_enabled = getattr(config, 'workstation_cleanup_enabled', False)
            workstation_cleanup_time = getattr(config, 'workstation_cleanup_time', '02:20')
            if workstation_cleanup_enabled:
                def workstation_cleanup_task():
                    try:
                        runtime_config = _reload_runtime_config()
                        if not getattr(runtime_config, 'workstation_cleanup_enabled', False):
                            logger.info("[WorkstationCleanup] 当前运行配置已关闭，跳过本轮日志缓存清理")
                            return
                        from src.services.workstation_cleanup_service import WorkstationCleanupService

                        cleanup_project_root = Path(__file__).resolve().parent
                        database_path = Path(str(getattr(runtime_config, 'database_path', '') or '')).expanduser().resolve()
                        if database_path.parent.name == "data":
                            cleanup_project_root = database_path.parent.parent
                        payload = WorkstationCleanupService(
                            config=runtime_config,
                            project_root=cleanup_project_root,
                        ).run()
                        totals = payload.get("totals") or {}
                        logger.info(
                            "[WorkstationCleanup] 旧日志/缓存清理完成: deleted=%s freed_bytes=%s ledger=%s",
                            totals.get("deleted_count", 0),
                            totals.get("freed_bytes", 0),
                            payload.get("ledger_path", ""),
                        )
                    except Exception as exc:
                        logger.exception("[WorkstationCleanup] 日志缓存清理失败: %s", exc)

                extra_daily_tasks.append({
                    "task": workstation_cleanup_task,
                    "schedule_time": workstation_cleanup_time,
                    "name": "workstation_cleanup",
                })
                logger.info("已注册旧日志/缓存清理任务，执行时间: %s", workstation_cleanup_time)

            if getattr(config, 'agent_event_monitor_enabled', False):
                from src.agent.events import build_event_monitor_from_config, run_event_monitor_once

                monitor = build_event_monitor_from_config(config)
                if monitor is not None:
                    interval_minutes = max(1, getattr(config, 'agent_event_monitor_interval_minutes', 5))

                    def event_monitor_task():
                        if not _is_cn_intraday_monitoring_session():
                            logger.debug("[EventMonitor] 非A股盘中交易时段，本轮跳过事件监控")
                            return
                        triggered = run_event_monitor_once(monitor)
                        if triggered:
                            logger.info("[EventMonitor] 本轮命中 %d 条规则", len(triggered))

                    background_tasks.append({
                        "task": event_monitor_task,
                        "interval_seconds": interval_minutes * 60,
                        "run_immediately": True,
                        "name": "agent_event_monitor",
                    })
                else:
                    logger.info("EventMonitor 已启用，但未加载到有效规则，跳过后台提醒任务")

            if getattr(config, 'intraday_snapshot_enabled', False):
                from src.intraday_snapshot_collector import collect_intraday_snapshots

                snapshot_interval_minutes = max(
                    1, getattr(config, 'intraday_snapshot_interval_minutes', 5)
                )

                def intraday_snapshot_task():
                    if not _is_cn_intraday_monitoring_session():
                        logger.debug("[IntradayCollector] 非A股盘中交易时段，本轮跳过快照采集")
                        return
                    payload = collect_intraday_snapshots(
                        jin10_api_key=getattr(config, 'jin10_api_key', ''),
                        jin10_x_token=getattr(config, 'jin10_x_token', ''),
                    )
                    if payload:
                        stock_reminder_payload = None
                        if getattr(config, 'stock_intraday_reminder_enabled', False):
                            from src.services.stock_intraday_reminder import run_stock_intraday_reminder_cycle

                            stock_reminder_payload = run_stock_intraday_reminder_cycle(
                                state_path=Path(__file__).resolve().parent / "reports" / "stock_intraday_reminder_state.json",
                                config=config,
                            )
                        shadow_payload = payload.get("shadow_monitoring_payload") or {}
                        logger.info(
                            "[IntradayCollector] 已记录快照: %s | 新事件: %s | 新基差信号: %s | M1-M2候选: %s | 事件簇: %s | 个股提醒数: %s | 个股推送: %s",
                            payload.get("snapshot_path", ""),
                            payload.get("new_event_count", 0),
                            payload.get("new_basis_signal_count", 0),
                            shadow_payload.get("candidate_count", 0),
                            shadow_payload.get("event_cluster_count", 0),
                            (stock_reminder_payload or {}).get("item_count", 0),
                            (stock_reminder_payload or {}).get("sent", False),
                        )

                background_tasks.append({
                    "task": intraday_snapshot_task,
                    "interval_seconds": snapshot_interval_minutes * 60,
                    "run_immediately": True,
                    "name": "intraday_snapshot_collector",
                })
                logger.info(
                    "已注册分钟级快照采集任务，执行间隔: %s 分钟",
                    snapshot_interval_minutes,
                )

            run_with_schedule(
                task=scheduled_task,
                schedule_time=config.schedule_time,
                run_immediately=should_run_immediately,
                background_tasks=background_tasks,
                schedule_time_provider=schedule_time_provider,
                extra_daily_tasks=extra_daily_tasks,
            )
            return 0

        # 模式3: 正常单次运行
        if config.run_immediately:
            run_full_analysis(config, args, stock_codes)
        else:
            logger.info("配置为不立即运行分析 (RUN_IMMEDIATELY=false)")

        logger.info("\n程序执行完成")

        # 如果启用了服务且是非定时任务模式，保持程序运行
        keep_running = start_serve and not schedule_mode
        if keep_running:
            logger.info("API 服务运行中 (按 Ctrl+C 退出)...")
            try:
                while True:
                    time.sleep(1)
            except KeyboardInterrupt:
                pass

        return 0

    except KeyboardInterrupt:
        logger.info("\n用户中断，程序退出")
        return 130

    except Exception as e:
        logger.exception(f"程序执行失败: {e}")
        return 1


if __name__ == "__main__":
    import multiprocessing

    multiprocessing.freeze_support()
    sys.exit(main())
