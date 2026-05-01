from __future__ import annotations

import hashlib
import json
import os
from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeoutError
from dataclasses import dataclass
from datetime import datetime, time
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Set

from src.config import get_config
from src.core.trading_calendar import get_market_now, is_market_open
from src.notification import NotificationService
from src.repositories.analysis_repo import AnalysisRepository
from src.services.signal_router import SignalEvent, SignalRouter
from src.services.stock_service import StockService

QUOTE_TIMEOUT_SECONDS = 3.0


@dataclass
class StockReminderItem:
    code: str
    name: str
    action: str
    operation_advice: str
    trend_prediction: str
    stop_loss: Optional[float]
    take_profit: Optional[float]
    current_price: Optional[float]
    change_percent: Optional[float]
    created_at: str
    quote_quality: str = "ok"
    trigger_reason: str = "历史分析信号"


@dataclass
class WatchlistBuyFilter:
    """Noise gate for watchlist buy alerts.

    Watchlist alerts are offensive entry signals, so the default posture is
    "宁可错过，不要骚扰": only alert when price, intraday move and risk/reward
    are all in a usable zone.
    """

    start_time: time = time(14, 30)
    end_time: time = time(14, 55)
    min_change_pct: float = -1.5
    max_change_pct: float = 2.5
    max_stop_loss_distance_pct: float = 3.5
    ma_proximity_pct: float = 1.5
    require_quote: bool = True
    require_stop_loss: bool = True


@dataclass
class HoldingRealtimeRadarConfig:
    """Trigger-only realtime radar for existing holdings.

    The radar is intentionally rule-based: it should interrupt only when live
    price behavior invalidates the holding plan, not when the market simply
    wiggles around.
    """

    enabled: bool = True
    intraday_drop_pct: float = -3.0
    ma20_break_buffer_pct: float = 0.2
    reversal_from_high_pct: float = 3.0
    tail_start_time: time = time(14, 30)


@contextmanager
def _file_lock(path: Path, *, exclusive: bool):
    path.parent.mkdir(parents=True, exist_ok=True)
    lock_path = path.with_name(f"{path.name}.lock")
    handle = lock_path.open("a+", encoding="utf-8")
    try:
        try:
            import fcntl

            fcntl.flock(handle.fileno(), fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH)
        except ImportError:
            pass
        yield
    finally:
        try:
            import fcntl

            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        except Exception:
            pass
        handle.close()


def _load_state(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        with _file_lock(path, exclusive=False):
            payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _save_state(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with _file_lock(path, exclusive=True):
        tmp_path = path.with_name(f".{path.name}.{os.getpid()}.tmp")
        tmp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        tmp_path.replace(path)


def _infer_exchange(code: str) -> str:
    normalized = str(code or "").strip()
    if normalized.startswith(("5", "6", "9")):
        return "SH"
    if normalized.startswith(("0", "1", "2", "3")):
        return "SZ"
    return ""


def _build_signal_id(*, now: datetime, scope: str, item: StockReminderItem, signal_key: str) -> str:
    raw = "|".join(
        [
            now.isoformat(),
            scope,
            item.code,
            item.action,
            item.trigger_reason,
            signal_key,
        ]
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]


def _ledger_signal_type(scope: str, item: StockReminderItem) -> str:
    if scope == "watchlist_buy" or item.action == "买":
        return "BUY_SETUP"
    reason = item.trigger_reason or ""
    if "止损" in reason:
        return "RISK_STOP"
    if "MA20" in reason or "MA5" in reason:
        return "RISK_MA_BREAK"
    return "RISK_ALERT"


def _append_replay_ledger(
    path: Path,
    *,
    now: datetime,
    scope: str,
    item: StockReminderItem,
    route_result: Optional[Dict[str, Any]],
    duplicate_group: bool,
    signal_key: str = "",
    rule_version: str = "stock_intraday_replay_v2",
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    route_decision = (route_result or {}).get("decision") if isinstance(route_result, dict) else None
    sent = bool((route_result or {}).get("sent"))
    policy_allowed = bool((route_result or {}).get("policy_allowed", True))
    payload = {
        "schema_version": 2,
        "signal_id": _build_signal_id(now=now, scope=scope, item=item, signal_key=signal_key),
        "rule_version": rule_version,
        "event_time": now.isoformat(),
        "trigger_timestamp": now.isoformat(timespec="milliseconds"),
        "scope": scope,
        "code": item.code,
        "symbol": item.code,
        "exchange": _infer_exchange(item.code),
        "name": item.name,
        "signal_type": _ledger_signal_type(scope, item),
        "action": item.action,
        "operation_advice": item.operation_advice,
        "trend_prediction": item.trend_prediction,
        "trigger_reason": item.trigger_reason,
        "trigger_condition_snapshot": {
            "current_price": item.current_price,
            "change_percent": item.change_percent,
            "stop_loss": item.stop_loss,
            "take_profit": item.take_profit,
            "quote_quality": item.quote_quality,
            "analysis_created_at": item.created_at,
            "operation_advice": item.operation_advice,
            "trend_prediction": item.trend_prediction,
        },
        "current_price": item.current_price,
        "change_percent": item.change_percent,
        "stop_loss": item.stop_loss,
        "take_profit": item.take_profit,
        "quote_quality": item.quote_quality,
        "analysis_created_at": item.created_at,
        "notified": sent,
        "send_status": "SENT" if sent else ("SUPPRESSED_ROUTE" if not policy_allowed else "PENDING_SEND"),
        "suppressed_duplicate": bool(duplicate_group),
        "is_duplicate": bool(duplicate_group),
        "route_decision": route_decision,
        "position_info": {
            "position_size": None,
            "avg_cost": None,
            "unrealized_pnl_pct": None,
        },
        "next_action_time": None,
        "forward_labels": {
            "t_plus_1": None,
            "t_plus_3": None,
            "t_plus_5": None,
        },
        "outcome_reference_window": {
            "outcome_max_adverse_1h": None,
            "outcome_max_favorable_1h": None,
            "outcome_max_adverse_1d": None,
            "outcome_max_favorable_1d": None,
            "outcome_hit_target": None,
        },
    }
    with _file_lock(path, exclusive=True):
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, ensure_ascii=False, sort_keys=True) + "\n")


def _is_cn_intraday_session(now: datetime) -> bool:
    now = get_market_now("cn", now)
    if not is_market_open("cn", now.date()):
        return False
    current = now.time()
    morning = time(9, 30) <= current <= time(11, 30)
    afternoon = time(13, 0) <= current <= time(15, 0)
    return morning or afternoon


def _normalize_action(operation_advice: str) -> Optional[str]:
    text = (operation_advice or "").strip()
    if not text:
        return None
    if "卖出" in text or "减仓" in text:
        return "卖"
    if "买入" in text:
        return "买"
    return None


def _safe_num(value: Any, digits: int = 2) -> str:
    if value in (None, ""):
        return "无"
    try:
        return f"{float(value):.{digits}f}"
    except Exception:
        return str(value)


def _to_float(value: Any) -> Optional[float]:
    if value in (None, ""):
        return None
    try:
        parsed = float(value)
    except Exception:
        return None
    return parsed if parsed > 0 else None


def _quote_float(quote: Dict[str, Any], key: str) -> Optional[float]:
    value = quote.get(key)
    if value in (None, ""):
        return None
    try:
        return float(value)
    except Exception:
        return None


def _parse_time(value: Any, default: time) -> time:
    text = str(value or "").strip()
    if not text:
        return default
    for fmt in ("%H:%M", "%H:%M:%S"):
        try:
            return datetime.strptime(text, fmt).time()
        except ValueError:
            continue
    return default


def _time_in_window(current: time, start: time, end: time) -> bool:
    if start <= end:
        return start <= current <= end
    return current >= start or current <= end


def _try_json_dict(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return value
    if not isinstance(value, str) or not value.strip():
        return {}
    try:
        payload = json.loads(value)
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _find_numeric_value(payload: Any, key: str) -> Optional[float]:
    if isinstance(payload, dict):
        for current_key, current_value in payload.items():
            if str(current_key).lower() == key.lower():
                found = _to_float(current_value)
                if found is not None:
                    return found
            nested = _find_numeric_value(current_value, key)
            if nested is not None:
                return nested
    elif isinstance(payload, list):
        for item in payload:
            nested = _find_numeric_value(item, key)
            if nested is not None:
                return nested
    return None


def _extract_ma_values(row: Any) -> Dict[str, float]:
    values: Dict[str, float] = {}
    for key in ("ma5", "ma10", "ma20"):
        direct = _to_float(getattr(row, key, None))
        if direct is not None:
            values[key] = direct
            continue
        for attr in ("raw_result", "context_snapshot"):
            found = _find_numeric_value(_try_json_dict(getattr(row, attr, None)), key)
            if found is not None:
                values[key] = found
                break
    return values


def _passes_watchlist_buy_filter(
    *,
    row: Any,
    current_time: datetime,
    current_price: Optional[float],
    change_percent: Optional[float],
    stop_loss: Optional[float],
    filter_config: WatchlistBuyFilter,
) -> bool:
    if not _time_in_window(current_time.time(), filter_config.start_time, filter_config.end_time):
        return False

    if filter_config.require_quote and current_price is None:
        return False

    if current_price is not None and current_price <= 0:
        return False

    if change_percent is None:
        return False
    if change_percent < filter_config.min_change_pct or change_percent > filter_config.max_change_pct:
        return False

    safe_stop_loss = _to_float(stop_loss)
    if safe_stop_loss is None:
        return not filter_config.require_stop_loss
    if current_price is None:
        return False
    stop_distance_pct = (current_price - safe_stop_loss) / current_price * 100
    if stop_distance_pct < 0 or stop_distance_pct > filter_config.max_stop_loss_distance_pct:
        return False

    ma_values = _extract_ma_values(row)
    core_ma_values = [ma_values[key] for key in ("ma5", "ma20") if key in ma_values]
    if core_ma_values:
        return any(
            abs(current_price - ma_value) / ma_value * 100 <= filter_config.ma_proximity_pct
            for ma_value in core_ma_values
            if ma_value > 0
        )

    # Older analysis rows may not have saved MA values. In that case keep the
    # alert usable, but only after the stricter price/stop-loss gates pass.
    return True


def _build_holding_realtime_radar_item(
    *,
    row: Any,
    quote: Dict[str, Any],
    current_time: datetime,
    current_price: Optional[float],
    change_percent: Optional[float],
    stop_loss: Optional[float],
    take_profit: Optional[float],
    quote_quality: str,
    radar_config: HoldingRealtimeRadarConfig,
) -> Optional[StockReminderItem]:
    if not radar_config.enabled or quote_quality != "ok" or current_price is None:
        return None

    code = str(getattr(row, "code", "") or "").strip()
    name = str(getattr(row, "name", "") or code)
    if not code:
        return None

    ma_values = _extract_ma_values(row)
    ma5 = ma_values.get("ma5")
    ma20 = ma_values.get("ma20")
    open_price = _quote_float(quote, "open")
    high_price = _quote_float(quote, "high")

    reason = ""
    advice = ""

    if stop_loss is not None and current_price <= stop_loss:
        reason = f"实时跌破止损位 {_safe_num(stop_loss)}"
        advice = f"跌破止损位({_safe_num(stop_loss)})，立即检查减仓/止损。"
    elif ma20 is not None and ma20 > 0:
        break_threshold = ma20 * (1 - radar_config.ma20_break_buffer_pct / 100)
        if current_price <= break_threshold:
            reason = f"实时跌破MA20({_safe_num(ma20)})"
            advice = f"跌破MA20({_safe_num(ma20)})，持仓风险升级，优先减仓或停止加仓。"

    if not reason and change_percent is not None and change_percent <= radar_config.intraday_drop_pct:
        reason = f"盘中跌幅达到{_safe_num(change_percent)}%"
        advice = f"盘中跌幅{_safe_num(change_percent)}%，先按风险信号处理，不等收盘确认。"

    if not reason and high_price is not None and high_price > 0:
        drawdown_from_high = (current_price - high_price) / high_price * 100
        if drawdown_from_high <= -abs(radar_config.reversal_from_high_pct) and (
            open_price is None or current_price < open_price
        ):
            reason = f"冲高回落{_safe_num(abs(drawdown_from_high))}%"
            advice = f"盘中冲高回落{_safe_num(abs(drawdown_from_high))}%，追高失败，优先收紧持仓。"

    if not reason and _time_in_window(current_time.time(), radar_config.tail_start_time, time(15, 0)):
        if ma5 is not None and ma5 > 0 and current_price < ma5:
            reason = f"尾盘失守MA5({_safe_num(ma5)})"
            advice = f"尾盘跌回MA5({_safe_num(ma5)})下方，短线防守优先。"

    if not reason:
        return None

    return StockReminderItem(
        code=code,
        name=name,
        action="卖",
        operation_advice=advice,
        trend_prediction="盘中实时风控",
        stop_loss=stop_loss,
        take_profit=take_profit,
        current_price=current_price,
        change_percent=change_percent,
        created_at=current_time.isoformat(),
        quote_quality=quote_quality,
        trigger_reason=reason,
    )


def _iter_latest_records(stock_codes: Iterable[str], repo: AnalysisRepository, days: int) -> Iterable[Any]:
    for code in stock_codes:
        rows = repo.get_list(code=code, days=days, limit=1)
        if rows:
            yield rows[0]


def _safe_get_realtime_quote(stock_service: StockService, code: str) -> Dict[str, Any]:
    executor = ThreadPoolExecutor(max_workers=1)
    future = executor.submit(stock_service.get_realtime_quote, code)
    try:
        payload = future.result(timeout=QUOTE_TIMEOUT_SECONDS)
        return payload or {}
    except FutureTimeoutError:
        future.cancel()
        return {}
    except Exception:
        return {}
    finally:
        executor.shutdown(wait=False, cancel_futures=True)


def collect_actionable_stock_items(
    *,
    stock_codes: Iterable[str],
    repo: Optional[AnalysisRepository] = None,
    stock_service: Optional[StockService] = None,
    analysis_max_age_days: int = 3,
    allowed_actions: Optional[Set[str]] = None,
    watchlist_buy_filter: Optional[WatchlistBuyFilter] = None,
    holding_realtime_radar: Optional[HoldingRealtimeRadarConfig] = None,
    bad_tick_max_abs_change_pct: float = 25.0,
    now: Optional[datetime] = None,
) -> List[StockReminderItem]:
    repository = repo or AnalysisRepository()
    quote_service = stock_service or StockService()
    items: List[StockReminderItem] = []

    for row in _iter_latest_records(stock_codes, repository, analysis_max_age_days):
        code = str(getattr(row, "code", "") or "").strip()
        name = str(getattr(row, "name", "") or code)
        if not code:
            continue

        quote = _safe_get_realtime_quote(quote_service, code)
        current_price = quote.get("current_price")
        if current_price is not None:
            try:
                current_price = float(current_price)
            except Exception:
                current_price = None
        change_percent = quote.get("change_percent")
        if change_percent is not None:
            try:
                change_percent = float(change_percent)
            except Exception:
                change_percent = None
        quote_quality = "ok"
        if current_price is not None and current_price <= 0:
            current_price = None
            quote_quality = "bad_tick"
        if change_percent is not None and abs(change_percent) > bad_tick_max_abs_change_pct:
            current_price = None
            change_percent = None
            quote_quality = "bad_tick"

        stop_loss = _to_float(getattr(row, "stop_loss", None))
        take_profit = _to_float(getattr(row, "take_profit", None))

        if holding_realtime_radar is not None:
            radar_item = _build_holding_realtime_radar_item(
                row=row,
                quote=quote,
                current_time=now or datetime.now(),
                current_price=current_price,
                change_percent=change_percent,
                stop_loss=stop_loss,
                take_profit=take_profit,
                quote_quality=quote_quality,
                radar_config=holding_realtime_radar,
            )
            if radar_item is not None:
                items.append(radar_item)
                continue

        action = _normalize_action(str(getattr(row, "operation_advice", "") or ""))
        if not action:
            continue
        if allowed_actions is not None and action not in allowed_actions:
            continue

        if watchlist_buy_filter is not None and action == "买":
            if not _passes_watchlist_buy_filter(
                row=row,
                current_time=now or datetime.now(),
                current_price=current_price,
                change_percent=change_percent,
                stop_loss=stop_loss,
                filter_config=watchlist_buy_filter,
            ):
                continue

        items.append(
            StockReminderItem(
                code=code,
                name=name,
                action=action,
                operation_advice=str(getattr(row, "operation_advice", "") or ""),
                trend_prediction=str(getattr(row, "trend_prediction", "") or ""),
                stop_loss=stop_loss,
                take_profit=take_profit,
                current_price=current_price,
                change_percent=change_percent,
                created_at=str(getattr(row, "created_at", "") or ""),
                quote_quality=quote_quality,
                trigger_reason="历史分析信号",
            )
        )

    action_rank = {"卖": 0, "买": 1}
    items.sort(key=lambda item: (action_rank.get(item.action, 9), item.code))
    return items


def render_stock_intraday_reminder(
    items: List[StockReminderItem],
    *,
    now: datetime,
    title: str = "个股盘中持续提醒",
    rule_text: str = "同日同一组买卖信号只提醒一次；信号变化后再提醒。IC 不在这里高频处理。",
) -> str:
    lines = [
        f"## {title}",
        "",
        f"- 时间: {now.strftime('%Y-%m-%d %H:%M:%S')}",
        f"- 提醒标的数: {len(items)}",
        f"- 规则: {rule_text}",
        "",
    ]
    for item in items:
        action_label = "买入提醒" if item.action == "买" else "卖出提醒"
        lines.extend(
            [
                f"### {action_label} | {item.name}({item.code})",
                f"- 最新建议: {item.operation_advice or '无'} / {item.trend_prediction or '无'}",
                f"- 触发原因: {item.trigger_reason or '无'}",
                f"- 当前价格: {_safe_num(item.current_price)}",
                f"- 涨跌幅: {_safe_num(item.change_percent)}%",
                f"- 行情质量: {'异常Tick已隔离' if item.quote_quality != 'ok' else '正常'}",
                f"- 止损位: {_safe_num(item.stop_loss)}",
                f"- 止盈/减仓位: {_safe_num(item.take_profit)}",
                f"- 分析时间: {item.created_at or '无'}",
                "",
            ]
        )
    return "\n".join(lines).strip()


def _build_signal_keys(items: List[StockReminderItem]) -> List[str]:
    """Build stable same-day de-duplication keys from analysis signals."""
    keys: List[str] = []
    for item in items:
        keys.append(
            "|".join(
                [
                    item.code,
                    item.action,
                    item.operation_advice,
                    item.trend_prediction,
                    _safe_num(item.stop_loss, digits=4),
                    _safe_num(item.take_profit, digits=4),
                ]
            )
        )
    return keys


def _date_prefix(value: Any) -> str:
    text = str(value or "")
    return text[:10] if len(text) >= 10 else ""


def _state_group(state: Dict[str, Any], scope: str) -> Dict[str, Any]:
    groups = state.get("groups")
    if not isinstance(groups, dict):
        return {}
    group_state = groups.get(scope)
    return group_state if isinstance(group_state, dict) else {}


def _is_duplicate_group_signal(
    *,
    state: Dict[str, Any],
    scope: str,
    today_iso: str,
    item_codes: List[str],
    signal_keys: List[str],
) -> bool:
    group_state = _state_group(state, scope)
    last_sent_date = str(group_state.get("last_sent_date") or "")
    last_signal_keys = group_state.get("last_signal_keys")
    if bool(signal_keys) and last_sent_date == today_iso and isinstance(last_signal_keys, list):
        return last_signal_keys == signal_keys

    # Legacy state before per-scope groups existed.
    if scope in {"stock_list_buy_sell", "holding_risk"}:
        legacy_keys = state.get("last_signal_keys")
        legacy_codes = state.get("last_active_codes")
        legacy_duplicate_same_day = (
            bool(signal_keys)
            and not isinstance(legacy_keys, list)
            and _date_prefix(state.get("last_sent_at")) == today_iso
            and isinstance(legacy_codes, list)
            and legacy_codes == item_codes
        )
        if legacy_duplicate_same_day:
            return True
        return (
            bool(signal_keys)
            and str(state.get("last_sent_date") or "") == today_iso
            and isinstance(legacy_keys, list)
            and legacy_keys == signal_keys
        )

    return False


def _build_watchlist_buy_filter(runtime_config: Any) -> WatchlistBuyFilter:
    return WatchlistBuyFilter(
        start_time=_parse_time(
            getattr(runtime_config, "stock_intraday_watchlist_buy_start_time", "14:30"),
            time(14, 30),
        ),
        end_time=_parse_time(
            getattr(runtime_config, "stock_intraday_watchlist_buy_end_time", "14:55"),
            time(14, 55),
        ),
        min_change_pct=float(getattr(runtime_config, "stock_intraday_watchlist_min_change_pct", -1.5)),
        max_change_pct=float(getattr(runtime_config, "stock_intraday_watchlist_max_change_pct", 2.5)),
        max_stop_loss_distance_pct=float(
            getattr(runtime_config, "stock_intraday_watchlist_max_stop_loss_distance_pct", 3.5)
        ),
        ma_proximity_pct=float(getattr(runtime_config, "stock_intraday_watchlist_ma_proximity_pct", 1.5)),
        require_quote=bool(getattr(runtime_config, "stock_intraday_watchlist_require_quote", True)),
        require_stop_loss=bool(getattr(runtime_config, "stock_intraday_watchlist_require_stop_loss", True)),
    )


def _build_holding_realtime_radar_config(runtime_config: Any) -> HoldingRealtimeRadarConfig:
    return HoldingRealtimeRadarConfig(
        enabled=bool(getattr(runtime_config, "stock_intraday_holding_realtime_radar_enabled", True)),
        intraday_drop_pct=float(getattr(runtime_config, "stock_intraday_holding_intraday_drop_pct", -3.0)),
        ma20_break_buffer_pct=float(getattr(runtime_config, "stock_intraday_holding_ma20_break_buffer_pct", 0.2)),
        reversal_from_high_pct=float(getattr(runtime_config, "stock_intraday_holding_reversal_from_high_pct", 3.0)),
        tail_start_time=_parse_time(
            getattr(runtime_config, "stock_intraday_holding_tail_start_time", "14:30"),
            time(14, 30),
        ),
    )


def run_stock_intraday_reminder_cycle(
    *,
    state_path: str | Path,
    config=None,
    now: Optional[datetime] = None,
    repo: Optional[AnalysisRepository] = None,
    stock_service: Optional[StockService] = None,
    notifier_factory: Optional[Callable[[], Any]] = None,
) -> Dict[str, Any]:
    runtime_config = config or get_config()
    state_file = Path(state_path)
    state = _load_state(state_file)
    current_time = get_market_now("cn", now or datetime.now())
    market_open = _is_cn_intraday_session(current_time)

    if not market_open:
        next_state = {
            "last_checked_at": current_time.isoformat(),
            "market_open": False,
            "last_sent_at": state.get("last_sent_at"),
            "last_sent_date": state.get("last_sent_date"),
            "last_active_codes": state.get("last_active_codes", []),
            "last_signal_keys": state.get("last_signal_keys", []),
            "last_suppressed_at": state.get("last_suppressed_at"),
        }
        _save_state(state_file, next_state)
        return {
            "market_open": False,
            "item_count": 0,
            "sent": False,
            "suppressed_duplicate": False,
            "state_path": str(state_file),
        }

    analysis_max_age_days = max(
        1,
        int(getattr(runtime_config, "stock_intraday_reminder_analysis_max_age_days", 3) or 3),
    )
    bad_tick_max_abs_change_pct = float(getattr(runtime_config, "stock_intraday_bad_tick_max_abs_change_pct", 25.0))
    max_items = max(1, int(getattr(runtime_config, "stock_intraday_reminder_max_items", 6) or 6))
    today_iso = current_time.date().isoformat()
    route_state_path = state_file.with_name(f"{state_file.stem}_route_state.json")
    replay_ledger_path = state_file.with_name("stock_intraday_replay_ledger.jsonl")
    replay_ledger_enabled = bool(getattr(runtime_config, "stock_intraday_replay_ledger_enabled", True))
    notifier = notifier_factory() if notifier_factory else NotificationService()
    router = SignalRouter(
        state_path=route_state_path,
        now=current_time,
        p1_cooldown_minutes=int(getattr(runtime_config, "stock_intraday_holding_cooldown_minutes", 30) or 30),
        p2_daily_limit=int(getattr(runtime_config, "stock_intraday_watchlist_daily_limit", 1) or 1),
    )

    groups: List[Dict[str, Any]] = []
    holding_codes = list(getattr(runtime_config, "stock_list", []) or [])
    if holding_codes:
        holding_items = collect_actionable_stock_items(
            stock_codes=holding_codes,
            repo=repo,
            stock_service=stock_service,
            analysis_max_age_days=analysis_max_age_days,
            allowed_actions={"卖"},
            holding_realtime_radar=_build_holding_realtime_radar_config(runtime_config),
            bad_tick_max_abs_change_pct=bad_tick_max_abs_change_pct,
            now=current_time,
        )[:max_items]
        systemic_batch_threshold = max(
            1,
            int(getattr(runtime_config, "stock_intraday_systemic_batch_threshold", 3) or 3),
        )
        groups.append(
            {
                "scope": "holding_risk",
                "items": holding_items,
                "reminder_title": "持仓盘中实时风控",
                "simple_alert_title": (
                    "系统性风险警告：多只持仓触发预警"
                    if len(holding_items) >= systemic_batch_threshold
                    else "持仓盘中风控提醒"
                ),
                "rule_text": "只扫当前持仓/重点清单；触发跌破止损、跌破MA20、急跌、冲高回落或尾盘失守才提醒。",
                "priority": "P1",
                "category": "holding",
                "action": "risk_alert",
                "channels": ["feishu", "desktop"],
                "systemic_batch": len(holding_items) >= systemic_batch_threshold,
            }
        )

    watchlist_codes = list(getattr(runtime_config, "watchlist_stock_list", []) or [])
    if watchlist_codes:
        watchlist_items = collect_actionable_stock_items(
            stock_codes=watchlist_codes,
            repo=repo,
            stock_service=stock_service,
            analysis_max_age_days=analysis_max_age_days,
            allowed_actions={"买"},
            watchlist_buy_filter=_build_watchlist_buy_filter(runtime_config),
            bad_tick_max_abs_change_pct=bad_tick_max_abs_change_pct,
            now=current_time,
        )[:max_items]
        groups.append(
            {
                "scope": "watchlist_buy",
                "items": watchlist_items,
                "reminder_title": "自选股盘中买入提醒",
                "simple_alert_title": "自选股买入提醒",
                "rule_text": "只扫描 WATCHLIST_STOCK_LIST；只在尾盘击球区提醒买入信号，其余信号不推送。",
                "priority": "P2",
                "category": "watchlist",
                "action": "buy",
                "channels": ["feishu"],
                "systemic_batch": False,
            }
        )

    sent = False
    suppressed_duplicate = False
    route_results: List[Dict[str, Any]] = []
    group_states: Dict[str, Any] = {}
    all_items: List[StockReminderItem] = []

    for group in groups:
        items = list(group["items"])
        scope = str(group["scope"])
        signal_keys = _build_signal_keys(items)
        item_codes = [item.code for item in items]
        duplicate_group = _is_duplicate_group_signal(
            state=state,
            scope=scope,
            today_iso=today_iso,
            item_codes=item_codes,
            signal_keys=signal_keys,
        )
        suppressed_duplicate = suppressed_duplicate or duplicate_group
        group_sent = False
        route_result: Optional[Dict[str, Any]] = None
        if items and not duplicate_group:
            content = render_stock_intraday_reminder(
                items,
                now=current_time,
                title=str(group["reminder_title"]),
                rule_text=str(group["rule_text"]),
            )
            signal_event = SignalEvent(
                source="stock_intraday_reminder",
                title=str(group["simple_alert_title"]),
                content=content,
                priority=str(group["priority"]),
                category=str(group["category"]),
                action=str(group["action"]),
                reason=str(group["rule_text"]),
                should_notify=True,
                channels=list(group["channels"]),
                dedupe_key=f"stock_intraday_reminder:{scope}:" + "|".join(signal_keys),
                created_at=current_time.isoformat(),
                metadata={
                    "scope": scope,
                    "item_count": len(items),
                    "active_codes": item_codes,
                    "systemic_batch": bool(group["systemic_batch"]),
                },
            )
            route_result = router.dispatch(signal_event, notifier)
            group_sent = bool(route_result.get("sent"))
            sent = sent or group_sent
            route_results.append(route_result)
            if replay_ledger_enabled:
                for item, signal_key in zip(items, signal_keys):
                    _append_replay_ledger(
                        replay_ledger_path,
                        now=current_time,
                        scope=scope,
                        item=item,
                        route_result=route_result,
                        duplicate_group=duplicate_group,
                        signal_key=signal_key,
                    )

        previous_group_state = _state_group(state, scope)
        group_states[scope] = {
            "last_checked_at": current_time.isoformat(),
            "last_sent_at": current_time.isoformat() if group_sent else previous_group_state.get("last_sent_at"),
            "last_sent_date": today_iso if (group_sent or duplicate_group) else previous_group_state.get("last_sent_date"),
            "last_active_codes": item_codes,
            "last_signal_keys": signal_keys if (group_sent or duplicate_group) else previous_group_state.get("last_signal_keys", []),
            "last_suppressed_at": current_time.isoformat() if duplicate_group else previous_group_state.get("last_suppressed_at"),
        }
        all_items.extend(items)

    active_codes = [item.code for item in all_items]
    signal_keys = _build_signal_keys(all_items)
    next_state = {
        "last_checked_at": current_time.isoformat(),
        "market_open": True,
        "last_sent_at": current_time.isoformat() if sent else state.get("last_sent_at"),
        "last_sent_date": today_iso if (sent or suppressed_duplicate) else state.get("last_sent_date"),
        "last_active_codes": active_codes,
        "last_signal_keys": signal_keys if (sent or suppressed_duplicate) else state.get("last_signal_keys", []),
        "last_suppressed_at": current_time.isoformat() if suppressed_duplicate else state.get("last_suppressed_at"),
        "groups": group_states,
    }
    _save_state(state_file, next_state)
    return {
        "market_open": True,
        "item_count": len(all_items),
        "sent": sent,
        "suppressed_duplicate": suppressed_duplicate,
        "state_path": str(state_file),
        "active_codes": active_codes,
        "scope": "mixed" if len(groups) > 1 else (groups[0]["scope"] if groups else ""),
        "group_counts": {str(group["scope"]): len(group["items"]) for group in groups},
        "route_decision": (route_results[-1].get("decision") if route_results else None),
        "route_results": route_results,
        "route_state_path": str(route_state_path),
        "replay_ledger_path": str(replay_ledger_path),
    }
