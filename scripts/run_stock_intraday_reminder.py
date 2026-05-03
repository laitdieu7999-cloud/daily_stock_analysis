#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Run the stock-only intraday reminder once."""

from __future__ import annotations

import json
import argparse
import os
import signal
import sys
import time as monotonic_time
import traceback
import uuid
from pathlib import Path
from contextlib import contextmanager
from datetime import datetime, time, timedelta
from typing import Any, Dict, Optional

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# The launchd intraday job runs every minute. Avoid a remote LiteLLM cost-map
# fetch on each short-lived process; the bundled local map is enough here.
os.environ.setdefault("LITELLM_LOCAL_MODEL_COST_MAP", "true")

from src.config import get_config  # noqa: E402
from src.core.trading_calendar import get_market_now, is_market_open  # noqa: E402
from src.notification import NotificationBuilder, NotificationService  # noqa: E402
from src.services.stock_intraday_reminder import run_stock_intraday_reminder_cycle  # noqa: E402

HEARTBEAT_PATH = PROJECT_ROOT / "reports" / "stock_intraday_heartbeat.json"
ERROR_LOG_PATH = PROJECT_ROOT / "reports" / "stock_intraday_errors.jsonl"
DEFAULT_TIMEOUT_SECONDS = 55


def _load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _save_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    tmp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    tmp_path.replace(path)


def _append_jsonl(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=False, sort_keys=True) + "\n")


def _write_heartbeat(
    *,
    run_id: str,
    status: str,
    started_at: str,
    details: Optional[Dict[str, Any]] = None,
    heartbeat_path: Path = HEARTBEAT_PATH,
) -> None:
    payload = {
        "schema_version": 1,
        "run_id": run_id,
        "status": status,
        "started_at": started_at,
        "updated_at": datetime.now().isoformat(timespec="seconds"),
        "pid": os.getpid(),
        "details": details or {},
    }
    _save_json(heartbeat_path, payload)


def _append_urgent_error(
    *,
    run_id: str,
    error_type: str,
    message: str,
    started_at: str,
    error_log_path: Path = ERROR_LOG_PATH,
    traceback_text: str = "",
) -> None:
    _append_jsonl(
        error_log_path,
        {
            "schema_version": 1,
            "level": "URGENT",
            "run_id": run_id,
            "error_type": error_type,
            "message": message,
            "started_at": started_at,
            "created_at": datetime.now().isoformat(timespec="seconds"),
            "traceback": traceback_text,
        },
    )


class _IntradayRunTimeout(TimeoutError):
    pass


@contextmanager
def _run_timeout(seconds: int):
    if seconds <= 0 or not hasattr(signal, "SIGALRM"):
        yield
        return

    def _handle_timeout(_signum, _frame):
        raise _IntradayRunTimeout(f"stock intraday reminder exceeded {seconds}s")

    previous_handler = signal.getsignal(signal.SIGALRM)
    signal.signal(signal.SIGALRM, _handle_timeout)
    signal.alarm(seconds)
    try:
        yield
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, previous_handler)


def _parse_hhmm(value: str, default: time) -> time:
    try:
        hour, minute = str(value or "").strip().split(":", 1)
        return time(int(hour), int(minute))
    except Exception:
        return default


@contextmanager
def _single_run_lock(lock_path: Path):
    """Avoid overlapping launchd runs when a data source is slow."""
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    handle = lock_path.open("w", encoding="utf-8")
    try:
        try:
            import fcntl

            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            yield False
            return
        except ImportError:
            # Windows fallback for local manual runs; LaunchAgent runs on macOS.
            pass
        handle.write(str(Path.cwd()))
        handle.flush()
        yield True
    finally:
        try:
            handle.close()
        except Exception:
            pass


def _maybe_send_self_check(
    config: Any,
    *,
    now: Optional[datetime] = None,
    state_path: Optional[Path] = None,
    notifier_factory: Optional[Any] = None,
) -> Dict[str, Any]:
    if not getattr(config, "stock_intraday_self_check_enabled", True):
        return {"sent": False, "reason": "self-check disabled"}
    if not getattr(config, "stock_intraday_reminder_enabled", False):
        return {"sent": False, "reason": "intraday reminder disabled"}

    current_time = get_market_now("cn", now or datetime.now())
    if not is_market_open("cn", current_time.date()):
        return {"sent": False, "reason": "not a CN trading day"}

    check_time = _parse_hhmm(
        str(getattr(config, "stock_intraday_self_check_time", "09:25")),
        time(9, 25),
    )
    scheduled_at = current_time.replace(
        hour=check_time.hour,
        minute=check_time.minute,
        second=0,
        microsecond=0,
    )
    if current_time < scheduled_at or current_time > scheduled_at + timedelta(minutes=5):
        return {"sent": False, "reason": "outside self-check window"}

    state_path = state_path or (PROJECT_ROOT / "reports" / "stock_intraday_self_check_state.json")
    state = _load_json(state_path)
    today = current_time.date().isoformat()
    if state.get("last_sent_date") == today:
        return {"sent": False, "reason": "self-check already sent today"}

    stock_count = len(list(getattr(config, "stock_list", []) or []))
    watchlist_count = len(list(getattr(config, "watchlist_stock_list", []) or []))
    feishu_ready = bool(getattr(config, "feishu_webhook_url", None))
    content = "\n".join(
        [
            f"- 时间: {current_time.strftime('%Y-%m-%d %H:%M')}",
            f"- 持仓监控: {stock_count} 个标的",
            f"- 自选买入池: {watchlist_count} 个标的",
            f"- 监控频率: 60 秒级 LaunchAgent",
            f"- 飞书通道: {'已配置' if feishu_ready else '未配置'}",
            "- 规则: 持仓只推风控；自选只推尾盘买入。",
        ]
    )
    payload = NotificationBuilder.build_simple_alert(
        title="盘中实时监控自检",
        content=content,
        alert_type="success",
    )
    notifier = notifier_factory() if notifier_factory else NotificationService()
    sent = bool(notifier.send(payload))
    _save_json(
        state_path,
        {
            "last_checked_at": current_time.isoformat(),
            "last_sent_date": today if sent else state.get("last_sent_date"),
            "last_sent_at": current_time.isoformat() if sent else state.get("last_sent_at"),
            "stock_count": stock_count,
            "watchlist_count": watchlist_count,
            "feishu_ready": feishu_ready,
        },
    )
    return {
        "sent": sent,
        "state_path": str(state_path),
        "stock_count": stock_count,
        "watchlist_count": watchlist_count,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Run stock intraday reminder once")
    parser.add_argument(
        "--force",
        action="store_true",
        help="Run even when STOCK_INTRADAY_REMINDER_ENABLED is false",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=int,
        default=DEFAULT_TIMEOUT_SECONDS,
        help="Hard timeout for one launchd cycle; set <=0 to disable",
    )
    args = parser.parse_args()

    run_id = str(uuid.uuid4())
    started_at = datetime.now().isoformat(timespec="seconds")
    started_monotonic = monotonic_time.monotonic()
    _write_heartbeat(run_id=run_id, status="running", started_at=started_at)

    try:
        with _run_timeout(args.timeout_seconds):
            config = get_config()
            if not args.force and not getattr(config, "stock_intraday_reminder_enabled", False):
                payload = {
                    "ok": True,
                    "skipped": True,
                    "reason": "STOCK_INTRADAY_REMINDER_ENABLED is false",
                }
                print(json.dumps(payload, ensure_ascii=False))
                _write_heartbeat(
                    run_id=run_id,
                    status="skipped",
                    started_at=started_at,
                    details={**payload, "duration_seconds": round(monotonic_time.monotonic() - started_monotonic, 3)},
                )
                return 0

            lock_path = PROJECT_ROOT / "reports" / "stock_intraday_reminder.lock"
            with _single_run_lock(lock_path) as acquired:
                if not acquired:
                    payload = {
                        "ok": True,
                        "skipped": True,
                        "reason": "previous intraday reminder run is still active",
                    }
                    print(json.dumps(payload, ensure_ascii=False))
                    _write_heartbeat(
                        run_id=run_id,
                        status="skipped",
                        started_at=started_at,
                        details={
                            **payload,
                            "duration_seconds": round(monotonic_time.monotonic() - started_monotonic, 3),
                        },
                    )
                    return 0

                self_check = _maybe_send_self_check(config)
                result = run_stock_intraday_reminder_cycle(
                    state_path=PROJECT_ROOT / "reports" / "stock_intraday_reminder_state.json",
                    config=config,
                )
                payload = {"ok": True, "self_check": self_check, **result}
                print(json.dumps(payload, ensure_ascii=False))
                _write_heartbeat(
                    run_id=run_id,
                    status="ok",
                    started_at=started_at,
                    details={
                        "duration_seconds": round(monotonic_time.monotonic() - started_monotonic, 3),
                        "market_open": result.get("market_open"),
                        "item_count": result.get("item_count"),
                        "sent": result.get("sent"),
                        "self_check_sent": self_check.get("sent"),
                    },
                )
        return 0
    except _IntradayRunTimeout as exc:
        _append_urgent_error(
            run_id=run_id,
            error_type="TIMEOUT",
            message=str(exc),
            started_at=started_at,
        )
        _write_heartbeat(
            run_id=run_id,
            status="timeout",
            started_at=started_at,
            details={"error": str(exc), "duration_seconds": round(monotonic_time.monotonic() - started_monotonic, 3)},
        )
        print(json.dumps({"ok": False, "error": "TIMEOUT", "message": str(exc)}, ensure_ascii=False))
        return 2
    except Exception as exc:
        trace = traceback.format_exc()
        _append_urgent_error(
            run_id=run_id,
            error_type=exc.__class__.__name__,
            message=str(exc),
            started_at=started_at,
            traceback_text=trace,
        )
        _write_heartbeat(
            run_id=run_id,
            status="error",
            started_at=started_at,
            details={"error": str(exc), "duration_seconds": round(monotonic_time.monotonic() - started_monotonic, 3)},
        )
        print(json.dumps({"ok": False, "error": exc.__class__.__name__, "message": str(exc)}, ensure_ascii=False))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
