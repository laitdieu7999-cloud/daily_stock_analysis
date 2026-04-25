"""DB-first K-line history loader for Agent tools."""

from __future__ import annotations

import contextvars
import logging
from datetime import date, timedelta
from threading import Lock
from typing import Optional, Tuple

import pandas as pd

logger = logging.getLogger(__name__)

_frozen_target_date: contextvars.ContextVar[Optional[date]] = contextvars.ContextVar(
    "_frozen_target_date",
    default=None,
)


def set_frozen_target_date(value: date) -> contextvars.Token:
    return _frozen_target_date.set(value)


def get_frozen_target_date() -> Optional[date]:
    return _frozen_target_date.get()


def reset_frozen_target_date(token: contextvars.Token) -> None:
    _frozen_target_date.reset(token)


_fetcher_singleton = None
_fetcher_lock = Lock()


def _get_fetcher_manager():
    global _fetcher_singleton
    if _fetcher_singleton is None:
        with _fetcher_lock:
            if _fetcher_singleton is None:
                from data_provider import DataFetcherManager

                _fetcher_singleton = DataFetcherManager()
    return _fetcher_singleton


def load_history_df(
    stock_code: str,
    days: int = 60,
    target_date: Optional[date] = None,
) -> Tuple[Optional[pd.DataFrame], str]:
    """Load K-line history from DB first, then fall back to fetchers."""
    from data_provider.base import canonical_stock_code, normalize_stock_code
    from src.storage import get_db

    if target_date is not None:
        end = target_date
    else:
        frozen = get_frozen_target_date()
        end = frozen if frozen else date.today()

    start = end - timedelta(days=int(days * 1.8) + 10)
    code = canonical_stock_code(stock_code)
    normalized_code = canonical_stock_code(normalize_stock_code(stock_code))
    candidates = []
    for candidate in (code, normalized_code):
        if candidate and candidate not in candidates:
            candidates.append(candidate)

    try:
        db = get_db()
        ranked_candidates = []
        min_required = days if days <= 5 else max(int(days * 0.3), 5)
        for candidate in candidates:
            bars = db.get_data_range(candidate, start, end)
            if not bars:
                continue
            latest = max(getattr(bar, "date", None) for bar in bars)
            ranked_candidates.append(
                (
                    latest == end,
                    latest,
                    len(bars),
                    candidate == normalized_code,
                    candidate,
                    bars,
                )
            )

        if ranked_candidates:
            _is_fresh, latest, _count, _preferred, best_code, bars = max(ranked_candidates)
            if latest == end and len(bars) >= min_required:
                df = pd.DataFrame([bar.to_dict() for bar in bars])
                if "code" in df.columns:
                    df["code"] = best_code
                logger.debug(
                    "load_history_df(%s): %d bars from DB candidate=%s (requested %d)",
                    stock_code,
                    len(df),
                    best_code,
                    days,
                )
                return df, "db_cache"
            logger.debug(
                "load_history_df(%s): DB cache stale or insufficient (latest=%s, requested_end=%s, min_required=%s)",
                stock_code,
                latest,
                end,
                min_required,
            )
    except Exception as exc:
        logger.debug("load_history_df(%s): DB read failed: %s", stock_code, exc)

    try:
        manager = _get_fetcher_manager()
        df, source = manager.get_daily_data(stock_code, days=days)
        if df is not None and not df.empty:
            return df, source
    except Exception as exc:
        logger.warning("load_history_df(%s): DataFetcherManager failed: %s", stock_code, exc)

    return None, "none"
