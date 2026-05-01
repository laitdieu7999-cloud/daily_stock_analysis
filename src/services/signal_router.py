from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional


PRIORITY_ORDER = {"P0": 0, "P1": 1, "P2": 2, "P3": 3, "P4": 4}
BUY_ACTIONS = {"buy", "买", "买入", "watchlist_buy"}
RISK_ACTIONS = {"alert", "risk_alert", "sell", "卖", "卖出", "reduce", "减仓"}


def _normalize_priority(value: str) -> str:
    priority = str(value or "").strip().upper()
    if priority not in PRIORITY_ORDER:
        raise ValueError(f"invalid signal priority: {value!r}")
    return priority


def _normalize_list(values: Optional[Iterable[str]]) -> List[str]:
    if not values:
        return []
    return [str(value).strip() for value in values if str(value).strip()]


@dataclass
class SignalEvent:
    """Canonical signal contract shared by strategies and notification routing.

    Strategy modules should produce this object, then let SignalRouter decide
    whether the user should be interrupted. This keeps "signal generation" and
    "notification routing" physically separated.
    """

    source: str
    title: str
    content: str
    priority: str
    category: str
    action: str
    symbol: Optional[str] = None
    name: Optional[str] = None
    reason: str = ""
    should_notify: Optional[bool] = None
    channels: List[str] = field(default_factory=list)
    dedupe_key: str = ""
    created_at: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.priority = _normalize_priority(self.priority)
        self.channels = _normalize_list(self.channels)
        if self.created_at is None:
            self.created_at = datetime.now().isoformat(timespec="seconds")
        if not self.dedupe_key:
            parts = [self.source, self.category, self.symbol or "", self.action, self.reason]
            self.dedupe_key = ":".join(str(part) for part in parts if str(part))

    def to_dict(self) -> Dict[str, Any]:
        return {
            "source": self.source,
            "symbol": self.symbol,
            "name": self.name,
            "priority": self.priority,
            "category": self.category,
            "action": self.action,
            "title": self.title,
            "content": self.content,
            "reason": self.reason,
            "should_notify": self.should_notify,
            "channels": list(self.channels),
            "dedupe_key": self.dedupe_key,
            "created_at": self.created_at,
            "metadata": dict(self.metadata),
        }

    @classmethod
    def from_dict(cls, payload: Dict[str, Any]) -> "SignalEvent":
        return cls(
            source=str(payload.get("source") or ""),
            title=str(payload.get("title") or ""),
            content=str(payload.get("content") or ""),
            priority=str(payload.get("priority") or ""),
            category=str(payload.get("category") or ""),
            action=str(payload.get("action") or ""),
            symbol=payload.get("symbol"),
            name=payload.get("name"),
            reason=str(payload.get("reason") or ""),
            should_notify=payload.get("should_notify"),
            channels=_normalize_list(payload.get("channels")),
            dedupe_key=str(payload.get("dedupe_key") or ""),
            created_at=payload.get("created_at"),
            metadata=payload.get("metadata") if isinstance(payload.get("metadata"), dict) else {},
        )


@dataclass
class RouteDecision:
    should_notify: bool
    should_archive: bool
    alert_type: str
    channels: List[str]
    reason: str

    def to_dict(self) -> Dict[str, Any]:
        return {
            "should_notify": self.should_notify,
            "should_archive": self.should_archive,
            "alert_type": self.alert_type,
            "channels": list(self.channels),
            "reason": self.reason,
        }


class SignalRouter:
    """Central routing rules for all user-facing signals."""

    def __init__(
        self,
        *,
        state_path: str | Path | None = None,
        now: Optional[datetime] = None,
        p1_cooldown_minutes: int = 30,
        p2_daily_limit: int = 1,
    ) -> None:
        self.state_path = Path(state_path) if state_path is not None else None
        self.now = now
        self.p1_cooldown_minutes = max(0, int(p1_cooldown_minutes))
        self.p2_daily_limit = max(1, int(p2_daily_limit))

    def route(self, event: SignalEvent) -> RouteDecision:
        priority = _normalize_priority(event.priority)
        category = str(event.category or "").strip().lower()
        action = str(event.action or "").strip().lower()
        channels = event.channels or self._default_channels(priority, category)

        if priority in {"P0", "P1"}:
            return RouteDecision(
                should_notify=True,
                should_archive=True,
                alert_type="error" if priority == "P0" else "warning",
                channels=channels,
                reason=f"{priority} risk signal must interrupt",
            )

        if priority == "P2":
            allowed = category == "watchlist" and action in BUY_ACTIONS
            return RouteDecision(
                should_notify=bool(allowed and event.should_notify is not False),
                should_archive=True,
                alert_type="warning",
                channels=channels,
                reason="watchlist buy signal" if allowed else "P2 non-buy signal suppressed",
            )

        return RouteDecision(
            should_notify=False,
            should_archive=True,
            alert_type="info",
            channels=channels,
            reason=f"{priority} is archive/shadow only",
        )

    def dispatch(self, event: SignalEvent, notifier: Any) -> Dict[str, Any]:
        """Route and send one signal through the existing notification service.

        The current notification service still fans out to configured channels.
        `channels` is kept in the route decision so the contract is explicit and
        future channel-specific delivery can be added without changing strategy
        modules again.
        """

        decision = self.route(event)
        policy_allowed = True
        policy_reason = ""
        state: Dict[str, Any] = {}
        if decision.should_notify and self.state_path is not None:
            state = self._load_policy_state()
            policy_allowed, policy_reason = self._apply_delivery_policy(event, state)
            if not policy_allowed:
                decision.should_notify = False
                decision.reason = f"{decision.reason}; {policy_reason}"

        sent = False
        if decision.should_notify:
            from src.notification import NotificationBuilder

            payload = NotificationBuilder.build_simple_alert(
                title=event.title,
                content=event.content,
                alert_type=decision.alert_type,
            )
            sent = bool(notifier.send(payload))
            if sent and self.state_path is not None:
                self._record_delivery(event, state)
                self._save_policy_state(state)

        return {
            "sent": sent,
            "policy_allowed": policy_allowed,
            "policy_reason": policy_reason,
            "event": event.to_dict(),
            "decision": decision.to_dict(),
        }

    @staticmethod
    def _default_channels(priority: str, category: str) -> List[str]:
        if priority in {"P0", "P1"}:
            return ["feishu", "desktop"]
        if priority == "P2":
            return ["feishu"]
        return []

    def _current_time(self) -> datetime:
        return self.now or datetime.now()

    def _load_policy_state(self) -> Dict[str, Any]:
        if self.state_path is None or not self.state_path.exists():
            return {"schema_version": 1, "deliveries": {}}
        try:
            payload = json.loads(self.state_path.read_text(encoding="utf-8"))
        except Exception:
            return {"schema_version": 1, "deliveries": {}}
        if not isinstance(payload, dict):
            return {"schema_version": 1, "deliveries": {}}
        deliveries = payload.get("deliveries")
        if not isinstance(deliveries, dict):
            payload["deliveries"] = {}
        payload.setdefault("schema_version", 1)
        return payload

    def _save_policy_state(self, state: Dict[str, Any]) -> None:
        if self.state_path is None:
            return
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        state["updated_at"] = self._current_time().isoformat(timespec="seconds")
        tmp_path = self.state_path.with_name(f".{self.state_path.name}.{os.getpid()}.tmp")
        tmp_path.write_text(
            json.dumps(state, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        tmp_path.replace(self.state_path)

    def _delivery_key(self, event: SignalEvent) -> str:
        priority = _normalize_priority(event.priority)
        if priority == "P1":
            identity = event.symbol or event.metadata.get("scope") or event.category or event.source
            return f"P1:{event.source}:{identity}:{event.action}"
        if priority == "P2":
            active_codes = event.metadata.get("active_codes")
            if event.symbol:
                identity = event.symbol
            elif isinstance(active_codes, list) and len(active_codes) == 1:
                identity = str(active_codes[0])
            else:
                identity = event.metadata.get("scope") or event.dedupe_key or event.title
            return f"P2:{self._current_time().date().isoformat()}:{identity}:{event.action}"
        return f"{priority}:{event.dedupe_key or event.title}"

    @staticmethod
    def _parse_time(value: Any) -> Optional[datetime]:
        text = str(value or "").strip()
        if not text:
            return None
        try:
            return datetime.fromisoformat(text)
        except ValueError:
            return None

    def _apply_delivery_policy(self, event: SignalEvent, state: Dict[str, Any]) -> tuple[bool, str]:
        priority = _normalize_priority(event.priority)
        if priority == "P0":
            return True, ""

        deliveries = state.setdefault("deliveries", {})
        key = self._delivery_key(event)
        record = deliveries.get(key) if isinstance(deliveries.get(key), dict) else {}
        now = self._current_time()

        if priority == "P1" and self.p1_cooldown_minutes > 0:
            last_sent_at = self._parse_time(record.get("last_sent_at"))
            if last_sent_at and now - last_sent_at < timedelta(minutes=self.p1_cooldown_minutes):
                return False, f"P1 cooldown active for {self.p1_cooldown_minutes} minutes"

        if priority == "P2":
            record_date = str(record.get("date") or "")
            count = int(record.get("count") or 0) if record_date == now.date().isoformat() else 0
            if count >= self.p2_daily_limit:
                return False, f"P2 daily limit reached ({self.p2_daily_limit})"

        return True, ""

    def _record_delivery(self, event: SignalEvent, state: Dict[str, Any]) -> None:
        deliveries = state.setdefault("deliveries", {})
        key = self._delivery_key(event)
        now = self._current_time()
        previous = deliveries.get(key) if isinstance(deliveries.get(key), dict) else {}
        previous_date = str(previous.get("date") or "")
        count = int(previous.get("count") or 0) if previous_date == now.date().isoformat() else 0
        deliveries[key] = {
            "date": now.date().isoformat(),
            "count": count + 1,
            "last_sent_at": now.isoformat(timespec="seconds"),
            "priority": event.priority,
            "symbol": event.symbol,
            "source": event.source,
            "title": event.title,
        }


def append_signal_event_archive(
    event: SignalEvent,
    *,
    archive_path: str | Path,
    router: Optional[SignalRouter] = None,
) -> Dict[str, Any]:
    """Append a routed signal event to a JSONL archive."""

    route = (router or SignalRouter()).route(event)
    payload = {
        "archived_at": datetime.now().isoformat(timespec="seconds"),
        "event": event.to_dict(),
        "decision": route.to_dict(),
    }
    path = Path(archive_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=False, sort_keys=True) + "\n")
    return {
        "archive_path": str(path),
        "event": event.to_dict(),
        "decision": route.to_dict(),
    }
