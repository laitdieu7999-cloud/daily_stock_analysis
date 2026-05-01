# -*- coding: utf-8 -*-
"""Safe cleanup for logs and disposable local caches."""

from __future__ import annotations

import json
import os
import shutil
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from src.config import get_config


PROJECT_ROOT = Path(__file__).resolve().parents[2]


class WorkstationCleanupService:
    """Remove old logs and temporary cache files without touching user data."""

    def __init__(self, *, config: Any = None, project_root: str | Path | None = None) -> None:
        self.config = config or get_config()
        self.project_root = Path(project_root) if project_root is not None else self._default_project_root()

    def run(self, *, dry_run: bool = False, now: Optional[datetime] = None) -> Dict[str, Any]:
        current = now or datetime.now()
        log_retention_days = max(1, int(getattr(self.config, "workstation_cleanup_log_retention_days", 14)))
        cache_retention_days = max(1, int(getattr(self.config, "workstation_cleanup_cache_retention_days", 30)))
        log_cutoff = current - timedelta(days=log_retention_days)
        cache_cutoff = current - timedelta(days=cache_retention_days)

        items: List[Dict[str, Any]] = []
        for path in self._log_roots():
            items.extend(self._cleanup_tree(path, cutoff=log_cutoff, dry_run=dry_run, category="log"))
        for path in self._cache_roots():
            items.extend(self._cleanup_tree(path, cutoff=cache_cutoff, dry_run=dry_run, category="cache"))

        payload = {
            "generated_at": current.isoformat(timespec="seconds"),
            "status": "ok",
            "dry_run": bool(dry_run),
            "log_retention_days": log_retention_days,
            "cache_retention_days": cache_retention_days,
            "totals": self._totals(items),
            "items": items,
        }
        payload["ledger_path"] = self._write_ledger(payload)
        return payload

    def latest_run(self) -> Dict[str, Any]:
        ledger_dir = self._ledger_dir()
        if not ledger_dir.exists():
            return {"status": "missing", "path": str(ledger_dir), "generated_at": None, "totals": {}}
        candidates = [path for path in ledger_dir.glob("*_workstation_cleanup.jsonl") if path.is_file()]
        if not candidates:
            return {"status": "missing", "path": str(ledger_dir), "generated_at": None, "totals": {}}
        latest = max(candidates, key=lambda item: item.stat().st_mtime)
        last_payload = None
        for line in latest.read_text(encoding="utf-8", errors="ignore").splitlines():
            if not line.strip():
                continue
            try:
                last_payload = json.loads(line)
            except json.JSONDecodeError:
                continue
        if not last_payload:
            return {"status": "unreadable", "path": str(latest), "generated_at": None, "totals": {}}
        return {
            "status": last_payload.get("status", "unknown"),
            "path": str(latest),
            "generated_at": last_payload.get("generated_at"),
            "totals": last_payload.get("totals") or {},
        }

    def _log_roots(self) -> List[Path]:
        home = Path.home()
        return self._existing_unique_paths(
            [
                self.project_root / "logs",
                Path(str(getattr(self.config, "log_dir", "") or "")).expanduser(),
                home / "Library" / "Application Support" / "Daily Stock Analysis" / "logs",
            ]
        )

    def _cache_roots(self) -> List[Path]:
        return self._existing_unique_paths(
            [
                self.project_root / ".cache",
                self.project_root / "data" / "metaphysical_cache",
            ]
        )

    def _cleanup_tree(self, root: Path, *, cutoff: datetime, dry_run: bool, category: str) -> List[Dict[str, Any]]:
        if not root.exists():
            return []
        items: List[Dict[str, Any]] = []
        protected_names = {".git", "node_modules", ".venv", ".venv311", "data", "reports", "strategies"}
        for current_root, dirs, files in os.walk(root, topdown=True):
            dirs[:] = [name for name in dirs if name not in protected_names]
            for name in files:
                path = Path(current_root) / name
                if not self._is_disposable(path, category=category):
                    continue
                item = self._delete_if_old(path, cutoff=cutoff, dry_run=dry_run, category=category)
                if item:
                    items.append(item)

        # Clean empty cache directories after files are removed.
        if category == "cache":
            for current_root, dirs, _files in os.walk(root, topdown=False):
                for name in dirs:
                    path = Path(current_root) / name
                    try:
                        if path.exists() and not any(path.iterdir()):
                            if not dry_run:
                                path.rmdir()
                            items.append(
                                {
                                    "category": "cache_dir",
                                    "path": str(path),
                                    "size_bytes": 0,
                                    "action": "would_delete" if dry_run else "deleted",
                                }
                            )
                    except OSError:
                        continue
        return items

    @staticmethod
    def _is_disposable(path: Path, *, category: str) -> bool:
        if category == "log":
            return path.suffix.lower() in {".log", ".out", ".err", ".txt"} or ".log." in path.name
        if category == "cache":
            lowered_parts = {part.lower() for part in path.parts}
            if lowered_parts & {"__pycache__", ".pytest_cache", ".vite", "tmp", "temp"}:
                return True
            return path.suffix.lower() in {".tmp", ".temp", ".log", ".bak", ".old", ".cache"}
        return False

    @staticmethod
    def _delete_if_old(path: Path, *, cutoff: datetime, dry_run: bool, category: str) -> Optional[Dict[str, Any]]:
        try:
            stat = path.stat()
            modified_at = datetime.fromtimestamp(stat.st_mtime)
        except OSError:
            return None
        if modified_at >= cutoff:
            return None

        action = "would_delete" if dry_run else "deleted"
        if not dry_run:
            try:
                if path.is_dir():
                    shutil.rmtree(path)
                else:
                    path.unlink()
            except OSError as exc:
                return {
                    "category": category,
                    "path": str(path),
                    "size_bytes": int(stat.st_size),
                    "modified_at": modified_at.isoformat(timespec="seconds"),
                    "action": "error",
                    "message": str(exc),
                }

        return {
            "category": category,
            "path": str(path),
            "size_bytes": int(stat.st_size),
            "modified_at": modified_at.isoformat(timespec="seconds"),
            "action": action,
        }

    @staticmethod
    def _totals(items: Iterable[Dict[str, Any]]) -> Dict[str, int]:
        materialized = list(items)
        deleted_items = [item for item in materialized if item.get("action") in {"deleted", "would_delete"}]
        return {
            "scanned_matches": len(materialized),
            "deleted_count": len(deleted_items),
            "freed_bytes": int(sum(int(item.get("size_bytes") or 0) for item in deleted_items)),
            "error_count": len([item for item in materialized if item.get("action") == "error"]),
        }

    @staticmethod
    def _existing_unique_paths(paths: Iterable[Path]) -> List[Path]:
        seen = set()
        result = []
        for raw in paths:
            if not raw:
                continue
            path = raw.expanduser().resolve()
            marker = str(path)
            if marker in seen or not path.exists():
                continue
            seen.add(marker)
            result.append(path)
        return result

    def _write_ledger(self, payload: Dict[str, Any]) -> str:
        ledger_dir = self._ledger_dir()
        ledger_dir.mkdir(parents=True, exist_ok=True)
        path = ledger_dir / f"{date.today().isoformat()}_workstation_cleanup.jsonl"
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, ensure_ascii=False, default=str) + "\n")
        return str(path)

    def _ledger_dir(self) -> Path:
        return self.project_root / "reports" / "workstation_cleanup"

    def _default_project_root(self) -> Path:
        database_path = Path(str(getattr(self.config, "database_path", "") or "")).expanduser().resolve()
        if database_path.parent.name == "data":
            return database_path.parent.parent
        return PROJECT_ROOT
