# -*- coding: utf-8 -*-
"""Tests for safe workstation cleanup."""

from __future__ import annotations

import os
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

from src.services.workstation_cleanup_service import WorkstationCleanupService


class WorkstationCleanupServiceTestCase(unittest.TestCase):
    def test_cleanup_removes_only_old_logs_and_cache(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            log_dir = root / "logs"
            cache_dir = root / ".cache"
            log_dir.mkdir()
            cache_dir.mkdir()
            old_log = log_dir / "old.log"
            new_log = log_dir / "new.log"
            old_cache = cache_dir / "old.tmp"
            report = root / "reports" / "keep.md"
            report.parent.mkdir()

            for path in (old_log, new_log, old_cache, report):
                path.write_text("x", encoding="utf-8")

            now = datetime(2026, 4, 29, 12, 0)
            old_ts = (now - timedelta(days=40)).timestamp()
            new_ts = (now - timedelta(days=1)).timestamp()
            os.utime(old_log, (old_ts, old_ts))
            os.utime(old_cache, (old_ts, old_ts))
            os.utime(new_log, (new_ts, new_ts))

            config = SimpleNamespace(
                log_dir=str(log_dir),
                workstation_cleanup_log_retention_days=14,
                workstation_cleanup_cache_retention_days=30,
            )

            result = WorkstationCleanupService(config=config, project_root=root).run(now=now)

            self.assertEqual(result["status"], "ok")
            self.assertFalse(old_log.exists())
            self.assertFalse(old_cache.exists())
            self.assertTrue(new_log.exists())
            self.assertTrue(report.exists())
            self.assertGreaterEqual(result["totals"]["deleted_count"], 2)
            self.assertTrue(Path(result["ledger_path"]).exists())


if __name__ == "__main__":
    unittest.main()
