#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Migrate existing JSONL/Markdown archives into local SQLite storage."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.local_storage import DEFAULT_DATABASE_PATH, DEFAULT_DOCUMENT_ROOT, init_local_storage  # noqa: E402


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Migrate existing local archives into SQLite-backed storage.")
    parser.add_argument(
        "--database-path",
        default=str(DEFAULT_DATABASE_PATH),
        help="SQLite database path.",
    )
    parser.add_argument(
        "--document-root",
        default=str(DEFAULT_DOCUMENT_ROOT),
        help="Local document storage root.",
    )
    parser.add_argument(
        "--feishu-audit-path",
        default=str(PROJECT_ROOT / "reports" / "feishu_push_audit.jsonl"),
        help="Feishu push audit JSONL path.",
    )
    parser.add_argument(
        "--metaphysical-daily-archive-dir",
        default=str(PROJECT_ROOT / "reports" / "metaphysical_daily_archive"),
        help="Metaphysical daily Markdown archive directory.",
    )
    parser.add_argument(
        "--learning-snapshot-path",
        default=str(PROJECT_ROOT / "reports" / "metaphysical_learning_samples.jsonl"),
        help="Metaphysical learning sample JSONL path.",
    )
    parser.add_argument("--json", action="store_true", help="Emit JSON output.")
    return parser


def main() -> int:
    args = _build_parser().parse_args()
    storage = init_local_storage(
        database_path=args.database_path,
        document_root=args.document_root,
    )
    payload = {
        "database_path": str(storage.database_path),
        "document_root": str(storage.document_root),
        "feishu_push_audit": storage.import_feishu_push_audit(args.feishu_audit_path),
        "metaphysical_daily_archives": storage.import_metaphysical_daily_archives(args.metaphysical_daily_archive_dir),
        "metaphysical_learning_samples": storage.import_metaphysical_learning_samples(args.learning_snapshot_path),
    }
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print("本地存储迁移完成")
        print(f"database_path: {payload['database_path']}")
        print(f"document_root: {payload['document_root']}")
        print(f"feishu_push_audit: {payload['feishu_push_audit']}")
        print(f"metaphysical_daily_archives: {payload['metaphysical_daily_archives']}")
        print(f"metaphysical_learning_samples: {payload['metaphysical_learning_samples']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
