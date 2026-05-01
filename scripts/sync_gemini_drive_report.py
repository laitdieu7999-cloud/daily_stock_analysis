#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Sync the latest Google Drive Gemini report into the local cache."""

from __future__ import annotations

import argparse
import contextlib
import json
import signal
import sys
from datetime import date
from pathlib import Path

import browser_cookie3
import requests

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.models.metaphysical import (  # noqa: E402
    extract_tactical_report_date_iso,
    sync_tactical_report_cache,
    sync_tactical_report_text,
)

PREFERRED_TITLE_SCORES = (
    ("战区中枢", 100),
    ("战术分析报告", 80),
    ("金融战术", 60),
    ("战术归档", 40),
)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Sync the latest Google Drive Gemini report.")
    parser.add_argument(
        "--source-file",
        default=None,
        help="Optional Google Drive source file or directory. Supports .gdoc/.md/.txt. Defaults to auto-detect.",
    )
    parser.add_argument(
        "--target-file",
        default=str(PROJECT_ROOT / "reports" / "gemini_daily.md"),
        help="Local cache file used by the scheduler.",
    )
    parser.add_argument(
        "--archive-dir",
        default=str(PROJECT_ROOT / "reports" / "gemini_daily_archive"),
        help="Archive directory for normalized copies.",
    )
    parser.add_argument(
        "--expected-report-date",
        default=None,
        help="Expected report date in YYYY-MM-DD format. Defaults to today.",
    )
    parser.add_argument(
        "--export-timeout-seconds",
        type=int,
        default=45,
        help="Maximum seconds allowed for Google Doc export, including browser-cookie lookup.",
    )
    parser.add_argument("--json", action="store_true", help="Emit JSON output.")
    return parser


def _auto_detect_google_drive_root() -> Path | None:
    cloud_storage = Path.home() / "Library" / "CloudStorage"
    if not cloud_storage.exists():
        return None
    candidates = sorted(cloud_storage.glob("GoogleDrive-*"))
    if not candidates:
        return None
    root = candidates[0]
    my_drive = root / "我的云端硬盘"
    return my_drive if my_drive.exists() else root


def _iter_candidate_files(root: Path) -> list[Path]:
    patterns = ("*.gdoc", "*.md", "*.txt")
    candidates: list[Path] = []
    for pattern in patterns:
        candidates.extend(root.glob(pattern))
        candidates.extend(root.glob(f"*/*/{pattern}"))
        candidates.extend(root.glob(f"*/{pattern}"))
    unique: dict[str, Path] = {}
    for item in candidates:
        if item.is_file():
            unique[str(item)] = item
    return list(unique.values())


def _title_score(path: Path) -> int:
    name = path.stem
    for keyword, score in PREFERRED_TITLE_SCORES:
        if keyword in name:
            return score
    return 0


def _candidate_sort_key(path: Path) -> tuple[int, float, str]:
    return (_title_score(path), path.stat().st_mtime, path.name)


def _build_selection_metadata(
    candidates: list[Path],
    *,
    expected_report_date: str | None,
) -> dict[str, object]:
    if not candidates:
        return {
            "selection_policy": "prefer_same_day_then_title_score_then_mtime",
            "same_day_candidate_count": 0,
            "same_day_candidates": [],
            "selected_from_same_day": False,
            "selected_report_date_iso": None,
            "selected_candidate_name": None,
            "selection_reason": "Google Drive 里没有找到可用的 Gemini 日报。",
        }

    dated_candidates = []
    for item in candidates:
        report_date = extract_tactical_report_date_iso(item.stem) or ""
        dated_candidates.append((item, report_date))

    same_day = [
        item
        for item, report_date in dated_candidates
        if expected_report_date and report_date == expected_report_date
    ]
    if same_day:
        selected = max(same_day, key=_candidate_sort_key)
        selection_reason = (
            f"今天找到 {len(same_day)} 份 Gemini 归档，已按固定规则选用《{selected.stem}》。"
            if len(same_day) > 1
            else f"今天找到 1 份 Gemini 归档，直接选用《{selected.stem}》。"
        )
        return {
            "selection_policy": "prefer_same_day_then_title_score_then_mtime",
            "same_day_candidate_count": len(same_day),
            "same_day_candidates": [str(item) for item in sorted(same_day)],
            "selected_from_same_day": True,
            "selected_report_date_iso": expected_report_date,
            "selected_candidate_name": selected.name,
            "selected_candidate_path": str(selected),
            "selection_reason": selection_reason,
            "selected_file": selected,
        }

    selected, selected_date = max(
        dated_candidates,
        key=lambda pair: (pair[1],) + _candidate_sort_key(pair[0]),
    )
    selected_date_text = selected_date or "未知日期"
    return {
        "selection_policy": "prefer_same_day_then_title_score_then_mtime",
        "same_day_candidate_count": 0,
        "same_day_candidates": [],
        "selected_from_same_day": False,
        "selected_report_date_iso": selected_date or None,
        "selected_candidate_name": selected.name,
        "selected_candidate_path": str(selected),
        "selection_reason": (
            f"今天没有新的 Gemini 归档，当前最新一份是 {selected_date_text} 的《{selected.stem}》。"
        ),
        "selected_file": selected,
    }


def _resolve_source_candidate(source_hint: str | None, *, expected_report_date: str | None) -> tuple[Path | None, dict[str, object]]:
    if source_hint:
        source = Path(source_hint).expanduser()
        if source.is_file():
            selection = _build_selection_metadata([source], expected_report_date=expected_report_date)
            selection["selected_file"] = source
            return source, selection
        if source.is_dir():
            candidates = _iter_candidate_files(source)
        else:
            return None, _build_selection_metadata([], expected_report_date=expected_report_date)
    else:
        root = _auto_detect_google_drive_root()
        if root is None:
            return None, _build_selection_metadata([], expected_report_date=expected_report_date)
        candidates = _iter_candidate_files(root)

    if not candidates:
        return None, _build_selection_metadata([], expected_report_date=expected_report_date)

    selection = _build_selection_metadata(candidates, expected_report_date=expected_report_date)
    selected = selection.get("selected_file")
    return (selected if isinstance(selected, Path) else None), selection


@contextlib.contextmanager
def _deadline(seconds: int):
    if seconds <= 0:
        yield
        return

    def _raise_timeout(_signum, _frame):
        raise TimeoutError(f"Google Doc 导出超过 {seconds} 秒")

    previous = signal.signal(signal.SIGALRM, _raise_timeout)
    signal.alarm(seconds)
    try:
        yield
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, previous)


def _export_google_doc_text(source_file: Path, *, timeout_seconds: int) -> tuple[str, str]:
    metadata = json.loads(source_file.read_text(encoding="utf-8"))
    doc_id = str(metadata.get("doc_id") or "").strip()
    if not doc_id:
        raise ValueError(f"Google Doc 指针文件缺少 doc_id: {source_file}")
    url = f"https://docs.google.com/document/d/{doc_id}/export?format=txt"
    with _deadline(timeout_seconds):
        cookies = browser_cookie3.chrome(domain_name=".google.com")
        response = requests.get(url, cookies=cookies, timeout=min(30, max(1, timeout_seconds)))
        response.raise_for_status()
        return response.text, doc_id


def _sync_from_source(
    source_file: Path,
    *,
    target_file: str,
    archive_dir: str,
    expected_report_date: str,
    export_timeout_seconds: int,
) -> dict[str, object]:
    suffix = source_file.suffix.lower()
    if suffix == ".gdoc":
        text, doc_id = _export_google_doc_text(source_file, timeout_seconds=export_timeout_seconds)
        payload = sync_tactical_report_text(
            text=text,
            source_label=f"{source_file}#doc_id={doc_id}",
            target_path=target_file,
            archive_dir=archive_dir,
            filename_label="gemini_daily",
            expected_date_iso=expected_report_date,
        )
        payload["resolved_source_file"] = str(source_file)
        payload["source_type"] = "gdoc"
        payload["doc_id"] = doc_id
        return payload
    payload = sync_tactical_report_cache(
        source_path=source_file,
        target_path=target_file,
        archive_dir=archive_dir,
        filename_label="gemini_daily",
        expected_date_iso=expected_report_date,
    )
    payload["resolved_source_file"] = str(source_file)
    payload["source_type"] = source_file.suffix.lower().lstrip(".") or "text"
    return payload


def main() -> int:
    args = _build_parser().parse_args()
    expected_report_date = args.expected_report_date or date.today().isoformat()
    source_file, selection = _resolve_source_candidate(
        args.source_file,
        expected_report_date=expected_report_date,
    )
    if source_file is None:
        payload = {
            "status": "missing_source",
            "source_path": args.source_file or "auto-detect",
            "target_path": str(Path(args.target_file)),
            "resolved_source_file": None,
            "synced": False,
            "freshness": {
                "report_date_iso": None,
                "expected_report_date_iso": expected_report_date,
                "is_fresh": False,
                "is_stale": True,
                "freshness_reason": "今天没有找到新的 Gemini 归档，今天不启用文本纠偏。",
            },
        }
    elif args.source_file is None and not bool(selection.get("selected_from_same_day")):
        payload = {
            "status": "stale_source",
            "source_path": str(source_file),
            "target_path": str(Path(args.target_file)),
            "resolved_source_file": str(source_file),
            "synced": False,
            "freshness": {
                "report_date_iso": selection.get("selected_report_date_iso"),
                "expected_report_date_iso": expected_report_date,
                "is_fresh": False,
                "is_stale": True,
                "freshness_reason": str(selection.get("selection_reason") or "今天没有新的 Gemini 归档，今天不启用文本纠偏。"),
            },
        }
    else:
        try:
            payload = _sync_from_source(
                source_file,
                target_file=args.target_file,
                archive_dir=args.archive_dir,
                expected_report_date=expected_report_date,
                export_timeout_seconds=args.export_timeout_seconds,
            )
        except Exception as exc:
            payload = {
                "status": "sync_error",
                "source_path": str(source_file),
                "target_path": str(Path(args.target_file)),
                "resolved_source_file": str(source_file),
                "synced": False,
                "error": str(exc),
                "freshness": {
                    "report_date_iso": selection.get("selected_report_date_iso"),
                    "expected_report_date_iso": expected_report_date,
                    "is_fresh": False,
                    "is_stale": True,
                    "freshness_reason": f"Google Drive 归档导出失败，今天不启用文本纠偏：{exc}",
                },
            }
        if payload.get("status") != "sync_error" and not bool((payload.get("freshness") or {}).get("is_fresh")):
            payload["freshness"]["freshness_reason"] = str(selection.get("selection_reason") or payload["freshness"]["freshness_reason"])

    selection.pop("selected_file", None)
    payload["selection"] = selection

    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        freshness = dict(payload.get("freshness") or {})
        print(f"status: {payload.get('status')}")
        print(f"source: {payload.get('source_path')}")
        print(f"resolved_source_file: {payload.get('resolved_source_file')}")
        print(f"target: {payload.get('target_path')}")
        print(f"fresh: {freshness.get('is_fresh')}")
        print(f"reason: {freshness.get('freshness_reason')}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
