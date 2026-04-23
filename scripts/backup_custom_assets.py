#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Backup and restore local custom assets outside the upstream baseline."""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import sys
import tarfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable


SCRIPT_PATH = Path(__file__).resolve()
DEFAULT_REPO = SCRIPT_PATH.parents[1]
DEFAULT_MANIFEST = DEFAULT_REPO / "custom_assets.manifest.json"
ARCHIVE_PREFIX = "custom-assets"
PAYLOAD_PREFIX = "payload"
METADATA_NAME = "backup_metadata.json"
MANIFEST_SNAPSHOT_NAME = "manifest_snapshot.json"


@dataclass(frozen=True)
class RestorePlan:
    new_files: list[str]
    changed_files: list[str]
    unchanged_files: list[str]


def _read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _sha256_path(path: Path) -> str:
    return _sha256_bytes(path.read_bytes())


def load_manifest(manifest_path: Path) -> dict:
    payload = _read_json(manifest_path)
    include = payload.get("include")
    if not isinstance(include, list) or not all(isinstance(item, str) for item in include):
        raise ValueError("manifest must contain an 'include' array of string patterns")
    return payload


def resolve_manifest_files(repo: Path, manifest_path: Path) -> list[Path]:
    manifest = load_manifest(manifest_path)
    files: set[Path] = set()
    missing_patterns: list[str] = []

    for pattern in manifest["include"]:
        matched = False
        for candidate in repo.glob(pattern):
            if candidate.is_file():
                files.add(candidate.resolve())
                matched = True
        if not matched:
            missing_patterns.append(pattern)

    if missing_patterns:
        print(
            "[warn] manifest patterns with no matches:\n" + "\n".join(f"  - {pattern}" for pattern in missing_patterns),
            file=sys.stderr,
        )

    return sorted(files)


def _relativize(repo: Path, files: Iterable[Path]) -> list[str]:
    return sorted(str(path.resolve().relative_to(repo.resolve())) for path in files)


def _build_archive_name(now: datetime | None = None) -> str:
    now = now or datetime.now(timezone.utc)
    return f"{ARCHIVE_PREFIX}-{now.strftime('%Y%m%d-%H%M%S')}.tar.gz"


def _git_head(repo: Path) -> str:
    import subprocess

    completed = subprocess.run(
        ["git", "-C", str(repo), "rev-parse", "--short", "HEAD"],
        check=False,
        text=True,
        capture_output=True,
    )
    return (completed.stdout or "").strip() or "unknown"


def create_backup(repo: Path, manifest_path: Path, output_path: Path) -> list[str]:
    repo = repo.resolve()
    files = resolve_manifest_files(repo, manifest_path)
    rel_paths = _relativize(repo, files)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    metadata = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "repo": str(repo),
        "git_head": _git_head(repo),
        "file_count": len(rel_paths),
        "files": rel_paths,
    }
    manifest_snapshot = load_manifest(manifest_path)

    with tarfile.open(output_path, "w:gz") as archive:
        for rel_path in rel_paths:
            archive.add(repo / rel_path, arcname=f"{PAYLOAD_PREFIX}/{rel_path}")

        for name, payload in (
            (METADATA_NAME, metadata),
            (MANIFEST_SNAPSHOT_NAME, manifest_snapshot),
        ):
            raw = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
            info = tarfile.TarInfo(name=name)
            info.size = len(raw)
            archive.addfile(info, io.BytesIO(raw))

    return rel_paths


def _read_archive_payload(archive_path: Path) -> dict[str, bytes]:
    payload: dict[str, bytes] = {}
    with tarfile.open(archive_path, "r:gz") as archive:
        for member in archive.getmembers():
            if not member.isfile():
                continue
            if not member.name.startswith(f"{PAYLOAD_PREFIX}/"):
                continue
            rel_path = member.name[len(f"{PAYLOAD_PREFIX}/") :]
            extracted = archive.extractfile(member)
            if extracted is None:
                continue
            payload[rel_path] = extracted.read()
    return payload


def build_restore_plan(archive_path: Path, target_repo: Path) -> RestorePlan:
    target_repo = target_repo.resolve()
    payload = _read_archive_payload(archive_path)
    new_files: list[str] = []
    changed_files: list[str] = []
    unchanged_files: list[str] = []

    for rel_path, raw in sorted(payload.items()):
        target_path = target_repo / rel_path
        if not target_path.exists():
            new_files.append(rel_path)
            continue
        if _sha256_path(target_path) == _sha256_bytes(raw):
            unchanged_files.append(rel_path)
        else:
            changed_files.append(rel_path)

    return RestorePlan(
        new_files=new_files,
        changed_files=changed_files,
        unchanged_files=unchanged_files,
    )


def apply_restore(archive_path: Path, target_repo: Path, *, overwrite: bool) -> RestorePlan:
    plan = build_restore_plan(archive_path, target_repo)
    if plan.changed_files and not overwrite:
        raise ValueError("restore would overwrite existing files; rerun with --overwrite after review")

    payload = _read_archive_payload(archive_path)
    for rel_path, raw in payload.items():
        destination = target_repo / rel_path
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(raw)
    return plan


def _print_rel_paths(rel_paths: list[str]) -> None:
    for rel_path in rel_paths:
        print(rel_path)


def _print_restore_plan(plan: RestorePlan) -> None:
    print(f"[summary] new={len(plan.new_files)} changed={len(plan.changed_files)} unchanged={len(plan.unchanged_files)}")
    if plan.new_files:
        print("[new]")
        _print_rel_paths(plan.new_files)
    if plan.changed_files:
        print("[changed]")
        _print_rel_paths(plan.changed_files)
    if plan.unchanged_files:
        print("[unchanged]")
        _print_rel_paths(plan.unchanged_files)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Backup or restore local custom assets so they can be reapplied after syncing upstream."
    )
    parser.add_argument(
        "--repo",
        default=str(DEFAULT_REPO),
        help="Repository root. Defaults to the current project root.",
    )
    parser.add_argument(
        "--manifest",
        default=str(DEFAULT_MANIFEST),
        help="Manifest JSON describing custom assets to backup.",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    list_parser = subparsers.add_parser("list", help="List files matched by the custom asset manifest.")
    list_parser.set_defaults(command_name="list")

    backup_parser = subparsers.add_parser("backup", help="Create a tar.gz archive for custom assets.")
    backup_parser.add_argument(
        "--output",
        default="",
        help="Archive path. Defaults to backups/custom-assets-<timestamp>.tar.gz under the repo.",
    )

    restore_parser = subparsers.add_parser("restore", help="Restore custom assets from a backup archive.")
    restore_parser.add_argument("archive", help="Path to a .tar.gz archive created by this script.")
    restore_parser.add_argument(
        "--target-repo",
        default="",
        help="Target repo path to restore into. Defaults to --repo.",
    )
    restore_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview restore results without modifying files.",
    )
    restore_parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Allow overwriting files that differ from the backup.",
    )

    return parser


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()
    repo = Path(args.repo).resolve()
    manifest_path = Path(args.manifest).resolve()

    try:
        if args.command == "list":
            rel_paths = _relativize(repo, resolve_manifest_files(repo, manifest_path))
            _print_rel_paths(rel_paths)
            return 0

        if args.command == "backup":
            output = (
                Path(args.output).resolve()
                if args.output
                else repo / "backups" / _build_archive_name()
            )
            rel_paths = create_backup(repo, manifest_path, output)
            print(f"[backup] {output}")
            print(f"[files]  {len(rel_paths)}")
            return 0

        if args.command == "restore":
            archive_path = Path(args.archive).resolve()
            target_repo = Path(args.target_repo).resolve() if args.target_repo else repo
            plan = build_restore_plan(archive_path, target_repo)
            _print_restore_plan(plan)
            if args.dry_run:
                return 0
            apply_restore(archive_path, target_repo, overwrite=args.overwrite)
            print("[result] restore completed")
            return 0

        parser.error("unknown command")
        return 2
    except (OSError, ValueError, json.JSONDecodeError, tarfile.TarError) as exc:
        print(f"[error] {exc}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
