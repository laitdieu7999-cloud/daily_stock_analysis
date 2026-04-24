#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Safely inspect upstream Git updates without modifying local work.

This script is intentionally conservative:
- it may run `git fetch <remote>`
- it never runs `git pull`, `git merge`, or `git rebase`
- it is safe to use on a dirty working tree
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Iterable, Sequence


DEFAULT_REMOTE = "origin"


@dataclass
class RepoUpdateStatus:
    repo: str
    branch: str
    remote: str
    compare_mode: str
    remote_ref: str
    local_head: str
    remote_head: str
    dirty_count: int
    ahead: int
    behind: int
    state: str
    has_updates: bool
    fetch_skipped: bool
    note: str = ""
    next_step: str = ""


def _run_git(repo: Path, *args: str, check: bool = True) -> str:
    completed = subprocess.run(
        ["git", "-C", str(repo), *args],
        check=check,
        text=True,
        capture_output=True,
    )
    return completed.stdout.strip()


def _safe_int(value: str) -> int:
    try:
        return int(value.strip())
    except (TypeError, ValueError):
        return 0


def _is_git_repo(repo: Path) -> bool:
    return (repo / ".git").exists()


def _discover_repos(root: Path, max_depth: int) -> list[Path]:
    root = root.resolve()
    repos: list[Path] = []

    if _is_git_repo(root):
        repos.append(root)

    for git_dir in root.rglob(".git"):
        try:
            rel_parts = git_dir.relative_to(root).parts
        except ValueError:
            continue
        if len(rel_parts) - 1 > max_depth:
            continue
        if not git_dir.is_dir():
            continue
        repo = git_dir.parent.resolve()
        if repo != root:
            repos.append(repo)

    return sorted(set(repos))


def _resolve_branch(repo: Path, branch: str) -> str:
    resolved = branch or _run_git(repo, "branch", "--show-current")
    if not resolved:
        raise ValueError("could not determine current branch")
    return resolved


def _resolve_remote_ref(repo: Path, branch: str, remote: str) -> tuple[str, str]:
    if remote != DEFAULT_REMOTE:
        return f"{remote}/{branch}", remote
    upstream = _run_git(repo, "rev-parse", "--abbrev-ref", f"{branch}@{{upstream}}", check=False).strip()
    if upstream and upstream != f"{branch}@{{upstream}}":
        remote_name = upstream.split("/", 1)[0]
        return upstream, remote_name
    return f"{remote}/{branch}", remote


def _resolve_latest_tag(repo: Path) -> str:
    latest = _run_git(repo, "tag", "--list", "v*", "--sort=-version:refname")
    tags = [line.strip() for line in latest.splitlines() if line.strip()]
    if not tags:
        latest = _run_git(repo, "tag", "--sort=-version:refname")
        tags = [line.strip() for line in latest.splitlines() if line.strip()]
    if not tags:
        raise ValueError("could not determine latest release tag")
    return tags[0]


def _build_note(dirty_count: int, state: str, remote_ref: str, compare_mode: str) -> tuple[str, str]:
    notes: list[str] = []
    if dirty_count:
        notes.append("local working tree is dirty; avoid direct pull/rebase until changes are reviewed or saved")
    if compare_mode == "latest-tag":
        notes.append("default check mode tracks the latest release tag and ignores unreleased upstream main commits")

    if state == "in_sync":
        next_step = "No action needed."
    elif state == "behind":
        next_step = f"Inspect `git log --oneline HEAD..{remote_ref}` and merge manually when ready."
    elif state == "ahead":
        next_step = "Review local commits before pushing."
    else:
        next_step = f"Inspect `git log --oneline --left-right HEAD...{remote_ref}` before any merge or rebase."

    return " ".join(notes), next_step


def _summarize_state(ahead: int, behind: int) -> tuple[str, bool]:
    if ahead == 0 and behind == 0:
        return "in_sync", False
    if behind > 0 and ahead == 0:
        return "behind", True
    if behind == 0 and ahead > 0:
        return "ahead", False
    return "diverged", True


def inspect_repo(
    repo: Path,
    branch: str = "",
    remote: str = DEFAULT_REMOTE,
    no_fetch: bool = False,
    compare_mode: str = "latest-tag",
) -> RepoUpdateStatus:
    repo = repo.resolve()
    if not _is_git_repo(repo):
        raise ValueError(f"not a git repository: {repo}")

    resolved_branch = _resolve_branch(repo, branch)

    if not no_fetch:
        if compare_mode == "latest-tag":
            _run_git(repo, "fetch", remote, "--tags")
        else:
            _run_git(repo, "fetch", remote)

    if compare_mode == "latest-tag":
        remote_ref = _resolve_latest_tag(repo)
        resolved_remote = remote
    else:
        remote_ref, resolved_remote = _resolve_remote_ref(repo, resolved_branch, remote)
    ahead_behind = _run_git(repo, "rev-list", "--left-right", "--count", f"{resolved_branch}...{remote_ref}")
    left, right = (ahead_behind.split() + ["0", "0"])[:2]
    ahead = _safe_int(left)
    behind = _safe_int(right)
    dirty_count = len(_run_git(repo, "status", "--short").splitlines())
    local_head = _run_git(repo, "rev-parse", "--short", resolved_branch)
    remote_head = _run_git(repo, "rev-parse", "--short", remote_ref)
    state, has_updates = _summarize_state(ahead, behind)
    note, next_step = _build_note(dirty_count, state, remote_ref, compare_mode)

    return RepoUpdateStatus(
        repo=str(repo),
        branch=resolved_branch,
        remote=resolved_remote,
        compare_mode=compare_mode,
        remote_ref=remote_ref,
        local_head=local_head,
        remote_head=remote_head,
        dirty_count=dirty_count,
        ahead=ahead,
        behind=behind,
        state=state,
        has_updates=has_updates,
        fetch_skipped=no_fetch,
        note=note,
        next_step=next_step,
    )


def _format_status(status: RepoUpdateStatus) -> str:
    lines = [
        f"[repo]      {status.repo}",
        f"[branch]    {status.branch}",
        f"[compare]   {status.compare_mode}",
        f"[remote]    {status.remote_ref}",
        f"[local]     {status.local_head}",
        f"[remote]    {status.remote_head}",
        f"[dirty]     {status.dirty_count} file(s)",
        f"[ahead]     {status.ahead}",
        f"[behind]    {status.behind}",
        f"[state]     {status.state}",
    ]
    if status.note:
        lines.append(f"[note]      {status.note}")
    lines.append(f"[next]      {status.next_step}")
    return "\n".join(lines)


def _print_summary(statuses: Sequence[RepoUpdateStatus]) -> None:
    behind = sum(1 for item in statuses if item.state == "behind")
    diverged = sum(1 for item in statuses if item.state == "diverged")
    ahead = sum(1 for item in statuses if item.state == "ahead")
    dirty = sum(1 for item in statuses if item.dirty_count > 0)
    print(
        f"[summary] repos={len(statuses)} behind={behind} diverged={diverged} "
        f"ahead={ahead} dirty={dirty}"
    )


def _collect_statuses(
    repos: Iterable[Path], branch: str, remote: str, no_fetch: bool, compare_mode: str
) -> list[RepoUpdateStatus]:
    statuses: list[RepoUpdateStatus] = []
    for repo in repos:
        statuses.append(
            inspect_repo(repo, branch=branch, remote=remote, no_fetch=no_fetch, compare_mode=compare_mode)
        )
    return statuses


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Check whether upstream has new commits without changing local branches."
    )
    parser.add_argument(
        "--repo",
        default=str(Path(__file__).resolve().parents[1]),
        help="Repository path. Defaults to the current project root.",
    )
    parser.add_argument(
        "--root",
        default="",
        help="Scan a directory for nested git repositories and inspect each one.",
    )
    parser.add_argument(
        "--max-depth",
        type=int,
        default=2,
        help="Maximum nested depth when used with --root. Defaults to 2.",
    )
    parser.add_argument(
        "--branch",
        default="",
        help="Local branch to compare. Defaults to the current branch of each repo.",
    )
    parser.add_argument(
        "--remote",
        default=DEFAULT_REMOTE,
        help="Remote name to fetch when no tracking branch is configured. Defaults to origin.",
    )
    parser.add_argument(
        "--compare-mode",
        choices=("latest-tag", "branch"),
        default="latest-tag",
        help="Comparison target. Defaults to latest-tag so unreleased upstream main commits are ignored.",
    )
    parser.add_argument(
        "--no-fetch",
        action="store_true",
        help="Skip `git fetch` and only inspect current refs.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit machine-readable JSON output.",
    )
    parser.add_argument(
        "--fail-on-updates",
        action="store_true",
        help="Return exit code 3 when any repo is behind or diverged. Useful for automation.",
    )
    return parser


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()

    try:
        if args.root:
            repos = _discover_repos(Path(args.root), max_depth=max(args.max_depth, 0))
            if not repos:
                raise ValueError(f"no git repositories found under: {Path(args.root).resolve()}")
        else:
            repos = [Path(args.repo).resolve()]

        if not args.no_fetch and not args.json:
            print(f"[info] fetching remote refs for {len(repos)} repo(s)...")

        statuses = _collect_statuses(
            repos,
            branch=args.branch,
            remote=args.remote,
            no_fetch=args.no_fetch,
            compare_mode=args.compare_mode,
        )

        if args.json:
            payload = {
                "repos": [asdict(status) for status in statuses],
                "summary": {
                    "total": len(statuses),
                    "behind": sum(1 for item in statuses if item.state == "behind"),
                    "diverged": sum(1 for item in statuses if item.state == "diverged"),
                    "ahead": sum(1 for item in statuses if item.state == "ahead"),
                    "dirty": sum(1 for item in statuses if item.dirty_count > 0),
                },
            }
            print(json.dumps(payload, ensure_ascii=False, indent=2))
        else:
            for index, status in enumerate(statuses):
                if index:
                    print()
                print(_format_status(status))
            if len(statuses) > 1:
                print()
                _print_summary(statuses)

        if args.fail_on_updates and any(item.has_updates for item in statuses):
            return 3
        return 0
    except (subprocess.CalledProcessError, ValueError) as exc:
        stderr = getattr(exc, "stderr", "") or ""
        stdout = getattr(exc, "stdout", "") or ""
        message = str(exc).strip() or stderr.strip() or stdout.strip() or repr(exc)
        print(f"[error] {message}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
