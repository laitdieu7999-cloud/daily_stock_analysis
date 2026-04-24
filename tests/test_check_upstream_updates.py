from pathlib import Path

from scripts.check_upstream_updates import (
    RepoUpdateStatus,
    _build_note,
    _discover_repos,
    _summarize_state,
    inspect_repo,
)


def test_summarize_state_marks_behind_as_update() -> None:
    state, has_updates = _summarize_state(ahead=0, behind=3)
    assert state == "behind"
    assert has_updates is True


def test_summarize_state_marks_diverged_as_update() -> None:
    state, has_updates = _summarize_state(ahead=2, behind=1)
    assert state == "diverged"
    assert has_updates is True


def test_build_note_for_dirty_repo_mentions_manual_review() -> None:
    note, next_step = _build_note(
        dirty_count=4,
        state="behind",
        remote_ref="origin/main",
        compare_mode="branch",
    )
    assert "dirty" in note
    assert "HEAD..origin/main" in next_step


def test_build_note_for_latest_tag_mentions_release_policy() -> None:
    note, _next_step = _build_note(
        dirty_count=0,
        state="in_sync",
        remote_ref="v3.13.0",
        compare_mode="latest-tag",
    )
    assert "latest release tag" in note


def test_discover_repos_includes_root_and_nested(tmp_path: Path) -> None:
    (tmp_path / ".git").mkdir()
    nested = tmp_path / "nested"
    (nested / ".git").mkdir(parents=True)
    deep = tmp_path / "too-deep" / "inner"
    (deep / ".git").mkdir(parents=True)

    repos = _discover_repos(tmp_path, max_depth=1)

    assert tmp_path.resolve() in repos
    assert nested.resolve() in repos
    assert deep.resolve() not in repos


def test_inspect_repo_prefers_tracking_branch(monkeypatch, tmp_path: Path) -> None:
    (tmp_path / ".git").mkdir()

    responses = {
        ("branch", "--show-current"): "main",
        ("rev-parse", "--abbrev-ref", "main@{upstream}"): "upstream/main",
        ("rev-list", "--left-right", "--count", "main...upstream/main"): "0\t2",
        ("status", "--short"): " M main.py\n?? new_file.py",
        ("rev-parse", "--short", "main"): "abc1234",
        ("rev-parse", "--short", "upstream/main"): "def5678",
    }
    calls: list[tuple[str, ...]] = []

    def fake_run_git(repo: Path, *args: str, check: bool = True) -> str:
        calls.append(args)
        key = tuple(args)
        if key == ("fetch", "origin"):
            return ""
        assert key in responses, f"unexpected git call: {key}"
        return responses[key]

    monkeypatch.setattr("scripts.check_upstream_updates._run_git", fake_run_git)

    status = inspect_repo(tmp_path, remote="origin", no_fetch=False, compare_mode="branch")

    assert isinstance(status, RepoUpdateStatus)
    assert status.remote == "upstream"
    assert status.remote_ref == "upstream/main"
    assert status.behind == 2
    assert status.state == "behind"
    assert status.dirty_count == 2
    assert ("fetch", "origin") in calls


def test_inspect_repo_branch_mode_honors_explicit_remote(monkeypatch, tmp_path: Path) -> None:
    (tmp_path / ".git").mkdir()

    responses = {
        ("branch", "--show-current"): "main",
        ("rev-list", "--left-right", "--count", "main...upstream/main"): "0\t2",
        ("status", "--short"): "",
        ("rev-parse", "--short", "main"): "abc1234",
        ("rev-parse", "--short", "upstream/main"): "def5678",
    }

    def fake_run_git(repo: Path, *args: str, check: bool = True) -> str:
        key = tuple(args)
        if key == ("fetch", "upstream"):
            return ""
        assert key in responses, f"unexpected git call: {key}"
        return responses[key]

    monkeypatch.setattr("scripts.check_upstream_updates._run_git", fake_run_git)

    status = inspect_repo(tmp_path, remote="upstream", no_fetch=False, compare_mode="branch")

    assert status.remote == "upstream"
    assert status.remote_ref == "upstream/main"


def test_inspect_repo_defaults_to_latest_tag(monkeypatch, tmp_path: Path) -> None:
    (tmp_path / ".git").mkdir()

    responses = {
        ("branch", "--show-current"): "main",
        ("tag", "--list", "v*", "--sort=-version:refname"): "v3.13.0\nv3.12.0",
        ("rev-list", "--left-right", "--count", "main...v3.13.0"): "0\t1",
        ("status", "--short"): "",
        ("rev-parse", "--short", "main"): "abc1234",
        ("rev-parse", "--short", "v3.13.0"): "def5678",
    }
    calls: list[tuple[str, ...]] = []

    def fake_run_git(repo: Path, *args: str, check: bool = True) -> str:
        calls.append(args)
        key = tuple(args)
        if key == ("fetch", "upstream", "--tags"):
            return ""
        assert key in responses, f"unexpected git call: {key}"
        return responses[key]

    monkeypatch.setattr("scripts.check_upstream_updates._run_git", fake_run_git)

    status = inspect_repo(tmp_path, remote="upstream", no_fetch=False)

    assert isinstance(status, RepoUpdateStatus)
    assert status.compare_mode == "latest-tag"
    assert status.remote_ref == "v3.13.0"
    assert status.behind == 1
    assert status.state == "behind"
    assert ("fetch", "upstream", "--tags") in calls
