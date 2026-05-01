"""Regression tests for the macOS desktop bundle sanity check."""

from __future__ import annotations

import importlib.util
import os
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = REPO_ROOT / "scripts" / "check_desktop_bundle_macos.py"


def _load_checker_module():
    spec = importlib.util.spec_from_file_location("check_desktop_bundle_macos", SCRIPT_PATH)
    assert spec is not None
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def _write_file(path: Path, content: str = "") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _make_fake_bundle(tmp_path: Path, checker) -> Path:
    app_bundle = tmp_path / "daily-stock-analysis-desktop.app"
    for relative_path in checker.REQUIRED_BUNDLE_FILES:
        _write_file(app_bundle / relative_path, "placeholder")

    for relative_path in (
        "Contents/MacOS/daily-stock-analysis-desktop",
        "Contents/Resources/backend/stock_analysis/stock_analysis",
    ):
        target = app_bundle / relative_path
        os.chmod(target, 0o755)

    return app_bundle


def test_check_bundle_passes_with_required_files(monkeypatch, tmp_path: Path) -> None:
    checker = _load_checker_module()
    app_bundle = _make_fake_bundle(tmp_path, checker)

    monkeypatch.setattr(
        checker,
        "_list_asar_entries",
        lambda _app_bundle, _repo_root: checker.REQUIRED_ASAR_ENTRIES,
    )
    smoke_calls = []
    monkeypatch.setattr(
        checker,
        "_run_backend_smoke",
        lambda checked_bundle, timeout_seconds: smoke_calls.append((checked_bundle, timeout_seconds)),
    )

    checker.check_bundle(app_bundle, skip_backend_smoke=False, timeout_seconds=12.5)

    assert smoke_calls == [(app_bundle, 12.5)]


def test_check_bundle_requires_py_mini_racer_native_library(monkeypatch, tmp_path: Path) -> None:
    checker = _load_checker_module()
    app_bundle = _make_fake_bundle(tmp_path, checker)
    monkeypatch.setattr(
        checker,
        "_list_asar_entries",
        lambda _app_bundle, _repo_root: checker.REQUIRED_ASAR_ENTRIES,
    )

    native_lib = (
        app_bundle
        / "Contents/Resources/backend/stock_analysis/_internal/py_mini_racer/libmini_racer.dylib"
    )
    native_lib.unlink()

    with pytest.raises(checker.BundleCheckError, match="libmini_racer.dylib"):
        checker.check_bundle(app_bundle, skip_backend_smoke=True, timeout_seconds=1)


def test_check_bundle_requires_loading_page_in_asar(monkeypatch, tmp_path: Path) -> None:
    checker = _load_checker_module()
    app_bundle = _make_fake_bundle(tmp_path, checker)
    monkeypatch.setattr(
        checker,
        "_list_asar_entries",
        lambda _app_bundle, _repo_root: tuple(
            entry for entry in checker.REQUIRED_ASAR_ENTRIES if entry != "/renderer/loading.html"
        ),
    )

    with pytest.raises(checker.BundleCheckError, match="renderer/loading.html"):
        checker.check_bundle(app_bundle, skip_backend_smoke=True, timeout_seconds=1)
