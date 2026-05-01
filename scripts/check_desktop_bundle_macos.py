#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Sanity check a packaged macOS desktop bundle.

This catches the two most expensive desktop packaging regressions before the
app is installed:
1. Electron shell files are missing from app.asar.
2. The bundled Python backend or one of its native libraries is missing.

By default the script also starts the packaged backend on a temporary port and
waits for /api/health, without opening the Electron UI.
"""

from __future__ import annotations

import argparse
import os
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Iterable, List, Sequence


REQUIRED_BUNDLE_FILES = (
    "Contents/MacOS/daily-stock-analysis-desktop",
    "Contents/Resources/app.asar",
    "Contents/Resources/.env.example",
    "Contents/Resources/backend/stock_analysis/stock_analysis",
    "Contents/Resources/backend/stock_analysis/_internal/static/index.html",
    "Contents/Resources/backend/stock_analysis/_internal/py_mini_racer/libmini_racer.dylib",
)

REQUIRED_ASAR_ENTRIES = (
    "/main.js",
    "/package.json",
    "/preload.js",
    "/renderer/loading.html",
)


class BundleCheckError(RuntimeError):
    """Raised when the desktop bundle is incomplete or cannot start."""


def _repo_root() -> Path:
    return Path(__file__).resolve().parent.parent


def _tail(path: Path, limit: int = 4000) -> str:
    if not path.exists():
        return ""
    text = path.read_text(encoding="utf-8", errors="replace")
    if len(text) <= limit:
        return text
    return text[-limit:]


def _find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _asar_command(repo_root: Path) -> List[str]:
    asar_bin = repo_root / "apps" / "dsa-desktop" / "node_modules" / ".bin" / "asar"
    if asar_bin.is_file():
        return [str(asar_bin)]
    return ["npx", "asar"]


def _require_paths(app_bundle: Path, relative_paths: Iterable[str]) -> None:
    missing: List[str] = []
    for relative_path in relative_paths:
        candidate = app_bundle / relative_path
        if not candidate.exists():
            missing.append(relative_path)

    if missing:
        formatted = "\n".join(f"  - {item}" for item in missing)
        raise BundleCheckError(f"missing required bundle file(s):\n{formatted}")

    backend_exe = app_bundle / "Contents/Resources/backend/stock_analysis/stock_analysis"
    electron_exe = app_bundle / "Contents/MacOS/daily-stock-analysis-desktop"
    not_executable = [
        str(path.relative_to(app_bundle))
        for path in (backend_exe, electron_exe)
        if not os.access(path, os.X_OK)
    ]
    if not_executable:
        formatted = "\n".join(f"  - {item}" for item in not_executable)
        raise BundleCheckError(f"required executable bit is missing:\n{formatted}")


def _list_asar_entries(app_bundle: Path, repo_root: Path) -> Sequence[str]:
    asar_path = app_bundle / "Contents/Resources/app.asar"
    cmd = [*_asar_command(repo_root), "list", str(asar_path)]
    result = subprocess.run(
        cmd,
        cwd=repo_root / "apps" / "dsa-desktop",
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    if result.returncode != 0:
        raise BundleCheckError(
            "failed to inspect app.asar:\n"
            f"command: {' '.join(cmd)}\n"
            f"stdout: {result.stdout.strip()}\n"
            f"stderr: {result.stderr.strip()}"
        )
    return tuple(line.strip() for line in result.stdout.splitlines() if line.strip())


def _require_asar_entries(app_bundle: Path, repo_root: Path) -> None:
    entries = set(_list_asar_entries(app_bundle, repo_root))
    missing = [entry for entry in REQUIRED_ASAR_ENTRIES if entry not in entries]
    if missing:
        formatted = "\n".join(f"  - {item}" for item in missing)
        raise BundleCheckError(f"missing required app.asar entry/entries:\n{formatted}")


def _wait_for_health(url: str, process: subprocess.Popen[str], timeout_seconds: float) -> None:
    deadline = time.monotonic() + timeout_seconds
    last_error = ""

    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise BundleCheckError(f"backend exited before health check: code={process.returncode}")

        try:
            with urllib.request.urlopen(url, timeout=1.0) as response:
                if response.status == 200:
                    return
                last_error = f"status={response.status}"
        except (OSError, urllib.error.URLError) as exc:
            last_error = str(exc)

        time.sleep(0.25)

    raise BundleCheckError(f"backend health check timed out: {url}; last_error={last_error}")


def _run_backend_smoke(app_bundle: Path, timeout_seconds: float) -> None:
    backend_exe = app_bundle / "Contents/Resources/backend/stock_analysis/stock_analysis"
    port = _find_free_port()

    with tempfile.TemporaryDirectory(prefix="dsa-desktop-smoke-") as tmp_name:
        tmp_dir = Path(tmp_name)
        env_file = tmp_dir / ".env"
        db_path = tmp_dir / "data" / "stock_analysis.db"
        log_dir = tmp_dir / "logs"
        stdout_path = tmp_dir / "backend.stdout.log"
        stderr_path = tmp_dir / "backend.stderr.log"

        db_path.parent.mkdir(parents=True, exist_ok=True)
        log_dir.mkdir(parents=True, exist_ok=True)
        env_file.write_text(
            "\n".join(
                [
                    "SCHEDULE_ENABLED=false",
                    "WEBUI_ENABLED=false",
                    "BOT_ENABLED=false",
                    "DINGTALK_STREAM_ENABLED=false",
                    "FEISHU_STREAM_ENABLED=false",
                    "ENABLE_SEARCH=false",
                    "",
                ]
            ),
            encoding="utf-8",
        )

        env = {
            **os.environ,
            "DSA_DESKTOP_MODE": "true",
            "ENV_FILE": str(env_file),
            "DATABASE_PATH": str(db_path),
            "LOG_DIR": str(log_dir),
            "WEBUI_HOST": "127.0.0.1",
            "WEBUI_PORT": str(port),
            "PYTHONUTF8": "1",
            "PYTHONIOENCODING": "utf-8",
            "SCHEDULE_ENABLED": "false",
            "WEBUI_ENABLED": "false",
            "BOT_ENABLED": "false",
            "DINGTALK_STREAM_ENABLED": "false",
            "FEISHU_STREAM_ENABLED": "false",
        }
        args = [
            str(backend_exe),
            "--serve-only",
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
        ]

        with stdout_path.open("w", encoding="utf-8") as stdout_file, stderr_path.open(
            "w", encoding="utf-8"
        ) as stderr_file:
            process = subprocess.Popen(
                args,
                cwd=backend_exe.parent,
                env=env,
                text=True,
                stdout=stdout_file,
                stderr=stderr_file,
            )

        try:
            _wait_for_health(f"http://127.0.0.1:{port}/api/health", process, timeout_seconds)
        except BundleCheckError as exc:
            raise BundleCheckError(
                f"{exc}\n"
                f"stdout tail:\n{_tail(stdout_path)}\n"
                f"stderr tail:\n{_tail(stderr_path)}"
            ) from exc
        finally:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)


def check_bundle(app_bundle: Path, *, skip_backend_smoke: bool, timeout_seconds: float) -> None:
    repo_root = _repo_root()
    if not app_bundle.is_dir():
        raise BundleCheckError(f"desktop app bundle not found: {app_bundle}")

    _require_paths(app_bundle, REQUIRED_BUNDLE_FILES)
    _require_asar_entries(app_bundle, repo_root)

    if not skip_backend_smoke:
        _run_backend_smoke(app_bundle, timeout_seconds)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Check a packaged macOS desktop app bundle.")
    parser.add_argument("app_bundle", type=Path, help="Path to daily-stock-analysis-desktop.app")
    parser.add_argument(
        "--skip-backend-smoke",
        action="store_true",
        help="Only inspect files; do not start the packaged backend.",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=45.0,
        help="Backend health-check timeout in seconds.",
    )
    args = parser.parse_args(argv)

    app_bundle = args.app_bundle.resolve()
    print(f"[check_desktop_bundle] inspecting {app_bundle}")

    try:
        check_bundle(
            app_bundle,
            skip_backend_smoke=args.skip_backend_smoke,
            timeout_seconds=args.timeout,
        )
    except BundleCheckError as exc:
        print(f"[check_desktop_bundle] ERROR: {exc}", file=sys.stderr)
        return 1

    smoke_note = "file-only" if args.skip_backend_smoke else "backend smoke"
    print(f"[check_desktop_bundle] OK: bundle integrity and {smoke_note} check passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
