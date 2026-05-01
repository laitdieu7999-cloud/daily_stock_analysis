#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Bootstrap large historical backtest datasets from GitHub releases.

Current primary source:
- chenditc/investment_data -> qlib_bin.tar.gz

This script is intentionally light on framework coupling. Its goal is to:
1. Discover the latest release asset.
2. Download and extract the dataset locally.
3. Inspect whether the user's focus A-share / ETF symbols are covered.
4. Emit a short markdown report under reports/backtests.
"""

from __future__ import annotations

import argparse
import json
import shutil
import tarfile
import urllib.request
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Optional


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent

DEFAULT_REPO = "chenditc/investment_data"
DEFAULT_ASSET_NAME = "qlib_bin.tar.gz"
DEFAULT_INSTALL_DIR = PROJECT_ROOT / ".cache" / "github_history_data" / "investment_data"
DEFAULT_REPORT_DIR = PROJECT_ROOT / "reports" / "backtests"
DEFAULT_TIMEOUT = 60
DEFAULT_FOCUS_CODES = [
    "000905",
    "002352",
    "300251",
    "510300",
    "510500",
    "512980",
    "600519",
    "600529",
    "600882",
    "600900",
    "600918",
    "601888",
    "159201",
    "159326",
    "159613",
    "159869",
    "159937",
]


@dataclass(frozen=True)
class ReleaseAsset:
    repo: str
    tag_name: str
    asset_name: str
    size_bytes: int
    download_url: str
    published_at: str


def _user_agent() -> str:
    return "daily-stock-analysis-history-bootstrap/1.0"


def _github_api_json(url: str, *, timeout: int = DEFAULT_TIMEOUT) -> Any:
    request = urllib.request.Request(
        url,
        headers={
            "Accept": "application/vnd.github+json",
            "User-Agent": _user_agent(),
        },
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return json.load(response)


def fetch_latest_release_asset(repo: str, asset_name: str) -> ReleaseAsset:
    payload = _github_api_json(f"https://api.github.com/repos/{repo}/releases/latest")
    for asset in payload.get("assets", []):
        if asset.get("name") == asset_name:
            return ReleaseAsset(
                repo=repo,
                tag_name=str(payload.get("tag_name") or ""),
                asset_name=asset_name,
                size_bytes=int(asset.get("size") or 0),
                download_url=str(asset.get("browser_download_url") or ""),
                published_at=str(payload.get("published_at") or ""),
            )
    raise RuntimeError(f"Asset {asset_name!r} not found in latest release of {repo}.")


def _ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def _download_with_progress(url: str, destination: Path, *, timeout: int = DEFAULT_TIMEOUT) -> Path:
    _ensure_parent(destination)
    request = urllib.request.Request(url, headers={"User-Agent": _user_agent()})
    with urllib.request.urlopen(request, timeout=timeout) as response, destination.open("wb") as handle:
        shutil.copyfileobj(response, handle)
    return destination


def _is_within_directory(directory: Path, target: Path) -> bool:
    try:
        target.resolve().relative_to(directory.resolve())
        return True
    except Exception:
        return False


def _safe_extract_tar(archive_path: Path, output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    with tarfile.open(archive_path, "r:gz") as archive:
        for member in archive.getmembers():
            target = output_dir / member.name
            if not _is_within_directory(output_dir, target):
                raise RuntimeError(f"Unsafe archive member detected: {member.name}")
        archive.extractall(output_dir)


def locate_qlib_root(search_root: Path) -> Optional[Path]:
    if not search_root.exists() or not search_root.is_dir():
        return None
    candidates = [search_root]
    candidates.extend(path for path in search_root.iterdir() if path.is_dir())
    for candidate in candidates:
        if (candidate / "features").exists() and (candidate / "instruments").exists():
            return candidate
    return None


def _normalize_cn_code(code: str) -> str:
    cleaned = (code or "").strip().lower()
    if not cleaned:
        return cleaned
    if cleaned.startswith(("sh", "sz", "bj")) and len(cleaned) >= 8:
        return cleaned
    digits = "".join(char for char in cleaned if char.isdigit())
    if len(digits) != 6:
        return cleaned
    if digits.startswith(("5", "6", "9")):
        return f"sh{digits}"
    return f"sz{digits}"


def _parse_instrument_code(line: str) -> Optional[str]:
    text = (line or "").strip()
    if not text:
        return None
    raw = text.split("\t", 1)[0].split(",", 1)[0].strip()
    if not raw:
        return None
    return _normalize_cn_code(raw)


def collect_available_instruments(qlib_root: Path) -> set[str]:
    instruments_file = qlib_root / "instruments" / "all.txt"
    available: set[str] = set()
    if instruments_file.exists():
        for line in instruments_file.read_text(encoding="utf-8").splitlines():
            parsed = _parse_instrument_code(line)
            if parsed:
                available.add(parsed)
    if available:
        return available

    features_dir = qlib_root / "features"
    if features_dir.exists():
        for child in features_dir.iterdir():
            if child.is_dir():
                available.add(_normalize_cn_code(child.name))
    return available


def build_focus_coverage_rows(codes: Iterable[str], available: set[str]) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for code in codes:
        normalized = _normalize_cn_code(code)
        rows.append(
            {
                "input_code": code,
                "normalized_code": normalized,
                "covered": "yes" if normalized in available else "no",
            }
        )
    return rows


def _format_size_mb(size_bytes: int) -> str:
    return f"{size_bytes / (1024 * 1024):.1f} MB"


def render_report(
    *,
    asset: ReleaseAsset,
    archive_path: Path,
    extract_root: Path,
    qlib_root: Optional[Path],
    available_count: int,
    coverage_rows: list[dict[str, str]],
) -> str:
    covered = sum(1 for row in coverage_rows if row["covered"] == "yes")
    lines = [
        f"# GitHub 历史数据接入报告 ({datetime.now().date().isoformat()})",
        "",
        "## 数据源",
        f"- 仓库: `{asset.repo}`",
        f"- Tag: `{asset.tag_name}`",
        f"- 资产: `{asset.asset_name}`",
        f"- 体积: `{_format_size_mb(asset.size_bytes)}`",
        f"- 发布时间: `{asset.published_at}`",
        f"- 下载地址: {asset.download_url}",
        "",
        "## 本地落地",
        f"- 压缩包: `{archive_path}`",
        f"- 解压目录: `{extract_root}`",
        f"- Qlib 根目录: `{qlib_root}`" if qlib_root else "- Qlib 根目录: `未识别`",
        f"- 可识别标的数量: `{available_count}`",
        "",
        "## 当前关注标的覆盖",
        f"- 覆盖数: `{covered}/{len(coverage_rows)}`",
        "",
        "| 输入代码 | 归一化代码 | 是否覆盖 |",
        "| --- | --- | --- |",
    ]
    for row in coverage_rows:
        status = "覆盖" if row["covered"] == "yes" else "缺失"
        lines.append(f"| {row['input_code']} | {row['normalized_code']} | {status} |")
    lines.extend(
        [
            "",
            "## 结论",
            "- 这份数据包更适合立刻给 A股 / ETF 的日线与分钟级研究做底座。",
            "- 它不能直接替代我们已经在做的 IC / 500ETF期权 / QVIX 专属链路，但可以先把股票与 ETF 的大样本历史仓补齐。",
            "- 如果后续要接入当前回测体系，优先把这份本地历史仓用作 A股 / ETF 的统一离线价格底库。",
        ]
    )
    return "\n".join(lines) + "\n"


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Download and inspect GitHub-hosted historical backtest data packages.")
    parser.add_argument("--repo", default=DEFAULT_REPO, help="GitHub repo in owner/name format.")
    parser.add_argument("--asset-name", default=DEFAULT_ASSET_NAME, help="Release asset name to download.")
    parser.add_argument(
        "--install-dir",
        default=str(DEFAULT_INSTALL_DIR),
        help="Local directory used to store the downloaded archive and extracted dataset.",
    )
    parser.add_argument(
        "--report-dir",
        default=str(DEFAULT_REPORT_DIR),
        help="Directory used to write the markdown inspection report.",
    )
    parser.add_argument(
        "--codes",
        default=",".join(DEFAULT_FOCUS_CODES),
        help="Comma-separated A-share/ETF codes to check coverage for.",
    )
    parser.add_argument(
        "--metadata-only",
        action="store_true",
        help="Only query release metadata and write a report stub without downloading the asset.",
    )
    parser.add_argument(
        "--refresh-download",
        action="store_true",
        help="Force re-download even if the archive already exists locally.",
    )
    parser.add_argument(
        "--refresh-extract",
        action="store_true",
        help="Force re-extract the archive even if a qlib root is already detected.",
    )
    return parser


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()

    install_dir = Path(args.install_dir).expanduser().resolve()
    report_dir = Path(args.report_dir).expanduser().resolve()
    focus_codes = [code.strip() for code in str(args.codes).split(",") if code.strip()]

    asset = fetch_latest_release_asset(args.repo, args.asset_name)

    archive_path = install_dir / asset.tag_name / asset.asset_name
    extract_root = install_dir / asset.tag_name / "extracted"
    qlib_root: Optional[Path] = None
    available: set[str] = set()

    if not args.metadata_only:
        if args.refresh_download or not archive_path.exists() or archive_path.stat().st_size != asset.size_bytes:
            _download_with_progress(asset.download_url, archive_path)
        if args.refresh_extract and extract_root.exists():
            shutil.rmtree(extract_root)
        qlib_root = locate_qlib_root(extract_root)
        if qlib_root is None:
            _safe_extract_tar(archive_path, extract_root)
            qlib_root = locate_qlib_root(extract_root)
        if qlib_root is not None:
            available = collect_available_instruments(qlib_root)

    coverage_rows = build_focus_coverage_rows(focus_codes, available)
    report_text = render_report(
        asset=asset,
        archive_path=archive_path,
        extract_root=extract_root,
        qlib_root=qlib_root,
        available_count=len(available),
        coverage_rows=coverage_rows,
    )

    report_dir.mkdir(parents=True, exist_ok=True)
    report_path = report_dir / f"{datetime.now().date().isoformat()}_GitHub历史数据接入报告.md"
    report_path.write_text(report_text, encoding="utf-8")
    print(report_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
