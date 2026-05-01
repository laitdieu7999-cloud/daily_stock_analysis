# GitHub 历史数据包接入

当前推荐优先接入的公开历史数据来源：

- `chenditc/investment_data`
  - 资产名：`qlib_bin.tar.gz`
  - 特点：持续更新、适合 A股 / ETF 的离线大样本回测底座

另外补一条本地 ETF 专用缓存线：

- `scripts/backfill_local_etf_history.py`
  - 作用：把常用 ETF 日线抓成本地缓存 CSV
  - 适合弥补 `qlib_bin` 不覆盖 ETF 的缺口

## 一键接入

```bash
cd /Users/laitdieu/Documents/github/daily_stock_analysis
./.venv311/bin/python scripts/bootstrap_github_history_data.py
```

默认行为：

- 查询 `investment_data` 最新 release
- 下载 `qlib_bin.tar.gz`
- 解压到：
  - `/Users/laitdieu/Documents/github/daily_stock_analysis/.cache/github_history_data/investment_data/<tag>/extracted`
- 自动检查当前常用 A股 / ETF 标的覆盖情况
- 写一份报告到：
  - `/Users/laitdieu/Documents/github/daily_stock_analysis/reports/backtests/<当天>_GitHub历史数据接入报告.md`
- 对已覆盖的股票 / 指数，现有
  - `/Users/laitdieu/Documents/github/daily_stock_analysis/scripts/run_five_year_daily_validation.py`
  会优先尝试使用这份本地 `qlib_bin` 历史仓，而不是先走在线抓数

## 只看元数据，不下载

```bash
cd /Users/laitdieu/Documents/github/daily_stock_analysis
./.venv311/bin/python scripts/bootstrap_github_history_data.py --metadata-only
```

## 自定义标的覆盖检查

```bash
cd /Users/laitdieu/Documents/github/daily_stock_analysis
./.venv311/bin/python scripts/bootstrap_github_history_data.py --codes 510500,159937,600519,000905
```

## 回填常用 ETF 本地日线

```bash
cd /Users/laitdieu/Documents/github/daily_stock_analysis
./.venv311/bin/python scripts/backfill_local_etf_history.py
```

默认会回填：

- `510300`
- `510500`
- `512980`
- `159201`
- `159326`
- `159613`
- `159869`
- `159937`

本地缓存目录：

- `/Users/laitdieu/Documents/github/daily_stock_analysis/.cache/local_market_history/etf_daily`

## 设计定位

- 这份数据包最适合补齐 `A股 / ETF` 的历史价格仓，避免很多回测只能等每天实盘自然积累。
- 它 **不直接替代** 当前已经在本地建设的：
  - `IC 全收益标签`
  - `M1-M2 前端塌陷`
  - `Q1-Q2 远季锚`
  - `500ETF 期权代理`
- 更合理的组合是：
  - 用这份 `qlib_bin` 当 A股 / ETF 的统一离线价格底库
  - 对 `qlib_bin` 不覆盖的 ETF，用本地 ETF 缓存仓补齐
  - 继续保留我们自己针对 `IC / 期权代理 / QVIX` 的专属验证链
