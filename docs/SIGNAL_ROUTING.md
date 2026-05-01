# Signal Routing Contract

本文件定义统一信号契约。策略模块只负责生成 `SignalEvent`，是否打扰用户由 `SignalRouter` 统一决定。

## 目标

- 持仓风控、自选股买入、黑天鹅、Gemini 外部观点、Shadow 策略统一从一个入口分发。
- 生产提醒和 Shadow 记录分离，避免观察型策略误触发飞书/桌面打扰。
- 自选股只提醒明确买入机会，非买入状态默认静默。

## 标准字段

```json
{
  "source": "holding_risk",
  "symbol": "600519",
  "name": "贵州茅台",
  "priority": "P1",
  "category": "holding",
  "action": "risk_alert",
  "title": "持仓风控触发",
  "content": "跌破MA20且放量走弱",
  "reason": "break_ma20",
  "should_notify": true,
  "channels": ["feishu", "desktop"],
  "dedupe_key": "holding_risk:600519:break_ma20",
  "created_at": "2026-04-28T14:35:00",
  "metadata": {}
}
```

## 优先级规则

| 优先级 | 典型来源 | 路由规则 |
| --- | --- | --- |
| `P0` | 黑天鹅、系统级风险 | 必须提醒，飞书 + 桌面 |
| `P1` | 持仓风控 | 必须提醒，飞书 + 桌面；同 scope 默认 30 分钟冷却 |
| `P2` | 自选股买入 | 仅 `category=watchlist` 且 `action=buy` 时提醒；同标的默认每日 1 次 |
| `P3` | IC/期权 Shadow | 只归档，不提醒 |
| `P4` | Gemini 外部观点 | 只归档，不提醒 |

## 降噪与状态对账

- `SignalRouter` 支持可选 `state_path`，用于持久化实际送达记录；重启后仍能识别当天已发送内容。
- `P1` 默认使用 `source + scope + action` 做冷却键，适合持仓风控批量告警；`P0` 不冷却。
- `P2` 默认使用 `symbol` 或单标的 `active_codes` 做每日限次键，防止同一自选股反复刷屏。
- 盘中提醒接入独立路由状态文件：`stock_intraday_reminder_route_state.json`。
- 盘中提醒会隔离基础异常 Tick：价格非正数或日内涨跌幅绝对值超过阈值时，不把该实时价用于自选买入判断。
- 可调参数：`STOCK_INTRADAY_HOLDING_COOLDOWN_MINUTES`、`STOCK_INTRADAY_WATCHLIST_DAILY_LIMIT`、`STOCK_INTRADAY_SYSTEMIC_BATCH_THRESHOLD`、`STOCK_INTRADAY_BAD_TICK_MAX_ABS_CHANGE_PCT`。

## 当前接入状态

- 已接入：盘中个股/ETF提醒。
- 已接入：Gemini 外部观点对比，以 `P4` 写入 `reports/signal_events/gemini_external_views.jsonl`，不提醒。
- 已接入：Gemini 黑天鹅报告当天段落明确“已触发”时，以 `P0` 强提醒，并写入 `reports/signal_events/black_swan_events.jsonl`。
- 已接入：IC M1-M2 / 纸面 Put Shadow 账本，以 `P3` 写入 `reports/signal_events/ic_shadow_events.jsonl`，不提醒。
- 已接入：持仓/自选盘中提醒的路由冷却、每日限次、系统性批量标题与异常 Tick 隔离。
- 待接入：暂无。后续新策略先进入 `P3/P4`，人工确认后再升格。

## 接入原则

- 新策略不得直接调用 `NotificationService.send()`。
- 新策略先生成 `SignalEvent`，再交给 `SignalRouter.dispatch()`。
- Shadow 策略必须使用 `P3` 或 `P4`，除非人工确认升格。
