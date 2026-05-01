# 资产扩展运行说明

本文档承接本地自定义扩展中的“运行层规则”，与 [asset_direction_policy.yaml](/Users/laitdieu/Documents/github/daily_stock_analysis/strategies/asset_direction_policy.yaml) 分离。

策略文件只保留“方向约束”和“分析原则”；这里负责记录推送顺序、事件监控、收盘提醒和推荐配置。

## 一、每日推送优先级

每日市场扩展推送建议按以下顺序组织：

1. 黄金（XAUUSD / AU）
2. 白银（XAGUSD / AG）
3. 中证500 / IC 期货
4. 当前持仓分析
5. 精选入场标的

其中前 3 项已经落地到 [src/daily_push_pipeline.py](/Users/laitdieu/Documents/github/daily_stock_analysis/src/daily_push_pipeline.py)。

## 二、推荐推送内容

### 1. 黄金

建议包含：

- 当前价格与日内涨跌幅
- 布林带状态
- 威科夫阶段判断
- 地缘风险框架结论
- 是否适合买入 / 持有 / 观望

### 2. 白银

建议包含：

- 当前价格与日内涨跌幅
- 金银比
- 多空方向判断
- 入场位 / 止损位 / 目标位

### 3. 中证500 / IC

建议包含：

- 中证500 现货指数点位
- IC 合约价格、基差、剩余天数
- 年化贴水收益率
- 主力合约判断
- 移仓建议

## 三、推荐运行时点

推荐时点：

- 早盘分析推送：`10:00`
- 收盘提醒：`15:10`

说明：

- `10:00` 适合 A 股开盘后做一次有实时信息的判断
- `15:10` 适合推送国债逆回购、保证金、持仓更新等收盘检查项

## 四、事件监控建议

当前本地扩展已支持三类事件：

- `basis_spike`
- `price_spike`
- `news_impact`

### 1. IC 贴水监控

建议规则：

- 深度贴水：年化贴水收益率 > `10%`
- 贴水突然加深：日内变化 > `2` 个百分点

### 2. 金银价格异动

建议规则：

- 检测窗口：`30` 分钟
- 黄金触发阈值：`1.0%`
- 白银触发阈值：`1.5%`

### 3. 重大事件监控

建议关键词方向：

- 金银：黄金、白银、金价、银价、美联储、利率、降息、CPI、地缘
- IC / A股：中证500、IC期货、A股、央行、降准、印花税
- 宏观：非农、就业、GDP、PMI、贸易战、关税

## 五、配置建议

推荐把运行层配置放在 `.env` 或 Web 设置中统一管理。

### 1. 市场扩展推送

```env
MARKET_DAILY_PUSH_ENABLED=true
MARKET_DAILY_PUSH_AI_ENABLED=true
JIN10_API_KEY=
```

### 2. 事件监控

```env
AGENT_EVENT_MONITOR_ENABLED=true
AGENT_EVENT_MONITOR_INTERVAL_MINUTES=5
AGENT_EVENT_ALERT_RULES_JSON=
```

### 3. 收盘提醒

```env
CLOSE_REMINDER_ENABLED=true
CLOSE_REMINDER_TIME=15:10
```

## 六、推荐的 Event Monitor 配置样例

```json
{
  "rules": [
    {
      "stock_code": "IC",
      "alert_type": "basis_spike",
      "description": "IC主力合约贴水监控",
      "deep_threshold": 10.0,
      "change_threshold": 2.0,
      "contract": "",
      "ttl_hours": 4.0
    },
    {
      "stock_code": "XAUUSD",
      "alert_type": "price_spike",
      "description": "黄金价格异动监控",
      "change_pct": 1.0,
      "direction": "both",
      "window_minutes": 30,
      "ttl_hours": 2.0
    },
    {
      "stock_code": "XAGUSD",
      "alert_type": "price_spike",
      "description": "白银价格异动监控",
      "change_pct": 1.5,
      "direction": "both",
      "window_minutes": 30,
      "ttl_hours": 2.0
    },
    {
      "stock_code": "NEWS",
      "alert_type": "news_impact",
      "description": "重大事件影响监控",
      "keywords": ["黄金", "白银", "美联储", "利率", "降息", "CPI", "地缘", "中证500", "A股", "央行", "非农", "PMI"],
      "ttl_hours": 1.0
    }
  ]
}
```

## 七、收盘提醒建议内容

推荐包含三项：

1. 国债逆回购检查
2. 期货保证金检查
3. 持仓截图更新

## 八、维护原则

后续维护建议：

- 资产方向和分析约束，放在策略 YAML
- 调度、推送顺序、提醒时点，放在运行说明文档
- 真正运行时使用的参数，放在 `Config` 和 `config_registry`
