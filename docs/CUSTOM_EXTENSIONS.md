# 自定义扩展说明

本文档用于梳理当前仓库中由本地二次开发引入的扩展能力，帮助后续继续完善时明确边界、优先级和落地顺序。

## 一、扩展目标

当前自定义扩展主要围绕三条线展开：

1. 金银 / 中证500 的每日推送分析
2. 事件驱动的实时监控与提醒
3. 星象、天干地支等实验性特征研究

其中前两条已经开始接入主运行链路，第三条仍处于研究与结构预留阶段。

## 二、主项目与自定义扩展的边界

### 1. 原始主项目能力

主项目本身已经具备以下稳定能力：

- 个股分析主流程
- A股 / 港股 / 美股行情与新闻聚合
- Web API / Web UI / Bot
- 定时运行与通知推送
- 历史记录、回测、持仓管理

这些能力主要分布在以下位置：

- [main.py](/Users/laitdieu/Documents/github/daily_stock_analysis/main.py)
- [src/core/pipeline.py](/Users/laitdieu/Documents/github/daily_stock_analysis/src/core/pipeline.py)
- [src/config.py](/Users/laitdieu/Documents/github/daily_stock_analysis/src/config.py)
- [api](/Users/laitdieu/Documents/github/daily_stock_analysis/api)
- [apps/dsa-web/src](/Users/laitdieu/Documents/github/daily_stock_analysis/apps/dsa-web/src)

### 2. 自定义扩展能力

当前新增内容主要在以下文件或目录：

- [src/daily_push_pipeline.py](/Users/laitdieu/Documents/github/daily_stock_analysis/src/daily_push_pipeline.py)
- [src/market_data_fetcher.py](/Users/laitdieu/Documents/github/daily_stock_analysis/src/market_data_fetcher.py)
- [src/agent/events.py](/Users/laitdieu/Documents/github/daily_stock_analysis/src/agent/events.py)
- [src/models/metaphysical_features.py](/Users/laitdieu/Documents/github/daily_stock_analysis/src/models/metaphysical_features.py)
- [strategies/asset_direction_policy.yaml](/Users/laitdieu/Documents/github/daily_stock_analysis/strategies/asset_direction_policy.yaml)
- [strategies/ic_basis_roll_framework.yaml](/Users/laitdieu/Documents/github/daily_stock_analysis/strategies/ic_basis_roll_framework.yaml)
- [strategies/gold_long_accumulation.yaml](/Users/laitdieu/Documents/github/daily_stock_analysis/strategies/gold_long_accumulation.yaml)
- [strategies/silver_dual_direction_framework.yaml](/Users/laitdieu/Documents/github/daily_stock_analysis/strategies/silver_dual_direction_framework.yaml)
- [strategies/geopolitical_risk_alert.yaml](/Users/laitdieu/Documents/github/daily_stock_analysis/strategies/geopolitical_risk_alert.yaml)
- [strategies/macro_cycle_positioning.yaml](/Users/laitdieu/Documents/github/daily_stock_analysis/strategies/macro_cycle_positioning.yaml)
- [strategies/wyckoff_analysis.yaml](/Users/laitdieu/Documents/github/daily_stock_analysis/strategies/wyckoff_analysis.yaml)

## 三、当前扩展状态

### A. 已接入主流程的扩展

#### 1. 每日市场品种推送

文件：

- [src/daily_push_pipeline.py](/Users/laitdieu/Documents/github/daily_stock_analysis/src/daily_push_pipeline.py)

当前职责：

- 在个股分析前推送黄金、白银、中证500
- 结合历史K线和技术指标生成结构化摘要
- 可选调用 LLM 生成短评

当前特点：

- 已经进入 [main.py](/Users/laitdieu/Documents/github/daily_stock_analysis/main.py) 的主流程
- 本质上属于“主系统上的增强推送层”
- 和原个股分析系统共享通知链路

#### 2. 市场数据抓取层

文件：

- [src/market_data_fetcher.py](/Users/laitdieu/Documents/github/daily_stock_analysis/src/market_data_fetcher.py)

当前职责：

- 通过 Jin10 MCP 获取黄金 / 白银报价
- 通过 AkShare 获取金银 / IC / 中证500 历史数据
- 为每日推送与事件监控提供统一数据入口

当前特点：

- 已经是自定义扩展中的核心基础设施
- 后续应继续作为“统一数据层”复用，避免在别处重复写临时抓取逻辑

#### 3. 实时事件监控

文件：

- [src/agent/events.py](/Users/laitdieu/Documents/github/daily_stock_analysis/src/agent/events.py)

当前新增能力：

- `basis_spike`：IC 贴水异动
- `price_spike`：金银价格异动
- `news_impact`：重大事件快讯影响

当前特点：

- 已经挂到调度体系上
- 已进入“可运行功能”范围
- 但仍属于增强模块，不应和原始股票主链路强耦合

### B. 策略层增强

#### 1. 资产方向偏好策略

文件：

- [strategies/asset_direction_policy.yaml](/Users/laitdieu/Documents/github/daily_stock_analysis/strategies/asset_direction_policy.yaml)
- [strategies/ic_basis_roll_framework.yaml](/Users/laitdieu/Documents/github/daily_stock_analysis/strategies/ic_basis_roll_framework.yaml)
- [strategies/gold_long_accumulation.yaml](/Users/laitdieu/Documents/github/daily_stock_analysis/strategies/gold_long_accumulation.yaml)
- [strategies/silver_dual_direction_framework.yaml](/Users/laitdieu/Documents/github/daily_stock_analysis/strategies/silver_dual_direction_framework.yaml)
- 运行说明：[docs/ASSET_OPERATIONS.md](/Users/laitdieu/Documents/github/daily_stock_analysis/docs/ASSET_OPERATIONS.md)

当前定位：

- `asset_direction_policy` 是总入口策略
- IC、黄金、白银已经拆成独立子策略文件
- 运行说明文档单独维护，不再和策略约束混写

当前特点：

- 已拆分为“总入口 + 子策略 + 运行说明”三层
- `ic_basis_roll_framework` 里已加入一条待审的 `next-production candidate` 执行层：
  当 `趋势破坏 + 单日弱势` 成立且基础仓位已处于半仓时，优先观察
  `空仓2日后再回到原框架` 这条候选节奏
- 这条结论当前只允许以“候选执行规则 / 待审执行层”的口径出现，不应直接当作正式生产默认动作
- 本地治理账本与摘要也已单独落地，用于明确记录：
  - 证据来自哪两份回测报告
  - 当前阶段是否仍为 `candidate`
  - 当前审核状态与升格门槛
  默认位置：
  - [reports/ic_candidate_execution_governance_runs.jsonl](/Users/laitdieu/Documents/github/daily_stock_analysis/reports/ic_candidate_execution_governance_runs.jsonl)
  - [reports/backtests/latest_ic_candidate_execution_governance.md](/Users/laitdieu/Documents/github/daily_stock_analysis/reports/backtests/latest_ic_candidate_execution_governance.md)
- `M1-M2前端塌陷` 现已单独进入 `shadow_monitoring` 监控层：
  - 继续沿用实时快照里的 `csi500_term_structure_shadow_signal`
  - 但额外生成一份可追溯账本和摘要，专门记录“若盘中亮灯，则记一笔纸面买入下月虚值 Put 保护”的事件
  默认位置：
  - [reports/ic_m1_m2_shadow_monitoring_events.jsonl](/Users/laitdieu/Documents/github/daily_stock_analysis/reports/ic_m1_m2_shadow_monitoring_events.jsonl)
  - [reports/backtests/latest_ic_m1_m2_shadow_monitoring.md](/Users/laitdieu/Documents/github/daily_stock_analysis/reports/backtests/latest_ic_m1_m2_shadow_monitoring.md)
- 当前总策略 YAML 负责资产方向和跨资产约束
- 子策略文件负责单一资产的细化逻辑
- 运行层说明迁移到独立文档，避免一个 YAML 同时承载策略、配置说明和产品流程

#### 2. 地缘 / 宏观 / 威科夫策略

文件：

- [strategies/geopolitical_risk_alert.yaml](/Users/laitdieu/Documents/github/daily_stock_analysis/strategies/geopolitical_risk_alert.yaml)
- [strategies/macro_cycle_positioning.yaml](/Users/laitdieu/Documents/github/daily_stock_analysis/strategies/macro_cycle_positioning.yaml)
- [strategies/wyckoff_analysis.yaml](/Users/laitdieu/Documents/github/daily_stock_analysis/strategies/wyckoff_analysis.yaml)

当前定位：

- 作为问股 Agent 或扩展分析框架使用
- 提供更偏“中观 / 宏观 / 风险框架”的解释层

当前特点：

- 文本定义已经较完整
- 但工程落地还依赖主系统实际调用这些策略的方式
- 当前更接近“策略知识层”，不是稳定的执行模块

### C. 实验性研究模块

#### 1. 星象 / 天干地支特征

文件：

- [src/models/metaphysical_features.py](/Users/laitdieu/Documents/github/daily_stock_analysis/src/models/metaphysical_features.py)
- [src/models/metaphysical](/Users/laitdieu/Documents/github/daily_stock_analysis/src/models/metaphysical)

当前内容：

- 天干地支批量计算
- 行星黄经计算
- 冥王星插值
- 相位检测
- 触发器解释摘要

当前定位：

- 这是明显的实验性研究模块
- 目前适合用于离线研究、特征工程、回测实验
- 还不适合直接进入生产推送或主分析结论

当前原因：

- 依赖额外库 `sxtwl`、`ephem`
- 与主项目现有配置和测试体系尚未打通
- 业务解释和风险边界还没有完全定义

当前结构：

- `calendar.py`：天干地支计算层
- `astro.py`：星象计算层
- `signals.py`：触发器生成层
- `time_law.py`：年柱、十神、节气、火星事件等时间法则
- `gann.py`：江恩九宫方与时间循环
- `trend_law.py`：保力加通道与趋势状态判断
- `strategy.py`：轻量共振策略摘要
- `explainer.py`：研究结果解释层
- `service.py`：开关与统一入口层
- `adapter.py`：按日期并入行情 DataFrame 的适配层
- `resonance.py`：为共振回测输出兼容旧脚本的 `bazi_risk / astro_risk / resonance` 列
- `regime.py`：作者因子的动态 regime 推断与加权层
- `metaphysical_features.py`：兼容旧导入路径的转发入口

当前接线状态：

- 外层 `xgboost_tail_risk_v2.py` 已切到共享研究层入口
- 外层 `xgboost_feature_importance.py` 已切到共享研究层入口
- 外层 `resonance_backtest_v2.py` 已切到共享研究层的回测适配入口

当前推荐标准路径：

- 如果你只有日期序列，走 `build_metaphysical_features_if_enabled()`
- 如果你有原始行情表并要直接挂下一版生产候选，走 `attach_next_production_metaphysical_features()`
- 如果你已经算好了量化 / 共振列，想收口成统一候选池，走 `finalize_next_production_candidate_frame()`
- 如果你在写回测或训练脚本，优先走 `build_next_production_backtest_features()`

当前推荐候选层：

- `NEXT_PRODUCTION_WEIGHTED_CANDIDATE_FEATURES`
- 含义：当前高价值量化 / 共振基线 + 动态 `Regime-aware` 作者核心因子

当前推荐候选生产模式：

- `NEXT_PRODUCTION_MODEL_DEFAULTS`
- `apply_next_production_position_sizing()`
- `apply_tactical_report_signal_overlay()`
- `build_next_production_signal_frame()`
- `latest_next_production_signal()`

当前日报纠偏逻辑：

- 每日 Google Doc 可先走 `parse_tactical_report_text()` / `build_tactical_report_optimization_notes()`
- 如果日报明显偏空但模型仍给出 `full_risk`，可用 `apply_tactical_report_signal_overlay()` 做一层文本风险覆盖
- 当前默认覆盖规则：
  - `report_risk_score >= 3` 且原始信号为 `full_risk`：下调到 `caution`
  - `report_risk_score >= 5` 且 `black_swan_warning + physical_blockade` 同时出现：强制切到 `risk_off`

当前推荐轻量执行入口：

- [scripts/generate_next_production_signal.py](/Users/laitdieu/Documents/github/daily_stock_analysis/scripts/generate_next_production_signal.py)
- 用途：直接读取已有概率缓存，输出最新原始信号和日报纠偏后的最终信号
- 推荐搭配：
  - `--tactical-report-file <当天 Google Doc 导出的 UTF-8 文本>`

当前主日报接线：

- [main.py](/Users/laitdieu/Documents/github/daily_stock_analysis/main.py) 的合并日报摘要仍会尝试读取外部战术报告
- 主日报 `今日结论` 当前只保留市场与外部战术结论，不再混入玄学模型治理细节，避免阅读上互相干扰
- `resolve_next_production_model_params()`
- 含义：
  在 `NEXT_PRODUCTION_WEIGHTED_CANDIDATE_FEATURES` 之上，统一用共享默认参数把 `tail_risk_probability` 映射成 `full_risk / caution / risk_off`

当前独立玄学日报入口：

- [scripts/generate_metaphysical_daily_report.py](/Users/laitdieu/Documents/github/daily_stock_analysis/scripts/generate_metaphysical_daily_report.py)
- 用途：从概率缓存、governance/lifecycle/stage-performance/switch-proposal 账本生成一份独立“玄学治理日报”
- 默认输出包括：
  - 模型信号
  - 治理动作
  - 生命周期
  - 阶段健康
  - 切换草案
  - 确认稿
  - 变更单
- 推荐搭配：
  - `--tactical-report-file <当天 Google Doc 导出的 UTF-8 文本>`
- 主流程接线：
  - 每日主报告生成后，会额外保存一份独立玄学治理日报到桌面目录 [玄学治理日报](/Users/laitdieu/Desktop/玄学治理日报)
  - 桌面目录只保留最近 `3` 天，旧文件自动删除
  - 后台长期归档保留在 [reports/metaphysical_daily_archive](/Users/laitdieu/Documents/github/daily_stock_analysis/reports/metaphysical_daily_archive)，不会因为桌面清理而删除
  - 当前已支持脱离 Codex 的本地自动执行：
    - 执行脚本：[run_metaphysical_local_scheduler.sh](/Users/laitdieu/Documents/github/daily_stock_analysis/scripts/run_metaphysical_local_scheduler.sh)
    - 本地任务：[~/Library/LaunchAgents/com.laitdieu.daily-stock-analysis.metaphysical.plist](/Users/laitdieu/Library/LaunchAgents/com.laitdieu.daily-stock-analysis.metaphysical.plist)
    - 安装脚本：[install_metaphysical_launchagent.sh](/Users/laitdieu/Documents/github/daily_stock_analysis/scripts/install_metaphysical_launchagent.sh)
    - 状态检查：[check_metaphysical_local_scheduler.py](/Users/laitdieu/Documents/github/daily_stock_analysis/scripts/check_metaphysical_local_scheduler.py)
  - 如果需要“未登录也执行”，当前仓库已提供 `LaunchDaemon` 模板：
    - [com.laitdieu.daily-stock-analysis.metaphysical.daemon.plist.template](/Users/laitdieu/Documents/github/daily_stock_analysis/scripts/com.laitdieu.daily-stock-analysis.metaphysical.daemon.plist.template)
    - 安装脚本：[install_metaphysical_launchdaemon.sh](/Users/laitdieu/Documents/github/daily_stock_analysis/scripts/install_metaphysical_launchdaemon.sh)
    - 但这一步需要写入 `/Library/LaunchDaemons`，通常需要手工 `sudo` 安装

当前自动治理账本：

- `reports/metaphysical_governance_runs.jsonl`
- `reports/metaphysical_stage_performance_runs.jsonl`
- `reports/metaphysical_lifecycle_runs.jsonl`
- `reports/metaphysical_version_switch_proposals.jsonl`

当前自动治理输出层级：

- `evaluate_governance_stage_flow()`：只回答当前阶段理论上该往哪一层走
- `evaluate_stage_guardrail()`：只回答当前阶段真实表现是否健康
- `evaluate_release_lifecycle()`：合并升版逻辑与阶段 guardrail，给出最终生命周期动作
- `build_version_switch_proposal()`：把生命周期动作转成待确认切换草案
- `build_version_switch_execution_plan()`：把切换草案展开成执行前检查项
- `build_version_switch_confirmation_draft()`：生成待确认的确认稿摘要
- `build_version_switch_change_request()`：生成更像正式变更单的模板，列出受影响入口、默认参数和回滚点

当前固定周报入口：

- [scripts/generate_metaphysical_weekly_summary.py](/Users/laitdieu/Documents/github/daily_stock_analysis/scripts/generate_metaphysical_weekly_summary.py)
- 用途：把 learning / governance / lifecycle / stage performance / switch proposal 这几层账本统一整理成一页“玄学模型周治理摘要”

最小训练示例：

- [scripts/train_next_production_metaphysical_example.py](/Users/laitdieu/Documents/github/daily_stock_analysis/scripts/train_next_production_metaphysical_example.py)
- 作用：演示如何从行情数据出发，构建 `build_next_production_backtest_features()` 所需输入并训练一个最小模型

模型驱动回测默认候选：

- [scripts/backtest_next_production_metaphysical_model.py](/Users/laitdieu/Documents/github/daily_stock_analysis/scripts/backtest_next_production_metaphysical_model.py)
- 当前默认参数已经固化到 `NEXT_PRODUCTION_MODEL_DEFAULTS`
- 默认训练节奏：`min_train_days=756`、`retrain_every=42`
- 默认仓位阈值：`caution_threshold=0.40`、`risk_off_threshold=0.60`
- 当前默认方案含义：
  使用更长训练窗 + 较低频重训，把 `NEXT_PRODUCTION_WEIGHTED_CANDIDATE_FEATURES` 变成尾部风险概率，再映射为 `1.0 / 0.5 / 0.0` 三档仓位

## 四、建议的分层

后续建议按下面的方式给自定义扩展分层：

### 1. 生产层

可以直接服务于日常运行和推送：

- `daily_push_pipeline`
- `market_data_fetcher`
- `EventMonitor` 中与金银 / IC / Jin10 相关的能力

### 2. 策略层

为问股、解释和决策提供约束或观点：

- `asset_direction_policy`
- `ic_basis_roll_framework`
- `gold_long_accumulation`
- `silver_dual_direction_framework`
- `geopolitical_risk_alert`
- `macro_cycle_positioning`
- `wyckoff_analysis`

### 3. 研究层

仅用于实验、回测、特征工程：

- `metaphysical_features`
- 外层独立实验脚本，如 `xgboost_*.py`、`resonance_backtest*.py`

## 五、后续补齐顺序

按当前仓库状态，建议严格按下面顺序推进。

### 第一步：固定边界

目标：

- 明确哪些扩展属于生产能力，哪些属于实验性能力
- 生产能力默认可运行，实验性能力默认关闭

建议动作：

- 给自定义扩展建立统一文档
- 给实验性能力增加明显标记
- 避免实验性模块直接影响主分析流程

本文档即为这一步的起点。

### 第二步：补齐配置

目标：

- 所有新增功能都进入统一 `Config`
- 不再依赖隐式环境、硬编码路径或口头约定

优先补的配置项：

- `JIN10_API_KEY`
- `CLOSE_REMINDER_ENABLED`
- `CLOSE_REMINDER_TIME`
- 未来如接入星象实验开关，建议新增：
  - `ENABLE_METAPHYSICAL_FEATURES`
  - `METAPHYSICAL_CACHE_DIR`

### 第三步：补测试

目标：

- 先覆盖最容易坏的接缝，而不是一开始追求全量覆盖

建议优先测试：

- `daily_push_pipeline` 的数据缺失与 fallback
- `market_data_fetcher` 的无 key / 有 key / 失败回退
- `EventMonitor` 三类新增告警
- 配置加载与默认值

### 第四步：再考虑产品化整理

目标：

- 把策略说明、监控规则、产品流程说明逐步拆开

优先方向：

- 将 `asset_direction_policy.yaml` 中的“策略约束”和“调度/提醒说明”拆分
- 将星象/天干地支特征从“代码存在”推进到“明确只用于研究”
- 如后续验证有效，再考虑研究层向策略层迁移

## 六、当前建议

现阶段最合适的判断是：

- 金银 / 白银 / 中证500 推送链路已经具备继续工程化的价值
- 事件驱动提醒已经值得继续补配置和测试
- 星象 / 天干地支现在更适合作为研究特征保留，不建议直接提升到生产主链路

## 七、下一步执行建议

建议下一个迭代直接做“第二步：补齐配置”，把自定义扩展全部纳入统一配置体系，并给研究模块加开关。
