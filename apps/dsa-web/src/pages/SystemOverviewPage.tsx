import React, { useEffect, useState } from 'react';
import { Activity, AlertTriangle, Archive, BellRing, CheckCircle2, ClipboardList, Clock3, Database, FileText, HardDrive, RefreshCw, Server, Settings2, ShieldCheck } from 'lucide-react';
import { Link } from 'react-router-dom';
import type { ParsedApiError } from '../api/error';
import { getParsedApiError } from '../api/error';
import { systemOverviewApi } from '../api/systemOverview';
import { ApiErrorAlert } from '../components/common';
import type { SystemOverview, SystemOverviewAlert, SystemOverviewFileInfo, SystemOverviewModule, SystemOverviewPriority, SystemOverviewRecommendation, SystemOverviewServiceStatus } from '../types/systemOverview';

function statusClass(status: string): string {
  if (status === 'active') return 'border-emerald-300/60 bg-emerald-50/80 text-emerald-950';
  if (status === 'shadow') return 'border-sky-300/60 bg-sky-50/80 text-sky-950';
  if (status === 'silent') return 'border-slate-300/70 bg-slate-50/85 text-slate-900';
  return 'border-amber-300/60 bg-amber-50/85 text-amber-950';
}

function priorityClass(priority: string): string {
  if (priority === 'P0') return 'bg-red-100 text-red-800 border-red-200';
  if (priority === 'P1') return 'bg-orange-100 text-orange-800 border-orange-200';
  if (priority === 'P2') return 'bg-emerald-100 text-emerald-800 border-emerald-200';
  if (priority === 'P3') return 'bg-sky-100 text-sky-800 border-sky-200';
  if (priority === 'P4') return 'bg-slate-100 text-slate-800 border-slate-200';
  return 'bg-indigo-100 text-indigo-800 border-indigo-200';
}

function alertClass(level: string): string {
  if (level === 'critical') return 'border-red-300/70 bg-red-50/85 text-red-950';
  if (level === 'warning') return 'border-amber-300/70 bg-amber-50/85 text-amber-950';
  return 'border-emerald-300/70 bg-emerald-50/85 text-emerald-950';
}

function formatDateTime(value?: string | null): string {
  if (!value) return '暂无';
  return value.replace('T', ' ').slice(0, 16);
}

function asText(value: unknown, fallback = '暂无'): string {
  if (value === null || value === undefined || value === '') return fallback;
  return String(value);
}

const RecommendationCard: React.FC<{ item: SystemOverviewRecommendation }> = ({ item }) => {
  const isSuccess = item.level === 'success';
  return (
    <div className={`rounded-2xl border px-4 py-3 ${isSuccess ? 'border-emerald-300/60 bg-emerald-50/80 text-emerald-950' : 'border-amber-300/60 bg-amber-50/80 text-amber-950'}`}>
      <div className="flex gap-3">
        {isSuccess ? <CheckCircle2 className="mt-0.5 h-4 w-4 shrink-0" /> : <AlertTriangle className="mt-0.5 h-4 w-4 shrink-0" />}
        <div>
          <p className="text-sm font-semibold">{item.title}</p>
          <p className="mt-1 text-xs leading-5 opacity-90">{item.description}</p>
        </div>
      </div>
    </div>
  );
};

const ModuleCard: React.FC<{ item: SystemOverviewModule }> = ({ item }) => (
  <div className={`rounded-3xl border p-5 shadow-soft-card ${statusClass(item.status)}`}>
    <div className="flex items-start justify-between gap-3">
      <div>
        <span className={`inline-flex rounded-full border px-2.5 py-1 text-[11px] font-semibold ${priorityClass(item.priority)}`}>
          {item.priority}
        </span>
        <h3 className="mt-3 text-base font-semibold">{item.name}</h3>
      </div>
      <ShieldCheck className="h-5 w-5 opacity-70" />
    </div>
    <p className="mt-3 text-sm leading-6 opacity-90">{item.detail}</p>
    <div className="mt-4 rounded-2xl bg-white/55 px-3 py-2 text-xs leading-5">
      <p><span className="font-semibold">提醒规则：</span>{item.notifyRule}</p>
      <p className="mt-1 break-all"><span className="font-semibold">归档：</span>{item.archivePath}</p>
    </div>
  </div>
);

const ServiceCard: React.FC<{ item: SystemOverviewServiceStatus }> = ({ item }) => (
  <div className={`rounded-3xl border p-5 shadow-soft-card ${statusClass(item.status)}`}>
    <div className="flex items-start justify-between gap-3">
      <div>
        <p className="text-sm font-semibold">{item.name}</p>
        <p className="mt-1 text-xs opacity-80">{item.status === 'active' ? '正常' : item.status === 'critical' ? '异常' : '需确认'}</p>
      </div>
      <Server className="h-5 w-5 opacity-70" />
    </div>
    <p className="mt-4 break-all text-sm leading-6 opacity-90">{item.detail}</p>
    <p className="mt-3 text-xs opacity-75">PID: {item.pid ?? '无'} | 更新: {formatDateTime(item.updatedAt)}</p>
  </div>
);

const AlertCard: React.FC<{ item: SystemOverviewAlert }> = ({ item }) => (
  <div className={`rounded-2xl border px-4 py-3 ${alertClass(item.level)}`}>
    <div className="flex gap-3">
      <AlertTriangle className="mt-0.5 h-4 w-4 shrink-0" />
      <div>
        <p className="text-sm font-semibold">{item.title}</p>
        <p className="mt-1 text-xs leading-5 opacity-90">{item.description}</p>
      </div>
    </div>
  </div>
);

const PriorityRow: React.FC<{ item: SystemOverviewPriority }> = ({ item }) => (
  <tr className="border-b border-border/50 last:border-0">
    <td className="py-3 pr-3">
      <span className={`inline-flex rounded-full border px-2.5 py-1 text-xs font-semibold ${priorityClass(item.priority)}`}>
        {item.priority}
      </span>
    </td>
    <td className="py-3 pr-3 text-sm font-medium text-foreground">{item.label}</td>
    <td className="py-3 pr-3 text-sm text-secondary-text">{item.notifyRule}</td>
    <td className="py-3 text-sm text-secondary-text">{item.archiveRule}</td>
  </tr>
);

const FileCard: React.FC<{ item: SystemOverviewFileInfo }> = ({ item }) => (
  <div className="rounded-2xl border border-border/70 bg-card/75 p-4 shadow-soft-card">
    <div className="flex items-start justify-between gap-3">
      <div>
        <p className="text-sm font-semibold text-foreground">{item.label}</p>
        <p className={`mt-1 text-xs ${item.exists ? 'text-emerald-600' : 'text-amber-600'}`}>{item.exists ? '已找到' : '未找到'}</p>
      </div>
      <FileText className="h-5 w-5 text-secondary-text" />
    </div>
    <div className="mt-4 flex items-end justify-between gap-3">
      <p className="text-xl font-semibold text-foreground">{item.sizeLabel}</p>
      <p className="text-xs text-secondary-text">{item.fileCount} 个文件</p>
    </div>
    <p className="mt-2 text-xs text-secondary-text">更新：{formatDateTime(item.modifiedAt)}</p>
    <p className="mt-3 break-all text-[11px] leading-5 text-secondary-text">{item.path}</p>
  </div>
);

const SystemShortcut: React.FC<{ title: string; description: string; to: string; icon: React.ReactNode }> = ({
  title,
  description,
  to,
  icon,
}) => (
  <Link
    to={to}
    className="group flex items-start gap-3 rounded-3xl border border-border/70 bg-card/80 p-5 shadow-soft-card transition hover:-translate-y-0.5 hover:border-cyan/40 hover:bg-hover/70"
  >
    <div className="flex h-11 w-11 shrink-0 items-center justify-center rounded-2xl bg-slate-900 text-white shadow-sm">
      {icon}
    </div>
    <div>
      <p className="text-sm font-semibold text-foreground group-hover:text-cyan-700">{title}</p>
      <p className="mt-2 text-xs leading-5 text-secondary-text">{description}</p>
    </div>
  </Link>
);

const SystemOverviewPage: React.FC = () => {
  const [overview, setOverview] = useState<SystemOverview | null>(null);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [error, setError] = useState<ParsedApiError | null>(null);

  const loadOverview = async (mode: 'initial' | 'refresh' = 'initial') => {
    if (mode === 'initial') setLoading(true);
    if (mode === 'refresh') setRefreshing(true);
    setError(null);
    try {
      const payload = await systemOverviewApi.getOverview();
      setOverview(payload);
    } catch (err) {
      setError(getParsedApiError(err));
    } finally {
      setLoading(false);
      setRefreshing(false);
    }
  };

  useEffect(() => {
    void loadOverview();
  }, []);

  if (loading) {
    return (
      <div className="flex min-h-[60vh] items-center justify-center">
        <div className="h-8 w-8 animate-spin rounded-full border-2 border-cyan/20 border-t-cyan" />
      </div>
    );
  }

  if (!overview) {
    return (
      <div className="space-y-4">
        {error ? <ApiErrorAlert error={error} /> : null}
        <button type="button" className="btn-primary" onClick={() => void loadOverview('refresh')}>重试</button>
      </div>
    );
  }

  const scheduler = overview.scheduler || {};
  const services = overview.services || [];
  const alerts = overview.alerts || [];
  const warehouse = overview.dataWarehouse || {};
  const database = warehouse.database || {};
  const reports = warehouse.reports || {};
  const healthArchive = warehouse.healthArchive || {};
  const disk = warehouse.disk || {};

  return (
    <div className="mx-auto max-w-7xl space-y-6 px-4 py-6 sm:px-6 lg:px-8">
      <section className="rounded-[2rem] border border-border/70 bg-[radial-gradient(circle_at_0%_0%,rgba(14,165,233,0.16),transparent_34%),linear-gradient(135deg,rgba(255,255,255,0.92),rgba(248,250,252,0.78))] p-6 shadow-soft-card">
        <div className="flex flex-col gap-5 lg:flex-row lg:items-center lg:justify-between">
          <div>
            <div className="inline-flex items-center gap-2 rounded-full border border-cyan/20 bg-cyan/10 px-3 py-1 text-xs font-semibold text-cyan-800">
              <Activity className="h-3.5 w-3.5" />
              系统控制面
            </div>
            <h1 className="mt-4 text-3xl font-semibold tracking-tight text-foreground">系统总览</h1>
            <p className="mt-2 max-w-3xl text-sm leading-6 text-secondary-text">
              只展示运行状态，不触发分析、不发送通知。这里用于确认 P0-P4 路由、Gemini 缓存、IC Shadow、桌面报告和后台调度是否处于预期状态。
            </p>
          </div>
          <button
            type="button"
            className="btn-primary inline-flex items-center gap-2"
            disabled={refreshing}
            onClick={() => void loadOverview('refresh')}
          >
            <RefreshCw className={`h-4 w-4 ${refreshing ? 'animate-spin' : ''}`} />
            刷新总览
          </button>
        </div>
      </section>

      <section>
        <div className="mb-4 flex items-center gap-2">
          <Settings2 className="h-5 w-5 text-slate-700" />
          <h2 className="text-lg font-semibold text-foreground">系统工具入口</h2>
        </div>
        <div className="grid gap-4 md:grid-cols-3">
          <SystemShortcut
            title="数据中心"
            description="行情、持仓、回测、报告和缓存维护集中在这里。"
            to="/data-center"
            icon={<Database className="h-5 w-5" />}
          />
          <SystemShortcut
            title="每日复盘"
            description="查看历史复盘、持仓复盘和每日记录，不占用左侧高频导航。"
            to="/reviews"
            icon={<ClipboardList className="h-5 w-5" />}
          />
          <SystemShortcut
            title="系统设置"
            description="模型、通知、数据源、安全认证和自动化配置统一管理。"
            to="/settings"
            icon={<Settings2 className="h-5 w-5" />}
          />
        </div>
      </section>

      {error ? <ApiErrorAlert error={error} /> : null}
      {alerts.length > 0 ? (
        <section className="grid gap-3 lg:grid-cols-2">
          {alerts.map((item) => (
            <AlertCard key={`${item.level}-${item.title}`} item={item} />
          ))}
        </section>
      ) : (
        <section className="rounded-2xl border border-emerald-300/60 bg-emerald-50/75 px-4 py-3 text-sm font-semibold text-emerald-900">
          当前没有失败提醒。
        </section>
      )}

      <section>
        <div className="mb-4 flex items-center gap-2">
          <Server className="h-5 w-5 text-cyan-700" />
          <h2 className="text-lg font-semibold text-foreground">工作站健康</h2>
        </div>
        <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
          {services.map((item) => (
            <ServiceCard key={item.key} item={item} />
          ))}
        </div>
      </section>

      <section className="grid gap-4 md:grid-cols-3">
        <div className="rounded-3xl border border-border/70 bg-card/80 p-5 shadow-soft-card">
          <div className="flex items-center gap-3">
            <Clock3 className="h-5 w-5 text-cyan-700" />
            <p className="text-sm font-semibold text-foreground">后台调度</p>
          </div>
          <p className="mt-4 text-2xl font-semibold text-foreground">{scheduler.launchAgentAlive || scheduler.lockAlive ? '运行中' : '未确认'}</p>
          <p className="mt-2 text-xs leading-5 text-secondary-text">PID: {scheduler.launchAgentPid ?? scheduler.lockPid ?? '无'} | 日报: {scheduler.scheduleTime || '未配置'}</p>
        </div>
        <div className="rounded-3xl border border-border/70 bg-card/80 p-5 shadow-soft-card">
          <div className="flex items-center gap-3">
            <BellRing className="h-5 w-5 text-emerald-700" />
            <p className="text-sm font-semibold text-foreground">22:30预判</p>
          </div>
          <p className="mt-4 text-2xl font-semibold text-foreground">{scheduler.nightlyMarketOutlookEnabled ? '已启用' : '未启用'}</p>
          <p className="mt-2 text-xs leading-5 text-secondary-text">执行时间: {scheduler.nightlyMarketOutlookTime || '22:30'}</p>
        </div>
        <div className="rounded-3xl border border-border/70 bg-card/80 p-5 shadow-soft-card">
          <div className="flex items-center gap-3">
            <Archive className="h-5 w-5 text-indigo-700" />
            <p className="text-sm font-semibold text-foreground">盘中雷达</p>
          </div>
          <p className="mt-4 text-2xl font-semibold text-foreground">{scheduler.stockIntradayReminderEnabled ? '已启用' : '未启用'}</p>
          <p className="mt-2 text-xs leading-5 text-secondary-text">持仓 P1 / 自选 P2 统一路由</p>
        </div>
      </section>

      <section>
        <div className="mb-4 flex items-center gap-2">
          <Database className="h-5 w-5 text-indigo-700" />
          <h2 className="text-lg font-semibold text-foreground">长期数据沉淀</h2>
        </div>
        <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
          <div className="rounded-3xl border border-border/70 bg-card/80 p-5 shadow-soft-card">
            <Database className="h-5 w-5 text-indigo-700" />
            <p className="mt-3 text-sm font-semibold text-foreground">本地数据库</p>
            <p className="mt-3 text-2xl font-semibold text-foreground">{asText(database.sizeLabel, '0 B')}</p>
            <p className="mt-2 text-xs leading-5 text-secondary-text">分析 {asText((database.tables as Record<string, unknown> | undefined)?.analysisHistory ?? (database.tables as Record<string, unknown> | undefined)?.analysis_history, '0')} | 回测 {asText((database.tables as Record<string, unknown> | undefined)?.backtestResults ?? (database.tables as Record<string, unknown> | undefined)?.backtest_results, '0')}</p>
          </div>
          <div className="rounded-3xl border border-border/70 bg-card/80 p-5 shadow-soft-card">
            <Archive className="h-5 w-5 text-emerald-700" />
            <p className="mt-3 text-sm font-semibold text-foreground">报告归档</p>
            <p className="mt-3 text-2xl font-semibold text-foreground">{asText(reports.fileCount, '0')} 个文件</p>
            <p className="mt-2 text-xs leading-5 text-secondary-text">最近：{formatDateTime(asText(reports.modifiedAt, '') || null)}</p>
          </div>
          <div className="rounded-3xl border border-border/70 bg-card/80 p-5 shadow-soft-card">
            <Activity className="h-5 w-5 text-cyan-700" />
            <p className="mt-3 text-sm font-semibold text-foreground">健康账本</p>
            <p className="mt-3 text-2xl font-semibold text-foreground">{asText(healthArchive.fileCount, '0')} 天</p>
            <p className="mt-2 text-xs leading-5 text-secondary-text">最近：{formatDateTime(asText(healthArchive.modifiedAt, '') || null)}</p>
          </div>
          <div className="rounded-3xl border border-border/70 bg-card/80 p-5 shadow-soft-card">
            <HardDrive className="h-5 w-5 text-slate-700" />
            <p className="mt-3 text-sm font-semibold text-foreground">磁盘空间</p>
            <p className="mt-3 text-2xl font-semibold text-foreground">{asText(disk.freeLabel)}</p>
            <p className="mt-2 text-xs leading-5 text-secondary-text">剩余 {asText(disk.freePct)}%</p>
          </div>
        </div>
      </section>

      <section className="grid gap-4 md:grid-cols-2 xl:grid-cols-3">
        {overview.modules.map((item) => (
          <ModuleCard key={item.key} item={item} />
        ))}
      </section>

      <section className="grid gap-6 lg:grid-cols-[1fr_0.8fr]">
        <div className="rounded-3xl border border-border/70 bg-card/80 p-5 shadow-soft-card">
          <h2 className="text-lg font-semibold text-foreground">P0-P4 路由优先级</h2>
          <div className="mt-4 overflow-x-auto">
            <table className="w-full min-w-[640px] border-collapse">
              <thead>
                <tr className="border-b border-border/70 text-left text-xs text-secondary-text">
                  <th className="pb-3 pr-3">级别</th>
                  <th className="pb-3 pr-3">来源</th>
                  <th className="pb-3 pr-3">提醒</th>
                  <th className="pb-3">归档</th>
                </tr>
              </thead>
              <tbody>
                {overview.priorities.map((item) => (
                  <PriorityRow key={item.priority} item={item} />
                ))}
              </tbody>
            </table>
          </div>
        </div>

        <div className="space-y-3">
          {overview.recommendations.map((item) => (
            <RecommendationCard key={item.title} item={item} />
          ))}
        </div>
      </section>

      <section>
        <h2 className="text-lg font-semibold text-foreground">关键文件与归档</h2>
        <div className="mt-4 grid gap-4 md:grid-cols-2 xl:grid-cols-3">
          {overview.files.map((item) => (
            <FileCard key={item.key} item={item} />
          ))}
        </div>
      </section>
    </div>
  );
};

export default SystemOverviewPage;
