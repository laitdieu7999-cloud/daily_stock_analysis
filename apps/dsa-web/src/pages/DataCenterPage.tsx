import React, { useEffect, useState } from 'react';
import {
  AlertTriangle,
  CheckCircle2,
  Cloud,
  Database,
  FileArchive,
  Gauge,
  HardDrive,
  PlayCircle,
  RefreshCw,
  ShieldCheck,
  Trash2,
} from 'lucide-react';
import type { ParsedApiError } from '../api/error';
import { getParsedApiError } from '../api/error';
import { dataCenterApi } from '../api/dataCenter';
import { ApiErrorAlert } from '../components/common';
import type {
  DataCenterFileInfo,
  DataCenterCleanupResponse,
  DataCenterMarketDataRefreshResponse,
  DataCenterOverview,
  DataCenterPortfolioBacktestItem,
  DataCenterPortfolioBacktestResponse,
  DataCenterPortfolioReviewResponse,
  DataCenterRecommendation,
} from '../types/dataCenter';

type MetricCard = {
  title: string;
  value: string;
  detail: string;
  accent: string;
};

type BacktestConclusionTone = 'strong' | 'watch' | 'weak' | 'empty' | 'error';

type BacktestConclusion = {
  code: string;
  title: string;
  label: string;
  description: string;
  tone: BacktestConclusionTone;
  stats: string[];
};

function formatNumber(value: number | undefined): string {
  return typeof value === 'number' ? value.toLocaleString('zh-CN') : '0';
}

function formatBytes(value: number | undefined): string {
  const size = typeof value === 'number' ? value : 0;
  if (size < 1024) return `${size} B`;
  if (size < 1024 * 1024) return `${(size / 1024).toFixed(1)} KB`;
  if (size < 1024 * 1024 * 1024) return `${(size / 1024 / 1024).toFixed(1)} MB`;
  return `${(size / 1024 / 1024 / 1024).toFixed(1)} GB`;
}

function formatDateTime(value?: string | null): string {
  if (!value) return '暂无';
  return value.replace('T', ' ').slice(0, 16);
}

function formatOptionalPercent(value: unknown): string {
  return typeof value === 'number' ? `${value.toFixed(2)}%` : '暂无';
}

function getSummaryNumber(
  summary: Record<string, unknown> | null | undefined,
  key: string,
): number | undefined {
  const value = summary?.[key];
  return typeof value === 'number' && Number.isFinite(value) ? value : undefined;
}

function conclusionClass(tone: BacktestConclusionTone): string {
  if (tone === 'strong') {
    return 'border-emerald-300/50 bg-emerald-50/75 text-emerald-950';
  }
  if (tone === 'weak') {
    return 'border-rose-300/50 bg-rose-50/75 text-rose-950';
  }
  if (tone === 'error') {
    return 'border-red-300/50 bg-red-50/75 text-red-950';
  }
  if (tone === 'empty') {
    return 'border-slate-300/50 bg-slate-50/75 text-slate-800';
  }
  return 'border-amber-300/50 bg-amber-50/75 text-amber-950';
}

function buildBacktestConclusion(item: DataCenterPortfolioBacktestItem): BacktestConclusion {
  const winRate = getSummaryNumber(item.summary, 'winRatePct');
  const avgReturn = getSummaryNumber(item.summary, 'avgSimulatedReturnPct');
  const completed = item.completed || getSummaryNumber(item.summary, 'completedCount') || 0;
  const stats = [
    `完成样本 ${formatNumber(completed)}`,
    `胜率 ${formatOptionalPercent(winRate)}`,
    `平均模拟收益 ${formatOptionalPercent(avgReturn)}`,
  ];

  if (item.status === 'error' || item.errors > 0) {
    return {
      code: item.code,
      title: `${item.code}：回测执行失败`,
      label: '需要排查',
      description: item.message || '这只持仓的回测没有成功，需要先检查历史分析或行情数据是否完整。',
      tone: 'error',
      stats,
    };
  }

  if (item.candidateCount > 0 && completed === 0 && item.insufficient > 0) {
    return {
      code: item.code,
      title: `${item.code}：已有分析但回测未成熟`,
      label: '待成熟',
      description: '这只持仓已有历史分析记录，但后续行情时间或数据还不够，暂时不能计算胜率和收益。',
      tone: 'empty',
      stats,
    };
  }

  if (item.candidateCount === 0 || completed === 0) {
    return {
      code: item.code,
      title: `${item.code}：暂无历史分析`,
      label: '先分析',
      description: '这只持仓目前没有可用于回测的历史分析记录。后续多做几次分析后再看回测更有意义。',
      tone: 'empty',
      stats,
    };
  }

  if ((winRate ?? 0) >= 60 && (avgReturn ?? 0) >= 0) {
    return {
      code: item.code,
      title: `${item.code}：历史表现较好`,
      label: '优先关注',
      description: `历史回测胜率 ${formatOptionalPercent(winRate)}，平均模拟收益 ${formatOptionalPercent(avgReturn)}。可以继续作为重点持仓观察，但仍要结合当前行情和仓位。`,
      tone: 'strong',
      stats,
    };
  }

  if ((winRate ?? 100) < 45 || (avgReturn ?? 0) < 0) {
    return {
      code: item.code,
      title: `${item.code}：历史表现偏弱`,
      label: '需要谨慎',
      description: `历史回测胜率 ${formatOptionalPercent(winRate)}，平均模拟收益 ${formatOptionalPercent(avgReturn)}。后续更适合降低仓位敏感度，先观察是否有新的有效信号。`,
      tone: 'weak',
      stats,
    };
  }

  return {
    code: item.code,
    title: `${item.code}：结果中性`,
    label: '继续观察',
    description: `历史回测胜率 ${formatOptionalPercent(winRate)}，平均模拟收益 ${formatOptionalPercent(avgReturn)}。目前没有明显优势或劣势，适合结合趋势和成本继续观察。`,
    tone: 'watch',
    stats,
  };
}

function recommendationClass(level: string): string {
  if (level === 'warning') {
    return 'border-amber-300/50 bg-amber-50/70 text-amber-900';
  }
  if (level === 'success') {
    return 'border-emerald-300/50 bg-emerald-50/70 text-emerald-900';
  }
  return 'border-sky-300/50 bg-sky-50/70 text-sky-900';
}

function warehouseStatusLabel(status?: string): string {
  if (status === 'ok') return '正常';
  if (status === 'partial') return '部分成功';
  if (status === 'error') return '失败';
  if (status === 'missing') return '未运行';
  if (status === 'unreadable') return '记录异常';
  return '未知';
}

function qualityClass(status: string): string {
  if (status === 'missing') return 'border-rose-200/70 bg-rose-50/70 text-rose-950';
  if (status === 'stale') return 'border-amber-200/70 bg-amber-50/70 text-amber-950';
  if (status === 'skipped') return 'border-slate-200/80 bg-slate-50/80 text-slate-800';
  return 'border-emerald-200/70 bg-emerald-50/70 text-emerald-950';
}

function buildMetricCards(data: DataCenterOverview): MetricCard[] {
  return [
    {
      title: '行情数据',
      value: formatNumber(data.marketData.barCount),
      detail: `${formatNumber(data.marketData.stockCount)} 只标的，最新 ${data.marketData.latestDate || '暂无'}`,
      accent: 'from-cyan-500/14 to-blue-500/8',
    },
    {
      title: '持仓资产',
      value: formatNumber(data.portfolio.positionCount),
      detail: `${formatNumber(data.portfolio.activeAccountCount)} 个活跃账户，${formatNumber(data.portfolio.tradeCount)} 条交易`,
      accent: 'from-emerald-500/14 to-teal-500/8',
    },
    {
      title: '分析报告',
      value: formatNumber(data.analysis.reportCount),
      detail: `${formatNumber(data.analysis.stockCount)} 只股票，最近 ${formatDateTime(data.analysis.latestCreatedAt)}`,
      accent: 'from-indigo-500/14 to-slate-500/8',
    },
    {
      title: '回测结果',
      value: formatNumber(data.backtests.resultCount),
      detail: `${formatNumber(data.backtests.summaryCount)} 条汇总，最近 ${formatDateTime(data.backtests.latestEvaluatedAt)}`,
      accent: 'from-orange-500/14 to-amber-500/8',
    },
    {
      title: '新闻情报',
      value: formatNumber(data.news.itemCount),
      detail: `${formatNumber(data.news.stockCount)} 只股票，最近 ${formatDateTime(data.news.latestFetchedAt)}`,
      accent: 'from-rose-500/12 to-pink-500/8',
    },
    {
      title: '基本面快照',
      value: formatNumber(data.fundamentals.snapshotCount),
      detail: `${formatNumber(data.fundamentals.stockCount)} 只股票，最近 ${formatDateTime(data.fundamentals.latestCreatedAt)}`,
      accent: 'from-violet-500/12 to-fuchsia-500/8',
    },
  ];
}

const FileInventoryCard: React.FC<{ item: DataCenterFileInfo }> = ({ item }) => (
  <div className="rounded-2xl border border-border/70 bg-card/75 p-4 shadow-soft-card">
    <div className="flex items-start justify-between gap-3">
      <div>
        <p className="text-sm font-semibold text-foreground">{item.label}</p>
        <p className="mt-1 text-xs text-secondary-text">{item.exists ? '已找到' : '未找到'}</p>
      </div>
      <FileArchive className="h-5 w-5 text-secondary-text" />
    </div>
    <div className="mt-4 flex items-end justify-between gap-4">
      <div>
        <p className="text-2xl font-semibold text-foreground">{item.sizeLabel}</p>
        <p className="mt-1 text-xs text-secondary-text">{formatNumber(item.fileCount)} 个文件</p>
      </div>
    </div>
    <p className="mt-3 break-all text-[11px] leading-5 text-secondary-text">{item.path}</p>
  </div>
);

const RecommendationCard: React.FC<{ item: DataCenterRecommendation }> = ({ item }) => (
  <div className={`rounded-2xl border px-4 py-3 ${recommendationClass(item.level)}`}>
    <div className="flex items-start gap-3">
      {item.level === 'success' ? (
        <CheckCircle2 className="mt-0.5 h-4 w-4 shrink-0" />
      ) : (
        <AlertTriangle className="mt-0.5 h-4 w-4 shrink-0" />
      )}
      <div>
        <p className="text-sm font-semibold">{item.title}</p>
        <p className="mt-1 text-xs leading-5 opacity-90">{item.description}</p>
      </div>
    </div>
  </div>
);

const BacktestConclusionCard: React.FC<{ item: BacktestConclusion }> = ({ item }) => (
  <div className={`rounded-2xl border px-4 py-4 ${conclusionClass(item.tone)}`}>
    <div className="flex flex-col gap-3 sm:flex-row sm:items-start sm:justify-between">
      <div>
        <p className="text-sm font-semibold">{item.title}</p>
        <p className="mt-2 text-xs leading-5 opacity-90">{item.description}</p>
      </div>
      <span className="inline-flex shrink-0 items-center justify-center rounded-full border border-current/20 px-3 py-1 text-xs font-semibold">
        {item.label}
      </span>
    </div>
    <div className="mt-4 flex flex-wrap gap-2">
      {item.stats.map((stat) => (
        <span key={`${item.code}-${stat}`} className="rounded-full bg-white/55 px-3 py-1 text-[11px] font-medium opacity-90">
          {stat}
        </span>
      ))}
    </div>
  </div>
);

const DataCenterPage: React.FC = () => {
  const [overview, setOverview] = useState<DataCenterOverview | null>(null);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [error, setError] = useState<ParsedApiError | null>(null);
  const [runningBacktest, setRunningBacktest] = useState(false);
  const [backtestError, setBacktestError] = useState<ParsedApiError | null>(null);
  const [backtestResult, setBacktestResult] = useState<DataCenterPortfolioBacktestResponse | null>(null);
  const [runningMarketRefresh, setRunningMarketRefresh] = useState(false);
  const [marketRefreshError, setMarketRefreshError] = useState<ParsedApiError | null>(null);
  const [marketRefreshResult, setMarketRefreshResult] = useState<DataCenterMarketDataRefreshResponse | null>(null);
  const [runningDailyReview, setRunningDailyReview] = useState(false);
  const [dailyReviewError, setDailyReviewError] = useState<ParsedApiError | null>(null);
  const [dailyReviewResult, setDailyReviewResult] = useState<DataCenterPortfolioReviewResponse | null>(null);
  const [runningCleanup, setRunningCleanup] = useState(false);
  const [cleanupError, setCleanupError] = useState<ParsedApiError | null>(null);
  const [cleanupResult, setCleanupResult] = useState<DataCenterCleanupResponse | null>(null);

  const loadOverview = async (isRefresh = false) => {
    try {
      if (isRefresh) {
        setRefreshing(true);
      } else {
        setLoading(true);
      }
      const data = await dataCenterApi.getOverview();
      setOverview(data);
      setError(null);
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

  const runPortfolioBacktest = async () => {
    try {
      setRunningBacktest(true);
      setBacktestError(null);
      const result = await dataCenterApi.runPortfolioBacktest({ limitPerSymbol: 50 });
      setBacktestResult(result);
      await loadOverview(true);
    } catch (err) {
      setBacktestError(getParsedApiError(err));
    } finally {
      setRunningBacktest(false);
    }
  };

  const refreshMarketData = async () => {
    try {
      setRunningMarketRefresh(true);
      setMarketRefreshError(null);
      const result = await dataCenterApi.refreshMarketData();
      setMarketRefreshResult(result);
      await loadOverview(true);
    } catch (err) {
      setMarketRefreshError(getParsedApiError(err));
    } finally {
      setRunningMarketRefresh(false);
    }
  };

  const runDailyReview = async () => {
    try {
      setRunningDailyReview(true);
      setDailyReviewError(null);
      const result = await dataCenterApi.runPortfolioDailyReview({ runBacktests: true });
      setDailyReviewResult(result);
      await loadOverview(true);
    } catch (err) {
      setDailyReviewError(getParsedApiError(err));
    } finally {
      setRunningDailyReview(false);
    }
  };

  const runCleanup = async () => {
    try {
      setRunningCleanup(true);
      setCleanupError(null);
      const result = await dataCenterApi.runMaintenanceCleanup({ dryRun: false });
      setCleanupResult(result);
      await loadOverview(true);
    } catch (err) {
      setCleanupError(getParsedApiError(err));
    } finally {
      setRunningCleanup(false);
    }
  };

  const metricCards = overview ? buildMetricCards(overview) : [];
  const sourceItems = overview?.marketData.dataSources || [];
  const backtestConclusions = backtestResult?.items.map(buildBacktestConclusion) || [];
  const warehouse = overview?.marketData.warehouse;
  const warehouseTargets = overview?.marketData.warehouseTargets;
  const quality = overview?.marketData.quality;
  const qualityItems = quality?.items || [];
  const maintenance = overview?.maintenance;
  const aiRouting = overview?.aiRouting;

  return (
    <div className="space-y-6 px-1 pb-8">
      <section className="overflow-hidden rounded-3xl border border-border/70 bg-card/80 p-6 shadow-soft-card backdrop-blur-sm">
        <div className="flex flex-col gap-4 lg:flex-row lg:items-center lg:justify-between">
          <div className="space-y-2">
            <p className="text-xs font-semibold uppercase tracking-[0.24em] text-secondary-text">本地金融数据中心</p>
            <h1 className="text-2xl font-semibold text-foreground">把这台 Mac 当成你的金融工作站</h1>
            <p className="max-w-3xl text-sm leading-6 text-secondary-text">
              这里集中查看本机已经沉淀的行情、持仓、分析、回测、资讯和文件缓存，后续大批量回测和长期复盘都可以围绕这里扩展。
            </p>
          </div>
          <button
            type="button"
            className="inline-flex items-center justify-center gap-2 rounded-2xl border border-border/70 bg-surface px-4 py-2 text-sm font-medium text-foreground transition hover:bg-hover disabled:cursor-not-allowed disabled:opacity-60"
            disabled={refreshing}
            onClick={() => void loadOverview(true)}
          >
            <RefreshCw className={`h-4 w-4 ${refreshing ? 'animate-spin' : ''}`} />
            刷新盘点
          </button>
        </div>
      </section>

      {error ? (
        <div className="rounded-3xl border border-border/70 bg-card/70 p-4 shadow-soft-card">
          <ApiErrorAlert error={error} actionLabel="重试" onAction={() => void loadOverview(true)} />
        </div>
      ) : null}

      {loading ? (
        <div className="rounded-3xl border border-border/70 bg-card/70 p-6 text-sm text-secondary-text shadow-soft-card">
          正在盘点本地数据...
        </div>
      ) : null}

      {overview && !loading ? (
        <>
          <section className="grid gap-4 md:grid-cols-2 xl:grid-cols-3">
            {metricCards.map((card) => (
              <div
                key={card.title}
                className={`rounded-3xl border border-border/70 bg-gradient-to-br ${card.accent} p-5 shadow-soft-card`}
              >
                <p className="text-sm font-semibold text-secondary-text">{card.title}</p>
                <p className="mt-3 text-3xl font-semibold text-foreground">{card.value}</p>
                <p className="mt-2 text-xs leading-5 text-secondary-text">{card.detail}</p>
              </div>
            ))}
          </section>

          <section className="rounded-3xl border border-border/70 bg-card/80 p-6 shadow-soft-card">
            <div className="flex flex-col gap-4 lg:flex-row lg:items-start lg:justify-between">
              <div className="space-y-2">
                <p className="text-xs font-semibold uppercase tracking-[0.24em] text-secondary-text">数据沉淀</p>
                <h2 className="text-lg font-semibold text-foreground">自动补齐持仓和自选历史行情</h2>
                <p className="max-w-3xl text-sm leading-6 text-secondary-text">
                  这一步把常用股票的日线行情持续写入本机数据库。它不会删除数据，也不会做备份，只补齐缺口并回刷最近几天修正迟到行情。
                </p>
              </div>
              <button
                type="button"
                className="inline-flex items-center justify-center gap-2 rounded-2xl bg-primary-gradient px-4 py-2 text-sm font-semibold text-[hsl(var(--primary-foreground))] shadow-[0_12px_28px_var(--nav-brand-shadow)] transition hover:opacity-95 disabled:cursor-not-allowed disabled:opacity-60"
                disabled={runningMarketRefresh || (warehouseTargets?.targetCount || 0) === 0}
                onClick={() => void refreshMarketData()}
              >
                <RefreshCw className={`h-4 w-4 ${runningMarketRefresh ? 'animate-spin' : ''}`} />
                {runningMarketRefresh ? '正在补齐行情' : '现在补齐一次'}
              </button>
            </div>

            {marketRefreshError ? (
              <div className="mt-4">
                <ApiErrorAlert error={marketRefreshError} actionLabel="重试" onAction={() => void refreshMarketData()} />
              </div>
            ) : null}

            <div className="mt-5 grid gap-3 md:grid-cols-4">
              <div className="rounded-2xl bg-base/60 px-4 py-3">
                <p className="text-xs text-secondary-text">自动任务状态</p>
                <p className="mt-1 text-xl font-semibold text-foreground">{warehouseStatusLabel(warehouse?.status)}</p>
              </div>
              <div className="rounded-2xl bg-base/60 px-4 py-3">
                <p className="text-xs text-secondary-text">覆盖标的</p>
                <p className="mt-1 text-xl font-semibold text-foreground">{formatNumber(warehouseTargets?.targetCount)}</p>
              </div>
              <div className="rounded-2xl bg-base/60 px-4 py-3">
                <p className="text-xs text-secondary-text">上次成功</p>
                <p className="mt-1 text-xl font-semibold text-foreground">{formatNumber(warehouse?.totals?.succeeded)}</p>
              </div>
              <div className="rounded-2xl bg-base/60 px-4 py-3">
                <p className="text-xs text-secondary-text">最近运行</p>
                <p className="mt-1 text-sm font-semibold text-foreground">{formatDateTime(warehouse?.generatedAt)}</p>
              </div>
            </div>

            {marketRefreshResult ? (
              <div className="mt-4 rounded-2xl border border-emerald-200/70 bg-emerald-50/70 px-4 py-3 text-sm text-emerald-950">
                本次处理 {formatNumber(marketRefreshResult.totals.processed)} 只，成功 {formatNumber(marketRefreshResult.totals.succeeded)} 只，新增
                {' '}{formatNumber(marketRefreshResult.totals.rowsInserted)} 条行情。
              </div>
            ) : null}
          </section>

          <section className="rounded-3xl border border-border/70 bg-card/80 p-6 shadow-soft-card">
            <div className="mb-5 flex flex-col gap-2">
              <div className="flex items-center gap-2">
                <Gauge className="h-5 w-5 text-secondary-text" />
                <h2 className="text-lg font-semibold text-foreground">数据质量看板</h2>
              </div>
              <p className="text-sm leading-6 text-secondary-text">
                这里直接告诉你哪些标的可放心回测，哪些滞后、缺失或被跳过。判断依据是本地数据库最新日期和每只标的覆盖情况。
              </p>
            </div>
            <div className="grid gap-3 md:grid-cols-4">
              <div className="rounded-2xl bg-base/60 px-4 py-3">
                <p className="text-xs text-secondary-text">正常</p>
                <p className="mt-1 text-xl font-semibold text-emerald-700">{formatNumber(quality?.summary?.fresh)}</p>
              </div>
              <div className="rounded-2xl bg-base/60 px-4 py-3">
                <p className="text-xs text-secondary-text">滞后</p>
                <p className="mt-1 text-xl font-semibold text-amber-700">{formatNumber(quality?.summary?.stale)}</p>
              </div>
              <div className="rounded-2xl bg-base/60 px-4 py-3">
                <p className="text-xs text-secondary-text">缺失</p>
                <p className="mt-1 text-xl font-semibold text-rose-700">{formatNumber(quality?.summary?.missing)}</p>
              </div>
              <div className="rounded-2xl bg-base/60 px-4 py-3">
                <p className="text-xs text-secondary-text">跳过</p>
                <p className="mt-1 text-xl font-semibold text-slate-700">{formatNumber(quality?.summary?.skipped)}</p>
              </div>
            </div>
            <div className="mt-4 grid gap-3 lg:grid-cols-2">
              {qualityItems.slice(0, 12).map((item) => (
                <div key={item.code} className={`rounded-2xl border px-4 py-3 ${qualityClass(item.status)}`}>
                  <div className="flex items-center justify-between gap-3">
                    <p className="font-semibold">{item.code}</p>
                    <span className="rounded-full border border-current/20 px-3 py-1 text-xs font-semibold">{item.label}</span>
                  </div>
                  <p className="mt-2 text-xs leading-5 opacity-90">{item.message}</p>
                  <p className="mt-2 text-xs opacity-80">
                    {formatNumber(item.barCount)} 条，最新 {item.latestDate || '暂无'}
                    {typeof item.daysBehind === 'number' ? `，滞后 ${item.daysBehind} 天` : ''}
                  </p>
                </div>
              ))}
            </div>
          </section>

          <section className="rounded-3xl border border-border/70 bg-card/80 p-6 shadow-soft-card">
            <div className="flex flex-col gap-4 lg:flex-row lg:items-start lg:justify-between">
              <div className="space-y-2">
                <p className="text-xs font-semibold uppercase tracking-[0.24em] text-secondary-text">持仓回测闭环</p>
                <h2 className="text-lg font-semibold text-foreground">让当前持仓自动沉淀回测结果</h2>
                <p className="max-w-3xl text-sm leading-6 text-secondary-text">
                  点击后会按当前持仓代码逐只回测已有历史分析样本。它不会编造新分析，只会把已有分析和后续走势做验证。
                </p>
              </div>
              <button
                type="button"
                className="inline-flex items-center justify-center gap-2 rounded-2xl bg-primary-gradient px-4 py-2 text-sm font-semibold text-[hsl(var(--primary-foreground))] shadow-[0_12px_28px_var(--nav-brand-shadow)] transition hover:opacity-95 disabled:cursor-not-allowed disabled:opacity-60"
                disabled={runningBacktest || (overview.portfolio.positionCount || 0) === 0}
                onClick={() => void runPortfolioBacktest()}
              >
                <PlayCircle className="h-4 w-4" />
                {runningBacktest ? '正在回测持仓' : '回测当前持仓'}
              </button>
            </div>

            {backtestError ? (
              <div className="mt-4">
                <ApiErrorAlert error={backtestError} actionLabel="重试" onAction={() => void runPortfolioBacktest()} />
              </div>
            ) : null}

            {backtestResult ? (
              <div className="mt-5 space-y-4">
                {backtestConclusions.length > 0 ? (
                  <div className="space-y-3">
                    <div>
                      <h3 className="text-base font-semibold text-foreground">中文结论</h3>
                      <p className="mt-1 text-xs leading-5 text-secondary-text">
                        这里把每只持仓的回测数字翻译成可执行判断；样本少的股票不会强行给结论。
                      </p>
                    </div>
                    <div className="grid gap-3 lg:grid-cols-2">
                      {backtestConclusions.map((item) => (
                        <BacktestConclusionCard key={item.code} item={item} />
                      ))}
                    </div>
                  </div>
                ) : null}
                <div className="grid gap-3 md:grid-cols-4">
                  <div className="rounded-2xl bg-base/60 px-4 py-3">
                    <p className="text-xs text-secondary-text">持仓代码</p>
                    <p className="mt-1 text-xl font-semibold text-foreground">{formatNumber(backtestResult.holdingCount)}</p>
                  </div>
                  <div className="rounded-2xl bg-base/60 px-4 py-3">
                    <p className="text-xs text-secondary-text">处理样本</p>
                    <p className="mt-1 text-xl font-semibold text-foreground">{formatNumber(backtestResult.totals.processed)}</p>
                  </div>
                  <div className="rounded-2xl bg-base/60 px-4 py-3">
                    <p className="text-xs text-secondary-text">新增结果</p>
                    <p className="mt-1 text-xl font-semibold text-foreground">{formatNumber(backtestResult.totals.saved)}</p>
                  </div>
                  <div className="rounded-2xl bg-base/60 px-4 py-3">
                    <p className="text-xs text-secondary-text">成功完成</p>
                    <p className="mt-1 text-xl font-semibold text-foreground">{formatNumber(backtestResult.totals.completed)}</p>
                  </div>
                </div>
                <div className="overflow-x-auto rounded-2xl border border-border/60">
                  <table className="w-full min-w-[760px] text-sm">
                    <thead className="bg-base/70 text-left text-xs text-secondary-text">
                      <tr>
                        <th className="px-4 py-3 font-semibold">代码</th>
                        <th className="px-4 py-3 font-semibold">状态</th>
                        <th className="px-4 py-3 font-semibold">新增结果</th>
                        <th className="px-4 py-3 font-semibold">胜率</th>
                        <th className="px-4 py-3 font-semibold">平均模拟收益</th>
                      </tr>
                    </thead>
                    <tbody>
                      {backtestResult.items.map((item) => (
                        <tr key={item.code} className="border-t border-border/50">
                          <td className="px-4 py-3 font-semibold text-foreground">{item.code}</td>
                          <td className="px-4 py-3 text-secondary-text">{item.message}</td>
                          <td className="px-4 py-3 text-foreground">{formatNumber(item.saved)}</td>
                          <td className="px-4 py-3 text-foreground">{formatOptionalPercent(item.summary?.['winRatePct'])}</td>
                          <td className="px-4 py-3 text-foreground">{formatOptionalPercent(item.summary?.['avgSimulatedReturnPct'])}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            ) : null}
          </section>

          <section className="grid gap-4 xl:grid-cols-2">
            <div className="rounded-3xl border border-border/70 bg-card/80 p-6 shadow-soft-card">
              <div className="flex flex-col gap-4 lg:flex-row lg:items-start lg:justify-between">
                <div className="space-y-2">
                  <div className="flex items-center gap-2">
                    <ShieldCheck className="h-5 w-5 text-secondary-text" />
                    <h2 className="text-lg font-semibold text-foreground">每日持仓复盘</h2>
                  </div>
                  <p className="text-sm leading-6 text-secondary-text">
                    每天收盘后自动生成本地复盘报告，把持仓、回测、数据质量和 AI 分工汇总到一个文件里。
                  </p>
                  <p className="text-xs text-secondary-text">
                    最近报告：{formatDateTime(maintenance?.portfolioDailyReview?.generatedAt)}
                  </p>
                </div>
                <button
                  type="button"
                  className="inline-flex items-center justify-center gap-2 rounded-2xl bg-primary-gradient px-4 py-2 text-sm font-semibold text-[hsl(var(--primary-foreground))] shadow-[0_12px_28px_var(--nav-brand-shadow)] transition hover:opacity-95 disabled:cursor-not-allowed disabled:opacity-60"
                  disabled={runningDailyReview}
                  onClick={() => void runDailyReview()}
                >
                  <PlayCircle className="h-4 w-4" />
                  {runningDailyReview ? '正在生成复盘' : '现在生成复盘'}
                </button>
              </div>
              {dailyReviewError ? (
                <div className="mt-4">
                  <ApiErrorAlert error={dailyReviewError} actionLabel="重试" onAction={() => void runDailyReview()} />
                </div>
              ) : null}
              {dailyReviewResult ? (
                <div className="mt-4 rounded-2xl border border-emerald-200/70 bg-emerald-50/70 px-4 py-3 text-sm text-emerald-950">
                  复盘已生成：{dailyReviewResult.markdownPath || '本地报告已写入'}。
                </div>
              ) : null}
            </div>

            <div className="rounded-3xl border border-border/70 bg-card/80 p-6 shadow-soft-card">
              <div className="flex flex-col gap-4 lg:flex-row lg:items-start lg:justify-between">
                <div className="space-y-2">
                  <div className="flex items-center gap-2">
                    <Trash2 className="h-5 w-5 text-secondary-text" />
                    <h2 className="text-lg font-semibold text-foreground">日志与缓存瘦身</h2>
                  </div>
                  <p className="text-sm leading-6 text-secondary-text">
                    只清理旧日志和临时缓存，不碰数据库、报告、持仓和策略文件。
                  </p>
                  <p className="text-xs text-secondary-text">最近清理：{formatDateTime(maintenance?.cleanup?.generatedAt)}</p>
                </div>
                <button
                  type="button"
                  className="inline-flex items-center justify-center gap-2 rounded-2xl border border-border/70 bg-surface px-4 py-2 text-sm font-medium text-foreground transition hover:bg-hover disabled:cursor-not-allowed disabled:opacity-60"
                  disabled={runningCleanup}
                  onClick={() => void runCleanup()}
                >
                  <Trash2 className="h-4 w-4" />
                  {runningCleanup ? '正在清理' : '现在清理一次'}
                </button>
              </div>
              {cleanupError ? (
                <div className="mt-4">
                  <ApiErrorAlert error={cleanupError} actionLabel="重试" onAction={() => void runCleanup()} />
                </div>
              ) : null}
              {cleanupResult ? (
                <div className="mt-4 rounded-2xl border border-sky-200/70 bg-sky-50/70 px-4 py-3 text-sm text-sky-950">
                  已处理 {formatNumber(cleanupResult.totals.deletedCount)} 项，释放 {formatBytes(cleanupResult.totals.freedBytes)}。
                </div>
              ) : null}
            </div>
          </section>

          <section className="rounded-3xl border border-border/70 bg-card/80 p-6 shadow-soft-card">
            <div className="flex items-start gap-3">
              <Cloud className="mt-1 h-5 w-5 text-secondary-text" />
              <div>
                <h2 className="text-lg font-semibold text-foreground">云端 AI 与本地工作站分工</h2>
                <p className="mt-2 text-sm leading-6 text-secondary-text">{aiRouting?.recommendation || '正在读取 AI 分工配置。'}</p>
              </div>
            </div>
            <div className="mt-5 grid gap-3 md:grid-cols-3">
              <div className="rounded-2xl bg-base/60 px-4 py-3">
                <p className="text-xs text-secondary-text">云端分析</p>
                <p className="mt-1 text-lg font-semibold text-foreground">{aiRouting?.cloudAnalysis?.enabled ? '已启用' : '未识别'}</p>
                <p className="mt-1 text-xs text-secondary-text">{aiRouting?.cloudAnalysis?.primaryModel || '暂无主模型'}</p>
              </div>
              <div className="rounded-2xl bg-base/60 px-4 py-3">
                <p className="text-xs text-secondary-text">本地工作站</p>
                <p className="mt-1 text-lg font-semibold text-foreground">已启用</p>
                <p className="mt-1 text-xs text-secondary-text">数据、回测、复盘、清理</p>
              </div>
              <div className="rounded-2xl bg-base/60 px-4 py-3">
                <p className="text-xs text-secondary-text">本地模型默认入口</p>
                <p className="mt-1 text-lg font-semibold text-foreground">{aiRouting?.localModel?.defaultEnabled ? '开启' : '关闭'}</p>
                <p className="mt-1 text-xs text-secondary-text">不作为股票分析默认入口</p>
              </div>
            </div>
          </section>

          <section className="grid gap-4 xl:grid-cols-[1.15fr_0.85fr]">
            <div className="rounded-3xl border border-border/70 bg-card/80 p-6 shadow-soft-card">
              <div className="mb-4 flex items-center gap-2">
                <HardDrive className="h-5 w-5 text-secondary-text" />
                <h2 className="text-lg font-semibold text-foreground">本地文件占用</h2>
              </div>
              <div className="grid gap-3 md:grid-cols-2">
                {overview.files.map((item) => (
                  <FileInventoryCard key={item.path} item={item} />
                ))}
              </div>
            </div>

            <div className="space-y-4 rounded-3xl border border-border/70 bg-card/80 p-6 shadow-soft-card">
              <div className="flex items-center gap-2">
                <Database className="h-5 w-5 text-secondary-text" />
                <h2 className="text-lg font-semibold text-foreground">数据来源</h2>
              </div>
              {sourceItems.length > 0 ? (
                <div className="space-y-2">
                  {sourceItems.map((item) => (
                    <div key={item.name} className="flex items-center justify-between rounded-2xl bg-base/60 px-4 py-3 text-sm">
                      <span className="font-medium text-foreground">{item.name}</span>
                      <span className="text-secondary-text">{formatNumber(item.count)} 条</span>
                    </div>
                  ))}
                </div>
              ) : (
                <p className="rounded-2xl bg-base/60 px-4 py-3 text-sm leading-6 text-secondary-text">
                  暂时没有可展示的数据来源，等行情缓存写入后这里会自动显示。
                </p>
              )}
              <p className="text-xs text-secondary-text">最近盘点：{formatDateTime(overview.generatedAt)}</p>
            </div>
          </section>

          <section className="rounded-3xl border border-border/70 bg-card/80 p-6 shadow-soft-card">
            <h2 className="mb-4 text-lg font-semibold text-foreground">下一步建议</h2>
            <div className="grid gap-3 lg:grid-cols-2">
              {overview.recommendations.map((item) => (
                <RecommendationCard key={`${item.level}-${item.title}`} item={item} />
              ))}
            </div>
          </section>
        </>
      ) : null}
    </div>
  );
};

export default DataCenterPage;
