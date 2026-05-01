import type React from 'react';
import { useState, useEffect, useCallback } from 'react';
import { Check, Minus, X } from 'lucide-react';
import { backtestApi } from '../api/backtest';
import type { ParsedApiError } from '../api/error';
import { getParsedApiError } from '../api/error';
import { ApiErrorAlert, Card, Badge, EmptyState, Pagination, StatusDot, Tooltip } from '../components/common';
import type {
  BacktestScanItem,
  BacktestScanResponse,
  BacktestResultItem,
  BacktestRunResponse,
  PerformanceMetrics,
} from '../types/backtest';

const BACKTEST_INPUT_CLASS =
  'input-surface input-focus-glow h-11 w-full rounded-xl border bg-transparent px-4 text-sm transition-all focus:outline-none disabled:cursor-not-allowed disabled:opacity-60';
const BACKTEST_COMPACT_INPUT_CLASS =
  'input-surface input-focus-glow h-10 rounded-xl border bg-transparent px-3 py-2 text-xs transition-all focus:outline-none disabled:cursor-not-allowed disabled:opacity-60';

// ============ Helpers ============

function pct(value?: number | null): string {
  if (value == null) return '--';
  return `${value.toFixed(1)}%`;
}

function scanValue(value?: number | null): string {
  if (value == null) return '不限';
  return String(value);
}

function scoreSourceLabel(source?: string | null): string {
  switch (source) {
    case 'signal_score':
      return '信号分';
    case 'sentiment_score':
      return '情绪分';
    case 'trend_score':
      return '趋势分';
    case 'missing':
      return '缺失';
    default:
      return source || '--';
  }
}

function directionLabel(direction?: string | null): string {
  switch (direction) {
    case 'long':
      return '看多';
    case 'short':
      return '看空';
    case 'cash':
      return '观望';
    default:
      return direction || '';
  }
}

type BacktestRunSetup = {
  evalWindowDays?: number | null;
  scoreThreshold?: number | null;
  topN?: number | null;
};

function sameOptionalNumber(left?: number | null, right?: number | null): boolean {
  return (left ?? null) === (right ?? null);
}

function sameRunSetup(left: BacktestRunSetup, right: BacktestRunSetup): boolean {
  return (
    sameOptionalNumber(left.evalWindowDays, right.evalWindowDays)
    && sameOptionalNumber(left.scoreThreshold, right.scoreThreshold)
    && sameOptionalNumber(left.topN, right.topN)
  );
}

function resolveRunPresetLabel(setup: BacktestRunSetup, scanResult: BacktestScanResponse | null): string {
  const recommended = scanResult?.conclusion?.recommendedScan;
  if (recommended && sameRunSetup(setup, recommended)) {
    return '推荐组合';
  }

  const secondary = scanResult?.conclusion?.secondaryScan;
  if (secondary && sameRunSetup(setup, secondary)) {
    return '备选组合';
  }

  return '自定义组合';
}

function formatRunSetup(setup: BacktestRunSetup): string {
  const parts = [];

  if (setup.evalWindowDays != null) {
    parts.push(`窗口 ${setup.evalWindowDays} 天`);
  }
  if (setup.scoreThreshold != null) {
    parts.push(`分数 >= ${scanValue(setup.scoreThreshold)}`);
  }
  if (setup.topN != null) {
    parts.push(`前 N ${scanValue(setup.topN)}`);
  }

  return parts.length > 0 ? parts.join(' · ') : '未使用分数筛选';
}

function formatSignedDelta(value?: number | null, suffix = ''): string {
  if (value == null) return '--';
  const sign = value > 0 ? '+' : '';
  return `${sign}${value.toFixed(1)}${suffix}`;
}

function outcomeBadge(outcome?: string) {
  if (!outcome) return <Badge variant="default">--</Badge>;
  switch (outcome) {
    case 'win':
      return <Badge variant="success" glow>盈利</Badge>;
    case 'loss':
      return <Badge variant="danger" glow>亏损</Badge>;
    case 'neutral':
      return <Badge variant="warning">持平</Badge>;
    default:
      return <Badge variant="default">{outcome}</Badge>;
  }
}

function statusBadge(status: string) {
  switch (status) {
    case 'completed':
      return <Badge variant="success">已完成</Badge>;
    case 'insufficient':
    case 'insufficient_data':
      return <Badge variant="warning">数据不足</Badge>;
    case 'error':
      return <Badge variant="danger">错误</Badge>;
    default:
      return <Badge variant="default">{status}</Badge>;
  }
}

function actualMovementBadge(movement?: string | null) {
  switch (movement) {
    case 'up':
      return <Badge variant="success">上涨</Badge>;
    case 'down':
      return <Badge variant="danger">下跌</Badge>;
    case 'flat':
      return <Badge variant="warning">横盘</Badge>;
    default:
      return <Badge variant="default">--</Badge>;
  }
}

function boolIcon(value?: boolean | null) {
  if (value === true) {
    return (
      <span
        className="backtest-status-chip backtest-status-chip-success"
        aria-label="yes"
      >
        <StatusDot tone="success" className="backtest-status-chip-dot" />
        <Check className="h-3.5 w-3.5" />
      </span>
    );
  }

  if (value === false) {
    return (
      <span
        className="backtest-status-chip backtest-status-chip-danger"
        aria-label="no"
      >
        <StatusDot tone="danger" className="backtest-status-chip-dot" />
        <X className="h-3.5 w-3.5" />
      </span>
    );
  }

  return (
    <span
      className="backtest-status-chip backtest-status-chip-neutral"
      aria-label="unknown"
    >
      <StatusDot tone="neutral" className="backtest-status-chip-dot" />
      <Minus className="h-3.5 w-3.5" />
    </span>
  );
}

// ============ Metric Row ============

const MetricRow: React.FC<{ label: string; value: string; accent?: boolean }> = ({ label, value, accent }) => (
  <div className="backtest-metric-row">
    <span className="label">{label}</span>
    <span className={`value ${accent ? 'accent' : ''}`}>{value}</span>
  </div>
);

// ============ Performance Card ============

const PerformanceCard: React.FC<{ metrics: PerformanceMetrics; title: string }> = ({ metrics, title }) => (
  <Card variant="gradient" padding="md" className="animate-fade-in">
    <div className="mb-3">
      <span className="label-uppercase">{title}</span>
    </div>
    <MetricRow label="方向准确率" value={pct(metrics.directionAccuracyPct)} accent />
    <MetricRow label="胜率" value={pct(metrics.winRatePct)} accent />
    <MetricRow label="平均模拟收益" value={pct(metrics.avgSimulatedReturnPct)} />
    <MetricRow label="平均标的收益" value={pct(metrics.avgStockReturnPct)} />
    <MetricRow label="止损触发率" value={pct(metrics.stopLossTriggerRate)} />
    <MetricRow label="止盈触发率" value={pct(metrics.takeProfitTriggerRate)} />
    <MetricRow label="平均触发天数" value={metrics.avgDaysToFirstHit != null ? metrics.avgDaysToFirstHit.toFixed(1) : '--'} />
    <div className="backtest-metric-footer">
      <span className="text-xs text-muted-text">评估数</span>
      <span className="text-xs text-secondary-text font-mono">
        {Number(metrics.completedCount)} / {Number(metrics.totalEvaluations)}
      </span>
    </div>
    <div className="flex items-center justify-between">
      <span className="text-xs text-muted-text">盈 / 亏 / 平</span>
      <span className="text-xs font-mono">
        <span className="text-success">{metrics.winCount}</span>
        {' / '}
        <span className="text-danger">{metrics.lossCount}</span>
        {' / '}
        <span className="text-warning">{metrics.neutralCount}</span>
      </span>
    </div>
  </Card>
);

// ============ Run Summary ============

const RunSummary: React.FC<{
  data: BacktestRunResponse;
  setupLabel?: string | null;
  setupSummary?: string | null;
}> = ({ data, setupLabel, setupSummary }) => (
  <div className="backtest-summary animate-fade-in">
    <span className="label">候选数: <span className="value">{data.candidateCount}</span></span>
    <span className="label">已处理: <span className="value">{data.processed}</span></span>
    <span className="label">已保存: <span className="value primary">{data.saved}</span></span>
    <span className="label">已完成: <span className="value success">{data.completed}</span></span>
    <span className="label">数据不足: <span className="value warning">{data.insufficient}</span></span>
    {data.errors > 0 && (
      <span className="label">错误: <span className="value danger">{data.errors}</span></span>
    )}
    {setupLabel && setupSummary && (
      <span className="label">
        最近一次运行: <span className="value primary">{setupLabel}</span>
        <span className="value"> · {setupSummary}</span>
      </span>
    )}
  </div>
);

const ScanSummary: React.FC<{
  data: BacktestScanResponse;
  onApplyScan: (scan: BacktestScanItem) => void;
  onRunScan: (scan: BacktestScanItem) => void;
  isRunning: boolean;
}> = ({ data, onApplyScan, onRunScan, isRunning }) => {
  const recommended = data.conclusion?.recommendedScan;
  const secondary = data.conclusion?.secondaryScan;
  const comparison = recommended && secondary ? {
    returnGap: (recommended.avgSimulatedReturnPct ?? 0) - (secondary.avgSimulatedReturnPct ?? 0),
    winRateGap: (recommended.winRatePct ?? 0) - (secondary.winRatePct ?? 0),
    candidateGap: (recommended.candidateCount ?? 0) - (secondary.candidateCount ?? 0),
  } : null;
  const comparisonNarrative = comparison
    ? `推荐组合在模拟收益上${comparison.returnGap >= 0 ? '领先' : '落后'} ${formatSignedDelta(comparison.returnGap, '%')}，`
      + `在胜率上${comparison.winRateGap >= 0 ? '领先' : '落后'} ${formatSignedDelta(comparison.winRateGap, 'pt')}。`
    : null;

  return (
    <Card variant="gradient" padding="md" className="animate-fade-in mt-3">
      <div className="flex items-center justify-between gap-3">
        <div>
          <span className="label-uppercase">参数扫描</span>
          <p className="mt-1 text-sm text-secondary-text">
            {data.conclusion?.summaryText || '暂时还没有扫描结论。'}
          </p>
        </div>
        <Badge variant={data.localDataOnly ? 'warning' : 'default'}>
          {data.localDataOnly ? '仅本地' : '允许补数'}
        </Badge>
      </div>

      {comparison && (
        <div className="mt-3 rounded-xl border border-white/8 bg-black/10 p-3">
          <div className="text-xs uppercase tracking-[0.18em] text-muted-text">推荐与备选对比</div>
          <div className="mt-2 grid gap-2 md:grid-cols-3">
            <div className="rounded-lg border border-white/6 bg-black/10 p-2">
              <div className="text-[11px] uppercase tracking-[0.14em] text-muted-text">收益差</div>
              <div className="mt-1 text-sm text-secondary-text">{formatSignedDelta(comparison.returnGap, '%')}</div>
            </div>
            <div className="rounded-lg border border-white/6 bg-black/10 p-2">
              <div className="text-[11px] uppercase tracking-[0.14em] text-muted-text">胜率差</div>
              <div className="mt-1 text-sm text-secondary-text">{formatSignedDelta(comparison.winRateGap, 'pt')}</div>
            </div>
            <div className="rounded-lg border border-white/6 bg-black/10 p-2">
              <div className="text-[11px] uppercase tracking-[0.14em] text-muted-text">覆盖数差</div>
              <div className="mt-1 text-sm text-secondary-text">{formatSignedDelta(comparison.candidateGap, ' 条')}</div>
            </div>
          </div>
          <p className="mt-2 text-xs text-muted-text">
            {comparisonNarrative}
          </p>
        </div>
      )}

      <div className="mt-3 grid gap-3 md:grid-cols-2">
        <div className="rounded-xl border border-white/8 bg-black/10 p-3">
          <div className="text-xs uppercase tracking-[0.18em] text-muted-text">推荐组合</div>
          {recommended ? (
            <div className="mt-2 space-y-1 text-sm">
              <div>窗口: <span className="text-secondary-text">{recommended.evalWindowDays} 天</span></div>
              <div>分数阈值: <span className="text-secondary-text">{scanValue(recommended.scoreThreshold)}</span></div>
              <div>前 N: <span className="text-secondary-text">{scanValue(recommended.topN)}</span></div>
              <div>模拟收益: <span className="text-secondary-text">{pct(recommended.avgSimulatedReturnPct)}</span></div>
              <div>胜率: <span className="text-secondary-text">{pct(recommended.winRatePct)}</span></div>
              <button
                type="button"
                onClick={() => onApplyScan(recommended)}
                className="btn-secondary mt-2 inline-flex items-center gap-1.5 whitespace-nowrap"
              >
                应用推荐
              </button>
              <button
                type="button"
                onClick={() => onRunScan(recommended)}
                disabled={isRunning}
                className="btn-primary mt-2 inline-flex items-center gap-1.5 whitespace-nowrap"
              >
                {isRunning ? '运行中...' : '运行推荐组合'}
              </button>
            </div>
          ) : (
            <p className="mt-2 text-sm text-muted-text">暂无推荐组合。</p>
          )}
        </div>
        <div className="rounded-xl border border-white/8 bg-black/10 p-3">
          <div className="text-xs uppercase tracking-[0.18em] text-muted-text">备选组合</div>
          {secondary ? (
            <div className="mt-2 space-y-1 text-sm">
              <div>窗口: <span className="text-secondary-text">{secondary.evalWindowDays} 天</span></div>
              <div>分数阈值: <span className="text-secondary-text">{scanValue(secondary.scoreThreshold)}</span></div>
              <div>前 N: <span className="text-secondary-text">{scanValue(secondary.topN)}</span></div>
              <div>模拟收益: <span className="text-secondary-text">{pct(secondary.avgSimulatedReturnPct)}</span></div>
              <div>胜率: <span className="text-secondary-text">{pct(secondary.winRatePct)}</span></div>
              <button
                type="button"
                onClick={() => onApplyScan(secondary)}
                className="btn-secondary mt-2 inline-flex items-center gap-1.5 whitespace-nowrap"
              >
                应用备选
              </button>
              <button
                type="button"
                onClick={() => onRunScan(secondary)}
                disabled={isRunning}
                className="btn-primary mt-2 inline-flex items-center gap-1.5 whitespace-nowrap"
              >
                {isRunning ? '运行中...' : '运行备选组合'}
              </button>
            </div>
          ) : (
            <p className="mt-2 text-sm text-muted-text">暂无备选组合。</p>
          )}
        </div>
      </div>
    </Card>
  );
};

// ============ Main Page ============

const BacktestPage: React.FC = () => {
  // Set page title
  useEffect(() => {
    document.title = '策略回测 - DSA';
  }, []);

  // Input state
  const [codeFilter, setCodeFilter] = useState('');
  const [analysisDateFrom, setAnalysisDateFrom] = useState('');
  const [analysisDateTo, setAnalysisDateTo] = useState('');
  const [evalDays, setEvalDays] = useState('');
  const [scoreThreshold, setScoreThreshold] = useState('');
  const [topN, setTopN] = useState('');
  const [advancedMode, setAdvancedMode] = useState(false);
  const [forceRerun, setForceRerun] = useState(false);
  const [isRunning, setIsRunning] = useState(false);
  const [runResult, setRunResult] = useState<BacktestRunResponse | null>(null);
  const [lastRunSetup, setLastRunSetup] = useState<BacktestRunSetup | null>(null);
  const [scanResult, setScanResult] = useState<BacktestScanResponse | null>(null);
  const [runError, setRunError] = useState<ParsedApiError | null>(null);
  const [scanError, setScanError] = useState<ParsedApiError | null>(null);
  const [pageError, setPageError] = useState<ParsedApiError | null>(null);
  const [isScanning, setIsScanning] = useState(false);
  const [scanLocalOnly, setScanLocalOnly] = useState(true);

  // Results state
  const [results, setResults] = useState<BacktestResultItem[]>([]);
  const [totalResults, setTotalResults] = useState(0);
  const [currentPage, setCurrentPage] = useState(1);
  const [isLoadingResults, setIsLoadingResults] = useState(false);
  const [resultScoreMin, setResultScoreMin] = useState('');
  const pageSize = 20;

  // Performance state
  const [overallPerf, setOverallPerf] = useState<PerformanceMetrics | null>(null);
  const [stockPerf, setStockPerf] = useState<PerformanceMetrics | null>(null);
  const [isLoadingPerf, setIsLoadingPerf] = useState(false);
  const effectiveWindowDays = evalDays ? parseInt(evalDays, 10) : overallPerf?.evalWindowDays;
  const isNextDayValidation = effectiveWindowDays === 1;
  const showNextDayActualColumns = isNextDayValidation;

  // Fetch results
  const fetchResults = useCallback(async (
    page = 1,
    code?: string,
    windowDays?: number,
    startDate?: string,
    endDate?: string,
  ) => {
    setIsLoadingResults(true);
    try {
      const response = await backtestApi.getResults({
        code: code || undefined,
        evalWindowDays: windowDays,
        analysisDateFrom: startDate || undefined,
        analysisDateTo: endDate || undefined,
        page,
        limit: pageSize,
      });
      setResults(response.items);
      setTotalResults(response.total);
      setCurrentPage(response.page);
      setPageError(null);
    } catch (err) {
      console.error('获取回测结果失败:', err);
      setPageError(getParsedApiError(err));
    } finally {
      setIsLoadingResults(false);
    }
  }, []);

  // Fetch performance
  const fetchPerformance = useCallback(async (
    code?: string,
    windowDays?: number,
    startDate?: string,
    endDate?: string,
  ) => {
    setIsLoadingPerf(true);
    try {
      const overall = await backtestApi.getOverallPerformance({
        evalWindowDays: windowDays,
        analysisDateFrom: startDate || undefined,
        analysisDateTo: endDate || undefined,
      });
      setOverallPerf(overall);

      if (code) {
        const stock = await backtestApi.getStockPerformance(code, {
          evalWindowDays: windowDays,
          analysisDateFrom: startDate || undefined,
          analysisDateTo: endDate || undefined,
        });
        setStockPerf(stock);
      } else {
        setStockPerf(null);
      }
      setPageError(null);
    } catch (err) {
      console.error('获取表现统计失败:', err);
      setPageError(getParsedApiError(err));
    } finally {
      setIsLoadingPerf(false);
    }
  }, []);

  // Initial load — fetch performance first, then filter results by its window
  useEffect(() => {
    const init = async () => {
      // Get latest performance (unfiltered returns most recent summary)
      const overall = await backtestApi.getOverallPerformance();
      setOverallPerf(overall);
      // Use the summary's eval_window_days to filter results consistently
      const windowDays = overall?.evalWindowDays;
      if (windowDays && !evalDays) {
        setEvalDays(String(windowDays));
      }
      fetchResults(1, undefined, windowDays, undefined, undefined);
    };
    init();
  }, []); // eslint-disable-line react-hooks/exhaustive-deps

  const executeRun = async (overrides?: {
    evalWindowDays?: number;
    scoreThreshold?: number;
    topN?: number;
  }) => {
    setIsRunning(true);
    setRunResult(null);
    setRunError(null);
    try {
      const code = codeFilter.trim() || undefined;
      const evalWindowDays = overrides?.evalWindowDays ?? (evalDays ? parseInt(evalDays, 10) : undefined);
      const parsedScoreThreshold = overrides?.scoreThreshold ?? (scoreThreshold ? parseFloat(scoreThreshold) : undefined);
      const parsedTopN = overrides?.topN ?? (topN ? parseInt(topN, 10) : undefined);

      const response = await backtestApi.run({
        code,
        force: forceRerun || undefined,
        minAgeDays: forceRerun ? 0 : undefined,
        evalWindowDays,
        scoreThreshold: parsedScoreThreshold,
        topN: parsedTopN,
      });
      setRunResult(response);
      setLastRunSetup({
        evalWindowDays,
        scoreThreshold: parsedScoreThreshold,
        topN: parsedTopN,
      });
      fetchResults(1, codeFilter.trim() || undefined, evalWindowDays, analysisDateFrom, analysisDateTo);
      fetchPerformance(codeFilter.trim() || undefined, evalWindowDays, analysisDateFrom, analysisDateTo);
    } catch (err) {
      setRunError(getParsedApiError(err));
    } finally {
      setIsRunning(false);
    }
  };

  // Run backtest
  const handleRun = async () => {
    await executeRun();
  };

  const handleRunScan = async (scan: BacktestScanItem) => {
    handleApplyScan(scan);
    await executeRun({
      evalWindowDays: scan.evalWindowDays,
      scoreThreshold: scan.scoreThreshold ?? undefined,
      topN: scan.topN ?? undefined,
    });
  };

  const handleScan = async () => {
    setIsScanning(true);
    setScanError(null);
    try {
      const code = codeFilter.trim() || undefined;
      const evalWindowDays = evalDays ? parseInt(evalDays, 10) : 10;
      const response = await backtestApi.scan({
        code,
        minAgeDays: 0,
        limit: 200,
        localDataOnly: scanLocalOnly,
        evalWindowDaysOptions: [evalWindowDays],
        scoreThresholdOptions: [null, 60, 70],
        topNOptions: [null, 3, 5],
      });
      setScanResult(response);
    } catch (err) {
      setScanError(getParsedApiError(err));
    } finally {
      setIsScanning(false);
    }
  };

  const handleApplyScan = (scan: BacktestScanItem) => {
    setEvalDays(String(scan.evalWindowDays));
    setScoreThreshold(scan.scoreThreshold != null ? String(scan.scoreThreshold) : '');
    setTopN(scan.topN != null ? String(scan.topN) : '');
  };

  // Filter by code
  const handleFilter = () => {
    const code = codeFilter.trim() || undefined;
    const windowDays = evalDays ? parseInt(evalDays, 10) : undefined;
    setCurrentPage(1);
    fetchResults(1, code, windowDays, analysisDateFrom, analysisDateTo);
    fetchPerformance(code, windowDays, analysisDateFrom, analysisDateTo);
  };

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter') {
      handleFilter();
    }
  };

  const handleShowNextDay = () => {
    const code = codeFilter.trim() || undefined;
    setEvalDays('1');
    setCurrentPage(1);
    fetchResults(1, code, 1, analysisDateFrom, analysisDateTo);
    fetchPerformance(code, 1, analysisDateFrom, analysisDateTo);
  };

  // Pagination
  const totalPages = Math.ceil(totalResults / pageSize);
  const runSetupLabel = lastRunSetup ? resolveRunPresetLabel(lastRunSetup, scanResult) : null;
  const runSetupSummary = lastRunSetup ? formatRunSetup(lastRunSetup) : null;
  const parsedResultScoreMin = resultScoreMin ? parseFloat(resultScoreMin) : undefined;
  const displayedResults = parsedResultScoreMin != null
    ? results.filter((row) => row.rankingScore != null && row.rankingScore >= parsedResultScoreMin)
    : results;
  const handlePageChange = (page: number) => {
    const windowDays = evalDays ? parseInt(evalDays, 10) : undefined;
    fetchResults(page, codeFilter.trim() || undefined, windowDays, analysisDateFrom, analysisDateTo);
  };

  return (
    <div className="min-h-full flex flex-col rounded-[1.5rem] bg-transparent">
      {/* Header */}
      <header className="flex-shrink-0 border-b border-white/5 px-3 py-3 sm:px-4">
        <div className="mb-3 flex flex-wrap items-center justify-between gap-2">
          <p className="text-xs text-secondary-text">
            {advancedMode
              ? '高级模式会显示分数阈值、前 N、日期范围、强制重跑和仅本地等控制项。'
              : '简单模式：输入股票代码和窗口，然后要么先参数扫描再运行推荐组合，要么直接运行回测。'}
          </p>
          <button
            type="button"
            onClick={() => setAdvancedMode((value) => !value)}
            className={`backtest-force-btn ${advancedMode ? 'active' : ''}`}
          >
            <span className="dot" />
            {advancedMode ? '高级模式已开' : '高级模式已关'}
          </button>
        </div>
        {!advancedMode && (
          <div className="mb-3 rounded-xl border border-white/8 bg-black/10 p-3">
            <div className="text-xs uppercase tracking-[0.18em] text-muted-text">快速上手</div>
            <div className="mt-2 space-y-1 text-sm text-secondary-text">
              <p>1. 输入股票代码；如果留空，就扫描全部股票。</p>
              <p>2. 如果没有特别需求，窗口先保持在 10。</p>
              <p>3. 不确定怎么选时，先点参数扫描，再点运行推荐组合。</p>
              <p>4. 跑完以后，先看最近一次运行和排名分。</p>
            </div>
          </div>
        )}
        <div className="flex max-w-5xl flex-wrap items-center gap-2">
          <div className="relative min-w-0 flex-[1_1_220px]">
            <input
              type="text"
              value={codeFilter}
              onChange={(e) => setCodeFilter(e.target.value.toUpperCase())}
              onKeyDown={handleKeyDown}
              placeholder="输入股票代码，不填则查看全部"
              disabled={isRunning}
              className={BACKTEST_INPUT_CLASS}
            />
          </div>
          <div className="flex items-center gap-2 whitespace-nowrap lg:w-40 lg:justify-between">
            <span className="text-xs text-muted-text">窗口</span>
            <input
              type="number"
              min={1}
              max={120}
              value={evalDays}
              onChange={(e) => setEvalDays(e.target.value)}
              placeholder="10"
              disabled={isRunning}
              className={`${BACKTEST_COMPACT_INPUT_CLASS} w-24 text-center tabular-nums`}
            />
          </div>
          <button
            type="button"
            onClick={handleRun}
            disabled={isRunning}
            className="btn-primary flex items-center gap-1.5 whitespace-nowrap"
          >
            {isRunning ? (
              <>
                <svg className="w-3.5 h-3.5 animate-spin" fill="none" viewBox="0 0 24 24">
                  <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
                  <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z" />
                </svg>
                运行中...
              </>
            ) : (
              '运行回测'
            )}
          </button>
          <button
            type="button"
            onClick={handleScan}
            disabled={isScanning}
            className="btn-secondary flex items-center gap-1.5 whitespace-nowrap"
          >
            {isScanning ? '扫描中...' : '参数扫描'}
          </button>
          {advancedMode && (
            <>
              <button
                type="button"
                onClick={handleFilter}
                disabled={isLoadingResults}
                className="btn-secondary flex items-center gap-1.5 whitespace-nowrap"
              >
                筛选
              </button>
              <div className="flex items-center gap-2 whitespace-nowrap lg:w-44 lg:justify-between">
                <span className="text-xs text-muted-text">分数 ≥</span>
                <input
                  type="number"
                  min={0}
                  max={100}
                  step="0.1"
                  aria-label="Score threshold"
                  value={scoreThreshold}
                  onChange={(e) => setScoreThreshold(e.target.value)}
                  placeholder="60"
                  disabled={isRunning}
                  className={`${BACKTEST_COMPACT_INPUT_CLASS} w-24 text-center tabular-nums`}
                />
              </div>
              <div className="flex items-center gap-2 whitespace-nowrap lg:w-36 lg:justify-between">
                <span className="text-xs text-muted-text">前 N</span>
                <input
                  type="number"
                  min={1}
                  max={100}
                  aria-label="Top N"
                  value={topN}
                  onChange={(e) => setTopN(e.target.value)}
                  placeholder="3"
                  disabled={isRunning}
                  className={`${BACKTEST_COMPACT_INPUT_CLASS} w-20 text-center tabular-nums`}
                />
              </div>
              <div className="flex items-center gap-2 whitespace-nowrap">
                <span className="text-xs text-muted-text">开始</span>
                <input
                  type="date"
                  aria-label="Analysis date from"
                  value={analysisDateFrom}
                  onChange={(e) => setAnalysisDateFrom(e.target.value)}
                  onKeyDown={handleKeyDown}
                  disabled={isRunning}
                  className={`${BACKTEST_COMPACT_INPUT_CLASS} w-40 text-center tabular-nums`}
                />
              </div>
              <div className="flex items-center gap-2 whitespace-nowrap">
                <span className="text-xs text-muted-text">结束</span>
                <input
                  type="date"
                  aria-label="Analysis date to"
                  value={analysisDateTo}
                  onChange={(e) => setAnalysisDateTo(e.target.value)}
                  onKeyDown={handleKeyDown}
                  disabled={isRunning}
                  className={`${BACKTEST_COMPACT_INPUT_CLASS} w-40 text-center tabular-nums`}
                />
              </div>
              <button
                type="button"
                onClick={handleShowNextDay}
                disabled={isLoadingResults || isLoadingPerf}
                className={`backtest-force-btn ${isNextDayValidation ? 'active' : ''}`}
              >
                <span className="dot" />
                次日验证
              </button>
              <button
                type="button"
                onClick={() => setForceRerun(!forceRerun)}
                disabled={isRunning}
                className={`backtest-force-btn ${forceRerun ? 'active' : ''}`}
              >
                <span className="dot" />
                强制重跑
              </button>
              <button
                type="button"
                onClick={() => setScanLocalOnly(!scanLocalOnly)}
                disabled={isScanning}
                className={`backtest-force-btn ${scanLocalOnly ? 'active' : ''}`}
              >
                <span className="dot" />
                仅本地
              </button>
            </>
          )}
        </div>
        {runResult && (
          <div className="mt-2 max-w-4xl">
            <RunSummary
              data={runResult}
              setupLabel={runSetupLabel}
              setupSummary={runSetupSummary}
            />
          </div>
        )}
        {scanResult && (
          <div className="max-w-4xl">
            <ScanSummary
              data={scanResult}
              onApplyScan={handleApplyScan}
              onRunScan={handleRunScan}
              isRunning={isRunning}
            />
          </div>
        )}
        {runError && (
          <ApiErrorAlert error={runError} className="mt-2 max-w-4xl" />
        )}
        {scanError && (
          <ApiErrorAlert error={scanError} className="mt-2 max-w-4xl" />
        )}
        <p className="mt-2 text-xs text-muted-text">
          {isNextDayValidation
            ? '次日验证模式会把 AI 预测与下一交易日收盘结果进行对照。'
            : '如果想看次日验证，把窗口改成 1 即可。'}
        </p>
      </header>

      {/* Main content */}
      <main className="flex min-h-0 flex-1 flex-col gap-3 overflow-hidden p-3 lg:flex-row">
        {/* Left sidebar - Performance */}
        <div className="flex max-h-[38vh] flex-col gap-3 overflow-y-auto lg:max-h-none lg:w-60 lg:flex-shrink-0">
          {isLoadingPerf ? (
            <div className="flex items-center justify-center py-8">
              <div className="backtest-spinner sm" />
            </div>
          ) : overallPerf ? (
            <PerformanceCard metrics={overallPerf} title="整体表现" />
          ) : (
            <EmptyState
              title="暂无统计"
              description="运行一次回测后，这里会显示组合层面的表现统计。"
              className="h-full min-h-[12rem] border-dashed bg-card/45 shadow-none"
            />
          )}

          {stockPerf && (
            <PerformanceCard metrics={stockPerf} title={`${stockPerf.code || codeFilter}`} />
          )}
        </div>

        {/* Right content - Results table */}
        <section className="min-h-0 flex-1 overflow-y-auto">
          {pageError ? (
            <ApiErrorAlert error={pageError} className="mb-3" />
          ) : null}
          {isLoadingResults ? (
            <div className="flex flex-col items-center justify-center h-64">
              <div className="backtest-spinner md" />
              <p className="mt-3 text-secondary-text text-sm">正在加载结果...</p>
            </div>
          ) : results.length === 0 ? (
            <EmptyState
              title="暂无结果"
              description="运行一次回测后，这里会显示历史分析的回测结果。"
              className="backtest-empty-state border-dashed"
              icon={(
                <svg className="h-6 w-6" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M9 5H7a2 2 0 00-2 2v12a2 2 0 002 2h10a2 2 0 002-2V7a2 2 0 00-2-2h-2M9 5a2 2 0 002 2h2a2 2 0 002-2M9 5a2 2 0 012-2h2a2 2 0 012 2" />
                </svg>
              )}
            />
          ) : (
            <div className="animate-fade-in">
              <div className="backtest-table-toolbar">
                <div className="backtest-table-toolbar-meta">
                  <span className="label-uppercase">{isNextDayValidation ? '次日验证结果' : '结果列表'}</span>
                  <span className="text-xs text-secondary-text">
                    {codeFilter.trim() ? `已按 ${codeFilter.trim()} 筛选` : '全部股票'}
                    {evalDays ? ` · ${evalDays} 天窗口` : ''}
                    {analysisDateFrom ? ` · 从 ${analysisDateFrom}` : ''}
                    {analysisDateTo ? ` · 到 ${analysisDateTo}` : ''}
                  </span>
                  {(runSetupLabel || evalDays || scoreThreshold || topN) && (
                    <span className="text-xs text-secondary-text">
                      {runSetupLabel ? `${runSetupLabel} · ` : ''}
                      {formatRunSetup({
                        evalWindowDays: evalDays ? parseInt(evalDays, 10) : undefined,
                        scoreThreshold: scoreThreshold ? parseFloat(scoreThreshold) : undefined,
                        topN: topN ? parseInt(topN, 10) : undefined,
                      })}
                    </span>
                  )}
                  <span className="text-xs text-muted-text">
                    结果列表按代码、窗口和日期筛选。分数与前 N 会影响最近一次运行，但这里仍可能包含之前保存的旧结果。
                  </span>
                </div>
                {advancedMode ? (
                  <div className="flex items-center gap-2">
                    <label className="flex items-center gap-2 text-xs text-secondary-text">
                      <span>结果分数 ≥</span>
                      <input
                        type="number"
                        min={0}
                        max={100}
                        step="0.1"
                        aria-label="Result minimum rank score"
                        value={resultScoreMin}
                        onChange={(e) => setResultScoreMin(e.target.value)}
                        placeholder="60"
                        className={`${BACKTEST_COMPACT_INPUT_CLASS} w-20 text-center tabular-nums`}
                      />
                    </label>
                    {resultScoreMin && (
                      <button
                        type="button"
                        onClick={() => setResultScoreMin('')}
                        className="btn-secondary whitespace-nowrap"
                      >
                        清除
                      </button>
                    )}
                    <span className="backtest-table-scroll-hint">小屏幕可横向滚动</span>
                  </div>
                ) : (
                  <span className="backtest-table-scroll-hint">小屏幕可横向滚动</span>
                )}
              </div>
              {displayedResults.length === 0 ? (
                <EmptyState
                  title="没有符合条件的记录"
                  description="当前页结果没有达到结果分数筛选条件。"
                  className="backtest-empty-state border-dashed"
                />
              ) : (
              <div className="backtest-table-wrapper">
                <table className="backtest-table min-w-[980px] w-full text-sm">
                  <thead className="backtest-table-head">
                    <tr className="text-left">
                      <th className="backtest-table-head-cell">股票</th>
                      <th className="backtest-table-head-cell">分析日期</th>
                      <th className="backtest-table-head-cell">排名分</th>
                      <th className="backtest-table-head-cell">AI 预测</th>
                      <th className="backtest-table-head-cell">
                        {showNextDayActualColumns ? '实际结果' : '窗口收益'}
                      </th>
                      <th className="backtest-table-head-cell">
                        {showNextDayActualColumns ? '是否正确' : '方向匹配'}
                      </th>
                      <th className="backtest-table-head-cell">结果</th>
                      <th className="backtest-table-head-cell">状态</th>
                    </tr>
                  </thead>
                  <tbody>
                    {displayedResults.map((row) => (
                      <tr
                        key={row.analysisHistoryId}
                        className="backtest-table-row"
                      >
                        <td className="backtest-table-cell backtest-table-code">
                          <div className="flex flex-col">
                            <span>{row.code}</span>
                            <span className="text-xs text-muted-text">{row.stockName || '--'}</span>
                          </div>
                        </td>
                        <td className="backtest-table-cell text-secondary-text">{row.analysisDate || '--'}</td>
                        <td className="backtest-table-cell">
                          <div className="flex flex-col gap-1">
                            <span className="text-secondary-text">
                              {row.rankingScore != null ? row.rankingScore.toFixed(1) : '--'}
                            </span>
                            <span className="text-xs text-muted-text">{scoreSourceLabel(row.scoreSource)}</span>
                          </div>
                        </td>
                        <td className="backtest-table-cell max-w-[220px] text-foreground">
                          {(row.trendPrediction || row.operationAdvice) ? (
                            <Tooltip
                              content={[row.trendPrediction, row.operationAdvice].filter(Boolean).join(' / ')}
                              focusable
                            >
                              <div className="flex flex-col gap-1">
                                <span className="block truncate">{row.trendPrediction || '--'}</span>
                                <span className="block truncate text-xs text-secondary-text">{row.operationAdvice || '--'}</span>
                              </div>
                            </Tooltip>
                          ) : (
                            '--'
                          )}
                        </td>
                        <td className="backtest-table-cell">
                          <div className="flex items-center gap-2">
                            {actualMovementBadge(row.actualMovement)}
                            <span className={
                              row.actualReturnPct != null
                                ? row.actualReturnPct > 0 ? 'text-success' : row.actualReturnPct < 0 ? 'text-danger' : 'text-secondary-text'
                                : 'text-muted-text'
                            }>
                              {pct(row.actualReturnPct)}
                            </span>
                          </div>
                        </td>
                        <td className="backtest-table-cell">
                          <span className="flex items-center gap-2">
                            {boolIcon(row.directionCorrect)}
                            <span className="text-muted-text">{directionLabel(row.directionExpected)}</span>
                          </span>
                        </td>
                        <td className="backtest-table-cell">{outcomeBadge(row.outcome)}</td>
                        <td className="backtest-table-cell">{statusBadge(row.evalStatus)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
              )}

              {/* Pagination */}
              <div className="mt-4">
                <Pagination
                  currentPage={currentPage}
                  totalPages={totalPages}
                  onPageChange={handlePageChange}
                />
              </div>

              <p className="text-xs text-muted-text text-center mt-2">
                当前页显示 {displayedResults.length} / {results.length} 条记录
                {' · '}
                共 {totalResults} 条结果 · 第 {currentPage} / {Math.max(totalPages, 1)} 页
              </p>
            </div>
          )}
        </section>
      </main>
    </div>
  );
};

export default BacktestPage;
