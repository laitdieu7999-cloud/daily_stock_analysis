import type React from 'react';
import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { useNavigate, useSearchParams } from 'react-router-dom';
import { backtestApi } from '../api/backtest';
import { icApi, type ICMarketSnapshot } from '../api/ic';
import { portfolioApi } from '../api/portfolio';
import { ApiErrorAlert, ConfirmDialog, Button, EmptyState, InlineAlert } from '../components/common';
import { DashboardStateBlock } from '../components/dashboard';
import { StockAutocomplete } from '../components/StockAutocomplete';
import { HistoryList } from '../components/history';
import { ReportMarkdown, ReportSummary } from '../components/report';
import { TaskPanel } from '../components/tasks';
import { useDashboardLifecycle, useHomeDashboardState } from '../hooks';
import { normalizeStockCode, removeMarketSuffix } from '../utils/normalizeQuery';
import { getReportText, normalizeReportLanguage } from '../utils/reportLanguage';

const toHoldingKey = (code?: string | null): string | null => {
  if (!code) {
    return null;
  }
  const normalized = normalizeStockCode(code);
  if (!normalized) {
    return null;
  }
  return removeMarketSuffix(normalized);
};

type IcActionState = {
  mainSymbol: string;
  mainAnnualizedPct: number | null;
  frontGapPct: number | null;
  status: 'normal' | 'watch' | 'collapse' | 'missing';
  label: string;
};

const formatPct = (value: number | null, digits = 1): string => {
  if (value == null || !Number.isFinite(value)) {
    return '--';
  }
  const fixed = value.toFixed(digits);
  return value > 0 ? `+${fixed}%` : `${fixed}%`;
};

const buildIcActionState = (snapshot: ICMarketSnapshot | null): IcActionState => {
  const contracts = [...(snapshot?.contracts || [])]
    .filter((item) => item.symbol && item.daysToExpiry > 0)
    .sort((a, b) => a.daysToExpiry - b.daysToExpiry);
  const main = contracts.find((item) => item.isMain) || contracts[0];
  const front = contracts[0];
  const next = contracts[1];
  const frontGapPct = front && next ? front.annualizedBasisPct - next.annualizedBasisPct : null;

  if (!main) {
    return {
      mainSymbol: '--',
      mainAnnualizedPct: null,
      frontGapPct,
      status: 'missing',
      label: '待刷新',
    };
  }

  if (frontGapPct != null && frontGapPct >= 6) {
    return {
      mainSymbol: main.symbol,
      mainAnnualizedPct: main.annualizedBasisPct,
      frontGapPct,
      status: 'collapse',
      label: '前端塌陷',
    };
  }

  if (frontGapPct != null && frontGapPct >= 3) {
    return {
      mainSymbol: main.symbol,
      mainAnnualizedPct: main.annualizedBasisPct,
      frontGapPct,
      status: 'watch',
      label: '需要关注',
    };
  }

  return {
    mainSymbol: main.symbol,
    mainAnnualizedPct: main.annualizedBasisPct,
    frontGapPct,
    status: 'normal',
    label: '结构正常',
  };
};

const icStatusClass = (status: IcActionState['status']): string => {
  if (status === 'collapse') return 'border-rose-300/70 bg-rose-50/95 text-rose-950 shadow-rose-950/5 dark:border-rose-400/35 dark:bg-rose-500/12 dark:text-rose-100';
  if (status === 'watch') return 'border-amber-300/75 bg-amber-50/95 text-amber-950 shadow-amber-950/5 dark:border-amber-400/35 dark:bg-amber-500/12 dark:text-amber-100';
  if (status === 'missing') return 'border-border/70 bg-card/85 text-secondary-text';
  return 'border-emerald-300/75 bg-emerald-50/95 text-emerald-950 shadow-emerald-950/5 dark:border-emerald-400/35 dark:bg-emerald-500/12 dark:text-emerald-100';
};

const actionCardBaseClass =
  'group rounded-[1.35rem] border p-4 text-left shadow-soft-card transition-all duration-200 hover:-translate-y-0.5 hover:shadow-soft-card-strong focus-visible:outline-none focus-visible:ring-4 focus-visible:ring-cyan/15';

const neutralActionCardClass =
  `${actionCardBaseClass} border-slate-200/90 bg-[linear-gradient(180deg,rgba(255,255,255,0.96),rgba(248,250,252,0.94))] hover:border-sky-200 hover:bg-white dark:border-white/10 dark:bg-card/80 dark:hover:border-cyan/30`;

const actionEyebrowClass =
  'text-[11px] font-bold uppercase tracking-[0.2em] text-slate-500 dark:text-secondary-text';

const actionPillClass =
  'rounded-full border border-slate-200 bg-white/80 px-2.5 py-1 text-[11px] font-semibold text-slate-600 shadow-[inset_0_1px_0_rgba(255,255,255,0.75)] dark:border-white/10 dark:bg-white/5 dark:text-secondary-text';

const HomePage: React.FC = () => {
  const navigate = useNavigate();
  const [searchParams] = useSearchParams();
  const autoLaunchKeyRef = useRef<string | null>(null);
  const [sidebarOpen, setSidebarOpen] = useState(false);
  const [showDeleteConfirm, setShowDeleteConfirm] = useState(false);
  const [holdingCodes, setHoldingCodes] = useState<Set<string>>(new Set());
  const [icSnapshot, setIcSnapshot] = useState<ICMarketSnapshot | null>(null);
  const [icSnapshotFailed, setIcSnapshotFailed] = useState(false);
  const [portfolioLaunchMessage, setPortfolioLaunchMessage] = useState<string | null>(null);

  const {
    query,
    inputError,
    duplicateError,
    error,
    isAnalyzing,
    historyItems,
    selectedHistoryIds,
    isDeletingHistory,
    isLoadingHistory,
    isLoadingMore,
    hasMore,
    selectedReport,
    isLoadingReport,
    activeTasks,
    markdownDrawerOpen,
    setQuery,
    clearError,
    loadInitialHistory,
    refreshHistory,
    loadMoreHistory,
    selectHistoryItem,
    selectLatestHistoryForStock,
    toggleHistorySelection,
    toggleSelectAllVisible,
    deleteSelectedHistory,
    submitAnalysis,
    notify,
    setNotify,
    syncTaskCreated,
    syncTaskUpdated,
    syncTaskFailed,
    removeTask,
    openMarkdownDrawer,
    closeMarkdownDrawer,
    selectedIds,
  } = useHomeDashboardState();

  useEffect(() => {
    document.title = '每日选股分析 - DSA';
  }, []);

  const loadHoldingCodes = useCallback(async () => {
    try {
      const snapshot = await portfolioApi.getSnapshot();
      const next = new Set<string>();
      for (const account of snapshot.accounts || []) {
        for (const position of account.positions || []) {
          const key = toHoldingKey(position.symbol);
          if (key) {
            next.add(key);
          }
        }
      }
      setHoldingCodes(next);
    } catch {
      setHoldingCodes(new Set());
    }
  }, []);

  useEffect(() => {
    void loadHoldingCodes();
  }, [loadHoldingCodes]);

  useEffect(() => {
    let cancelled = false;
    const loadIcSnapshot = async () => {
      try {
        const snapshot = await icApi.getSnapshot();
        if (!cancelled) {
          setIcSnapshot(snapshot);
          setIcSnapshotFailed(false);
        }
      } catch {
        if (!cancelled) {
          setIcSnapshot(null);
          setIcSnapshotFailed(true);
        }
      }
    };

    void loadIcSnapshot();
    return () => {
      cancelled = true;
    };
  }, []);

  const reportLanguage = normalizeReportLanguage(selectedReport?.meta.reportLanguage);
  const reportText = getReportText(reportLanguage);
  const icActionState = useMemo(() => buildIcActionState(icSnapshot), [icSnapshot]);

  useDashboardLifecycle({
    loadInitialHistory,
    refreshHistory,
    refreshHoldingCodes: loadHoldingCodes,
    syncTaskCreated,
    syncTaskUpdated,
    syncTaskFailed,
    selectLatestHistoryForStock,
    removeTask,
  });

  const handleHistoryItemClick = useCallback((recordId: number) => {
    void selectHistoryItem(recordId);
    setSidebarOpen(false);
  }, [selectHistoryItem]);

  const handleSubmitAnalysis = useCallback(
    (
      stockCode?: string,
      stockName?: string,
      selectionSource?: 'manual' | 'autocomplete' | 'import' | 'image' | 'portfolio',
    ) => {
      void submitAnalysis({
        stockCode,
        stockName,
        originalQuery: query,
        selectionSource: selectionSource ?? 'manual',
      });
    },
    [query, submitAnalysis],
  );

  useEffect(() => {
    const source = searchParams.get('source');
    const stockCode = searchParams.get('analyze')?.trim();
    if (source !== 'portfolio' || !stockCode) {
      return;
    }

    const stockName = searchParams.get('name')?.trim() || undefined;
    const forceRefresh = searchParams.get('force') === '1';
    const launchKey = `${stockCode}:${stockName || ''}:${forceRefresh ? 'force' : 'normal'}`;
    if (autoLaunchKeyRef.current === launchKey) {
      return;
    }
    autoLaunchKeyRef.current = launchKey;

    navigate('/', { replace: true });
    setPortfolioLaunchMessage(
      `${stockName || stockCode} 已从持仓发起回测分析；完成后首页会自动显示最新报告。`,
    );
    void backtestApi.run({
      code: stockCode,
      force: true,
      minAgeDays: 0,
    }).catch((err) => {
      console.warn('Portfolio backtest launch failed:', err);
    });
    void submitAnalysis({
      stockCode,
      stockName,
      originalQuery: stockCode,
      selectionSource: 'portfolio',
      forceRefresh,
    });
  }, [navigate, searchParams, submitAnalysis]);

  const handleAskFollowUp = useCallback(() => {
    if (selectedReport?.meta.id === undefined) {
      return;
    }

    const code = selectedReport.meta.stockCode;
    const name = selectedReport.meta.stockName;
    const rid = selectedReport.meta.id;
    navigate(`/chat?stock=${encodeURIComponent(code)}&name=${encodeURIComponent(name)}&recordId=${rid}`);
  }, [navigate, selectedReport]);

  const handleReanalyze = useCallback(() => {
    if (!selectedReport) {
      return;
    }

    void submitAnalysis({
      stockCode: selectedReport.meta.stockCode,
      stockName: selectedReport.meta.stockName,
      originalQuery: selectedReport.meta.stockCode,
      selectionSource: 'manual',
      forceRefresh: true,
    });
  }, [selectedReport, submitAnalysis]);

  const handleDeleteSelectedHistory = useCallback(() => {
    void deleteSelectedHistory();
    setShowDeleteConfirm(false);
  }, [deleteSelectedHistory]);

  const sidebarContent = useMemo(
    () => (
      <div className="flex min-h-0 h-full flex-col gap-3 overflow-hidden">
        <TaskPanel tasks={activeTasks} />
        <HistoryList
          items={historyItems}
          holdingCodes={holdingCodes}
          isLoading={isLoadingHistory}
          isLoadingMore={isLoadingMore}
          hasMore={hasMore}
          selectedId={selectedReport?.meta.id}
          selectedIds={selectedIds}
          isDeleting={isDeletingHistory}
          onItemClick={handleHistoryItemClick}
          onLoadMore={() => void loadMoreHistory()}
          onToggleItemSelection={toggleHistorySelection}
          onToggleSelectAll={toggleSelectAllVisible}
          onDeleteSelected={() => setShowDeleteConfirm(true)}
          className="flex-1 overflow-hidden"
        />
      </div>
    ),
    [
      activeTasks,
      hasMore,
      historyItems,
      holdingCodes,
      isDeletingHistory,
      isLoadingHistory,
      isLoadingMore,
      handleHistoryItemClick,
      loadMoreHistory,
      selectedIds,
      selectedReport?.meta.id,
      toggleHistorySelection,
      toggleSelectAllVisible,
    ],
  );

  return (
    <div
      data-testid="home-dashboard"
      className="flex h-[calc(100vh-5rem)] w-full flex-col overflow-hidden md:flex-row sm:h-[calc(100vh-5.5rem)] lg:h-[calc(100vh-2rem)]"
    >
      <div className="flex-1 flex flex-col min-h-0 min-w-0 max-w-full xl:max-w-7xl mx-auto w-full">
        <header className="flex min-w-0 flex-shrink-0 items-center overflow-visible px-3 py-3 md:px-4 md:py-4">
          <div className="flex min-w-0 flex-1 flex-wrap items-center gap-2.5 md:flex-nowrap">
            <button
              onClick={() => setSidebarOpen(true)}
              className="md:hidden -ml-1 flex-shrink-0 rounded-lg p-1.5 text-secondary-text transition-colors hover:bg-hover hover:text-foreground"
              aria-label="历史记录"
            >
              <svg className="h-5 w-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 6h16M4 12h16M4 18h16" />
              </svg>
            </button>
            <div className="relative min-w-0 flex-1">
              <StockAutocomplete
                value={query}
                onChange={setQuery}
                onSubmit={(stockCode, stockName, selectionSource) => {
                  handleSubmitAnalysis(stockCode, stockName, selectionSource);
                }}
                placeholder="输入股票代码或名称，如 600519、贵州茅台、AAPL"
                disabled={isAnalyzing}
                className={inputError ? 'border-danger/50' : undefined}
              />
            </div>
            <label className="flex h-10 flex-shrink-0 cursor-pointer items-center gap-1.5 rounded-xl border border-slate-200/90 bg-white/75 px-3 text-xs font-medium text-slate-600 shadow-soft-card select-none transition-colors hover:border-sky-200 hover:text-slate-950 dark:border-white/10 dark:bg-card/75 dark:text-secondary-text dark:hover:text-foreground">
              <input
                type="checkbox"
                checked={notify}
                onChange={(e) => setNotify(e.target.checked)}
                className="h-3.5 w-3.5 rounded border-border accent-primary"
              />
              推送通知
            </label>
            <button
              type="button"
              onClick={() => handleSubmitAnalysis()}
              disabled={!query || isAnalyzing}
              className="btn-primary flex h-10 flex-shrink-0 items-center gap-1.5 whitespace-nowrap px-4"
            >
              {isAnalyzing ? (
                <>
                  <svg className="h-3.5 w-3.5 animate-spin" fill="none" viewBox="0 0 24 24">
                    <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
                    <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z" />
                  </svg>
                  分析中
                </>
              ) : (
                '分析'
              )}
            </button>
          </div>
        </header>

        {inputError || duplicateError ? (
          <div className="px-3 pb-2 md:px-4">
            {inputError ? (
              <InlineAlert
                variant="danger"
                title="输入有误"
                message={inputError}
                className="rounded-xl px-3 py-2 text-xs shadow-none"
              />
            ) : null}
            {!inputError && duplicateError ? (
              <InlineAlert
                variant="warning"
                title="任务已存在"
                message={duplicateError}
                className="rounded-xl px-3 py-2 text-xs shadow-none"
              />
            ) : null}
          </div>
        ) : null}
        {portfolioLaunchMessage ? (
          <div className="px-3 pb-2 md:px-4">
            <InlineAlert
              variant="info"
              title="持仓回测分析"
              message={portfolioLaunchMessage}
              className="rounded-xl px-3 py-2 text-xs shadow-none"
            />
          </div>
        ) : null}

        <section className="px-3 pb-3 md:px-4" aria-label="今日行动面板">
          <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
            <button
              type="button"
              onClick={() => navigate('/portfolio')}
              className={neutralActionCardClass}
            >
              <div className="flex items-center justify-between gap-2">
                <p className={actionEyebrowClass}>P1 风控</p>
                <span className={actionPillClass}>持仓</span>
              </div>
              <p className="mt-3 text-2xl font-bold tracking-tight text-slate-950 dark:text-foreground">
                {holdingCodes.size > 0 ? `${holdingCodes.size} 只持仓` : '暂无持仓'}
              </p>
              <p className="mt-1.5 text-sm leading-5 text-slate-600 dark:text-secondary-text">只对破位、止损、异常波动做行动提醒。</p>
            </button>

            <button
              type="button"
              onClick={() => navigate('/ic-calculator')}
              className={`${actionCardBaseClass} ${icStatusClass(icActionState.status)} relative overflow-hidden`}
            >
              <span className="pointer-events-none absolute inset-x-0 top-0 h-px bg-gradient-to-r from-transparent via-current/25 to-transparent opacity-70" />
              <div className="flex items-center justify-between gap-2">
                <p className="text-[11px] font-bold uppercase tracking-[0.2em] opacity-70">IC 期现</p>
                <span className="rounded-full border border-current/25 bg-white/45 px-2.5 py-1 text-[11px] font-semibold dark:bg-white/5">{icActionState.label}</span>
              </div>
              <p className="mt-3 text-2xl font-bold tracking-tight">
                IC {icActionState.mainSymbol} {formatPct(icActionState.mainAnnualizedPct)}
              </p>
              <p className="mt-1.5 text-sm leading-5 opacity-75">
                M1-M2 {formatPct(icActionState.frontGapPct)}
                {icSnapshotFailed ? '，行情待刷新' : '，点击看合约明细'}
              </p>
            </button>

            <button
              type="button"
              onClick={() => navigate('/chat')}
              className={neutralActionCardClass}
            >
              <div className="flex items-center justify-between gap-2">
                <p className={actionEyebrowClass}>P2 机会</p>
                <span className={actionPillClass}>自选</span>
              </div>
              <p className="mt-3 text-2xl font-bold tracking-tight text-slate-950 dark:text-foreground">只推买入点</p>
              <p className="mt-1.5 text-sm leading-5 text-slate-600 dark:text-secondary-text">其余震荡、观察、普通利好全部静默。</p>
            </button>

            <button
              type="button"
              onClick={selectedReport?.meta.id ? openMarkdownDrawer : () => void refreshHistory()}
              className={neutralActionCardClass}
            >
              <div className="flex items-center justify-between gap-2">
                <p className={actionEyebrowClass}>报告</p>
                <span className={actionPillClass}>核心</span>
              </div>
              <p className="mt-3 line-clamp-1 text-2xl font-bold tracking-tight text-slate-950 dark:text-foreground">
                {selectedReport ? selectedReport.meta.stockName || selectedReport.meta.stockCode : '等待报告'}
              </p>
              <p className="mt-1.5 text-sm leading-5 text-slate-600 dark:text-secondary-text">桌面只保留核心报告，辅助内容后台归档。</p>
            </button>
          </div>
        </section>

        <div className="flex-1 flex min-h-0 overflow-hidden">
          <div className="hidden min-h-0 w-64 shrink-0 flex-col overflow-hidden pl-4 pb-4 md:flex lg:w-72">
            {sidebarContent}
          </div>

          {sidebarOpen ? (
            <div className="fixed inset-0 z-40 md:hidden" onClick={() => setSidebarOpen(false)}>
              <div className="page-drawer-overlay absolute inset-0" />
              <div
                className="dashboard-card absolute bottom-0 left-0 top-0 flex w-72 flex-col overflow-hidden !rounded-none !rounded-r-xl p-3 shadow-2xl"
                onClick={(event) => event.stopPropagation()}
              >
                {sidebarContent}
              </div>
            </div>
          ) : null}

          <section className="flex-1 min-w-0 min-h-0 overflow-x-auto overflow-y-auto px-3 pb-4 md:px-6 touch-pan-y">
            {error ? (
              <ApiErrorAlert
                error={error}
                className="mb-3"
                onDismiss={clearError}
              />
            ) : null}
            {isLoadingReport ? (
              <div className="flex h-full flex-col items-center justify-center">
                <DashboardStateBlock title="加载报告中..." loading />
              </div>
            ) : selectedReport ? (
              <div className="max-w-5xl space-y-4 pb-8">
                <div className="flex flex-wrap items-center justify-end gap-2">
                  <Button
                    variant="home-action-ai"
                    size="sm"
                    disabled={isAnalyzing || selectedReport.meta.id === undefined}
                    onClick={handleReanalyze}
                  >
                    <svg className="h-4 w-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15" />
                    </svg>
                    {reportText.reanalyze}
                  </Button>
                  <Button
                    variant="home-action-ai"
                    size="sm"
                    disabled={selectedReport.meta.id === undefined}
                    onClick={handleAskFollowUp}
                  >
                    <svg className="h-4 w-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M8 12h.01M12 12h.01M16 12h.01M21 12c0 4.418-4.03 8-9 8a9.863 9.863 0 01-4.255-.949L3 20l1.395-3.72C3.512 15.042 3 13.574 3 12c0-4.418 4.03-8 9-8s9 3.582 9 8z" />
                    </svg>
                    追问 AI
                  </Button>
                  <Button
                    variant="home-action-ai"
                    size="sm"
                    disabled={selectedReport.meta.id === undefined}
                    onClick={openMarkdownDrawer}
                  >
                    <svg className="h-4 w-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" />
                    </svg>
                    {reportText.fullReport}
                  </Button>
                </div>
                <ReportSummary data={selectedReport} isHistory />
              </div>
            ) : (
              <div className="flex min-h-full flex-col gap-4">
                <div className="flex flex-1 items-center justify-center">
                  <EmptyState
                    title="开始分析"
                    description="输入股票代码进行分析，或从左侧选择历史报告查看。"
                    className="max-w-xl border-dashed"
                    icon={(
                      <svg className="h-6 w-6" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z" />
                      </svg>
                    )}
                  />
                </div>
              </div>
            )}
          </section>
        </div>
      </div>

      {markdownDrawerOpen && selectedReport?.meta.id ? (
        <ReportMarkdown
          recordId={selectedReport.meta.id}
          stockName={selectedReport.meta.stockName || ''}
          stockCode={selectedReport.meta.stockCode}
          reportLanguage={reportLanguage}
          onClose={closeMarkdownDrawer}
        />
      ) : null}

      <ConfirmDialog
        isOpen={showDeleteConfirm}
        title="删除历史记录"
        message={
          selectedHistoryIds.length === 1
            ? '确认删除这条历史记录吗？删除后将不可恢复。'
            : `确认删除选中的 ${selectedHistoryIds.length} 条历史记录吗？删除后将不可恢复。`
        }
        confirmText={isDeletingHistory ? '删除中...' : '确认删除'}
        cancelText="取消"
        isDanger={true}
        onConfirm={handleDeleteSelectedHistory}
        onCancel={() => setShowDeleteConfirm(false)}
      />
    </div>
  );
};

export default HomePage;
