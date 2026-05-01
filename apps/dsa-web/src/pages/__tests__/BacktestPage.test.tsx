import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';

let BacktestPage: (typeof import('../BacktestPage'))['default'];

const {
  mockGetResults,
  mockGetOverallPerformance,
  mockGetStockPerformance,
  mockRun,
  mockScan,
} = vi.hoisted(() => ({
  mockGetResults: vi.fn(),
  mockGetOverallPerformance: vi.fn(),
  mockGetStockPerformance: vi.fn(),
  mockRun: vi.fn(),
  mockScan: vi.fn(),
}));

vi.mock('../../api/backtest', () => ({
  backtestApi: {
    getResults: mockGetResults,
    getOverallPerformance: mockGetOverallPerformance,
    getStockPerformance: mockGetStockPerformance,
    run: mockRun,
    scan: mockScan,
  },
}));

const basePerformance = {
  scope: 'overall',
  evalWindowDays: 10,
  engineVersion: 'test-engine',
  totalEvaluations: 3,
  completedCount: 2,
  insufficientCount: 1,
  longCount: 2,
  cashCount: 1,
  winCount: 1,
  lossCount: 1,
  neutralCount: 0,
  directionAccuracyPct: 66.7,
  winRatePct: 50,
  neutralRatePct: 0,
  avgStockReturnPct: 2.4,
  avgSimulatedReturnPct: 1.2,
  stopLossTriggerRate: 10,
  takeProfitTriggerRate: 20,
  ambiguousRate: 0,
  avgDaysToFirstHit: 3.5,
  adviceBreakdown: {},
  diagnostics: {},
};

beforeEach(() => {
  vi.clearAllMocks();
  Object.defineProperty(globalThis, 'localStorage', {
    value: {
      getItem: vi.fn(() => null),
      setItem: vi.fn(),
      removeItem: vi.fn(),
      clear: vi.fn(),
    },
    configurable: true,
  });
  mockGetOverallPerformance.mockResolvedValue(basePerformance);
  mockGetStockPerformance.mockResolvedValue(null);
  mockGetResults.mockResolvedValue({
    total: 2,
    page: 1,
    limit: 20,
    items: [
      {
        analysisHistoryId: 101,
        code: '600519',
        stockName: '贵州茅台',
        analysisDate: '2026-03-20',
        evalWindowDays: 10,
        engineVersion: 'test-engine',
        evalStatus: 'completed',
        operationAdvice: '继续持有',
        rankingScore: 72,
        scoreSource: 'signal_score',
        trendPrediction: '震荡偏多',
        actualMovement: 'up',
        actualReturnPct: 3.8,
        directionExpected: 'long',
        directionCorrect: true,
        outcome: 'win',
        simulatedReturnPct: 3.8,
      },
      {
        analysisHistoryId: 102,
        code: '000001',
        stockName: '平安银行',
        analysisDate: '2026-03-19',
        evalWindowDays: 10,
        engineVersion: 'test-engine',
        evalStatus: 'completed',
        operationAdvice: '观望',
        rankingScore: 48,
        scoreSource: 'sentiment_score',
        trendPrediction: '震荡',
        actualMovement: 'down',
        actualReturnPct: -1.2,
        directionExpected: 'cash',
        directionCorrect: false,
        outcome: 'loss',
        simulatedReturnPct: -0.8,
      },
    ],
  });
  mockRun.mockResolvedValue({
    candidateCount: 1,
    processed: 1,
    saved: 1,
    completed: 1,
    insufficient: 0,
    errors: 0,
  });
  mockScan.mockResolvedValue({
    rawCandidateCount: 12,
    rankedCandidateCount: 10,
    localDataOnly: true,
    bestByReturn: {
      evalWindowDays: 10,
      scoreThreshold: 60,
      topN: 3,
      candidateCount: 3,
      completedCount: 3,
      insufficientCount: 0,
      winCount: 2,
      lossCount: 1,
      neutralCount: 0,
      winRatePct: 66.7,
      directionAccuracyPct: 66.7,
      avgStockReturnPct: 2.4,
      avgSimulatedReturnPct: 1.8,
      stopLossTriggerRate: 0,
      takeProfitTriggerRate: 33.3,
      adviceBreakdown: {},
      diagnostics: {},
    },
    bestByWinRate: {
      evalWindowDays: 10,
      scoreThreshold: 70,
      topN: 3,
      candidateCount: 2,
      completedCount: 2,
      insufficientCount: 0,
      winCount: 2,
      lossCount: 0,
      neutralCount: 0,
      winRatePct: 100,
      directionAccuracyPct: 100,
      avgStockReturnPct: 3.1,
      avgSimulatedReturnPct: 1.2,
      stopLossTriggerRate: 0,
      takeProfitTriggerRate: 50,
      adviceBreakdown: {},
      diagnostics: {},
    },
    conclusion: {
      status: 'ok',
      summaryText: '优先考虑持有 10 天，分数阈值 60，前 N 3；模拟收益 1.80%, 胜率 66.70%。',
      recommendedScan: {
        evalWindowDays: 10,
        scoreThreshold: 60,
        topN: 3,
        candidateCount: 3,
        completedCount: 3,
        insufficientCount: 0,
        winCount: 2,
        lossCount: 1,
        neutralCount: 0,
        winRatePct: 66.7,
        directionAccuracyPct: 66.7,
        avgStockReturnPct: 2.4,
        avgSimulatedReturnPct: 1.8,
        stopLossTriggerRate: 0,
        takeProfitTriggerRate: 33.3,
        adviceBreakdown: {},
        diagnostics: {},
      },
      secondaryScan: {
        evalWindowDays: 10,
        scoreThreshold: 70,
        topN: 3,
        candidateCount: 2,
        completedCount: 2,
        insufficientCount: 0,
        winCount: 2,
        lossCount: 0,
        neutralCount: 0,
        winRatePct: 100,
        directionAccuracyPct: 100,
        avgStockReturnPct: 3.1,
        avgSimulatedReturnPct: 1.2,
        stopLossTriggerRate: 0,
        takeProfitTriggerRate: 50,
        adviceBreakdown: {},
        diagnostics: {},
      },
    },
    scans: [],
  });
});

beforeEach(async () => {
  BacktestPage = (await import('../BacktestPage')).default;
});

describe('BacktestPage', () => {
  it('renders shared surface inputs and prediction tracking outputs', async () => {
    render(<BacktestPage />);

    const filterInput = await screen.findByPlaceholderText('输入股票代码，不填则查看全部');
    const windowInput = screen.getByPlaceholderText('10');

    expect(filterInput).toHaveClass('input-surface');
    expect(filterInput).toHaveClass('input-focus-glow');
    expect(windowInput).toHaveClass('input-surface');
    expect(windowInput).toHaveClass('input-focus-glow');
    expect(screen.getByText(/简单模式：/)).toBeInTheDocument();
    expect(screen.getByText('快速上手')).toBeInTheDocument();
    expect(screen.getByText(/不确定怎么选时，先点参数扫描，再点运行推荐组合/)).toBeInTheDocument();
    expect(screen.getByRole('button', { name: '高级模式已关' })).toBeInTheDocument();
    expect(screen.queryByLabelText('Score threshold')).not.toBeInTheDocument();

    expect(await screen.findByText('盈利')).toBeInTheDocument();
    expect(screen.getAllByText('已完成').length).toBeGreaterThan(0);
    expect(screen.getByText('600519')).toBeInTheDocument();
    expect(screen.getByText('贵州茅台')).toBeInTheDocument();
    expect(screen.getByText('震荡偏多')).toBeInTheDocument();
    expect(screen.getByText('上涨')).toBeInTheDocument();
    expect(screen.getByText('窗口收益')).toBeInTheDocument();
    expect(screen.getByText('方向匹配')).toBeInTheDocument();
    expect(screen.getByText('排名分')).toBeInTheDocument();
    expect(screen.getByText('72.0')).toBeInTheDocument();
    expect(screen.getByText('48.0')).toBeInTheDocument();
    expect(screen.getByText('信号分')).toBeInTheDocument();
    expect(screen.getAllByLabelText('yes').length).toBeGreaterThan(0);
  });

  it('filters the current page by minimum rank score', async () => {
    render(<BacktestPage />);

    await screen.findByText('72.0');
    expect(screen.getByText('平安银行')).toBeInTheDocument();

    fireEvent.click(screen.getByRole('button', { name: '高级模式已关' }));
    fireEvent.change(screen.getByLabelText('Result minimum rank score'), { target: { value: '60' } });

    expect(screen.getByText('贵州茅台')).toBeInTheDocument();
    expect(screen.queryByText('平安银行')).not.toBeInTheDocument();
    expect(screen.getByText(/当前页显示 1 \/ 2 条记录/)).toBeInTheDocument();
  });

  it('filters results with stock code, window, and analysis date range when clicking Filter', async () => {
    render(<BacktestPage />);

    const filterInput = await screen.findByPlaceholderText('输入股票代码，不填则查看全部');
    const windowInput = screen.getByPlaceholderText('10');
    fireEvent.click(screen.getByRole('button', { name: '高级模式已关' }));
    const fromInput = screen.getByLabelText('Analysis date from');
    const toInput = screen.getByLabelText('Analysis date to');

    fireEvent.change(filterInput, { target: { value: 'aapl' } });
    fireEvent.change(windowInput, { target: { value: '20' } });
    fireEvent.change(fromInput, { target: { value: '2026-03-01' } });
    fireEvent.change(toInput, { target: { value: '2026-03-31' } });
    fireEvent.click(screen.getByRole('button', { name: '筛选' }));

    await waitFor(() => {
      expect(mockGetResults).toHaveBeenLastCalledWith({
        code: 'AAPL',
        evalWindowDays: 20,
        analysisDateFrom: '2026-03-01',
        analysisDateTo: '2026-03-31',
        page: 1,
        limit: 20,
      });
      expect(mockGetStockPerformance).toHaveBeenLastCalledWith('AAPL', {
        evalWindowDays: 20,
        analysisDateFrom: '2026-03-01',
        analysisDateTo: '2026-03-31',
      });
    });
  });

  it('runs a backtest and refreshes results using the shared filter values', async () => {
    render(<BacktestPage />);

    const filterInput = await screen.findByPlaceholderText('输入股票代码，不填则查看全部');
    const windowInput = screen.getByPlaceholderText('10');

    fireEvent.change(filterInput, { target: { value: 'tsla' } });
    fireEvent.change(windowInput, { target: { value: '15' } });
    fireEvent.click(screen.getByRole('button', { name: '运行回测' }));

    await waitFor(() => {
      expect(mockRun).toHaveBeenCalledWith({
        code: 'TSLA',
        force: undefined,
        minAgeDays: undefined,
        evalWindowDays: 15,
        scoreThreshold: undefined,
        topN: undefined,
      });
    });

    await waitFor(() => {
      expect(mockGetResults).toHaveBeenLastCalledWith({
        code: 'TSLA',
        evalWindowDays: 15,
        analysisDateFrom: undefined,
        analysisDateTo: undefined,
        page: 1,
        limit: 20,
      });
      expect(mockGetStockPerformance).toHaveBeenLastCalledWith('TSLA', {
        evalWindowDays: 15,
        analysisDateFrom: undefined,
        analysisDateTo: undefined,
      });
    });

    expect(await screen.findByText('已处理:')).toBeInTheDocument();
    expect(screen.getByText('已保存:')).toBeInTheDocument();
  });

  it('runs parameter scan and shows conclusion summary', async () => {
    render(<BacktestPage />);

    const filterInput = await screen.findByPlaceholderText('输入股票代码，不填则查看全部');
    const windowInput = screen.getByPlaceholderText('10');

    fireEvent.change(filterInput, { target: { value: 'tsla' } });
    fireEvent.change(windowInput, { target: { value: '15' } });
    fireEvent.click(screen.getByRole('button', { name: '参数扫描' }));

    await waitFor(() => {
      expect(mockScan).toHaveBeenCalledWith({
        code: 'TSLA',
        minAgeDays: 0,
        limit: 200,
        localDataOnly: true,
        evalWindowDaysOptions: [15],
        scoreThresholdOptions: [null, 60, 70],
        topNOptions: [null, 3, 5],
      });
    });

    expect((await screen.findAllByText('参数扫描')).length).toBeGreaterThan(0);
    expect(screen.getByText(/优先考虑持有 10 天/)).toBeInTheDocument();
    expect(screen.getAllByText('推荐组合').length).toBeGreaterThan(0);
    expect(screen.getByText('备选组合')).toBeInTheDocument();
    expect(screen.getByText('推荐与备选对比')).toBeInTheDocument();
    expect(screen.getByText('收益差')).toBeInTheDocument();
    expect(screen.getByText('+0.6%')).toBeInTheDocument();
    expect(screen.getByText('-33.3pt')).toBeInTheDocument();
  });

  it('applies a recommended scan back into the form and uses it for the next run', async () => {
    render(<BacktestPage />);

    const filterInput = await screen.findByPlaceholderText('输入股票代码，不填则查看全部');
    const windowInput = screen.getByPlaceholderText('10');

    fireEvent.change(filterInput, { target: { value: 'tsla' } });
    fireEvent.change(windowInput, { target: { value: '15' } });
    fireEvent.click(screen.getByRole('button', { name: '参数扫描' }));

    await screen.findByText('应用推荐');
    fireEvent.click(screen.getByRole('button', { name: '应用推荐' }));

    fireEvent.click(screen.getByRole('button', { name: '高级模式已关' }));
    expect(screen.getByPlaceholderText('10')).toHaveValue(10);
    expect(screen.getByLabelText('Score threshold')).toHaveValue(60);
    expect(screen.getByLabelText('Top N')).toHaveValue(3);

    fireEvent.click(screen.getByRole('button', { name: '运行回测' }));

    await waitFor(() => {
      expect(mockRun).toHaveBeenLastCalledWith({
        code: 'TSLA',
        force: undefined,
        minAgeDays: undefined,
        evalWindowDays: 10,
        scoreThreshold: 60,
        topN: 3,
      });
    });
  });

  it('runs the recommended scan directly from the summary card', async () => {
    render(<BacktestPage />);

    const filterInput = await screen.findByPlaceholderText('输入股票代码，不填则查看全部');
    fireEvent.change(filterInput, { target: { value: 'tsla' } });
    fireEvent.click(screen.getByRole('button', { name: '参数扫描' }));

    await screen.findByText('运行推荐组合');
    fireEvent.click(screen.getByRole('button', { name: '运行推荐组合' }));

    await waitFor(() => {
      expect(mockRun).toHaveBeenLastCalledWith({
        code: 'TSLA',
        force: undefined,
        minAgeDays: undefined,
        evalWindowDays: 10,
        scoreThreshold: 60,
        topN: 3,
      });
    });

    fireEvent.click(screen.getByRole('button', { name: '高级模式已关' }));
    expect(screen.getByPlaceholderText('10')).toHaveValue(10);
    expect(screen.getByLabelText('Score threshold')).toHaveValue(60);
    expect(screen.getByLabelText('Top N')).toHaveValue(3);
    expect(await screen.findByText('最近一次运行:')).toBeInTheDocument();
    expect(screen.getAllByText('推荐组合').length).toBeGreaterThan(0);
    expect(screen.getAllByText(/窗口 10 天/).length).toBeGreaterThan(0);
    expect(screen.getByText(/结果列表按代码、窗口和日期筛选/)).toBeInTheDocument();
  });

  it('switches to next-day validation with the 1D shortcut', async () => {
    render(<BacktestPage />);

    await screen.findByPlaceholderText('输入股票代码，不填则查看全部');
    fireEvent.click(screen.getByRole('button', { name: '高级模式已关' }));
    fireEvent.click(screen.getByRole('button', { name: '次日验证' }));

    await waitFor(() => {
      expect(mockGetResults).toHaveBeenLastCalledWith({
        code: undefined,
        evalWindowDays: 1,
        analysisDateFrom: undefined,
        analysisDateTo: undefined,
        page: 1,
        limit: 20,
      });
      expect(mockGetOverallPerformance).toHaveBeenLastCalledWith({
        evalWindowDays: 1,
        analysisDateFrom: undefined,
        analysisDateTo: undefined,
      });
    });

    expect(screen.getByText('实际结果')).toBeInTheDocument();
    expect(screen.getByText('是否正确')).toBeInTheDocument();
    expect(screen.getByText('次日验证模式会把 AI 预测与下一交易日收盘结果进行对照。')).toBeInTheDocument();
  });
});
