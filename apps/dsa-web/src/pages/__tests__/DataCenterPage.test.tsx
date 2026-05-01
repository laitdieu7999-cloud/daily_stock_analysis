import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import DataCenterPage from '../DataCenterPage';
import { dataCenterApi } from '../../api/dataCenter';

vi.mock('../../api/dataCenter', () => ({
  dataCenterApi: {
    getOverview: vi.fn(),
    runPortfolioBacktest: vi.fn(),
  },
}));

const mockedGetOverview = vi.mocked(dataCenterApi.getOverview);
const mockedRunPortfolioBacktest = vi.mocked(dataCenterApi.runPortfolioBacktest);

const overview = {
  generatedAt: '2026-04-28T18:30:00',
  database: {
    path: '/tmp/stock_analysis.db',
    exists: true,
    sizeBytes: 4096,
    sizeLabel: '4.0 KB',
  },
  marketData: {
    stockCount: 2,
    barCount: 1234,
    firstDate: '2026-04-01',
    latestDate: '2026-04-28',
    dataSources: [{ name: 'unit-test', count: 1234 }],
  },
  analysis: {
    reportCount: 8,
    stockCount: 3,
    latestCreatedAt: '2026-04-28T09:30:00',
  },
  backtests: {
    resultCount: 12,
    summaryCount: 2,
    stockCount: 3,
    latestEvaluatedAt: '2026-04-28T10:00:00',
  },
  portfolio: {
    accountCount: 1,
    activeAccountCount: 1,
    positionCount: 4,
    tradeCount: 10,
    snapshotCount: 1,
    latestUpdatedAt: '2026-04-28T10:10:00',
  },
  news: {
    itemCount: 5,
    stockCount: 2,
    latestFetchedAt: '2026-04-28T10:20:00',
  },
  fundamentals: {
    snapshotCount: 6,
    stockCount: 2,
    latestCreatedAt: '2026-04-28T10:30:00',
  },
  files: [
    {
      key: 'database',
      label: '本地数据库',
      path: '/tmp/stock_analysis.db',
      exists: true,
      fileCount: 1,
      sizeBytes: 4096,
      sizeLabel: '4.0 KB',
    },
  ],
  recommendations: [
    {
      level: 'success',
      title: '分析与回测链路已经开始闭环',
      description: '这台机器可以继续承担本地数据沉淀、批量回测和长期复盘。',
    },
  ],
  warnings: [],
};

describe('DataCenterPage', () => {
  beforeEach(() => {
    mockedGetOverview.mockReset();
    mockedRunPortfolioBacktest.mockReset();
  });

  it('renders local data center overview cards', async () => {
    mockedGetOverview.mockResolvedValue(overview);

    render(<DataCenterPage />);

    expect(await screen.findByText('把这台 Mac 当成你的金融工作站')).toBeInTheDocument();
    expect(screen.getByText('行情数据')).toBeInTheDocument();
    expect(screen.getByText('1,234')).toBeInTheDocument();
    expect(screen.getByText('持仓资产')).toBeInTheDocument();
    expect(screen.getByText('本地数据库')).toBeInTheDocument();
    expect(screen.getByText('unit-test')).toBeInTheDocument();
    expect(screen.getByText('分析与回测链路已经开始闭环')).toBeInTheDocument();
  });

  it('refreshes overview on demand', async () => {
    mockedGetOverview.mockResolvedValue(overview);

    render(<DataCenterPage />);
    await screen.findByText('本地数据库');

    fireEvent.click(screen.getByRole('button', { name: '刷新盘点' }));

    await waitFor(() => {
      expect(mockedGetOverview).toHaveBeenCalledTimes(2);
    });
  });

  it('runs portfolio backtest from the data center page', async () => {
    mockedGetOverview.mockResolvedValue(overview);
    mockedRunPortfolioBacktest.mockResolvedValue({
      generatedAt: '2026-04-28T18:40:00',
      holdingCount: 1,
      processedSymbols: 1,
      totals: {
        candidateCount: 1,
        processed: 1,
        saved: 1,
        completed: 1,
        insufficient: 0,
        errors: 0,
      },
      items: [
        {
          code: '600519',
          status: 'ok',
          message: '已完成',
          candidateCount: 1,
          processed: 1,
          saved: 1,
          completed: 1,
          insufficient: 0,
          errors: 0,
          summary: {
            winRatePct: 66.67,
            avgSimulatedReturnPct: 8.2,
          },
        },
      ],
    });

    render(<DataCenterPage />);
    await screen.findByText('本地数据库');

    fireEvent.click(screen.getByRole('button', { name: '回测当前持仓' }));

    await waitFor(() => {
      expect(mockedRunPortfolioBacktest).toHaveBeenCalledWith({ limitPerSymbol: 50 });
    });
    expect(await screen.findByText('600519')).toBeInTheDocument();
    expect(screen.getByText('中文结论')).toBeInTheDocument();
    expect(screen.getByText('600519：历史表现较好')).toBeInTheDocument();
    expect(screen.getByText('优先关注')).toBeInTheDocument();
    expect(screen.getByText('66.67%')).toBeInTheDocument();
    expect(screen.getByText('8.20%')).toBeInTheDocument();
  });

  it('does not force conclusions when portfolio backtest has no samples', async () => {
    mockedGetOverview.mockResolvedValue(overview);
    mockedRunPortfolioBacktest.mockResolvedValue({
      generatedAt: '2026-04-28T18:40:00',
      holdingCount: 1,
      processedSymbols: 1,
      totals: {
        candidateCount: 0,
        processed: 0,
        saved: 0,
        completed: 0,
        insufficient: 0,
        errors: 0,
      },
      items: [
        {
          code: '000001',
          status: 'ok',
          message: '暂无可回测的历史分析样本',
          candidateCount: 0,
          processed: 0,
          saved: 0,
          completed: 0,
          insufficient: 0,
          errors: 0,
          summary: null,
        },
      ],
    });

    render(<DataCenterPage />);
    await screen.findByText('本地数据库');

    fireEvent.click(screen.getByRole('button', { name: '回测当前持仓' }));

    expect(await screen.findByText('000001：暂无历史分析')).toBeInTheDocument();
    expect(screen.getByText('先分析')).toBeInTheDocument();
    expect(screen.getByText('暂无可回测的历史分析样本')).toBeInTheDocument();
  });

  it('marks insufficient backtest results as immature instead of no samples', async () => {
    mockedGetOverview.mockResolvedValue(overview);
    mockedRunPortfolioBacktest.mockResolvedValue({
      generatedAt: '2026-04-28T18:40:00',
      holdingCount: 1,
      processedSymbols: 1,
      totals: {
        candidateCount: 1,
        processed: 1,
        saved: 1,
        completed: 0,
        insufficient: 1,
        errors: 0,
      },
      items: [
        {
          code: '600918',
          status: 'ok',
          message: '已完成',
          candidateCount: 1,
          processed: 1,
          saved: 1,
          completed: 0,
          insufficient: 1,
          errors: 0,
          summary: null,
        },
      ],
    });

    render(<DataCenterPage />);
    await screen.findByText('本地数据库');

    fireEvent.click(screen.getByRole('button', { name: '回测当前持仓' }));

    expect(await screen.findByText('600918：已有分析但回测未成熟')).toBeInTheDocument();
    expect(screen.getByText('待成熟')).toBeInTheDocument();
  });
});
