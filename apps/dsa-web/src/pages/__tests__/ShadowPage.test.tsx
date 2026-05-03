import { render, screen, waitFor } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import { beforeEach, describe, expect, it, vi } from 'vitest';

let ShadowPage: (typeof import('../ShadowPage'))['default'];

const { mockGetShadowDashboard } = vi.hoisted(() => ({
  mockGetShadowDashboard: vi.fn(),
}));

vi.mock('../../api/backtest', () => ({
  backtestApi: {
    getShadowDashboard: mockGetShadowDashboard,
  },
}));

beforeEach(() => {
  vi.clearAllMocks();
  mockGetShadowDashboard.mockResolvedValue({
    status: 'ok',
    generatedAt: '2026-04-30T16:20:00',
    backtestDir: '/tmp/backtests',
    scorecard: {
      status: 'ok',
      jsonPath: '/tmp/scorecard.json',
      reportPath: '/tmp/report.md',
      generatedAt: '2026-04-30T16:20:00',
      primaryWindow: 5,
      minSamples: 50,
      dailyMeta: {},
      candidates: [
        {
          module: '日线技术信号',
          directionType: 'offensive',
          rule: 'VWAP成本线(看多)',
          sampleCount: 15658,
          walkForwardGate: '通过',
          walkForwardPassRatePct: 100,
          permutationGate: '通过',
          pValue: 0.0099,
          costNetAvgReturnPct: 0.18,
          payoffRatio: 1.2,
          finalDecision: '可进Shadow',
          symbolAttribution: {
            top3ContributionPct: 20.8,
            concentrationStatus: '分散',
          },
        },
      ],
      allRows: [],
    },
    ledger: {
      status: 'ok',
      ledgerPath: '/tmp/ledger.jsonl',
      summaryPath: '/tmp/ledger.md',
      totalCount: 20,
      openCount: 18,
      settledCount: 2,
      ruleCounts: [{ rule: 'VWAP成本线(看多)', count: 12 }],
      entries: [
        {
          entryId: '2026-04-29|600519|日线技术信号|VWAP成本线(看多)',
          status: 'open',
          signalDate: '2026-04-29',
          code: '600519',
          module: '日线技术信号',
          directionType: 'offensive',
          rule: 'VWAP成本线(看多)',
          description: '价格相对20日VWAP位置',
          entryPrice: 100.1234,
          marketRegime: '上行',
          windows: [3, 5, 10],
          settlements: {},
        },
      ],
    },
    intradayReplay: {
      status: 'ok',
      ledgerPath: '/tmp/stock_intraday_replay_ledger.jsonl',
      totalCount: 2,
      labeledCount: 2,
      pendingCount: 0,
      effectiveCount: 2,
      effectiveRatePct: 100,
      avgPrimaryReturnPct: -1,
      avgMfePct: 4,
      avgMaePct: -1,
      signalTypeCounts: [
        {
          signalType: 'BUY_SETUP',
          count: 1,
          labeledCount: 1,
          effectiveCount: 1,
          effectiveRatePct: 100,
          avgPrimaryReturnPct: 3,
        },
      ],
      entries: [
        {
          signalId: 'buy-1',
          triggerTimestamp: '2026-04-29T14:35:00+08:00',
          code: '600519',
          name: '贵州茅台',
          scope: 'watchlist_buy',
          signalType: 'BUY_SETUP',
          entryPrice: 100,
          primaryHorizon: 't_plus_5',
          primaryReturnPct: 3,
          effective: true,
          tPlus1ReturnPct: 1,
          tPlus3ReturnPct: 2,
          tPlus5ReturnPct: 3,
          mfePct: 4,
          maePct: -1,
        },
      ],
    },
  });
});

beforeEach(async () => {
  ShadowPage = (await import('../ShadowPage')).default;
});

describe('ShadowPage', () => {
  it('renders scorecard candidates and ledger entries', async () => {
    render(
      <MemoryRouter>
        <ShadowPage />
      </MemoryRouter>,
    );

    expect(await screen.findByRole('heading', { name: 'Shadow 纸面信号' })).toBeInTheDocument();
    expect(screen.getByText('研究入口')).toBeInTheDocument();
    expect(screen.getByText('历史回测')).toBeInTheDocument();
    expect(screen.getByText('信号表现摘要')).toBeInTheDocument();
    expect(screen.getAllByText('自选买入').length).toBeGreaterThan(0);
    expect(screen.getByText('IC Shadow')).toBeInTheDocument();
    expect(screen.getByText('可进 Shadow')).toBeInTheDocument();
    expect(screen.getByText('20')).toBeInTheDocument();
    expect(screen.getAllByText('VWAP成本线(看多)').length).toBeGreaterThan(0);
    expect(screen.getAllByText('600519').length).toBeGreaterThan(0);
    expect(screen.getByText('价格相对20日VWAP位置')).toBeInTheDocument();
    expect(screen.getByText('盘中提醒准确率')).toBeInTheDocument();
    expect(screen.getAllByText('买入提醒').length).toBeGreaterThan(0);
    expect(screen.getByText('贵州茅台 · 2026-04-29T14:35:00+08:00')).toBeInTheDocument();
    expect(screen.getAllByText('查看K线').length).toBeGreaterThan(0);
    expect(screen.getAllByText('观察中').length).toBeGreaterThan(0);

    await waitFor(() => {
      expect(mockGetShadowDashboard).toHaveBeenCalledWith({ limit: 80 });
    });
  });
});
