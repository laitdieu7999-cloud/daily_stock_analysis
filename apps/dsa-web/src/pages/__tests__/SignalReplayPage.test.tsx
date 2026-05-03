import { render, screen, waitFor } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import SignalReplayPage from '../SignalReplayPage';
import { stocksApi } from '../../api/stocks';

vi.mock('../../api/stocks', () => ({
  stocksApi: {
    getHistory: vi.fn(),
  },
}));

const mockedGetHistory = vi.mocked(stocksApi.getHistory);

const makeRows = () => Array.from({ length: 30 }, (_, index) => {
  const close = 100 + index;
  return {
    date: `2026-04-${String(index + 1).padStart(2, '0')}`,
    open: close - 0.5,
    high: close + 1,
    low: close - 1,
    close,
    volume: 10000 + index,
    amount: 1000000 + index,
    changePercent: 1,
  };
});

describe('SignalReplayPage', () => {
  beforeEach(() => {
    mockedGetHistory.mockReset();
    mockedGetHistory.mockResolvedValue({
      stockCode: '600519',
      stockName: '贵州茅台',
      period: 'daily',
      data: makeRows(),
    });
  });

  it('renders daily kline replay with MA and signal summary', async () => {
    render(
      <MemoryRouter initialEntries={['/signal-replay?code=600519&name=%E8%B4%B5%E5%B7%9E%E8%8C%85%E5%8F%B0&signalDate=2026-04-20&entryPrice=119&mfePct=4&maePct=-1&signalType=%E4%B9%B0%E5%85%A5%E6%8F%90%E9%86%92']}>
        <SignalReplayPage />
      </MemoryRouter>,
    );

    expect(await screen.findByRole('heading', { name: '信号 K 线复盘' })).toBeInTheDocument();
    expect(screen.getByText('贵州茅台 600519')).toBeInTheDocument();
    expect(screen.getByRole('img', { name: '信号K线复盘图' })).toBeInTheDocument();
    expect(screen.getByText('MA5')).toBeInTheDocument();
    expect(screen.getByText('MA20')).toBeInTheDocument();
    expect(screen.getByText('涨红跌绿，符合中国市场习惯。')).toBeInTheDocument();
    expect(screen.getByText('2026-04-20')).toBeInTheDocument();
    expect(screen.getByText('119.0000')).toBeInTheDocument();
    expect(screen.getByText('4.00%')).toBeInTheDocument();
    expect(screen.getByText('-1.00%')).toBeInTheDocument();

    await waitFor(() => {
      expect(mockedGetHistory).toHaveBeenCalledWith('600519', { days: 160 });
    });
  });
});
