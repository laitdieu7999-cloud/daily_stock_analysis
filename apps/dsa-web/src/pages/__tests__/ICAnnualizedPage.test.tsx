import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import ICAnnualizedPage, { calculateIcAnnualized } from '../ICAnnualizedPage';
import { icApi } from '../../api/ic';

vi.mock('../../api/ic', () => ({
  icApi: {
    getSnapshot: vi.fn(),
  },
}));

const mockedGetSnapshot = vi.mocked(icApi.getSnapshot);

describe('ICAnnualizedPage', () => {
  beforeEach(() => {
    mockedGetSnapshot.mockReset();
  });

  it('calculates IC annualized basis yield with the project formula', () => {
    expect(calculateIcAnnualized(5800, 5700, 30)).toMatchObject({
      basis: 100,
      marketState: '贴水',
    });
    expect(calculateIcAnnualized(5800, 5700, 30)?.annualizedPct).toBeCloseTo(21.345, 2);
  });

  it('loads snapshot data and shows selected contract details', async () => {
    mockedGetSnapshot.mockResolvedValue({
      spotPrice: 8299.05,
      mainContractCode: 'IC2606',
      fetchedAt: '2026-04-27T09:30:00',
      optionProxy: {
        boardTimestamp: '2026-04-27',
        expiryYm: '2606',
        expiryStyle: 'M',
        qvixLatest: 21.23,
        qvixPrev: 20.5,
        qvixJumpPct: 3.56,
        qvixZscore: 1.2,
        atmStrike: 8.3,
        atmPutPrice: 0.12,
        otmPutStrike: 7.9,
        otmPutPrice: 0.05,
        putSkewRatio: 0.417,
        atmPutCallVolumeRatio: 1.58,
        expiryDaysToExpiry: 44,
        rollWindowShifted: false,
      },
      contracts: [
        {
          symbol: 'IC2606',
          price: 8186.4,
          expiryDate: '2026-06-19',
          daysToExpiry: 53,
          termGapDays: 0,
          basis: 112.65,
          annualizedBasisPct: 9.48,
          isMain: true,
        },
        {
          symbol: 'IC2609',
          price: 8042.4,
          expiryDate: '2026-09-18',
          daysToExpiry: 144,
          termGapDays: 91,
          basis: 256.65,
          annualizedBasisPct: 8.09,
          isMain: false,
        },
      ],
    });

    render(<ICAnnualizedPage />);

    expect((await screen.findAllByText('现货价格')).length).toBeGreaterThan(0);
    expect(screen.getByText('8299.05')).toBeInTheDocument();
    expect(screen.getByText('已选合约')).toBeInTheDocument();
    expect(screen.getAllByText('IC2606').length).toBeGreaterThan(0);
    expect(screen.getAllByText('2026/06/19周五').length).toBeGreaterThan(0);
    expect(screen.getAllByText('+9.48%').length).toBeGreaterThan(0);
    expect(screen.getByText('M1-M2 状态')).toBeInTheDocument();
    expect(screen.getByText('结构正常')).toBeInTheDocument();
    expect(screen.getAllByText('+1.39%').length).toBeGreaterThan(0);
    expect(screen.getByText('QVIX 21.23 / Skew 0.417')).toBeInTheDocument();
    expect(screen.getByText('PCR 1.58 · 2606')).toBeInTheDocument();
    expect(screen.getByText(/IC 执行提示/)).toBeInTheDocument();
    expect(screen.queryByText('主力')).not.toBeInTheDocument();
  });

  it('refreshes prices and switches selected contract when a contract name is clicked', async () => {
    mockedGetSnapshot
      .mockResolvedValueOnce({
        spotPrice: 8299.05,
        mainContractCode: 'IC2606',
        fetchedAt: '2026-04-27T09:30:00',
        contracts: [
          {
            symbol: 'IC2606',
            price: 8186.4,
            expiryDate: '2026-06-19',
            daysToExpiry: 53,
            termGapDays: 0,
            basis: 112.65,
            annualizedBasisPct: 9.48,
            isMain: true,
          },
          {
            symbol: 'IC2609',
            price: 8042.4,
            expiryDate: '2026-09-18',
            daysToExpiry: 144,
            termGapDays: 91,
            basis: 256.65,
            annualizedBasisPct: 8.09,
            isMain: false,
          },
        ],
      })
      .mockResolvedValueOnce({
        spotPrice: 8310.0,
        mainContractCode: 'IC2606',
        fetchedAt: '2026-04-27T09:31:00',
        contracts: [
          {
            symbol: 'IC2606',
            price: 8190.0,
            expiryDate: '2026-06-19',
            daysToExpiry: 53,
            termGapDays: 0,
            basis: 120.0,
            annualizedBasisPct: 10.08,
            isMain: true,
          },
          {
            symbol: 'IC2609',
            price: 8055.0,
            expiryDate: '2026-09-18',
            daysToExpiry: 144,
            termGapDays: 91,
            basis: 255.0,
            annualizedBasisPct: 8.03,
            isMain: false,
          },
        ],
      });

    render(<ICAnnualizedPage />);
    await screen.findByRole('button', { name: /IC2606/ });

    const contractButton = screen.getAllByRole('button', { name: /IC2609/ })[0];
    fireEvent.click(contractButton);

    await waitFor(() => {
      expect(mockedGetSnapshot).toHaveBeenCalledTimes(2);
    });

    expect(screen.getByText('已选合约')).toBeInTheDocument();
    expect(screen.getAllByText('8310.00').length).toBeGreaterThan(0);
    expect(screen.getAllByText('8055.00').length).toBeGreaterThan(0);
    expect(screen.getAllByText('2026/09/18周五').length).toBeGreaterThan(0);
    expect(screen.getAllByText('+8.03%').length).toBeGreaterThan(0);
  });

  it('colors annualized rate cells by discount depth bands only', async () => {
    mockedGetSnapshot.mockResolvedValue({
      spotPrice: 8299.05,
      mainContractCode: 'IC2606',
      fetchedAt: '2026-04-27T09:30:00',
      contracts: [
        {
          symbol: 'IC2606',
          price: 8186.4,
          expiryDate: '2026-06-19',
          daysToExpiry: 53,
          termGapDays: 0,
          basis: 112.65,
          annualizedBasisPct: 9.48,
          isMain: true,
        },
        {
          symbol: 'IC2605',
          price: 8280.0,
          expiryDate: '2026-05-15',
          daysToExpiry: 18,
          termGapDays: 0,
          basis: 19.05,
          annualizedBasisPct: 4.66,
          isMain: false,
        },
        {
          symbol: 'IC2609',
          price: 8042.4,
          expiryDate: '2026-09-18',
          daysToExpiry: 144,
          termGapDays: 91,
          basis: 256.65,
          annualizedBasisPct: 12.09,
          isMain: false,
        },
        {
          symbol: 'IC2612',
          price: 7899.0,
          expiryDate: '2026-12-18',
          daysToExpiry: 235,
          termGapDays: 182,
          basis: 400.05,
          annualizedBasisPct: 16.25,
          isMain: false,
        },
      ],
    });

    const { container } = render(<ICAnnualizedPage />);
    await screen.findByRole('button', { name: /IC2605/ });

    const contractButton = screen.getByRole('button', { name: /IC2605/ });
    expect(contractButton.className).not.toContain('text-emerald-400');

    const daysCell = Array.from(container.querySelectorAll('td')).find((node) => node.textContent === '18');
    expect(daysCell?.className).not.toContain('text-emerald-300');

    const shallowCell = Array.from(container.querySelectorAll('td')).find((node) => node.textContent === '+4.66%');
    expect(shallowCell?.className).toContain('text-emerald-400');

    const deepCell = Array.from(container.querySelectorAll('td')).find((node) => node.textContent === '+12.09%');
    expect(deepCell?.className).toContain('text-rose-400');

    const extremeCell = Array.from(container.querySelectorAll('td')).find((node) => node.textContent === '+16.25%');
    expect(extremeCell?.className).toContain('text-violet-400');

    const normalCell = Array.from(container.querySelectorAll('td')).find((node) => node.textContent === '+9.48%');
    expect(normalCell?.className).toContain('text-foreground');
  });

  it('keeps 9 percent annualized basis in default color', async () => {
    mockedGetSnapshot.mockResolvedValue({
      spotPrice: 8299.05,
      mainContractCode: 'IC2606',
      fetchedAt: '2026-04-27T09:30:00',
      contracts: [
        {
          symbol: 'IC2606',
          price: 8186.4,
          expiryDate: '2026-06-19',
          daysToExpiry: 53,
          termGapDays: 0,
          basis: 112.65,
          annualizedBasisPct: 9.0,
          isMain: true,
        },
      ],
    });

    render(<ICAnnualizedPage />);
    const button = await screen.findByRole('button', { name: /IC2606/ });
    expect(button.className).not.toContain('text-emerald-400');
    expect(screen.getAllByText('正常贴水').length).toBeGreaterThan(0);
    expect(screen.getAllByText('+9.00%')[0].className).toContain('text-foreground');
  });

  it('renders a manual calculator seeded from the selected contract', async () => {
    mockedGetSnapshot.mockResolvedValue({
      spotPrice: 8299.05,
      mainContractCode: 'IC2606',
      fetchedAt: '2026-04-27T09:30:00',
      contracts: [
        {
          symbol: 'IC2606',
          price: 8186.4,
          expiryDate: '2026-06-19',
          daysToExpiry: 53,
          termGapDays: 0,
          basis: 112.65,
          annualizedBasisPct: 9.48,
          isMain: true,
        },
      ],
    });

    render(<ICAnnualizedPage />);
    expect(await screen.findByText('手动计算器')).toBeInTheDocument();
    await waitFor(() => {
      expect(screen.getByLabelText('现货价格')).toHaveValue('8299.05');
      expect(screen.getByLabelText('期现差')).toHaveValue('112.65');
      expect(screen.getByLabelText('期货价格')).toHaveValue('8186.40');
      expect(screen.getByLabelText('剩余天数')).toHaveValue('53');
    });
    expect(screen.getByLabelText('期货价格')).toHaveAttribute('readonly');

    fireEvent.change(screen.getByLabelText('现货价格'), { target: { value: '8300' } });
    fireEvent.change(screen.getByLabelText('期现差'), { target: { value: '100' } });
    fireEvent.change(screen.getByLabelText('剩余天数'), { target: { value: '30' } });

    expect(screen.getByLabelText('期货价格')).toHaveValue('8200.00');
    expect(screen.getByText('+14.84%')).toBeInTheDocument();
    expect(screen.getByText('深水')).toBeInTheDocument();
  });
});
