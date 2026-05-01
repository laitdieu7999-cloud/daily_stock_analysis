import { render, screen } from '@testing-library/react';
import { describe, expect, it } from 'vitest';
import { ReportPortfolioPosition } from '../ReportPortfolioPosition';

describe('ReportPortfolioPosition', () => {
  it('renders current holding metrics when position context exists', () => {
    render(
      <ReportPortfolioPosition
        position={{
          hasPosition: true,
          currency: 'CNY',
          quantity: 100,
          avgCost: 1.2,
          lastPrice: 1.5,
          marketValue: 150,
          unrealizedPnl: 30,
          unrealizedPnlPct: 25,
          weightPct: 7.5,
        }}
      />,
    );

    expect(screen.getByText('当前持仓')).toBeInTheDocument();
    expect(screen.getByText('这份分析已结合你的持仓成本与盈亏')).toBeInTheDocument();
    expect(screen.getByText('持仓均价')).toBeInTheDocument();
    expect(screen.getByText('CNY 30.00')).toBeInTheDocument();
    expect(screen.getByText('7.50%')).toBeInTheDocument();
    expect(screen.getByText('25.00%')).toBeInTheDocument();
  });

  it('renders nothing when no holding context exists', () => {
    const { container } = render(<ReportPortfolioPosition position={{ hasPosition: false }} />);
    expect(container).toBeEmptyDOMElement();
  });
});
