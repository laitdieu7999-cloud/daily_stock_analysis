import type React from 'react';
import type { PortfolioPositionContext } from '../../types/analysis';
import { Card } from '../common';
import { DashboardPanelHeader } from '../dashboard';

interface ReportPortfolioPositionProps {
  position?: PortfolioPositionContext | null;
}

const formatNumber = (value?: number | null, digits = 2): string => {
  if (value === null || value === undefined || Number.isNaN(Number(value))) {
    return '—';
  }
  return Number(value).toLocaleString('zh-CN', {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  });
};

const formatMoney = (value?: number | null, currency = 'CNY'): string => {
  if (value === null || value === undefined || Number.isNaN(Number(value))) {
    return '—';
  }
  return `${currency} ${formatNumber(value, 2)}`;
};

const formatPct = (value?: number | null): string => {
  if (value === null || value === undefined || Number.isNaN(Number(value))) {
    return '—';
  }
  return `${formatNumber(value, 2)}%`;
};

const pnlClassName = (value?: number | null): string => {
  if (value === null || value === undefined) {
    return 'text-secondary-text';
  }
  return value >= 0 ? 'text-red-500' : 'text-emerald-600';
};

export const ReportPortfolioPosition: React.FC<ReportPortfolioPositionProps> = ({ position }) => {
  if (!position?.hasPosition) {
    return null;
  }

  const currency = position.currency || 'CNY';
  const accounts = position.accounts || [];

  return (
    <Card variant="bordered" padding="md" className="home-panel-card">
      <DashboardPanelHeader
        eyebrow="当前持仓"
        title="这份分析已结合你的持仓成本与盈亏"
        className="mb-3"
      />
      <div className="grid grid-cols-2 gap-3 md:grid-cols-4">
        <div className="home-subpanel p-3">
          <div className="text-xs text-secondary-text">持仓均价</div>
          <div className="mt-1 font-mono text-lg font-bold text-foreground">{formatNumber(position.avgCost, 4)}</div>
        </div>
        <div className="home-subpanel p-3">
          <div className="text-xs text-secondary-text">最新价格</div>
          <div className="mt-1 font-mono text-lg font-bold text-foreground">{formatNumber(position.lastPrice, 4)}</div>
        </div>
        <div className="home-subpanel p-3">
          <div className="text-xs text-secondary-text">浮动盈亏</div>
          <div className={`mt-1 font-mono text-lg font-bold ${pnlClassName(position.unrealizedPnl)}`}>
            {formatMoney(position.unrealizedPnl, currency)}
          </div>
        </div>
        <div className="home-subpanel p-3">
          <div className="text-xs text-secondary-text">组合仓位</div>
          <div className="mt-1 font-mono text-lg font-bold text-foreground">{formatPct(position.weightPct)}</div>
        </div>
      </div>

      <div className="mt-3 grid gap-2 text-sm md:grid-cols-3">
        <div className="rounded-xl border border-subtle bg-surface/40 px-3 py-2">
          <span className="text-secondary-text">持仓数量：</span>
          <span className="font-mono text-foreground">{formatNumber(position.quantity, 4)}</span>
        </div>
        <div className="rounded-xl border border-subtle bg-surface/40 px-3 py-2">
          <span className="text-secondary-text">当前市值：</span>
          <span className="font-mono text-foreground">{formatMoney(position.marketValue, currency)}</span>
        </div>
        <div className="rounded-xl border border-subtle bg-surface/40 px-3 py-2">
          <span className="text-secondary-text">浮盈亏率：</span>
          <span className={`font-mono ${pnlClassName(position.unrealizedPnlPct)}`}>
            {formatPct(position.unrealizedPnlPct)}
          </span>
        </div>
      </div>

      {accounts.length > 1 ? (
        <div className="mt-3 overflow-x-auto rounded-xl border border-subtle">
          <table className="w-full text-xs">
            <thead className="bg-surface/40 text-secondary-text">
              <tr>
                <th className="px-3 py-2 text-left">账户</th>
                <th className="px-3 py-2 text-right">数量</th>
                <th className="px-3 py-2 text-right">均价</th>
                <th className="px-3 py-2 text-right">浮盈亏</th>
              </tr>
            </thead>
            <tbody>
              {accounts.map((account, index) => (
                <tr key={`${account.accountId || account.accountName || 'account'}-${index}`} className="border-t border-subtle">
                  <td className="px-3 py-2 text-foreground">{account.accountName || account.accountId || '账户'}</td>
                  <td className="px-3 py-2 text-right font-mono">{formatNumber(account.quantity, 4)}</td>
                  <td className="px-3 py-2 text-right font-mono">{formatNumber(account.avgCost, 4)}</td>
                  <td className={`px-3 py-2 text-right font-mono ${pnlClassName(account.unrealizedPnl)}`}>
                    {formatMoney(account.unrealizedPnl, account.valuationCurrency || currency)}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ) : null}
    </Card>
  );
};
