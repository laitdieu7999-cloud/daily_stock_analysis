import type React from 'react';
import { useCallback, useEffect, useState } from 'react';
import { BarChart3, RefreshCw, ShieldCheck } from 'lucide-react';
import { Link } from 'react-router-dom';
import { backtestApi } from '../api/backtest';
import type { ParsedApiError } from '../api/error';
import { getParsedApiError } from '../api/error';
import { ApiErrorAlert, AppPage, Badge, Card, EmptyState, PageHeader } from '../components/common';
import type { IntradayReplayEntry, ShadowDashboardResponse, ShadowLedgerEntry, ShadowScorecardRow } from '../types/backtest';

function pct(value?: number | null, digits = 2): string {
  if (value == null || !Number.isFinite(value)) return '--';
  return `${value.toFixed(digits)}%`;
}

function num(value?: number | null, digits = 2): string {
  if (value == null || !Number.isFinite(value)) return '--';
  return value.toFixed(digits);
}

function gateBadge(value?: string | null) {
  if (!value) return <Badge variant="default">--</Badge>;
  if (value.includes('通过') || value.includes('Shadow')) return <Badge variant="success">{value}</Badge>;
  if (value.includes('未覆盖') || value.includes('提示')) return <Badge variant="warning">{value}</Badge>;
  return <Badge variant="default">{value}</Badge>;
}

function entryStatusBadge(status: string) {
  if (status === 'settled') return <Badge variant="success">已结算</Badge>;
  if (status === 'open') return <Badge variant="info">观察中</Badge>;
  return <Badge variant="default">{status}</Badge>;
}

function signalTypeLabel(value?: string | null): string {
  if (value === 'BUY_SETUP') return '买入提醒';
  if (value === 'RISK_STOP') return '持仓风控';
  if (value?.startsWith('RISK_')) return '风险提醒';
  return value || '--';
}

function scopeLabel(value?: string | null): string {
  if (value === 'watchlist_buy') return '自选买入';
  if (value === 'holding_risk') return '持仓风控';
  return value || '--';
}

function effectiveBadge(value?: boolean | null) {
  if (value === true) return <Badge variant="success">有效</Badge>;
  if (value === false) return <Badge variant="warning">未验证</Badge>;
  return <Badge variant="default">待回填</Badge>;
}

function settlementValue(entry: ShadowLedgerEntry, key: string): string {
  const item = entry.settlements?.[key];
  if (!item) return '--';
  return pct(item.returnPct);
}

const SummaryMetric: React.FC<{ label: string; value: string | number; hint?: string }> = ({ label, value, hint }) => (
  <Card variant="gradient" padding="md">
    <span className="label-uppercase">{label}</span>
    <p className="mt-2 text-3xl font-semibold tracking-tight text-foreground">{value}</p>
    {hint ? <p className="mt-2 text-xs text-secondary-text">{hint}</p> : null}
  </Card>
);

const ResearchShortcut: React.FC<{ title: string; description: string; to: string; icon: React.ReactNode }> = ({
  title,
  description,
  to,
  icon,
}) => (
  <Link
    to={to}
    className="group flex items-start gap-3 rounded-2xl border border-border/70 bg-card/75 p-4 shadow-soft-card transition hover:-translate-y-0.5 hover:border-cyan/40 hover:bg-hover/70"
  >
    <div className="flex h-10 w-10 shrink-0 items-center justify-center rounded-2xl bg-cyan/10 text-cyan-700">
      {icon}
    </div>
    <div>
      <p className="font-semibold text-foreground group-hover:text-cyan-700">{title}</p>
      <p className="mt-1 text-xs leading-5 text-secondary-text">{description}</p>
    </div>
  </Link>
);

const CandidateTable: React.FC<{ rows: ShadowScorecardRow[] }> = ({ rows }) => {
  if (!rows.length) {
    return <EmptyState title="暂无可进 Shadow 的进攻信号" description="收盘后刷新完成后，这里会展示通过治理门槛的纸面信号。" />;
  }
  return (
    <div className="overflow-x-auto">
      <table className="min-w-full text-left text-sm">
        <thead className="border-b border-border/60 text-xs uppercase text-muted-text">
          <tr>
            <th className="px-3 py-3">信号</th>
            <th className="px-3 py-3">样本</th>
            <th className="px-3 py-3">OOS</th>
            <th className="px-3 py-3">随机检验</th>
            <th className="px-3 py-3 text-right">成本后收益</th>
            <th className="px-3 py-3 text-right">盈亏比</th>
            <th className="px-3 py-3 text-right">集中度</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-border/40">
          {rows.map((row) => (
            <tr key={`${row.module}-${row.rule}`} className="hover:bg-hover/50">
              <td className="px-3 py-3">
                <p className="font-medium text-foreground">{row.rule}</p>
                <p className="mt-1 text-xs text-muted-text">{row.module || '--'} · {row.directionType || '--'}</p>
              </td>
              <td className="px-3 py-3 font-mono text-secondary-text">{row.sampleCount ?? '--'}</td>
              <td className="px-3 py-3">
                <div className="flex flex-col gap-1">
                  {gateBadge(row.walkForwardGate)}
                  <span className="text-xs text-muted-text">{pct(row.walkForwardPassRatePct, 1)}</span>
                </div>
              </td>
              <td className="px-3 py-3">
                <div className="flex flex-col gap-1">
                  {gateBadge(row.permutationGate)}
                  <span className="text-xs text-muted-text">p={num(row.pValue, 4)}</span>
                </div>
              </td>
              <td className="px-3 py-3 text-right font-mono text-secondary-text">{pct(row.costNetAvgReturnPct)}</td>
              <td className="px-3 py-3 text-right font-mono text-secondary-text">{num(row.payoffRatio, 2)}</td>
              <td className="px-3 py-3 text-right">
                <p className="font-mono text-secondary-text">{pct(row.symbolAttribution?.top3ContributionPct, 1)}</p>
                <p className="mt-1 text-xs text-muted-text">{row.symbolAttribution?.concentrationStatus || '--'}</p>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
};

const IntradayReplayTable: React.FC<{ rows: IntradayReplayEntry[] }> = ({ rows }) => {
  if (!rows.length) {
    return <EmptyState title="暂无盘中提醒回放记录" description="真实交易日触发持仓风控或自选买入后，这里会展示 T+1/T+3/T+5 结果。" />;
  }
  return (
    <div className="overflow-x-auto">
      <table className="min-w-full text-left text-sm">
        <thead className="border-b border-border/60 text-xs uppercase text-muted-text">
          <tr>
            <th className="px-3 py-3">时间/标的</th>
            <th className="px-3 py-3">类型</th>
            <th className="px-3 py-3 text-right">触发价</th>
            <th className="px-3 py-3 text-right">T+1</th>
            <th className="px-3 py-3 text-right">T+3</th>
            <th className="px-3 py-3 text-right">T+5</th>
            <th className="px-3 py-3 text-right">MFE/MAE</th>
            <th className="px-3 py-3">判定</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-border/40">
          {rows.map((row, index) => (
            <tr key={row.signalId || `${row.triggerTimestamp}-${row.code}-${index}`} className="hover:bg-hover/50">
              <td className="px-3 py-3">
                <p className="font-mono text-foreground">{row.code || '--'}</p>
                <p className="mt-1 text-xs text-muted-text">{row.name || '--'} · {row.triggerTimestamp || '--'}</p>
              </td>
              <td className="px-3 py-3">
                <p className="font-medium text-foreground">{signalTypeLabel(row.signalType)}</p>
                <p className="mt-1 text-xs text-muted-text">{scopeLabel(row.scope)}</p>
              </td>
              <td className="px-3 py-3 text-right font-mono text-secondary-text">{num(row.entryPrice, 4)}</td>
              <td className="px-3 py-3 text-right font-mono text-secondary-text">{pct(row.tPlus1ReturnPct)}</td>
              <td className="px-3 py-3 text-right font-mono text-secondary-text">{pct(row.tPlus3ReturnPct)}</td>
              <td className="px-3 py-3 text-right font-mono text-secondary-text">{pct(row.tPlus5ReturnPct)}</td>
              <td className="px-3 py-3 text-right font-mono text-secondary-text">
                {pct(row.mfePct)} / {pct(row.maePct)}
              </td>
              <td className="px-3 py-3">{effectiveBadge(row.effective)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
};

const LedgerTable: React.FC<{ rows: ShadowLedgerEntry[] }> = ({ rows }) => {
  if (!rows.length) {
    return <EmptyState title="暂无 Shadow 纸面交易" description="当可进 Shadow 的信号在最新数据上触发时，会自动写入这里。" />;
  }
  return (
    <div className="overflow-x-auto">
      <table className="min-w-full text-left text-sm">
        <thead className="border-b border-border/60 text-xs uppercase text-muted-text">
          <tr>
            <th className="px-3 py-3">日期/标的</th>
            <th className="px-3 py-3">信号</th>
            <th className="px-3 py-3 text-right">入场价</th>
            <th className="px-3 py-3">环境</th>
            <th className="px-3 py-3 text-right">T+3</th>
            <th className="px-3 py-3 text-right">T+5</th>
            <th className="px-3 py-3 text-right">T+10</th>
            <th className="px-3 py-3">状态</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-border/40">
          {rows.map((row) => (
            <tr key={row.entryId || `${row.signalDate}-${row.code}-${row.rule}`} className="hover:bg-hover/50">
              <td className="px-3 py-3">
                <p className="font-mono text-foreground">{row.code}</p>
                <p className="mt-1 text-xs text-muted-text">{row.signalDate}</p>
              </td>
              <td className="px-3 py-3">
                <p className="font-medium text-foreground">{row.rule}</p>
                <p className="mt-1 text-xs text-muted-text">{row.description || '--'}</p>
              </td>
              <td className="px-3 py-3 text-right font-mono text-secondary-text">{num(row.entryPrice, 4)}</td>
              <td className="px-3 py-3 text-secondary-text">{row.marketRegime || '--'}</td>
              <td className="px-3 py-3 text-right font-mono text-secondary-text">{settlementValue(row, 'T+3')}</td>
              <td className="px-3 py-3 text-right font-mono text-secondary-text">{settlementValue(row, 'T+5')}</td>
              <td className="px-3 py-3 text-right font-mono text-secondary-text">{settlementValue(row, 'T+10')}</td>
              <td className="px-3 py-3">{entryStatusBadge(row.status)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
};

const ShadowPage: React.FC = () => {
  const [data, setData] = useState<ShadowDashboardResponse | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<ParsedApiError | null>(null);

  const load = useCallback(async () => {
    setIsLoading(true);
    setError(null);
    try {
      const payload = await backtestApi.getShadowDashboard({ limit: 80 });
      setData(payload);
    } catch (err) {
      setError(getParsedApiError(err));
    } finally {
      setIsLoading(false);
    }
  }, []);

  useEffect(() => {
    void load();
  }, [load]);

  return (
    <AppPage>
      <PageHeader
        eyebrow="Paper Trading"
        title="Shadow 纸面信号"
        description="只读取收盘后生成的理论评分表和纸面账本；不下单、不推送，用来观察信号是否值得升级。"
        actions={(
          <button type="button" className="btn-secondary" onClick={() => void load()} disabled={isLoading}>
            <RefreshCw className={`h-4 w-4 ${isLoading ? 'animate-spin' : ''}`} />
            刷新
          </button>
        )}
      />

      <div className="mt-5 space-y-5">
        {error ? <ApiErrorAlert error={error} actionLabel="重试" onAction={() => void load()} /> : null}

        {isLoading && !data ? (
          <div className="flex min-h-[240px] items-center justify-center">
            <div className="h-8 w-8 animate-spin rounded-full border-2 border-cyan/20 border-t-cyan" />
          </div>
        ) : null}

        {data ? (
          <>
            <div className="grid gap-4 md:grid-cols-4">
              <SummaryMetric label="可进 Shadow" value={data.scorecard.candidates.length} hint={`窗口 T+${data.scorecard.primaryWindow ?? '--'}`} />
              <SummaryMetric label="纸面交易" value={data.ledger.totalCount} hint="累计记录数" />
              <SummaryMetric label="观察中" value={data.ledger.openCount} hint="尚未完成 T+10" />
              <SummaryMetric label="已结算" value={data.ledger.settledCount} hint="T+3/T+5/T+10 均已回填" />
            </div>

            <Card title="研究入口" subtitle="Research Shortcuts">
              <div className="grid gap-3 md:grid-cols-2">
                <ResearchShortcut
                  title="历史回测"
                  description="查看历史分析准确率、收益、止损/止盈触发情况。"
                  to="/backtest"
                  icon={<BarChart3 className="h-5 w-5" />}
                />
                <ResearchShortcut
                  title="Shadow 纸面账本"
                  description="当前页继续观察理论信号、盘中提醒准确率和纸面交易。"
                  to="/shadow"
                  icon={<ShieldCheck className="h-5 w-5" />}
                />
              </div>
            </Card>

            <Card title="盘中提醒准确率" subtitle="Intraday Replay">
              <div className="mb-5 grid gap-3 md:grid-cols-5">
                <SummaryMetric label="触发总数" value={data.intradayReplay.totalCount} hint="持仓风控 + 自选买入" />
                <SummaryMetric label="已回填" value={data.intradayReplay.labeledCount} hint={`待回填 ${data.intradayReplay.pendingCount}`} />
                <SummaryMetric label="有效率" value={pct(data.intradayReplay.effectiveRatePct, 1)} hint="买入看涨，风控看跌" />
                <SummaryMetric label="平均结果" value={pct(data.intradayReplay.avgPrimaryReturnPct)} hint="优先 T+5，其次 T+3/T+1" />
                <SummaryMetric label="MFE / MAE" value={`${pct(data.intradayReplay.avgMfePct)} / ${pct(data.intradayReplay.avgMaePct)}`} hint="最大有利 / 不利波动" />
              </div>
              {data.intradayReplay.signalTypeCounts.length ? (
                <div className="mb-5 grid gap-3 md:grid-cols-2">
                  {data.intradayReplay.signalTypeCounts.map((item) => (
                    <div key={item.signalType} className="rounded-2xl border border-border/60 bg-surface-2/60 p-4">
                      <div className="flex items-center justify-between gap-3">
                        <div>
                          <p className="font-semibold text-foreground">{signalTypeLabel(item.signalType)}</p>
                          <p className="mt-1 text-xs text-muted-text">已回填 {item.labeledCount} / 总数 {item.count}</p>
                        </div>
                        <Badge variant="info">{pct(item.effectiveRatePct, 1)}</Badge>
                      </div>
                      <p className="mt-3 text-xs text-secondary-text">平均结果：{pct(item.avgPrimaryReturnPct)}</p>
                    </div>
                  ))}
                </div>
              ) : null}
              <IntradayReplayTable rows={data.intradayReplay.entries} />
            </Card>

            <Card title="通过治理门槛的进攻信号" subtitle="Theory Scorecard">
              <CandidateTable rows={data.scorecard.candidates} />
            </Card>

            <div className="grid gap-5 xl:grid-cols-[1fr_320px]">
              <Card title="Shadow 纸面账本" subtitle="Ledger">
                <LedgerTable rows={data.ledger.entries} />
              </Card>

              <Card title="信号分布" subtitle="Breakdown">
                {data.ledger.ruleCounts.length ? (
                  <div className="space-y-3">
                    {data.ledger.ruleCounts.map((item) => (
                      <div key={item.rule} className="rounded-xl border border-border/50 bg-surface-2/70 px-3 py-3">
                        <div className="flex items-center justify-between gap-3">
                          <span className="text-sm font-medium text-foreground">{item.rule}</span>
                          <Badge variant="info">{item.count}</Badge>
                        </div>
                      </div>
                    ))}
                  </div>
                ) : (
                  <EmptyState
                    title="暂无分布"
                    description="账本产生记录后，这里会显示各信号触发次数。"
                    icon={<ShieldCheck className="h-6 w-6" />}
                  />
                )}
              </Card>
            </div>
          </>
        ) : null}
      </div>
    </AppPage>
  );
};

export default ShadowPage;
