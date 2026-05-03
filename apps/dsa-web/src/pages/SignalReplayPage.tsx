import type React from 'react';
import { useCallback, useEffect, useMemo, useState } from 'react';
import { RefreshCw } from 'lucide-react';
import { useSearchParams } from 'react-router-dom';
import { stocksApi, type StockKLineItem } from '../api/stocks';
import type { ParsedApiError } from '../api/error';
import { getParsedApiError } from '../api/error';
import { ApiErrorAlert, AppPage, Badge, Card, EmptyState, PageHeader } from '../components/common';

type ChartPoint = StockKLineItem & {
  ma5?: number | null;
  ma20?: number | null;
};

const CHART_WIDTH = 1040;
const CHART_HEIGHT = 360;
const PAD_X = 48;
const PAD_Y = 24;

function num(value?: number | null, digits = 2): string {
  if (value == null || !Number.isFinite(value)) return '--';
  return value.toFixed(digits);
}

function pct(value?: number | null, digits = 2): string {
  if (value == null || !Number.isFinite(value)) return '--';
  return `${value.toFixed(digits)}%`;
}

function maybeNumber(value: string | null): number | null {
  if (value == null || value.trim() === '') return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function movingAverage(rows: StockKLineItem[], period: number, index: number): number | null {
  if (index + 1 < period) return null;
  const window = rows.slice(index + 1 - period, index + 1);
  const total = window.reduce((sum, row) => sum + row.close, 0);
  return total / period;
}

function withMovingAverages(rows: StockKLineItem[]): ChartPoint[] {
  return rows.map((row, index) => ({
    ...row,
    ma5: movingAverage(rows, 5, index),
    ma20: movingAverage(rows, 20, index),
  }));
}

function findSignalIndex(rows: ChartPoint[], signalDate: string): number {
  if (!rows.length) return -1;
  if (!signalDate) return rows.length - 1;
  const exact = rows.findIndex((row) => row.date === signalDate);
  if (exact >= 0) return exact;
  const firstAfter = rows.findIndex((row) => row.date > signalDate);
  return firstAfter >= 0 ? firstAfter : rows.length - 1;
}

function buildPolyline(rows: ChartPoint[], valueKey: 'ma5' | 'ma20', xFor: (index: number) => number, yFor: (value: number) => number): string {
  return rows
    .map((row, index) => {
      const value = row[valueKey];
      return value == null ? '' : `${xFor(index)},${yFor(value)}`;
    })
    .filter(Boolean)
    .join(' ');
}

function windowStats(rows: ChartPoint[], signalIndex: number, entryPrice?: number | null) {
  if (signalIndex < 0 || entryPrice == null || !Number.isFinite(entryPrice) || entryPrice <= 0) {
    return { mfePct: null, maePct: null, t5Pct: null, t10Pct: null };
  }
  const futureRows = rows.slice(signalIndex, Math.min(rows.length, signalIndex + 11));
  const maxHigh = Math.max(...futureRows.map((row) => row.high));
  const minLow = Math.min(...futureRows.map((row) => row.low));
  const t5 = rows[Math.min(rows.length - 1, signalIndex + 5)]?.close;
  const t10 = rows[Math.min(rows.length - 1, signalIndex + 10)]?.close;
  return {
    mfePct: ((maxHigh - entryPrice) / entryPrice) * 100,
    maePct: ((minLow - entryPrice) / entryPrice) * 100,
    t5Pct: t5 ? ((t5 - entryPrice) / entryPrice) * 100 : null,
    t10Pct: t10 ? ((t10 - entryPrice) / entryPrice) * 100 : null,
  };
}

const SignalKLineChart: React.FC<{
  rows: ChartPoint[];
  signalIndex: number;
  entryPrice?: number | null;
  stopPrice?: number | null;
}> = ({ rows, signalIndex, entryPrice, stopPrice }) => {
  if (!rows.length) {
    return <EmptyState title="暂无 K 线数据" description="本地行情缓存不足时，这里会等待数据刷新。" />;
  }

  const prices = rows.flatMap((row) => [row.high, row.low, row.ma5 ?? row.close, row.ma20 ?? row.close]);
  if (stopPrice && Number.isFinite(stopPrice)) prices.push(stopPrice);
  const minPrice = Math.min(...prices);
  const maxPrice = Math.max(...prices);
  const padding = Math.max((maxPrice - minPrice) * 0.08, 0.01);
  const scaleMin = minPrice - padding;
  const scaleMax = maxPrice + padding;
  const plotWidth = CHART_WIDTH - PAD_X * 2;
  const plotHeight = CHART_HEIGHT - PAD_Y * 2;
  const step = plotWidth / Math.max(rows.length - 1, 1);
  const candleWidth = Math.max(3, Math.min(9, step * 0.58));
  const xFor = (index: number) => PAD_X + index * step;
  const yFor = (value: number) => PAD_Y + ((scaleMax - value) / (scaleMax - scaleMin)) * plotHeight;
  const gridValues = [0, 0.25, 0.5, 0.75, 1].map((ratio) => scaleMax - (scaleMax - scaleMin) * ratio);
  const signalX = signalIndex >= 0 ? xFor(signalIndex) : null;
  const stopY = stopPrice && Number.isFinite(stopPrice) ? yFor(stopPrice) : null;

  return (
    <div className="overflow-x-auto">
      <svg role="img" aria-label="信号K线复盘图" viewBox={`0 0 ${CHART_WIDTH} ${CHART_HEIGHT}`} className="min-w-[920px] rounded-2xl border border-border/60 bg-surface-2/70">
        {gridValues.map((value) => {
          const y = yFor(value);
          return (
            <g key={value}>
              <line x1={PAD_X} x2={CHART_WIDTH - PAD_X} y1={y} y2={y} stroke="currentColor" className="text-border/60" strokeDasharray="4 6" />
              <text x={10} y={y + 4} className="fill-secondary-text text-[11px]">{num(value)}</text>
            </g>
          );
        })}
        {signalX != null ? (
          <>
            <rect x={signalX - step / 2} y={PAD_Y} width={Math.max(step, 6)} height={plotHeight} className="fill-cyan/10" />
            <line x1={signalX} x2={signalX} y1={PAD_Y} y2={CHART_HEIGHT - PAD_Y} className="stroke-cyan" strokeWidth={1.5} strokeDasharray="5 5" />
          </>
        ) : null}
        {stopY != null ? (
          <>
            <line x1={PAD_X} x2={CHART_WIDTH - PAD_X} y1={stopY} y2={stopY} className="stroke-rose-400" strokeWidth={1.5} strokeDasharray="6 6" />
            <text x={CHART_WIDTH - PAD_X - 78} y={stopY - 7} className="fill-rose-400 text-[12px]">止损 {num(stopPrice)}</text>
          </>
        ) : null}
        {rows.map((row, index) => {
          const x = xFor(index);
          const openY = yFor(row.open);
          const closeY = yFor(row.close);
          const highY = yFor(row.high);
          const lowY = yFor(row.low);
          const isUp = row.close >= row.open;
          const bodyY = Math.min(openY, closeY);
          const bodyHeight = Math.max(Math.abs(closeY - openY), 2);
          const colorClass = isUp ? 'stroke-rose-500 fill-rose-500' : 'stroke-emerald-500 fill-emerald-500';
          return (
            <g key={`${row.date}-${index}`} className={colorClass}>
              <line x1={x} x2={x} y1={highY} y2={lowY} strokeWidth={1.2} />
              <rect x={x - candleWidth / 2} y={bodyY} width={candleWidth} height={bodyHeight} rx={1.5} />
            </g>
          );
        })}
        <polyline points={buildPolyline(rows, 'ma5', xFor, yFor)} fill="none" className="stroke-amber-300" strokeWidth={1.8} />
        <polyline points={buildPolyline(rows, 'ma20', xFor, yFor)} fill="none" className="stroke-sky-300" strokeWidth={1.8} />
        {entryPrice && signalX != null ? (
          <circle cx={signalX} cy={yFor(entryPrice)} r={5} className="fill-cyan stroke-background" strokeWidth={2} />
        ) : null}
        <text x={PAD_X} y={CHART_HEIGHT - 6} className="fill-secondary-text text-[11px]">{rows[0]?.date}</text>
        <text x={CHART_WIDTH - PAD_X - 72} y={CHART_HEIGHT - 6} className="fill-secondary-text text-[11px]">{rows[rows.length - 1]?.date}</text>
      </svg>
    </div>
  );
};

const SignalReplayPage: React.FC = () => {
  const [searchParams] = useSearchParams();
  const code = searchParams.get('code') || '600519';
  const name = searchParams.get('name') || '';
  const signalDate = searchParams.get('signalDate') || '';
  const signalType = searchParams.get('signalType') || '信号复盘';
  const entryPrice = maybeNumber(searchParams.get('entryPrice'));
  const queryMfePct = maybeNumber(searchParams.get('mfePct'));
  const queryMaePct = maybeNumber(searchParams.get('maePct'));
  const stopPriceFromQuery = maybeNumber(searchParams.get('stopPrice'));
  const [rows, setRows] = useState<ChartPoint[]>([]);
  const [stockName, setStockName] = useState(name);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<ParsedApiError | null>(null);

  const load = useCallback(async () => {
    setIsLoading(true);
    setError(null);
    try {
      const payload = await stocksApi.getHistory(code, { days: 160 });
      setRows(withMovingAverages(payload.data || []));
      setStockName(payload.stockName || name);
    } catch (err) {
      setError(getParsedApiError(err));
    } finally {
      setIsLoading(false);
    }
  }, [code, name]);

  useEffect(() => {
    document.title = '信号复盘 - DSA';
  }, []);

  useEffect(() => {
    void load();
  }, [load]);

  const signalIndex = useMemo(() => findSignalIndex(rows, signalDate), [rows, signalDate]);
  const fallbackEntry = entryPrice != null && entryPrice > 0 ? entryPrice : rows[signalIndex]?.close ?? null;
  const stopPrice = stopPriceFromQuery != null && stopPriceFromQuery > 0
    ? stopPriceFromQuery
    : fallbackEntry
      ? fallbackEntry * 0.965
      : null;
  const stats = useMemo(() => windowStats(rows, signalIndex, fallbackEntry), [fallbackEntry, rows, signalIndex]);
  const displayMfe = queryMfePct != null ? queryMfePct : stats.mfePct;
  const displayMae = queryMaePct != null ? queryMaePct : stats.maePct;
  const signalRow = signalIndex >= 0 ? rows[signalIndex] : null;

  return (
    <AppPage>
      <PageHeader
        eyebrow="Replay"
        title="信号 K 线复盘"
        description="把盘中提醒和回测结果叠到日 K 线、MA5、MA20 上，先解决“信号发生在什么价格背景里”。"
        actions={(
          <button type="button" className="btn-secondary" onClick={() => void load()} disabled={isLoading}>
            <RefreshCw className={`h-4 w-4 ${isLoading ? 'animate-spin' : ''}`} />
            刷新
          </button>
        )}
      />

      <div className="mt-5 space-y-5">
        {error ? <ApiErrorAlert error={error} actionLabel="重试" onAction={() => void load()} /> : null}
        <div className="grid gap-4 lg:grid-cols-[1fr_320px]">
          <Card title={`${stockName || code} ${code}`} subtitle={signalType}>
            {isLoading && !rows.length ? (
              <div className="flex min-h-[260px] items-center justify-center">
                <div className="h-8 w-8 animate-spin rounded-full border-2 border-cyan/20 border-t-cyan" />
              </div>
            ) : (
              <SignalKLineChart rows={rows} signalIndex={signalIndex} entryPrice={fallbackEntry} stopPrice={stopPrice} />
            )}
            <div className="mt-4 flex flex-wrap gap-3 text-xs text-secondary-text">
              <span className="inline-flex items-center gap-2"><span className="h-2 w-6 rounded-full bg-amber-300" />MA5</span>
              <span className="inline-flex items-center gap-2"><span className="h-2 w-6 rounded-full bg-sky-300" />MA20</span>
              <span>涨红跌绿，符合中国市场习惯。</span>
            </div>
          </Card>

          <Card title="复盘摘要" subtitle="Signal Context">
            <div className="space-y-3 text-sm">
              <div className="flex items-center justify-between gap-3">
                <span className="text-secondary-text">触发日</span>
                <span className="font-mono text-foreground">{signalRow?.date || signalDate || '--'}</span>
              </div>
              <div className="flex items-center justify-between gap-3">
                <span className="text-secondary-text">入场/触发价</span>
                <span className="font-mono text-foreground">{num(fallbackEntry, 4)}</span>
              </div>
              <div className="flex items-center justify-between gap-3">
                <span className="text-secondary-text">止损线</span>
                <span className="font-mono text-rose-400">{num(stopPrice, 4)}</span>
              </div>
              <div className="flex items-center justify-between gap-3">
                <span className="text-secondary-text">T+5</span>
                <span className="font-mono text-foreground">{pct(stats.t5Pct)}</span>
              </div>
              <div className="flex items-center justify-between gap-3">
                <span className="text-secondary-text">T+10</span>
                <span className="font-mono text-foreground">{pct(stats.t10Pct)}</span>
              </div>
              <div className="grid grid-cols-2 gap-2 pt-2">
                <div className="rounded-2xl border border-border/60 bg-surface-2/60 p-3">
                  <span className="label-uppercase">MFE</span>
                  <p className="mt-2 text-lg font-semibold text-rose-400">{pct(displayMfe)}</p>
                </div>
                <div className="rounded-2xl border border-border/60 bg-surface-2/60 p-3">
                  <span className="label-uppercase">MAE</span>
                  <p className="mt-2 text-lg font-semibold text-emerald-400">{pct(displayMae)}</p>
                </div>
              </div>
              <Badge variant="info">日线 MVP：不做分时、不做画线工具</Badge>
            </div>
          </Card>
        </div>
      </div>
    </AppPage>
  );
};

export default SignalReplayPage;
