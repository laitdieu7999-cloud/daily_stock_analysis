import type React from 'react';
import type { ReportLanguage, ReportStrategy as ReportStrategyType } from '../../types/analysis';
import { Card } from '../common';
import { DashboardPanelHeader } from '../dashboard';
import { getReportText, normalizeReportLanguage } from '../../utils/reportLanguage';

type StrategySnakeKey = 'ideal_buy' | 'secondary_buy' | 'stop_loss' | 'take_profit';
type StrategyMode = 'bullish' | 'neutral' | 'bearish';
type StrategyValue = string | number | null | undefined;
type StrategyLike = ReportStrategyType & Partial<Record<StrategySnakeKey, StrategyValue>>;

interface ReportStrategyProps {
  strategy?: StrategyLike;
  language?: ReportLanguage;
  operationAdvice?: string;
  trendPrediction?: string;
  quoteContext?: StrategyQuoteContext;
}

export interface StrategyQuoteContext {
  currentPrice?: string | number | null;
  currentPriceSource?: string | null;
  quoteTime?: string | null;
  quoteSource?: string | null;
  analysisTime?: string | null;
}

interface StrategyItemProps {
  label: string;
  value?: string;
  tone: string;
}

const StrategyRow: React.FC<StrategyItemProps> = ({
  label,
  value,
  tone,
}) => (
  <div
    className="home-subpanel home-strategy-card flex min-h-[108px] flex-col gap-3 px-4 py-3"
    style={{ ['--home-strategy-tone' as string]: `var(${tone})` }}
  >
    <div className="flex items-center gap-2">
      <span
        className="h-2.5 w-2.5 shrink-0 rounded-full"
        style={{ background: `var(${tone})` }}
      />
      <span className="home-strategy-label text-xs font-semibold uppercase tracking-[0.14em]">{label}</span>
    </div>
    <span
      className="home-strategy-value min-w-0 flex-1 text-base font-semibold leading-relaxed"
      style={!value ? { color: 'var(--text-muted-text)' } : undefined}
    >
      {value || '—'}
    </span>
  </div>
);

const readStrategyValue = (
  strategy: StrategyLike,
  camelKey: keyof ReportStrategyType,
  snakeKey: StrategySnakeKey,
): string | undefined => {
  const value = strategy[camelKey] ?? strategy[snakeKey];
  if (value === null || value === undefined) {
    return undefined;
  }
  const text = String(value).trim();
  return text.length > 0 ? text : undefined;
};

const formatPrice = (price: number): string => {
  if (Math.abs(price) < 10 && Math.round(price * 100) !== Math.round(price * 1000) / 10) {
    return price.toFixed(3);
  }
  return price.toFixed(2);
};

const extractPrices = (value?: string): number[] => {
  if (!value) {
    return [];
  }
  const prices: number[] = [];
  const cleanText = value.replaceAll(',', '');
  for (const match of cleanText.matchAll(/-?\d+(?:\.\d+)?/g)) {
    const index = match.index ?? 0;
    if (index >= 2 && cleanText.slice(index - 2, index).toUpperCase() === 'MA') {
      continue;
    }
    const price = Math.abs(Number(match[0]));
    if (Number.isFinite(price) && price > 0 && !prices.some((item) => Math.abs(item - price) <= Math.max(price * 0.001, 0.001))) {
      prices.push(price);
    }
  }
  return prices;
};

const stripStrategyPrefix = (value?: string): string | undefined => {
  if (!value) {
    return value;
  }
  return value
    .replace(/^\s*(理想买入点|理想入场位|买入区|首笔建仓区|次优买入点|次优入场位|加仓区|确认加仓区|止损位|止损价|止损线|破位止损线|持仓防守线|目标位|止盈位|目标区|分批止盈区|反弹压力|反抽出局|反抽出局线|重新评估线|确认转强线)\s*[:：]\s*/i, '')
    .trim();
};

const isBearishStrategy = (values: Array<string | undefined>): boolean => {
  const joined = values.filter(Boolean).join(' ');
  return /暂不|不新开仓|重新站回|清仓|卖出|看空|空头|立即止损|反弹压力|反抽出局|不是止盈/.test(joined);
};

const inferStrategyMode = (
  values: Array<string | undefined>,
  operationAdvice?: string,
  trendPrediction?: string,
): StrategyMode => {
  const signalText = [operationAdvice, trendPrediction].filter(Boolean).join(' ').toLowerCase();
  if (/sell|reduce|trim|bear|卖出|强烈卖出|减仓|清仓|看空|强烈看空|空头/.test(signalText)) {
    return 'bearish';
  }
  if (/watch|wait|hold|sideways|neutral|观望|等待|持有|震荡|箱体/.test(signalText)) {
    return 'neutral';
  }
  if (/buy|bull|买入|强烈买入|看多|强烈看多|多头/.test(signalText)) {
    return 'bullish';
  }

  const valueText = values.filter(Boolean).join(' ').toLowerCase();
  if (/暂不接回|暂不买入|不新开仓|重新站回|反抽出局|不是止盈|清仓/.test(valueText)) {
    return 'bearish';
  }
  if (/观望|等待|震荡|箱体/.test(valueText)) {
    return 'neutral';
  }
  return 'bullish';
};

const compactBearishStop = (value: string | undefined, current?: number): string | undefined => {
  if (!value) {
    return value;
  }
  const noisy = /现价|当前价|立即|清仓|截图|若无法执行|强制止损/.test(value);
  if (value.length <= 42 && !noisy) {
    return stripStrategyPrefix(value);
  }
  const prices = extractPrices(value);
  const exitPrice = current ?? prices[0];
  if (!exitPrice) {
    return stripStrategyPrefix(value);
  }
  const hardStop = prices.find((price) => Math.abs(price - exitPrice) > Math.max(exitPrice * 0.003, 0.01));
  const lowerHardStop = prices.find((price) => price < exitPrice && Math.abs(price - exitPrice) > Math.max(exitPrice * 0.003, 0.01));
  return hardStop
    ? lowerHardStop
      ? `${formatPrice(exitPrice)}元附近离场；硬止损${formatPrice(lowerHardStop)}元`
      : `${formatPrice(exitPrice)}元附近离场`
    : `${formatPrice(exitPrice)}元附近离场`;
};

const normalizeStrategyValues = (strategy: StrategyLike): Record<StrategySnakeKey, string | undefined> => {
  const raw = {
    ideal_buy: stripStrategyPrefix(readStrategyValue(strategy, 'idealBuy', 'ideal_buy')),
    secondary_buy: stripStrategyPrefix(readStrategyValue(strategy, 'secondaryBuy', 'secondary_buy')),
    stop_loss: stripStrategyPrefix(readStrategyValue(strategy, 'stopLoss', 'stop_loss')),
    take_profit: stripStrategyPrefix(readStrategyValue(strategy, 'takeProfit', 'take_profit')),
  };

  if (!isBearishStrategy(Object.values(raw))) {
    return raw;
  }

  const primary = extractPrices(raw.ideal_buy)[0] ?? extractPrices(raw.secondary_buy)[0];
  const secondary = extractPrices(raw.secondary_buy)[0];
  const stopPrices = extractPrices(raw.stop_loss);
  const current = stopPrices[0];
  const normalized = { ...raw };

  if (primary && /不新开仓|MA\d+|附近/.test(raw.ideal_buy ?? '')) {
    const distance = current ? `（较现价+${(((primary - current) / current) * 100).toFixed(1)}%）` : '';
    normalized.ideal_buy = `暂不接回；重新站回${formatPrice(primary)}元后再评估${distance}`;
  }
  if (normalized.ideal_buy?.startsWith('暂不买入')) {
    normalized.ideal_buy = normalized.ideal_buy.replace(/^暂不买入/, '暂不接回');
  }
  if (secondary && /保守|等待|回踩|站稳|止跌/.test(raw.secondary_buy ?? '')) {
    normalized.secondary_buy = `确认转强：站稳${formatPrice(secondary)}元且止跌后再看`;
  }
  if (normalized.secondary_buy?.startsWith('保守确认')) {
    normalized.secondary_buy = normalized.secondary_buy.replace(/^保守确认/, '确认转强');
  }
  normalized.stop_loss = compactBearishStop(raw.stop_loss, current);
  if (primary && /目标位|止盈|风险回报|压力|33\.64/.test(raw.take_profit ?? '')) {
    normalized.take_profit = `${formatPrice(primary)}元附近（反抽出局，不是止盈）`;
  }
  return normalized;
};

const isProtectivelyDowngraded = (values: Array<string | undefined>): boolean => {
  const joined = values.filter(Boolean).join(' ');
  return /偏离现价.*过大|行情上下文不一致|待行情刷新后重算|暂不设(买入区|加仓区|止损线|目标区|点位|防守线)|不设反抽出局线|原点位偏离/.test(joined);
};

const formatContextPrice = (value?: string | number | null): string | undefined => {
  if (value === null || value === undefined || value === '') {
    return undefined;
  }
  if (typeof value === 'number') {
    return `${formatPrice(value)}元`;
  }
  const text = String(value).trim();
  if (!text || text === 'N/A') {
    return undefined;
  }
  return /元|HK\$|\$|¥/.test(text) ? text : `${text}元`;
};

const formatContextTime = (value?: string | null): string | undefined => {
  if (!value) {
    return undefined;
  }
  const text = String(value).trim();
  if (!text || text === 'N/A') {
    return undefined;
  }
  return text.replace('T', ' ').slice(0, 16);
};

const QuoteContextBar: React.FC<{
  context?: StrategyQuoteContext;
  reportLanguage: ReportLanguage;
}> = ({ context, reportLanguage }) => {
  if (!context) {
    return null;
  }

  const currentPrice = formatContextPrice(context.currentPrice);
  const quoteTime = formatContextTime(context.quoteTime || context.analysisTime || null);
  const quoteSource = context.quoteSource || context.currentPriceSource;
  const items = [
    currentPrice
      ? {
        label: reportLanguage === 'en' ? 'Price basis' : '当前价依据',
        value: currentPrice,
      }
      : null,
    quoteTime
      ? {
        label: reportLanguage === 'en' ? 'Time' : '时间',
        value: quoteTime,
      }
      : null,
    quoteSource
      ? {
        label: reportLanguage === 'en' ? 'Source' : '数据源',
        value: quoteSource,
      }
      : null,
  ].filter(Boolean) as Array<{ label: string; value: string }>;

  if (items.length === 0) {
    return null;
  }

  return (
    <div className="mb-3 flex flex-wrap gap-2 rounded-2xl border border-border/70 bg-white/65 px-3 py-2 text-xs text-secondary-text">
      {items.map((item) => (
        <span key={item.label} className="inline-flex items-center gap-1.5">
          <span className="font-semibold text-foreground/80">{item.label}</span>
          <span>{item.value}</span>
        </span>
      ))}
    </div>
  );
};

const getStrategyItems = (
  displayStrategy: Record<StrategySnakeKey, string | undefined>,
  mode: StrategyMode,
  reportLanguage: ReportLanguage,
) => {
  const labels = {
    bullish: reportLanguage === 'en'
      ? ['Initial Entry', 'Confirm Add', 'Break Stop', 'Scale-out Target']
      : ['首笔建仓区', '确认加仓区', '破位止损线', '分批止盈区'],
    neutral: reportLanguage === 'en'
      ? ['Current Plan', 'Confirm Follow', 'Break Alert', 'Upper Pressure']
      : ['观察买入区', '确认跟进线', '破位警戒线', '上方压力区'],
    bearish: reportLanguage === 'en'
      ? ['Reduce Now', 'Exit on Bounce', 'Defense Line', 'Recheck Line']
      : ['立即减仓区', '反抽出局线', '持仓防守线', '重新评估线'],
  }[mode];

  if (mode === 'bearish') {
    return [
      { label: labels[0], value: displayStrategy.stop_loss, tone: '--home-strategy-stop' },
      { label: labels[1], value: displayStrategy.take_profit, tone: '--home-strategy-take' },
      { label: labels[2], value: displayStrategy.secondary_buy, tone: '--home-strategy-secondary' },
      { label: labels[3], value: displayStrategy.ideal_buy, tone: '--home-strategy-buy' },
    ];
  }

  return [
    { label: labels[0], value: displayStrategy.ideal_buy, tone: '--home-strategy-buy' },
    { label: labels[1], value: displayStrategy.secondary_buy, tone: '--home-strategy-secondary' },
    { label: labels[2], value: displayStrategy.stop_loss, tone: '--home-strategy-stop' },
    { label: labels[3], value: displayStrategy.take_profit, tone: '--home-strategy-take' },
  ];
};

/**
 * 作战计划点位区组件 - 终端风格
 */
export const ReportStrategy: React.FC<ReportStrategyProps> = ({
  strategy,
  language = 'zh',
  operationAdvice,
  trendPrediction,
  quoteContext,
}) => {
  if (!strategy) {
    return null;
  }

  const reportLanguage = normalizeReportLanguage(language);
  const text = getReportText(reportLanguage);
  const displayStrategy = normalizeStrategyValues(strategy);
  const mode = inferStrategyMode(Object.values(displayStrategy), operationAdvice, trendPrediction);
  const strategyItems = getStrategyItems(displayStrategy, mode, reportLanguage);
  const hasProtectiveDowngrade = isProtectivelyDowngraded(Object.values(displayStrategy));
  const downgradeTitle = reportLanguage === 'en' ? 'Levels protected' : '点位已保护性降级';
  const downgradeText = reportLanguage === 'en'
    ? 'One or more action levels were too far from the latest price and have been marked for recalculation.'
    : '有点位相对现价偏离过大，系统已拦截为“待行情刷新后重算”，不要按原价位执行。';

  return (
    <Card variant="bordered" padding="md" className="home-panel-card">
      <DashboardPanelHeader
        title={text.strategyPoints}
        className="mb-3"
      />
      <QuoteContextBar context={quoteContext} reportLanguage={reportLanguage} />
      {hasProtectiveDowngrade ? (
        <div className="mb-3 rounded-2xl border border-amber-300/70 bg-amber-50 px-4 py-3 text-sm text-amber-900 shadow-sm">
          <div className="font-semibold">{downgradeTitle}</div>
          <div className="mt-1 leading-relaxed">{downgradeText}</div>
        </div>
      ) : null}
      <div className="grid grid-cols-1 gap-3 sm:grid-cols-2 xl:grid-cols-4">
        {strategyItems.map((item) => (
          <StrategyRow key={item.label} {...item} />
        ))}
      </div>
    </Card>
  );
};
