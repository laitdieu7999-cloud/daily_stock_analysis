import React from 'react';
import type { AnalysisResult, AnalysisReport, PortfolioPositionContext } from '../../types/analysis';
import { ReportOverview } from './ReportOverview';
import { ReportStrategy, type StrategyQuoteContext } from './ReportStrategy';
import { ReportPortfolioPosition } from './ReportPortfolioPosition';
import { ReportNews } from './ReportNews';
import { ReportDetails } from './ReportDetails';
import { getReportText, normalizeReportLanguage } from '../../utils/reportLanguage';

interface ReportSummaryProps {
  data: AnalysisResult | AnalysisReport;
  isHistory?: boolean;
}

type PlainRecord = Record<string, unknown>;

const isRecord = (value: unknown): value is PlainRecord => (
  typeof value === 'object' && value !== null && !Array.isArray(value)
);

const readRecord = (source: unknown, ...keys: string[]): PlainRecord | undefined => {
  if (!isRecord(source)) return undefined;
  for (const key of keys) {
    const value = source[key];
    if (isRecord(value)) return value;
  }
  return undefined;
};

const readValue = (source: unknown, ...keys: string[]): unknown => {
  if (!isRecord(source)) return undefined;
  for (const key of keys) {
    const value = source[key];
    if (value !== undefined && value !== null && value !== '') return value;
  }
  return undefined;
};

const readText = (source: unknown, ...keys: string[]): string | undefined => {
  const value = readValue(source, ...keys);
  if (value === undefined || value === null) return undefined;
  const text = String(value).trim();
  return text && text.toLowerCase() !== 'n/a' ? text : undefined;
};

const buildQuoteContext = (
  report: AnalysisReport,
  rootCreatedAt?: string,
): StrategyQuoteContext => {
  const rawResult = report.details?.rawResult;
  const contextSnapshot = report.details?.contextSnapshot;
  const rawMarketSnapshot = readRecord(rawResult, 'marketSnapshot', 'market_snapshot');
  const detailMarketSnapshot = report.details?.marketSnapshot;
  const marketSnapshot = detailMarketSnapshot || rawMarketSnapshot;
  const enhancedContext = readRecord(contextSnapshot, 'enhancedContext', 'enhanced_context');
  const realtime = readRecord(enhancedContext, 'realtime');
  const realtimeQuoteRaw = readRecord(contextSnapshot, 'realtimeQuoteRaw', 'realtime_quote_raw');

  const currentPrice = (
    report.meta.currentPrice
    ?? readValue(marketSnapshot, 'price', 'currentPrice', 'current_price')
    ?? readValue(realtime, 'price')
    ?? readValue(realtimeQuoteRaw, 'price')
  ) as StrategyQuoteContext['currentPrice'];

  const quoteTime = (
    readText(marketSnapshot, 'time', 'datetime', 'timestamp', 'date')
    || readText(enhancedContext, 'date')
    || report.meta.createdAt
    || rootCreatedAt
  );

  const quoteSource = (
    readText(marketSnapshot, 'source')
    || readText(realtime, 'source')
    || readText(realtimeQuoteRaw, 'source')
    || report.details?.dataSources
    || undefined
  );

  return {
    currentPrice,
    currentPriceSource: currentPrice !== undefined && currentPrice !== null ? '报告快照' : undefined,
    quoteTime,
    quoteSource,
    analysisTime: report.meta.createdAt || rootCreatedAt,
  };
};

/**
 * 完整报告展示组件
 * 整合概览、策略、资讯、详情四个区域
 */
export const ReportSummary: React.FC<ReportSummaryProps> = ({
  data,
  isHistory = false,
}) => {
  // 兼容 AnalysisResult 和 AnalysisReport 两种数据格式
  const report: AnalysisReport = 'report' in data ? data.report : data;
  const rootCreatedAt = 'createdAt' in data ? data.createdAt : undefined;
  // 使用 report id，因为 queryId 在批量分析时可能重复，且历史报告详情接口需要 recordId 来获取关联资讯和详情数据
  const recordId = report.meta.id;

  const { meta, summary, strategy, details } = report;
  const portfolioPosition = (
    details?.portfolioPosition
    || (details?.contextSnapshot?.enhancedContext as { portfolioPosition?: PortfolioPositionContext } | undefined)?.portfolioPosition
  );
  const reportLanguage = normalizeReportLanguage(meta.reportLanguage);
  const text = getReportText(reportLanguage);
  const modelUsed = (meta.modelUsed || '').trim();
  const shouldShowModel = Boolean(
    modelUsed && !['unknown', 'error', 'none', 'null', 'n/a'].includes(modelUsed.toLowerCase()),
  );

  return (
    <div className="space-y-5 pb-8 animate-fade-in">
      {/* 作战计划区：放在报告顶部，避免用户需要向下滚动才看到执行点位。 */}
      <ReportStrategy
        strategy={strategy}
        language={reportLanguage}
        operationAdvice={summary.operationAdvice}
        trendPrediction={summary.trendPrediction}
        quoteContext={buildQuoteContext(report, rootCreatedAt)}
      />

      {/* 概览区 */}
      <ReportOverview
        meta={meta}
        summary={summary}
        details={details}
        isHistory={isHistory}
      />

      {/* 当前持仓：贴近资讯区，便于先看结论再看持仓影响。 */}
      <ReportPortfolioPosition position={portfolioPosition} />

      {/* 资讯区 */}
      <ReportNews recordId={recordId} limit={8} language={reportLanguage} />

      {/* 透明度与追溯区 */}
      <ReportDetails details={details} recordId={recordId} language={reportLanguage} />

      {/* 分析模型标记（Issue #528）— 报告末尾 */}
      {shouldShowModel && (
        <p className="px-1 text-xs text-muted-text">
          {text.analysisModel}: {modelUsed}
        </p>
      )}
    </div>
  );
};
