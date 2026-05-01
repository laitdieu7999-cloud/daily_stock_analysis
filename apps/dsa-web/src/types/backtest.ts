/**
 * Backtest API type definitions
 * Mirrors api/v1/schemas/backtest.py
 */

// ============ Request / Response ============

export interface BacktestRunRequest {
  code?: string;
  force?: boolean;
  evalWindowDays?: number;
  minAgeDays?: number;
  limit?: number;
  scoreThreshold?: number;
  topN?: number;
}

export interface BacktestRunResponse {
  candidateCount: number;
  processed: number;
  saved: number;
  completed: number;
  insufficient: number;
  errors: number;
}

export interface BacktestScanRequest {
  code?: string;
  minAgeDays?: number;
  limit?: number;
  localDataOnly?: boolean;
  evalWindowDaysOptions?: number[];
  scoreThresholdOptions?: Array<number | null>;
  topNOptions?: Array<number | null>;
}

export interface BacktestScanItem {
  evalWindowDays: number;
  scoreThreshold?: number | null;
  topN?: number | null;
  candidateCount: number;
  completedCount: number;
  insufficientCount: number;
  winCount: number;
  lossCount: number;
  neutralCount: number;
  winRatePct?: number | null;
  directionAccuracyPct?: number | null;
  avgStockReturnPct?: number | null;
  avgSimulatedReturnPct?: number | null;
  stopLossTriggerRate?: number | null;
  takeProfitTriggerRate?: number | null;
  adviceBreakdown: Record<string, unknown>;
  diagnostics: Record<string, unknown>;
}

export interface BacktestScanConclusion {
  status: string;
  summaryText: string;
  recommendedScan?: BacktestScanItem | null;
  secondaryScan?: BacktestScanItem | null;
}

export interface BacktestScanResponse {
  rawCandidateCount: number;
  rankedCandidateCount: number;
  localDataOnly: boolean;
  bestByReturn?: BacktestScanItem | null;
  bestByWinRate?: BacktestScanItem | null;
  conclusion?: BacktestScanConclusion | null;
  scans: BacktestScanItem[];
}

// ============ Result Item ============

export interface BacktestResultItem {
  analysisHistoryId: number;
  code: string;
  stockName?: string;
  analysisDate?: string;
  evalWindowDays: number;
  engineVersion: string;
  evalStatus: string;
  evaluatedAt?: string;
  operationAdvice?: string;
  rankingScore?: number;
  scoreSource?: string;
  trendPrediction?: string;
  positionRecommendation?: string;
  startPrice?: number;
  endClose?: number;
  maxHigh?: number;
  minLow?: number;
  stockReturnPct?: number;
  actualReturnPct?: number;
  actualMovement?: string;
  directionExpected?: string;
  directionCorrect?: boolean;
  outcome?: string;
  stopLoss?: number;
  takeProfit?: number;
  hitStopLoss?: boolean;
  hitTakeProfit?: boolean;
  firstHit?: string;
  firstHitDate?: string;
  firstHitTradingDays?: number;
  simulatedEntryPrice?: number;
  simulatedExitPrice?: number;
  simulatedExitReason?: string;
  simulatedReturnPct?: number;
}

export interface BacktestResultsResponse {
  total: number;
  page: number;
  limit: number;
  items: BacktestResultItem[];
}

// ============ Performance Metrics ============

export interface PerformanceMetrics {
  scope: string;
  code?: string;
  evalWindowDays: number;
  engineVersion: string;
  computedAt?: string;

  totalEvaluations: number;
  completedCount: number;
  insufficientCount: number;
  longCount: number;
  cashCount: number;
  winCount: number;
  lossCount: number;
  neutralCount: number;

  directionAccuracyPct?: number;
  winRatePct?: number;
  neutralRatePct?: number;
  avgStockReturnPct?: number;
  avgSimulatedReturnPct?: number;

  stopLossTriggerRate?: number;
  takeProfitTriggerRate?: number;
  ambiguousRate?: number;
  avgDaysToFirstHit?: number;

  adviceBreakdown: Record<string, unknown>;
  diagnostics: Record<string, unknown>;
}

// ============ Shadow Dashboard ============

export interface ShadowRuleCount {
  rule: string;
  count: number;
}

export interface ShadowSymbolAttribution {
  codeCount?: number;
  positiveCodeCount?: number;
  minSymbolSamples?: number;
  top1ContributionPct?: number;
  top3ContributionPct?: number;
  concentrationStatus?: string;
}

export interface ShadowScorecardRow {
  module?: string;
  directionType?: string;
  rule: string;
  sampleCount?: number;
  rawGrade?: string;
  walkForwardGate?: string;
  walkForwardPassRatePct?: number;
  plateauGate?: string;
  permutationGate?: string;
  permutationStatus?: string;
  pValue?: number;
  costGate?: string;
  costNetAvgReturnPct?: number;
  metricValuePct?: number;
  payoffRatio?: number;
  finalDecision?: string;
  symbolAttribution?: ShadowSymbolAttribution;
}

export interface ShadowSettlement {
  window: number;
  settleDate: string;
  exitPrice: number;
  returnPct: number;
  mfePct?: number | null;
  maePct?: number | null;
}

export interface ShadowLedgerEntry {
  entryId?: string;
  status: string;
  signalDate: string;
  code: string;
  module?: string;
  directionType?: string;
  rule: string;
  description?: string;
  entryPrice: number;
  marketRegime?: string;
  windows?: number[];
  settlements?: Record<string, ShadowSettlement>;
  createdAt?: string;
  updatedAt?: string;
}

export interface ShadowScorecardSection {
  status: string;
  jsonPath?: string | null;
  reportPath?: string | null;
  generatedAt?: string | null;
  primaryWindow?: number | null;
  minSamples?: number | null;
  dailyMeta: Record<string, unknown>;
  candidates: ShadowScorecardRow[];
  allRows: ShadowScorecardRow[];
}

export interface ShadowLedgerSection {
  status: string;
  ledgerPath?: string | null;
  summaryPath?: string | null;
  totalCount: number;
  openCount: number;
  settledCount: number;
  ruleCounts: ShadowRuleCount[];
  entries: ShadowLedgerEntry[];
}

export interface IntradayReplaySignalTypeCount {
  signalType: string;
  count: number;
  labeledCount: number;
  effectiveCount: number;
  effectiveRatePct?: number | null;
  avgPrimaryReturnPct?: number | null;
}

export interface IntradayReplayEntry {
  signalId?: string | null;
  triggerTimestamp?: string | null;
  code?: string | null;
  name?: string | null;
  scope?: string | null;
  signalType: string;
  entryPrice?: number | null;
  primaryHorizon?: string | null;
  primaryReturnPct?: number | null;
  effective?: boolean | null;
  tPlus1ReturnPct?: number | null;
  tPlus3ReturnPct?: number | null;
  tPlus5ReturnPct?: number | null;
  mfePct?: number | null;
  maePct?: number | null;
}

export interface IntradayReplaySection {
  status: string;
  ledgerPath?: string | null;
  totalCount: number;
  labeledCount: number;
  pendingCount: number;
  effectiveCount: number;
  effectiveRatePct?: number | null;
  avgPrimaryReturnPct?: number | null;
  avgMfePct?: number | null;
  avgMaePct?: number | null;
  signalTypeCounts: IntradayReplaySignalTypeCount[];
  entries: IntradayReplayEntry[];
}

export interface ShadowDashboardResponse {
  status: string;
  generatedAt: string;
  backtestDir: string;
  scorecard: ShadowScorecardSection;
  ledger: ShadowLedgerSection;
  intradayReplay: IntradayReplaySection;
}
