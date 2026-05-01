export interface DataCenterSourceItem {
  name: string;
  count: number;
}

export interface DataCenterDatabaseInfo {
  path?: string | null;
  exists: boolean;
  sizeBytes: number;
  sizeLabel: string;
}

export interface DataCenterMarketData {
  stockCount?: number;
  barCount?: number;
  firstDate?: string | null;
  latestDate?: string | null;
  dataSources?: DataCenterSourceItem[];
  warehouse?: {
    status?: string;
    path?: string;
    generatedAt?: string | null;
    endDate?: string | null;
    totals?: Record<string, number>;
  };
  warehouseTargets?: {
    targetCount?: number;
    targets?: Array<{
      code: string;
      sources: string[];
      coverage?: {
        barCount?: number;
        firstDate?: string | null;
        latestDate?: string | null;
      };
    }>;
  };
  quality?: {
    status?: string;
    latestDate?: string | null;
    staleCutoffDate?: string | null;
    summary?: {
      fresh?: number;
      stale?: number;
      missing?: number;
      skipped?: number;
    };
    items?: Array<{
      code: string;
      status: 'fresh' | 'stale' | 'missing' | 'skipped' | string;
      label: string;
      message: string;
      barCount: number;
      latestDate?: string | null;
      daysBehind?: number | null;
      sources: string[];
    }>;
  };
}

export interface DataCenterAnalysis {
  reportCount?: number;
  stockCount?: number;
  latestCreatedAt?: string | null;
}

export interface DataCenterBacktests {
  resultCount?: number;
  summaryCount?: number;
  stockCount?: number;
  latestEvaluatedAt?: string | null;
}

export interface DataCenterPortfolio {
  accountCount?: number;
  activeAccountCount?: number;
  positionCount?: number;
  tradeCount?: number;
  snapshotCount?: number;
  latestUpdatedAt?: string | null;
}

export interface DataCenterNews {
  itemCount?: number;
  stockCount?: number;
  latestFetchedAt?: string | null;
}

export interface DataCenterFundamentals {
  snapshotCount?: number;
  stockCount?: number;
  latestCreatedAt?: string | null;
}

export interface DataCenterFileInfo {
  key: string;
  label: string;
  path: string;
  exists: boolean;
  fileCount: number;
  sizeBytes: number;
  sizeLabel: string;
}

export interface DataCenterRecommendation {
  level: 'info' | 'warning' | 'success' | string;
  title: string;
  description: string;
}

export interface DataCenterPortfolioBacktestRequest {
  force?: boolean;
  evalWindowDays?: number;
  minAgeDays?: number;
  limitPerSymbol?: number;
}

export interface DataCenterPortfolioBacktestItem {
  code: string;
  status: 'ok' | 'error' | string;
  message: string;
  candidateCount: number;
  processed: number;
  saved: number;
  completed: number;
  insufficient: number;
  errors: number;
  summary?: Record<string, unknown> | null;
}

export interface DataCenterPortfolioBacktestResponse {
  generatedAt: string;
  holdingCount: number;
  processedSymbols: number;
  totals: {
    candidateCount?: number;
    processed?: number;
    saved?: number;
    completed?: number;
    insufficient?: number;
    errors?: number;
  };
  items: DataCenterPortfolioBacktestItem[];
}

export interface DataCenterMarketDataRefreshRequest {
  force?: boolean;
  lookbackDays?: number;
  refreshOverlapDays?: number;
  maxSymbols?: number;
}

export interface DataCenterMarketDataRefreshResponse {
  generatedAt: string;
  status: string;
  lookbackDays: number;
  refreshOverlapDays: number;
  maxSymbols: number;
  force: boolean;
  endDate: string;
  totals: {
    targetCount?: number;
    processed?: number;
    succeeded?: number;
    failed?: number;
    rowsFetched?: number;
    rowsInserted?: number;
  };
  items: Array<Record<string, unknown>>;
  ledgerPath?: string | null;
}

export interface DataCenterPortfolioReviewRequest {
  runBacktests?: boolean;
  sendNotification?: boolean | null;
}

export interface DataCenterPortfolioReviewResponse {
  generatedAt: string;
  reportDate: string;
  status: string;
  jsonPath?: string | null;
  markdownPath?: string | null;
  notificationSent: boolean;
  portfolio?: Record<string, unknown>;
  marketData?: Record<string, unknown>;
  backtest?: Record<string, unknown> | null;
  radar?: Record<string, unknown>;
  aiRouting?: Record<string, unknown>;
}

export interface DataCenterCleanupRequest {
  dryRun?: boolean;
}

export interface DataCenterCleanupResponse {
  generatedAt: string;
  status: string;
  dryRun: boolean;
  logRetentionDays: number;
  cacheRetentionDays: number;
  totals: {
    scannedMatches?: number;
    deletedCount?: number;
    freedBytes?: number;
    errorCount?: number;
  };
  items: Array<Record<string, unknown>>;
  ledgerPath?: string | null;
}

export interface DataCenterPortfolioRiskRadarItem {
  code: string;
  quantity: number;
  marketValueBase: number;
  updatedAt?: string | null;
  tone: 'strong' | 'watch' | 'weak' | 'empty' | 'error' | string;
  label: string;
  title: string;
  message: string;
  totalEvaluations: number;
  completedCount: number;
  insufficientCount: number;
  winRatePct?: number | null;
  avgSimulatedReturnPct?: number | null;
  summary?: Record<string, unknown> | null;
}

export interface DataCenterPortfolioRiskRadarResponse {
  generatedAt: string;
  holdingCount: number;
  items: DataCenterPortfolioRiskRadarItem[];
}

export interface DataCenterOverview {
  generatedAt: string;
  database: DataCenterDatabaseInfo;
  marketData: DataCenterMarketData;
  analysis: DataCenterAnalysis;
  backtests: DataCenterBacktests;
  portfolio: DataCenterPortfolio;
  news: DataCenterNews;
  fundamentals: DataCenterFundamentals;
  files: DataCenterFileInfo[];
  maintenance?: {
    portfolioDailyReview?: {
      status?: string;
      path?: string;
      markdownPath?: string;
      generatedAt?: string | null;
      reportDate?: string | null;
      holdingCount?: number;
    };
    cleanup?: {
      status?: string;
      path?: string;
      generatedAt?: string | null;
      totals?: Record<string, number>;
    };
  };
  aiRouting?: {
    mode?: string;
    cloudAnalysis?: {
      enabled?: boolean;
      providerCount?: number;
      providers?: string[];
      primaryModel?: string;
      configuredModels?: string[];
      role?: string;
    };
    localWorkstation?: {
      enabled?: boolean;
      role?: string;
      tasks?: string[];
    };
    localModel?: {
      defaultEnabled?: boolean;
      role?: string;
    };
    recommendation?: string;
  };
  recommendations: DataCenterRecommendation[];
  warnings: string[];
}
