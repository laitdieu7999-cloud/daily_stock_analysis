import apiClient from './index';
import { toCamelCase } from './utils';
import type {
  DataCenterOverview,
  DataCenterCleanupRequest,
  DataCenterCleanupResponse,
  DataCenterMarketDataRefreshRequest,
  DataCenterMarketDataRefreshResponse,
  DataCenterPortfolioBacktestRequest,
  DataCenterPortfolioBacktestResponse,
  DataCenterPortfolioReviewRequest,
  DataCenterPortfolioReviewResponse,
  DataCenterPortfolioRiskRadarResponse,
} from '../types/dataCenter';

export const dataCenterApi = {
  getOverview: async (): Promise<DataCenterOverview> => {
    const response = await apiClient.get<Record<string, unknown>>('/api/v1/data-center/overview');
    return toCamelCase<DataCenterOverview>(response.data);
  },

  runPortfolioBacktest: async (
    params: DataCenterPortfolioBacktestRequest = {},
  ): Promise<DataCenterPortfolioBacktestResponse> => {
    const requestData: Record<string, unknown> = {};
    if (params.force != null) requestData.force = params.force;
    if (params.evalWindowDays != null) requestData.eval_window_days = params.evalWindowDays;
    if (params.minAgeDays != null) requestData.min_age_days = params.minAgeDays;
    if (params.limitPerSymbol != null) requestData.limit_per_symbol = params.limitPerSymbol;

    const response = await apiClient.post<Record<string, unknown>>(
      '/api/v1/data-center/portfolio-backtest',
      requestData,
    );
    return toCamelCase<DataCenterPortfolioBacktestResponse>(response.data);
  },

  refreshMarketData: async (
    params: DataCenterMarketDataRefreshRequest = {},
  ): Promise<DataCenterMarketDataRefreshResponse> => {
    const requestData: Record<string, unknown> = {};
    if (params.force != null) requestData.force = params.force;
    if (params.lookbackDays != null) requestData.lookback_days = params.lookbackDays;
    if (params.refreshOverlapDays != null) requestData.refresh_overlap_days = params.refreshOverlapDays;
    if (params.maxSymbols != null) requestData.max_symbols = params.maxSymbols;

    const response = await apiClient.post<Record<string, unknown>>(
      '/api/v1/data-center/market-data-refresh',
      requestData,
    );
    return toCamelCase<DataCenterMarketDataRefreshResponse>(response.data);
  },

  getPortfolioRiskRadar: async (): Promise<DataCenterPortfolioRiskRadarResponse> => {
    const response = await apiClient.get<Record<string, unknown>>('/api/v1/data-center/portfolio-risk-radar');
    return toCamelCase<DataCenterPortfolioRiskRadarResponse>(response.data);
  },

  runPortfolioDailyReview: async (
    params: DataCenterPortfolioReviewRequest = {},
  ): Promise<DataCenterPortfolioReviewResponse> => {
    const requestData: Record<string, unknown> = {};
    if (params.runBacktests != null) requestData.run_backtests = params.runBacktests;
    if (params.sendNotification != null) requestData.send_notification = params.sendNotification;

    const response = await apiClient.post<Record<string, unknown>>(
      '/api/v1/data-center/portfolio-daily-review',
      requestData,
    );
    return toCamelCase<DataCenterPortfolioReviewResponse>(response.data);
  },

  runMaintenanceCleanup: async (
    params: DataCenterCleanupRequest = {},
  ): Promise<DataCenterCleanupResponse> => {
    const response = await apiClient.post<Record<string, unknown>>(
      '/api/v1/data-center/maintenance-cleanup',
      { dry_run: params.dryRun ?? false },
    );
    return toCamelCase<DataCenterCleanupResponse>(response.data);
  },
};
