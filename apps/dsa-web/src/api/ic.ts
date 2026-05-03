import apiClient from './index';

export type ICContractSnapshot = {
  symbol: string;
  price: number;
  expiryDate: string;
  daysToExpiry: number;
  termGapDays: number;
  basis: number;
  annualizedBasisPct: number;
  isMain: boolean;
};

export type ICMarketSnapshot = {
  spotPrice: number;
  mainContractCode: string;
  fetchedAt: string;
  contracts: ICContractSnapshot[];
  optionProxy?: ICOptionProxySnapshot | null;
};

export type ICOptionProxySnapshot = {
  boardTimestamp: string;
  expiryYm: string;
  expiryStyle: string;
  qvixLatest: number;
  qvixPrev: number;
  qvixJumpPct: number;
  qvixZscore: number;
  atmStrike: number;
  atmPutPrice: number;
  otmPutStrike: number;
  otmPutPrice: number;
  putSkewRatio: number;
  atmPutCallVolumeRatio?: number | null;
  expiryDaysToExpiry?: number | null;
  rollWindowShifted?: boolean;
};

const normalizeContract = (payload: Record<string, unknown>): ICContractSnapshot => ({
  symbol: String(payload.symbol || ''),
  price: Number(payload.price || 0),
  expiryDate: String(payload.expiry_date || payload.expiryDate || ''),
  daysToExpiry: Number(payload.days_to_expiry || payload.daysToExpiry || 0),
  termGapDays: Number(payload.term_gap_days || payload.termGapDays || 0),
  basis: Number(payload.basis || 0),
  annualizedBasisPct: Number(payload.annualized_basis_pct || payload.annualizedBasisPct || 0),
  isMain: Boolean(payload.is_main ?? payload.isMain),
});

const readString = (payload: Record<string, unknown>, snakeKey: string, camelKey: string): string => (
  String(payload[snakeKey] ?? payload[camelKey] ?? '')
);

const readNumber = (payload: Record<string, unknown>, snakeKey: string, camelKey: string): number => (
  Number(payload[snakeKey] ?? payload[camelKey] ?? 0)
);

const normalizeOptionProxy = (payload: Record<string, unknown> | null | undefined): ICOptionProxySnapshot | null => {
  if (!payload) return null;
  return {
    boardTimestamp: readString(payload, 'board_timestamp', 'boardTimestamp'),
    expiryYm: readString(payload, 'expiry_ym', 'expiryYm'),
    expiryStyle: readString(payload, 'expiry_style', 'expiryStyle'),
    qvixLatest: readNumber(payload, 'qvix_latest', 'qvixLatest'),
    qvixPrev: readNumber(payload, 'qvix_prev', 'qvixPrev'),
    qvixJumpPct: readNumber(payload, 'qvix_jump_pct', 'qvixJumpPct'),
    qvixZscore: readNumber(payload, 'qvix_zscore', 'qvixZscore'),
    atmStrike: readNumber(payload, 'atm_strike', 'atmStrike'),
    atmPutPrice: readNumber(payload, 'atm_put_price', 'atmPutPrice'),
    otmPutStrike: readNumber(payload, 'otm_put_strike', 'otmPutStrike'),
    otmPutPrice: readNumber(payload, 'otm_put_price', 'otmPutPrice'),
    putSkewRatio: readNumber(payload, 'put_skew_ratio', 'putSkewRatio'),
    atmPutCallVolumeRatio: payload.atm_put_call_volume_ratio == null && payload.atmPutCallVolumeRatio == null
      ? null
      : Number(payload.atm_put_call_volume_ratio ?? payload.atmPutCallVolumeRatio),
    expiryDaysToExpiry: payload.expiry_days_to_expiry == null && payload.expiryDaysToExpiry == null
      ? null
      : Number(payload.expiry_days_to_expiry ?? payload.expiryDaysToExpiry),
    rollWindowShifted: Boolean(payload.roll_window_shifted ?? payload.rollWindowShifted),
  };
};

export const icApi = {
  async getSnapshot(): Promise<ICMarketSnapshot> {
    const response = await apiClient.get<Record<string, unknown>>('/api/v1/ic/snapshot');
    const payload = response.data || {};
    return {
      spotPrice: Number(payload.spot_price || payload.spotPrice || 0),
      mainContractCode: String(payload.main_contract_code || payload.mainContractCode || ''),
      fetchedAt: String(payload.fetched_at || payload.fetchedAt || ''),
      contracts: Array.isArray(payload.contracts)
        ? payload.contracts.map((item) => normalizeContract(item as Record<string, unknown>))
        : [],
      optionProxy: normalizeOptionProxy((payload.option_proxy || payload.optionProxy) as Record<string, unknown> | null | undefined),
    };
  },
};
