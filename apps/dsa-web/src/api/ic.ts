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
    };
  },
};
