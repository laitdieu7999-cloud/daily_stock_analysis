import apiClient from './index';
import { toCamelCase } from './utils';
import type { SystemOverview } from '../types/systemOverview';

export const systemOverviewApi = {
  getOverview: async (): Promise<SystemOverview> => {
    const response = await apiClient.get<Record<string, unknown>>('/api/v1/system/overview');
    return toCamelCase<SystemOverview>(response.data);
  },
};
