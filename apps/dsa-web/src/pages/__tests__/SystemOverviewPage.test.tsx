import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import SystemOverviewPage from '../SystemOverviewPage';
import { systemOverviewApi } from '../../api/systemOverview';

vi.mock('../../api/systemOverview', () => ({
  systemOverviewApi: {
    getOverview: vi.fn(),
  },
}));

const mockedGetOverview = vi.mocked(systemOverviewApi.getOverview);

const overview = {
  generatedAt: '2026-04-28T23:30:00',
  scheduler: {
    scheduleEnabled: true,
    scheduleTime: '09:40',
    nightlyMarketOutlookEnabled: true,
    nightlyMarketOutlookTime: '22:30',
    stockIntradayReminderEnabled: true,
    lockPath: '/Users/test/.dsa_schedule.lock',
    lockPid: 123,
    lockAlive: true,
    launchAgentAlive: true,
    launchAgentPid: 123,
  },
  services: [
    {
      key: 'desktop_backend',
      name: '桌面端服务',
      status: 'active',
      detail: '127.0.0.1:8000 本地接口',
      pid: 456,
      updatedAt: '2026-04-28T23:30:00',
    },
    {
      key: 'workstation_health',
      name: '工作站健康巡检',
      status: 'active',
      detail: '每 15 分钟写入健康账本',
      pid: null,
      updatedAt: '2026-04-28T23:30:00',
    },
  ],
  dataWarehouse: {
    database: {
      sizeLabel: '8.9 MB',
      tables: {
        analysisHistory: 16,
        backtestResults: 16,
      },
    },
    reports: {
      fileCount: 20,
      modifiedAt: '2026-04-28T23:00:00',
    },
    healthArchive: {
      fileCount: 1,
      modifiedAt: '2026-04-28T23:00:00',
    },
    disk: {
      freeLabel: '7.0 TB',
      freePct: 96,
    },
  },
  alerts: [],
  priorities: [
    { priority: 'P0', label: '黑天鹅 / 系统级风险', notifyRule: '必须提醒', archiveRule: '长期归档', status: 'active' },
    { priority: 'P3', label: 'IC/期权 Shadow', notifyRule: '不提醒', archiveRule: '只写账本', status: 'shadow' },
    { priority: 'P4', label: 'Gemini 外部观点', notifyRule: '不提醒', archiveRule: '只做对比归档', status: 'silent' },
  ],
  modules: [
    {
      key: 'black_swan',
      name: '黑天鹅监控',
      priority: 'P0',
      status: 'active',
      notifyRule: '当天段落明确已触发才强提醒',
      archivePath: '/tmp/black_swan_events.jsonl',
      detail: '已归档 0 条P0事件。',
    },
    {
      key: 'ic_shadow',
      name: 'IC/期权 Shadow',
      priority: 'P3',
      status: 'shadow',
      notifyRule: '只归档，不提醒',
      archivePath: '/tmp/ic_shadow_events.jsonl',
      detail: '统一P3账本 1 条。',
    },
    {
      key: 'sniper_point_guard',
      name: '狙击点位保护',
      priority: 'control',
      status: 'active',
      notifyRule: '点位异常只降级展示，不直接给可执行价格',
      archivePath: '/tmp/sniper_point_downgrade_audit.jsonl',
      detail: '已拦截 0 条异常点位或行情上下文冲突。',
    },
  ],
  files: [
    {
      key: 'signal_contract',
      label: '信号路由契约',
      path: '/tmp/SIGNAL_ROUTING.md',
      exists: true,
      fileCount: 1,
      sizeBytes: 100,
      sizeLabel: '100 B',
      modifiedAt: '2026-04-28T23:00:00',
    },
    {
      key: 'sniper_point_downgrade_summary',
      label: '狙击点位保护摘要',
      path: '/tmp/sniper_point_downgrade_summary.md',
      exists: true,
      fileCount: 1,
      sizeBytes: 200,
      sizeLabel: '200 B',
      modifiedAt: '2026-04-28T23:10:00',
    },
  ],
  recommendations: [
    {
      level: 'success',
      title: '控制面状态清晰',
      description: '可继续做真实运行巡检。',
    },
  ],
};

describe('SystemOverviewPage', () => {
  beforeEach(() => {
    mockedGetOverview.mockReset();
  });

  it('renders routing priorities and module status', async () => {
    mockedGetOverview.mockResolvedValue(overview);

    render(
      <MemoryRouter>
        <SystemOverviewPage />
      </MemoryRouter>,
    );

    expect(await screen.findByText('系统总览')).toBeInTheDocument();
    expect(screen.getByText('系统工具入口')).toBeInTheDocument();
    expect(screen.getByText('数据中心')).toBeInTheDocument();
    expect(screen.getByText('每日复盘')).toBeInTheDocument();
    expect(screen.getByText('系统设置')).toBeInTheDocument();
    expect(screen.getByText('黑天鹅监控')).toBeInTheDocument();
    expect(screen.getAllByText('IC/期权 Shadow').length).toBeGreaterThan(0);
    expect(screen.getByText('Gemini 外部观点')).toBeInTheDocument();
    expect(screen.getByText('狙击点位保护')).toBeInTheDocument();
    expect(screen.getByText('工作站健康')).toBeInTheDocument();
    expect(screen.getByText('桌面端服务')).toBeInTheDocument();
    expect(screen.getByText('长期数据沉淀')).toBeInTheDocument();
    expect(screen.getByText((content) => content.includes('执行时间') && content.includes('22:30'))).toBeInTheDocument();
    expect(screen.getByText('信号路由契约')).toBeInTheDocument();
    expect(screen.getByText('狙击点位保护摘要')).toBeInTheDocument();
  });

  it('refreshes overview on demand', async () => {
    mockedGetOverview.mockResolvedValue(overview);

    render(
      <MemoryRouter>
        <SystemOverviewPage />
      </MemoryRouter>,
    );
    await screen.findByText('系统总览');
    fireEvent.click(screen.getByRole('button', { name: '刷新总览' }));

    await waitFor(() => {
      expect(mockedGetOverview).toHaveBeenCalledTimes(2);
    });
  });
});
