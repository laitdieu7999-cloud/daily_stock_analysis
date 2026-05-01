import { fireEvent, render, screen } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';
import { HistoryList } from '../HistoryList';
import type { HistoryItem } from '../../../types/analysis';

const baseProps = {
  isLoading: false,
  isLoadingMore: false,
  hasMore: false,
  selectedIds: new Set<number>(),
  onItemClick: vi.fn(),
  onLoadMore: vi.fn(),
  onToggleItemSelection: vi.fn(),
  onToggleSelectAll: vi.fn(),
  onDeleteSelected: vi.fn(),
};

const items: HistoryItem[] = [
  {
    id: 1,
    queryId: 'q-1',
    stockCode: '600519',
    stockName: '贵州茅台',
    sentimentScore: 82,
    operationAdvice: '买入',
    createdAt: '2026-03-15T08:00:00Z',
  },
];

const suffixedItems: HistoryItem[] = [
  {
    ...items[0],
    id: 3,
    stockCode: '600519.SH',
  },
];

const longChineseNameItem: HistoryItem = {
  id: 2,
  queryId: 'q-2',
  stockCode: '600519',
  stockName: '贵州茅台股票股份有限公司',
  sentimentScore: 75,
  operationAdvice: '持有',
  createdAt: '2026-03-16T08:00:00Z',
};

describe('HistoryList', () => {
  it('shows the empty state copy when no history exists', () => {
    const { container } = render(<HistoryList {...baseProps} items={[]} />);

    expect(screen.getByText('暂无历史分析记录')).toBeInTheDocument();
    expect(screen.getByText('完成首次分析后，这里会保留最近结果。')).toBeInTheDocument();
    expect(screen.getByText('历史分析')).toBeInTheDocument();
    expect(container.querySelector('.glass-card')).toBeTruthy();
  });

  it('renders selected count and forwards item interactions', () => {
    const onItemClick = vi.fn();
    const onToggleItemSelection = vi.fn();

    render(
      <HistoryList
        {...baseProps}
        items={items}
        selectedIds={new Set([1])}
        selectedId={1}
        onItemClick={onItemClick}
        onToggleItemSelection={onToggleItemSelection}
      />,
    );

    expect(screen.getByText('已选 1')).toBeInTheDocument();
    expect(screen.getByText('买入 82')).toBeInTheDocument();

    fireEvent.click(screen.getByRole('button', { name: /贵州茅台/i }));
    expect(onItemClick).toHaveBeenCalledWith(1);

    fireEvent.click(screen.getAllByRole('checkbox')[1]);
    expect(onToggleItemSelection).toHaveBeenCalledWith(1);
  });

  it('shows a holding badge for stocks in the current portfolio', () => {
    render(
      <HistoryList
        {...baseProps}
        items={items}
        holdingCodes={new Set(['600519'])}
      />,
    );

    expect(screen.getByText('持仓')).toBeInTheDocument();
  });

  it('matches holding badges when history stock codes include market suffixes', () => {
    render(
      <HistoryList
        {...baseProps}
        items={suffixedItems}
        holdingCodes={new Set(['600519'])}
      />,
    );

    expect(screen.getByText('持仓')).toBeInTheDocument();
  });

  it('sorts history rows by holding status and advice priority', () => {
    const orderedItems: HistoryItem[] = [
      {
        id: 11,
        queryId: 'q-11',
        stockCode: '300001',
        stockName: '非持仓中性',
        operationAdvice: '观望',
        sentimentScore: 50,
        createdAt: '2026-03-11T08:00:00Z',
      },
      {
        id: 12,
        queryId: 'q-12',
        stockCode: '600001',
        stockName: '持仓买入',
        operationAdvice: '买入',
        sentimentScore: 80,
        createdAt: '2026-03-12T08:00:00Z',
      },
      {
        id: 13,
        queryId: 'q-13',
        stockCode: '600002.SH',
        stockName: '持仓卖出',
        operationAdvice: '卖出',
        sentimentScore: 20,
        createdAt: '2026-03-13T08:00:00Z',
      },
      {
        id: 14,
        queryId: 'q-14',
        stockCode: '300002',
        stockName: '非持仓卖出',
        operationAdvice: '减仓',
        sentimentScore: 30,
        createdAt: '2026-03-14T08:00:00Z',
      },
      {
        id: 15,
        queryId: 'q-15',
        stockCode: '600003',
        stockName: '持仓中性',
        operationAdvice: '继续观察',
        sentimentScore: 60,
        createdAt: '2026-03-15T08:00:00Z',
      },
      {
        id: 16,
        queryId: 'q-16',
        stockCode: '300003',
        stockName: '非持仓买入',
        operationAdvice: '布局',
        sentimentScore: 70,
        createdAt: '2026-03-16T08:00:00Z',
      },
    ];

    render(
      <HistoryList
        {...baseProps}
        items={orderedItems}
        holdingCodes={new Set(['600001', '600002', '600003'])}
      />,
    );

    const labels = screen
      .getAllByRole('button')
      .map((button) => button.textContent || '')
      .filter((text) => text.includes('持仓') || text.includes('非持仓'));

    expect(labels[0]).toContain('持仓卖出');
    expect(labels[1]).toContain('持仓买入');
    expect(labels[2]).toContain('持仓中性');
    expect(labels[3]).toContain('非持仓卖出');
    expect(labels[4]).toContain('非持仓买入');
    expect(labels[5]).toContain('非持仓中性');
  });

  it('toggles select-all when clicking the label text', () => {
    const onToggleSelectAll = vi.fn();

    render(
      <HistoryList
        {...baseProps}
        items={items}
        onToggleSelectAll={onToggleSelectAll}
      />,
    );

    fireEvent.click(screen.getByText('全选当前'));

    expect(onToggleSelectAll).toHaveBeenCalledTimes(1);
  });

  it('disables delete when nothing is selected', () => {
    render(<HistoryList {...baseProps} items={items} />);

    expect(screen.getByRole('button', { name: '删除' })).toBeDisabled();
  });

  it('truncates long stock names with trailing dot', () => {
    render(
      <HistoryList
        {...baseProps}
        items={[longChineseNameItem]}
      />,
    );

    // '贵州茅台股票股份有限公司' (12 Chinese chars) should be truncated to '贵州茅台股票股份.' (8 chars + dot)
    // The full name exists in a hidden span, visible on hover
    expect(screen.getByText('贵州茅台股票股份.')).toBeInTheDocument();
    const fullNameHidden = screen.queryByText('贵州茅台股票股份有限公司');
    expect(fullNameHidden).toBeInTheDocument();
    expect(fullNameHidden).toHaveClass('hidden');
  });

  it('generates unique select-all ids across multiple instances', () => {
    const { container } = render(
      <>
        <HistoryList {...baseProps} items={items} />
        <HistoryList {...baseProps} items={items} />
      </>,
    );

    const labels = container.querySelectorAll('label[for]');
    const ids = Array.from(labels).map((label) => label.getAttribute('for'));

    expect(ids).toHaveLength(2);
    expect(new Set(ids).size).toBe(ids.length);
  });
});
