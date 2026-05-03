import type React from 'react';
import { useRef, useCallback, useEffect, useId, useMemo, useState } from 'react';
import type { HistoryItem } from '../../types/analysis';
import { normalizeStockCode, removeMarketSuffix } from '../../utils/normalizeQuery';
import { Badge, Button, ScrollArea } from '../common';
import { DashboardPanelHeader, DashboardStateBlock } from '../dashboard';
import { HistoryListItem } from './HistoryListItem';

interface HistoryListProps {
  items: HistoryItem[];
  holdingCodes?: Set<string>;
  isLoading: boolean;
  isLoadingMore: boolean;
  hasMore: boolean;
  selectedId?: number;  // 当前选中的历史记录 ID
  selectedIds: Set<number>;
  isDeleting?: boolean;
  onItemClick: (recordId: number) => void;  // 点击记录的回调
  onLoadMore: () => void;
  onToggleItemSelection: (recordId: number) => void;
  onToggleSelectAll: () => void;
  onDeleteSelected: () => void;
  className?: string;
}

/**
 * 历史记录列表组件 (升级版)
 * 使用新设计系统组件实现，支持批量选择和滚动加载
 */
export const HistoryList: React.FC<HistoryListProps> = ({
  items,
  holdingCodes,
  isLoading,
  isLoadingMore,
  hasMore,
  selectedId,
  selectedIds,
  isDeleting = false,
  onItemClick,
  onLoadMore,
  onToggleItemSelection,
  onToggleSelectAll,
  onDeleteSelected,
  className = '',
}) => {
  const toHoldingKey = (code?: string | null): string | null => {
    if (!code) {
      return null;
    }
    const normalized = normalizeStockCode(code);
    if (!normalized) {
      return null;
    }
    return removeMarketSuffix(normalized);
  };

  const getAdvicePriority = (advice?: string): number => {
    const normalized = advice?.trim() || '';
    if (normalized.includes('减仓') || normalized.includes('卖')) {
      return 0;
    }
    if (normalized.includes('买') || normalized.includes('布局')) {
      return 1;
    }
    return 2;
  };

  const sortedItems = useMemo(() => {
    return [...items].sort((left, right) => {
      const leftKey = toHoldingKey(left.stockCode);
      const rightKey = toHoldingKey(right.stockCode);
      const leftHolding = leftKey ? (holdingCodes?.has(leftKey) ?? false) : false;
      const rightHolding = rightKey ? (holdingCodes?.has(rightKey) ?? false) : false;

      if (leftHolding !== rightHolding) {
        return leftHolding ? -1 : 1;
      }

      const adviceGap = getAdvicePriority(left.operationAdvice) - getAdvicePriority(right.operationAdvice);
      if (adviceGap !== 0) {
        return adviceGap;
      }

      const leftTime = Date.parse(left.createdAt || '') || 0;
      const rightTime = Date.parse(right.createdAt || '') || 0;
      if (leftTime !== rightTime) {
        return rightTime - leftTime;
      }

      return right.id - left.id;
    });
  }, [items, holdingCodes]);

  const scrollContainerRef = useRef<HTMLDivElement>(null);
  const loadMoreTriggerRef = useRef<HTMLDivElement>(null);
  const selectAllRef = useRef<HTMLInputElement>(null);
  const selectAllId = useId();
  const [isManagingSelection, setIsManagingSelection] = useState(false);

  const selectedCount = sortedItems.filter((item) => selectedIds.has(item.id)).length;
  const allVisibleSelected = sortedItems.length > 0 && selectedCount === sortedItems.length;
  const someVisibleSelected = selectedCount > 0 && !allVisibleSelected;
  const selectionMode = isManagingSelection || selectedCount > 0;

  // 使用 IntersectionObserver 检测滚动到底部
  const handleObserver = useCallback(
    (entries: IntersectionObserverEntry[]) => {
      const target = entries[0];
      if (target.isIntersecting && hasMore && !isLoading && !isLoadingMore) {
        const container = scrollContainerRef.current;
        if (container && container.scrollHeight > container.clientHeight) {
          onLoadMore();
        }
      }
    },
    [hasMore, isLoading, isLoadingMore, onLoadMore]
  );

  useEffect(() => {
    const trigger = loadMoreTriggerRef.current;
    const container = scrollContainerRef.current;
    if (!trigger || !container) return;

    const observer = new IntersectionObserver(handleObserver, {
      root: container,
      rootMargin: '20px',
      threshold: 0.1,
    });

    observer.observe(trigger);
    return () => observer.disconnect();
  }, [handleObserver]);

  useEffect(() => {
    if (selectAllRef.current) {
      selectAllRef.current.indeterminate = someVisibleSelected;
    }
  }, [someVisibleSelected]);

  return (
    <aside className={`glass-card overflow-hidden flex flex-col ${className}`}>
      <ScrollArea
        viewportRef={scrollContainerRef}
        viewportClassName="p-4"
        testId="home-history-list-scroll"
      >
        <div className="mb-4 space-y-3">
          <DashboardPanelHeader
            className="mb-1"
            title="历史分析"
            titleClassName="text-sm font-medium"
            leading={(
              <svg className="h-4 w-4 text-primary" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 8v4l3 3m6-3a9 9 0 11-18 0 9 9 0 0118 0z" />
              </svg>
            )}
            headingClassName="items-center"
            actions={
              sortedItems.length > 0 ? (
                <>
                  {selectedCount > 0 ? (
                    <Badge variant="info" size="sm" className="history-selection-badge animate-in fade-in zoom-in duration-200">
                      已选 {selectedCount}
                    </Badge>
                  ) : null}
                  <button
                    type="button"
                    onClick={() => setIsManagingSelection((value) => !value)}
                    className="rounded-full border border-border/70 bg-subtle/60 px-2.5 py-1 text-[11px] font-semibold text-secondary-text transition-colors hover:border-primary/30 hover:bg-primary/10 hover:text-primary"
                  >
                    {selectionMode ? '完成' : '管理'}
                  </button>
                </>
              ) : undefined
            }
          />

          {sortedItems.length > 0 ? (
            <p className="rounded-xl border border-border/55 bg-subtle/40 px-2.5 py-2 text-[11px] leading-5 text-secondary-text">
              优先显示持仓，再按减仓、买入、观望排序。
            </p>
          ) : null}

          {sortedItems.length > 0 && selectionMode && (
            <div className="flex items-center gap-2">
              <label
                className="flex flex-1 cursor-pointer items-center gap-2 rounded-lg px-2 py-1"
                htmlFor={selectAllId}
              >
                <input
                  id={selectAllId}
                  ref={selectAllRef}
                  type="checkbox"
                  checked={allVisibleSelected}
                  onChange={onToggleSelectAll}
                  disabled={isDeleting}
                  aria-label="全选当前已加载历史记录"
                  className="history-select-all-checkbox h-3.5 w-3.5 cursor-pointer bg-transparent accent-primary focus:ring-primary/30 disabled:opacity-50"
                />
                <span className="text-[11px] text-muted-text select-none">全选当前</span>
              </label>
              <Button
                variant="danger-subtle"
                size="xsm"
                onClick={onDeleteSelected}
                disabled={selectedCount === 0 || isDeleting}
                isLoading={isDeleting}
                className="history-batch-delete-button disabled:!border-transparent disabled:!bg-transparent"
              >
                {isDeleting ? '删除中' : '删除'}
              </Button>
            </div>
          )}
        </div>

        {isLoading ? (
          <DashboardStateBlock
            loading
            compact
            title="加载历史记录中..."
          />
        ) : sortedItems.length === 0 ? (
          <DashboardStateBlock
            title="暂无历史分析记录"
            description="新的分析会自动保存在这里；如果刚清空过历史，直接在顶部输入股票即可重新生成。"
            icon={(
              <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M12 8v4l3 3m6-3a9 9 0 11-18 0 9 9 0 0118 0z" />
              </svg>
            )}
            action={(
              <div className="grid w-full gap-2 text-left text-[11px] text-secondary-text">
                <div className="rounded-xl border border-border/60 bg-subtle/50 px-3 py-2">
                  顶部输入代码：生成新报告
                </div>
                <div className="rounded-xl border border-border/60 bg-subtle/50 px-3 py-2">
                  持仓页点击股票：自动回测并回到首页
                </div>
              </div>
            )}
          />
        ) : (
          <div className="space-y-2">
            {sortedItems.map((item) => (
              <HistoryListItem
                key={item.id}
                item={item}
                isViewing={selectedId === item.id}
                isHolding={(() => {
                  const key = toHoldingKey(item.stockCode);
                  return key ? (holdingCodes?.has(key) ?? false) : false;
                })()}
                selectionMode={selectionMode}
                isChecked={selectedIds.has(item.id)}
                isDeleting={isDeleting}
                onToggleChecked={onToggleItemSelection}
                onClick={onItemClick}
              />
            ))}

            <div ref={loadMoreTriggerRef} className="h-4" />
            
            {isLoadingMore && (
              <div className="flex justify-center py-4">
                <div className="home-spinner h-5 w-5 animate-spin border-2" />
              </div>
            )}

            {!hasMore && sortedItems.length > 0 && (
              <div className="text-center py-5">
                <div className="h-px bg-subtle w-full mb-3" />
                <span className="text-[10px] text-secondary-text uppercase tracking-[0.2em]">已到底部</span>
              </div>
            )}
          </div>
        )}
      </ScrollArea>
    </aside>
  );
};
