import { useEffect, useRef } from 'react';
import type { TaskInfo } from '../types/analysis';
import { useTaskStream } from './useTaskStream';

type UseDashboardLifecycleOptions = {
  loadInitialHistory: () => Promise<void>;
  refreshHistory: (silent?: boolean) => Promise<void>;
  refreshHoldingCodes?: () => Promise<void>;
  syncTaskCreated: (task: TaskInfo) => void;
  syncTaskUpdated: (task: TaskInfo) => void;
  syncTaskFailed: (task: TaskInfo) => void;
  selectLatestHistoryForStock?: (stockCode: string) => Promise<void>;
  removeTask: (taskId: string) => void;
  enabled?: boolean;
};

export function useDashboardLifecycle({
  loadInitialHistory,
  refreshHistory,
  refreshHoldingCodes,
  syncTaskCreated,
  syncTaskUpdated,
  syncTaskFailed,
  selectLatestHistoryForStock,
  removeTask,
  enabled = true,
}: UseDashboardLifecycleOptions): void {
  const removalTimeoutsRef = useRef<number[]>([]);

  useEffect(() => {
    if (!enabled) {
      return;
    }

    void loadInitialHistory();
  }, [enabled, loadInitialHistory]);

  useEffect(() => {
    if (!enabled) {
      return;
    }

    const intervalId = window.setInterval(() => {
      void refreshHistory(true);
      void refreshHoldingCodes?.();
    }, 30_000);

    return () => window.clearInterval(intervalId);
  }, [enabled, refreshHistory, refreshHoldingCodes]);

  useEffect(() => {
    if (!enabled) {
      return;
    }

    const handleVisibilityChange = () => {
      if (document.visibilityState === 'visible') {
        void refreshHistory(true);
        void refreshHoldingCodes?.();
      }
    };

    document.addEventListener('visibilitychange', handleVisibilityChange);
    return () => document.removeEventListener('visibilitychange', handleVisibilityChange);
  }, [enabled, refreshHistory, refreshHoldingCodes]);

  useEffect(() => {
    return () => {
      removalTimeoutsRef.current.forEach((timeoutId) => window.clearTimeout(timeoutId));
      removalTimeoutsRef.current = [];
    };
  }, []);

  const scheduleTaskRemoval = (taskId: string, delayMs: number) => {
    const timeoutId = window.setTimeout(() => {
      removeTask(taskId);
      removalTimeoutsRef.current = removalTimeoutsRef.current.filter((item) => item !== timeoutId);
    }, delayMs);

    removalTimeoutsRef.current.push(timeoutId);
  };

  const scheduleRefreshRetry = (task: TaskInfo, delayMs: number) => {
    const timeoutId = window.setTimeout(() => {
      void refreshHistory(true).then(() => selectLatestHistoryForStock?.(task.stockCode));
      removalTimeoutsRef.current = removalTimeoutsRef.current.filter((item) => item !== timeoutId);
    }, delayMs);

    removalTimeoutsRef.current.push(timeoutId);
  };

  useTaskStream({
    onTaskCreated: syncTaskCreated,
    onTaskStarted: syncTaskUpdated,
    onTaskProgress: syncTaskUpdated,
    onTaskCompleted: (task) => {
      syncTaskUpdated(task);
      void refreshHistory(true).then(() => selectLatestHistoryForStock?.(task.stockCode));
      void refreshHoldingCodes?.();
      scheduleRefreshRetry(task, 1_200);
      scheduleTaskRemoval(task.taskId, 2_000);
    },
    onTaskFailed: (task) => {
      syncTaskFailed(task);
      scheduleTaskRemoval(task.taskId, 5_000);
    },
    onError: () => {
      console.warn('SSE connection disconnected, reconnecting...');
    },
    enabled,
  });
}

export default useDashboardLifecycle;
