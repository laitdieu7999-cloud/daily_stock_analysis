import React, { useEffect, useState } from 'react';
import type { ParsedApiError } from '../api/error';
import { getParsedApiError } from '../api/error';
import { ApiErrorAlert } from '../components/common';
import { historyApi } from '../api/history';
import type { ArchiveInsightItem } from '../types/analysis';

const ReviewPage: React.FC = () => {
  const [items, setItems] = useState<ArchiveInsightItem[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<ParsedApiError | null>(null);

  useEffect(() => {
    let mounted = true;
    const load = async () => {
      try {
        setLoading(true);
        const response = await historyApi.getArchiveInsights();
        if (!mounted) return;
        setItems(response.items || []);
        setError(null);
      } catch (err) {
        if (!mounted) return;
        setError(getParsedApiError(err));
      } finally {
        if (mounted) setLoading(false);
      }
    };
    void load();
    return () => {
      mounted = false;
    };
  }, []);

  return (
    <div className="space-y-6 px-1 pb-8">
      <section className="rounded-3xl border border-border/70 bg-card/80 p-6 shadow-soft-card backdrop-blur-sm">
        <div className="space-y-2">
          <p className="text-xs font-semibold uppercase tracking-[0.24em] text-secondary-text">长期归档</p>
          <h1 className="text-2xl font-semibold text-foreground">复盘看板</h1>
          <p className="max-w-3xl text-sm leading-6 text-secondary-text">
            这里会集中展示最近生成的周度、月度、样本复盘和调整建议，方便直接在网页里查看，不用再翻本地文件夹。
          </p>
        </div>
      </section>

      {loading ? (
        <div className="rounded-3xl border border-border/70 bg-card/70 p-6 text-sm text-secondary-text shadow-soft-card">
          正在加载长期归档洞察...
        </div>
      ) : null}

      {error ? (
        <div className="rounded-3xl border border-border/70 bg-card/70 p-4 shadow-soft-card">
          <ApiErrorAlert error={error} />
        </div>
      ) : null}

      {!loading && !error && items.length === 0 ? (
        <div className="rounded-3xl border border-border/70 bg-card/70 p-6 text-sm text-secondary-text shadow-soft-card">
          当前还没有可展示的长期归档洞察，等日报样本再积累一些，这里会自动长出来。
        </div>
      ) : null}

      <div className="grid gap-4 xl:grid-cols-2">
        {items.map((item) => (
          <section
            key={item.key}
            className="rounded-3xl border border-border/70 bg-card/80 p-5 shadow-soft-card backdrop-blur-sm"
          >
            <div className="mb-3 space-y-1">
              <h2 className="text-lg font-semibold text-foreground">{item.title}</h2>
              <p className="text-xs text-secondary-text">
                {item.updatedAt ? `更新于 ${item.updatedAt.replace('T', ' ').slice(0, 16)}` : '更新时间未知'}
              </p>
            </div>
            <pre className="max-h-[420px] overflow-auto rounded-2xl bg-base/60 p-4 text-xs leading-6 text-foreground whitespace-pre-wrap">
              {item.content}
            </pre>
          </section>
        ))}
      </div>
    </div>
  );
};

export default ReviewPage;
