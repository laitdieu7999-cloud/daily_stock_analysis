import React, { useCallback, useEffect, useMemo, useState } from 'react';
import { icApi, type ICContractSnapshot, type ICMarketSnapshot } from '../api/ic';

type CalculationResult = {
  basis: number;
  annualizedPct: number;
  marketState: '贴水' | '升水' | '平水';
  directionHint: string;
};

function calculateIcAnnualized(
  spotPrice: number,
  futuresPrice: number,
  daysToExpiry: number,
): CalculationResult | null {
  if (spotPrice <= 0 || futuresPrice <= 0 || daysToExpiry <= 0) {
    return null;
  }

  const basis = spotPrice - futuresPrice;
  const annualizedPct = ((basis / futuresPrice) / (daysToExpiry / 365)) * 100;
  const marketState = basis > 0 ? '贴水' : basis < 0 ? '升水' : '平水';
  const directionHint =
    basis > 0
      ? '现货高于期货，适合按贴水思路评估年化收益。'
      : basis < 0
        ? '期货高于现货，当前是升水结构。'
        : '现货与期货接近平水，期限收益不明显。';

  return {
    basis,
    annualizedPct,
    marketState,
    directionHint,
  };
}

const formatSigned = (value: number, digits = 2): string => {
  const fixed = value.toFixed(digits);
  return value > 0 ? `+${fixed}` : fixed;
};

const parsePositiveNumber = (value: string): number => {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : 0;
};

const parseNumber = (value: string): number => {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : 0;
};

const describeAnnualized = (annualizedPct: number): string => {
  if (annualizedPct >= 20) return '贴水较深，年化吸引力较强。';
  if (annualizedPct >= 8) return '存在可观贴水，具备一定年化空间。';
  if (annualizedPct > 0) return '轻度贴水，年化空间有限。';
  if (annualizedPct <= -20) return '升水较深，做多贴水思路不成立。';
  if (annualizedPct < 0) return '处于升水，年化方向偏弱。';
  return '现货与期货基本贴近，年化不明显。';
};

const getAnnualizedStatus = (annualizedPct: number): string => {
  if (annualizedPct > 15) return '极端深水';
  if (annualizedPct >= 10) return '深水';
  if (annualizedPct >= 2 && annualizedPct <= 8) return '浅水合约';
  if (annualizedPct < 0) return '升水 / 平水';
  return '正常贴水';
};

const getAnnualizedToneClass = (annualizedPct: number): string => {
  if (annualizedPct > 15) return 'text-violet-400';
  if (annualizedPct >= 10) return 'text-rose-400';
  if (annualizedPct >= 2 && annualizedPct <= 8) return 'text-emerald-400';
  return 'text-foreground';
};

type IcDeskState = {
  frontLabel: string;
  frontTone: string;
  frontGapPct?: number | null;
  farAnchorPct?: number | null;
  putProxyLabel: string;
  putProxyHint: string;
  actionHint: string;
};

const buildIcDeskState = (snapshot: ICMarketSnapshot | null): IcDeskState => {
  const contracts = [...(snapshot?.contracts || [])].sort((a, b) => a.daysToExpiry - b.daysToExpiry);
  const frontGapPct = contracts.length >= 2
    ? contracts[0].annualizedBasisPct - contracts[1].annualizedBasisPct
    : null;
  const farA = contracts.length >= 4 ? contracts[2] : contracts[contracts.length - 2];
  const farB = contracts.length >= 4 ? contracts[3] : contracts[contracts.length - 1];
  const farAnchorPct = farA && farB ? farA.annualizedBasisPct - farB.annualizedBasisPct : null;
  const optionProxy = snapshot?.optionProxy || null;
  const putProxyLabel = optionProxy
    ? `QVIX ${optionProxy.qvixLatest.toFixed(2)} / Skew ${optionProxy.putSkewRatio.toFixed(3)}`
    : '期权代理未接入';
  const putProxyHint = optionProxy
    ? `PCR ${optionProxy.atmPutCallVolumeRatio == null ? '--' : optionProxy.atmPutCallVolumeRatio.toFixed(2)} · ${optionProxy.expiryYm}${optionProxy.rollWindowShifted ? ' 已避开末日轮' : ''}`
    : 'PCR / 虚平认沽比后续接数据。';

  if (frontGapPct == null) {
    return {
      frontLabel: '待刷新',
      frontTone: 'border-border bg-surface/60 text-secondary-text',
      frontGapPct,
      farAnchorPct,
      putProxyLabel,
      putProxyHint,
      actionHint: '等待 IC 合约行情。Shadow 指标只记录，不强推送。',
    };
  }
  if (frontGapPct >= 6) {
    return {
      frontLabel: '前端塌陷',
      frontTone: 'border-rose-400/40 bg-rose-500/10 text-rose-200',
      frontGapPct,
      farAnchorPct,
      putProxyLabel: optionProxy ? putProxyLabel : '需查看500ETF认沽',
      putProxyHint,
      actionHint: '近月贴水显著深于次月，优先人工确认是否需要保护；系统仅写 Shadow。',
    };
  }
  if (frontGapPct >= 3) {
    return {
      frontLabel: '需要关注',
      frontTone: 'border-amber-400/40 bg-amber-500/10 text-amber-200',
      frontGapPct,
      farAnchorPct,
      putProxyLabel: optionProxy ? putProxyLabel : '观察认沽价格比',
      putProxyHint,
      actionHint: '前端斜率开始走陡，先看是否伴随现货弱势和认沽放量。',
    };
  }
  return {
    frontLabel: '结构正常',
    frontTone: 'border-emerald-400/40 bg-emerald-500/10 text-emerald-200',
    frontGapPct,
    farAnchorPct,
    putProxyLabel: optionProxy ? putProxyLabel : '无需动作',
    putProxyHint,
    actionHint: '期限结构未显示踩踏，继续按面板观察贴水和持仓计划。',
  };
};

const formatDateTime = (value: Date): string =>
  new Intl.DateTimeFormat('zh-CN', {
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
  }).format(value);

const formatDateOnly = (value: string): string => {
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return '--';
  return new Intl.DateTimeFormat('zh-CN', {
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    weekday: 'short',
  }).format(parsed);
};

const ICAnnualizedPage: React.FC = () => {
  const [snapshot, setSnapshot] = useState<ICMarketSnapshot | null>(null);
  const [selectedSymbol, setSelectedSymbol] = useState('');
  const [now, setNow] = useState(() => new Date());
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [error, setError] = useState('');
  const [manualSpot, setManualSpot] = useState('');
  const [manualBasis, setManualBasis] = useState('');
  const [manualDays, setManualDays] = useState('');
  const [manualInitialized, setManualInitialized] = useState(false);

  useEffect(() => {
    document.title = 'IC - DSA';
  }, []);

  useEffect(() => {
    const timer = window.setInterval(() => setNow(new Date()), 1000);
    return () => window.clearInterval(timer);
  }, []);

  const loadSnapshot = useCallback(async (preferredSymbol?: string) => {
    setRefreshing(true);
    setError('');
    try {
      const data = await icApi.getSnapshot();
      setSnapshot(data);
      setSelectedSymbol((current) => {
        const target = preferredSymbol || current;
        if (target && data.contracts.some((item) => item.symbol === target)) {
          return target;
        }
        if (data.mainContractCode && data.contracts.some((item) => item.symbol === data.mainContractCode)) {
          return data.mainContractCode;
        }
        return data.contracts[0]?.symbol || '';
      });
    } catch (err) {
      console.error('加载 IC 快照失败', err);
      setError('IC 行情获取失败，请稍后重试。');
    } finally {
      setLoading(false);
      setRefreshing(false);
    }
  }, []);

  useEffect(() => {
    void loadSnapshot();
  }, [loadSnapshot]);

  const currentContract = useMemo<ICContractSnapshot | null>(() => {
    if (!snapshot?.contracts.length) return null;
    return (
      snapshot.contracts.find((item) => item.symbol === selectedSymbol) ||
      snapshot.contracts.find((item) => item.isMain) ||
      snapshot.contracts[0] ||
      null
    );
  }, [selectedSymbol, snapshot]);

  const derivedResult = useMemo(() => {
    if (!snapshot || !currentContract) return null;
    return calculateIcAnnualized(snapshot.spotPrice, currentContract.price, currentContract.daysToExpiry);
  }, [currentContract, snapshot]);

  const manualFuturesPrice = useMemo(() => {
    const spotPrice = parsePositiveNumber(manualSpot);
    if (spotPrice <= 0) return 0;
    return spotPrice - parseNumber(manualBasis);
  }, [manualBasis, manualSpot]);

  const manualResult = useMemo(
    () => calculateIcAnnualized(parsePositiveNumber(manualSpot), manualFuturesPrice, parsePositiveNumber(manualDays)),
    [manualDays, manualFuturesPrice, manualSpot],
  );

  const handleRefresh = async () => {
    await loadSnapshot(currentContract?.symbol);
  };

  const handleSelectContract = async (symbol: string) => {
    setSelectedSymbol(symbol);
    await loadSnapshot(symbol);
  };

  const applyCurrentContractToManual = useCallback(() => {
    if (!snapshot || !currentContract) return;
    setManualSpot(snapshot.spotPrice.toFixed(2));
    setManualBasis(currentContract.basis.toFixed(2));
    setManualDays(String(currentContract.daysToExpiry));
  }, [currentContract, snapshot]);

  useEffect(() => {
    if (manualInitialized || !snapshot || !currentContract) return;
    applyCurrentContractToManual();
    setManualInitialized(true);
  }, [applyCurrentContractToManual, currentContract, manualInitialized, snapshot]);

  const currentStatus = currentContract ? getAnnualizedStatus(currentContract.annualizedBasisPct) : null;
  const icDeskState = useMemo(() => buildIcDeskState(snapshot), [snapshot]);
  const currentAnnualizedToneClass = currentContract
    ? getAnnualizedToneClass(currentContract.annualizedBasisPct)
    : 'text-foreground';
  const manualStatus = manualResult ? getAnnualizedStatus(manualResult.annualizedPct) : null;
  const manualAnnualizedToneClass = manualResult
    ? getAnnualizedToneClass(manualResult.annualizedPct)
    : 'text-foreground';

  return (
    <div className="min-h-screen space-y-4 p-4 md:p-6">
      <div className="space-y-2">
        <h1 className="text-xl font-semibold text-foreground md:text-2xl">IC 年化率</h1>
        <p className="max-w-4xl text-sm text-secondary-text">
          上方刷新现货价格和全部 IC 合约。点击下方合约名称，切换当前合约并同步更新最新价格、到期日、剩余天数和年化率。
        </p>
      </div>

      <section className="glass-card space-y-4 p-4 md:p-6">
        <div className="flex flex-col gap-3 lg:flex-row lg:items-start lg:justify-between">
          <div className="space-y-2">
            <div className="text-sm text-secondary-text">当前时间</div>
            <div className="text-lg font-medium text-foreground">{formatDateTime(now)}</div>
            <div className="text-sm text-secondary-text">
              最近行情刷新：
              <span className="ml-1 text-foreground">
                {snapshot?.fetchedAt ? formatDateTime(new Date(snapshot.fetchedAt)) : '--'}
              </span>
            </div>
          </div>

          <div className="flex flex-col gap-3 sm:flex-row sm:items-center">
            <div className="rounded-2xl border border-subtle bg-surface/70 px-4 py-3">
              <div className="text-sm text-secondary-text">现货价格</div>
              <div className="mt-1 text-2xl font-semibold text-foreground">
                {snapshot ? snapshot.spotPrice.toFixed(2) : '--'}
              </div>
            </div>
            <button
              type="button"
              onClick={handleRefresh}
              disabled={refreshing}
              className="rounded-2xl border border-subtle bg-surface/70 px-4 py-3 text-sm font-medium text-foreground transition hover:bg-surface disabled:cursor-not-allowed disabled:opacity-60"
            >
              {refreshing ? '刷新中...' : '刷新现货与合约'}
            </button>
          </div>
        </div>

        {error ? (
          <div className="rounded-2xl border border-rose-400/30 bg-rose-500/10 px-4 py-3 text-sm text-rose-200">
            {error}
          </div>
        ) : null}

        <div className="grid gap-3 md:grid-cols-4">
          <div className="rounded-3xl border border-subtle bg-surface/60 p-4">
            <div className="text-xs text-secondary-text">近月年化贴水</div>
            <div className={`mt-2 text-2xl font-semibold ${currentAnnualizedToneClass}`}>
              {currentContract ? `${formatSigned(currentContract.annualizedBasisPct)}%` : '--'}
            </div>
            <div className="mt-1 text-xs text-secondary-text">{currentContract?.symbol || '主力合约'}</div>
          </div>
          <div className={`rounded-3xl border p-4 ${icDeskState.frontTone}`}>
            <div className="text-xs opacity-80">M1-M2 状态</div>
            <div className="mt-2 text-2xl font-semibold">{icDeskState.frontLabel}</div>
            <div className="mt-1 text-xs opacity-80">
              {icDeskState.frontGapPct == null ? '--' : `${formatSigned(icDeskState.frontGapPct)}%`}
            </div>
          </div>
          <div className="rounded-3xl border border-subtle bg-surface/60 p-4">
            <div className="text-xs text-secondary-text">Q1-Q2 远季锚</div>
            <div className="mt-2 text-2xl font-semibold text-foreground">
              {icDeskState.farAnchorPct == null ? '--' : `${formatSigned(icDeskState.farAnchorPct)}%`}
            </div>
            <div className="mt-1 text-xs text-secondary-text">用于分红季镇静，不直接推送。</div>
          </div>
          <div className="rounded-3xl border border-subtle bg-surface/60 p-4">
            <div className="text-xs text-secondary-text">500ETF认沽代理</div>
            <div className="mt-2 text-lg font-semibold text-foreground">{icDeskState.putProxyLabel}</div>
            <div className="mt-1 text-xs text-secondary-text">{icDeskState.putProxyHint}</div>
          </div>
        </div>

        <div className="rounded-3xl border border-subtle bg-surface/60 px-4 py-3 text-sm text-secondary-text">
          <span className="font-medium text-foreground">IC 执行提示：</span>
          <span className="ml-2">{icDeskState.actionHint}</span>
        </div>

        <div className="grid gap-4 lg:grid-cols-[320px_1fr]">
          <div className="rounded-3xl border border-subtle bg-surface/60 p-4">
            <div className="text-sm text-secondary-text">已选合约</div>
            <div className="mt-1 text-2xl font-semibold text-foreground">
              {currentContract?.symbol || '--'}
            </div>

            <div className="mt-4 space-y-3 text-sm">
              <div className="flex items-center justify-between gap-3">
                <span className="text-secondary-text">最新价格</span>
                <span className="font-medium text-foreground">
                  {currentContract ? currentContract.price.toFixed(2) : '--'}
                </span>
              </div>
              <div className="flex items-center justify-between gap-3">
                <span className="text-secondary-text">到期日</span>
                <span className="font-medium text-foreground">
                  {currentContract ? formatDateOnly(currentContract.expiryDate) : '--'}
                </span>
              </div>
              <div className="flex items-center justify-between gap-3">
                <span className="text-secondary-text">剩余天数</span>
                <span className="font-medium text-foreground">
                  {currentContract ? `${currentContract.daysToExpiry} 天` : '--'}
                </span>
              </div>
              <div className="flex items-center justify-between gap-3">
                <span className="text-secondary-text">期现差</span>
                <span className="font-medium text-foreground">
                  {currentContract ? formatSigned(currentContract.basis) : '--'}
                </span>
              </div>
              <div className="flex items-center justify-between gap-3">
                <span className="text-secondary-text">年化率</span>
                <span className={`font-medium ${currentAnnualizedToneClass}`}>
                  {currentContract ? `${formatSigned(currentContract.annualizedBasisPct)}%` : '--'}
                </span>
              </div>
              <div className="flex items-center justify-between gap-3">
                <span className="text-secondary-text">合约状态</span>
                <span className="font-medium text-foreground">
                  {currentStatus || '--'}
                </span>
              </div>
            </div>

            <div className="mt-4 rounded-2xl border border-subtle bg-surface/70 px-4 py-3 text-sm text-secondary-text">
              {derivedResult
                ? `${derivedResult.marketState} · ${describeAnnualized(derivedResult.annualizedPct)} ${derivedResult.directionHint}`
                : '等待行情数据。'}
            </div>
          </div>

          <div className="overflow-hidden rounded-3xl border border-subtle bg-surface/60">
            <table className="w-full text-sm">
              <thead className="bg-surface/80">
                <tr className="border-b border-subtle">
                  <th className="px-4 py-3 text-left font-medium text-secondary-text">合约名称</th>
                  <th className="px-4 py-3 text-right font-medium text-secondary-text">最新价格</th>
                  <th className="px-4 py-3 text-left font-medium text-secondary-text">到期日</th>
                  <th className="px-4 py-3 text-right font-medium text-secondary-text">剩余天数</th>
                  <th className="px-4 py-3 text-right font-medium text-secondary-text">期现差</th>
                  <th className="px-4 py-3 text-right font-medium text-secondary-text">年化率</th>
                </tr>
              </thead>
              <tbody>
                {loading ? (
                  <tr>
                    <td colSpan={6} className="px-4 py-8 text-center text-secondary-text">
                      加载中...
                    </td>
                  </tr>
                ) : snapshot?.contracts.length ? (
                  snapshot.contracts.map((contract) => {
                    const selected = contract.symbol === currentContract?.symbol;
                    const annualizedToneClass = getAnnualizedToneClass(contract.annualizedBasisPct);
                    return (
                      <tr
                        key={contract.symbol}
                        className={`border-b border-subtle transition-colors last:border-b-0 ${
                          selected ? 'bg-[rgba(59,130,246,0.12)]' : 'hover:bg-white/[0.02]'
                        }`}
                      >
                        <td className="px-4 py-3">
                          <button
                            type="button"
                            onClick={() => void handleSelectContract(contract.symbol)}
                            className={`inline-flex items-center gap-2 text-left transition ${
                              selected ? 'font-semibold text-foreground' : 'text-foreground hover:text-sky-300'
                            }`}
                          >
                            <span>{contract.symbol}</span>
                          </button>
                        </td>
                        <td className="px-4 py-3 text-right text-foreground">{contract.price.toFixed(2)}</td>
                        <td className="px-4 py-3 text-foreground">{formatDateOnly(contract.expiryDate)}</td>
                        <td className="px-4 py-3 text-right text-foreground">{contract.daysToExpiry}</td>
                        <td className="px-4 py-3 text-right text-foreground">{formatSigned(contract.basis)}</td>
                        <td className={`px-4 py-3 text-right font-medium ${annualizedToneClass}`}>
                          {formatSigned(contract.annualizedBasisPct)}%
                        </td>
                      </tr>
                    );
                  })
                ) : (
                  <tr>
                    <td colSpan={6} className="px-4 py-8 text-center text-secondary-text">
                      暂无可用合约数据。
                    </td>
                  </tr>
                )}
              </tbody>
            </table>

            <div className="border-t border-subtle p-4">
              <div className="flex flex-col gap-3 lg:flex-row lg:items-start lg:justify-between">
                <div>
                  <div className="text-sm font-medium text-foreground">手动计算器</div>
                  <div className="mt-1 text-xs text-secondary-text">手动输入现货、期现差和剩余天数，自动锁定计算期货价格。</div>
                </div>
                <button
                  type="button"
                  onClick={applyCurrentContractToManual}
                  disabled={!snapshot || !currentContract}
                  className="rounded-lg border border-subtle bg-surface/70 px-3 py-2 text-xs font-medium text-foreground transition hover:bg-surface disabled:cursor-not-allowed disabled:opacity-50"
                >
                  套用已选合约
                </button>
              </div>

              <div className="mt-4 grid gap-3 md:grid-cols-4">
                <label className="space-y-1 text-xs text-secondary-text">
                  <span>现货价格</span>
                  <input
                    value={manualSpot}
                    onChange={(event) => setManualSpot(event.target.value)}
                    inputMode="decimal"
                    className="w-full rounded-lg border border-subtle bg-surface/70 px-3 py-2 text-sm text-foreground outline-none transition focus:border-sky-400/60"
                  />
                </label>
                <label className="space-y-1 text-xs text-secondary-text">
                  <span>期现差</span>
                  <input
                    value={manualBasis}
                    onChange={(event) => setManualBasis(event.target.value)}
                    inputMode="decimal"
                    className="w-full rounded-lg border border-subtle bg-surface/70 px-3 py-2 text-sm text-foreground outline-none transition focus:border-sky-400/60"
                  />
                </label>
                <label className="space-y-1 text-xs text-secondary-text">
                  <span>期货价格（自动）</span>
                  <input
                    aria-label="期货价格"
                    value={manualFuturesPrice > 0 ? manualFuturesPrice.toFixed(2) : ''}
                    readOnly
                    inputMode="decimal"
                    className="w-full cursor-not-allowed rounded-lg border border-subtle bg-muted/40 px-3 py-2 text-sm text-secondary-text outline-none"
                  />
                </label>
                <label className="space-y-1 text-xs text-secondary-text">
                  <span>剩余天数</span>
                  <input
                    value={manualDays}
                    onChange={(event) => setManualDays(event.target.value)}
                    inputMode="numeric"
                    className="w-full rounded-lg border border-subtle bg-surface/70 px-3 py-2 text-sm text-foreground outline-none transition focus:border-sky-400/60"
                  />
                </label>
              </div>

              <div className="mt-4 grid gap-3 md:grid-cols-3">
                <div className="rounded-lg border border-subtle bg-surface/60 px-3 py-2">
                  <div className="text-xs text-secondary-text">期货价格</div>
                  <div className="mt-1 text-base font-medium text-foreground">
                    {manualFuturesPrice > 0 ? manualFuturesPrice.toFixed(2) : '--'}
                  </div>
                </div>
                <div className="rounded-lg border border-subtle bg-surface/60 px-3 py-2">
                  <div className="text-xs text-secondary-text">年化率</div>
                  <div className={`mt-1 text-base font-medium ${manualAnnualizedToneClass}`}>
                    {manualResult ? `${formatSigned(manualResult.annualizedPct)}%` : '--'}
                  </div>
                </div>
                <div className="rounded-lg border border-subtle bg-surface/60 px-3 py-2">
                  <div className="text-xs text-secondary-text">合约状态</div>
                  <div className="mt-1 text-base font-medium text-foreground">{manualStatus || '--'}</div>
                </div>
              </div>
            </div>
          </div>
        </div>
      </section>
    </div>
  );
};

export { calculateIcAnnualized };
export default ICAnnualizedPage;
