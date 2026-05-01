import { render, screen } from '@testing-library/react';
import { describe, expect, it } from 'vitest';
import { ReportStrategy } from '../ReportStrategy';

describe('ReportStrategy', () => {
  it('renders camelCase strategy values', () => {
    const { container } = render(
      <ReportStrategy
        strategy={{
          idealBuy: '99.00元',
          secondaryBuy: '96.00元',
          stopLoss: '90.16元',
          takeProfit: '108.00元',
        }}
        operationAdvice="买入"
        trendPrediction="看多"
      />,
    );

    expect(screen.getByText('作战计划')).toBeInTheDocument();
    expect(container.querySelector('.grid.sm\\:grid-cols-2.xl\\:grid-cols-4')).toBeTruthy();
    expect(container.querySelectorAll('.home-strategy-card')).toHaveLength(4);
    expect(screen.getByText('首笔建仓区')).toBeInTheDocument();
    expect(screen.getByText('确认加仓区')).toBeInTheDocument();
    expect(screen.getByText('破位止损线')).toBeInTheDocument();
    expect(screen.getByText('分批止盈区')).toBeInTheDocument();
    expect(screen.getByText('99.00元')).toBeInTheDocument();
    expect(screen.getByText('96.00元')).toBeInTheDocument();
    expect(screen.getByText('90.16元')).toBeInTheDocument();
    expect(screen.getByText('108.00元')).toBeInTheDocument();
  });

  it('renders snake_case strategy values from history payloads', () => {
    render(
      <ReportStrategy
        strategy={{
          ideal_buy: '暂不买入；重新站回1.006元后再评估',
          secondary_buy: '保守确认：站稳1.019元且止跌后再看',
          stop_loss: '0.964元',
          take_profit: '反弹压力：1.006元（不是止盈目标）',
        }}
        operationAdvice="卖出"
        trendPrediction="强烈看空"
      />,
    );

    expect(screen.getByText('立即减仓区')).toBeInTheDocument();
    expect(screen.getByText('反抽出局线')).toBeInTheDocument();
    expect(screen.getByText('持仓防守线')).toBeInTheDocument();
    expect(screen.getByText('重新评估线')).toBeInTheDocument();
    expect(screen.queryByText('目标区')).not.toBeInTheDocument();
    expect(screen.getByText('暂不接回；重新站回1.006元后再评估')).toBeInTheDocument();
    expect(screen.getByText('确认转强：站稳1.019元且止跌后再看')).toBeInTheDocument();
    expect(screen.getByText('0.964元')).toBeInTheDocument();
    expect(screen.getByText('1.006元附近（反抽出局，不是止盈）')).toBeInTheDocument();
  });

  it('compacts old bearish backend strategy values in the UI', () => {
    render(
      <ReportStrategy
        strategy={{
          ideal_buy: '暂不新开仓；重新站回MA5附近 28.42元 后再评估',
          secondary_buy: '保守等待回踩MA10附近 28.91元 且止跌后再看',
          stop_loss: '立即止损：对于截图持仓B，以现价27.48元或更优价格清仓。若无法执行，强制止损位设于26.00元整数关口。',
          take_profit: '目标位：33.64元（压力位或约8%风险回报目标）',
        }}
        operationAdvice="卖出"
        trendPrediction="强烈看空"
      />,
    );

    expect(screen.getByText('暂不接回；重新站回28.42元后再评估（较现价+3.4%）')).toBeInTheDocument();
    expect(screen.getByText('确认转强：站稳28.91元且止跌后再看')).toBeInTheDocument();
    expect(screen.getByText('27.48元附近离场；硬止损26.00元')).toBeInTheDocument();
    expect(screen.getByText('28.42元附近（反抽出局，不是止盈）')).toBeInTheDocument();
    expect(screen.queryByText(/33\.64/)).not.toBeInTheDocument();
  });

  it('does not call an upper bounce level a hard stop in bearish mode', () => {
    render(
      <ReportStrategy
        strategy={{
          ideal_buy: '暂不新开仓；重新站回MA5附近 99.42元 后再评估',
          secondary_buy: '保守等待回踩MA10附近 100.33元 且止跌后再看',
          stop_loss: '立即止损：以现价97.08元附近离场，若反抽至101.81元再重新评估。',
          take_profit: '目标位：108.00元（压力位）',
        }}
        operationAdvice="减仓"
        trendPrediction="看空"
      />,
    );

    expect(screen.getByText('97.08元附近离场')).toBeInTheDocument();
    expect(screen.queryByText(/硬止损101\.81/)).not.toBeInTheDocument();
  });

  it('does not treat a normal stop-loss sentence as a bearish panel', () => {
    render(
      <ReportStrategy
        strategy={{
          idealBuy: '20.36元（MA5附近，需等缩量回踩企稳）',
          secondaryBuy: '20.35元（MA10附近，作为更保守的加仓位）',
          stopLoss: '19.58元（跌破MA20必须止损离场）',
          takeProfit: '21.00元（前期压力位）',
        }}
        operationAdvice="观望"
        trendPrediction="看多"
      />,
    );

    expect(screen.getByText('观察买入区')).toBeInTheDocument();
    expect(screen.getByText('确认跟进线')).toBeInTheDocument();
    expect(screen.getByText('破位警戒线')).toBeInTheDocument();
    expect(screen.getByText('上方压力区')).toBeInTheDocument();
    expect(screen.queryByText('立即减仓区')).not.toBeInTheDocument();
  });

  it('shows a protective downgrade hint when levels are too far from current price', () => {
    render(
      <ReportStrategy
        strategy={{
          ideal_buy: '暂不设买入区；原点位偏离现价27.48元过大，待行情刷新后重算',
          secondary_buy: '暂不设加仓区；原点位偏离现价27.48元过大，待行情刷新后重算',
          stop_loss: '暂不设止损线；原点位偏离现价27.48元过大，待行情刷新后重算',
          take_profit: '暂不设目标区；原点位偏离现价27.48元过大，待行情刷新后重算',
        }}
        operationAdvice="买入"
        trendPrediction="看多"
      />,
    );

    expect(screen.getByText('点位已保护性降级')).toBeInTheDocument();
    expect(screen.getByText(/不要按原价位执行/)).toBeInTheDocument();
  });

  it('does not show the protective downgrade hint for normal nearby levels', () => {
    render(
      <ReportStrategy
        strategy={{
          idealBuy: '27.20元',
          secondaryBuy: '27.80元',
          stopLoss: '26.90元',
          takeProfit: '29.20元',
        }}
        operationAdvice="买入"
        trendPrediction="看多"
      />,
    );

    expect(screen.queryByText('点位已保护性降级')).not.toBeInTheDocument();
  });

  it('shows quote context above strategy levels', () => {
    render(
      <ReportStrategy
        strategy={{
          idealBuy: '27.20元',
          secondaryBuy: '27.80元',
          stopLoss: '26.90元',
          takeProfit: '29.20元',
        }}
        operationAdvice="买入"
        trendPrediction="看多"
        quoteContext={{
          currentPrice: 27.48,
          quoteTime: '2026-04-30T14:58:00',
          quoteSource: 'akshare',
        }}
      />,
    );

    expect(screen.getByText('当前价依据')).toBeInTheDocument();
    expect(screen.getByText('27.48元')).toBeInTheDocument();
    expect(screen.getByText('时间')).toBeInTheDocument();
    expect(screen.getByText('2026-04-30 14:58')).toBeInTheDocument();
    expect(screen.getByText('数据源')).toBeInTheDocument();
    expect(screen.getByText('akshare')).toBeInTheDocument();
  });
});
