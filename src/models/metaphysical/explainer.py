"""Human-readable summaries for metaphysical research signals."""

from __future__ import annotations

TRIGGER_LABELS = {
    "csi500_liquidity_crisis": "中证500流动性危机触发",
    "csi500_flash_crash": "中证500闪崩相位触发",
    "csi500_capital_drain": "中证500资金抽离触发",
    "gold_panic_rush": "黄金恐慌抢购触发",
    "gold_macro_shock": "黄金宏观冲击触发",
    "gold_currency_crisis": "黄金货币危机触发",
}


def summarize_trigger_row(row) -> list[str]:
    """Return active trigger labels for a single feature row."""
    active = []
    for key, label in TRIGGER_LABELS.items():
        if int(row.get(key, 0) or 0) == 1:
            active.append(label)
    return active


def summarize_trigger_frame(df) -> list[dict]:
    """Return readable summaries aligned to the dataframe index."""
    records = []
    for idx, row in df.iterrows():
        active = summarize_trigger_row(row)
        records.append(
            {
                "date": idx,
                "active_triggers": active,
                "summary": "、".join(active) if active else "无触发",
            }
        )
    return records
