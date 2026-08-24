"""Replay Binance BTCUSDT incremental L2 and report integrity."""
from __future__ import annotations

import sys
from pathlib import Path

import matplotlib.pyplot as plt
import polars as pl

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from lotech_dq.book import replay_order_book  # noqa: E402
from lotech_dq.checks import profile_frame  # noqa: E402
from lotech_dq.clocks import all_skews  # noqa: E402
from lotech_dq.io import FIGURES_DIR, ensure_dirs, load_parquet  # noqa: E402
from lotech_dq.report import write_table_json  # noqa: E402


def main() -> None:
    ensure_dirs()
    df = load_parquet("D")
    profile = profile_frame(df, "D_binance_btcusdt_orderbook_incremental.parquet")
    for f in all_skews(df, profile["file"]):
        profile["findings"].append(f.to_dict())

    # Snapshot flag anomaly
    snap_true = int(df.filter(pl.col("snapshot") == True).height)  # noqa: E712
    level_stats = df.select(
        pl.col("bid_prices").list.len().alias("n_bid"),
        pl.col("ask_prices").list.len().alias("n_ask"),
    ).with_columns((pl.col("n_bid") + pl.col("n_ask")).alias("n_lvl"))

    result = replay_order_book(df, snapshot_level_threshold=200)
    top = result["top_of_book"]
    integrity = result["integrity"]

    # downsample for plot
    plot_df = top.filter(pl.col("mid").is_not_null())
    if plot_df.height > 5000:
        step = plot_df.height // 5000
        plot_df = plot_df.with_row_index("i").filter(pl.col("i") % step == 0)

    fig, axes = plt.subplots(2, 1, figsize=(12, 7), sharex=True)
    x = range(plot_df.height)
    axes[0].plot(x, plot_df["mid"].to_list(), lw=0.8, label="mid")
    axes[0].set_ylabel("Mid (USDT)")
    axes[0].set_title("D Binance BTCUSDT reconstructed mid")
    axes[0].legend(loc="upper right")
    axes[1].plot(x, plot_df["spread"].to_list(), lw=0.8, color="tab:orange", label="spread")
    axes[1].set_ylabel("Spread")
    axes[1].set_xlabel("Update index (downsampled)")
    axes[1].legend(loc="upper right")
    fig.tight_layout()
    fig_path = FIGURES_DIR / "D_binance_l2_mid_spread.png"
    fig.savefig(fig_path, dpi=120)
    plt.close(fig)

    crossed_pct = (
        integrity["crossed_events"] / max(integrity["updates_applied"], 1) * 100.0
    )
    out = {
        "snapshot_true_count": snap_true,
        "level_stats": {
            "median_n_lvl": float(level_stats["n_lvl"].median()),  # type: ignore[arg-type]
            "max_n_lvl": int(level_stats["n_lvl"].max()),  # type: ignore[arg-type]
            "p99_n_lvl": float(level_stats.select(pl.col("n_lvl").quantile(0.99)).item()),
        },
        "integrity": integrity,
        "crossed_pct": crossed_pct,
        "top_summary": {
            "rows": top.height,
            "mid_min": float(top["mid"].drop_nulls().min()) if top.height else None,
            "mid_max": float(top["mid"].drop_nulls().max()) if top.height else None,
            "spread_median": float(top["spread"].drop_nulls().median()) if top.height else None,  # type: ignore[arg-type]
        },
        "profile_findings": profile["findings"],
        "figure": str(fig_path.name),
    }
    write_table_json("D_binance_l2.json", out)
    print("integrity:", integrity)
    print("crossed_pct:", crossed_pct)
    print("wrote", fig_path)


if __name__ == "__main__":
    main()
