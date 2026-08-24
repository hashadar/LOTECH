"""NASDAQ 20-symbol TOB aggregations and anomaly ranking."""
from __future__ import annotations

import sys
from pathlib import Path

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from lotech_dq.checks import profile_frame  # noqa: E402
from lotech_dq.clocks import skew_stats  # noqa: E402
from lotech_dq.io import ensure_dirs, load_parquet  # noqa: E402
from lotech_dq.report import write_table_json  # noqa: E402


def main() -> None:
    ensure_dirs()
    df = load_parquet("C")

    # Light global profile on column subset for speed/memory notes
    subset_cols = [
        c
        for c in (
            "instrument",
            "ingress_ts",
            "transaction_ts",
            "publish_ts",
            "seq_id",
            "bid_price",
            "ask_price",
            "bid_qty",
            "ask_qty",
        )
        if c in df.columns
    ]
    profile = profile_frame(df.select(subset_cols), "C_nasdaq_top_of_book_20_symbols.parquet")

    per_symbol = (
        df.sort(["instrument", "transaction_ts"])
        .with_columns(
            [
                (pl.col("bid_price") > pl.col("ask_price")).alias("crossed"),
                (pl.col("bid_price") == pl.col("ask_price")).alias("locked"),
                (pl.col("ask_price") - pl.col("bid_price")).alias("spread"),
                pl.col("transaction_ts").diff().over("instrument").dt.total_seconds().alias("gap_s"),
                (
                    (pl.col("ingress_ts") - pl.col("transaction_ts")).dt.total_microseconds()
                    / 1_000_000.0
                ).alias("skew_s"),
            ]
        )
        .group_by("instrument")
        .agg(
            [
                pl.len().alias("rows"),
                pl.col("crossed").sum().alias("crossed_n"),
                pl.col("locked").sum().alias("locked_n"),
                pl.col("spread").median().alias("spread_median"),
                pl.col("spread").quantile(0.99).alias("spread_p99"),
                pl.col("spread").max().alias("spread_max"),
                pl.col("gap_s").median().alias("gap_median_s"),
                pl.col("gap_s").max().alias("gap_max_s"),
                (pl.col("gap_s") > 60).sum().alias("gaps_over_60s"),
                pl.col("skew_s").median().alias("skew_median_s"),
                pl.col("skew_s").quantile(0.99).alias("skew_p99_s"),
                (pl.col("skew_s") < 0).sum().alias("neg_skew_n"),
                pl.col("transaction_ts").min().alias("tx_min"),
                pl.col("transaction_ts").max().alias("tx_max"),
            ]
        )
        .with_columns(
            [
                (pl.col("crossed_n") / pl.col("rows") * 100.0).alias("crossed_pct"),
                (pl.col("locked_n") / pl.col("rows") * 100.0).alias("locked_pct"),
            ]
        )
        .with_columns(
            (
                pl.col("crossed_pct") * 5
                + pl.col("gaps_over_60s").cast(pl.Float64)
                + pl.col("neg_skew_n").cast(pl.Float64) / pl.col("rows") * 100.0
            ).alias("anomaly_score")
        )
        .sort("anomaly_score", descending=True)
    )

    per_symbol.write_csv(ROOT / "outputs" / "tables" / "C_nasdaq_per_symbol.csv")
    worst = per_symbol.head(3).to_dicts()

    global_skew = skew_stats(df)
    out = {
        "rows": df.height,
        "n_instruments": df["instrument"].n_unique(),
        "instruments": df["instrument"].unique().sort().to_list(),
        "global_skew": global_skew,
        "worst_symbols": [
            {
                k: (str(v) if hasattr(v, "isoformat") else v)
                for k, v in row.items()
            }
            for row in worst
        ],
        "crossed_total": int(df.filter(pl.col("bid_price") > pl.col("ask_price")).height),
        "locked_total": int(df.filter(pl.col("bid_price") == pl.col("ask_price")).height),
        "profile_findings": profile["findings"],
    }
    write_table_json("C_nasdaq.json", out)
    print("instruments", out["n_instruments"])
    print("worst", [w["instrument"] for w in out["worst_symbols"]])
    print("wrote outputs/tables/C_nasdaq.json")


if __name__ == "__main__":
    main()
