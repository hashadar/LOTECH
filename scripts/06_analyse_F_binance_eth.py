"""Binance ETHUSDT top-of-book DQ."""
from __future__ import annotations

import sys
from pathlib import Path

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from lotech_dq.checks import profile_frame, tob_crossed_locked  # noqa: E402
from lotech_dq.io import ensure_dirs, load_parquet  # noqa: E402
from lotech_dq.report import write_table_json  # noqa: E402


def main() -> None:
    ensure_dirs()
    df = load_parquet("F").sort("ingress_ts")
    profile = profile_frame(df, "F_binance_ethusdt_top_of_book.parquet")

    gaps = df.select(pl.col("ingress_ts").diff().dt.total_milliseconds().alias("gap_ms")).drop_nulls()
    # duplicate quotes (same bid/ask/qty)
    dup_quotes = (
        df.group_by(["bid_price", "ask_price", "bid_qty", "ask_qty"])
        .len()
        .filter(pl.col("len") > 1)
    )
    seq = df.select(pl.col("seq_id").diff().alias("d")).drop_nulls()
    seq_gaps = int(seq.filter(pl.col("d") > 1).height)
    seq_back = int(seq.filter(pl.col("d") < 0).height)

    # identical consecutive rows
    same_as_prev = df.select(
        (
            (pl.col("bid_price") == pl.col("bid_price").shift(1))
            & (pl.col("ask_price") == pl.col("ask_price").shift(1))
            & (pl.col("bid_qty") == pl.col("bid_qty").shift(1))
            & (pl.col("ask_qty") == pl.col("ask_qty").shift(1))
        )
        .fill_null(False)
        .alias("same")
    )
    identical_consecutive = int(same_as_prev.filter(pl.col("same")).height)

    out = {
        "rows": df.height,
        "instrument": df["instrument"].unique().to_list(),
        "ingress_min": str(df["ingress_ts"].min()),
        "ingress_max": str(df["ingress_ts"].max()),
        "has_venue_clocks": {
            "transaction_ts": "transaction_ts" in df.columns,
            "publish_ts": "publish_ts" in df.columns,
        },
        "crossed_locked": [f.to_dict() for f in tob_crossed_locked(df)],
        "gap_ms": {
            "median": float(gaps["gap_ms"].median()),  # type: ignore[arg-type]
            "p99": float(gaps.select(pl.col("gap_ms").quantile(0.99)).item()),
            "max": float(gaps["gap_ms"].max()),
            "over_1000ms": int(gaps.filter(pl.col("gap_ms") > 1000).height),
            "over_5000ms": int(gaps.filter(pl.col("gap_ms") > 5000).height),
        },
        "seq_id": {"gaps_gt_1": seq_gaps, "backward": seq_back},
        "identical_consecutive_quotes": identical_consecutive,
        "identical_consecutive_pct": identical_consecutive / max(df.height - 1, 1) * 100.0,
        "duplicate_quote_groups": dup_quotes.height,
        "spread": {
            "median": float((df["ask_price"] - df["bid_price"]).median()),  # type: ignore[arg-type]
            "p99": float((df["ask_price"] - df["bid_price"]).quantile(0.99)),
            "max": float((df["ask_price"] - df["bid_price"]).max()),
        },
        "profile_findings": profile["findings"],
    }
    write_table_json("F_binance_eth.json", out)
    print("crossed", out["crossed_locked"])
    print("gaps over 5s", out["gap_ms"]["over_5000ms"])
    print("missing venue clocks", out["has_venue_clocks"])
    print("wrote outputs/tables/F_binance_eth.json")


if __name__ == "__main__":
    main()
