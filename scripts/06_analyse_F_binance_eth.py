"""Binance ETHUSDT top-of-book DQ."""
from __future__ import annotations

import sys
from pathlib import Path

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from lotech_dq.checks import EXPECTED_COLUMNS, profile_frame, tob_crossed_locked  # noqa: E402
from lotech_dq.io import data_path, ensure_dirs, load_parquet  # noqa: E402
from lotech_dq.report import write_table_json  # noqa: E402


def main() -> None:
    ensure_dirs()
    df = load_parquet("F").sort("ingress_ts")
    profile = profile_frame(df, "F_binance_ethusdt_top_of_book.parquet")

    # read the schema straight off the parquet so "absent" is a fact about the file and
    # not an artefact of a projection somewhere upstream
    parquet_columns = pl.scan_parquet(data_path("F")).collect_schema().names()

    # dt.total_milliseconds() floors a Duration(us), so sub-millisecond gaps become 0
    gaps = df.select(
        (pl.col("ingress_ts").diff().dt.total_microseconds() / 1000.0).alias("gap_ms")
    ).drop_nulls()

    dup_quotes = (
        df.group_by(["bid_price", "ask_price", "bid_qty", "ask_qty"]).len().filter(pl.col("len") > 1)
    )
    seq = df.select(pl.col("seq_id").diff().alias("d")).drop_nulls()

    consecutive = df.select(
        (
            (pl.col("bid_price") == pl.col("bid_price").shift(1))
            & (pl.col("ask_price") == pl.col("ask_price").shift(1))
        ).alias("prices_same"),
        (
            (pl.col("bid_price") == pl.col("bid_price").shift(1))
            & (pl.col("ask_price") == pl.col("ask_price").shift(1))
            & (pl.col("bid_qty") == pl.col("bid_qty").shift(1))
            & (pl.col("ask_qty") == pl.col("ask_qty").shift(1))
        ).alias("all_same"),
    ).drop_nulls()

    spread = df.select((pl.col("ask_price") - pl.col("bid_price")).alias("s"))
    # float subtraction of 2dp prices does not land exactly on the tick
    one_tick = int(spread.filter((pl.col("s") - 0.01).abs() < 1e-9).height)

    out = {
        "rows": df.height,
        "instrument": df["instrument"].unique().to_list(),
        "ingress_min": str(df["ingress_ts"].min()),
        "ingress_max": str(df["ingress_ts"].max()),
        "clock_columns": {
            "parquet_schema_columns": parquet_columns,
            "expected": list(EXPECTED_COLUMNS),
            "absent_from_parquet_schema": [c for c in EXPECTED_COLUMNS if c not in parquet_columns],
            "note": "absent from the schema, not present-and-null: a null-rate alert cannot fire on these",
        },
        "crossed_locked": [f.to_dict() for f in tob_crossed_locked(df)],
        "quote_tightness": {
            "crossed": int(df.filter(pl.col("bid_price") > pl.col("ask_price")).height),
            "locked": int(df.filter(pl.col("bid_price") == pl.col("ask_price")).height),
            "one_tick_rows": one_tick,
            "one_tick_pct": one_tick / df.height * 100.0,
            "tick_size": 0.01,
        },
        "gap_ms": {
            "median": float(gaps["gap_ms"].median()),
            "p99": float(gaps.select(pl.col("gap_ms").quantile(0.99)).item()),
            "max": float(gaps["gap_ms"].max()),
            "over_1000ms": int(gaps.filter(pl.col("gap_ms") > 1000).height),
            "over_5000ms": int(gaps.filter(pl.col("gap_ms") > 5000).height),
        },
        "seq_id": {
            "gaps_gt_1": int(seq.filter(pl.col("d") > 1).height),
            "backward": int(seq.filter(pl.col("d") < 0).height),
            "step_median": float(seq["d"].median()),
            "step_p99": float(seq.select(pl.col("d").quantile(0.99)).item()),
            "step_max": int(seq["d"].max()),
            "distinct": int(df["seq_id"].n_unique()),
        },
        "identical_consecutive_quotes": int(consecutive.filter(pl.col("all_same")).height),
        "consecutive_pairs": consecutive.height,
        "consecutive_prices_unchanged": int(consecutive.filter(pl.col("prices_same")).height),
        "consecutive_prices_unchanged_pct": int(consecutive.filter(pl.col("prices_same")).height)
        / consecutive.height * 100.0,
        "duplicate_quote_groups": dup_quotes.height,
        "duplicate_quote_rows": int(dup_quotes["len"].sum()),
        "duplicate_quote_excess_rows": int(dup_quotes.select((pl.col("len") - 1).sum()).item()),
        "duplicate_quote_largest_group": int(dup_quotes["len"].max()),
        "spread": {
            "median": float(spread["s"].median()),
            "min": float(spread["s"].min()),
            "p99": float(spread.select(pl.col("s").quantile(0.99)).item()),
            "max": float(spread["s"].max()),
        },
        "profile_findings": profile["findings"],
    }
    write_table_json("F_binance_eth.json", out)
    print("one tick pct", out["quote_tightness"]["one_tick_pct"])
    print("gap median ms", out["gap_ms"]["median"], "over 1000ms", out["gap_ms"]["over_1000ms"])
    print("absent clocks", out["clock_columns"]["absent_from_parquet_schema"])
    print("wrote outputs/tables/F_binance_eth.json")


if __name__ == "__main__":
    main()
