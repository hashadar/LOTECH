"""HKEX 2800 TOB + trades DQ and cross-file consistency."""
from __future__ import annotations

import sys
from pathlib import Path

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from lotech_dq.checks import duplicate_rows, profile_frame, tob_crossed_locked  # noqa: E402
from lotech_dq.clocks import all_skews  # noqa: E402
from lotech_dq.io import ensure_dirs, load_parquet  # noqa: E402
from lotech_dq.report import write_table_json  # noqa: E402


def main() -> None:
    ensure_dirs()
    tob = load_parquet("A_tob").sort("transaction_ts")
    trades = load_parquet("A_trades").sort("transaction_ts")

    findings = []
    for label, df in (
        ("A_hkex_2800_top_of_book.parquet", tob),
        ("A_hkex_2800_trades.parquet", trades),
    ):
        p = profile_frame(df, label)
        findings.extend(p["findings"])
        for f in all_skews(df, label):
            findings.append(f.to_dict())

    dup = duplicate_rows(trades, ["trade_id"])
    if dup:
        dup.file = "A_hkex_2800_trades.parquet"
        findings.append(dup.to_dict())

    # session / coverage
    null_tx = int(trades.filter(pl.col("transaction_ts").is_null()).height)
    coverage = {
        "tob_tx_min": str(tob["transaction_ts"].min()),
        "tob_tx_max": str(tob["transaction_ts"].max()),
        "trades_tx_min": str(trades["transaction_ts"].drop_nulls().min()),
        "trades_tx_max": str(trades["transaction_ts"].drop_nulls().max()),
        "tob_rows": tob.height,
        "trade_rows": trades.height,
        "trades_null_transaction_ts": null_tx,
        "instruments_tob": tob["instrument"].unique().to_list(),
        "instruments_trades": trades["instrument"].unique().to_list(),
    }

    # asof join trade vs prevailing quote
    tob_key = tob.select(
        [
            pl.col("transaction_ts"),
            pl.col("bid_price"),
            pl.col("ask_price"),
            pl.col("bid_qty"),
            pl.col("ask_qty"),
        ]
    )
    joined = trades.join_asof(
        tob_key,
        on="transaction_ts",
        strategy="backward",
    ).with_columns(
        [
            (pl.col("price") > pl.col("ask_price")).alias("above_ask"),
            (pl.col("price") < pl.col("bid_price")).alias("below_bid"),
            (
                (pl.col("price") >= pl.col("bid_price"))
                & (pl.col("price") <= pl.col("ask_price"))
            ).alias("inside_spread"),
        ]
    )
    matched = joined.filter(pl.col("bid_price").is_not_null())
    trade_through = matched.filter(pl.col("above_ask") | pl.col("below_bid"))

    # hour-of-day activity (HKT = UTC+8)
    by_hour = (
        trades.with_columns(pl.col("transaction_ts").dt.hour().alias("utc_hour"))
        .group_by("utc_hour")
        .agg(pl.len().alias("n_trades"))
        .sort("utc_hour")
        .to_dicts()
    )

    # large TOB gaps
    tob_gaps = tob.select(
        pl.col("transaction_ts").diff().dt.total_seconds().alias("gap_s")
    ).drop_nulls()
    gap_summary = {
        "median_s": float(tob_gaps["gap_s"].median()),  # type: ignore[arg-type]
        "p99_s": float(tob_gaps.select(pl.col("gap_s").quantile(0.99)).item()),
        "max_s": float(tob_gaps["gap_s"].max()),
        "gaps_over_60s": int(tob_gaps.filter(pl.col("gap_s") > 60).height),
        "gaps_over_300s": int(tob_gaps.filter(pl.col("gap_s") > 300).height),
    }

    crossed = [f.to_dict() for f in tob_crossed_locked(tob)]

    out = {
        "coverage": coverage,
        "gap_summary": gap_summary,
        "crossed_locked": crossed,
        "trade_vs_tob": {
            "matched_trades": matched.height,
            "inside_spread": int(matched.filter(pl.col("inside_spread")).height),
            "above_ask": int(matched.filter(pl.col("above_ask")).height),
            "below_bid": int(matched.filter(pl.col("below_bid")).height),
            "trade_through": trade_through.height,
            "trade_through_pct": trade_through.height / max(matched.height, 1) * 100.0,
        },
        "trades_by_utc_hour": by_hour,
        "side_counts": trades["side"].value_counts().to_dicts(),
        "findings": findings,
    }
    write_table_json("A_hkex.json", out)
    print("trade_through_pct", out["trade_vs_tob"]["trade_through_pct"])
    print("gaps_over_300s", gap_summary["gaps_over_300s"])
    print("wrote outputs/tables/A_hkex.json")


if __name__ == "__main__":
    main()
