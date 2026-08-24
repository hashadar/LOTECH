"""Bitfinex BTCUSD trades quick DQ."""
from __future__ import annotations

import sys
from pathlib import Path

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from lotech_dq.checks import duplicate_rows, profile_frame  # noqa: E402
from lotech_dq.clocks import all_skews, ensure_datetime  # noqa: E402
from lotech_dq.io import ensure_dirs, load_parquet  # noqa: E402
from lotech_dq.report import write_table_json  # noqa: E402


def main() -> None:
    ensure_dirs()
    df = load_parquet("G")
    for c in ("ingress_ts", "transaction_ts", "publish_ts"):
        df = ensure_datetime(df, c)
    df = df.sort("transaction_ts")
    profile = profile_frame(df, "G_bitfinex_btcusd_trades.parquet")
    findings = list(profile["findings"])
    for f in all_skews(df, profile["file"]):
        findings.append(f.to_dict())
    dup = duplicate_rows(df, ["trade_id"])
    if dup:
        dup.file = profile["file"]
        findings.append(dup.to_dict())

    # Bitfinex often encodes side via signed amount; check qty sign vs side
    qty_sign = {
        "neg_qty": int(df.filter(pl.col("qty") < 0).height),
        "pos_qty": int(df.filter(pl.col("qty") > 0).height),
        "zero_qty": int(df.filter(pl.col("qty") == 0).height),
    }
    side_counts = df["side"].value_counts().to_dicts() if "side" in df.columns else []

    # side vs qty consistency if both present
    inconsistency = None
    if "side" in df.columns:
        # assume buy => qty>0, sell => qty>0 with side label (unsigned) OR sell => qty<0
        sides = {str(s).lower() for s in df["side"].unique().to_list()}
        inconsistency = {
            "unique_sides": sorted(sides),
            "sell_with_pos_qty": int(
                df.filter(
                    (pl.col("side").cast(pl.Utf8).str.to_lowercase().is_in(["sell", "s", "ask"]))
                    & (pl.col("qty") > 0)
                ).height
            ),
            "buy_with_neg_qty": int(
                df.filter(
                    (pl.col("side").cast(pl.Utf8).str.to_lowercase().is_in(["buy", "b", "bid"]))
                    & (pl.col("qty") < 0)
                ).height
            ),
        }

    gaps = df.select(
        pl.col("transaction_ts").diff().dt.total_seconds().alias("gap_s")
    ).drop_nulls()

    out = {
        "rows": df.height,
        "instrument": df["instrument"].unique().to_list(),
        "window": {
            "tx_min": str(df["transaction_ts"].min()),
            "tx_max": str(df["transaction_ts"].max()),
        },
        "price": {
            "min": float(df["price"].min()),
            "max": float(df["price"].max()),
            "median": float(df["price"].median()),  # type: ignore[arg-type]
        },
        "qty_sign": qty_sign,
        "side_counts": side_counts,
        "side_qty_consistency": inconsistency,
        "gap_s": {
            "median": float(gaps["gap_s"].median()) if gaps.height else None,  # type: ignore[arg-type]
            "max": float(gaps["gap_s"].max()) if gaps.height else None,
            "over_60s": int(gaps.filter(pl.col("gap_s") > 60).height) if gaps.height else 0,
        },
        "findings": findings,
    }
    write_table_json("G_bitfinex.json", out)
    print(out["qty_sign"], out["side_counts"])
    print("wrote outputs/tables/G_bitfinex.json")


if __name__ == "__main__":
    main()
