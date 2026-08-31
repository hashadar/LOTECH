"""Gate.io BTCUSDT perp volume + public candle comparison."""
from __future__ import annotations

import sys
from pathlib import Path

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from lotech_dq.checks import EXPECTED_COLUMNS, profile_frame  # noqa: E402
from lotech_dq.clocks import all_skews, ensure_datetime  # noqa: E402
from lotech_dq.io import data_path, ensure_dirs, load_parquet  # noqa: E402
from lotech_dq.report import write_table_json  # noqa: E402
from lotech_dq.volume import compute_volumes, fetch_gate_candle, select_static_row  # noqa: E402


def static_clocks(static: pl.DataFrame) -> dict:
    """Absent versus present-and-null, stated separately.

    A null-rate alert can only fire on a column that exists, so the two failure modes
    need different checks and the clocks table has to distinguish them.
    """
    schema_cols = pl.scan_parquet(data_path("H_static")).collect_schema().names()
    per_column = {}
    for c in EXPECTED_COLUMNS:
        if c not in schema_cols:
            per_column[c] = {"status": "absent", "dtype": None, "nulls": None, "null_pct": None}
            continue
        nulls = int(static[c].null_count())
        per_column[c] = {
            "status": "present_and_all_null" if nulls == static.height else "present",
            "dtype": str(static.schema[c]),
            "nulls": nulls,
            "null_pct": nulls / static.height * 100.0 if static.height else 0.0,
        }
    return {"rows": static.height, "parquet_schema_columns": schema_cols, "clocks": per_column}


def main() -> None:
    ensure_dirs()
    trades = load_parquet("H_trades")
    static = load_parquet("H_static")

    for c in ("ingress_ts", "transaction_ts", "publish_ts"):
        trades = ensure_datetime(trades, c)
    trades = trades.sort("transaction_ts")

    profile = profile_frame(trades, "H_gateio_btcusdt_perp_trades.parquet")
    for f in all_skews(trades, profile["file"]):
        profile["findings"].append(f.to_dict())
    static_profile = profile_frame(static, "H_gateio_btcusdt_perp_instrument_static.parquet")

    static_row = select_static_row(static)
    raw_mult = static_row.get("quantity_multiplier")
    mult = 1.0 if raw_mult is None else float(raw_mult)
    volumes = compute_volumes(trades, mult)

    t_min = trades.select(pl.col("transaction_ts").min()).item()
    t_max = trades.select(pl.col("transaction_ts").max()).item()
    from_s = int(trades.select(pl.col("transaction_ts").dt.epoch("s").min()).item())
    to_s = int(trades.select(pl.col("transaction_ts").dt.epoch("s").max()).item())
    # align to hour buckets covering the window
    hour_from = from_s - (from_s % 3600)
    hour_to = to_s

    candles = []
    candle_error = None
    try:
        candles = fetch_gate_candle(
            contract=str(static_row.get("exchange_symbol") or "BTC_USDT"),
            from_s=hour_from,
            to_s=hour_to,
            interval="1h",
        )
    except Exception as exc:  # noqa: BLE001
        candle_error = str(exc)

    # Gate docs: v = contract volume, sum = quote volume
    public = {"candles": candles, "error": candle_error}
    if candles:
        # sum across returned candles that overlap window
        v_contracts = 0.0
        v_quote = 0.0
        for c in candles:
            # object form uses string keys
            v = c.get("v", c.get("volume"))
            s = c.get("sum")
            if v is not None:
                v_contracts += float(v)
            if s is not None:
                v_quote += float(s)
        public["vol_contracts"] = v_contracts
        public["vol_quote"] = v_quote
        public["vol_contracts_diff"] = volumes["vol_contracts"] - v_contracts
        public["vol_quote_diff"] = volumes["vol_quote"] - v_quote
        if v_contracts:
            public["vol_contracts_pct_diff"] = (
                (volumes["vol_contracts"] - v_contracts) / v_contracts * 100.0
            )
        if v_quote:
            public["vol_quote_pct_diff"] = (
                (volumes["vol_quote"] - v_quote) / v_quote * 100.0
            )

    # side / qty sanity
    side_counts = trades["side"].value_counts().to_dicts() if "side" in trades.columns else []
    neg_qty = int(trades.filter(pl.col("qty") < 0).height)
    zero_qty = int(trades.filter(pl.col("qty") == 0).height)
    by_side = (
        trades.group_by("side")
        .agg(
            pl.col("qty").sum().alias("contracts"),
            (pl.col("qty") * mult * pl.col("price")).sum().alias("quote"),
            pl.len().alias("trades"),
        )
        .sort("side")
        .to_dicts()
    )

    gaps = trades.select(
        (pl.col("transaction_ts").diff().dt.total_microseconds() / 1e6).alias("gap_s")
    ).drop_nulls()

    out = {
        "window": {
            "transaction_ts_min": str(t_min),
            "transaction_ts_max": str(t_max),
            "ingress_min": str(trades["ingress_ts"].min()),
            "ingress_max": str(trades["ingress_ts"].max()),
            "from_s": from_s,
            "to_s": to_s,
            "hour_from": hour_from,
            "hour_to": hour_to,
        },
        "static": {
            k: static_row[k]
            for k in (
                "instrument",
                "exchange_symbol",
                "quantity_multiplier",
                "scale",
                "qty_step_size",
                "price_tick_size",
                "trading_state",
            )
            if k in static_row
        },
        "static_dtypes": {
            k: str(static.schema[k])
            for k in ("quantity_multiplier", "price_tick_size", "qty_step_size", "scale")
            if k in static.schema
        },
        "static_clocks": static_clocks(static),
        "static_findings": static_profile["findings"],
        "volumes": volumes,
        "volume_by_side": by_side,
        "public_compare": public,
        "side_counts": side_counts,
        "neg_qty": neg_qty,
        "zero_qty": zero_qty,
        "qty_all_integral": bool(
            trades.select((pl.col("qty") == pl.col("qty").round(0)).all()).item()
        ),
        "trade_id": {
            "rows": trades.height,
            "distinct": int(trades["trade_id"].n_unique()),
            "duplicate_groups": int(
                trades.group_by("trade_id").len().filter(pl.col("len") > 1).height
            ),
        },
        "gap_s": {
            "median": float(gaps["gap_s"].median()),
            "max": float(gaps["gap_s"].max()),
            "over_60s": int(gaps.filter(pl.col("gap_s") > 60).height),
        },
        "profile_findings": profile["findings"],
    }
    write_table_json("H_gateio_volume.json", out)
    print("volumes:", volumes)
    print("public:", {k: public[k] for k in public if k != "candles"})
    if candle_error:
        print("CANDLE FETCH FAILED, reconciliation not performed:", candle_error)
    print("static clocks:", {k: v["status"] for k, v in out["static_clocks"]["clocks"].items()})
    print("wrote outputs/tables/H_gateio_volume.json")


if __name__ == "__main__":
    main()
