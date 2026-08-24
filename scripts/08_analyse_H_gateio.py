"""Gate.io BTCUSDT perp volume + public candle comparison."""
from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from lotech_dq.checks import duplicate_rows, profile_frame  # noqa: E402
from lotech_dq.clocks import all_skews  # noqa: E402
from lotech_dq.io import ensure_dirs, load_parquet  # noqa: E402
from lotech_dq.report import write_table_json  # noqa: E402
from lotech_dq.volume import compute_volumes, fetch_gate_candle, select_static_row  # noqa: E402


def main() -> None:
    ensure_dirs()
    trades = load_parquet("H_trades")
    static = load_parquet("H_static")

    # Timestamps may be int epoch us
    from lotech_dq.clocks import ensure_datetime

    for c in ("ingress_ts", "transaction_ts", "publish_ts"):
        trades = ensure_datetime(trades, c)
    trades = trades.sort("transaction_ts")

    profile = profile_frame(trades, "H_gateio_btcusdt_perp_trades.parquet")
    for f in all_skews(trades, profile["file"]):
        profile["findings"].append(f.to_dict())
    dup = duplicate_rows(trades, ["trade_id"])
    if dup:
        dup.file = profile["file"]
        profile["findings"].append(dup.to_dict())

    static_row = select_static_row(static)
    mult = float(static_row.get("quantity_multiplier") or 1.0)
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

    out = {
        "window": {
            "transaction_ts_min": str(t_min),
            "transaction_ts_max": str(t_max),
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
            )
            if k in static_row
        },
        "volumes": volumes,
        "public_compare": public,
        "side_counts": side_counts,
        "neg_qty": neg_qty,
        "zero_qty": zero_qty,
        "profile_findings": profile["findings"],
    }
    write_table_json("H_gateio_volume.json", out)
    print("volumes:", volumes)
    print("public:", {k: public[k] for k in public if k != "candles"})
    print("wrote outputs/tables/H_gateio_volume.json")


if __name__ == "__main__":
    main()
