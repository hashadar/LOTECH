from __future__ import annotations

from collections.abc import Iterable
from decimal import Decimal
from typing import Any

import httpx
import polars as pl


def exact_sum(values: Iterable[Any]) -> Decimal:
    """Exact sum via each value's shortest round-trip decimal string.

    Float addition of a few hundred eight-decimal quantities loses the last digit, which is
    the digit at which a reconciliation has to demonstrate a difference of exactly zero.
    """
    total = Decimal(0)
    for value in values:
        total += Decimal(str(value))
    return total


def exact_dot(left: Iterable[Any], right: Iterable[Any]) -> Decimal:
    """Exact sum of pairwise products, for notional from quantity and price."""
    total = Decimal(0)
    for x, y in zip(left, right, strict=True):
        total += Decimal(str(x)) * Decimal(str(y))
    return total


def select_static_row(static: pl.DataFrame, asof_ts: int | None = None) -> dict[str, Any]:
    """Most recent applicable instrument-static row."""
    if static.height == 0:
        raise ValueError("Empty instrument static")
    df = static
    # prefer a validity / effective timestamp if present
    for col in ("effective_ts", "valid_from", "as_of", "ingress_ts", "publish_ts"):
        if col in df.columns:
            df = df.sort(col)
            if asof_ts is not None:
                df = df.filter(pl.col(col) <= asof_ts)
            break
    if df.is_empty():
        df = static
    row = df.tail(1).to_dicts()[0]
    mult = row.get("quantity_multiplier")
    if mult is None:
        row["quantity_multiplier"] = 1.0
    return row


def compute_volumes(trades: pl.DataFrame, quantity_multiplier: float) -> dict[str, Any]:
    """Native contracts, base-asset, and quote-currency volume.

    Units:
    - contracts: sum(qty) — native contract count
    - base: sum(qty * quantity_multiplier) — underlying base asset
    - quote: sum(qty * quantity_multiplier * price) — quote currency notional
    """
    qty_col = "qty" if "qty" in trades.columns else "quantity"
    px_col = "price" if "price" in trades.columns else None
    if px_col is None:
        for c in ("px", "trade_price"):
            if c in trades.columns:
                px_col = c
                break
    if px_col is None:
        raise ValueError(f"No price column in trades: {trades.columns}")

    mult = float(quantity_multiplier)
    work = trades.with_columns(
        [
            pl.col(qty_col).cast(pl.Float64).alias("_qty"),
            pl.col(px_col).cast(pl.Float64).alias("_px"),
            pl.lit(mult).alias("_mult"),
        ]
    ).with_columns(
        [
            (pl.col("_qty") * pl.col("_mult")).alias("_base"),
            (pl.col("_qty") * pl.col("_mult") * pl.col("_px")).alias("_quote"),
        ]
    )

    return {
        "qty_col": qty_col,
        "price_col": px_col,
        "quantity_multiplier": mult,
        "n_trades": work.height,
        "vol_contracts": float(work.select(pl.col("_qty").sum()).item()),
        "vol_base": float(work.select(pl.col("_base").sum()).item()),
        "vol_quote": float(work.select(pl.col("_quote").sum()).item()),
        "units": {
            "vol_contracts": "native contracts (sum of qty)",
            "vol_base": "base asset (qty * quantity_multiplier)",
            "vol_quote": "quote currency (qty * quantity_multiplier * price)",
        },
    }


def fetch_gate_candle(
    contract: str = "BTC_USDT",
    from_s: int | None = None,
    to_s: int | None = None,
    interval: str = "1h",
) -> list[dict[str, Any]]:
    """Fetch Gate.io USDT-margined futures candlesticks."""
    params: dict[str, Any] = {"contract": contract, "interval": interval}
    if from_s is not None:
        params["from"] = from_s
    if to_s is not None:
        params["to"] = to_s
    url = "https://api.gateio.ws/api/v4/futures/usdt/candlesticks"
    with httpx.Client(timeout=30.0) as client:
        r = client.get(url, params=params)
        r.raise_for_status()
        data = r.json()
    # Gate returns list of objects or arrays depending on version
    if not data:
        return []
    if isinstance(data[0], dict):
        return data
    # array form: [t, v, c, h, l, o, sum] per docs historically
    rows = []
    for row in data:
        rows.append(
            {
                "t": row[0],
                "v": row[1],
                "c": row[2],
                "h": row[3],
                "l": row[4],
                "o": row[5],
                "sum": row[6] if len(row) > 6 else None,
            }
        )
    return rows
