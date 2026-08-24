from __future__ import annotations

import polars as pl

# Microprice: (ask_size * bid_price + bid_size * ask_price) / (bid_size + ask_size)


def resolve_tob_cols(df: pl.DataFrame) -> dict[str, str]:
    """Map logical TOB fields to actual column names."""
    candidates = {
        "bid_price": ["bid_price", "bid_px", "best_bid", "bid"],
        "ask_price": ["ask_price", "ask_px", "best_ask", "ask"],
        "bid_size": ["bid_size", "bid_qty", "bid_sz", "bid_quantity", "bid_amount"],
        "ask_size": ["ask_size", "ask_qty", "ask_sz", "ask_quantity", "ask_amount"],
    }
    resolved: dict[str, str] = {}
    for logical, names in candidates.items():
        for n in names:
            if n in df.columns:
                resolved[logical] = n
                break
    return resolved


def add_microprice(df: pl.DataFrame) -> pl.DataFrame:
    cols = resolve_tob_cols(df)
    required = ("bid_price", "ask_price", "bid_size", "ask_size")
    missing = [k for k in required if k not in cols]
    if missing:
        raise ValueError(f"Missing TOB columns for microprice: {missing}; have {df.columns}")

    bp, ap = cols["bid_price"], cols["ask_price"]
    bs, as_ = cols["bid_size"], cols["ask_size"]

    out = df.with_columns(
        [
            ((pl.col(ap) + pl.col(bp)) / 2).alias("mid"),
            (pl.col(ap) - pl.col(bp)).alias("spread"),
            (pl.col(bs) + pl.col(as_)).alias("size_sum"),
            (
                (pl.col(as_) * pl.col(bp) + pl.col(bs) * pl.col(ap))
                / (pl.col(bs) + pl.col(as_))
            ).alias("microprice_raw"),
        ]
    ).with_columns(
        (
            pl.when(
                pl.col("size_sum").is_null()
                | (pl.col("size_sum") <= 0)
                | pl.col(bp).is_null()
                | pl.col(ap).is_null()
                | pl.col(bs).is_null()
                | pl.col(as_).is_null()
                | (pl.col(bp) > pl.col(ap))
            )
            .then(None)
            .otherwise(pl.col("microprice_raw"))
        ).alias("microprice")
    )
    return out.with_columns(
        pl.when(pl.col("microprice").is_null())
        .then(pl.lit("undefined"))
        .otherwise(pl.lit("ok"))
        .alias("microprice_status")
    )
