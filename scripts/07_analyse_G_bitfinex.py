"""Bitfinex BTCUSD trades DQ, reconciled against the venue's own public tape."""
from __future__ import annotations

import argparse
import statistics as st
import sys
from decimal import Decimal
from pathlib import Path

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from lotech_dq import bitfinex  # noqa: E402
from lotech_dq.checks import monotonic_backwards, profile_frame  # noqa: E402
from lotech_dq.clocks import all_skews, ensure_datetime  # noqa: E402
from lotech_dq.io import ensure_dirs, load_parquet  # noqa: E402
from lotech_dq.report import write_table_json  # noqa: E402
from lotech_dq.volume import exact_dot, exact_sum  # noqa: E402

CLOCKS = ("ingress_ts", "transaction_ts", "publish_ts")

SYMBOL = "tBTCUSD"
TIMEFRAME = "1m"
TIMEFRAME_MS = 60_000
PAD_MS = 1_000  # the query window is inclusive; a second either side cannot lose a boundary trade
WIDE_PAD_MS = 60_000
TRADES_LIMIT = 1000
CANDLES_LIMIT = 20


def _us(df: pl.DataFrame, col: str) -> pl.Expr:
    """Epoch microseconds, whether the column is stored as a datetime or a raw integer."""
    if isinstance(df.schema[col], pl.Datetime):
        return pl.col(col).dt.timestamp("us")
    return pl.col(col).cast(pl.Int64)


def _amt(value: Decimal) -> str:
    """Eight decimal places, the precision both the venue and the file quote to."""
    return f"{value:.8f}"


def _diff(ours: Decimal, venue: Decimal) -> str:
    """Unrounded, so a residual at the ninth decimal cannot be formatted out of sight."""
    delta = ours - venue
    return "0" if delta == 0 else str(delta)


def ordering_matrix(df: pl.DataFrame) -> dict:
    """Backward steps under each candidate stored order.

    Which clock the file is sorted by decides which clock looks broken. The claim is
    only meaningful next to the ordering it assumes.
    """
    def _backward(frame: pl.DataFrame, col: str) -> int:
        f = monotonic_backwards(frame, col)
        return int(f.metric["backward_jumps"]) if f else 0

    out: dict[str, dict[str, int]] = {}
    for order in ("file", *CLOCKS):
        frame = df if order == "file" else df.sort(order)
        out[f"sorted_by_{order}"] = {c: _backward(frame, c) for c in CLOCKS}
    return out


def duplicate_anatomy(df: pl.DataFrame) -> dict:
    """What varies inside a duplicate trade_id group tells you which stage duplicated it."""
    g = df.group_by("trade_id").agg(
        pl.len().alias("n"),
        pl.col("price").n_unique().alias("n_price"),
        pl.col("qty").n_unique().alias("n_qty"),
        pl.col("side").n_unique().alias("n_side"),
        pl.col("transaction_ts").n_unique().alias("n_tx"),
        pl.col("publish_ts").n_unique().alias("n_pub"),
        pl.col("ingress_ts").n_unique().alias("n_ing"),
        (pl.col("ingress_ts").max() - pl.col("ingress_ts").min()).alias("ing_span"),
        (pl.col("publish_ts").max() - pl.col("publish_ts").min()).alias("pub_span"),
    )
    dups = g.filter(pl.col("n") > 1)
    ing_ms = dups.select(pl.col("ing_span").dt.total_microseconds() / 1000.0)["ing_span"]
    pub_ms = dups.select(pl.col("pub_span").dt.total_microseconds() / 1000.0)["pub_span"]
    return {
        "distinct_trade_ids": int(df["trade_id"].n_unique()),
        "duplicate_groups": dups.height,
        "excess_rows": int(dups.select((pl.col("n") - 1).sum()).item()),
        "group_sizes": dups["n"].value_counts().sort("n").to_dicts(),
        "groups_with_identical_price": int(dups.filter(pl.col("n_price") == 1).height),
        "groups_with_identical_qty": int(dups.filter(pl.col("n_qty") == 1).height),
        "groups_with_identical_side": int(dups.filter(pl.col("n_side") == 1).height),
        "groups_with_identical_transaction_ts": int(dups.filter(pl.col("n_tx") == 1).height),
        "groups_with_identical_publish_ts": int(dups.filter(pl.col("n_pub") == 1).height),
        "groups_with_identical_ingress_ts": int(dups.filter(pl.col("n_ing") == 1).height),
        "pair_separation_ms": {
            "ingress_median": float(ing_ms.median()),
            "ingress_min": float(ing_ms.min()),
            "ingress_max": float(ing_ms.max()),
            "publish_median": float(pub_ms.median()),
            "publish_min": float(pub_ms.min()),
            "publish_max": float(pub_ms.max()),
        },
        "verdict": (
            "every group carries identical economics and identical venue time but two "
            "distinct publish_ts and ingress_ts, so the venue republished; it did not retrade"
        ),
    }


def duplicate_pair_timing(df: pl.DataFrame) -> dict:
    """Which clock the two copies separate on, and what that excludes.

    publish_ts is the venue's stamp and ingress_ts is ours, so (ingress - publish) isolates
    our capture latency. If the second copy arrived down a different capture path, those two
    latencies differ. The remaining tests are the shapes each alternative mechanism would
    have to produce. A flush timer is flat and grid-aligned. A replay is confined to one
    stretch of the window. Queueing scales with trade rate.
    """
    us = df.select(
        pl.col("trade_id").cast(pl.Utf8),
        _us(df, "transaction_ts").alias("tx"),
        _us(df, "publish_ts").alias("pub"),
        _us(df, "ingress_ts").alias("ing"),
    )
    pairs = (
        us.sort(["trade_id", "pub"])
        .group_by("trade_id")
        .agg(
            pl.col("pub").min().alias("pub1"),
            pl.col("pub").max().alias("pub2"),
            pl.col("ing").min().alias("ing1"),
            pl.col("ing").max().alias("ing2"),
            pl.col("tx").first().alias("tx"),
        )
        .with_columns(
            ((pl.col("pub2") - pl.col("pub1")) / 1000.0).alias("pub_lag_ms"),
            ((pl.col("ing2") - pl.col("ing1")) / 1000.0).alias("ing_lag_ms"),
            ((pl.col("pub1") - pl.col("tx")) / 1000.0).alias("pub1_minus_tx_ms"),
            ((pl.col("pub2") - pl.col("tx")) / 1000.0).alias("pub2_minus_tx_ms"),
            ((pl.col("ing1") - pl.col("pub1")) / 1000.0).alias("cap_lag1_ms"),
            ((pl.col("ing2") - pl.col("pub2")) / 1000.0).alias("cap_lag2_ms"),
        )
        .sort(["tx", "pub1"])
    )

    quantities = {}
    for col in (
        "pub_lag_ms",
        "ing_lag_ms",
        "pub1_minus_tx_ms",
        "pub2_minus_tx_ms",
        "cap_lag1_ms",
        "cap_lag2_ms",
    ):
        s = pairs[col]
        quantities[col] = {
            "min": float(s.min()),  # type: ignore[arg-type]
            "p25": float(s.quantile(0.25)),  # type: ignore[arg-type]
            "median": float(s.median()),  # type: ignore[arg-type]
            "p75": float(s.quantile(0.75)),  # type: ignore[arg-type]
            "max": float(s.max()),  # type: ignore[arg-type]
            "sd": float(s.std()),  # type: ignore[arg-type]
        }

    lag = sorted(pairs["pub_lag_ms"].to_list())
    lo, hi = lag[0], lag[-1]
    pstdev = st.pstdev(lag)
    uniform_sd = (hi - lo) / 12**0.5
    shape = {
        "n": len(lag),
        "support_min_ms": lo,
        "support_max_ms": hi,
        "mean_ms": st.mean(lag),
        "median_ms": st.median(lag),
        "sd_population_ms": pstdev,
        "sd_sample_ms": st.stdev(lag),
        "uniform_sd_over_same_support_ms": uniform_sd,
        "sd_ratio_vs_uniform": pstdev / uniform_sd,
        "pearson_skew": 3 * (st.mean(lag) - st.median(lag)) / pstdev,
        "deciles_observed": [lag[min(int(len(lag) * k / 10), len(lag) - 1)] for k in range(11)],
        "deciles_if_uniform": [lo + (hi - lo) * k / 10 for k in range(11)],
        "histogram_5ms": [
            {"lo": b, "hi": b + 5, "n": sum(1 for x in lag if b <= x < b + 5)}
            for b in range(10, 110, 5)
        ],
    }

    grid = {}
    for n in (5, 10, 20, 25, 50, 100):
        grid[f"mod_{n}ms"] = {
            "possible_residues": n,
            "pub1_residues_used": len({int(x % (n * 1000)) // 1000 for x in pairs["pub1"]}),
            "pub2_residues_used": len({int(x % (n * 1000)) // 1000 for x in pairs["pub2"]}),
        }

    first_minute = int(pairs["tx"].min()) // (TIMEFRAME_MS * 1000)
    by_minute = (
        pairs.with_columns(
            (pl.col("tx") // (TIMEFRAME_MS * 1000) - first_minute).alias("minute")
        )
        .group_by("minute")
        .agg(
            pl.len().alias("pairs"),
            pl.col("pub_lag_ms").min().alias("min_lag_ms"),
            pl.col("pub_lag_ms").median().alias("median_lag_ms"),
            pl.col("pub_lag_ms").max().alias("max_lag_ms"),
        )
        .sort("minute")
    )

    tx_all = sorted(pairs["tx"].to_list())
    busy = [sum(1 for u in tx_all if t - 1_000_000 <= u <= t) for t in pairs["tx"].to_list()]
    corr_frame = pl.DataFrame(
        {
            "lag": pairs["pub_lag_ms"],
            "ing_lag": pairs["ing_lag_ms"],
            "position": pl.Series(range(pairs.height), dtype=pl.Float64),
            "busy": pl.Series(busy, dtype=pl.Float64),
        }
    )
    by_pub1 = pairs.sort("pub1")

    return {
        "pairs": pairs.height,
        "quantities_ms": quantities,
        "capture_path": {
            "cap_lag1_median_ms": quantities["cap_lag1_ms"]["median"],
            "cap_lag2_median_ms": quantities["cap_lag2_ms"]["median"],
            "median_difference_ms": round(
                quantities["cap_lag2_ms"]["median"] - quantities["cap_lag1_ms"]["median"], 3
            ),
        },
        "publish_lag_shape": shape,
        "grid_snapping": grid,
        "coverage_by_minute": by_minute.to_dicts(),
        "minutes_covered": by_minute.height,
        "minutes_in_window": int(pairs["tx"].max()) // (TIMEFRAME_MS * 1000) - first_minute + 1,
        "queueing": {
            "trades_in_preceding_1s_min": min(busy),
            "trades_in_preceding_1s_median": st.median(busy),
            "trades_in_preceding_1s_max": max(busy),
            "corr_pub_lag_vs_trade_rate": float(corr_frame.select(pl.corr("lag", "busy")).item()),
        },
        "corr_pub_lag_vs_position": float(corr_frame.select(pl.corr("lag", "position")).item()),
        "corr_pub_lag_vs_ingress_lag": float(corr_frame.select(pl.corr("lag", "ing_lag")).item()),
        "first_copy": {
            "pub1_minus_tx_ms_counts": pairs.select(pl.col("pub1_minus_tx_ms").cast(pl.Int64))
            .to_series()
            .value_counts()
            .sort("pub1_minus_tx_ms")
            .to_dicts(),
            "pub2_minus_tx_min_ms": quantities["pub2_minus_tx_ms"]["min"],
            "second_copies_before_match": int(
                pairs.filter(pl.col("pub2_minus_tx_ms") < 0).height
            ),
        },
        "interleaving": {
            "consecutive_pairs": pairs.height - 1,
            "second_copy_after_next_first_copy": int(
                by_pub1.select((pl.col("pub1").shift(-1) < pl.col("pub2")).alias("ov"))
                .filter(pl.col("ov"))
                .height
            ),
            "pub2_backward_steps_ordered_by_pub1": int(
                by_pub1.select(pl.col("pub2").diff().alias("d")).filter(pl.col("d") < 0).height
            ),
            "ing2_backward_steps_ordered_by_ing1": int(
                pairs.sort("ing1")
                .select(pl.col("ing2").diff().alias("d"))
                .filter(pl.col("d") < 0)
                .height
            ),
        },
        "verdict": (
            "the two copies separate on the venue's publish clock, not on ours: capture "
            "latency is the same on both, so the venue emitted the trade twice and a single "
            "capture path recorded both. REST exposes only the final persisted record. The "
            "original live frames cannot be observed on that endpoint. The timing identifies "
            "a venue republish"
        ),
    }


def public_reconciliation(df: pl.DataFrame, deduped: pl.DataFrame) -> dict:
    """Difference the file against Bitfinex's own tape over the same window.

    The trade fetch is a minute wider than the file window and clipped locally.
    Neither a `limit` cap nor an off-by-one on start/end can produce false agreement.
    The 1m candles are an independently aggregated second opinion on the same volume.
    """
    tx_us = df.select(_us(df, "transaction_ts").alias("tx"))["tx"]
    win_lo = int(tx_us.min()) // 1000  # type: ignore[arg-type]
    win_hi = int(tx_us.max()) // 1000  # type: ignore[arg-type]
    log: list[dict] = []

    narrow = bitfinex.fetch_trades(
        SYMBOL, win_lo - PAD_MS, win_hi + PAD_MS, limit=TRADES_LIMIT, log=log
    )
    candles = bitfinex.fetch_candles(
        SYMBOL,
        win_lo - PAD_MS,
        win_hi + PAD_MS,
        timeframe=TIMEFRAME,
        limit=CANDLES_LIMIT,
        log=log,
    )
    wide = bitfinex.fetch_trades(
        SYMBOL, win_lo - WIDE_PAD_MS, win_hi + WIDE_PAD_MS, limit=TRADES_LIMIT, log=log
    )
    if not wide or not candles:
        raise bitfinex.BitfinexError(
            f"{SYMBOL} returned no history for {win_lo}-{win_hi} ms; there is nothing to "
            "reconcile against and no figure may be reported"
        )

    pub = pl.DataFrame(
        {
            "trade_id": [str(t[0]) for t in wide],
            "mts": [int(t[1]) for t in wide],
            "amount": [t[2] for t in wide],
            "px": [t[3] for t in wide],
        }
    )
    clip = pub.filter((pl.col("mts") >= win_lo) & (pl.col("mts") <= win_hi))
    narrow_ids = {str(t[0]) for t in narrow if win_lo <= int(t[1]) <= win_hi}

    span_lo = int(candles[0][0])
    span_hi = int(candles[-1][0]) + TIMEFRAME_MS
    edge = pub.filter(
        ((pl.col("mts") >= span_lo) & (pl.col("mts") < win_lo))
        | ((pl.col("mts") > win_hi) & (pl.col("mts") < span_hi))
    )

    file_ids = set(df["trade_id"].cast(pl.Utf8).to_list())
    venue_ids = set(clip["trade_id"].to_list())

    matched = (
        deduped.select(
            pl.col("trade_id").cast(pl.Utf8),
            "price",
            "qty",
            pl.col("side").cast(pl.Utf8).str.to_lowercase().alias("side"),
            _us(deduped, "transaction_ts").alias("tx_us"),
        )
        .join(clip, on="trade_id", how="inner")
        .with_columns(
            (pl.col("price") - pl.col("px")).abs().alias("d_px"),
            (pl.col("qty") - pl.col("amount").abs()).abs().alias("d_qty"),
            pl.when(pl.col("amount") < 0)
            .then(pl.lit("sell"))
            .otherwise(pl.lit("buy"))
            .alias("pub_side"),
            ((pl.col("tx_us") // 1000) - pl.col("mts")).alias("d_ms"),
        )
    )

    venue_base = exact_sum(abs(a) for a in clip["amount"])
    venue_signed = exact_sum(clip["amount"])
    venue_notional = exact_dot((abs(a) for a in clip["amount"]), clip["px"])
    venue_candle_base = exact_sum(c[5] for c in candles)

    ours_base = exact_sum(deduped["qty"])
    ours_signed = exact_sum(
        q if s.lower() == "buy" else -q
        for q, s in zip(deduped["qty"], deduped["side"].cast(pl.Utf8), strict=True)
    )
    ours_notional = exact_dot(deduped["qty"], deduped["price"])
    delivered_base = exact_sum(df["qty"])

    diffs = {
        "trades_diff": deduped.height - clip.height,
        "vol_base_diff": _diff(ours_base, venue_base),
        "vol_base_signed_diff": _diff(ours_signed, venue_signed),
        "notional_quote_diff": _diff(ours_notional, venue_notional),
        "candle_vol_base_diff": _diff(venue_candle_base, venue_base),
    }
    all_zero = (
        diffs["trades_diff"] == 0
        and ours_base == venue_base
        and ours_signed == venue_signed
        and ours_notional == venue_notional
        and venue_candle_base == venue_base
    )

    return {
        "venue": "bitfinex",
        "symbol": SYMBOL,
        "endpoints": {
            "trades": bitfinex.trades_url(SYMBOL),
            "candles": bitfinex.candles_url(SYMBOL, TIMEFRAME),
        },
        "query_ms": {
            "window_start": win_lo,
            "window_end": win_hi,
            "padded_start": win_lo - PAD_MS,
            "padded_end": win_hi + PAD_MS,
            "wide_start": win_lo - WIDE_PAD_MS,
            "wide_end": win_hi + WIDE_PAD_MS,
        },
        "requests": log,
        "error": None,
        "venue_trades": clip.height,
        "venue_vol_base": _amt(venue_base),
        "venue_vol_base_signed": _amt(venue_signed),
        "venue_notional_quote": _amt(venue_notional),
        "venue_candle_vol_base": _amt(venue_candle_base),
        "trades": deduped.height,
        "vol_base": _amt(ours_base),
        "vol_base_signed": _amt(ours_signed),
        "notional_quote": _amt(ours_notional),
        **diffs,
        "all_diffs_zero": bool(all_zero),
        "as_delivered": {
            "trades": df.height,
            "trades_diff": df.height - clip.height,
            "vol_base": _amt(delivered_base),
            "vol_base_diff": _diff(delivered_base, venue_base),
            "vol_base_pct_diff": float((delivered_base - venue_base) / venue_base * 100),
            "overstatement_factor": float(delivered_base / venue_base),
        },
        "id_sets": {
            "file_distinct": len(file_ids),
            "venue": len(venue_ids),
            "file_only": len(file_ids - venue_ids),
            "venue_only": len(venue_ids - file_ids),
            "identical": file_ids == venue_ids,
        },
        "per_trade": {
            "matched": matched.height,
            "price_exact": int(matched.filter(pl.col("d_px") == 0).height),
            "price_max_abs_diff": float(matched["d_px"].max()),  # type: ignore[arg-type]
            "qty_exact": int(matched.filter(pl.col("d_qty") == 0).height),
            "qty_max_abs_diff": float(matched["d_qty"].max()),  # type: ignore[arg-type]
            "side_agree": int(matched.filter(pl.col("side") == pl.col("pub_side")).height),
        },
        "sign_convention": {
            "cross_tab": matched.group_by("side", "pub_side").len().sort("side").to_dicts(),
            "venue_positive_amount": int(clip.filter(pl.col("amount") > 0).height),
            "venue_negative_amount": int(clip.filter(pl.col("amount") < 0).height),
            "venue_zero_amount": int(clip.filter(pl.col("amount") == 0).height),
        },
        "transaction_ts_vs_venue_mts": {
            "identical": int(matched.filter(pl.col("d_ms") == 0).height),
            "file_earlier_by_1ms": int(matched.filter(pl.col("d_ms") == -1).height),
            "file_later": int(matched.filter(pl.col("d_ms") > 0).height),
            "max_abs_diff_ms": int(matched["d_ms"].abs().max()),  # type: ignore[arg-type]
            "distribution_ms": matched["d_ms"].value_counts().sort("d_ms").to_dicts(),
        },
        "guards": {
            "wide_rows": pub.height,
            "wide_before_window": int(pub.filter(pl.col("mts") < win_lo).height),
            "wide_after_window": int(pub.filter(pl.col("mts") > win_hi).height),
            "wide_inside_window": clip.height,
            "limit_cap_hit": any(
                (r.get("rows") or 0) >= TRADES_LIMIT for r in log if "trades" in r["url"]
            ),
            "venue_duplicate_trade_ids": clip.height - clip["trade_id"].n_unique(),
            "narrow_query_rows": len(narrow),
            "narrow_agrees_with_clipped_wide": narrow_ids == venue_ids,
            "candles": len(candles),
            "candle_span_start_ms": span_lo,
            "candle_span_end_ms": span_hi,
            "venue_trades_in_candle_span_outside_window": edge.height,
            "venue_volume_in_candle_span_outside_window": _amt(
                exact_sum(abs(a) for a in edge["amount"])
            ),
        },
        "units": {
            "vol_base": "BTC: venue sum(abs(AMOUNT)), file sum(qty) deduplicated on trade_id",
            "vol_base_signed": "BTC: venue sum(AMOUNT), file sum(qty) signed by side",
            "notional_quote": "USD: sum(abs(AMOUNT) * PRICE)",
            "candle_vol_base": f"BTC: sum of {TIMEFRAME} candle VOLUME over the candle span",
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--skip-public",
        action="store_true",
        help="regenerate local metrics only; the venue reconciliation is omitted, not estimated",
    )
    args = parser.parse_args()

    ensure_dirs()
    df = load_parquet("G")
    for c in CLOCKS:
        df = ensure_datetime(df, c)
    profile = profile_frame(df, "G_bitfinex_btcusd_trades.parquet")
    findings = list(profile["findings"])
    for f in all_skews(df, profile["file"]):
        findings.append(f.to_dict())
    tx_sorted = df.sort("transaction_ts")
    deduped = (
        df.sort(["trade_id", "ingress_ts"])
        .unique(subset=["trade_id"], keep="first")
        .sort("transaction_ts")
    )

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

    # dt.total_seconds() floors a Duration(us), which erases every sub-second gap
    def _gap_stats(frame: pl.DataFrame) -> dict:
        g = frame.select(
            (pl.col("transaction_ts").diff().dt.total_microseconds() / 1e6).alias("gap_s")
        ).drop_nulls()
        return {
            "median": float(g["gap_s"].median()),
            "p99": float(g.select(pl.col("gap_s").quantile(0.99)).item()),
            "max": float(g["gap_s"].max()),
            "zero_gaps": int(g.filter(pl.col("gap_s") == 0).height),
            "over_60s": int(g.filter(pl.col("gap_s") > 60).height),
        }

    if args.skip_public:
        public = {
            "venue": "bitfinex",
            "symbol": SYMBOL,
            "status": "skipped",
            "error": "run without --skip-public to reconcile against the venue",
        }
        print("PUBLIC RECONCILIATION SKIPPED BY --skip-public: no venue figures were computed")
    else:
        try:
            public = public_reconciliation(df, deduped)
        except bitfinex.BitfinexError as exc:
            print(
                "PUBLIC RECONCILIATION FAILED, nothing written.\n"
                f"  {exc}\n"
                "  outputs/tables/G_bitfinex.json is unchanged. The venue figures cannot be "
                "estimated, so none are reported.\n"
                "  Re-run when the venue is reachable, or with --skip-public to regenerate "
                "the local metrics without the reconciliation.",
                file=sys.stderr,
            )
            raise SystemExit(1) from exc

    out = {
        "rows": df.height,
        "instrument": df["instrument"].unique().to_list(),
        "window": {
            "tx_min": str(tx_sorted["transaction_ts"].min()),
            "tx_max": str(tx_sorted["transaction_ts"].max()),
            "ingress_min": str(df["ingress_ts"].min()),
            "ingress_max": str(df["ingress_ts"].max()),
            "duration_s": float(
                (df["transaction_ts"].max() - df["transaction_ts"].min()).total_seconds()
            ),
        },
        "price": {
            "min": float(df["price"].min()),
            "max": float(df["price"].max()),
            "median": float(df["price"].median()),  # type: ignore[arg-type]
        },
        "qty_sign": qty_sign,
        "side_counts": side_counts,
        "side_qty_consistency": inconsistency,
        "volume": {
            "qty_sum_all_rows": float(df["qty"].sum()),
            "qty_sum_deduped": float(deduped["qty"].sum()),
            "overstatement_factor": float(df["qty"].sum()) / float(deduped["qty"].sum()),
            "qty_min": float(df["qty"].min()),
            "qty_max": float(df["qty"].max()),
        },
        "public_compare": public,
        "gap_s": _gap_stats(tx_sorted),
        "gap_s_deduped": _gap_stats(deduped),
        "duplicates": duplicate_anatomy(df),
        "mechanism": duplicate_pair_timing(df),
        "ordering": ordering_matrix(df),
        "clock_resolution": {
            c: int(
                df.select(
                    (pl.col(c).dt.timestamp("us") % 1000).n_unique()
                ).item()
            )
            for c in CLOCKS
        },
        "findings": findings,
    }
    write_table_json("G_bitfinex.json", out)
    print(out["qty_sign"], out["side_counts"])
    print("dedup", out["duplicates"]["duplicate_groups"], "excess", out["duplicates"]["excess_rows"])
    print("gap_s", out["gap_s"], "deduped", out["gap_s_deduped"])
    if "venue_trades" in public:
        print(
            "public:",
            {
                k: public[k]
                for k in (
                    "venue_trades",
                    "trades",
                    "venue_vol_base",
                    "vol_base",
                    "vol_base_diff",
                    "vol_base_signed_diff",
                    "notional_quote_diff",
                    "candle_vol_base_diff",
                    "all_diffs_zero",
                )
            },
        )
        print("as delivered:", public["as_delivered"])
        print("ids:", public["id_sets"], "per trade:", public["per_trade"])
        print("guards:", public["guards"])
        print("capture path:", out["mechanism"]["capture_path"])
    print("wrote outputs/tables/G_bitfinex.json")


if __name__ == "__main__":
    main()
