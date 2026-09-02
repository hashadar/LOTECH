"""Exhibits for WRITEUP.md: sample rows, localisation, ruled-out alternatives.

Every sample here is paired with the distribution it was drawn from. A `.head(n)`
on its own has already produced one false generalisation in this write-up.
"""
from __future__ import annotations

import sys
from itertools import pairwise
from pathlib import Path

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from lotech_dq.book import replay_order_book  # noqa: E402
from lotech_dq.clocks import ensure_datetime  # noqa: E402
from lotech_dq.io import ensure_dirs, load_parquet  # noqa: E402
from lotech_dq.microprice import add_microprice  # noqa: E402
from lotech_dq.report import write_table_json  # noqa: E402


def _ts(v) -> str:
    return str(v) if v is not None else ""


def _rows(df: pl.DataFrame) -> list[dict]:
    return [{k: _ts(v) if "ts" in k else v for k, v in r.items()} for r in df.to_dicts()]


def _dist(df: pl.DataFrame, col: str, top: int = 10) -> dict:
    """Value counts plus range, so a sample can never stand in for the population."""
    if col not in df.columns or df.height == 0:
        return {}
    vc = df[col].value_counts().sort("count", descending=True)
    s = df[col].drop_nulls()
    out: dict = {
        "n": df.height,
        "distinct": int(df[col].n_unique()),
        "top_values": [
            {"value": r[col], "count": r["count"], "pct": r["count"] / df.height * 100.0}
            for r in vc.head(top).to_dicts()
        ],
    }
    if s.len() and s.dtype.is_numeric():
        out |= {
            "min": float(s.min()),  # type: ignore[arg-type]
            "median": float(s.median()),  # type: ignore[arg-type]
            "max": float(s.max()),  # type: ignore[arg-type]
        }
    return out


def _gap_s(col: str) -> pl.Expr:
    """dt.total_seconds() floors a Duration(us); every sub-second gap becomes 0."""
    return pl.col(col).diff().dt.total_microseconds() / 1e6


def exhibits_a() -> dict:
    tob = load_parquet("A_tob")
    trades = load_parquet("A_trades")

    # Null venue clocks on trades: cluster by ingress hour
    null_tx = trades.filter(pl.col("transaction_ts").is_null())
    null_by_ingress_hour = (
        null_tx.with_columns(pl.col("ingress_ts").dt.hour().alias("utc_hour"))
        .group_by("utc_hour")
        .len()
        .sort("utc_hour")
        .to_dicts()
    )
    null_sample = null_tx.select(
        "trade_id", "ingress_ts", "publish_ts", "price", "qty", "side"
    ).head(5)

    # Crossed TOB: distributions first, sample second
    crossed = (
        tob.sort("transaction_ts")
        .with_columns(
            [
                (pl.col("bid_price") > pl.col("ask_price")).alias("crossed"),
                (pl.col("bid_price") - pl.col("ask_price")).alias("cross_amt"),
            ]
        )
        .filter(pl.col("crossed"))
    )

    # Clock-skew tail: ingress - transaction
    skew = tob.with_columns(
        (
            (pl.col("ingress_ts") - pl.col("transaction_ts")).dt.total_microseconds() / 1e6
        ).alias("skew_s")
    )
    skew_tail = (
        skew.filter(pl.col("skew_s") > 60)
        .select("transaction_ts", "ingress_ts", "publish_ts", "bid_price", "ask_price", "skew_s")
        .sort("skew_s", descending=True)
    )
    big_gaps = (
        tob.sort("transaction_ts")
        .select(
            pl.col("transaction_ts").alias("from_ts"),
            pl.col("transaction_ts").shift(-1).alias("to_ts"),
            _gap_s("transaction_ts").shift(-1).alias("gap_s"),
        )
        .filter(pl.col("gap_s") > 300)
        .sort("gap_s", descending=True)
    )
    all_gaps = tob.sort("transaction_ts").select(_gap_s("transaction_ts").alias("g")).drop_nulls()

    # Trade-through exhibit
    tob_key = tob.select("transaction_ts", "bid_price", "ask_price")
    joined = (
        trades.filter(pl.col("transaction_ts").is_not_null())
        .sort("transaction_ts")
        .join_asof(tob_key.sort("transaction_ts"), on="transaction_ts", strategy="backward")
    )
    # a crossed prevailing quote has no satisfiable inside, so bound it by min/max
    through = joined.filter(
        (pl.col("price") > pl.max_horizontal("bid_price", "ask_price"))
        | (pl.col("price") < pl.min_horizontal("bid_price", "ask_price"))
    ).select("trade_id", "transaction_ts", "price", "qty", "side", "bid_price", "ask_price")

    return {
        "sides": trades["side"].value_counts().to_dicts(),
        "null_tx_n": null_tx.height,
        "null_tx_pct": null_tx.height / trades.height * 100.0,
        "null_by_ingress_hour": null_by_ingress_hour,
        "null_sample": _rows(null_sample),
        "null_publish_always_present": int(null_tx["publish_ts"].null_count()) == 0,
        "crossed_n": crossed.height,
        "crossed_median_amt": float(crossed["cross_amt"].median() or 0),
        "crossed_max_amt": float(crossed["cross_amt"].max() or 0),
        "crossed_ask_price_dist": _dist(crossed, "ask_price"),
        "crossed_ask_qty_dist": _dist(crossed, "ask_qty"),
        "crossed_bid_price_dist": _dist(crossed, "bid_price"),
        "crossed_bid_qty_dist": _dist(crossed, "bid_qty"),
        "crossed_amount_dist": _dist(crossed, "cross_amt"),
        "crossed_sample": _rows(
            crossed.select(
                "transaction_ts", "bid_price", "ask_price", "bid_qty", "ask_qty", "cross_amt"
            ).head(8)
        ),
        "skew_over_60s": skew_tail.height,
        "skew_max_s": float(skew["skew_s"].max()),
        "skew_median_s": float(skew["skew_s"].median()),
        "skew_tail": _rows(skew_tail.head(5)),
        "gap_stats_s": {
            "n": all_gaps.height,
            "median": float(all_gaps["g"].median()),
            "p99": float(all_gaps.select(pl.col("g").quantile(0.99)).item()),
            "max": float(all_gaps["g"].max()),
            "over_60s": int(all_gaps.filter(pl.col("g") > 60).height),
            "over_300s": int(all_gaps.filter(pl.col("g") > 300).height),
        },
        "large_gaps": _rows(big_gaps),
        "trade_through": _rows(through),
        "tob_one_null_bid": int(tob.filter(pl.col("bid_price").is_null()).height),
        "tob_one_null_ask": int(tob.filter(pl.col("ask_price").is_null()).height),
    }


def exhibits_b() -> dict:
    df = load_parquet("B").sort("ingress_ts")
    mp = add_microprice(df)
    ask_only = mp.filter(pl.col("bid_price").is_null())
    # run-length of ask-only via group id on status changes
    flagged = mp.with_columns(
        (pl.col("bid_price").is_null() != pl.col("bid_price").shift(1).is_null())
        .fill_null(True)
        .cum_sum()
        .alias("run")
    )
    ask_runs = (
        flagged.filter(pl.col("bid_price").is_null())
        .group_by("run")
        .agg(
            [
                pl.len().alias("n"),
                pl.col("ingress_ts").min().alias("start"),
                pl.col("ingress_ts").max().alias("end"),
            ]
        )
        .sort("n", descending=True)
    )
    sample = ask_only.select("ingress_ts", "bid_price", "bid_qty", "ask_price", "ask_qty").head(4)
    two_sided = mp.filter(pl.col("bid_price").is_not_null()).select(
        "ingress_ts", "bid_price", "ask_price", "microprice", "mid"
    ).head(3)
    return {
        "instrument": df["instrument"].unique().to_list(),
        "venue_clocks_all_null": {
            "transaction_ts": int(df["transaction_ts"].null_count()),
            "publish_ts": int(df["publish_ts"].null_count()),
            "rows": df.height,
        },
        "ask_only_n": ask_only.height,
        "ask_only_pct": ask_only.height / df.height * 100.0,
        "ask_only_runs": ask_runs.height,
        "ask_only_longest_run": int(ask_runs["n"].max()),
        "ask_only_median_run": float(ask_runs["n"].median()),
        "ask_only_run_lengths": ask_runs["n"].value_counts().sort("n").to_dicts(),
        "microprice_reason_dist": mp["microprice_reason"].value_counts().to_dicts(),
        "ask_only_sample": _rows(sample),
        "two_sided_sample": _rows(two_sided),
        "longest_runs": [
            {k: _ts(v) if k in ("start", "end") else v for k, v in r.items()}
            for r in ask_runs.head(5).to_dicts()
        ],
    }


def exhibits_c() -> dict:
    df = load_parquet("C")
    instruments = df["instrument"].unique().sort().to_list()

    # Sorting by the partition key AND the differenced column before diffing over the
    # partition makes the result zero by construction, whatever the input. Both figures
    # below are taken in stored file order.
    global_back = int(
        df.select(pl.col("ingress_ts").diff().alias("d")).filter(pl.col("d") < 0).height
    )
    per_instr_frame = (
        df.select(pl.col("ingress_ts").diff().over("instrument").alias("d"))
        .filter(pl.col("d") < 0)
        .select((pl.col("d").dt.total_microseconds() / 1000.0).alias("ms"))
    )
    per_instr = per_instr_frame.height
    tautology_check = int(
        df.sort(["instrument", "ingress_ts"])
        .select(pl.col("ingress_ts").diff().over("instrument").alias("d"))
        .filter(pl.col("d") < 0)
        .height
    )

    # the residual is only meaningful if the stored order is the venue's own order
    stored_order = {
        c: int(df.select(pl.col(c).diff().alias("d")).filter(pl.col("d") < 0).height)
        for c in ("ingress_ts", "transaction_ts", "publish_ts")
    }
    if_sorted_by_ingress = {
        c: int(
            df.sort("ingress_ts").select(pl.col(c).diff().alias("d")).filter(pl.col("d") < 0).height
        )
        for c in ("transaction_ts", "publish_ts")
    }
    publish_per_instr = int(
        df.select(pl.col("publish_ts").diff().over("instrument").alias("d"))
        .filter(pl.col("d") < 0)
        .height
    )

    crossed = df.filter(pl.col("bid_price") > pl.col("ask_price"))
    crossed_by = (
        crossed.group_by("instrument")
        .agg(
            [
                pl.len().alias("n"),
                (pl.col("bid_price") - pl.col("ask_price")).median().alias("median_cross"),
                (pl.col("bid_price") - pl.col("ask_price")).max().alias("max_cross"),
            ]
        )
        .sort("n", descending=True)
    )
    seq_dups = df.group_by(["instrument", "seq_id"]).len().filter(pl.col("len") > 1).height
    sample_cross = crossed.select(
        "instrument", "transaction_ts", "bid_price", "ask_price", "bid_qty", "ask_qty"
    ).head(5)
    return {
        "n_instruments": len(instruments),
        "instruments": instruments,
        "filename_claims": 20,
        "global_ingress_backward": global_back,
        "per_instrument_ingress_backward": per_instr,
        "per_instrument_reduction_pct": (global_back - per_instr) / global_back * 100.0,
        "per_instrument_backward_if_pre_sorted": tautology_check,
        "per_instrument_residual_ms": {
            "within_1ms": int(per_instr_frame.filter(pl.col("ms") >= -1.0).height),
            "over_500ms": int(per_instr_frame.filter(pl.col("ms") < -500.0).height),
            "worst_ms": float(per_instr_frame["ms"].min()),
            "median_ms": float(per_instr_frame["ms"].median()),
        },
        "stored_order_backward": stored_order,
        "backward_if_sorted_by_ingress": if_sorted_by_ingress,
        "publish_ts_backward_global": stored_order["publish_ts"],
        "publish_ts_backward_per_instrument": publish_per_instr,
        "crossed_n": crossed.height,
        "locked_n": int(df.filter(pl.col("bid_price") == pl.col("ask_price")).height),
        "crossed_by_symbol": crossed_by.to_dicts(),
        "crossed_ask_qty_dist": _dist(crossed, "ask_qty"),
        "crossed_bid_qty_dist": _dist(crossed, "bid_qty"),
        "crossed_ask_price_distinct": int(crossed["ask_price"].n_unique()),
        "seq_id_global_unique": df["seq_id"].n_unique(),
        "seq_id_dup_groups_per_instrument": seq_dups,
        "cross_sample": _rows(sample_cross),
    }


def exhibits_d() -> dict:
    df = load_parquet("D")
    levels = df.select(
        (pl.col("bid_prices").list.len() + pl.col("ask_prices").list.len()).alias("n_lvl")
    )
    hist_bins = [0, 20, 50, 100, 200, 500, 2000]
    hist = []
    for lo, hi in pairwise(hist_bins):
        hist.append(
            {
                "lo": lo,
                "hi": hi,
                "n": int(levels.filter((pl.col("n_lvl") >= lo) & (pl.col("n_lvl") < hi)).height),
            }
        )
    primary = replay_order_book(df, snapshot_level_threshold=200, validate_snapshots=True)
    legacy = replay_order_book(df, snapshot_level_threshold=200, validate_snapshots=False)
    top = legacy["top_of_book"]
    crossed = top.filter(pl.col("crossed"))
    top2 = top.with_columns(pl.col("is_snapshot").shift(1).alias("prev_snap"))
    after_snap = int(top2.filter(pl.col("crossed") & pl.col("prev_snap")).height)
    sample_cross = crossed.select(
        "seq_id", "n_levels_in_msg", "is_snapshot", "best_bid", "best_ask", "spread",
        "n_bid_levels", "n_ask_levels",
    ).head(8)
    seq_diff = df.sort("seq_id").select(pl.col("seq_id").diff().alias("d")).drop_nulls()
    return {
        "snapshot_true": int(df.filter(pl.col("snapshot")).height),
        "transaction_ts_nulls": int(df["transaction_ts"].null_count()),
        "level_hist": hist,
        "median_n_lvl": float(levels["n_lvl"].median()),
        "max_n_lvl": int(levels["n_lvl"].max()),
        "integrity_validated": primary["integrity"],
        "integrity_threshold200_unvalidated": legacy["integrity"],
        "crossed_after_snapshot": after_snap,
        "crossed_best_ask_dist": _dist(crossed, "best_ask"),
        "crossed_best_bid_dist": _dist(crossed, "best_bid"),
        "crossed_msg_levels_dist": _dist(crossed, "n_levels_in_msg"),
        "crossed_sample": sample_cross.to_dicts(),
        "seq_diff_median": float(seq_diff["d"].median()),
        "seq_diff_p50": float(seq_diff.select(pl.col("d").quantile(0.5)).item()),
        "seq_diff_p99": float(seq_diff.select(pl.col("d").quantile(0.99)).item()),
        "seq_diff_max": int(seq_diff["d"].max()),
    }


def exhibits_f() -> dict:
    df = load_parquet("F").sort("ingress_ts")
    seq = df.select(
        pl.col("seq_id"),
        pl.col("seq_id").shift(1).alias("prev"),
        (pl.col("seq_id") - pl.col("seq_id").shift(1)).alias("d"),
        "ingress_ts",
        "bid_price",
        "ask_price",
    ).drop_nulls()
    back = seq.filter(pl.col("d") < 0).head(8)
    d_cols = load_parquet("D").columns
    f_cols = df.columns
    return {
        "columns": f_cols,
        "d_columns": d_cols,
        "missing_vs_d": sorted(
            set(d_cols)
            - set(f_cols)
            - {"bid_prices", "bid_qtys", "ask_prices", "ask_qtys", "snapshot"}
        ),
        "backward_n": int(seq.filter(pl.col("d") < 0).height),
        "backward_sample": _rows(back),
        "seq_diff_median": float(seq["d"].median()),
        "seq_diff_p50": float(seq.select(pl.col("d").quantile(0.5)).item()),
        "seq_step_dist": _dist(seq, "d"),
    }


def exhibits_g() -> dict:
    df = load_parquet("G")
    for c in ("ingress_ts", "transaction_ts", "publish_ts"):
        df = ensure_datetime(df, c)
    dups = df.group_by("trade_id").agg(pl.len().alias("n")).filter(pl.col("n") > 1)
    both = df.sort(["trade_id", "ingress_ts"])
    first = both.unique(subset=["trade_id"], keep="first")
    second = both.unique(subset=["trade_id"], keep="last")
    cmp = first.join(second, on="trade_id", suffix="_2")
    n = cmp.height
    same_price = int((cmp["price"] == cmp["price_2"]).sum())
    same_qty = int((cmp["qty"] == cmp["qty_2"]).sum())
    same_side = int((cmp["side"] == cmp["side_2"]).sum())
    same_tx = int((cmp["transaction_ts"] == cmp["transaction_ts_2"]).sum())
    same_pub = int((cmp["publish_ts"] == cmp["publish_ts_2"]).sum())
    same_ing = int((cmp["ingress_ts"] == cmp["ingress_ts_2"]).sum())
    opp_side = int(
        cmp.filter(
            ((pl.col("side") == "Buy") & (pl.col("side_2") == "Sell"))
            | ((pl.col("side") == "Sell") & (pl.col("side_2") == "Buy"))
        ).height
    )
    exhibit_id = cmp["trade_id"][0]
    pair = df.filter(pl.col("trade_id") == exhibit_id).select(
        "trade_id", "ingress_ts", "transaction_ts", "publish_ts", "price", "qty", "side"
    )
    as_stored_back = int(
        load_parquet("G").select(pl.col("ingress_ts").diff().alias("d")).filter(pl.col("d") < 0).height
    )
    return {
        "rows": df.height,
        "unique_trade_id": df["trade_id"].n_unique(),
        "dup_groups": dups.height,
        "dup_extra_rows": int(dups.select((pl.col("n") - 1).sum()).item()),
        "dup_group_size_dist": dups["n"].value_counts().sort("n").to_dicts(),
        "all_ids_exactly_twice": bool(
            dups.height == df["trade_id"].n_unique()
            and int(dups["n"].min()) == 2
            and int(dups["n"].max()) == 2
        ),
        "copies_same_price": same_price,
        "copies_same_qty": same_qty,
        "copies_same_side": same_side,
        "copies_same_transaction_ts": same_tx,
        "copies_same_publish_ts": same_pub,
        "copies_same_ingress_ts": same_ing,
        "copies_opposite_side": opp_side,
        "n_pairs_compared": n,
        "exhibit_pair": _rows(pair),
        "as_stored_ingress_backward": as_stored_back,
        "timestamp_dtype_raw": str(load_parquet("G").schema["ingress_ts"]),
    }


def exhibits_h() -> dict:
    static = load_parquet("H_static")
    return {
        "static_row": {
            k: v
            for k, v in static.to_dicts()[0].items()
            if k
            in (
                "instrument",
                "exchange_symbol",
                "quantity_multiplier",
                "scale",
                "trading_state",
                "price_tick_size",
                "qty_step_size",
                "present",
            )
        },
        "static_schema": {k: str(v) for k, v in static.schema.items()},
        "static_nulls": {c: int(static[c].null_count()) for c in static.columns},
    }


def main() -> None:
    ensure_dirs()
    out = {
        "A": exhibits_a(),
        "B": exhibits_b(),
        "C": exhibits_c(),
        "D": exhibits_d(),
        "F": exhibits_f(),
        "G": exhibits_g(),
        "H": exhibits_h(),
    }
    write_table_json("exhibits.json", out)
    print("G exactly twice", out["G"]["all_ids_exactly_twice"], "opp side", out["G"]["copies_opposite_side"])
    print("A skew max", out["A"]["skew_max_s"], "gaps>300s", out["A"]["gap_stats_s"]["over_300s"])
    print(
        "C global back", out["C"]["global_ingress_backward"],
        "per-instr", out["C"]["per_instrument_ingress_backward"],
        "if pre-sorted", out["C"]["per_instrument_backward_if_pre_sorted"],
    )
    print("C residual", out["C"]["per_instrument_residual_ms"])
    print("wrote outputs/tables/exhibits.json")


if __name__ == "__main__":
    main()
