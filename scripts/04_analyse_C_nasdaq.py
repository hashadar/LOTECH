"""NASDAQ 20-symbol TOB aggregations, cross structure and sequencing."""
from __future__ import annotations

import sys
from itertools import pairwise
from pathlib import Path

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from lotech_dq.checks import profile_frame  # noqa: E402
from lotech_dq.clocks import skew_stats  # noqa: E402
from lotech_dq.io import ensure_dirs, load_parquet  # noqa: E402
from lotech_dq.report import write_table_json  # noqa: E402

LARGE_GAP_S = 60.0


def _rows(df: pl.DataFrame) -> list[dict]:
    return [
        {k: (str(v) if hasattr(v, "isoformat") else v) for k, v in r.items()}
        for r in df.to_dicts()
    ]


def per_symbol(df: pl.DataFrame) -> pl.DataFrame:
    """Per-instrument quote quality.

    Two scores are emitted. `legacy_anomaly_score` adds a rate to a raw count, so it is
    dominated by the gap count and ranks illiquid names worst. It is kept because the
    write-up uses it as the example of a generic score failing. `anomaly_score`
    expresses every term per 1,000 rows so the terms are commensurable.
    """
    return (
        df.sort(["instrument", "transaction_ts"])
        .with_columns(
            (pl.col("bid_price") > pl.col("ask_price")).alias("crossed"),
            (pl.col("bid_price") == pl.col("ask_price")).alias("locked"),
            (pl.col("ask_price") - pl.col("bid_price")).alias("spread"),
            (
                pl.col("transaction_ts").diff().over("instrument").dt.total_microseconds() / 1e6
            ).alias("gap_s"),
            (
                (pl.col("ingress_ts") - pl.col("transaction_ts")).dt.total_microseconds() / 1e6
            ).alias("skew_s"),
        )
        .group_by("instrument")
        .agg(
            pl.len().alias("rows"),
            pl.col("crossed").sum().alias("crossed_n"),
            pl.col("locked").sum().alias("locked_n"),
            pl.col("spread").median().alias("spread_median"),
            pl.col("spread").quantile(0.99).alias("spread_p99"),
            pl.col("spread").max().alias("spread_max"),
            pl.col("gap_s").median().alias("gap_median_s"),
            pl.col("gap_s").max().alias("gap_max_s"),
            (pl.col("gap_s") > LARGE_GAP_S).sum().alias("gaps_over_60s"),
            pl.col("skew_s").median().alias("skew_median_s"),
            pl.col("skew_s").quantile(0.99).alias("skew_p99_s"),
            (pl.col("skew_s") < 0).sum().alias("neg_skew_n"),
            pl.col("transaction_ts").min().alias("tx_min"),
            pl.col("transaction_ts").max().alias("tx_max"),
        )
        .with_columns(
            (pl.col("crossed_n") / pl.col("rows") * 100.0).alias("crossed_pct"),
            (pl.col("locked_n") / pl.col("rows") * 100.0).alias("locked_pct"),
        )
        .with_columns(
            (
                pl.col("crossed_pct") * 5
                + pl.col("gaps_over_60s").cast(pl.Float64)
                + pl.col("neg_skew_n").cast(pl.Float64) / pl.col("rows") * 100.0
            ).alias("legacy_anomaly_score"),
            (
                pl.col("crossed_n") / pl.col("rows") * 1000.0 * 5.0
                + pl.col("gaps_over_60s") / pl.col("rows") * 1000.0
                + pl.col("neg_skew_n") / pl.col("rows") * 1000.0
            ).alias("anomaly_score"),
        )
        .sort("anomaly_score", descending=True)
    )


def cross_structure(df: pl.DataFrame) -> dict:
    """Temporal, size and sequencing structure of the 1,030 crossed quotes.

    The distinguishing facts are that the crosses arrive in a burst across unrelated
    symbols at once, and that they carry ordinary round-lot sizes. A per-symbol
    consolidation race would produce neither of those facts.
    """
    work = df.with_columns(
        (pl.col("bid_price") - pl.col("ask_price")).alias("inversion"),
        pl.col("transaction_ts").shift(1).over("instrument").alias("prev_tx"),
        pl.col("bid_price").shift(1).over("instrument").alias("prev_bid"),
        pl.col("ask_price").shift(1).over("instrument").alias("prev_ask"),
        (pl.col("bid_price") > pl.col("ask_price")).alias("crossed"),
    ).with_columns(
        (pl.col("crossed") & pl.col("crossed").shift(1).over("instrument")).alias("after_crossed")
    )
    crossed = work.filter(pl.col("crossed"))
    n = crossed.height

    by_second = (
        crossed.group_by(pl.col("transaction_ts").dt.truncate("1s").alias("second"))
        .agg(pl.len().alias("n"), pl.col("instrument").n_unique().alias("symbols"))
        .sort("n", descending=True)
    )
    by_minute = (
        crossed.group_by(pl.col("transaction_ts").dt.truncate("1m").alias("minute"))
        .agg(pl.len().alias("n"), pl.col("instrument").n_unique().alias("symbols"))
        .sort("n", descending=True)
    )
    peak_second = by_second.head(1).to_dicts()[0]["second"]
    burst3 = crossed.filter(
        (pl.col("transaction_ts") >= peak_second)
        & (pl.col("transaction_ts") < peak_second + pl.duration(seconds=3))
    )

    cents = (crossed["inversion"] * 100).round(0).cast(pl.Int64)
    cent_hist = (
        pl.DataFrame({"cents": cents})["cents"]
        .value_counts()
        .sort("cents")
        .rename({"count": "n"})
    )
    worst = crossed.sort("inversion", descending=True).head(1).to_dicts()[0]

    ask_levels = (
        crossed.group_by("instrument", "ask_price").len().sort("len", descending=True).head(5)
    )

    # run structure of consecutive crossed quotes for the same instrument
    runs = (
        work.with_columns(
            (pl.col("crossed") != pl.col("crossed").shift(1).over("instrument"))
            .fill_null(True)
            .cum_sum()
            .over("instrument")
            .alias("run")
        )
        .filter(pl.col("crossed"))
        .group_by("instrument", "run")
        .len()
    )

    return {
        "crossed_n": n,
        "nulls_in_bid_price": int(df["bid_price"].null_count()),
        "nulls_in_ask_price": int(df["ask_price"].null_count()),
        "burst": {
            "peak_second_utc": str(peak_second),
            "peak_second_crosses": int(by_second.head(1).to_dicts()[0]["n"]),
            "peak_second_pct": int(by_second.head(1).to_dicts()[0]["n"]) / n * 100.0,
            "peak_second_distinct_symbols": int(by_second.head(1).to_dicts()[0]["symbols"]),
            "three_second_window_crosses": burst3.height,
            "three_second_window_pct": burst3.height / n * 100.0,
            "three_second_window_symbols": _rows(
                burst3.group_by("instrument").len().sort("len", descending=True)
            ),
            "top_seconds": _rows(by_second.head(5)),
            "top_minutes": _rows(by_minute.head(5)),
            "crosses_in_first_minute": int(
                crossed.filter(
                    pl.col("transaction_ts") < crossed["transaction_ts"].min() + pl.duration(minutes=1)
                ).height
            ),
        },
        "sizes": {
            "bid_qty_median": float(crossed["bid_qty"].median()),
            "bid_qty_min": float(crossed["bid_qty"].min()),
            "ask_qty_median": float(crossed["ask_qty"].median()),
            "ask_qty_min": float(crossed["ask_qty"].min()),
            "rows_with_ask_qty_le_2": int(crossed.filter(pl.col("ask_qty") <= 2).height),
            "rows_with_bid_qty_le_2": int(crossed.filter(pl.col("bid_qty") <= 2).height),
            "distinct_ask_prices": int(crossed["ask_price"].n_unique()),
            "most_repeated_ask_level": _rows(ask_levels),
        },
        "inversion": {
            "median": float(crossed["inversion"].median()),
            "max": float(crossed["inversion"].max()),
            "one_cent_n": int(cent_hist.filter(pl.col("cents") == 1)["n"].sum()),
            "one_cent_pct": int(cent_hist.filter(pl.col("cents") == 1)["n"].sum()) / n * 100.0,
            "le_five_cents_pct": int(cent_hist.filter(pl.col("cents") <= 5)["n"].sum()) / n * 100.0,
            "worst_instrument": worst["instrument"],
            "worst_cents": round(float(worst["inversion"]) * 100),
            "cent_histogram": [{"cents": r["cents"], "n": r["n"]} for r in cent_hist.to_dicts()],
        },
        "sequencing": {
            "share_transaction_ts_with_previous_quote": int(
                crossed.filter(pl.col("transaction_ts") == pl.col("prev_tx")).height
            ),
            "share_transaction_ts_pct": int(
                crossed.filter(pl.col("transaction_ts") == pl.col("prev_tx")).height
            ) / n * 100.0,
            "prices_unchanged_from_previous_quote": int(
                crossed.filter(
                    (pl.col("bid_price") == pl.col("prev_bid"))
                    & (pl.col("ask_price") == pl.col("prev_ask"))
                ).height
            ),
            "followed_by_another_crossed_quote": int(crossed.filter(pl.col("after_crossed")).height),
            "n_runs": runs.height,
            "median_run_length": float(runs["len"].median()),
            "max_run_length": int(runs["len"].max()),
        },
    }


def seq_id_structure(df: pl.DataFrame) -> dict:
    """`seq_id` is unique per instrument but is not a per-instrument ordering key.

    The instruments fall into a few disjoint value bands with symbol-dependent step
    sizes. That is what a per-channel counter shared by several symbols looks like.
    The file does not carry the channel. The partition key its own `seq_id` is
    defined against is absent.
    """
    per = (
        df.with_columns(pl.col("seq_id").diff().over("instrument").alias("d"))
        .group_by("instrument")
        .agg(
            pl.len().alias("rows"),
            pl.col("seq_id").min().alias("seq_min"),
            pl.col("seq_id").max().alias("seq_max"),
            pl.col("seq_id").n_unique().alias("seq_distinct"),
            pl.col("d").median().alias("step_median"),
            (pl.col("d") < 0).sum().alias("backward_steps"),
        )
        .sort("seq_min")
    )

    # group instruments whose seq_id ranges start close together; a jump of this size
    # between consecutive starting values separates one counter from the next
    band_gap = 200_000
    rows = per.to_dicts()
    bands: list[dict] = [{"instruments": [rows[0]["instrument"]], "seq_min": rows[0]["seq_min"]}]
    for prev, cur in pairwise(rows):
        if cur["seq_min"] - prev["seq_min"] > band_gap:
            bands.append({"instruments": [], "seq_min": cur["seq_min"]})
        bands[-1]["instruments"].append(cur["instrument"])
    for b in bands:
        sub = df.filter(pl.col("instrument").is_in(b["instruments"]))
        d = sub.select(pl.col("seq_id").diff().alias("d")).drop_nulls()
        b["n_instruments"] = len(b["instruments"])
        b["rows"] = sub.height
        b["seq_max"] = int(sub["seq_id"].max())
        b["distinct_seq_id"] = int(sub["seq_id"].n_unique())
        b["duplicate_groups_within_band"] = int(
            sub.group_by("seq_id").len().filter(pl.col("len") > 1).height
        )
        b["step_median"] = float(d["d"].median())
        b["backward_steps_in_file_order"] = int(d.filter(pl.col("d") < 0).height)

    dup_per_instrument = int(
        df.group_by(["instrument", "seq_id"]).len().filter(pl.col("len") > 1).height
    )
    return {
        "distinct_seq_id_global": int(df["seq_id"].n_unique()),
        "global_collision_groups": int(
            df.group_by("seq_id").len().filter(pl.col("len") > 1).height
        ),
        "duplicate_groups_per_instrument": dup_per_instrument,
        "backward_steps_per_instrument_file_order": int(per["backward_steps"].sum()),
        "band_grouping_rule": f"consecutive min(seq_id) values within {band_gap:,} of each other",
        "bands": bands,
        "band_sizes": [b["n_instruments"] for b in bands],
        "bands_with_zero_collisions": sum(
            1 for b in bands if b["duplicate_groups_within_band"] == 0
        ),
        "per_instrument": _rows(per.sort("seq_min")),
        "note": (
            "seq_id is unique per instrument but steps backwards within instrument in file "
            "order, so it is not usable as a per-instrument ordering key. The bands are "
            "clusters of starting value, not disjoint ranges. The observed intervals overlap. "
            "Groups of symbols sharing a counter origin and a step size are what a per-channel "
            "counter looks like. The channel is not a column in the file."
        ),
    }


def clock_ordering(df: pl.DataFrame) -> dict:
    """Backward steps per clock, in stored order, globally and per instrument."""
    out: dict = {
        "file_order_is": "transaction_ts order (0 backward venue-clock steps globally)",
    }
    for col in ("transaction_ts", "publish_ts", "ingress_ts"):
        g = int(df.select(pl.col(col).diff().alias("d")).filter(pl.col("d") < 0).height)
        per = df.select(pl.col(col).diff().over("instrument").alias("d")).filter(pl.col("d") < 0)
        out[col] = {
            "backward_global": g,
            "backward_per_instrument": per.height,
            "reduction_pct": (1 - per.height / g) * 100.0 if g else None,
        }
        if per.height:
            us = per.select((pl.col("d").dt.total_microseconds()).alias("us"))
            out[col]["median_backward_ms"] = float(us["us"].median()) / 1000.0
            out[col]["worst_backward_ms"] = float(us["us"].min()) / 1000.0
            for label, limit_us in (("within_1ms", -1_000), ("worse_than_500ms", -500_000)):
                out[col][label] = int(
                    us.filter(pl.col("us") >= limit_us).height
                    if label == "within_1ms"
                    else us.filter(pl.col("us") < limit_us).height
                )
    out["publish_vs_transaction"] = {
        "publish_after_transaction": int(
            df.filter(pl.col("publish_ts") > pl.col("transaction_ts")).height
        ),
        "publish_before_transaction": int(
            df.filter(pl.col("publish_ts") < pl.col("transaction_ts")).height
        ),
        "publish_equals_transaction": int(
            df.filter(pl.col("publish_ts") == pl.col("transaction_ts")).height
        ),
    }
    return out


def main() -> None:
    ensure_dirs()
    df = load_parquet("C")

    subset_cols = [
        c
        for c in (
            "instrument",
            "ingress_ts",
            "transaction_ts",
            "publish_ts",
            "seq_id",
            "bid_price",
            "ask_price",
            "bid_qty",
            "ask_qty",
        )
        if c in df.columns
    ]
    profile = profile_frame(df.select(subset_cols), "C_nasdaq_top_of_book_20_symbols.parquet")

    ps = per_symbol(df)
    ps.write_csv(ROOT / "outputs" / "tables" / "C_nasdaq_per_symbol.csv")

    locked_rank = ps.select("instrument", "rows", "locked_n", "locked_pct").sort(
        "locked_pct", descending=True
    )

    out = {
        "rows": df.height,
        "n_instruments": df["instrument"].n_unique(),
        "instruments": df["instrument"].unique().sort().to_list(),
        "window": {
            "ingress_min": str(df["ingress_ts"].min()),
            "ingress_max": str(df["ingress_ts"].max()),
            "transaction_min": str(df["transaction_ts"].min()),
            "transaction_max": str(df["transaction_ts"].max()),
        },
        "global_skew": skew_stats(df),
        "crossed_total": int(df.filter(pl.col("bid_price") > pl.col("ask_price")).height),
        "locked_total": int(df.filter(pl.col("bid_price") == pl.col("ask_price")).height),
        "cross_structure": cross_structure(df),
        "locked_by_symbol": _rows(locked_rank),
        "crossed_by_symbol": _rows(
            ps.select("instrument", "rows", "crossed_n", "crossed_pct").sort(
                "crossed_n", descending=True
            )
        ),
        "instruments_with_crosses": int(ps.filter(pl.col("crossed_n") > 0).height),
        "clock_ordering": clock_ordering(df),
        "seq_id_structure": seq_id_structure(df),
        "worst_symbols_by_anomaly_score": _rows(ps.head(3)),
        "worst_symbols_by_legacy_score": _rows(
            ps.sort("legacy_anomaly_score", descending=True).head(3)
        ),
        "score_note": (
            "legacy_anomaly_score adds crossed_pct*5 (a rate) to gaps_over_60s (a count), so it "
            "is the gap count and ranks the illiquid names worst. anomaly_score puts every term "
            "on a per-1,000-row basis. The CSV is sorted by it."
        ),
        "profile_findings": profile["findings"],
    }
    write_table_json("C_nasdaq.json", out)
    print("instruments", out["n_instruments"], "crossed", out["crossed_total"])
    print("burst", out["cross_structure"]["burst"]["peak_second_utc"],
          out["cross_structure"]["burst"]["peak_second_crosses"])
    print("ingress backward", out["clock_ordering"]["ingress_ts"])
    print("wrote outputs/tables/C_nasdaq.json")


if __name__ == "__main__":
    main()
