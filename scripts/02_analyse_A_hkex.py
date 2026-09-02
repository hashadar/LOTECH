"""HKEX 2800 TOB + trades DQ and cross-file consistency."""
from __future__ import annotations

import sys
from pathlib import Path

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from lotech_dq.checks import profile_frame, tob_crossed_locked  # noqa: E402
from lotech_dq.clocks import all_skews  # noqa: E402
from lotech_dq.io import ensure_dirs, load_parquet  # noqa: E402
from lotech_dq.report import write_table_json  # noqa: E402

# HKEX 2800 session boundaries on 2026-08-13, in UTC. Used to split quote defects by
# session phase. A locked book means different things in an auction and in continuous
# trading. In continuous trading a locked book would execute.
SESSION = {
    "opening_auction_end": "2026-08-13T01:30:00Z",
    "lunch_start": "2026-08-13T04:00:00Z",
    "lunch_end": "2026-08-13T05:00:00Z",
    "closing_auction_start": "2026-08-13T08:00:00Z",
}
_OPEN = pl.datetime(2026, 8, 13, 1, 30, 0, time_zone="UTC")
_LUNCH0 = pl.datetime(2026, 8, 13, 4, 0, 0, time_zone="UTC")
_LUNCH1 = pl.datetime(2026, 8, 13, 5, 0, 0, time_zone="UTC")
_CLOSE = pl.datetime(2026, 8, 13, 8, 0, 0, time_zone="UTC")


def session_phase(ts: str = "transaction_ts") -> pl.Expr:
    return (
        pl.when(pl.col(ts) < _OPEN)
        .then(pl.lit("opening_auction"))
        .when(pl.col(ts) >= _CLOSE)
        .then(pl.lit("closing_auction"))
        .when((pl.col(ts) >= _LUNCH0) & (pl.col(ts) < _LUNCH1))
        .then(pl.lit("lunch"))
        .otherwise(pl.lit("continuous"))
        .alias("session_phase")
    )


def _rows(df: pl.DataFrame) -> list[dict]:
    return [{k: (str(v) if hasattr(v, "isoformat") else v) for k, v in r.items()} for r in df.to_dicts()]


def _value_counts(df: pl.DataFrame, col: str, top: int | None = None) -> list[dict]:
    vc = df[col].value_counts().sort("count", descending=True)
    if top is not None:
        vc = vc.head(top)
    return [{"value": r[col], "n": r["count"]} for r in vc.to_dicts()]


def gap_table(tob: pl.DataFrame) -> dict:
    """TOB venue-clock gaps. Duration.dt.total_seconds() floors to whole seconds in
    polars 1.44, which drops a 300.51 s gap at a 300 s threshold; work in microseconds."""
    g = (
        tob.sort("transaction_ts")
        .select(
            pl.col("transaction_ts").alias("from_ts"),
            pl.col("transaction_ts").shift(-1).alias("to_ts"),
            (
                pl.col("transaction_ts").shift(-1) - pl.col("transaction_ts")
            ).dt.total_microseconds().alias("gap_us"),
        )
        .drop_nulls()
        .with_columns((pl.col("gap_us") / 1e6).alias("gap_s"))
    )
    return {
        "n_gaps": g.height,
        "median_s": float(g["gap_s"].median()),
        "p99_s": float(g.select(pl.col("gap_s").quantile(0.99)).item()),
        "max_s": float(g["gap_s"].max()),
        "gaps_over_60s": int(g.filter(pl.col("gap_s") > 60).height),
        "gaps_over_300s": int(g.filter(pl.col("gap_s") > 300).height),
        "gaps_over_300s_rows": _rows(
            g.filter(pl.col("gap_s") > 300).sort("from_ts").select("from_ts", "to_ts", "gap_s")
        ),
    }


def crossed_locked_detail(tob: pl.DataFrame) -> dict:
    """Full distributions for the crossed and locked sets.

    Sample rows cannot stand in for the population here. An earlier draft
    generalised an eight-row head into a claim about all 71 crossed rows.
    """
    two_sided = tob.filter(pl.col("bid_price").is_not_null() & pl.col("ask_price").is_not_null())
    state_key = ["transaction_ts", "bid_price", "bid_qty", "ask_price", "ask_qty"]

    crossed = (
        two_sided.filter(pl.col("bid_price") > pl.col("ask_price"))
        .with_columns((pl.col("bid_price") - pl.col("ask_price")).alias("inversion"))
        .with_columns(session_phase())
        .sort("transaction_ts")
    )
    locked = (
        two_sided.filter(pl.col("bid_price") == pl.col("ask_price"))
        .with_columns(session_phase())
        .sort("transaction_ts")
    )

    inv = crossed["inversion"]
    states = crossed.select(state_key).unique().height
    by_state = (
        crossed.group_by(state_key)
        .agg(pl.len().alias("n"), pl.col("inversion").first())
        .sort("n", descending=True)
    )

    return {
        "two_sided_rows": two_sided.height,
        "crossed_n": crossed.height,
        "crossed_pct": crossed.height / two_sided.height * 100.0,
        "locked_n": locked.height,
        "locked_pct": locked.height / two_sided.height * 100.0,
        "crossed_distinct_quote_states": states,
        "crossed_rows_per_state_top": _rows(by_state.head(10)),
        "inversion": {
            "median": float(inv.median()),
            "max": float(inv.max()),
            "min": float(inv.min()),
            "one_tick_0_02": int(crossed.filter(pl.col("inversion") <= 0.0201).height),
            "le_0_10": int(crossed.filter(pl.col("inversion") <= 0.1001).height),
            "histogram": _value_counts(
                crossed.with_columns(pl.col("inversion").round(2)), "inversion"
            ),
        },
        "ask_price_distribution": _value_counts(crossed, "ask_price"),
        "ask_qty_distribution": _value_counts(crossed, "ask_qty"),
        "bid_price_range": [float(crossed["bid_price"].min()), float(crossed["bid_price"].max())],
        "ask_price_range": [float(crossed["ask_price"].min()), float(crossed["ask_price"].max())],
        "ask_is_25_00_n": int(crossed.filter(pl.col("ask_price") == 25.0).height),
        "ask_qty_1_or_2_n": int(crossed.filter(pl.col("ask_qty").is_in([1.0, 2.0])).height),
        "ask_qty_max": float(crossed["ask_qty"].max()),
        "crossed_by_session_phase": _value_counts(crossed, "session_phase"),
        "locked_by_session_phase": _value_counts(locked, "session_phase"),
        "locked_before_open": int(locked.filter(pl.col("transaction_ts") < _OPEN).height),
        "locked_after_close": int(locked.filter(pl.col("transaction_ts") >= _CLOSE).height),
        "session_boundaries_utc": SESSION,
        "crossed_state_sample": _rows(
            crossed.select(
                "transaction_ts", "ingress_ts", "bid_price", "ask_price", "bid_qty", "ask_qty", "inversion"
            ).head(8)
        ),
    }


def stale_reemission(tob: pl.DataFrame) -> dict:
    """Quote states published more than once, with a fresh capture time each time.

    This is the single mechanism behind both the crossed book and the skew tail.
    It is measured over the whole session rather than described from the worst rows.
    """
    key = ["transaction_ts", "bid_price", "bid_qty", "ask_price", "ask_qty"]
    work = tob.sort("ingress_ts").with_columns(
        pl.first("ingress_ts").over(key).alias("first_ingress"),
        pl.len().over(key).alias("state_rows"),
        pl.int_range(pl.len()).over(key).alias("emission_index"),
    ).with_columns(
        ((pl.col("ingress_ts") - pl.col("first_ingress")).dt.total_microseconds() / 1e6).alias("lag_s"),
        (pl.col("bid_price") > pl.col("ask_price")).alias("crossed"),
    )
    repeats = work.filter(pl.col("state_rows") > 1)
    late = repeats.filter(pl.col("lag_s") > 60)

    grp = tob.group_by(key).len()
    crossed_rows = work.filter(pl.col("crossed"))

    # the two clusters that dominate the skew tail, with their full emission schedule
    clusters = (
        repeats.filter(pl.col("lag_s") > 60)
        .group_by("transaction_ts")
        .agg(
            pl.len().alias("late_emissions"),
            pl.col("lag_s").max().alias("max_lag_s"),
            pl.col("bid_price").first(),
            pl.col("ask_price").first(),
        )
        .sort("max_lag_s", descending=True)
        .head(5)
    )
    schedules = []
    for ts in clusters["transaction_ts"].to_list():
        emissions = (
            work.filter(pl.col("transaction_ts") == ts)
            .sort("ingress_ts")
            .with_columns(
                (
                    (pl.col("ingress_ts") - pl.col("ingress_ts").shift(1)).dt.total_microseconds() / 1e6
                ).alias("interval_s")
            )
        )
        gaps = emissions["interval_s"].drop_nulls()
        schedules.append(
            {
                "transaction_ts": str(ts),
                "emissions": emissions.height,
                "first_ingress": str(emissions["ingress_ts"][0]),
                "last_ingress": str(emissions["ingress_ts"][-1]),
                "max_lag_s": float(emissions["lag_s"].max()),
                "interval_s_median": float(gaps.median()) if gaps.len() else None,
                "interval_s_min": float(gaps.min()) if gaps.len() else None,
                "interval_s_max": float(gaps.max()) if gaps.len() else None,
                "crossed_rows": int(emissions.filter(pl.col("crossed")).height),
                "rows": _rows(
                    emissions.select(
                        "ingress_ts", "bid_price", "ask_price", "bid_qty", "ask_qty", "lag_s", "interval_s"
                    )
                ),
            }
        )

    skew = tob.with_columns(
        ((pl.col("ingress_ts") - pl.col("transaction_ts")).dt.total_microseconds() / 1e6).alias("skew_s")
    )
    tail = skew.filter(pl.col("skew_s") > 60)
    tail_by_state = (
        tail.group_by("transaction_ts")
        .agg(
            pl.len().alias("n"),
            pl.col("skew_s").max().alias("max_skew_s"),
            pl.col("bid_price").first(),
            pl.col("ask_price").first(),
        )
        .sort("max_skew_s", descending=True)
    )

    return {
        "distinct_quote_states": grp.height,
        "states_emitted_more_than_once": int(grp.filter(pl.col("len") > 1).height),
        "extra_rows_from_repeats": int(
            grp.filter(pl.col("len") > 1).select((pl.col("len") - 1).sum()).item()
        ),
        "repeat_rows_over_60s_late": late.height,
        "repeat_rows_over_60s_late_and_crossed": int(late.filter(pl.col("crossed")).height),
        "crossed_rows_that_are_late_reemissions": int(crossed_rows.filter(pl.col("lag_s") > 60).height),
        "crossed_rows_total": crossed_rows.height,
        "skew_over_60s_rows": tail.height,
        "skew_over_60s_distinct_transaction_ts": int(tail["transaction_ts"].n_unique()),
        "skew_max_s": float(skew["skew_s"].max()),
        "skew_median_s": float(skew["skew_s"].median()),
        "skew_tail_by_venue_ts": _rows(tail_by_state),
        "skew_tail_ingress_hours": _value_counts(
            tail.with_columns(pl.col("ingress_ts").dt.hour().alias("ingress_hour")), "ingress_hour"
        ),
        "cluster_schedules": schedules,
    }


def trade_vs_book(tob: pl.DataFrame, trades: pl.DataFrame) -> dict:
    """Asof trades onto the prevailing quote, on both venue clocks.

    The join key is filtered non-null before the join and the match is carried by an
    explicit indicator from the quote side, so "matched" is not inferred from a price
    being non-null. `inside_spread` is evaluated against [min(bid,ask), max(bid,ask)]
    because `bid <= price <= ask` is unsatisfiable on a crossed quote. That test would
    create a trade-through for every trade that lands on a crossed quote.
    """
    tob_key = (
        tob.sort("transaction_ts")
        .select(
            "transaction_ts",
            "publish_ts",
            "bid_price",
            "ask_price",
            "bid_qty",
            "ask_qty",
            pl.col("seq_id").alias("quote_seq_id"),
        )
    )

    def _classify(df: pl.DataFrame) -> pl.DataFrame:
        lo = pl.min_horizontal("bid_price", "ask_price")
        hi = pl.max_horizontal("bid_price", "ask_price")
        return df.with_columns(
            (pl.col("bid_price") > pl.col("ask_price")).alias("quote_crossed"),
            (pl.col("bid_price") == pl.col("ask_price")).alias("quote_locked"),
            (pl.col("price") > pl.col("ask_price")).alias("above_ask"),
            (pl.col("price") < pl.col("bid_price")).alias("below_bid"),
            ((pl.col("price") >= lo) & (pl.col("price") <= hi)).alias("inside_spread"),
            ((pl.col("bid_price") + pl.col("ask_price")) / 2).alias("quote_mid"),
        )

    def _summary(matched: pl.DataFrame, joinable: int, key: str) -> dict:
        strict_through = matched.filter(pl.col("above_ask") | pl.col("below_bid"))
        tolerant_through = matched.filter(~pl.col("inside_spread"))
        return {
            "join_key": key,
            "joinable_trades": joinable,
            "matched": matched.height,
            "unmatched": joinable - matched.height,
            "inside_spread": int(matched.filter(pl.col("inside_spread")).height),
            "above_ask": int(matched.filter(pl.col("above_ask")).height),
            "below_bid": int(matched.filter(pl.col("below_bid")).height),
            "trade_through_strict": strict_through.height,
            "trade_through_strict_pct": strict_through.height / max(matched.height, 1) * 100.0,
            "trade_through_crossing_tolerant": tolerant_through.height,
            "trade_through_crossing_tolerant_pct": tolerant_through.height
            / max(matched.height, 1) * 100.0,
            "matched_on_crossed_quote": int(matched.filter(pl.col("quote_crossed")).height),
            "matched_on_locked_quote": int(matched.filter(pl.col("quote_locked")).height),
            "trade_through_rows": _rows(
                strict_through.select(
                    "trade_id", "transaction_ts", "publish_ts", "price", "qty",
                    "bid_price", "ask_price", "quote_crossed", "quote_locked",
                )
            ),
        }

    tx_trades = trades.filter(pl.col("transaction_ts").is_not_null()).sort("transaction_ts")
    tx_joined = _classify(
        tx_trades.join_asof(tob_key, on="transaction_ts", strategy="backward")
    )
    tx_matched = tx_joined.filter(pl.col("quote_seq_id").is_not_null())

    # publish_ts is byte-identical to transaction_ts wherever both exist, so it recovers
    # the trades the venue-time join drops without changing the quote being compared
    pub_key = tob_key.drop("transaction_ts").sort("publish_ts")
    pub_joined = _classify(
        trades.sort("publish_ts").join_asof(pub_key, on="publish_ts", strategy="backward")
    ).with_columns(pl.col("transaction_ts").is_null().alias("no_venue_time"))
    pub_matched = pub_joined.filter(pl.col("quote_seq_id").is_not_null())

    ties = int(
        tx_trades.join(tob.select("transaction_ts").unique(), on="transaction_ts", how="semi").height
    )
    ambiguous_ts = int(
        tob.group_by("transaction_ts")
        .agg(pl.struct("bid_price", "ask_price").n_unique().alias("nq"))
        .filter(pl.col("nq") > 1)
        .height
    )

    return {
        "transaction_ts_join": _summary(tx_matched, tx_trades.height, "transaction_ts"),
        "publish_ts_join": _summary(pub_matched, trades.height, "publish_ts"),
        "publish_ts_join_split": {
            "has_venue_time": _summary(
                pub_matched.filter(~pl.col("no_venue_time")),
                int(pub_joined.filter(~pl.col("no_venue_time")).height),
                "publish_ts|has_transaction_ts",
            ),
            "missing_venue_time": _summary(
                pub_matched.filter(pl.col("no_venue_time")),
                int(pub_joined.filter(pl.col("no_venue_time")).height),
                "publish_ts|null_transaction_ts",
            ),
        },
        "asof_ties": {
            "trades_on_an_exact_quote_timestamp": ties,
            "quote_timestamps_with_conflicting_quotes": ambiguous_ts,
            "convention": "backward asof, <= semantics; polars keeps the last row at a tied key",
        },
    }


def quote_rule_side(tob: pl.DataFrame, trades: pl.DataFrame) -> dict:
    """Lee-Ready quote rule against the prevailing quote.

    The file labels every trade Buy. The top of book is still usable. Side is
    recoverable for the trades that print exactly at a quote.
    """
    tob_key = tob.sort("transaction_ts").select(
        "transaction_ts", "bid_price", "ask_price", pl.col("seq_id").alias("quote_seq_id")
    )
    j = (
        trades.filter(pl.col("transaction_ts").is_not_null())
        .sort("transaction_ts")
        .join_asof(tob_key, on="transaction_ts", strategy="backward")
        .filter(pl.col("quote_seq_id").is_not_null())
        .with_columns(((pl.col("bid_price") + pl.col("ask_price")) / 2).alias("quote_mid"))
        .with_columns(
            pl.when(pl.col("price") == pl.col("ask_price"))
            .then(pl.lit("Buy"))
            .when(pl.col("price") == pl.col("bid_price"))
            .then(pl.lit("Sell"))
            .when(pl.col("price") > pl.col("quote_mid"))
            .then(pl.lit("Buy"))
            .when(pl.col("price") < pl.col("quote_mid"))
            .then(pl.lit("Sell"))
            .otherwise(pl.lit("unclassified"))
            .alias("recovered_side")
        )
    )
    at_ask = int(j.filter(pl.col("price") == pl.col("ask_price")).height)
    at_bid = int(j.filter(pl.col("price") == pl.col("bid_price")).height)
    at_touch = int(
        j.filter(
            (pl.col("price") == pl.col("ask_price")) | (pl.col("price") == pl.col("bid_price"))
        ).height
    )
    counts = {r["value"]: r["n"] for r in _value_counts(j, "recovered_side")}
    buy, sell = counts.get("Buy", 0), counts.get("Sell", 0)
    return {
        "rule": "at ask -> Buy, at bid -> Sell, else vs prevailing mid; ties unclassified",
        "joined_trades": j.height,
        "printed_at_bid_or_ask": at_touch,
        "printed_at_ask": at_ask,
        "printed_at_bid": at_bid,
        "recovered": counts,
        "buy": buy,
        "sell": sell,
        "unclassified": counts.get("unclassified", 0),
        "buy_pct_of_classified": buy / max(buy + sell, 1) * 100.0,
        "labelled_side_counts": _value_counts(trades, "side"),
    }


def missing_venue_time_cohort(tob: pl.DataFrame, trades: pl.DataFrame) -> dict:
    """Are the trades with no `transaction_ts` a random slice, or a distinct category?"""
    has = trades.filter(pl.col("transaction_ts").is_not_null())
    missing = trades.filter(pl.col("transaction_ts").is_null())
    total_vol = float(trades["qty"].sum())

    # 2800 trades in board lots of 500; a print that is not a multiple is off-board
    board_lot = 500

    def _profile(df: pl.DataFrame, label: str) -> dict:
        odd = int(df.filter(pl.col("qty") % board_lot != 0).height)
        return {
            "cohort": label,
            "n": df.height,
            "board_lot": board_lot,
            "odd_lot_n": odd,
            "odd_lot_pct": odd / max(df.height, 1) * 100.0,
            "pct_of_rows": df.height / trades.height * 100.0,
            "volume": float(df["qty"].sum()),
            "pct_of_volume": float(df["qty"].sum()) / total_vol * 100.0,
            "qty_median": float(df["qty"].median()),
            "qty_mean": float(df["qty"].mean()),
            "qty_max": float(df["qty"].max()),
            "distinct_prices": int(df["price"].n_unique()),
            "price_min": float(df["price"].min()),
            "price_max": float(df["price"].max()),
            "price_std": float(df["price"].std()),
        }

    with_phase = missing.with_columns(session_phase("publish_ts"))
    return {
        "missing": _profile(missing, "null_transaction_ts"),
        "present": _profile(has, "has_transaction_ts"),
        "qty_median_ratio": float(missing["qty"].median()) / float(has["qty"].median()),
        "price_std_ratio": float(missing["price"].std()) / float(has["price"].std()),
        "publish_ts_nulls_in_cohort": int(missing["publish_ts"].null_count()),
        "by_publish_session_phase": _value_counts(with_phase, "session_phase"),
        "by_ingress_hour": _value_counts(
            missing.with_columns(pl.col("ingress_ts").dt.hour().alias("ingress_hour")), "ingress_hour"
        ),
        "qty_value_counts_top": _value_counts(missing, "qty", top=10),
        "qty_value_counts_present_top": _value_counts(has, "qty", top=10),
        "clock_identity": {
            "tob_publish_equals_transaction": int(
                tob.filter(pl.col("publish_ts") == pl.col("transaction_ts")).height
            ),
            "tob_rows": tob.height,
            "trades_publish_equals_transaction": int(
                has.filter(pl.col("publish_ts") == pl.col("transaction_ts")).height
            ),
            "trades_with_both_clocks": has.height,
        },
    }


def main() -> None:
    ensure_dirs()
    tob = load_parquet("A_tob")
    trades = load_parquet("A_trades")

    findings = []
    for label, df in (
        ("A_hkex_2800_top_of_book.parquet", tob),
        ("A_hkex_2800_trades.parquet", trades),
    ):
        p = profile_frame(df, label)
        findings.extend(p["findings"])
        for f in all_skews(df, label):
            findings.append(f.to_dict())

    coverage = {
        "tob_tx_min": str(tob["transaction_ts"].min()),
        "tob_tx_max": str(tob["transaction_ts"].max()),
        "trades_tx_min": str(trades["transaction_ts"].drop_nulls().min()),
        "trades_tx_max": str(trades["transaction_ts"].drop_nulls().max()),
        "trades_ingress_min": str(trades["ingress_ts"].min()),
        "trades_ingress_max": str(trades["ingress_ts"].max()),
        "tob_rows": tob.height,
        "trade_rows": trades.height,
        "trades_null_transaction_ts": int(trades["transaction_ts"].null_count()),
        "trades_null_transaction_ts_pct": trades["transaction_ts"].null_count() / trades.height * 100.0,
        "instruments_tob": tob["instrument"].unique().to_list(),
        "instruments_trades": trades["instrument"].unique().to_list(),
        "tob_null_bid_rows": _rows(
            tob.filter(pl.col("bid_price").is_null()).select("transaction_ts", "bid_price", "ask_price")
        ),
        "tob_null_ask_rows": _rows(
            tob.filter(pl.col("ask_price").is_null()).select("transaction_ts", "bid_price", "ask_price")
        ),
    }

    by_hour = (
        trades.with_columns(pl.col("transaction_ts").dt.hour().alias("utc_hour"))
        .group_by("utc_hour")
        .agg(pl.len().alias("n_trades"))
        .sort("utc_hour")
        .to_dicts()
    )

    out = {
        "coverage": coverage,
        "gap_summary": gap_table(tob),
        "crossed_locked": [f.to_dict() for f in tob_crossed_locked(tob)],
        "crossed_locked_detail": crossed_locked_detail(tob),
        "stale_reemission": stale_reemission(tob),
        "trade_vs_tob": trade_vs_book(tob, trades),
        "side_recovery": quote_rule_side(tob, trades),
        "missing_venue_time": missing_venue_time_cohort(tob, trades),
        "trades_by_utc_hour": by_hour,
        "side_counts": trades["side"].value_counts().to_dicts(),
        "findings": findings,
    }
    write_table_json("A_hkex.json", out)
    tx = out["trade_vs_tob"]["transaction_ts_join"]
    print("gaps_over_300s", out["gap_summary"]["gaps_over_300s"], "over_60s", out["gap_summary"]["gaps_over_60s"])
    print("matched", tx["matched"], "through strict", tx["trade_through_strict"],
          "tolerant", tx["trade_through_crossing_tolerant"])
    print("side recovery", out["side_recovery"]["buy"], "buy /", out["side_recovery"]["sell"], "sell")
    print("wrote outputs/tables/A_hkex.json")


if __name__ == "__main__":
    main()
