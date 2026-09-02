"""Replay Binance BTCUSDT incremental L2, reconstruct the book and report integrity."""
from __future__ import annotations

import sys
from datetime import timedelta
from pathlib import Path

import matplotlib.pyplot as plt
import polars as pl

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from lotech_dq.book import message_is_internally_crossed, replay_order_book  # noqa: E402
from lotech_dq.checks import profile_frame  # noqa: E402
from lotech_dq.clocks import all_skews  # noqa: E402
from lotech_dq.io import FIGURES_DIR, TABLES_DIR, ensure_dirs, load_parquet  # noqa: E402
from lotech_dq.report import write_table_json  # noqa: E402

SWEEP_THRESHOLDS = (None, 20, 50, 100, 150, 200, 250, 300, 400, 500, 1000)


def _rows(df: pl.DataFrame) -> list[dict]:
    return [
        {k: (str(v) if hasattr(v, "isoformat") else v) for k, v in r.items()}
        for r in df.to_dicts()
    ]


def with_time(top: pl.DataFrame) -> pl.DataFrame:
    return top.with_columns(
        pl.from_epoch(pl.col("ingress_ts_us"), time_unit="us")
        .dt.replace_time_zone("UTC")
        .alias("ingress_ts")
    ).drop("ingress_ts_us")


def crossed_episodes(top: pl.DataFrame) -> pl.DataFrame:
    """Contiguous runs of crossed states. That is what an event should mean."""
    flagged = top.with_row_index("i").with_columns(
        (pl.col("crossed") != pl.col("crossed").shift(1)).fill_null(True).cum_sum().alias("run")
    )
    return (
        flagged.filter(pl.col("crossed"))
        .group_by("run")
        .agg(
            pl.len().alias("messages"),
            pl.col("i").min().alias("start_index"),
            pl.col("i").max().alias("end_index"),
            pl.col("ingress_ts").min().alias("start_ts"),
            pl.col("ingress_ts").max().alias("end_ts"),
            pl.col("seq_id").min().alias("first_seq_id"),
            pl.col("spread").min().alias("worst_spread"),
        )
        .with_columns(
            (
                (pl.col("end_ts") - pl.col("start_ts")).dt.total_microseconds() / 1e6
            ).alias("duration_s")
        )
        .sort("start_index")
        .drop("run")
    )


def threshold_sweep(df: pl.DataFrame) -> list[dict]:
    """How much of the crossed count is the data, and how much is the threshold.

    Run without snapshot validation so the numbers are comparable to the unguarded
    heuristic the shipped code used.
    """
    out = []
    for t in SWEEP_THRESHOLDS:
        r = replay_order_book(df, snapshot_level_threshold=t, validate_snapshots=False)
        integ = r["integrity"]
        first = r["top_of_book"].with_row_index("i").filter(pl.col("crossed")).head(1)
        out.append(
            {
                "threshold": t,
                "snapshots_taken": integ["snapshot_rows_heuristic_accepted"],
                "crossed_states": integ["crossed_states"],
                "crossed_pct": integ["crossed_pct"],
                "crossed_episodes": integ["crossed_episodes"],
                "deletes_missed": integ["deletes_missed"],
                "deletes_missed_pct": integ["deletes_missed_pct"],
                "first_crossed_index": int(first["i"][0]) if first.height else None,
                "first_crossed_seq_id": int(first["seq_id"][0]) if first.height else None,
            }
        )
    return out


def message_diagnostics(df: pl.DataFrame) -> dict:
    """Message-local checks that involve no replay state at all.

    An internally crossed message is a source defect by construction. It cannot be
    produced by a reconstruction error, because nothing is reconstructed.
    """
    work = df.sort("seq_id")
    bp, bq = work["bid_prices"].to_list(), work["bid_qtys"].to_list()
    ap, aq = work["ask_prices"].to_list(), work["ask_qtys"].to_list()
    seq = work["seq_id"].to_list()
    ts = work["ingress_ts"].to_list()
    n = work.height

    price_side_counts: dict[float, dict[str, int]] = {}
    for i in range(n):
        for p in bp[i]:
            price_side_counts.setdefault(p, {"bid": 0, "ask": 0})["bid"] += 1
        for p in ap[i]:
            price_side_counts.setdefault(p, {"bid": 0, "ask": 0})["ask"] += 1

    culprits = [
        i for i in range(n) if message_is_internally_crossed(bp[i], bq[i], ap[i], aq[i])
    ]

    details = []
    for i in culprits:
        live_bids = sorted((p for p, q in zip(bp[i], bq[i], strict=True) if q), reverse=True)
        live_asks = sorted(p for p, q in zip(ap[i], aq[i], strict=True) if q)
        sub_market = [p for p in live_asks if p < live_bids[0]]
        details.append(
            {
                "index": i,
                "seq_id": seq[i],
                "ingress_ts": str(ts[i]),
                "local_best_bid": live_bids[0],
                "local_best_ask": live_asks[0],
                "local_spread": live_asks[0] - live_bids[0],
                "n_levels": len(bp[i]) + len(ap[i]),
                "sub_market_ask_levels": [
                    {
                        "price": p,
                        "qty": next(q for pp, q in zip(ap[i], aq[i], strict=True) if pp == p),
                        "times_seen_as_ask_in_file": price_side_counts[p]["ask"],
                        "times_seen_as_bid_in_file": price_side_counts[p]["bid"],
                    }
                    for p in sub_market
                ],
            }
        )

    # the market the neighbouring messages quote, for comparison
    neighbours = []
    if culprits:
        c = culprits[0]
        for i in range(max(c - 4, 0), min(c + 5, n)):
            live_bids = [p for p, q in zip(bp[i], bq[i], strict=True) if q]
            live_asks = [p for p, q in zip(ap[i], aq[i], strict=True) if q]
            neighbours.append(
                {
                    "index": i,
                    "seq_id": seq[i],
                    "local_best_bid": max(live_bids) if live_bids else None,
                    "local_best_ask": min(live_asks) if live_asks else None,
                }
            )

    all_prices = list(price_side_counts)
    return {
        "messages": n,
        "messages_internally_crossed": len(culprits),
        "internally_crossed_detail": details,
        "neighbouring_messages": neighbours,
        "distinct_prices": len(all_prices),
        "prices_not_exact_2dp": sum(1 for p in all_prices if p != round(p, 2)),
    }


def heuristic_and_delete_diagnostics(df: pl.DataFrame, threshold: int = 200) -> dict:
    """Are the heuristic's snapshots actually snapshots, and where do the deletes land?

    Runs the unguarded threshold-200 policy so the delete accounting is comparable with
    the existing output. Indexes the book by scaled price so the float-key hypothesis
    can be tested rather than assumed away.
    """
    work = df.sort("seq_id")
    bp, bq = work["bid_prices"].to_list(), work["bid_qtys"].to_list()
    ap, aq = work["ask_prices"].to_list(), work["ask_qtys"].to_list()
    n = work.height

    heur = [i for i in range(n) if len(bp[i]) + len(ap[i]) >= threshold]
    heur_with_deletes = 0
    heur_delete_entries = 0
    for i in heur:
        d = sum(1 for q in bq[i] if q == 0) + sum(1 for q in aq[i] if q == 0)
        if d:
            heur_with_deletes += 1
            heur_delete_entries += d

    bids: dict[float, float] = {}
    asks: dict[float, float] = {}
    scaled: dict[str, dict[int, set]] = {"bid": {}, "ask": {}}
    hit = missed = near_miss = 0

    def _add(side, book, p, q):
        book[p] = q
        scaled[side].setdefault(round(p * 1_000_000), set()).add(p)

    def _drop(side, book, p):
        del book[p]
        s = round(p * 1_000_000)
        scaled[side][s].discard(p)
        if not scaled[side][s]:
            del scaled[side][s]

    for i in range(n):
        if len(bp[i]) + len(ap[i]) >= threshold or i == 0:
            bids.clear()
            asks.clear()
            scaled["bid"].clear()
            scaled["ask"].clear()
        for side, book, ps, qs in (("bid", bids, bp[i], bq[i]), ("ask", asks, ap[i], aq[i])):
            for p, q in zip(ps, qs, strict=True):
                if q == 0:
                    if p in book:
                        hit += 1
                        _drop(side, book, p)
                    else:
                        missed += 1
                        # a level within 1e-6 of the delete price would mean the miss is
                        # float-key fragility rather than a genuinely unknown level
                        if scaled[side].get(round(p * 1_000_000)):
                            near_miss += 1
                else:
                    _add(side, book, p, q)

    return {
        "threshold": threshold,
        "messages_at_or_over_threshold": len(heur),
        "of_which_contain_deletes": heur_with_deletes,
        "delete_entries_inside_them": heur_delete_entries,
        "deletes_applied": hit,
        "deletes_missed": missed,
        "deletes_total": hit + missed,
        "deletes_missed_pct": missed / (hit + missed) * 100.0,
        "missed_deletes_with_float_near_miss": near_miss,
        "verdict": (
            "every message the heuristic would treat as a snapshot carries delete "
            "instructions, so none of them is a snapshot"
            if heur_with_deletes == len(heur) and heur
            else "some candidate snapshots carry no deletes"
        ),
    }


def make_figure(primary: pl.DataFrame, legacy: pl.DataFrame, episode: dict, path: Path) -> None:
    """Time axis, both snapshot policies, and a zoom on the inversion.

    Plotting the two policies together is the point. The difference between the lines is
    what the level-count threshold is choosing, not what the data says.
    """
    fig, axes = plt.subplots(3, 1, figsize=(12, 10))
    t_p, t_l = primary["ingress_ts"].to_list(), legacy["ingress_ts"].to_list()

    axes[0].plot(t_l, legacy["mid"].to_list(), lw=0.8, color="tab:blue",
                 label="mid, >=200-level messages treated as snapshots")
    axes[0].plot(t_p, primary["mid"].to_list(), lw=0.8, color="tab:red", alpha=0.8,
                 label="mid, snapshot candidates validated (none accepted)")
    axes[0].set_ylabel("Mid (USDT)")
    axes[0].set_title("D Binance BTCUSDT reconstructed top of book, 2026-08-13")
    axes[0].legend(loc="lower left", fontsize=8)

    axes[1].plot(t_l, legacy["spread"].to_list(), lw=0.8, color="tab:blue", label="spread, threshold 200")
    axes[1].plot(t_p, primary["spread"].to_list(), lw=0.8, color="tab:red", alpha=0.8,
                 label="spread, validated")
    axes[1].set_yscale("symlog", linthresh=0.1)
    axes[1].axhline(0.0, color="k", lw=0.5)
    axes[1].set_ylabel("Spread (USDT, symlog)")
    axes[1].legend(loc="lower left", fontsize=8)

    start = episode["start_ts"]
    end = episode["end_ts"]
    for ax in axes[:2]:
        ax.axvline(start, color="k", ls="--", lw=0.7)
        ax.set_xlim(t_p[0], t_p[-1])
    axes[0].annotate(
        f"first crossed state {start:%H:%M:%S.%f}\nseq_id {episode['first_seq_id']}",
        xy=(start, float(legacy["mid"].drop_nulls().min())),
        xytext=(0.12, 0.62),
        textcoords="axes fraction",
        fontsize=8,
        arrowprops={"arrowstyle": "->", "lw": 0.8},
    )

    zoom_l = legacy.filter(
        (pl.col("ingress_ts") >= start - timedelta(seconds=5))
        & (pl.col("ingress_ts") <= end + timedelta(seconds=10))
    )
    axes[2].plot(zoom_l["ingress_ts"].to_list(), zoom_l["best_bid"].to_list(), lw=2.2,
                 color="tab:green", label="best bid (threshold 200)")
    axes[2].plot(zoom_l["ingress_ts"].to_list(), zoom_l["best_ask"].to_list(), lw=1.0,
                 ls="--", color="tab:orange", label="best ask (threshold 200)")
    axes[2].axvspan(start, end, color="tab:red", alpha=0.12, lw=0)
    axes[2].set_ylabel("Price (USDT)")
    axes[2].set_xlabel("ingress_ts (UTC)")
    axes[2].set_title(
        f"Crossed-book window: {episode['duration_s']:.1f} s, "
        f"{episode['messages']} crossed messages, worst spread {episode['worst_spread']:.2f} USDT",
        fontsize=9,
    )
    axes[2].legend(loc="center left", fontsize=8)

    # each panel carries its own x range, so each needs its own labels
    for ax in axes:
        ax.tick_params(axis="x", labelrotation=20, labelsize=8)
    axes[0].set_xlabel("ingress_ts (UTC), full window", fontsize=8)
    axes[1].set_xlabel("ingress_ts (UTC), full window", fontsize=8)
    fig.tight_layout()
    fig.savefig(path, dpi=120)
    plt.close(fig)


def main() -> None:
    ensure_dirs()
    df = load_parquet("D")
    profile = profile_frame(df, "D_binance_btcusdt_orderbook_incremental.parquet")
    for f in all_skews(df, profile["file"]):
        profile["findings"].append(f.to_dict())

    snap_true = int(df.select(pl.col("snapshot").fill_null(False).sum()).item())
    level_stats = df.select(
        (pl.col("bid_prices").list.len() + pl.col("ask_prices").list.len()).alias("n_lvl")
    )

    primary_res = replay_order_book(df, snapshot_level_threshold=200, validate_snapshots=True)
    legacy_res = replay_order_book(df, snapshot_level_threshold=200, validate_snapshots=False)
    primary = with_time(primary_res["top_of_book"])
    legacy = with_time(legacy_res["top_of_book"])

    primary_episodes = crossed_episodes(primary)
    legacy_episodes = crossed_episodes(legacy)

    uncrossed = primary.filter(~pl.col("crossed") & pl.col("mid").is_not_null())
    legacy_uncrossed = legacy.filter(~pl.col("crossed") & pl.col("mid").is_not_null())

    series_path = TABLES_DIR / "D_top_of_book_series.parquet"
    primary.write_parquet(series_path)
    legacy_path = TABLES_DIR / "D_top_of_book_series_threshold200.parquet"
    legacy.write_parquet(legacy_path)
    final_book = primary_res["final"].to_frame()
    book_path = TABLES_DIR / "D_final_book.csv"
    final_book.write_csv(book_path)

    fig_path = FIGURES_DIR / "D_binance_l2_mid_spread.png"
    make_figure(primary, legacy, legacy_episodes.to_dicts()[0], fig_path)

    out = {
        "window": {
            "ingress_min": str(df["ingress_ts"].min()),
            "ingress_max": str(df["ingress_ts"].max()),
            "duration_min": float(
                (df["ingress_ts"].max() - df["ingress_ts"].min()).total_seconds() / 60.0
            ),
            "instrument": df["instrument"].unique().to_list(),
            "rows": df.height,
        },
        "snapshot_flag": {
            "true_count": snap_true,
            "null_count": int(df["snapshot"].null_count()),
            "dtype": str(df.schema["snapshot"]),
        },
        "level_stats": {
            "median_n_lvl": float(level_stats["n_lvl"].median()),
            "max_n_lvl": int(level_stats["n_lvl"].max()),
            "p99_n_lvl": float(level_stats.select(pl.col("n_lvl").quantile(0.99)).item()),
        },
        "replay_primary": {
            "policy": "snapshot flag only; level-count candidates validated against deletes; first row seeded",
            "integrity": primary_res["integrity"],
            "crossed_episodes_detail": _rows(primary_episodes),
            "final_book_levels": final_book.height,
        },
        "replay_threshold_200_unvalidated": {
            "policy": "unguarded heuristic: any message with >=200 levels clears and replaces the book",
            "integrity": legacy_res["integrity"],
            "crossed_episodes_detail": _rows(legacy_episodes),
        },
        "healthy_mid_range": {
            "note": "uncrossed states only; taking the range over all rows puts the crossed mid in as the floor",
            "primary_uncrossed_mid_min": float(uncrossed["mid"].min()),
            "primary_uncrossed_mid_max": float(uncrossed["mid"].max()),
            "primary_uncrossed_spread_median": float(uncrossed["spread"].median()),
            "threshold200_uncrossed_mid_min": float(legacy_uncrossed["mid"].min()),
            "threshold200_uncrossed_mid_max": float(legacy_uncrossed["mid"].max()),
            "threshold200_uncrossed_spread_median": float(legacy_uncrossed["spread"].median()),
            "threshold200_all_rows_mid_min": float(legacy["mid"].drop_nulls().min()),
            "threshold200_all_rows_mid_max": float(legacy["mid"].drop_nulls().max()),
        },
        "snapshot_threshold_sweep": threshold_sweep(df),
        "heuristic_validation": heuristic_and_delete_diagnostics(df),
        "message_diagnostics": message_diagnostics(df),
        "seq_id": {
            "gap_sum": legacy_res["integrity"]["seq_id_gaps"],
            "median_step": float(
                df.sort("seq_id").select(pl.col("seq_id").diff().alias("d")).drop_nulls()["d"].median()
            ),
            "p99_step_nearest": float(
                df.sort("seq_id")
                .select(pl.col("seq_id").diff().alias("d"))
                .drop_nulls()
                .select(pl.col("d").quantile(0.99, interpolation="nearest"))
                .item()
            ),
            "max_step": int(
                df.sort("seq_id").select(pl.col("seq_id").diff().alias("d")).drop_nulls()["d"].max()
            ),
        },
        "artefacts": {
            "top_of_book_series": series_path.name,
            "top_of_book_series_threshold200": legacy_path.name,
            "final_book": book_path.name,
            "figure": fig_path.name,
        },
        "profile_findings": profile["findings"],
    }
    write_table_json("D_binance_l2.json", out)
    print("primary crossed", primary_res["integrity"]["crossed_states"],
          "episodes", primary_res["integrity"]["crossed_episodes"])
    print("threshold200 crossed", legacy_res["integrity"]["crossed_states"],
          "episodes", legacy_res["integrity"]["crossed_episodes"])
    print("heuristic", out["heuristic_validation"]["verdict"])
    print("wrote", series_path, book_path, fig_path)


if __name__ == "__main__":
    main()
