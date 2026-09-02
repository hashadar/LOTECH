"""DreamDex WETH microprice series + DQ."""
from __future__ import annotations

import sys
from pathlib import Path

import matplotlib.pyplot as plt
import polars as pl

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from lotech_dq.checks import profile_frame, tob_crossed_locked  # noqa: E402
from lotech_dq.clocks import all_skews  # noqa: E402
from lotech_dq.io import FIGURES_DIR, TABLES_DIR, ensure_dirs, load_parquet  # noqa: E402
from lotech_dq.microprice import add_microprice, resolve_tob_cols  # noqa: E402
from lotech_dq.report import write_table_json  # noqa: E402

SERIES_COLS = [
    "ingress_ts",
    "bid_price",
    "bid_qty",
    "ask_price",
    "ask_qty",
    "mid",
    "spread",
    "microprice",
    "microprice_status",
    "microprice_reason",
]


def ask_only_runs(mp: pl.DataFrame) -> pl.DataFrame:
    flagged = mp.with_columns(
        (pl.col("bid_price").is_null() != pl.col("bid_price").shift(1).is_null())
        .fill_null(True)
        .cum_sum()
        .alias("run")
    )
    return (
        flagged.filter(pl.col("bid_price").is_null())
        .group_by("run")
        .agg(
            pl.len().alias("n"),
            pl.col("ingress_ts").min().alias("start"),
            pl.col("ingress_ts").max().alias("end"),
        )
        .sort("n", descending=True)
    )


def vanishing_bid_evidence(mp: pl.DataFrame, runs: pl.DataFrame) -> dict:
    """Discriminate a dropped bid side in the normaliser from a genuinely one-sided book.

    Three checks the file can settle. Does the bid decay, or does it go null between two
    consecutive updates? Do the two bid fields ever go null independently? Does capture
    stall at the same moment?
    """
    longest = runs.head(1).to_dicts()[0]
    start = longest["start"]
    idx = mp.with_row_index("i").filter(pl.col("ingress_ts") == start)["i"][0]
    boundary = mp.slice(max(idx - 1, 0), 2)
    before, after = boundary.to_dicts()[0], boundary.to_dicts()[-1]
    delta_us = int((after["ingress_ts"] - before["ingress_ts"]).total_seconds() * 1_000_000)

    run = mp.slice(idx, longest["n"])
    ask_changes = int(
        run.select((pl.col("ask_price") != pl.col("ask_price").shift(1)).sum()).item() or 0
    )
    run_gaps = run.select(
        (pl.col("ingress_ts").diff().dt.total_microseconds() / 1e6).alias("gap_s")
    ).drop_nulls()

    return {
        "last_two_sided_row": {
            "ingress_ts": str(before["ingress_ts"]),
            "bid_price": before["bid_price"],
            "bid_qty": before["bid_qty"],
            "ask_price": before["ask_price"],
            "ask_qty": before["ask_qty"],
        },
        "first_ask_only_row": {
            "ingress_ts": str(after["ingress_ts"]),
            "bid_price": after["bid_price"],
            "bid_qty": after["bid_qty"],
            "ask_price": after["ask_price"],
            "ask_qty": after["ask_qty"],
        },
        "boundary_delta_us": delta_us,
        "ask_identical_across_boundary": bool(
            before["ask_price"] == after["ask_price"] and before["ask_qty"] == after["ask_qty"]
        ),
        "bid_price_null_n": int(mp["bid_price"].null_count()),
        "bid_qty_null_n": int(mp["bid_qty"].null_count()),
        "bid_price_null_only": int(
            mp.filter(pl.col("bid_price").is_null() & pl.col("bid_qty").is_not_null()).height
        ),
        "bid_qty_null_only": int(
            mp.filter(pl.col("bid_qty").is_null() & pl.col("bid_price").is_not_null()).height
        ),
        "bid_qty_zero_n": int(mp.filter(pl.col("bid_qty") == 0).height),
        "ask_price_null_n": int(mp["ask_price"].null_count()),
        "ask_qty_null_n": int(mp["ask_qty"].null_count()),
        "ask_qty_zero_n": int(mp.filter(pl.col("ask_qty") == 0).height),
        "run_rows": run.height,
        "run_ask_price_changes": ask_changes,
        "run_distinct_ask_prices": int(run["ask_price"].n_unique()),
        "run_distinct_ask_qtys": int(run["ask_qty"].n_unique()),
        "run_ask_price_min": float(run["ask_price"].min()),
        "run_ask_price_max": float(run["ask_price"].max()),
        "run_max_ingress_gap_s": float(run_gaps["gap_s"].max()),
        "run_duration_s": float(
            (run["ingress_ts"].max() - run["ingress_ts"].min()).total_seconds()
        ),
    }


def make_figure(mp: pl.DataFrame, runs: pl.DataFrame, path: Path) -> None:
    """Plot on the time axis with explicit limits.

    Row index plus autoscale drops the last 1,513-row ask-only run off the right
    edge. Marking undefined points by scattering `mid` marks nothing, because `mid`
    is null on exactly those rows. The undefined region is shaded. The surviving ask
    is drawn through it instead.
    """
    t = mp["ingress_ts"].to_list()
    fig, ax = plt.subplots(figsize=(12, 5))
    ax.plot(t, mp["mid"].to_list(), lw=0.7, alpha=0.7, label="mid")
    ax.plot(t, mp["microprice"].to_list(), lw=0.8, label="microprice")

    # null outside the undefined rows so matplotlib breaks the line between runs
    n_undefined = int(mp.filter(pl.col("microprice_status") == "undefined").height)
    if n_undefined:
        ask_when_undefined = mp.select(
            pl.when(pl.col("microprice_status") == "undefined")
            .then(pl.col("ask_price"))
            .otherwise(None)
            .alias("v")
        )["v"].to_list()
        ax.plot(
            t,
            ask_when_undefined,
            lw=0.8,
            color="tab:red",
            label=f"ask only, microprice undefined ({n_undefined} rows)",
        )
    for r in runs.to_dicts():
        ax.axvspan(r["start"], r["end"], color="tab:red", alpha=0.10, lw=0)

    ax.set_xlim(t[0], t[-1])

    # a handful of ask prints sit about 5% above the band and compress the vertical scale
    body = pl.concat(
        [mp.select(pl.col(c).alias("v")) for c in ("mid", "microprice", "ask_price")]
    ).drop_nulls()["v"]
    lo, hi = float(body.quantile(0.001)), float(body.quantile(0.999))
    pad = (hi - lo) * 0.15
    above = int(mp.filter(pl.col("ask_price") > hi + pad).height)
    ax.set_ylim(lo - pad, hi + pad)
    if above:
        ax.text(
            0.995,
            0.02,
            f"{above} ask prints above the axis, max {float(mp['ask_price'].max()):.2f}",
            transform=ax.transAxes,
            ha="right",
            fontsize=8,
            color="tab:red",
        )

    longest = runs.head(1).to_dicts()[0]
    ax.annotate(
        f"bid_price null from {longest['start']:%H:%M:%S} to end of file ({longest['n']} rows)",
        xy=(longest["start"], hi),
        xytext=(0.40, 0.94),
        textcoords="axes fraction",
        fontsize=9,
        arrowprops={"arrowstyle": "->", "lw": 0.8},
    )
    ax.set_title(
        f"B DreamDex WETH-USDso: microprice ({t[0]:%Y-%m-%d %H:%M:%S} - {t[-1]:%H:%M:%S} UTC)"
    )
    ax.set_xlabel("ingress_ts (UTC)")
    ax.set_ylabel("Price (USDso)")
    ax.legend(loc="upper left")
    fig.autofmt_xdate()
    fig.tight_layout()
    fig.savefig(path, dpi=120)
    plt.close(fig)


def main() -> None:
    ensure_dirs()
    df = load_parquet("B").sort("ingress_ts")
    profile = profile_frame(df, "B_dreamdex_weth_top_of_book.parquet")
    for f in all_skews(df, profile["file"]):
        profile["findings"].append(f.to_dict())

    cols = resolve_tob_cols(df)
    mp = add_microprice(df)
    undefined = int(mp.filter(pl.col("microprice_status") == "undefined").height)
    ok_rows = mp.filter(pl.col("microprice_status") == "ok")

    gaps = mp.select(
        (pl.col("ingress_ts").diff().dt.total_microseconds() / 1e6).alias("gap_s")
    ).drop_nulls()
    runs = ask_only_runs(mp)

    series_path = TABLES_DIR / "B_microprice_series.parquet"
    mp.select(SERIES_COLS).write_parquet(series_path)

    fig_path = FIGURES_DIR / "B_microprice.png"
    make_figure(mp, runs, fig_path)

    out = {
        "columns_resolved": cols,
        "rows": mp.height,
        "window": {"ingress_min": str(df["ingress_ts"].min()), "ingress_max": str(df["ingress_ts"].max())},
        "instrument": df["instrument"].unique().to_list(),
        "microprice_ok": ok_rows.height,
        "microprice_undefined": undefined,
        "undefined_pct": undefined / mp.height * 100.0,
        "undefined_reasons": [
            {"reason": r["microprice_reason"], "n": r["count"]}
            for r in mp.filter(pl.col("microprice_status") == "undefined")["microprice_reason"]
            .value_counts()
            .sort("count", descending=True)
            .to_dicts()
        ],
        # over the defined rows only: on a file with crossed quotes or null sizes the
        # "both prices present" set and the "microprice defined" set are not the same
        "spread_median_defined": float(ok_rows["spread"].median()),
        "spread_max_defined": float(ok_rows["spread"].max()),
        "spread_min_defined": float(ok_rows["spread"].min()),
        "gaps_over_60s": int(gaps.filter(pl.col("gap_s") > 60).height),
        "max_gap_s": float(gaps["gap_s"].max()),
        "ask_only_runs": runs.height,
        "ask_only_longest_run": int(runs["n"].max()),
        "ask_only_median_run": float(runs["n"].median()),
        "ask_only_run_lengths": sorted(runs["n"].to_list()),
        "ask_only_runs_detail": [
            {"n": r["n"], "start": str(r["start"]), "end": str(r["end"])} for r in runs.to_dicts()
        ],
        "vanishing_bid_evidence": vanishing_bid_evidence(mp, runs),
        "crossed": [f.to_dict() for f in tob_crossed_locked(df)],
        "profile_findings": profile["findings"],
        "figure": fig_path.name,
        "series_artefact": series_path.name,
        "definition": (
            "microprice = (ask_qty * bid_price + bid_qty * ask_price) / (bid_qty + ask_qty); "
            "size-weighted mid. Undefined when any price or size is null, either size is "
            "negative, bid_qty+ask_qty <= 0, or the book is crossed. Never forward-filled."
        ),
    }
    write_table_json("B_dreamdex_microprice.json", out)
    print(out["microprice_ok"], "ok,", out["microprice_undefined"], "undefined")
    print("runs", out["ask_only_runs"], "longest", out["ask_only_longest_run"])
    print("wrote", series_path, "and", fig_path)


if __name__ == "__main__":
    main()
