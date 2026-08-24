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
from lotech_dq.io import FIGURES_DIR, ensure_dirs, load_parquet  # noqa: E402
from lotech_dq.microprice import add_microprice, resolve_tob_cols  # noqa: E402
from lotech_dq.report import write_table_json  # noqa: E402


def main() -> None:
    ensure_dirs()
    df = load_parquet("B").sort("ingress_ts")
    profile = profile_frame(df, "B_dreamdex_weth_top_of_book.parquet")
    for f in all_skews(df, profile["file"]):
        profile["findings"].append(f.to_dict())

    cols = resolve_tob_cols(df)
    mp = add_microprice(df)
    undefined = int(mp.filter(pl.col("microprice_status") == "undefined").height)
    ok = int(mp.filter(pl.col("microprice_status") == "ok").height)

    # large ingress gaps
    gaps = mp.select(pl.col("ingress_ts").diff().dt.total_seconds().alias("gap_s")).drop_nulls()
    large_gaps = gaps.filter(pl.col("gap_s") > 60)

    fig, ax = plt.subplots(figsize=(12, 5))
    # use row index for x to avoid tz plotting issues; annotate time range in title
    x = range(mp.height)
    ax.plot(x, mp["mid"].to_list(), lw=0.7, alpha=0.7, label="mid")
    ax.plot(x, mp["microprice"].to_list(), lw=0.8, label="microprice")
    # mark undefined
    undef_idx = [i for i, s in enumerate(mp["microprice_status"].to_list()) if s == "undefined"]
    if undef_idx:
        ax.scatter(
            undef_idx,
            [mp["mid"][i] for i in undef_idx],
            s=8,
            c="red",
            alpha=0.5,
            label="microprice undefined",
        )
    t0, t1 = str(df["ingress_ts"][0]), str(df["ingress_ts"][-1])
    ax.set_title(f"B DreamDex WETH microprice ({t0} → {t1})")
    ax.set_xlabel("Update index")
    ax.set_ylabel("Price")
    ax.legend(loc="best")
    fig.tight_layout()
    fig_path = FIGURES_DIR / "B_microprice.png"
    fig.savefig(fig_path, dpi=120)
    plt.close(fig)

    out = {
        "columns_resolved": cols,
        "rows": mp.height,
        "microprice_ok": ok,
        "microprice_undefined": undefined,
        "undefined_pct": undefined / mp.height * 100.0,
        "spread_median": float(mp["spread"].drop_nulls().median()),  # type: ignore[arg-type]
        "spread_max": float(mp["spread"].drop_nulls().max()),
        "gaps_over_60s": large_gaps.height,
        "max_gap_s": float(gaps["gap_s"].max()) if gaps.height else None,
        "crossed": [f.to_dict() for f in tob_crossed_locked(df)],
        "profile_findings": profile["findings"],
        "figure": fig_path.name,
        "definition": (
            "microprice = (ask_qty * bid_price + bid_qty * ask_price) / (bid_qty + ask_qty); "
            "undefined when size_sum<=0, nulls, or crossed book"
        ),
    }
    write_table_json("B_dreamdex_microprice.json", out)
    print(out["microprice_ok"], "ok,", out["microprice_undefined"], "undefined")
    print("wrote", fig_path)


if __name__ == "__main__":
    main()
