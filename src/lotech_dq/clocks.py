from __future__ import annotations

from typing import Any

import polars as pl

from lotech_dq.checks import Finding, infer_ts_unit, to_datetime


def ensure_datetime(df: pl.DataFrame, col: str) -> pl.DataFrame:
    """Ensure `{col}` is a UTC datetime; integers are interpreted via magnitude."""
    if col not in df.columns:
        return df
    dtype = df.schema[col]
    if dtype == pl.Datetime or str(dtype).startswith("Datetime"):
        return df
    unit = infer_ts_unit(df[col])
    if unit in {"ns", "us", "ms", "s"}:
        return df.with_columns(
            pl.from_epoch(pl.col(col).cast(pl.Int64), time_unit=unit)
            .dt.replace_time_zone("UTC")
            .alias(col)
        )
    return df


def skew_stats(
    df: pl.DataFrame,
    ingress_col: str = "ingress_ts",
    venue_col: str = "transaction_ts",
) -> dict[str, Any]:
    """Distribution of ingress time minus venue time, converted to seconds."""
    if ingress_col not in df.columns or venue_col not in df.columns:
        return {}
    if df[venue_col].null_count() == df.height or df[ingress_col].null_count() == df.height:
        return {}

    work = ensure_datetime(ensure_datetime(df, ingress_col), venue_col)
    work = to_datetime(to_datetime(work, ingress_col), venue_col)
    i_dt, v_dt = f"{ingress_col}_dt", f"{venue_col}_dt"
    if i_dt not in work.columns or v_dt not in work.columns:
        return {}
    # a naive and an aware column have no supertype; force both to UTC before subtracting
    work = work.with_columns(
        [
            pl.col(c).dt.replace_time_zone("UTC")
            if work.schema[c] == pl.Datetime(time_unit="us")
            else pl.col(c).dt.convert_time_zone("UTC")
            for c in (i_dt, v_dt)
        ]
    )

    unit = infer_ts_unit(df[venue_col])
    # after from_epoch, cast Int64 is microseconds for us datetime — use duration in seconds
    skew_s = work.select(
        ((pl.col(i_dt) - pl.col(v_dt)).dt.total_microseconds() / 1_000_000.0).alias("skew_s")
    ).drop_nulls()
    if skew_s.is_empty():
        return {}

    neg = int(skew_s.filter(pl.col("skew_s") < 0).height)
    return {
        "ingress": ingress_col,
        "venue": venue_col,
        "venue_unit_hint": unit,
        "n": skew_s.height,
        "median_s": float(skew_s["skew_s"].median()),  # type: ignore[arg-type]
        "p50_s": float(skew_s.select(pl.col("skew_s").quantile(0.5)).item()),
        "p99_s": float(skew_s.select(pl.col("skew_s").quantile(0.99)).item()),
        "min_s": float(skew_s["skew_s"].min()),  # type: ignore[arg-type]
        "max_s": float(skew_s["skew_s"].max()),  # type: ignore[arg-type]
        "negative_count": neg,
        "negative_pct": neg / skew_s.height * 100.0,
    }


def skew_finding(file_label: str, stats: dict[str, Any]) -> Finding | None:
    if not stats:
        return None
    med = abs(stats.get("median_s", 0.0))
    neg_pct = stats.get("negative_pct", 0.0)
    severity = "info"
    classification = "ok"
    notes = "Capture latency is within the expected delay."
    if med > 5 or neg_pct > 5:
        severity = "medium"
        classification = "pipeline"
        notes = "Median capture latency is large, or negative latency is frequent."
    if med > 60 or neg_pct > 20:
        severity = "high"
        classification = "pipeline"
        notes = "Median capture latency is severe, or negative latency is frequent."
    return Finding(
        file=file_label,
        check_id=f"clock_skew_{stats.get('ingress')}_{stats.get('venue')}",
        severity=severity,
        classification=classification if classification != "ok" else "unclear",
        metric=stats,
        notes=notes,
    )


def all_skews(df: pl.DataFrame, file_label: str) -> list[Finding]:
    findings: list[Finding] = []
    if "ingress_ts" not in df.columns:
        return findings
    for venue in ("transaction_ts", "publish_ts", "exchange_ts"):
        if venue in df.columns:
            st = skew_stats(df, "ingress_ts", venue)
            f = skew_finding(file_label, st)
            if f:
                findings.append(f)
    return findings
