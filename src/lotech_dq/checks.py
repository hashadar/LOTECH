from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any

import polars as pl


@dataclass
class Finding:
    file: str
    check_id: str
    severity: str  # critical | high | medium | low | info
    classification: str  # pipeline | market | unclear | ok
    metric: dict[str, Any] = field(default_factory=dict)
    notes: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


TS_CANDIDATES = ("ingress_ts", "transaction_ts", "publish_ts", "exchange_ts", "ts")
PRICE_CANDIDATES = (
    "bid_price",
    "ask_price",
    "price",
    "bid_px",
    "ask_px",
    "best_bid",
    "best_ask",
)
SIZE_CANDIDATES = (
    "bid_size",
    "ask_size",
    "qty",
    "quantity",
    "size",
    "bid_qty",
    "ask_qty",
    "bid_sz",
    "ask_sz",
)


def _present(df: pl.DataFrame, names: tuple[str, ...]) -> list[str]:
    return [c for c in names if c in df.columns]


def schema_profile(df: pl.DataFrame) -> dict[str, str]:
    return {name: str(dtype) for name, dtype in df.schema.items()}


def null_report(df: pl.DataFrame) -> list[dict[str, Any]]:
    n = df.height
    out: list[dict[str, Any]] = []
    for col in df.columns:
        nulls = int(df[col].null_count())
        out.append({"column": col, "nulls": nulls, "null_pct": (nulls / n * 100.0) if n else 0.0})
    return out


def distinct_counts(df: pl.DataFrame, cols: list[str] | None = None) -> dict[str, int]:
    cols = cols or [
        c
        for c in ("symbol", "instrument", "side", "trade_id", "id", "update_id", "seq_id")
        if c in df.columns
    ]
    return {c: int(df[c].n_unique()) for c in cols if c in df.columns}


def infer_ts_unit(series: pl.Series) -> str:
    """Heuristic for integer epoch timestamps."""
    if series.dtype in (pl.Datetime, pl.Date):
        return "datetime"
    s = series.drop_nulls()
    if s.is_empty():
        return "empty"
    # sample median magnitude
    med = float(s.cast(pl.Float64).median())  # type: ignore[arg-type]
    if med > 1e17:
        return "ns"
    if med > 1e14:
        return "us"
    if med > 1e11:
        return "ms"
    if med > 1e9:
        return "s"
    return "unknown"


def to_datetime(df: pl.DataFrame, col: str) -> pl.DataFrame:
    """Add `{col}_dt` as UTC datetime, inferring unit for integers."""
    if col not in df.columns:
        return df
    dtype = df.schema[col]
    out_col = f"{col}_dt"
    if dtype == pl.Datetime or str(dtype).startswith("Datetime"):
        return df.with_columns(pl.col(col).alias(out_col))
    unit = infer_ts_unit(df[col])
    if unit in {"ns", "us", "ms", "s"}:
        return df.with_columns(
            pl.from_epoch(pl.col(col).cast(pl.Int64), time_unit=unit).alias(out_col)
        )
    return df.with_columns(pl.col(col).cast(pl.Datetime(time_unit="us")).alias(out_col))


def monotonic_backwards(
    df: pl.DataFrame,
    col: str,
    partition_by: str | None = None,
) -> Finding | None:
    """Backward steps in `col` over the frame's stored order.

    The frame is never sorted on `col`: sorting the column being differenced makes the
    check vacuous. When `partition_by` is given the diff is taken within each partition,
    still in stored order.
    """
    if col not in df.columns or df.height < 2:
        return None
    if partition_by is not None and partition_by in df.columns:
        diffs = df.select(pl.col(col).diff().over(partition_by).alias("d"))
        check_id = f"non_monotonic_{col}_over_{partition_by}"
        scope = f" within {partition_by}"
    else:
        diffs = df.select(pl.col(col).diff().alias("d"))
        check_id = f"non_monotonic_{col}"
        scope = ""
    bad_frame = diffs.filter(pl.col("d") < 0)
    bad = int(bad_frame.height)
    if bad == 0:
        return None
    metric: dict[str, Any] = {"backward_jumps": bad, "rows": df.height}
    if partition_by is not None and partition_by in df.columns:
        metric["partition_by"] = partition_by
    if str(df.schema[col]).startswith("Datetime"):
        ms = bad_frame.select(pl.col("d").dt.total_microseconds() / 1000.0)["d"]
        metric["worst_step_ms"] = float(ms.min())  # type: ignore[arg-type]
        metric["median_backward_step_ms"] = float(ms.median())  # type: ignore[arg-type]
        metric["within_1ms"] = int(bad_frame.select(
            (pl.col("d").dt.total_microseconds() >= -1000).sum()
        ).item())
    else:
        metric["worst_step"] = bad_frame.select(pl.col("d").min()).item()
        metric["median_backward_step"] = bad_frame.select(pl.col("d").median()).item()
    return Finding(
        file="",
        check_id=check_id,
        severity="medium" if bad < 10 else "high",
        classification="pipeline",
        metric=metric,
        notes=f"{bad} backward jumps in {col}{scope} (stored order).",
    )


def _seconds_expr(df: pl.DataFrame, col: str, diff_expr: pl.Expr) -> pl.Expr | None:
    """Convert a diff of `col` into float seconds, whatever the column's storage."""
    dtype = df.schema[col]
    if str(dtype).startswith("Datetime"):
        return diff_expr.dt.total_microseconds() / 1_000_000.0
    unit = infer_ts_unit(df[col])
    divisor = {"s": 1.0, "ms": 1e3, "us": 1e6, "ns": 1e9}.get(unit)
    if divisor is None:
        return None
    return diff_expr.cast(pl.Float64) / divisor


def duplicate_rows(df: pl.DataFrame, keys: list[str]) -> Finding | None:
    keys = [k for k in keys if k in df.columns]
    if not keys:
        return None
    dup = df.group_by(keys).len().filter(pl.col("len") > 1)
    n_dup_groups = dup.height
    if n_dup_groups == 0:
        return None
    extra = int(dup.select((pl.col("len") - 1).sum()).item())
    return Finding(
        file="",
        check_id=f"duplicate_keys_{'_'.join(keys)}",
        severity="medium",
        classification="pipeline",
        metric={"duplicate_groups": n_dup_groups, "extra_rows": extra},
        notes=f"Duplicate key groups on {keys}.",
    )


def gap_stats(
    df: pl.DataFrame,
    col: str,
    partition_by: str | None = None,
    large_gap_threshold_s: float = 60.0,
) -> dict[str, Any]:
    """Inter-row gaps in `col`, in seconds, over the frame's stored order.

    Never sorts on `col`, so a backward step is observable rather than sorted away.
    `partition_by` gives per-instrument gaps on a multiplexed file, where the
    unpartitioned figure is an inter-message interval across symbols.
    `large_gap_threshold_s` is absolute: a quantile of the same distribution would
    return a fixed fraction of the rows regardless of the data.
    """
    if col not in df.columns or df.height < 3:
        return {}
    partitioned = partition_by is not None and partition_by in df.columns
    diff_expr = (
        pl.col(col).diff().over(partition_by) if partitioned else pl.col(col).diff()
    )
    sec_expr = _seconds_expr(df, col, diff_expr)
    if sec_expr is None:
        return {}
    gaps = df.select(sec_expr.alias("gap_s")).drop_nulls()
    if gaps.is_empty():
        return {}
    neg = int(gaps.filter(pl.col("gap_s") < 0).height)
    return {
        "gap_unit": "s",
        "partition_by": partition_by if partitioned else None,
        "n_gaps": gaps.height,
        "median_gap_s": float(gaps.select(pl.col("gap_s").median()).item()),
        "p999_gap_s": float(gaps.select(pl.col("gap_s").quantile(0.999)).item()),
        "max_gap_s": float(gaps.select(pl.col("gap_s").max()).item()),
        "min_gap_s": float(gaps.select(pl.col("gap_s").min()).item()),
        "large_gap_threshold_s": large_gap_threshold_s,
        "large_gap_count": int(gaps.filter(pl.col("gap_s") > large_gap_threshold_s).height),
        "negative_gap_count": neg,
        "source_unit_hint": infer_ts_unit(df[col]),
    }


def price_size_anomalies(df: pl.DataFrame) -> list[Finding]:
    findings: list[Finding] = []
    for col in _present(df, PRICE_CANDIDATES + SIZE_CANDIDATES):
        s = df[col]
        if s.dtype not in (
            pl.Float32,
            pl.Float64,
            pl.Int8,
            pl.Int16,
            pl.Int32,
            pl.Int64,
            pl.UInt8,
            pl.UInt16,
            pl.UInt32,
            pl.UInt64,
            pl.Decimal,
        ):
            continue
        neg = int(df.filter(pl.col(col) < 0).height)
        zero = int(df.filter(pl.col(col) == 0).height)
        if neg:
            findings.append(
                Finding(
                    file="",
                    check_id=f"negative_{col}",
                    severity="high",
                    classification="pipeline",
                    metric={"count": neg},
                    notes=f"Negative values in {col}.",
                )
            )
        # zero prices are suspicious; zero sizes can be valid deletes on L2
        if zero and ("price" in col.lower() or col in {"price", "bid_price", "ask_price"}):
            findings.append(
                Finding(
                    file="",
                    check_id=f"zero_{col}",
                    severity="medium",
                    classification="unclear",
                    metric={"count": zero},
                    notes=f"Zero values in {col}.",
                )
            )
    return findings


def tob_crossed_locked(
    df: pl.DataFrame,
    bid_col: str = "bid_price",
    ask_col: str = "ask_price",
) -> list[Finding]:
    if bid_col not in df.columns or ask_col not in df.columns:
        # try common aliases
        aliases = [
            ("bid_px", "ask_px"),
            ("best_bid", "best_ask"),
            ("bid", "ask"),
        ]
        for b, a in aliases:
            if b in df.columns and a in df.columns:
                bid_col, ask_col = b, a
                break
        else:
            return []

    valid = df.filter(pl.col(bid_col).is_not_null() & pl.col(ask_col).is_not_null())
    n = valid.height
    if n == 0:
        return []
    crossed = int(valid.filter(pl.col(bid_col) > pl.col(ask_col)).height)
    locked = int(valid.filter(pl.col(bid_col) == pl.col(ask_col)).height)
    findings: list[Finding] = []
    if crossed:
        findings.append(
            Finding(
                file="",
                check_id="crossed_book",
                severity="high",
                classification="pipeline",
                metric={
                    "crossed": crossed,
                    "crossed_pct": crossed / n * 100.0,
                    "bid_col": bid_col,
                    "ask_col": ask_col,
                },
                notes="Bid above ask on top of book.",
            )
        )
    if locked:
        findings.append(
            Finding(
                file="",
                check_id="locked_book",
                severity="low",
                classification="market",
                metric={
                    "locked": locked,
                    "locked_pct": locked / n * 100.0,
                    "bid_col": bid_col,
                    "ask_col": ask_col,
                },
                notes="Bid equals ask (locked). Often market microstructure on equities.",
            )
        )
    return findings


def spread_stats(
    df: pl.DataFrame,
    bid_col: str = "bid_price",
    ask_col: str = "ask_price",
) -> dict[str, Any]:
    if bid_col not in df.columns or ask_col not in df.columns:
        return {}
    s = (
        df.filter(pl.col(bid_col).is_not_null() & pl.col(ask_col).is_not_null())
        .select(
            (pl.col(ask_col) - pl.col(bid_col)).alias("spread"),
            ((pl.col(ask_col) + pl.col(bid_col)) / 2).alias("mid"),
        )
        .filter(pl.col("spread").is_not_null())
    )
    if s.is_empty():
        return {}
    return {
        "spread_median": float(s["spread"].median()),  # type: ignore[arg-type]
        "spread_p99": float(s.select(pl.col("spread").quantile(0.99)).item()),
        "spread_max": float(s["spread"].max()),  # type: ignore[arg-type]
        "negative_spreads": int(s.filter(pl.col("spread") < 0).height),
    }


EXPECTED_COLUMNS = ("ingress_ts", "transaction_ts", "publish_ts")
UNIQUE_KEY_CANDIDATES = (["trade_id"], ["seq_id"])
PARTITION_CANDIDATES = ("instrument", "symbol")


def missing_columns(
    df: pl.DataFrame,
    expected: tuple[str, ...] = EXPECTED_COLUMNS,
) -> list[Finding]:
    """Absent columns other venues populate. A null-rate check cannot see these."""
    out: list[Finding] = []
    for col in expected:
        if col in df.columns:
            continue
        out.append(
            Finding(
                file="",
                check_id=f"missing_column_{col}",
                severity="high",
                classification="pipeline",
                metric={"column": col, "expected_columns": list(expected)},
                notes=f"Column {col} is absent from the schema, not present-and-null.",
            )
        )
    return out


def null_rate_findings(
    df: pl.DataFrame,
    expected: tuple[str, ...] = EXPECTED_COLUMNS,
    threshold_pct: float = 1.0,
) -> list[Finding]:
    out: list[Finding] = []
    n = df.height
    if n == 0:
        return out
    for col in expected:
        if col not in df.columns:
            continue
        nulls = int(df[col].null_count())
        pct = nulls / n * 100.0
        if pct <= threshold_pct:
            continue
        out.append(
            Finding(
                file="",
                check_id=f"null_clock_{col}",
                severity="high" if pct > 50 else "medium",
                classification="pipeline",
                metric={"column": col, "nulls": nulls, "null_pct": pct, "rows": n},
                notes=f"{col} null on {pct:.4f}% of rows (> {threshold_pct}% alert threshold).",
            )
        )
    return out


def profile_frame(
    df: pl.DataFrame,
    file_label: str,
    expected_columns: tuple[str, ...] = EXPECTED_COLUMNS,
    unique_keys: tuple[list[str], ...] = UNIQUE_KEY_CANDIDATES,
) -> dict[str, Any]:
    ts_cols = _present(df, TS_CANDIDATES)
    partition = next((c for c in PARTITION_CANDIDATES if c in df.columns), None)
    multiplexed = partition is not None and int(df[partition].n_unique()) > 1

    findings: list[Finding] = []
    findings.extend(missing_columns(df, expected_columns))
    findings.extend(null_rate_findings(df, expected_columns))
    findings.extend(price_size_anomalies(df))
    findings.extend(tob_crossed_locked(df))
    for col in ts_cols:
        f = monotonic_backwards(df, col)
        if f:
            findings.append(f)
        if multiplexed:
            fp = monotonic_backwards(df, col, partition_by=partition)
            if fp:
                findings.append(fp)
    for keys in unique_keys:
        keys_here = [k for k in keys if k in df.columns]
        if not keys_here:
            continue
        scoped = ([partition] if partition else []) + keys_here
        f = duplicate_rows(df, scoped)
        if f:
            findings.append(f)

    for f in findings:
        if not f.file:
            f.file = file_label

    gaps = {col: gap_stats(df, col) for col in ts_cols}
    if multiplexed:
        gaps.update(
            {
                f"{col}__over_{partition}": gap_stats(df, col, partition_by=partition)
                for col in ts_cols
            }
        )
    return {
        "file": file_label,
        "rows": df.height,
        "columns": df.columns,
        "expected_columns": list(expected_columns),
        "absent_columns": [c for c in expected_columns if c not in df.columns],
        "partition_column": partition if multiplexed else None,
        "schema": schema_profile(df),
        "nulls": null_report(df),
        "distinct": distinct_counts(df),
        "ts_units": {c: infer_ts_unit(df[c]) for c in ts_cols},
        "gaps": gaps,
        "spread": spread_stats(df),
        "findings": [f.to_dict() for f in findings],
    }
