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


def monotonic_backwards(df: pl.DataFrame, col: str) -> Finding | None:
    if col not in df.columns or df.height < 2:
        return None
    diffs = df.select(pl.col(col).diff().alias("d"))
    bad = int(diffs.filter(pl.col("d") < 0).height)
    if bad == 0:
        return None
    return Finding(
        file="",
        check_id=f"non_monotonic_{col}",
        severity="medium" if bad < 10 else "high",
        classification="pipeline",
        metric={"backward_jumps": bad, "rows": df.height},
        notes=f"{bad} backward jumps in {col}.",
    )


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


def gap_stats(df: pl.DataFrame, col: str, large_gap_quantile: float = 0.999) -> dict[str, Any]:
    if col not in df.columns or df.height < 3:
        return {}
    gaps = (
        df.sort(col)
        .select(pl.col(col).diff().alias("gap"))
        .drop_nulls()
        .with_columns(pl.col("gap").cast(pl.Float64))
    )
    if gaps.is_empty():
        return {}
    q = float(gaps.select(pl.col("gap").quantile(large_gap_quantile)).item())
    med = float(gaps.select(pl.col("gap").median()).item())
    mx = float(gaps.select(pl.col("gap").max()).item())
    large = int(gaps.filter(pl.col("gap") > q).height) if q > 0 else 0
    return {
        "median_gap": med,
        f"p{int(large_gap_quantile * 1000)}_gap": q,
        "max_gap": mx,
        "large_gap_count": large,
        "unit_hint": infer_ts_unit(df[col]),
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


def profile_frame(df: pl.DataFrame, file_label: str) -> dict[str, Any]:
    ts_cols = _present(df, TS_CANDIDATES)
    findings: list[Finding] = []
    findings.extend(price_size_anomalies(df))
    findings.extend(tob_crossed_locked(df))
    for col in ts_cols:
        f = monotonic_backwards(df, col)
        if f:
            f.file = file_label
            findings.append(f)

    for f in findings:
        if not f.file:
            f.file = file_label

    gaps = {col: gap_stats(df, col) for col in ts_cols}
    return {
        "file": file_label,
        "rows": df.height,
        "columns": df.columns,
        "schema": schema_profile(df),
        "nulls": null_report(df),
        "distinct": distinct_counts(df),
        "ts_units": {c: infer_ts_unit(df[c]) for c in ts_cols},
        "gaps": gaps,
        "spread": spread_stats(df),
        "findings": [f.to_dict() for f in findings],
    }
