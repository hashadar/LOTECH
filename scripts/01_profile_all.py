"""Profile every parquet with the shared DQ battery."""
from __future__ import annotations

import sys
from pathlib import Path

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from lotech_dq.checks import EXPECTED_COLUMNS, infer_ts_unit, profile_frame  # noqa: E402
from lotech_dq.clocks import all_skews  # noqa: E402
from lotech_dq.io import FILES, ensure_dirs, load_parquet  # noqa: E402
from lotech_dq.report import write_table_json  # noqa: E402


def _sample_rows(df: pl.DataFrame, n: int = 2) -> list[dict]:
    """Small readable sample; list columns are summarised by length only."""
    list_cols = [c for c, t in df.schema.items() if str(t).startswith("List")]
    head = df.head(n)
    if list_cols:
        head = head.with_columns([pl.col(c).list.len().alias(f"{c}__len") for c in list_cols]).drop(
            list_cols
        )
    return [{k: (str(v) if hasattr(v, "isoformat") else v) for k, v in r.items()} for r in head.to_dicts()]


def clock_matrix(frames: dict[str, pl.DataFrame]) -> list[dict]:
    """Per-file clock coverage, distinguishing an absent column from a null one.

    The distinction matters: a null-rate alert cannot fire on a column that is not in
    the schema, so absence and 100% null are different defects with different fixes.
    """
    rows = []
    for filename, df in frames.items():
        n = df.height
        entry: dict = {"file": filename, "rows": n}
        for col in EXPECTED_COLUMNS:
            if col not in df.columns:
                entry[col] = {
                    "status": "absent",
                    "dtype": None,
                    "nulls": None,
                    "null_pct": None,
                    "unit": None,
                }
                continue
            nulls = int(df[col].null_count())
            entry[col] = {
                "status": "present_all_null" if nulls == n and n else "present",
                "dtype": str(df.schema[col]),
                "nulls": nulls,
                "null_pct": (nulls / n * 100.0) if n else 0.0,
                "unit": infer_ts_unit(df[col]),
            }
        present = [entry[c]["dtype"] for c in EXPECTED_COLUMNS if entry[c]["dtype"]]
        entry["dtype_family"] = sorted(set(present))
        entry["mixed_clock_dtypes"] = len(set(present)) > 1
        rows.append(entry)
    return rows


def main() -> None:
    ensure_dirs()
    summaries = []
    all_findings = []
    schemas: dict[str, dict] = {}
    frames: dict[str, pl.DataFrame] = {}

    for key, filename in FILES.items():
        if filename in frames:
            continue
        print(f"profiling {filename} ...")
        df = load_parquet(key)
        frames[filename] = df
        summary = profile_frame(df, filename)
        for f in all_skews(df, filename):
            summary["findings"].append(f.to_dict())
        summaries.append(summary)
        all_findings.extend(summary["findings"])
        schemas[filename] = {
            "rows": df.height,
            "schema": {name: str(dtype) for name, dtype in df.schema.items()},
            "sample": _sample_rows(df),
        }
        print(f"  rows={summary['rows']} findings={len(summary['findings'])}")

    write_table_json("profile_summary.json", summaries)
    write_table_json("profile_findings.json", all_findings)
    write_table_json("schemas.json", schemas)
    write_table_json(
        "clocks_matrix.json",
        {
            "expected_columns": list(EXPECTED_COLUMNS),
            "note": (
                "status=absent means the column is not in the parquet schema; "
                "present_all_null means it exists and every row is null."
            ),
            "files": clock_matrix(frames),
        },
    )
    print("wrote profile_summary.json, profile_findings.json, schemas.json, clocks_matrix.json")


if __name__ == "__main__":
    main()
