from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import polars as pl

from lotech_dq.io import TABLES_DIR, ensure_dirs


def write_json(path: Path | str, payload: Any) -> Path:
    ensure_dirs()
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    return path


def write_table_json(name: str, payload: Any) -> Path:
    return write_json(TABLES_DIR / name, payload)


def findings_to_frame(findings: list[dict[str, Any]]) -> pl.DataFrame:
    if not findings:
        return pl.DataFrame(
            schema={
                "file": pl.Utf8,
                "check_id": pl.Utf8,
                "severity": pl.Utf8,
                "classification": pl.Utf8,
                "notes": pl.Utf8,
            }
        )
    rows = []
    for f in findings:
        rows.append(
            {
                "file": f.get("file"),
                "check_id": f.get("check_id"),
                "severity": f.get("severity"),
                "classification": f.get("classification"),
                "notes": f.get("notes"),
                "metric": json.dumps(f.get("metric", {}), default=str),
            }
        )
    return pl.DataFrame(rows)
