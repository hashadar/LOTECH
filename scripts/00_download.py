"""Download LO:TECH take-home parquet files into data/."""
from __future__ import annotations

import hashlib
import sys
from pathlib import Path

import httpx
import polars as pl

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from lotech_dq.io import (  # noqa: E402
    DATA_DIR,
    EXPECTED_ROWS,
    FILES,
    ensure_dirs,
    file_url,
)


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def download_one(filename: str, client: httpx.Client) -> dict:
    dest = DATA_DIR / filename
    url = file_url(filename)
    if dest.exists() and dest.stat().st_size > 0:
        print(f"skip (exists): {filename}")
    else:
        print(f"downloading: {filename}")
        with client.stream("GET", url) as r:
            r.raise_for_status()
            with dest.open("wb") as out:
                for chunk in r.iter_bytes():
                    out.write(chunk)

    df = pl.read_parquet(dest)
    rows = df.height
    expected = EXPECTED_ROWS.get(filename)
    return {
        "file": filename,
        "bytes": dest.stat().st_size,
        "rows": rows,
        "expected_rows": expected,
        "rows_match": rows == expected if expected is not None else None,
        "sha256": sha256_file(dest),
        "columns": df.columns,
    }


def main() -> None:
    ensure_dirs()
    filenames = list(dict.fromkeys(FILES.values()))
    results = []
    with httpx.Client(timeout=120.0, follow_redirects=True) as client:
        for name in filenames:
            results.append(download_one(name, client))

    lines = ["# Local data manifest", ""]
    for r in results:
        match = r["rows_match"]
        flag = "OK" if match else ("MISMATCH" if match is False else "?")
        row_word = "row" if r["rows"] == 1 else "rows"
        expected = r["expected_rows"]
        expected_word = "row" if expected == 1 else "rows"
        lines.append(
            f"- `{r['file']}` — {r['bytes']} bytes, {r['rows']} {row_word} "
            f"(expected {expected} {expected_word}) [{flag}]"
        )
        lines.append(f"  - sha256: `{r['sha256']}`")
        lines.append(f"  - columns: {', '.join(r['columns'])}")
    manifest = DATA_DIR / "MANIFEST.md"
    manifest.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"wrote {manifest}")
    bad = [r for r in results if r["rows_match"] is False]
    if bad:
        print("WARNING: row counts do not match:", [r["file"] for r in bad])
        sys.exit(1)


if __name__ == "__main__":
    main()
