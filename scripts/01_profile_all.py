"""Profile every parquet with the shared DQ battery."""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from lotech_dq.checks import profile_frame  # noqa: E402
from lotech_dq.clocks import all_skews  # noqa: E402
from lotech_dq.io import FILES, ensure_dirs, load_parquet  # noqa: E402
from lotech_dq.report import write_table_json  # noqa: E402


def main() -> None:
    ensure_dirs()
    summaries = []
    all_findings = []
    for key, filename in FILES.items():
        print(f"profiling {filename} ...")
        df = load_parquet(key)
        # For huge C, still full profile but skip ultra-heavy ops already vectorised
        summary = profile_frame(df, filename)
        for f in all_skews(df, filename):
            summary["findings"].append(f.to_dict())
        summaries.append(summary)
        all_findings.extend(summary["findings"])
        print(f"  rows={summary['rows']} findings={len(summary['findings'])}")

    write_table_json("profile_summary.json", summaries)
    write_table_json("profile_findings.json", all_findings)
    print("wrote outputs/tables/profile_summary.json")


if __name__ == "__main__":
    main()
