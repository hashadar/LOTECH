# LO:TECH — Market Data Quality Take-Home

Python + Polars analysis of the LO:TECH market-data quality exercise.

- **Findings (read this):** [WRITEUP.md](WRITEUP.md)
- Brief (reference): [gist](https://gist.github.com/jembishop/6e2aa508cd8c2a19c22515bacf2e86fc)

WRITEUP.md is the findings document. Scripts and `outputs/tables/` are the exhibits it cites.

## Setup

Requires Python ≥3.11.

```powershell
cd LOTECH
python -m pip install -e .
```

On Windows, `tzdata` is included so Polars UTC timestamps resolve correctly.

## Data

Parquet files are not committed in this repository. Download them from the public S3 objects. This step requires network access:

```powershell
python scripts/00_download.py
```

The script writes into `data/` and generates `data/MANIFEST.md` (sizes, row counts, SHA-256).

## Run analyses

```powershell
python scripts/01_profile_all.py
python scripts/02_analyse_A_hkex.py
python scripts/03_analyse_B_dreamdex.py
python scripts/04_analyse_C_nasdaq.py
python scripts/05_analyse_D_binance_l2.py
python scripts/06_analyse_F_binance_eth.py
python scripts/07_analyse_G_bitfinex.py
python scripts/08_analyse_H_gateio.py
python scripts/09_exhibits.py
```

`07_analyse_G_bitfinex.py` and `08_analyse_H_gateio.py` reconcile against public venue APIs. On first run they fetch live responses and write JSON fixtures under `fixtures/venue/` (committed in this repository). Network is required once to populate those fixtures. After that, both scripts run offline from the cache.

The last verified comparisons (difference 0 on volume) are recorded in `outputs/tables/G_bitfinex.json` and `outputs/tables/H_gateio_volume.json`. Every other script is offline once `data/` is populated.

Outputs:

- `outputs/tables/*.json` — profiler (`profile_summary.json`, `profile_findings.json`, `schemas.json`, `clocks_matrix.json`), per-file metrics, and `exhibits.json` (sample rows / ruled-out alternatives)
- `outputs/tables/C_nasdaq_per_symbol.csv` — per-symbol NASDAQ quality metrics
- `outputs/tables/D_final_book.csv` — final reconstructed L2 book after replay
- `outputs/tables/B_microprice_series.parquet` — full microprice series with reason codes
- `outputs/tables/D_top_of_book_series.parquet`, `outputs/tables/D_top_of_book_series_threshold200.parquet` — replayed top of book per message, under each snapshot policy
- `outputs/figures/B_microprice.png`
- `outputs/figures/D_binance_l2_mid_spread.png`
- `fixtures/venue/` — cached Bitfinex and Gate.io API responses for G and H reconciliation

The three `.parquet` series under `outputs/tables/` are committed (`.gitignore` excludes other `*.parquet` paths).

## Layout

```
WRITEUP.md         findings document (findings + links into this tree)
src/lotech_dq/     shared helpers (checks, clocks, book replay, microprice, volume)
scripts/           one script per file + 09_exhibits.py
data/              downloaded parquets (gitignored)
fixtures/venue/    committed venue API fixtures (G, H)
outputs/           figures + JSON summaries
```

## Notes

- Derivatives base quantity = `qty * quantity_multiplier` (null multiplier → 1).
- Do not redistribute the raw market-data files. Use the download script.
