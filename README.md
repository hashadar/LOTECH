# LO:TECH — Market Data Quality Take-Home

Python + Polars analysis of the LO:TECH market-data quality exercise.

- Findings: [WRITEUP.md](WRITEUP.md)
- Brief (reference): [gist](https://gist.github.com/jembishop/6e2aa508cd8c2a19c22515bacf2e86fc)

## Setup

Requires Python ≥3.11.

```powershell
cd LOTECH
python -m pip install -e .
```

On Windows, `tzdata` is included so Polars UTC timestamps resolve correctly.

## Data

Parquet files are **not** vendored. Download them (public S3 objects):

```powershell
python scripts/00_download.py
```

This writes into `data/` and generates `data/MANIFEST.md` (sizes, row counts, SHA-256).

## Run analyses

```powershell
python scripts/01_profile_all.py
python scripts/08_analyse_H_gateio.py
python scripts/05_analyse_D_binance_l2.py
python scripts/03_analyse_B_dreamdex.py
python scripts/02_analyse_A_hkex.py
python scripts/04_analyse_C_nasdaq.py
python scripts/06_analyse_F_binance_eth.py
python scripts/07_analyse_G_bitfinex.py
```

Outputs:

- `outputs/tables/*.json` (+ `C_nasdaq_per_symbol.csv`)
- `outputs/figures/B_microprice.png`
- `outputs/figures/D_binance_l2_mid_spread.png`

## Layout

```
src/lotech_dq/     shared helpers (checks, clocks, book replay, microprice, volume)
scripts/           thin runners per file
data/              downloaded parquets (gitignored)
outputs/           figures + JSON summaries (gitignored figures/tables contents ok to regenerate)
WRITEUP.md         submission narrative
```

## Notes

- Derivatives base quantity = `qty * quantity_multiplier` (null multiplier → 1).
- Do not redistribute the raw market-data files; use the download script.
