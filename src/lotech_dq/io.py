from __future__ import annotations

from pathlib import Path

import polars as pl

ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = ROOT / "data"
OUTPUTS_DIR = ROOT / "outputs"
FIGURES_DIR = OUTPUTS_DIR / "figures"
TABLES_DIR = OUTPUTS_DIR / "tables"

S3_BASE = (
    "https://data-quality-test-files-d78b6a3f-8695-4883-b094-8a2f65e59c3a"
    ".s3.eu-west-1.amazonaws.com"
)

FILES = {
    "A_tob": "A_hkex_2800_top_of_book.parquet",
    "A_trades": "A_hkex_2800_trades.parquet",
    "B": "B_dreamdex_weth_top_of_book.parquet",
    "C": "C_nasdaq_top_of_book_20_symbols.parquet",
    "D": "D_binance_btcusdt_orderbook_incremental.parquet",
    "F": "F_binance_ethusdt_top_of_book.parquet",
    "G": "G_bitfinex_btcusd_trades.parquet",
    "H_trades": "H_gateio_btcusdt_perp_trades.parquet",
    "H_static": "H_gateio_btcusdt_perp_instrument_static.parquet",
}

EXPECTED_ROWS = {
    "A_hkex_2800_top_of_book.parquet": 109_544,
    "A_hkex_2800_trades.parquet": 9_666,
    "B_dreamdex_weth_top_of_book.parquet": 6_988,
    "C_nasdaq_top_of_book_20_symbols.parquet": 3_752_799,
    "D_binance_btcusdt_orderbook_incremental.parquet": 17_994,
    "F_binance_ethusdt_top_of_book.parquet": 235_270,
    "G_bitfinex_btcusd_trades.parquet": 642,
    "H_gateio_btcusdt_perp_trades.parquet": 9_359,
    "H_gateio_btcusdt_perp_instrument_static.parquet": 1,
}


def ensure_dirs() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    FIGURES_DIR.mkdir(parents=True, exist_ok=True)
    TABLES_DIR.mkdir(parents=True, exist_ok=True)


def data_path(name: str) -> Path:
    """Resolve a data file by FILES key or bare filename."""
    filename = FILES.get(name, name)
    return DATA_DIR / filename


def load_parquet(name: str, columns: list[str] | None = None) -> pl.DataFrame:
    path = data_path(name)
    if not path.exists():
        raise FileNotFoundError(f"Missing data file: {path}. Run scripts/00_download.py")
    if columns is None:
        return pl.read_parquet(path)
    return pl.read_parquet(path, columns=columns)


def file_url(filename: str) -> str:
    return f"{S3_BASE}/{filename}"
