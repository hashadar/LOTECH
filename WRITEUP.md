# Market Data Quality — Findings

**Candidate deliverable for LO:TECH take-home**  
Stack: Python + Polars. Code under `src/lotech_dq/` and `scripts/`. Figures under `outputs/figures/`.

## Approach

1. Download the public parquet objects and verify row counts against the brief.
2. Run a shared DQ battery (nulls, monotonicity, gaps, crossed/locked TOB, clock skew).
3. Deep-dive each file; fully complete the three explicit tasks (B microprice, D L2 replay, H volume).
4. Classify each material finding as **pipeline**, **market**, or **unclear**.

**Derivatives units (H):**  
`quantity_multiplier = 0.0001` (null → 1).  
- native contracts = `sum(qty)`  
- base asset (BTC) = `sum(qty * quantity_multiplier)`  
- quote (USDT) = `sum(qty * quantity_multiplier * price)`

---

## Executive summary

| File | Top finding | Class |
|------|-------------|-------|
| A HKEX TOB+trades | All 9,666 trades tagged `side=Buy`; 1,513 trades missing `transaction_ts` | pipeline |
| B DreamDex TOB | 22.0% of quotes have null bid (ask-only); microprice undefined there | unclear / market-leaning |
| C NASDAQ TOB | Filename says 20 symbols; file has **18**. 1,030 crossed TOB rows | pipeline (+ some market locks) |
| D Binance L2 | `snapshot` is **always false**; large updates never flagged. Book rebuilt via heuristic | pipeline |
| F Binance ETH TOB | No `transaction_ts` / `publish_ts`; 36 backward `seq_id` jumps | pipeline |
| G Bitfinex trades | Timestamps stored as Int64 µs (not Datetime); otherwise clean | pipeline (schema) |
| H Gate.io perp | Volumes match Gate public 1h candle **exactly**; Int64 timestamps | ok / schema note |

---

## A — HKEX 2800 (`S|2800-HKD:SPOT`)

**Window:** 2026-08-13 ~01:00–08:08 UTC (TOB). Trades start ~01:20.  
**Rows:** 109,544 TOB; 9,666 trades.

### Findings

1. **Trade side is always `Buy` (9,666/9,666).** No Sell prints at all.  
   - **Classification: pipeline.** A full session of one-sided aggressor labels is not credible for Tracker Fund 2800.

2. **1,513 trades (15.7%) have null `transaction_ts`.** Venue time is missing on a large minority of prints.  
   - **Classification: pipeline.**

3. **Crossed TOB:** 71 rows (0.065%); **locked:** 112 (0.10%).  
   - Crossed → **pipeline** (should not persist on a consolidated TOB).  
   - Locked → **market** (possible on equities).

4. **Lunch gap:** no trades in UTC hour 04 (HKT 12:00). Gaps &gt;300s: 4.  
   - **Classification: market** (HK continuous session lunch break).

5. **Trade vs contemporaneous TOB (asof):** of 8,153 matched trades, only 3 (0.037%) trade-through. Price/book consistency is otherwise good — the side/timestamp issues dominate.

---

## B — DreamDex WETH TOB (`WETH-USDso:SPOT`)

**Window:** 2026-08-14 04:00–10:13 UTC. **Rows:** 6,988.

### Microprice (explicit task)

Definition used:

\[
p_{\mathrm{micro}} = \frac{ask\_qty \cdot bid\_price + bid\_qty \cdot ask\_price}{bid\_qty + ask\_qty}
\]

**Undefined** when any of: null prices/sizes, `bid_qty+ask_qty ≤ 0`, or crossed book. Undefined points are **not** forward-filled (masked in the series).

| Metric | Value |
|--------|------:|
| Defined microprice rows | 5,448 (78.0%) |
| Undefined | 1,540 (22.0%) |
| Cause of undefined | null `bid_price` + null `bid_qty` (ask-only quotes) |
| Median spread | 0.57 |
| Max spread | 5.05 |
| Ingress gaps &gt;60s | 7 (max 105s) |
| Crossed book | 0 |

Plot: `outputs/figures/B_microprice.png` (mid + microprice; red markers where undefined).

**Classification:** Ask-only TOB updates are common on thin DEX books → lean **market** / feed sparsity; worth confirming whether the normaliser should emit a prior bid instead of null (**unclear** pipeline polish). Instrument quote asset `USDso` is an unusual normalised form (**unclear**).

---

## C — NASDAQ 20-symbol TOB

**Rows:** 3,752,799. **Distinct instruments:** **18** (not 20).

Instruments: AAEQ, AAPL, AMD, AMZN, ATRO, BOTZ, GPIX, INTC, LRGE, MSFT, NVDA, QMOM, QQQ, SMH, TIGO, TLT, TSLA, WYNN (all `*-USD:SPOT`, no `S|` equity prefix — inconsistent with HKEX `S|…` style).

### Findings

1. **18 ≠ 20 symbols in the filename/brief.**  
   - **Classification: pipeline** (manifest/export incomplete or mislabelled).

2. **Crossed TOB:** 1,030 rows; concentrated in liquid names (AAPL 233, QQQ 221, INTC 194, TSLA 158, NVDA 96).  
   - **Classification: pipeline** (NBBO should not cross this often in a clean feed).

3. **Locked TOB:** 26,814 — largely **market** microstructure.

4. **Sparse names:** TIGO / ATRO / WYNN lead anomaly score via multi-minute gaps (17 / 7 / 5 gaps &gt;60s). Plausible illiquidity → **market**, with residual risk of dropped updates.

5. Ingress−transaction skew median ~88ms, no negatives — capture latency looks healthy.

Per-symbol table: `outputs/tables/C_nasdaq_per_symbol.csv`.

---

## D — Binance BTCUSDT L2 incremental (explicit task)

**Rows:** 17,994. Each row carries `bid_prices`/`bid_qtys`/`ask_prices`/`ask_qtys` lists plus `snapshot: bool`.

### Replay method

1. Sort by `seq_id`, then timestamps.  
2. Treat as **snapshot** (clear book, replace levels) if `snapshot==True`, **or** combined level count ≥ 200, **or** first row (seed).  
3. Otherwise apply level upserts; `qty==0` deletes.  
4. Record best bid/ask/mid/spread after every message.

### Findings

1. **`snapshot` is true on 0 / 17,994 rows** despite max message size 1,207 levels and 460 messages ≥200 levels.  
   - **Classification: pipeline** (snapshot flag not set; replay must guess).

2. **`seq_id` gaps sum to ~1.24M.** Likely exchange-global sequence, not a dense per-stream counter — do not over-call as dropped depth. **Unclear** without Binance stream metadata.

3. After heuristic replay: **154 crossed TOB events (0.86%)**; mid in ~63,653–63,884 USDT; median spread 0.01. Crosses may be artefacts of mis-seeded state when the flag is wrong → **pipeline**.

Plot: `outputs/figures/D_binance_l2_mid_spread.png`.  
Metrics: `outputs/tables/D_binance_l2.json`.

---

## F — Binance ETHUSDT TOB (`ETH-USDT:SPOT`)

**Window:** 2026-08-13 02:00–03:59 UTC. **Rows:** 235,270.

### Findings

1. **No `transaction_ts` or `publish_ts` columns** — only `ingress_ts`.  
   - **Classification: pipeline** (venue clocks dropped vs other Binance file D).

2. **`seq_id`:** 36 backward jumps; many gaps &gt;1 (likely non-dense IDs). Backward jumps → **pipeline**.

3. No crossed/locked TOB; median spread 0.01; max ingress gap ~3.2s. Quote quality otherwise fine.

---

## G — Bitfinex BTCUSD trades (`BTC-USD:SPOT`)

**Window:** 2026-05-23 12:00–12:09 UTC. **Rows:** 642.

### Findings

1. **`ingress_ts` / `transaction_ts` / `publish_ts` are Int64 epoch microseconds**, not `Datetime[µs, UTC]` as in A/B/C/D/F. Same pattern as H.  
   - **Classification: pipeline** (schema inconsistency across the lake).

2. Sides `Buy`/`Sell` with strictly positive `qty` — consistent. No duplicate `trade_id`s of concern. Max gap 38s. Price ~74.8k — plausible.

Overall: **clean** aside from timestamp dtype.

---

## H — Gate.io BTCUSDT perp trades + static (explicit task)

**Trades window:** 2026-05-23 12:00:00–12:59:59 UTC (one hour). **Rows:** 9,359 trades; 1 static row.

### Instrument static

| Field | Value |
|-------|------:|
| instrument | `BTC-USDT:PERP:LINEAR` |
| exchange_symbol | `BTC_USDT` |
| quantity_multiplier | `0.0001` |
| scale | null (treated as unused per brief) |

### Volumes

| Unit | Value |
|------|------:|
| Native contracts `sum(qty)` | **8,959,318** |
| Base BTC `sum(qty × 0.0001)` | **895.9318 BTC** |
| Quote USDT `sum(qty × 0.0001 × price)` | **66,923,451.09224 USDT** |

### Public comparison

Gate.io USDT-futures 1h candle (`BTC_USDT`, `from=1779537600`, `to=1779541199`):

| Gate field | Meaning | Value | Ours | Diff |
|------------|---------|------:|-----:|-----:|
| `v` | contract volume | 8,959,318 | 8,959,318 | **0** |
| `sum` | quote volume | 66,923,451.09224 | 66,923,451.09224 | **0** |

Exact match on both contract and quote volume for the hour. Base BTC is implied by `quantity_multiplier` and is consistent with Gate’s contract size (0.0001 BTC / contract).

### Other DQ

- Timestamp columns are Int64 µs (same as G) → **pipeline** schema inconsistency.  
- 8 backward jumps in `publish_ts` → minor **pipeline**.  
- Ingress−transaction median lag ~1.8ms — healthy.  
- No negative/zero qty.

---

## Cross-cutting themes

1. **Timestamp typing is not uniform:** A/B/C/D/F use timezone-aware Datetime; G/H use raw Int64 µs. Downstream consumers will mishandle joins/plots unless normalised.
2. **Venue clocks sometimes absent:** F has only ingress; A has many null transaction times on trades.
3. **Symbol normalisation differs by asset class:** equities sometimes `S|…` (HKEX), sometimes bare `AAPL-USD:SPOT` (NASDAQ); DreamDex quote `USDso` is idiosyncratic.
4. **Flags you cannot trust:** D’s `snapshot` column is unused (always false).
5. **Not everything is broken:** H matches public data perfectly; B has no crosses; F spreads look tight; G trade tape is coherent.

---

## Appendix

### Reproduce

See [README.md](README.md).

### Microprice

See B section; implementation in `src/lotech_dq/microprice.py`.

### L2 replay sketch

See D section; implementation in `src/lotech_dq/book.py`. Heuristic snapshot threshold = 200 levels (configurable).
