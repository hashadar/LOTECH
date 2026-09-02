# Market Data Quality — Findings

Candidate deliverable for the [LO:TECH take-home](https://gist.github.com/jembishop/6e2aa508cd8c2a19c22515bacf2e86fc). Python + Polars. Every claim comes from [`src/lotech_dq/`](src/lotech_dq) and [`scripts/`](scripts). The numbers are in [`outputs/tables/`](outputs/tables).

Capture latency is within a normal range. Median capture latency is tens of milliseconds. Gate.io volume matches the public candle with a difference of 0. The Binance ETH book is one tick wide on 97.51% of rows.

The normaliser contract is not consistent. Clocks, symbol format, uniqueness rules and flags differ by venue. The six defects below would give a client the wrong picture of the market.

A shared profiler is necessary. It is not sufficient. It classifies C's 461,372 backward `ingress_ts` steps as high-severity pipeline. Most of those steps come from 18 symbols in one file. It classifies G's duplicated file as clean. A monotonicity check that sorts on the column it then differences cannot fail.

Classification uses three classes:

- `pipeline`: the normaliser introduced the defect
- `market`: real venue behaviour that only looks like a defect
- `unclear`: the file cannot settle the class

Units are quote currency throughout: HKD (A), USD (C), USDT (D, F, H), USDso (B). USDT is treated as approximately USD. Amounts are never written with `$`. Durations are SI seconds.

| Client impact | File | Class |
|---|---|---|
| Every `trade_id` emitted twice, so `sum(qty)` is exactly 2× true volume | G Bitfinex | pipeline |
| Session 100% `side=Buy`; 15.65% of trades missing venue time, and those trades are 38.15% of volume | A HKEX trades | pipeline |
| `snapshot` never set; one internally crossed message inverts the replayed book | D Binance L2 | pipeline |
| Filename says 20 symbols, file has 18, none with the `S\|` prefix; 1,030 crossed NBBO rows | C NASDAQ | pipeline |
| `bid_price` and `bid_qty` are null for the last 1.5 h; microprice undefined on 22% of rows | B DreamDex | unclear. Evidence supports pipeline more than market. |
| Both venue clocks absent from the schema, so a null-rate alert cannot run | F Binance ETH | pipeline |

H is the file that matches the venue. The same pipeline can emit a file that reconciles exactly with its venue.

## Contents

1. [Capture latency](#1-capture-latency)
2. [G Bitfinex: duplicate trades](#2-g-bitfinex-duplicate-trades)
3. [A HKEX 2800: side, clocks, and stale quote re-emission](#3-a-hkex-2800-side-clocks-and-stale-quote-re-emission)
4. [D Binance L2: snapshot flag and inverted book](#4-d-binance-l2-snapshot-flag-and-inverted-book)
5. [C NASDAQ: symbol count, symbol format, and crossed NBBO](#5-c-nasdaq-symbol-count-symbol-format-and-crossed-nbbo)
6. [B DreamDex: microprice and null bid](#6-b-dreamdex-microprice-and-null-bid)
7. [H Gate.io: volume matches the public candle](#7-h-gateio-volume-matches-the-public-candle)
8. [F Binance ETH: quote quality and absent clocks](#8-f-binance-eth-quote-quality-and-absent-clocks)
9. [Clock and schema contract by file](#9-clock-and-schema-contract-by-file)
10. [Pipeline recommendations](#pipeline-recommendations)

## How this was done

[`00_download.py`](scripts/00_download.py) fetches the eight parquet objects and checks row counts ([`data/MANIFEST.md`](data/MANIFEST.md)). [`01_profile_all.py`](scripts/01_profile_all.py) runs a shared set of checks. It writes [`profile_summary.json`](outputs/tables/profile_summary.json), [`profile_findings.json`](outputs/tables/profile_findings.json), [`schemas.json`](outputs/tables/schemas.json) and [`clocks_matrix.json`](outputs/tables/clocks_matrix.json). Runners `02`–`08` do the per-file deep dives. [`09_exhibits.py`](scripts/09_exhibits.py) collects sample rows into [`exhibits.json`](outputs/tables/exhibits.json).

The two microprice and top-of-book series are committed under `outputs/tables/`: [`B_microprice_series.parquet`](outputs/tables/B_microprice_series.parquet), [`D_top_of_book_series.parquet`](outputs/tables/D_top_of_book_series.parquet), [`D_top_of_book_series_threshold200.parquet`](outputs/tables/D_top_of_book_series_threshold200.parquet). G and H public reconciliations use committed venue fixtures in [`fixtures/venue/`](fixtures/venue/). Network is required once to populate those fixtures. Reproduce: [README.md](README.md).

---

## 1. Capture latency

Median ingress time minus venue time:

- **A** 33 ms (0 negatives on 109,544 TOB rows)
- **C** 88 ms with p99 286 ms (0 negatives on 3,752,799)
- **D** 0.5 ms against `publish_ts`
- **H** 1.8 ms

No file shows a negative capture latency.

One exception is not latency. File A re-publishes stale quote states with a fresh `ingress_ts` (section 3). A's TOB then shows 41 backward `ingress_ts` steps in stored order. The worst step is −5,332 s. Every other finding is after capture.

---

## 2. G Bitfinex: duplicate trades

**Window** 2026-05-23 12:00:00.913–12:09:50.190 UTC. **642 rows, 321 distinct `trade_id`**, `BTC-USD:SPOT` ([`07_analyse_G_bitfinex.py`](scripts/07_analyse_G_bitfinex.py) → [`G_bitfinex.json`](outputs/tables/G_bitfinex.json)).

Every `trade_id` appears exactly twice. There are 321 groups. Every group has size 2. Price, qty, side and `transaction_ts` are identical in each group. `publish_ts` and `ingress_ts` differ in all 321 groups. Opposite-side pairs: **0**. Exhibit, `trade_id=1922482898`: both copies 74,831 / 0.0001 / Sell at venue time 12:00:00.913, published 12:00:00.914 and 12:00:00.963.

### Ruled-out alternatives

A signed-amount convention is ruled out. There are 0 negative qty values and 0 zero qty values. A maker/taker double-print is ruled out. Sides match in all 321 groups. An append of an adjacent window is ruled out. Every id is duplicated, not a suffix.

Pair separation is on `publish_ts` (median **49.0 ms**, range 15–104 ms). Capture latency is identical on both copies. The venue emitted the trade twice. The file is not two capture paths.

**Classification: pipeline, high confidence** on the duplication. A client summing `qty` reports **10.89697542 BTC** against a true **5.44848771 BTC**. The ratio is exactly 2×. VWAP survives, because the copies are economically identical. Trade count, last trade and inter-trade timing do not survive. 338 of 641 gaps are exactly zero raw. 17 remain after deduplication. Deduplication on `(instrument, trade_id)` is an implemented check. It reports a finding here with 321 groups.

### Public reconciliation

Reconciled against Bitfinex's own REST tape over the same window ([`src/lotech_dq/bitfinex.py`](src/lotech_dq/bitfinex.py), `public_compare` in [`G_bitfinex.json`](outputs/tables/G_bitfinex.json)):

| measure | as delivered | deduped / venue | diff |
|---|---:|---:|---|
| trades | 642 | 321 | 0 (deduped versus venue) |
| base volume (BTC) | 10.89697542 | 5.44848771 | 0 (deduped) |
| notional (USD) | — | 407,630.10018880 | 0 |
| 1m candle volume (BTC) | — | 5.44848771 | 0 |

`as_delivered.overstatement_factor` is **2.0**. `all_diffs_zero` is **true** on the deduplicated file. Trade-id sets are **identical** (321 file, 321 venue, 0 file-only, 0 venue-only). Per-trade match: **321/321** on price, qty and side. The duplication is confirmed externally, not only by internal pairing.

---

## 3. A HKEX 2800: side, clocks, and stale quote re-emission

**Window** TOB 2026-08-13 01:00:00.083–08:08:12.709 UTC; trades from 01:20 in venue time. **109,544 TOB + 9,666 trades**, `S|2800-HKD:SPOT` ([`02_analyse_A_hkex.py`](scripts/02_analyse_A_hkex.py) → [`A_hkex.json`](outputs/tables/A_hkex.json)).

**Side.** 9,666 of 9,666 rows are `side=Buy`. Of 8,153 trades joining the prevailing TOB, Lee-Ready recovers **Buy 3,932 / Sell 3,850**. The recovered counts are almost equal. The recovered split is evidence the label is fixed on every row. **Classification: pipeline.**

**Missing venue time.** `transaction_ts` is null on **1,513 trades (15.65% of rows, 38.15% of volume)**. That cohort has median size 50,000 versus 15,000 elsewhere. Odd lots are 5.09% versus 0%. Trade-through rate is 4.03% versus 0.037%. The missing-time cohort is not a lunch artefact. 1,512 of 1,513 trades fall in continuous trading.

The null-venue rows look like off-board or special-lot prints. They appear to use a path that never carried a venue match time. They do not look like a clock dropped off the main file. The delivered data cannot separate those two fixes. **Classification: pipeline.** Joins on venue time drop 38% of traded volume without an alert. Full cohort table in [`A_hkex.json`](outputs/tables/A_hkex.json).

**Crossed book and skew tail.** The 71 crossed rows and the skew tail share one mechanism. Stale quote states are published more than once. Each re-publication has a fresh `ingress_ts`.

71 crossed rows. All of them fall in continuous trading. 112 locked rows. All locked rows fall in auction windows. Locked is **market**.

The 71 crossed rows are **30 distinct quote states**. **39 re-emissions** arrive more than 60 s late. Stale states re-publish every 5.8–9.2 minutes with a fresh `ingress_ts`. Worst lag is **5,333 s** on the 26.10 / 24.00 state. That state has 13 emissions from 03:36:36 venue time to 05:05:29 ingress.

The pattern is session-wide, not lunch-only. **19 distinct venue timestamps** have skew above 60 s. A second cluster is in the afternoon (25.94 / 25.86, worst **4,457 s**).

Crossed ask prices span nine levels (24.00–25.96). Most inversions are small. The 13 re-emissions of the 26.10 / 24.00 state add the large inversions. **Classification: pipeline** for re-emission. Locked is **market**.

Five TOB gaps exceed 300 s. All of them are session structure:

- auction into continuous at 596 s
- lunch gaps including **300.51 s**, **1,501 s**, **327 s**, **974 s**

Those gaps are **market**. Detail in [`A_hkex.json`](outputs/tables/A_hkex.json).

**Trades versus book.** On joinable trades: 8,153 matched, 3 strict trade-throughs (0.037%). Two are genuine. One sits beside a crossed quote from the re-emission mechanism. Joining the null-venue cohort on `publish_ts` gives **4.03%** trade-through. That rate is 110× the joinable rate.

---

## 4. D Binance L2: snapshot flag and inverted book

**Window** 2026-08-13 14:00:00.014628–14:29:59.915000 UTC. **17,994 incremental L2 messages**, `BTC-USDT:SPOT` ([`05_analyse_D_binance_l2.py`](scripts/05_analyse_D_binance_l2.py), [`book.py`](src/lotech_dq/book.py) → [`D_binance_l2.json`](outputs/tables/D_binance_l2.json)).

`snapshot` is **false on all 17,994 rows**. The column is a mapping defect, not a missing column. `transaction_ts` is 100% null. **Classification: pipeline.**

### Check that uses the message only

`seq_id 98502143047` (index 8997) is the **only message whose own payload crosses**. Bid **63,810.79** versus ask **63,493.72**. Spread **−317.07 USDT**. No replay state is required. This message cannot be a reconstruction artefact. Six ask levels in this message appear nowhere else as asks. 63,812.00 / 63,812.01 is the best bid and best ask on adjacent messages. **Classification: pipeline, high confidence.**

### Replay policy

With no truthful snapshot flag, a size heuristic (≥ 200 levels) is falsifiable. **All 460 candidates carry `qty == 0` deletes.** Snapshots cannot carry deletes. The file has **207,901 deletes**. 20–95% land on levels never delivered, depending on policy. None of those misses are floating-point near-misses. Crossed-state count moves with policy:

| snapshot policy | snapshots | crossed states | deletes missed |
|---|---:|---:|---:|
| none | 0 | 8,997 (50.0%) | 41,796 (20.1%) |
| threshold 200 | 460 | 154 (0.86%) | 128,105 (61.6%) |

Under threshold 200: **154 crossed states**. One contiguous episode **14:15:00.015055 → 14:15:15.315142 UTC** (15.3 s). Worst spread **−318.28 USDT**. The episode ends when a 250-level message clears the book. It does not end when the market stops being crossed. Outside the episode, median spread **0.01 USDT**. Mid range is 63,686–63,884 USDT. Full sweep in [`D_binance_l2.json`](outputs/tables/D_binance_l2.json). Per-message TOB is in [`D_top_of_book_series.parquet`](outputs/tables/D_top_of_book_series.parquet) and [`D_top_of_book_series_threshold200.parquet`](outputs/tables/D_top_of_book_series_threshold200.parquet). Final book is in [`D_final_book.csv`](outputs/tables/D_final_book.csv).

![D Binance BTCUSDT reconstructed top of book, 2026-08-13](outputs/figures/D_binance_l2_mid_spread.png)

---

## 5. C NASDAQ: symbol count, symbol format, and crossed NBBO

**Rows** 3,752,799. **18 instruments, not 20.** Window 2026-08-13 14:00–15:59 UTC by capture ([`04_analyse_C_nasdaq.py`](scripts/04_analyse_C_nasdaq.py) → [`C_nasdaq.json`](outputs/tables/C_nasdaq.json)).

**Contract.** The brief's equity format is `S|2800-HKD:SPOT`. Every C symbol is `AAPL-USD:SPOT`. There is **no `S|` prefix** and no venue qualifier. The filename says 20 symbols. The file has 18. No manifest ships with the export. Shortfalls are undetectable without an external reference. **Classification: pipeline.**

**Crossed NBBO.** 1,030 crossed rows (0.027%). 26,814 locked (0.71%). Both prices have 0 nulls. **229 of 1,030 (22.2%) land in the single second 15:27:42 across 7 symbols**. 296 (28.7%) land in three seconds across 9 symbols (AAPL 139, QQQ 112, TLT 21, NVDA 8, INTC 5, TSLA 5, BOTZ 3, AMZN 2, AMD 1). These crosses are not independent per-symbol latency races.

**881 of 1,030 (85.5%)** share a `transaction_ts` with the preceding quote for the same instrument. 341 have unchanged prices. The cross survives a size-only update. C's crosses use ordinary round lots at **299 distinct ask prices**. File A's crosses used a handful of stale small-size asks. **Classification: pipeline.**

Locked rows concentrate on ETFs with a one-cent spread (BOTZ 8.05%, TLT 4.74%). Locked rate falls to 0% on wide-spread names. Locked is **market**.

**Clocks.** The file is stored in `transaction_ts` order (0 backward venue steps). Partitioning `ingress_ts` by instrument reduces backward steps from **461,372 to 12,514** (97% multiplexing artefact). `publish_ts` still has **1,259 backward steps** after partitioning. **Classification: pipeline, low severity.**

A zero count was once reported. That result came from sorting on `ingress_ts` before differencing it. That check cannot fail. `seq_id` is not monotone within instrument (**158,337 backward steps**). `seq_id` is defined against a channel the file omits. Detail in [`C_nasdaq_per_symbol.csv`](outputs/tables/C_nasdaq_per_symbol.csv) and [`exhibits.json`](outputs/tables/exhibits.json).

---

## 6. B DreamDex: microprice and null bid

**Window** 2026-08-14 04:00:01.318–10:13:52.425 UTC. **6,988 rows**, `WETH-USDso:SPOT` ([`03_analyse_B_dreamdex.py`](scripts/03_analyse_B_dreamdex.py) → [`B_dreamdex_microprice.json`](outputs/tables/B_dreamdex_microprice.json)).

Microprice is **undefined** on **1,540 rows (22.0%)**. `bid_price` and `bid_qty` are null together. 0 crossed books. Median spread **0.57 USDso**. Defined series in [`B_microprice_series.parquet`](outputs/tables/B_microprice_series.parquet).

![Microprice, WETH-USDso, 2026-08-14](outputs/figures/B_microprice.png)

The undefined region is dominated by one run: **1,513 consecutive rows** from 08:42:30 to end of file ([`exhibits.json`](outputs/tables/exhibits.json) → `B.longest_runs`). Shorter runs precede it (runs of **11** and **7** rows in the 90 seconds before the bid goes null).

The bid goes null between two updates **131 µs apart**. The ask is identical across that boundary. `bid_price` and `bid_qty` are null on exactly the same 1,540 rows. `bid_qty` is never 0 anywhere in the file.

Inside the last run the ask keeps moving. There are **1,512 price changes**, 550 distinct ask prices, and a max ingress gap of 18.9 s over 5,482 s.

**Classification: unclear.** The evidence supports a pipeline defect more than a market defect. A dropped bid side fits all four facts. A one-sided DEX book fits none of them cleanly. `transaction_ts` and `publish_ts` are present and **100% null**. That clock gap is **pipeline**. `USDso` is an unrecognised quote-asset code.

---

## 7. H Gate.io: volume matches the public candle

**Window** 2026-05-23 12:00:00.979–12:59:59.684 UTC. **9,359 trades**, 1 static row ([`08_analyse_H_gateio.py`](scripts/08_analyse_H_gateio.py) → [`H_gateio_volume.json`](outputs/tables/H_gateio_volume.json)).

| unit | value |
|---|---:|
| Native contracts | **8,959,318** |
| Base BTC (`qty × 0.0001`) | **895.9318 BTC** |
| Quote USDT | **66,923,451.09224 USDT** |

Against the public Gate.io USDT-futures 1h candle for `BTC_USDT` (`from=1779537600`, `to=1779541199`, [API](https://api.gateio.ws/api/v4/futures/usdt/candlesticks?contract=BTC_USDT&from=1779537600&to=1779541199&interval=1h)): contract volume diff **0**, quote diff **0**. Base BTC is not separately published on that endpoint. It is implied by the 0.0001 contract size.

`trade_id` has no duplicates. `qty` is integral everywhere. There are no negatives and no zeros. Volume is balanced across sides: Buy 4,467,142 contracts / 4,611 trades, Sell 4,492,176 / 4,748.

Clocks are Int64 µs timestamps, as in G. There are 8 backward `publish_ts` jumps. Median capture latency is 1.8 ms.

The list below is the standard for every file:

- units stated
- static applied
- public series compared
- difference shown

G meets that standard after deduplication. Deduplicated volume matches Bitfinex exactly (section 2).

---

## 8. F Binance ETH: quote quality and absent clocks

**Window** 2026-08-13 02:00:00.257–03:59:59.695 UTC. **235,270 rows**, `ETH-USDT:SPOT` ([`06_analyse_F_binance_eth.py`](scripts/06_analyse_F_binance_eth.py) → [`F_binance_eth.json`](outputs/tables/F_binance_eth.json)).

Both venue clocks are absent from the parquet schema. Columns are `instrument, ingress_ts, seq_id, bid_price, bid_qty, ask_price, ask_qty`. An absent column cannot trigger a null-rate alert. The profiler originally reported **no findings** for F. D is the same venue ten hours earlier and kept `publish_ts`. **Classification: pipeline.**

**Quote quality.** One tick (0.01 USDT) wide on **97.51%** of rows. Spread p99 0.05 USDT. Max 0.89 USDT. The book never locks. The book never crosses. **14,678 duplicate quote groups** (36,185 rows). **0 consecutive identical quotes**. 97.10% of consecutive pairs differ only in size. Near-static prices plus size updates regenerate tuples that are not adjacent.

---

## 9. Clock and schema contract by file

From [`clocks_matrix.json`](outputs/tables/clocks_matrix.json). "Absent" and "present, 100% null" are different defects.

| file | ingress_ts | transaction_ts | publish_ts | clock dtype |
|---|---|---|---|---|
| A TOB | present | present | present | Datetime µs UTC |
| A trades | present | **15.65% null** | present | Datetime µs UTC |
| B | present | **present, 100% null** | **present, 100% null** | Datetime µs UTC |
| C | present | present | present | Datetime µs UTC |
| D | present | **present, 100% null** | present | Datetime µs UTC |
| F | present | **absent** | **absent** | Datetime µs UTC |
| G | present | present | present | **Int64 epoch µs** |
| H trades | present | present | present | **Int64 epoch µs** |
| H static | present | **absent** | **present, 100% null** | **Int64 epoch µs** |

Symbol format differs:

- HKEX `S|` prefix
- NASDAQ `AAPL-USD:SPOT`
- DreamDex `USDso`

Flags differ. D's `snapshot` is Boolean and false on every row. G's `trade_id` is unique at the venue and duplicated in the file. C's `seq_id` is not monotone within instrument. The contract is not one contract. Each file needs venue-specific validation. A single global schema is not enough.

---

## Pipeline recommendations

Five priorities. Items 1 and 2 are already built in [`checks.py`](src/lotech_dq/checks.py):

1. **Uniqueness** — `(instrument, trade_id)` unique. Reports a finding on G (321 groups). **Built.**
2. **Clock completeness** — alert on `transaction_ts` null rate above 1% *and* on absent clock columns. Reports a finding on A trades, B, D, F, H static. **Built.**
3. **Stateless L2 check** — reject any single L2 message whose own bid and ask cross. On D that is one message in 17,994. No replay is required.
4. **Stateful L2 discipline** — require `snapshot=true` on real snapshots. Never infer from message size (on D every size candidate carries deletes). Report missed deletes as a first-class metric.
5. **Export completeness** — ship a manifest of the expected instrument universe. C's two missing symbols were detectable only because the count was in the filename.

H shows that correct output is possible. Replay policy is configurable in [`book.py`](src/lotech_dq/book.py). Every policy is disclosed in [`D_binance_l2.json`](outputs/tables/D_binance_l2.json).
