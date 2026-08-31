# Actions before final submission

Hand-off checklist for converting the current draft (`draft/audit-and-regeneration` snapshot) into the final LO:TECH take-home deliverable. **Do not re-run the full audit from scratch** unless verification fails; start from `outputs/review/*.md` and the committed `outputs/tables/*.json`.

Draft metrics (baseline): `WRITEUP.md` **5,586 words**, nine numbered finding sections plus cross-cutting §9 and pipeline recommendations. Pipeline already emits **`G_bitfinex.json` → `public_compare`** (Bitfinex public tape reconciliation); prose in §2 and §7 still partly pre-dates that work.

---

### Must do before submitting

1. **Integrate G public reconciliation into §2 (highest prose gap)**  
   - **Evidence:** `outputs/tables/G_bitfinex.json` → `public_compare`: **321** venue trades vs **642** file rows; deduped file matches venue on trades, `vol_base` **5.44848771 BTC**, notional **407630.10018880 USD**, candle volume diff **0**; `as_delivered.overstatement_factor` **2.0**; `all_diffs_zero`: **true**; per-trade **321/321** price/qty/side agreement.  
   - **WRITEUP gaps:** §2 has no public-reconciliation paragraph or table; §7 L226 still says *"G's duplication could be settled the same way against a public Bitfinex candle, and was not."* — **delete/replace** with H-parity wording (units, window, diff shown).  
   - **Method pointer:** `scripts/07_analyse_G_bitfinex.py` + `src/lotech_dq/bitfinex.py`; optional narrative source: `outputs/review/G_public_reconciliation.md` (local only — do not commit `outputs/review/`).  
   - **Keep:** internal 2× pairing proof; add external proof as second pillar (id-set identity + volume match).

2. **Fix stale reproducibility text now that exhibits are in git**  
   - **`WRITEUP.md` L22:** still claims *"the repository's `*.parquet` rule keeps them out of git"*. **Update** to link the three committed series:  
     - `outputs/tables/B_microprice_series.parquet`  
     - `outputs/tables/D_top_of_book_series.parquet`  
     - `outputs/tables/D_top_of_book_series_threshold200.parquet`  
   - **`README.md` L59–60:** same stale sentence — align with `.gitignore` (`*.parquet` ignored except `!outputs/tables/*.parquet`).  
   - Spot-check all **63** relative links in `WRITEUP.md` resolve in a clean clone (figures, JSON, CSV, parquet, `data/MANIFEST.md`, `scripts/09_exhibits.py`).

3. **Length strategy — draft is ~5,586 words; brief asks for a "short write-up"**  
   **Recommended:** **split**, not blunt cut of findings.  
   - **`WRITEUP.md` (submission-facing, target ~2,500–3,500 words):** thesis table, §1 latency, §2 G, §3 A (compressed), §4 D (state-free proof + one replay paragraph), §5 C (headline defects only), §6 B (undefined region + plot reference), §7 H, §8 F (promote from stub — see item 4), §9 clocks **table only** + one paragraph, pipeline **top 5** recommendations.  
   - **`APPENDIX.md` or `TECHNICAL_DETAIL.md` (optional second file, linked once):** A §3.3 mechanism detail (71 crossed rows, 19 venue timestamps, gap table with **5** gaps >300 s including **300.51228 s**), D threshold sweep (**2 / 154 / 8,997** crossed states), C monotonicity tautology (**461,372 → 12,514**), B run-length / `exhibits.json` longest runs, full "What we are **not** claiming" list, full pipeline list (items 1–9).  
   - **If user insists on single file:** apply cuts from `outputs/review/compliance_editorial_review.md` §4.4 (fold §9 F stub into §8, remove duplicate reproduce block, compress §1, trim artefact-map "Code" column) — expect **~800–1,200** word reduction, still likely long.

4. **F (Binance ETH) — brief parity**  
   - Compliance noted F is only a stub inside §8/§9 while other letters get full sections. **Either** expand §8 to match A–G structure (window, row count, instrument, runner link, classification) **or** merge §9 "F in one place" into §8 and delete redundancy.  
   - Preserve facts: **14,678** duplicate quote groups; **0** consecutive identical quotes; missing clocks (**transaction_ts` / `publish_ts` absent**).

5. **Offline reproducibility for live API steps (G + H)**  
   - **`08_analyse_H_gateio.py`:** cache Gate.io candle JSON under e.g. `data/cache/gateio/` or `fixtures/`; re-use on second run; document in README. Last verified: `H_gateio_volume.json` — contract **8,959,318**, quote **66,923,451.09224 USDT**, diffs **0**.  
   - **`07_analyse_G_bitfinex.py` / `bitfinex.py`:** same for Bitfinex trade + candle responses (see `public_compare.requests` in JSON). Copy raw JSON from `outputs/review/scratch/raw_P*.json` into a committed cache path if licence allows.  
   - README must state: **network required once** to populate cache; thereafter offline.

6. **Editorial fixes from compliance review (quick wins)**  
   - §5: change *"Filename **and brief** say 20 symbols"* → *"The filename says 20 symbols; the file has 18."*  
   - Add explicit note that brief has **no E file** (partially in §"How this was done" — ensure it survives length cut).  
   - **Units on spreads:** B median **0.57 USDso**, D/F/C **0.01 USDT/USD** where cited.  
   - **Classification legend:** already at L7 — enforce three labels consistently; remove ad-hoc "not a defect" unless mapped to disposal in §9.

7. **Commit hygiene for final tag**  
   - Ensure `outputs/review/` stays ignored; do not commit scratch under `outputs/review/scratch/`.  
   - Confirm `data/*.parquet` never staged (`data/MANIFEST.md` only).  
   - Consider a single final commit message after prose pass: *"Final submission: short write-up, linked exhibits, cached venue fixtures"*.

---

### Should strengthen

- **Table of contents** after thesis (9 sections + appendix link if split).  
- **Embed or prominently display** `outputs/figures/B_microprice.png` and `D_binance_l2_mid_spread.png` in `WRITEUP.md` (brief explicitly asks to plot B).  
- **Promote** profiler false-alarm paragraph (C ingress tautology, G "clean" file) from "How this was done" into §1 or thesis — differentiator.  
- **B §6:** cite `exhibits.json` → `B.longest_runs` (runs of **11** and **7** rows before terminal **1,513**-row ask-only run) as evidence for feed-death hypothesis.  
- **H:** one sentence that base BTC is not independently on the candle endpoint (already partially there); optional second public source if found.  
- **`09_exhibits.py`:** mention in §1 artefact map if not trimmed away.  
- **D sensitivity:** one sentence pointing to both parquet TOB series and `D_binance_l2.json` policy block — already partially done; ensure survives cut.

---

### Do not change

- **H control narrative:** native **8,959,318** contracts, base **895.9318 BTC**, quote **66,923,451.09224 USDT**; Gate.io 1h candle diffs **0** on contract and quote (`outputs/tables/H_gateio_volume.json`).  
- **G duplication finding:** **321** distinct `trade_id`, **642** rows, **2×** volume (**10.89697542** vs **5.44848771 BTC**) — now **externally** confirmed via `public_compare`.  
- **D state-free proof:** message index **8997**, `seq_id 98502143047`, locally crossed bid **63,810.79** vs ask **63,493.72**; phantom **ask block**; not a replay artefact.  
- **D episode framing:** **154** crossed states, one contiguous episode **14:15:00.015055 → 14:15:15.315142 UTC**, **15.300087 s** (threshold-200 policy).  
- **A side fabrication + recoverability:** 100% Buy; Lee-Ready **3,932 / 3,850** on joinable trades.  
- **A clock drop:** **15.65%** rows, **38.15%** volume; mechanism in §3.3 (session-wide re-emission, not lunch-only).  
- **C crossed burst:** **229/1,030** in one second **15:27:42** — pipeline classification, not SIP flicker.  
- **C ingress monotonicity fix:** partitioned backward steps **12,514** (not **0**); tautology documented in `exhibits.json`.  
- **Built checks** referenced in pipeline § (uniqueness, clock absence, partitioned monotonicity).  
- **No redistribution** of raw `data/*.parquet`; download via `scripts/00_download.py`.

---

### Verification checklist

Run from repo root after `python -m pip install -e .` and populated `data/` (or cached fixtures):

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

**Spot-check numbers (JSON is source of truth):**

| Check | File / key | Expected |
|-------|------------|----------|
| G duplicate groups | `G_bitfinex.json` | **321** groups, size 2 |
| G public match | `G_bitfinex.json` → `public_compare.all_diffs_zero` | **true** |
| G 2× as delivered | `public_compare.as_delivered.overstatement_factor` | **2.0** |
| H Gate diff | `H_gateio_volume.json` | contract & quote diff **0** |
| D crossed episode | `D_binance_l2.json` | **154** states, ~**15.3 s** episode |
| C per-instrument ingress | `C_nasdaq.json` / `exhibits.json` | **12,514** backward steps partitioned |
| A gaps >300 s | `A_hkex.json` | **5** (incl. **300.51228 s** gap) |
| Clocks matrix rows | `clocks_matrix.json` | **9** rows incl. H static |
| B undefined share | `B_dreamdex_microprice.json` | **22.0%** undefined |
| Row counts vs brief | `data/MANIFEST.md` | all **[OK]** vs expected rows |

**Links:** open `WRITEUP.md` in preview; click every `outputs/` and `scripts/` link.  
**Figures:** confirm PNGs render (B microprice, D mid/spread).  
**Word count:** `python -c "print(len(open('WRITEUP.md',encoding='utf-8').read().split()))"` — re-check after cut/split.

---

### Open decisions for the user

1. **Length:** single shortened `WRITEUP.md` vs **WRITEUP + APPENDIX** (recommended split above)? Reviewers may prefer one PDF — if so, generate export from combined markdown.  
2. **Commit venue API caches:** OK to vend small JSON fixtures in-repo for reproducibility, or keep network-mandatory with documented snapshot date?  
3. **Final branch name:** merge `draft/audit-and-regeneration` to `master` for submission, or submit from the draft branch?  
4. **G mechanism:** keep `te`/`tu` as explicit hypothesis only, or add one sentence that public id-set match rules out partial-window / suffix append hypotheses (already in review doc)?  
5. **Include `ACTIONS_BEFORE_FINAL.md` in final submission** or delete before tag (currently useful for agents; may look internal to reviewers).

---

*Generated at draft snapshot; cross-reference `outputs/review/rewrite_log.md`, `regenerated_numbers.md`, `compliance_editorial_review.md` for audit trail (local, not in git).*
