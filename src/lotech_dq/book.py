from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import polars as pl


@dataclass
class SideResult:
    """Per-message outcome of applying one side's level list."""

    upserts: int = 0
    deletes_applied: int = 0
    deletes_missed: int = 0
    negative_rejected: int = 0

    def add(self, other: SideResult) -> None:
        self.upserts += other.upserts
        self.deletes_applied += other.deletes_applied
        self.deletes_missed += other.deletes_missed
        self.negative_rejected += other.negative_rejected


@dataclass
class BookState:
    bids: dict[float, float] = field(default_factory=dict)
    asks: dict[float, float] = field(default_factory=dict)

    def clear(self) -> None:
        self.bids.clear()
        self.asks.clear()

    def apply_side(self, side: str, prices: list[float], qtys: list[float]) -> SideResult:
        """Apply level updates. qty==0 deletes; a delete for an absent level is counted.

        A negative quantity is rejected rather than stored. A live level cannot have
        negative size. Storing it would corrupt the best bid or best ask with no alert.
        """
        if len(prices) != len(qtys):
            raise ValueError(
                f"{side} price/qty list length mismatch: {len(prices)} vs {len(qtys)}"
            )
        book = self.bids if side == "bid" else self.asks
        res = SideResult()
        for price, qty in zip(prices, qtys, strict=True):
            if qty is None or price is None:
                continue
            q = float(qty)
            p = float(price)
            if q < 0:
                res.negative_rejected += 1
                continue
            if q == 0:
                if book.pop(p, None) is None:
                    res.deletes_missed += 1
                else:
                    res.deletes_applied += 1
            else:
                book[p] = q
                res.upserts += 1
        return res

    def replace_side(self, side: str, prices: list[float], qtys: list[float]) -> SideResult:
        book = self.bids if side == "bid" else self.asks
        book.clear()
        return self.apply_side(side, prices, qtys)

    def best_bid(self) -> tuple[float | None, float | None]:
        if not self.bids:
            return None, None
        p = max(self.bids)
        return p, self.bids[p]

    def best_ask(self) -> tuple[float | None, float | None]:
        if not self.asks:
            return None, None
        p = min(self.asks)
        return p, self.asks[p]

    def is_crossed(self) -> bool:
        bb, _ = self.best_bid()
        ba, _ = self.best_ask()
        return bb is not None and ba is not None and bb > ba

    def is_locked(self) -> bool:
        bb, _ = self.best_bid()
        ba, _ = self.best_ask()
        return bb is not None and ba is not None and bb == ba

    def to_frame(self) -> pl.DataFrame:
        rows = [{"side": "bid", "price": p, "qty": q} for p, q in sorted(self.bids.items(), reverse=True)]
        rows += [{"side": "ask", "price": p, "qty": q} for p, q in sorted(self.asks.items())]
        if not rows:
            return pl.DataFrame(schema={"side": pl.Utf8, "price": pl.Float64, "qty": pl.Float64})
        return pl.DataFrame(rows)


def message_is_internally_crossed(
    bid_prices: list[float],
    bid_qtys: list[float],
    ask_prices: list[float],
    ask_qtys: list[float],
) -> bool:
    """True if one message's own live levels put its best bid above its best ask.

    Stateless: uses no replay state. This separates a source defect from a
    reconstruction artefact.
    """
    live_bids = [p for p, q in zip(bid_prices, bid_qtys, strict=True) if q]
    live_asks = [p for p, q in zip(ask_prices, ask_qtys, strict=True) if q]
    if not live_bids or not live_asks:
        return False
    return max(live_bids) > min(live_asks)


def replay_order_book(
    df: pl.DataFrame,
    snapshot_level_threshold: int = 200,
    validate_snapshots: bool = True,
    seed_first_row: bool = True,
) -> dict[str, Any]:
    """Replay Binance-style L2 where each row carries bid/ask price/qty lists.

    Snapshot detection, in order of authority:

    1. `snapshot == True` on the row.
    2. A level-count heuristic (`>= snapshot_level_threshold`) for feeds whose flag is
       unusable. With `validate_snapshots` the candidate is rejected if the message
       carries any `qty == 0` entry. A complete picture of the book cannot also
       carry delete instructions. Rejected candidates are applied as deltas and counted.
    3. The first row, seeded so the replay has somewhere to start (`seed_first_row`).

    `validate_snapshots=False` reproduces the unguarded heuristic. The threshold
    sweep needs that mode to show how much of the result the threshold is choosing.
    """
    required = ["bid_prices", "bid_qtys", "ask_prices", "ask_qtys"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing L2 list columns {missing}; have {df.columns}")

    sort_cols = [c for c in ("seq_id", "ingress_ts", "transaction_ts") if c in df.columns]
    work = df.sort(sort_cols) if sort_cols else df

    book = BookState()
    tops: list[dict[str, Any]] = []
    totals = SideResult()
    integrity: dict[str, Any] = {
        "crossed_states": 0,
        "crossed_episodes": 0,
        "locked_states": 0,
        "empty_bid_events": 0,
        "empty_ask_events": 0,
        "negative_qty_levels_rejected": 0,
        "deletes_applied": 0,
        "deletes_missed": 0,
        "deletes_total": 0,
        "deletes_missed_pct": 0.0,
        "level_upserts": 0,
        "updates_applied": 0,
        "snapshot_rows_flagged": 0,
        "snapshot_rows_heuristic_accepted": 0,
        "snapshot_rows_heuristic_rejected": 0,
        "heuristic_rejected_delete_entries": 0,
        "delta_rows": 0,
        "seq_id_gaps": 0,
        "snapshot_col_true_count": 0,
        "first_row_treated_as_snapshot": False,
        "first_row_seeded_levels": 0,
        "messages_internally_crossed": 0,
        "snapshot_level_threshold": snapshot_level_threshold,
        "validate_snapshots": validate_snapshots,
    }

    prev_seq: int | None = None
    seeded = False
    prev_crossed = False

    n = work.height
    bid_prices = work["bid_prices"].to_list()
    bid_qtys = work["bid_qtys"].to_list()
    ask_prices = work["ask_prices"].to_list()
    ask_qtys = work["ask_qtys"].to_list()
    snap_flags = work["snapshot"].to_list() if "snapshot" in work.columns else [None] * n
    seq_ids = work["seq_id"].to_list() if "seq_id" in work.columns else [None] * n
    ingress = (
        work["ingress_ts"].dt.timestamp("us").to_list()
        if "ingress_ts" in work.columns
        else [None] * n
    )

    integrity["snapshot_col_true_count"] = int(sum(1 for s in snap_flags if s is True))
    integrity["snapshot_col_nulls"] = int(sum(1 for s in snap_flags if s is None))

    for i in range(n):
        bp, bq = bid_prices[i] or [], bid_qtys[i] or []
        ap, aq = ask_prices[i] or [], ask_qtys[i] or []
        n_lvl = len(bp) + len(ap)
        n_deletes_in_msg = sum(1 for q in bq if q == 0) + sum(1 for q in aq if q == 0)
        flagged = snap_flags[i] is True

        heuristic_candidate = (
            snapshot_level_threshold is not None and n_lvl >= snapshot_level_threshold
        )
        heuristic_ok = heuristic_candidate
        if heuristic_candidate and validate_snapshots and n_deletes_in_msg > 0:
            heuristic_ok = False
            integrity["snapshot_rows_heuristic_rejected"] += 1
            integrity["heuristic_rejected_delete_entries"] += n_deletes_in_msg
        elif heuristic_candidate and not flagged:
            integrity["snapshot_rows_heuristic_accepted"] += 1

        seeding = seed_first_row and not seeded
        is_snapshot = flagged or heuristic_ok or seeding

        if flagged:
            integrity["snapshot_rows_flagged"] += 1
        if seeding and not flagged and not heuristic_ok:
            integrity["first_row_treated_as_snapshot"] = True

        if message_is_internally_crossed(bp, bq, ap, aq):
            integrity["messages_internally_crossed"] += 1

        if is_snapshot:
            book.clear()
            totals.add(book.replace_side("bid", bp, bq))
            totals.add(book.replace_side("ask", ap, aq))
            if seeding:
                integrity["first_row_seeded_levels"] = len(book.bids) + len(book.asks)
                integrity["first_row_delete_entries"] = n_deletes_in_msg
            seeded = True
        else:
            integrity["delta_rows"] += 1
            totals.add(book.apply_side("bid", bp, bq))
            totals.add(book.apply_side("ask", ap, aq))

        integrity["updates_applied"] += 1

        seq = seq_ids[i]
        if seq is not None and prev_seq is not None and seq > prev_seq + 1:
            integrity["seq_id_gaps"] += int(seq - prev_seq - 1)
        if seq is not None:
            prev_seq = int(seq)

        bb, bbs = book.best_bid()
        ba, bas = book.best_ask()
        if bb is None:
            integrity["empty_bid_events"] += 1
        if ba is None:
            integrity["empty_ask_events"] += 1
        crossed = book.is_crossed()
        if crossed:
            integrity["crossed_states"] += 1
            if not prev_crossed:
                integrity["crossed_episodes"] += 1
        prev_crossed = crossed
        if book.is_locked():
            integrity["locked_states"] += 1

        tops.append(
            {
                "ingress_ts_us": ingress[i],
                "seq_id": seq,
                "n_levels_in_msg": n_lvl,
                "n_deletes_in_msg": n_deletes_in_msg,
                "is_snapshot": is_snapshot,
                "best_bid": bb,
                "best_bid_size": bbs,
                "best_ask": ba,
                "best_ask_size": bas,
                "mid": ((bb + ba) / 2) if bb is not None and ba is not None else None,
                "spread": (ba - bb) if bb is not None and ba is not None else None,
                "crossed": crossed,
                "n_bid_levels": len(book.bids),
                "n_ask_levels": len(book.asks),
            }
        )

    integrity["negative_qty_levels_rejected"] = totals.negative_rejected
    integrity["deletes_applied"] = totals.deletes_applied
    integrity["deletes_missed"] = totals.deletes_missed
    integrity["deletes_total"] = totals.deletes_applied + totals.deletes_missed
    integrity["level_upserts"] = totals.upserts
    if integrity["deletes_total"]:
        integrity["deletes_missed_pct"] = (
            totals.deletes_missed / integrity["deletes_total"] * 100.0
        )
    integrity["crossed_pct"] = (
        integrity["crossed_states"] / max(integrity["updates_applied"], 1) * 100.0
    )

    top_df = pl.DataFrame(tops) if tops else pl.DataFrame()
    return {"integrity": integrity, "top_of_book": top_df, "final": book}
