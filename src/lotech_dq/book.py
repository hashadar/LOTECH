from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import polars as pl


@dataclass
class BookState:
    bids: dict[float, float] = field(default_factory=dict)
    asks: dict[float, float] = field(default_factory=dict)

    def clear(self) -> None:
        self.bids.clear()
        self.asks.clear()

    def apply_side(self, side: str, prices: list[float], qtys: list[float]) -> int:
        """Apply level updates. qty==0 deletes. Returns negative-qty count."""
        book = self.bids if side == "bid" else self.asks
        neg = 0
        for price, qty in zip(prices, qtys):
            if qty is None or price is None:
                continue
            q = float(qty)
            p = float(price)
            if q < 0:
                neg += 1
            if q == 0:
                book.pop(p, None)
            else:
                book[p] = q
        return neg

    def replace_side(self, side: str, prices: list[float], qtys: list[float]) -> int:
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


def replay_order_book(
    df: pl.DataFrame,
    snapshot_level_threshold: int = 200,
) -> dict[str, Any]:
    """Replay Binance-style L2 where each row carries bid/ask price/qty lists.

    Snapshot detection:
    - explicit `snapshot == True`, or
    - combined level count >= snapshot_level_threshold (heuristic when flag is broken)
    """
    required = ["bid_prices", "bid_qtys", "ask_prices", "ask_qtys"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing L2 list columns {missing}; have {df.columns}")

    sort_cols = [c for c in ("seq_id", "ingress_ts", "transaction_ts") if c in df.columns]
    work = df.sort(sort_cols) if sort_cols else df

    book = BookState()
    tops: list[dict[str, Any]] = []
    integrity: dict[str, Any] = {
        "crossed_events": 0,
        "locked_events": 0,
        "empty_bid_events": 0,
        "empty_ask_events": 0,
        "negative_qty_levels": 0,
        "updates_applied": 0,
        "snapshot_rows_flagged": 0,
        "snapshot_rows_heuristic": 0,
        "delta_rows": 0,
        "seq_id_gaps": 0,
        "snapshot_col_true_count": 0,
        "first_row_treated_as_snapshot": False,
        "snapshot_level_threshold": snapshot_level_threshold,
    }

    prev_seq: int | None = None
    seeded = False

    # Use row indices via to_numpy-ish iteration without timezone dict conversion issues
    n = work.height
    bid_prices = work["bid_prices"].to_list()
    bid_qtys = work["bid_qtys"].to_list()
    ask_prices = work["ask_prices"].to_list()
    ask_qtys = work["ask_qtys"].to_list()
    snap_flags = (
        work["snapshot"].to_list() if "snapshot" in work.columns else [False] * n
    )
    seq_ids = work["seq_id"].to_list() if "seq_id" in work.columns else [None] * n
    ingress = work["ingress_ts"].dt.timestamp("us").to_list() if "ingress_ts" in work.columns else [None] * n

    integrity["snapshot_col_true_count"] = int(sum(1 for s in snap_flags if s))

    for i in range(n):
        bp, bq = bid_prices[i] or [], bid_qtys[i] or []
        ap, aq = ask_prices[i] or [], ask_qtys[i] or []
        n_lvl = len(bp) + len(ap)
        flagged = bool(snap_flags[i])
        heuristic = n_lvl >= snapshot_level_threshold
        is_snapshot = flagged or heuristic or (not seeded)

        if flagged:
            integrity["snapshot_rows_flagged"] += 1
        if heuristic and not flagged:
            integrity["snapshot_rows_heuristic"] += 1
        if not seeded and not flagged and not heuristic:
            integrity["first_row_treated_as_snapshot"] = True

        if is_snapshot:
            book.clear()
            integrity["negative_qty_levels"] += book.replace_side("bid", bp, bq)
            integrity["negative_qty_levels"] += book.replace_side("ask", ap, aq)
            seeded = True
        else:
            integrity["delta_rows"] += 1
            integrity["negative_qty_levels"] += book.apply_side("bid", bp, bq)
            integrity["negative_qty_levels"] += book.apply_side("ask", ap, aq)

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
        if book.is_crossed():
            integrity["crossed_events"] += 1
        if book.is_locked():
            integrity["locked_events"] += 1

        tops.append(
            {
                "ingress_ts_us": ingress[i],
                "seq_id": seq,
                "n_levels_in_msg": n_lvl,
                "is_snapshot": is_snapshot,
                "best_bid": bb,
                "best_bid_size": bbs,
                "best_ask": ba,
                "best_ask_size": bas,
                "mid": ((bb + ba) / 2) if bb is not None and ba is not None else None,
                "spread": (ba - bb) if bb is not None and ba is not None else None,
                "crossed": book.is_crossed(),
                "n_bid_levels": len(book.bids),
                "n_ask_levels": len(book.asks),
            }
        )

    top_df = pl.DataFrame(tops) if tops else pl.DataFrame()
    return {"integrity": integrity, "top_of_book": top_df, "final": book}
