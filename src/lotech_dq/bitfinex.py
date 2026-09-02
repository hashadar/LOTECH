"""Bitfinex public REST v2 access (api-pub), for reconciling G against the venue.

Every failure mode raises. An empty list or a zero returned on a network error would read
downstream as a venue that traded nothing, which is the one wrong answer a reconciliation
must never produce.
"""
from __future__ import annotations

import time
from typing import Any

import httpx

from lotech_dq import cache as venue_cache

BASE_URL = "https://api-pub.bitfinex.com/v2"
USER_AGENT = "lotech-dq/1.0 (take-home reconciliation)"
MIN_INTERVAL_S = 3.0  # the endpoint is rate limited; one request per 3 s is well inside it
TIMEOUT_S = 30.0
MAX_ATTEMPTS = 3
MAX_PAGES = 50

_last_request_at = 0.0


class BitfinexError(RuntimeError):
    """Public API unreachable, refusing the request, or returning an error payload."""


def trades_url(symbol: str) -> str:
    return f"{BASE_URL}/trades/{symbol}/hist"


def candles_url(symbol: str, timeframe: str = "1m") -> str:
    return f"{BASE_URL}/candles/trade:{timeframe}:{symbol}/hist"


def _throttle() -> None:
    global _last_request_at
    if _last_request_at:
        wait = MIN_INTERVAL_S - (time.monotonic() - _last_request_at)
        if wait > 0:
            time.sleep(wait)
    _last_request_at = time.monotonic()


def _retry_after_s(response: httpx.Response, fallback: float) -> float:
    raw = response.headers.get("Retry-After")
    try:
        return max(float(raw), fallback)
    except (TypeError, ValueError):
        return fallback


def _get(url: str, params: dict[str, Any], log: list[dict[str, Any]]) -> list:
    """One GET, retried on transport failure and rate limiting, logged either way."""
    cache_key = venue_cache.key_for_request(url, params)
    cached = venue_cache.load("bitfinex", cache_key)
    if cached is not None:
        log.append(
            {
                "url": url,
                "params": dict(params),
                "attempt": 0,
                "status": 200,
                "rows": len(cached),
                "source": "fixture",
            }
        )
        return cached

    delay = MIN_INTERVAL_S
    for attempt in range(1, MAX_ATTEMPTS + 1):
        _throttle()
        t0 = time.monotonic()
        entry: dict[str, Any] = {"url": url, "params": dict(params), "attempt": attempt}
        try:
            with httpx.Client(timeout=TIMEOUT_S, headers={"User-Agent": USER_AGENT}) as client:
                response = client.get(url, params=params)
        except httpx.HTTPError as exc:
            entry |= {
                "status": None,
                "rows": None,
                "elapsed_s": round(time.monotonic() - t0, 3),
                "error": f"{type(exc).__name__}: {exc}",
            }
            log.append(entry)
            if attempt == MAX_ATTEMPTS:
                raise BitfinexError(
                    f"GET {url} {params}: transport failure after {MAX_ATTEMPTS} attempts "
                    f"({type(exc).__name__}: {exc})"
                ) from exc
            time.sleep(delay)
            delay *= 2
            continue

        entry |= {"status": response.status_code, "elapsed_s": round(time.monotonic() - t0, 3)}
        if response.status_code == 429:
            entry |= {"rows": None, "error": "rate limited"}
            log.append(entry)
            if attempt == MAX_ATTEMPTS:
                raise BitfinexError(
                    f"GET {url} {params}: rate limited after {MAX_ATTEMPTS} attempts"
                )
            time.sleep(_retry_after_s(response, delay))
            delay *= 2
            continue
        if response.status_code != 200:
            entry |= {"rows": None, "error": response.text[:300]}
            log.append(entry)
            raise BitfinexError(
                f"GET {url} {params}: HTTP {response.status_code} {response.text[:300]}"
            )

        try:
            data = response.json()
        except ValueError as exc:
            entry |= {"rows": None, "error": f"undecodable body: {response.text[:300]}"}
            log.append(entry)
            raise BitfinexError(f"GET {url} {params}: response was not JSON ({exc})") from exc
        if not isinstance(data, list):
            entry |= {"rows": None, "error": f"unexpected payload type {type(data).__name__}"}
            log.append(entry)
            raise BitfinexError(f"GET {url} {params}: expected a JSON array, got {data!r:.200}")
        # API-level errors come back HTTP 200 as ["error", 10020, "limit: invalid"]
        if data and isinstance(data[0], str):
            entry |= {"rows": None, "error": str(data)}
            log.append(entry)
            raise BitfinexError(f"GET {url} {params}: API error payload {data}")

        entry["rows"] = len(data)
        log.append(entry)
        venue_cache.save("bitfinex", cache_key, data)
        return data
    raise BitfinexError(f"GET {url} {params}: exhausted {MAX_ATTEMPTS} attempts")


def fetch_trades(
    symbol: str,
    start_ms: int,
    end_ms: int,
    *,
    limit: int = 1000,
    log: list[dict[str, Any]] | None = None,
) -> list[list]:
    """Every public trade in [start_ms, end_ms] as [ID, MTS, AMOUNT, PRICE], sorted by MTS.

    `limit` is capped by the venue, so a full page means there may be more; page forward
    on the last MTS and deduplicate on ID rather than trusting a single response.
    """
    log = [] if log is None else log
    rows: list[list] = []
    seen: set[Any] = set()
    cursor = start_ms
    for page_no in range(1, MAX_PAGES + 1):
        page = _get(
            trades_url(symbol),
            {"limit": limit, "sort": 1, "start": cursor, "end": end_ms},
            log,
        )
        fresh = [t for t in page if t[0] not in seen]
        seen.update(t[0] for t in fresh)
        rows.extend(fresh)
        if len(page) < limit:
            break
        last_mts = int(page[-1][1])
        if not fresh or last_mts <= cursor:
            raise BitfinexError(
                f"{trades_url(symbol)}: paging stalled at start={cursor} after {page_no} pages; "
                "the window cannot be shown to be complete"
            )
        cursor = last_mts
    else:
        raise BitfinexError(
            f"{trades_url(symbol)}: more than {MAX_PAGES} pages for {start_ms}-{end_ms}"
        )
    rows.sort(key=lambda t: (t[1], t[0]))
    return rows


def fetch_candles(
    symbol: str,
    start_ms: int,
    end_ms: int,
    *,
    timeframe: str = "1m",
    limit: int = 20,
    log: list[dict[str, Any]] | None = None,
) -> list[list]:
    """Candles over the window as [MTS, OPEN, CLOSE, HIGH, LOW, VOLUME], sorted by MTS."""
    log = [] if log is None else log
    url = candles_url(symbol, timeframe)
    rows = _get(url, {"limit": limit, "sort": 1, "start": start_ms, "end": end_ms}, log)
    if len(rows) >= limit:
        raise BitfinexError(
            f"{url}: returned {len(rows)} rows against limit={limit}; the response may be "
            "truncated, so its volume total cannot be trusted"
        )
    return rows
