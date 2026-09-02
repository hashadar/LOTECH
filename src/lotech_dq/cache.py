"""Read-through JSON cache for venue API responses used in G and H reconciliation."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

FIXTURES_ROOT = Path(__file__).resolve().parents[2] / "fixtures" / "venue"


def _key(*parts: Any) -> str:
    raw = json.dumps(parts, sort_keys=True, default=str)
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


def path(namespace: str, cache_key: str) -> Path:
    return FIXTURES_ROOT / namespace / f"{cache_key}.json"


def load(namespace: str, cache_key: str) -> Any | None:
    p = path(namespace, cache_key)
    if not p.is_file():
        return None
    return json.loads(p.read_text(encoding="utf-8"))


def save(namespace: str, cache_key: str, payload: Any) -> None:
    p = path(namespace, cache_key)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(payload), encoding="utf-8")


def key_for_request(url: str, params: dict[str, Any]) -> str:
    return _key(url, params)
