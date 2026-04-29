import hashlib
import json
from typing import Any


def canonical_json(payload: Any) -> str:
    if isinstance(payload, str):
        payload = json.loads(payload)

    return json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )


def compute_idempotency_key(payload: Any) -> str:
    canonical_payload = canonical_json(payload)
    return hashlib.sha256(canonical_payload.encode("utf-8")).hexdigest()
