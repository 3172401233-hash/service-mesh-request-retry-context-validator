"""Entry-generated idempotency keys and a digest-bound dedup store.

The store models the state that a gateway / side-effect owner would keep.  A
record is bound to BOTH the idempotency key and the canonical body digest:

* same key + same digest -> safe replay of the stored result;
* same key + different digest -> conflict, cached result must not be reused;
* unknown key -> new operation.
"""
from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, Optional


NEW = "new"
REPLAY = "replay"
IN_FLIGHT = "in_flight"
CONFLICT = "conflict"


def generate_idempotency_key(prefix: str = "idem-") -> str:
    """Generate the one key for a logical request.

    Only the entry point (gateway) is allowed to call this; downstream hops
    must propagate the resulting value instead of minting their own.
    """
    return prefix + uuid.uuid4().hex


@dataclass
class IdempotencyRecord:
    idempotency_key: str
    body_digest: str
    state: str = "in_flight"  # in_flight|completed
    response_status: Optional[int] = None
    response_body: Any = None
    attempt_id: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "idempotency_key": self.idempotency_key,
            "body_digest": self.body_digest,
            "state": self.state,
            "response_status": self.response_status,
            "attempt_id": self.attempt_id,
        }


@dataclass
class LookupResult:
    outcome: str
    record: Optional[IdempotencyRecord] = None

    @property
    def replay_allowed(self) -> bool:
        return self.outcome == REPLAY

    @property
    def conflict(self) -> bool:
        return self.outcome == CONFLICT


class IdempotencyStore:
    """In-memory digest-bound idempotency records (test double, no I/O)."""

    def __init__(self) -> None:
        self._records: Dict[str, IdempotencyRecord] = {}

    def lookup(self, idempotency_key: str, body_digest: str) -> LookupResult:
        record = self._records.get(idempotency_key)
        if record is None:
            return LookupResult(NEW)
        if record.body_digest != body_digest:
            return LookupResult(CONFLICT, record)
        if record.state == "completed":
            return LookupResult(REPLAY, record)
        return LookupResult(IN_FLIGHT, record)

    def begin(
        self,
        idempotency_key: str,
        body_digest: str,
        attempt_id: Optional[str] = None,
    ) -> LookupResult:
        """Register an in-flight operation if new; return the lookup result."""
        result = self.lookup(idempotency_key, body_digest)
        if result.outcome == NEW:
            self._records[idempotency_key] = IdempotencyRecord(
                idempotency_key=idempotency_key,
                body_digest=body_digest,
                state="in_flight",
                attempt_id=attempt_id,
            )
        return result

    def complete(
        self,
        idempotency_key: str,
        body_digest: str,
        response_status: int,
        response_body: Any = None,
    ) -> IdempotencyRecord:
        result = self.lookup(idempotency_key, body_digest)
        if result.conflict:
            raise ValueError(
                "cannot complete key %r: body digest conflicts with stored record"
                % idempotency_key
            )
        record = self._records[idempotency_key]
        record.state = "completed"
        record.response_status = response_status
        record.response_body = response_body
        record.body_digest = body_digest
        return record

    def get(self, idempotency_key: str) -> Optional[IdempotencyRecord]:
        return self._records.get(idempotency_key)

    def snapshot(self) -> Dict[str, Dict[str, Any]]:
        return {key: record.to_dict() for key, record in self._records.items()}
