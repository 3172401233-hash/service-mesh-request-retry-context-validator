"""Local retry-context validator for gateway + two-service call chains.

This package performs offline verification only.  It never opens a network
connection and does not talk to a real proxy / service mesh.
"""
from __future__ import annotations

from .cancellation import propagate_cancellation
from .digest import (
    canonical_body_digest,
    canonical_request_digest,
    decode_body,
    sha256_digest,
    wire_body_digest,
)
from .idempotency import (
    CONFLICT,
    IN_FLIGHT,
    NEW,
    REPLAY,
    IdempotencyStore,
    generate_idempotency_key,
)
from .model import (
    Attempt,
    Branch,
    HttpRequest,
    Position,
    SideEffect,
    scenario_from_dict,
)
from .validator import (
    DECISION_BLOCKED,
    DECISION_GUARDED,
    DECISION_NOT_REQUIRED,
    DECISION_RETRYABLE,
    RetryContextValidator,
    evaluate_retry,
    validate_scenario,
)

__version__ = "1.0.0"

# Readable alias for callers that think in terms of a retry context.
RetryContext = RetryContextValidator

__all__ = [
    "RetryContextValidator",
    "RetryContext",
    "evaluate_retry",
    "validate_scenario",
    "scenario_from_dict",
    "HttpRequest",
    "Attempt",
    "Position",
    "SideEffect",
    "Branch",
    "propagate_cancellation",
    "sha256_digest",
    "wire_body_digest",
    "canonical_body_digest",
    "canonical_request_digest",
    "decode_body",
    "generate_idempotency_key",
    "IdempotencyStore",
    "NEW",
    "REPLAY",
    "IN_FLIGHT",
    "CONFLICT",
    "DECISION_RETRYABLE",
    "DECISION_GUARDED",
    "DECISION_BLOCKED",
    "DECISION_NOT_REQUIRED",
]
