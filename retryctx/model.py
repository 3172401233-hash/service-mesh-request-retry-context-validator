"""Typed inputs for the retry context validator.

A scenario describes one logical request moving through an ordered call chain
(typically ``client -> gateway -> service-a -> service-b``).  Every hop/edge
records its attempts; every attempt carries its own ``attempt_id`` and response
status and may declare committed side effects.
"""
from __future__ import annotations

import base64
import gzip
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from .digest import decode_body, normalize_digest, sha256_digest


# HTTP statuses that commonly make a caller consider a retry.  Side effects
# recorded on the attempt still override this optimistic classification.
RETRYABLE_STATUSES = frozenset({408, 425, 429, 500, 502, 503, 504})

SUCCESS_STATUS_RANGE = range(200, 300)


def is_success_status(status: Optional[int]) -> bool:
    return isinstance(status, int) and status in SUCCESS_STATUS_RANGE


def is_retryable_status(status: Optional[int]) -> bool:
    """Whether the status code by itself permits a retry.

    ``None`` means no HTTP response was received (timeout / reset / connect
    failure); the outcome is therefore unknown and also qualifies at the
    status level.
    """
    if status is None:
        return True
    return status in RETRYABLE_STATUSES


def header_lookup(headers: Dict[str, str], name: str) -> Optional[str]:
    wanted = name.lower()
    for key, value in (headers or {}).items():
        if str(key).lower() == wanted:
            return value
    return None


@dataclass
class HttpRequest:
    method: str = "POST"
    path: str = "/"
    body: bytes = b""
    content_encoding: Optional[str] = None
    headers: Dict[str, str] = field(default_factory=dict)

    def header(self, name: str) -> Optional[str]:
        return header_lookup(self.headers, name)

    def decoded_body(self) -> bytes:
        return decode_body(self.body, self.content_encoding)

    def wire_digest(self) -> str:
        return sha256_digest(self.body)

    def body_digest(self) -> str:
        """Canonical body digest over the decompressed logical body."""
        return sha256_digest(self.decoded_body())

    @classmethod
    def from_text(
        cls,
        text: str,
        compress: Optional[str] = None,
        method: str = "POST",
        path: str = "/",
        headers: Optional[Dict[str, str]] = None,
    ) -> "HttpRequest":
        """Convenience constructor used by tests/examples.

        ``compress='gzip'`` stores gzip-compressed wire bytes while
        ``body_digest()`` still hashes the plain *text*.
        """
        raw = text.encode("utf-8")
        encoding = None
        wire = raw
        if compress:
            algorithm = compress.lower()
            if algorithm == "gzip":
                wire = gzip.compress(raw)
                encoding = "gzip"
            elif algorithm in ("identity", "none"):
                wire = raw
            else:
                raise ValueError("unsupported compression: %r" % compress)
        merged_headers = dict(headers or {})
        if encoding and "Content-Encoding" not in merged_headers:
            merged_headers["Content-Encoding"] = encoding
        return cls(
            method=method,
            path=path,
            body=wire,
            content_encoding=encoding,
            headers=merged_headers,
        )

    @classmethod
    def from_dict(cls, data: Any) -> "HttpRequest":
        if isinstance(data, HttpRequest):
            return data
        if data is None:
            return cls()
        if isinstance(data, (bytes, bytearray)):
            return cls(body=bytes(data))
        if isinstance(data, str):
            return cls(body=data.encode("utf-8"))
        if not isinstance(data, dict):
            raise ValueError("original_request must be an object")

        headers = dict(data.get("headers") or {})
        encoding = data.get("content_encoding")
        if encoding is None:
            encoding = header_lookup(headers, "content-encoding")

        if "body_text" in data and data.get("body_text") is not None:
            logical = str(data["body_text"]).encode("utf-8")
            compress = data.get("compress") or encoding
            if compress and str(compress).lower() == "gzip":
                body = gzip.compress(logical)
                encoding = "gzip"
                if "Content-Encoding" not in headers:
                    headers["Content-Encoding"] = "gzip"
            else:
                body = logical
                if compress and str(compress).lower() not in ("identity", "none"):
                    raise ValueError("unsupported compress value: %r" % compress)
        else:
            raw_body = data.get("body", b"")
            if isinstance(raw_body, bytes):
                body = raw_body
            elif data.get("body_base64"):
                body = base64.b64decode(str(raw_body))
            else:
                body = str(raw_body).encode("utf-8")

        return cls(
            method=str(data.get("method", "POST")),
            path=str(data.get("path", "/")),
            body=body,
            content_encoding=encoding,
            headers=headers,
        )


@dataclass
class SideEffect:
    name: str = "effect"
    committed: bool = True
    idempotency_protected: bool = False
    dedup_key: Optional[str] = None
    branch_id: Optional[str] = None
    detail: Optional[str] = None

    @classmethod
    def from_any(cls, value: Any) -> "SideEffect":
        if isinstance(value, SideEffect):
            return value
        if isinstance(value, str):
            return cls(name=value)
        if not isinstance(value, dict):
            raise ValueError("side effect must be a string or object")
        return cls(
            name=str(value.get("name", "effect")),
            committed=bool(value.get("committed", True)),
            idempotency_protected=bool(
                value.get("idempotency_protected", value.get("protected", False))
            ),
            dedup_key=value.get("dedup_key") or value.get("idempotency_key"),
            branch_id=value.get("branch_id"),
            detail=value.get("detail"),
        )


@dataclass
class Attempt:
    attempt_id: str = ""
    response_status: Optional[int] = None
    retry_count: int = 0
    idempotency_key: Optional[str] = None
    body_digest: Optional[str] = None
    digest_basis: str = "canonical"  # canonical|wire
    side_effects: List[SideEffect] = field(default_factory=list)
    error: Optional[str] = None
    generated_key_here: bool = False

    @classmethod
    def from_dict(cls, data: Any, index: int = 0) -> "Attempt":
        if isinstance(data, Attempt):
            return data
        if not isinstance(data, dict):
            raise ValueError("attempt must be an object")
        attempt_id = (
            data.get("attempt_id") or data.get("id") or ("attempt-%d" % (index + 1))
        )
        status = data.get("response_status", data.get("status"))
        return cls(
            attempt_id=str(attempt_id),
            response_status=None if status is None else int(status),
            retry_count=int(data.get("retry_count", index)),
            idempotency_key=data.get("idempotency_key"),
            body_digest=normalize_digest(
                data.get("body_digest", data.get("request_body_digest"))
            ),
            digest_basis=str(data.get("digest_basis", "canonical")).lower(),
            side_effects=[
                SideEffect.from_any(item) for item in data.get("side_effects", [])
            ],
            error=data.get("error"),
            generated_key_here=bool(data.get("generated_key_here", False)),
        )

    def outcome_unknown(self) -> bool:
        """True when the caller cannot know whether the attempt completed."""
        if self.error:
            return True
        return (
            self.response_status is None
            or self.response_status in RETRYABLE_STATUSES
        )


@dataclass
class Position:
    """One edge in the call chain, e.g. ``gateway -> order-service``."""

    name: str = ""
    caller: str = ""
    callee: str = ""
    max_retries: Optional[int] = None
    idempotency_key: Optional[str] = None
    generated_key_here: bool = False
    attempts: List[Attempt] = field(default_factory=list)

    @classmethod
    def from_any(
        cls, data: Any, index: int = 0, default_max_retries: int = 2
    ) -> "Position":
        if isinstance(data, Position):
            return data
        if isinstance(data, str):
            return cls(
                name=data,
                caller="",
                callee=data,
                max_retries=default_max_retries,
            )
        if not isinstance(data, dict):
            raise ValueError("call_chain entries must be strings or objects")

        caller = str(data.get("caller", data.get("from", "")) or "")
        callee = str(
            data.get(
                "callee",
                data.get("to", data.get("hop", "hop-%d" % index)),
            )
            or ""
        )
        name = str(data.get("name") or data.get("position") or "")
        if not name:
            name = ("%s->%s" % (caller, callee)) if caller else callee

        raw_attempts = data.get("attempts", [])
        if (
            not raw_attempts
            and data.get("attempt_id", data.get("id")) is not None
        ):
            # Allow a flat hop record: {name, attempt_id, response_status}.
            raw_attempts = [
                {
                    "attempt_id": data.get("attempt_id", data.get("id")),
                    "response_status": data.get(
                        "response_status", data.get("status")
                    ),
                    "retry_count": data.get("retry_count", 0),
                    "idempotency_key": data.get("idempotency_key"),
                    "body_digest": data.get(
                        "body_digest", data.get("request_body_digest")
                    ),
                    "digest_basis": data.get("digest_basis", "canonical"),
                    "side_effects": data.get("side_effects", []),
                    "error": data.get("error"),
                    "generated_key_here": data.get("generated_key_here", False),
                }
            ]
        attempts = [Attempt.from_dict(item, i) for i, item in enumerate(raw_attempts)]

        return cls(
            name=name,
            caller=caller,
            callee=callee,
            max_retries=(
                None
                if data.get("max_retries") is None
                else int(data["max_retries"])
            ),
            idempotency_key=data.get("idempotency_key"),
            generated_key_here=bool(data.get("generated_key_here", False)),
            attempts=attempts,
        )

    def retry_budget(self, default: int) -> int:
        return default if self.max_retries is None else self.max_retries

    def retries_used(self) -> int:
        if not self.attempts:
            return 0
        from_records = max(a.retry_count for a in self.attempts)
        return max(from_records, len(self.attempts) - 1)


@dataclass
class Branch:
    branch_id: str = ""
    state: str = "running"  # running|completed|cancelled
    cancel_received: bool = False
    reachable: bool = True
    side_effects: List[SideEffect] = field(default_factory=list)

    @property
    def completed(self) -> bool:
        return self.state in ("completed", "committed", "succeeded", "done")

    @classmethod
    def from_dict(cls, data: Any, index: int = 0) -> "Branch":
        if isinstance(data, Branch):
            return data
        if isinstance(data, str):
            return cls(branch_id=data)
        if not isinstance(data, dict):
            raise ValueError("branch must be a string or object")
        branch_id = str(
            data.get("branch_id") or data.get("id") or ("branch-%d" % (index + 1))
        )
        if data.get("completed") is True and "state" not in data:
            state = "completed"
        else:
            state = str(data.get("state", "running"))
        return cls(
            branch_id=branch_id,
            state=state,
            cancel_received=bool(data.get("cancel_received", False)),
            reachable=bool(data.get("reachable", data.get("cancel_delivered", True))),
            side_effects=[
                SideEffect.from_any(item) for item in data.get("side_effects", [])
            ],
        )


@dataclass
class Scenario:
    original_request: Optional[HttpRequest] = None
    body_digest: Optional[str] = None
    body_digest_basis: str = "canonical"
    idempotency_key: Optional[str] = None
    retry_count: int = 2
    call_chain: List[Position] = field(default_factory=list)
    branches: List[Branch] = field(default_factory=list)
    cancellation: Dict[str, Any] = field(default_factory=dict)
    attempt_id: Optional[str] = None
    response_status: Optional[int] = None


def scenario_from_dict(data: Any) -> Scenario:
    if isinstance(data, Scenario):
        return data
    if not isinstance(data, dict):
        raise ValueError("scenario must be a dict")

    raw_request = data.get("original_request")
    request = HttpRequest.from_dict(raw_request) if raw_request is not None else None

    key = data.get("idempotency_key")
    if key is None and request is not None:
        key = request.header("Idempotency-Key")

    retry_count = data.get("retry_count", data.get("max_retries", 2))

    chain_data = (
        data.get("call_chain")
        or data.get("chain")
        or data.get("hops")
        or []
    )
    positions = [
        Position.from_any(item, i, int(retry_count))
        for i, item in enumerate(chain_data)
    ]

    branches = [
        Branch.from_dict(item, i)
        for i, item in enumerate(data.get("branches", []) or [])
    ]

    cancellation = dict(data.get("cancellation") or {})

    return Scenario(
        original_request=request,
        body_digest=normalize_digest(
            data.get("body_digest", data.get("request_body_digest"))
        ),
        body_digest_basis=str(data.get("body_digest_basis", "canonical")).lower(),
        idempotency_key=key,
        retry_count=int(retry_count),
        call_chain=positions,
        branches=branches,
        cancellation=cancellation,
        attempt_id=data.get("attempt_id"),
        response_status=data.get("response_status"),
    )
