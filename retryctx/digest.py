"""Body digest helpers.

The first hop (gateway) may decompress the request body before forwarding it.
Hashing the raw wire bytes before and after decompression therefore yields
*different* digests even though the logical request is unchanged.

Idempotency records must be bound to the canonical digest, i.e. the digest of
the fully decoded logical body (optionally together with method/path), never to
the transient wire digest of compressed bytes.
"""
from __future__ import annotations

import gzip
import hashlib
import zlib
from typing import Optional


DIGEST_PREFIX = "sha256:"


class UnsupportedContentEncoding(ValueError):
    """Raised when the wire body uses an encoding without a stdlib decoder."""


def sha256_digest(data):
    # type: (bytes) -> str
    """Return ``sha256:<hex>`` for *data*."""
    if not isinstance(data, (bytes, bytearray)):
        raise TypeError("digest input must be bytes, got %s" % type(data).__name__)
    return DIGEST_PREFIX + hashlib.sha256(bytes(data)).hexdigest()


def normalize_digest(value):
    # type: (Optional[str]) -> Optional[str]
    """Accept bare hex digests and normalize them to the ``sha256:`` form."""
    if value is None:
        return None
    text = str(value).strip().lower()
    if not text:
        return None
    if text.startswith(DIGEST_PREFIX):
        return text
    if len(text) == 64:
        try:
            int(text, 16)
        except ValueError:
            return text
        return DIGEST_PREFIX + text
    return text


def decode_body(body, content_encoding=None):
    # type: (bytes, Optional[str]) -> bytes
    """Decode the HTTP content encoding (``gzip``/``deflate``/``identity``)."""
    encoding = (content_encoding or "identity").strip().lower()
    if encoding in ("", "identity", "none"):
        return body
    if encoding == "gzip":
        return gzip.decompress(body)
    if encoding == "deflate":
        try:
            return zlib.decompress(body)
        except zlib.error:
            # Some peers send raw deflate (RFC 1951) without zlib framing.
            return zlib.decompress(body, -zlib.MAX_WBITS)
    raise UnsupportedContentEncoding("unsupported content-encoding: %r" % content_encoding)


def canonical_body_digest(body, content_encoding=None):
    # type: (bytes, Optional[str]) -> str
    """Digest of the decoded logical body -- the value idempotency binds to."""
    return sha256_digest(decode_body(body, content_encoding))


def wire_body_digest(body):
    # type: (bytes) -> str
    """Digest of the raw bytes received on the wire (may be compressed)."""
    return sha256_digest(body)


def canonical_request_digest(method, path, body, content_encoding=None):
    # type: (str, str, bytes, Optional[str]) -> str
    """Digest that also binds the HTTP method and path to the body."""
    logical_body = decode_body(body, content_encoding)
    hasher = hashlib.sha256()
    hasher.update((method.upper() + "\n" + path + "\n").encode("utf-8"))
    hasher.update(logical_body)
    return DIGEST_PREFIX + hasher.hexdigest()
