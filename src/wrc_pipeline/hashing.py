"""Content hashing -- the backbone of idempotency.

We hash the *bytes we stored*, not the URL and not the HTTP ETag:

* URLs are unstable (session tokens, tracking params) and say nothing about
  whether the content changed.
* ETag/Last-Modified are advisory and this site does not set them reliably.

SHA-256 over the raw body gives us a deterministic answer to the only question
that matters on a re-run: "is this byte-for-byte what I already have?"
"""

from __future__ import annotations

import hashlib
from typing import BinaryIO

_CHUNK = 1024 * 1024  # 1 MiB


def hash_bytes(data: bytes) -> str:
    """SHA-256 of an in-memory payload, as lowercase hex."""
    return hashlib.sha256(data).hexdigest()


def hash_stream(stream: BinaryIO) -> str:
    """SHA-256 of a file-like object, read in chunks.

    Used for the transform stage, where a document may be large enough that we
    do not want the whole thing resident in memory at once.
    """
    digest = hashlib.sha256()
    while chunk := stream.read(_CHUNK):
        digest.update(chunk)
    return digest.hexdigest()
