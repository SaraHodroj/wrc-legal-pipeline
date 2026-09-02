"""Hashing underpins idempotency: if it is wrong, we either re-download the
world every night or silently miss amended decisions."""

import io

from wrc_pipeline.hashing import hash_bytes, hash_stream


def test_hash_is_deterministic():
    assert hash_bytes(b"decision text") == hash_bytes(b"decision text")


def test_hash_detects_a_single_byte_change():
    assert hash_bytes(b"ADJ-00054658 upheld") != hash_bytes(b"ADJ-00054658 upheld.")


def test_stream_and_bytes_hashes_agree():
    payload = b"x" * (3 * 1024 * 1024 + 17)  # spans several read chunks
    assert hash_stream(io.BytesIO(payload)) == hash_bytes(payload)


def test_empty_input_still_hashes():
    assert len(hash_bytes(b"")) == 64
