"""Idempotency tests.

Requirement 9 is the one that separates a working pipeline from a correct one:
"Running it twice on the same date range must not create duplicate records or
re-download unchanged files."

These tests exercise that against an in-memory Mongo (``mongomock``) so they run
in CI with no infrastructure.
"""

from datetime import date

import mongomock
import pytest

from wrc_pipeline.config import MongoSettings
from wrc_pipeline.models import DecisionRecord, DocumentType
from wrc_pipeline.storage.metadata_store import MetadataStore


@pytest.fixture()
def store():
    settings = MongoSettings(uri="mongodb://localhost:27017", database="test_wrc")
    metadata = MetadataStore(settings)
    metadata._client = mongomock.MongoClient()  # noqa: SLF001 - test seam
    metadata.ensure_indexes()
    return metadata


def make_record(*, file_hash: str = "hash-a", run_id: str = "run-1") -> DecisionRecord:
    return DecisionRecord(
        identifier="ADJ-00054658",
        body="Workplace Relations Commission",
        title="Declan Holden V Ger Brennan Construction",
        description="Declan Holden V Ger Brennan Construction",
        published_date=date(2025, 7, 17),
        source_url="https://example.ie/cases/adj-00054658.html",
        document_url="https://example.ie/cases/adj-00054658.html",
        partition_date=date(2025, 7, 1),
        run_id=run_id,
        document_type=DocumentType.HTML,
        file_hash=file_hash,
        file_size_bytes=1234,
        storage_path="s3://landing-zone/wrc/2025-07-01/ADJ-00054658.html",
    )


def test_second_identical_run_creates_no_duplicate(store):
    assert store.upsert_landing(make_record()) == "new"
    assert store.upsert_landing(make_record(run_id="run-2")) == "unchanged"
    assert store.landing.count_documents({}) == 1


def test_changed_content_updates_in_place(store):
    store.upsert_landing(make_record(file_hash="hash-a"))
    outcome = store.upsert_landing(make_record(file_hash="hash-b", run_id="run-2"))

    assert outcome == "updated"
    assert store.landing.count_documents({}) == 1

    stored = store.landing.find_one({"identifier": "ADJ-00054658"})
    assert stored["file_hash"] == "hash-b"
    assert stored["content_changed_at"] is not None


def test_first_seen_at_survives_later_runs(store):
    """Audit trail must record when we *first* saw a document, not the last run."""
    store.upsert_landing(make_record())
    original = store.landing.find_one({})["first_seen_at"]

    store.upsert_landing(make_record(file_hash="hash-b", run_id="run-2"))
    assert store.landing.find_one({})["first_seen_at"] == original


def test_known_hash_index_is_keyed_by_identifier_and_body(store):
    store.upsert_landing(make_record())
    index = store.load_known_hashes(date(2025, 7, 1), date(2025, 7, 31))

    assert index[("ADJ-00054658", "Workplace Relations Commission")] == "hash-a"


def test_known_hash_index_respects_the_date_window(store):
    """A partition outside the requested window must not be preloaded."""
    store.upsert_landing(make_record())
    index = store.load_known_hashes(date(2024, 1, 1), date(2024, 12, 31))
    assert index == {}


def test_identifier_normalisation_prevents_duplicate_rows(store):
    """'IR - SC - 00001595' and 'IR-SC-00001595' are the same decision."""
    spaced = make_record()
    spaced_dict = spaced.model_dump()
    spaced_dict["identifier"] = "IR - SC - 00001595"
    store.upsert_landing(DecisionRecord(**spaced_dict))

    tight = spaced.model_dump()
    tight["identifier"] = "IR-SC-00001595"
    outcome = store.upsert_landing(DecisionRecord(**tight))

    assert outcome == "unchanged"
    assert store.landing.count_documents({}) == 1


def test_touch_updates_audit_without_touching_content(store):
    """Landing data is immutable; only the audit trail may change on a re-run."""
    store.upsert_landing(make_record())
    before = store.landing.find_one({})

    store.touch_last_seen("ADJ-00054658", "Workplace Relations Commission", "run-9")
    after = store.landing.find_one({})

    assert after["file_hash"] == before["file_hash"]
    assert after["storage_path"] == before["storage_path"]
    assert after["last_seen_run_id"] == "run-9"


def test_storage_key_is_deterministic():
    """Same record, same key -- a re-run overwrites rather than accumulating."""
    assert make_record().storage_key() == make_record(run_id="other").storage_key()


def test_storage_key_is_partitioned_and_extensioned():
    key = make_record().storage_key()
    assert key.startswith("workplace-relations-commission/2025-07-01/")
    assert key.endswith("ADJ-00054658.html")
