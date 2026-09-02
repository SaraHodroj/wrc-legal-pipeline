"""Idempotency and landing-zone immutability tests.

Two requirements meet here and must BOTH hold:

* "Running it twice on the same date range must not create duplicate records
  or re-download unchanged files."
* "Don't delete/update any of the stored data in the Landing Zone."

The design that satisfies both is an append-only, hash-versioned landing zone:
an unchanged re-run inserts nothing (only the audit trail is touched), while
changed content lands as a NEW version row under a NEW object key -- the
previous version is never mutated.

These tests exercise that against an in-memory Mongo (``mongomock``) so they
run in CI with no infrastructure.
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
        storage_path="s3://landing-zone/wrc/2025-07-01/ADJ-00054658/hash-a.html",
    )


def test_second_identical_run_creates_no_duplicate(store):
    assert store.record_version(make_record()) == "new"
    assert store.record_version(make_record(run_id="run-2")) == "unchanged"
    assert store.landing.count_documents({}) == 1


def test_changed_content_appends_a_new_version_and_mutates_nothing(store):
    """The landing zone is append-only: an amendment is a second row, and the
    original row survives byte-for-byte."""
    store.record_version(make_record(file_hash="hash-a"))
    original = store.landing.find_one({"file_hash": "hash-a"})

    outcome = store.record_version(make_record(file_hash="hash-b", run_id="run-2"))

    assert outcome == "updated"
    assert store.landing.count_documents({}) == 2  # both versions retained

    still_original = store.landing.find_one({"file_hash": "hash-a"})
    assert still_original["storage_path"] == original["storage_path"]
    assert still_original["run_id"] == original["run_id"]
    assert still_original.get("content_changed_at") is None

    amended = store.landing.find_one({"file_hash": "hash-b"})
    assert amended["content_changed_at"] is not None


def test_latest_hash_wins_in_the_idempotency_index(store):
    """After an amendment, the known-hash index must answer with the NEW hash,
    or the next run would re-land the amendment forever."""
    store.record_version(make_record(file_hash="hash-a"))
    store.record_version(make_record(file_hash="hash-b", run_id="run-2"))

    index = store.load_known_hashes(date(2025, 7, 1), date(2025, 7, 31))
    assert index[("ADJ-00054658", "Workplace Relations Commission")] == "hash-b"


def test_known_hash_index_respects_the_date_window(store):
    """A partition outside the requested window must not be preloaded."""
    store.record_version(make_record())
    index = store.load_known_hashes(date(2024, 1, 1), date(2024, 12, 31))
    assert index == {}


def test_identifier_normalisation_prevents_duplicate_rows(store):
    """'IR - SC - 00001595' and 'IR-SC-00001595' are the same decision."""
    spaced = make_record()
    spaced_dict = spaced.model_dump()
    spaced_dict["identifier"] = "IR - SC - 00001595"
    store.record_version(DecisionRecord(**spaced_dict))

    tight = spaced.model_dump()
    tight["identifier"] = "IR-SC-00001595"
    outcome = store.record_version(DecisionRecord(**tight))

    assert outcome == "unchanged"
    assert store.landing.count_documents({}) == 1


def test_touch_updates_audit_without_touching_content(store):
    """Only the audit trail (last_seen_*) may change on a re-run."""
    store.record_version(make_record())
    before = store.landing.find_one({})

    store.touch_last_seen("ADJ-00054658", "Workplace Relations Commission", "run-9")
    after = store.landing.find_one({})

    assert after["file_hash"] == before["file_hash"]
    assert after["storage_path"] == before["storage_path"]
    assert after["run_id"] == before["run_id"]
    assert after["last_seen_run_id"] == "run-9"


def test_iter_latest_landing_returns_one_doc_per_record(store):
    """The transform must see the latest version only -- never both."""
    store.record_version(make_record(file_hash="hash-a"))
    store.record_version(make_record(file_hash="hash-b", run_id="run-2"))

    docs = list(store.iter_latest_landing(date(2025, 7, 1), date(2025, 7, 31)))
    assert len(docs) == 1
    assert docs[0]["file_hash"] == "hash-b"
    assert store.count_latest_landing(date(2025, 7, 1), date(2025, 7, 31)) == 1


def test_storage_key_is_deterministic_per_version():
    """Same content, same key -- an unchanged re-run resolves to the identical
    object; changed content resolves to a DIFFERENT key, never an overwrite."""
    assert make_record().storage_key() == make_record(run_id="other").storage_key()
    assert make_record().storage_key() != make_record(file_hash="hash-b").storage_key()


def test_storage_key_is_partitioned_versioned_and_extensioned():
    key = make_record(file_hash="a" * 64).storage_key()
    assert key.startswith("workplace-relations-commission/2025-07-01/ADJ-00054658/")
    assert key.endswith(f"{'a' * 16}.html")
