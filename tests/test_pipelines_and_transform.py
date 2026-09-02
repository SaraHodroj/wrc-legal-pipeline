"""Persistence pipeline + transform job tests.

Together with test_spider.py this closes the loop: spider decides, pipeline
persists, transform curates -- each stage proven against in-memory doubles
(mongomock for Mongo, moto for S3), so the suite still needs no infrastructure.
"""

from datetime import UTC, date, datetime

import mongomock
import pytest
from itemadapter import ItemAdapter
from moto import mock_aws

from wrc_pipeline.config import MongoSettings, ObjectStoreSettings, get_settings
from wrc_pipeline.hashing import hash_bytes
from wrc_pipeline.models import DecisionRecord, DocumentType
from wrc_pipeline.scraping.pipelines import PersistencePipeline
from wrc_pipeline.storage.metadata_store import MetadataStore
from wrc_pipeline.storage.object_store import ObjectStore
from wrc_pipeline.transform.job import _transform_one


class SpiderStub:
    run_id = "test-run"
    stats_model = None


@pytest.fixture()
def pipeline():
    with mock_aws():
        p = PersistencePipeline.__new__(PersistencePipeline)
        p.settings_obj = get_settings()

        p.metadata = MetadataStore(MongoSettings(uri="mongodb://localhost", database="t"))
        p.metadata._client = mongomock.MongoClient()
        p.metadata.ensure_indexes()

        p.objects = ObjectStore(
            ObjectStoreSettings(endpoint_url=None, access_key="t", secret_key="t")
        )
        p.objects.ensure_bucket(p.settings_obj.object_store.landing_bucket)
        p.objects.ensure_bucket(p.settings_obj.object_store.curated_bucket)
        yield p


def make_record(**overrides) -> DecisionRecord:
    defaults: dict = {
        "identifier": "ADJ-00054658",
        "body": "Workplace Relations Commission",
        "title": "Declan Holden V Ger Brennan Construction",
        "source_url": "https://example.ie/cases/adj-00054658.html",
        "document_url": "https://example.ie/cases/adj-00054658.html",
        "partition_date": date(2025, 7, 1),
        "run_id": "test-run",
        "document_type": DocumentType.HTML,
        "file_hash": hash_bytes(b"<html>decision</html>"),
        "file_size_bytes": 21,
    }
    defaults.update(overrides)
    return DecisionRecord(**defaults)


# ------------------------------------------------------------------- pipeline
def test_store_writes_object_then_metadata(pipeline):
    item = ItemAdapter(
        {
            "__action__": "store",
            "record": make_record(),
            "payload": b"<html>decision</html>",
            "content_type": "text/html",
        }
    )
    result = pipeline._store(item, SpiderStub())

    assert result["action"] == "new"
    stored = pipeline.metadata.landing.find_one({"identifier": "ADJ-00054658"})
    assert stored is not None
    bucket, key = ObjectStore.parse_uri(stored["storage_path"])
    assert pipeline.objects.get_bytes(bucket, key) == b"<html>decision</html>"


def test_store_twice_overwrites_object_not_duplicates(pipeline):
    item = {
        "__action__": "store",
        "record": make_record(),
        "payload": b"<html>decision</html>",
        "content_type": "text/html",
    }
    pipeline._store(ItemAdapter(item), SpiderStub())
    outcome = pipeline._store(ItemAdapter(dict(item)), SpiderStub())["action"]

    assert outcome == "unchanged"
    assert pipeline.metadata.landing.count_documents({}) == 1


def test_object_metadata_carries_provenance(pipeline):
    pipeline._store(
        ItemAdapter(
            {
                "__action__": "store",
                "record": make_record(),
                "payload": b"<html>decision</html>",
                "content_type": "text/html",
            }
        ),
        SpiderStub(),
    )
    stored = pipeline.metadata.landing.find_one({})
    bucket, key = ObjectStore.parse_uri(stored["storage_path"])
    head = pipeline.objects.client.head_object(Bucket=bucket, Key=key)
    assert head["Metadata"]["identifier"] == "ADJ-00054658"
    assert head["Metadata"]["run-id"] == "test-run"


def test_touch_action_appends_observation_and_leaves_landing_alone(pipeline):
    """Round-2 review regression: a 'seen again' touch must not modify the
    landing row at all -- it appends to record_observations instead."""
    record = make_record()
    pipeline.metadata.record_version(record)
    before = pipeline.metadata.landing.find_one({})

    pipeline._touch(
        ItemAdapter(
            {
                "__action__": "touch",
                "identifier": record.identifier,
                "body": record.body,
            }
        ),
        SpiderStub(),
    )

    assert pipeline.metadata.landing.find_one({}) == before  # byte-identical
    observation = pipeline.metadata.observations.find_one({})
    assert observation is not None
    assert observation["identifier"] == record.identifier


# ------------------------------------------------------------------ transform
HTML_PAGE = (
    "<html><body>"
    '<nav class="navigation">Cases | Contact</nav>'
    '<main id="main"><h1>ADJ-00054658</h1>'
    "<p>ADJUDICATION OFFICER Decision under the Industrial Relations Act 1969. "
    "Having considered the submissions of both parties I find the complaint is "
    "well founded and recommend compensation be paid accordingly.</p></main>"
    '<footer class="site-footer">Sitemap</footer>'
    "</body></html>"
)


@pytest.fixture()
def transform_env():
    with mock_aws():
        settings = get_settings()
        objects = ObjectStore(
            ObjectStoreSettings(endpoint_url=None, access_key="t", secret_key="t")
        )
        objects.ensure_bucket(settings.object_store.landing_bucket)
        objects.ensure_bucket(settings.object_store.curated_bucket)
        yield settings, objects


def landing_doc(objects, settings, payload: bytes, doc_type: str) -> dict:
    key = f"wrc/2025-07-01/ADJ-00054658.{doc_type}"
    uri = objects.put_bytes(settings.object_store.landing_bucket, key, payload)
    return {
        "identifier": "ADJ-00054658",
        "body": "Workplace Relations Commission",
        "title": "t",
        "description": "d",
        "published_date": datetime(2025, 7, 17, tzinfo=UTC),
        "partition_date": datetime(2025, 7, 1, tzinfo=UTC),
        "source_url": "https://example.ie/x",
        "document_url": "https://example.ie/x",
        "document_type": doc_type,
        "storage_path": uri,
        "file_hash": hash_bytes(payload),
    }


def test_html_is_cleaned_renamed_and_rehashed(transform_env):
    settings, objects = transform_env
    doc = landing_doc(objects, settings, HTML_PAGE.encode(), "html")

    result = _transform_one(doc, objects, settings, "t-run", {})

    assert result is not None
    # Renamed to identifier.ext in the curated bucket.
    assert result["storage_path"].endswith("ADJ-00054658.html")
    # Re-hashed: cleaning changed the bytes, so the hash must differ.
    assert result["file_hash"] != doc["file_hash"]
    # Chrome is gone, substance survives.
    curated = objects.get_bytes(*ObjectStore.parse_uri(result["storage_path"])).decode()
    assert "Sitemap" not in curated
    assert "Industrial Relations Act 1969" in curated
    # Lineage back to the landing zone is preserved.
    assert result["landing_storage_path"] == doc["storage_path"]


def test_pdf_passes_through_byte_identical(transform_env):
    settings, objects = transform_env
    payload = b"%PDF-1.4 original bytes"
    doc = landing_doc(objects, settings, payload, "pdf")

    result = _transform_one(doc, objects, settings, "t-run", {})

    curated = objects.get_bytes(*ObjectStore.parse_uri(result["storage_path"]))
    assert curated == payload
    assert result["file_hash"] == doc["file_hash"]


def test_transform_rerun_is_idempotent(transform_env):
    settings, objects = transform_env
    doc = landing_doc(objects, settings, HTML_PAGE.encode(), "html")

    first = _transform_one(doc, objects, settings, "run-1", {})
    assert first is not None
    # Second run, with the curated metadata now reflecting the first run:
    # both the object AND the metadata are verified current -> full skip.
    curated_index = {(doc["identifier"], doc["body"]): first["file_hash"]}
    assert _transform_one(doc, objects, settings, "run-2", curated_index) is None


def test_existing_object_does_not_mask_missing_metadata(transform_env):
    """Recovery regression: if a previous run uploaded the curated object but
    died before writing Mongo, the rerun must re-emit the metadata row -- an
    existing object alone is NOT proof the record is done."""
    settings, objects = transform_env
    doc = landing_doc(objects, settings, HTML_PAGE.encode(), "html")

    first = _transform_one(doc, objects, settings, "run-1", {})
    assert first is not None
    # Simulate the crash: object exists, but curated metadata was never
    # written (empty curated_index). The rerun must produce the row again.
    repaired = _transform_one(doc, objects, settings, "run-2", {})
    assert repaired is not None
    assert repaired["file_hash"] == first["file_hash"]


def test_transform_never_touches_the_landing_object(transform_env):
    settings, objects = transform_env
    payload = HTML_PAGE.encode()
    doc = landing_doc(objects, settings, payload, "html")

    _transform_one(doc, objects, settings, "t-run", {})

    bucket, key = ObjectStore.parse_uri(doc["storage_path"])
    assert objects.get_bytes(bucket, key) == payload  # byte-identical


def test_curated_filename_is_literally_identifier_ext_no_subfolder(transform_env):
    """The brief's exact wording: 'Change the name of ALL the files to become
    identifier.ext'. Verify the curated key is flat at the bucket root, not
    nested under a body/date folder the way the landing zone is."""
    settings, objects = transform_env
    doc = landing_doc(objects, settings, HTML_PAGE.encode(), "html")

    result = _transform_one(doc, objects, settings, "t-run", {})

    bucket, key = ObjectStore.parse_uri(result["storage_path"])
    assert key == "ADJ-00054658.html", f"expected flat 'identifier.ext', got {key!r}"
    assert "/" not in key


def test_empty_extraction_raises_rather_than_storing_garbage(transform_env):
    settings, objects = transform_env
    doc = landing_doc(objects, settings, b"<html><body></body></html>", "html")

    with pytest.raises(ValueError, match="empty"):
        _transform_one(doc, objects, settings, "t-run", {})
