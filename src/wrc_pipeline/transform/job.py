"""Transformation stage: landing zone -> curated zone.

Contract, per the brief:

* PDF/DOC files pass through untouched.
* HTML files are stripped to their substantive content, re-hashed, and stored.
* Every file is renamed to ``{identifier}.{ext}``.
* Output goes to a **new** bucket and a **new** collection.
* The landing zone is never modified or deleted.

That last point is the reason this is a separate stage at all. The landing zone
is our immutable record of what the source actually served; if we improve the
HTML extraction next month -- and we will -- we re-run this stage over the same
landing data rather than re-crawling the site. Re-crawling costs the source
bandwidth we have no right to spend twice, and worse, older pages may no longer
be available in the form we first saw them.
"""

from __future__ import annotations

import argparse
import sys
import uuid
from datetime import UTC, date, datetime
from typing import Any

from ..config import get_settings
from ..hashing import hash_bytes
from ..logging_setup import configure_logging, get_logger
from ..models import DocumentType
from ..storage.metadata_store import MetadataStore
from ..storage.object_store import ObjectStore
from .html_extract import extract_main_content, extract_plain_text

logger = get_logger(__name__)

PASSTHROUGH_TYPES = {DocumentType.PDF.value, DocumentType.DOC.value, DocumentType.DOCX.value}


def transform_window(start: date, end: date, run_id: str | None = None) -> dict[str, Any]:
    """Transform every landing record whose partition falls in ``[start, end]``."""
    settings = get_settings()
    run_id = run_id or f"transform-{datetime.now(UTC):%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:8]}"

    metadata = MetadataStore(settings.mongo)
    objects = ObjectStore(settings.object_store)
    objects.ensure_bucket(settings.object_store.curated_bucket)
    metadata.ensure_indexes()

    batch_size = settings.transform.batch_size
    records_found = metadata.count_latest_landing(start, end)
    logger.info(
        "Transform starting",
        extra={
            "event": "transform_started",
            "run_id": run_id,
            "start_date": start,
            "end_date": end,
            "records_found": records_found,
            "batch_size": batch_size,
        },
    )

    stats = {
        "records_found": records_found,
        "transformed": 0,
        "passthrough": 0,
        "skipped_unchanged": 0,
        "failed": 0,
    }
    failures: list[dict[str, Any]] = []
    written = 0
    batch: list[dict[str, Any]] = []
    curated_index = metadata.load_curated_hashes(start, end)


    for doc in metadata.iter_latest_landing(start, end, batch_size=batch_size):
        identifier = doc.get("identifier", "<unknown>")
        try:
            result = _transform_one(doc, objects, settings, run_id, curated_index)
            if result is None:
                stats["skipped_unchanged"] += 1
                continue
            batch.append(result)
            if result["document_type"] in PASSTHROUGH_TYPES:
                stats["passthrough"] += 1
            else:
                stats["transformed"] += 1
        except Exception as exc:  # one bad document must not kill the batch
            stats["failed"] += 1
            entry = {
                "identifier": identifier,
                "storage_path": doc.get("storage_path"),
                "reason": f"{type(exc).__name__}: {exc}",
            }
            failures.append(entry)
            logger.error(
                "Transform failed for record",
                extra={"event": "transform_record_failed", "run_id": run_id, **entry},
            )
        if len(batch) >= batch_size:
            written += metadata.bulk_upsert_curated(batch)
            batch = []

    if batch:
        written += metadata.bulk_upsert_curated(batch)
    metadata.close()

    summary = {
        "event": "transform_summary",
        "run_id": run_id,
        "curated_written": written,
        "failure_sample": failures[:25],
        **stats,
    }
    logger.info("Transform complete", extra=summary)
    return summary


def _transform_one(
    doc: dict[str, Any],
    objects: ObjectStore,
    settings: Any,
    run_id: str,
    curated_index: dict[tuple[str, str], str | None],
) -> dict[str, Any] | None:
    """Transform a single landing record. Returns ``None`` if already current."""
    storage_path = doc.get("storage_path")
    if not storage_path:
        raise ValueError("Landing record has no storage_path")

    bucket, key = ObjectStore.parse_uri(storage_path)
    payload = objects.get_bytes(bucket, key)

    doc_type = doc.get("document_type", DocumentType.UNKNOWN.value)
    identifier = doc["identifier"]

    if doc_type in PASSTHROUGH_TYPES:
        new_payload = payload
        new_hash = doc.get("file_hash") or hash_bytes(payload)
        text_length = None
    else:
        cleaned = extract_main_content(payload.decode("utf-8", errors="replace"))
        if not cleaned:
            raise ValueError("HTML content extraction produced an empty document")
        new_payload = cleaned.encode("utf-8")
        new_hash = hash_bytes(new_payload)
        text_length = len(extract_plain_text(cleaned))

    extension = doc_type if doc_type != DocumentType.UNKNOWN.value else "bin"
    curated_bucket = settings.object_store.curated_bucket
    object_current = False
    if objects.exists(curated_bucket, new_key):
        existing = objects.get_bytes(curated_bucket, new_key)
        object_current = hash_bytes(existing) == new_hash

    metadata_current = curated_index.get((identifier, doc["body"])) == new_hash
    if object_current and metadata_current:
        return None  # both halves verified current -- nothing to do

    if object_current:
        new_uri = f"s3://{curated_bucket}/{new_key}"
    else:
        new_uri = objects.put_bytes(
            bucket=curated_bucket,
            key=new_key,
            data=new_payload,
            content_type=_content_type_for(extension),
            metadata={"identifier": identifier, "file-hash": new_hash, "run-id": run_id},
        )

    return {
        "identifier": identifier,
        "source": doc.get("source", "wrc"),
        "body": doc["body"],
        "title": doc.get("title"),
        "description": doc.get("description"),
        "published_date": doc.get("published_date"),
        "partition_date": doc.get("partition_date"),
        "source_url": doc.get("source_url"),
        "document_url": doc.get("document_url"),
        "document_type": doc_type,
        "storage_path": new_uri,
        "file_hash": new_hash,
        "file_size_bytes": len(new_payload),
        "text_length": text_length,
        "landing_storage_path": storage_path,
        "landing_file_hash": doc.get("file_hash"),
        "transform_run_id": run_id,
        "transformed_at": datetime.now(UTC),
    }


def _content_type_for(extension: str) -> str:
    return {
        "pdf": "application/pdf",
        "doc": "application/msword",
        "docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        "html": "text/html; charset=utf-8",
    }.get(extension, "application/octet-stream")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Transform landing-zone data into the curated zone."
    )
    parser.add_argument("--start-date", type=date.fromisoformat, required=True)
    parser.add_argument("--end-date", type=date.fromisoformat, required=True)
    parser.add_argument("--run-id")
    args = parser.parse_args(argv)

    settings = get_settings()
    configure_logging(settings.run.log_level, settings.run.log_format)
    summary = transform_window(args.start_date, args.end_date, args.run_id)
    return 0 if summary["failed"] == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
