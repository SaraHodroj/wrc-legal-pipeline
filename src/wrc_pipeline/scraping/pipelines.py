"""Scrapy item pipelines.

The spider decides *what* to persist; these pipelines do the persisting.

The critical detail: ``pymongo`` and ``boto3`` are both synchronous. Calling
them directly from ``process_item`` would block Twisted's reactor thread and
serialise the entire crawl behind each write. We therefore hand the blocking
work to ``deferToThread``, which runs it on Twisted's thread pool and returns a
Deferred the framework already knows how to wait on. Concurrency is preserved
and we keep using the mature sync drivers rather than reaching for an async
client we would then have to justify.
"""

from __future__ import annotations

from typing import Any

from itemadapter import ItemAdapter
from scrapy import Spider
from scrapy.exceptions import DropItem
from twisted.internet.threads import deferToThread

from ..config import get_settings
from ..logging_setup import get_logger
from ..models import DecisionRecord
from ..storage.metadata_store import MetadataStore
from ..storage.object_store import ObjectStore

logger = get_logger(__name__)


class StorageInitPipeline:
    """Opens the storage clients once per crawl and ensures buckets/indexes."""

    def __init__(self) -> None:
        settings = get_settings()
        self.settings_obj = settings
        self.metadata = MetadataStore(settings.mongo)
        self.objects = ObjectStore(settings.object_store)

    def open_spider(self, spider: Spider) -> None:  # noqa: ARG002 - Scrapy interface
        self.metadata.ensure_indexes()
        self.objects.ensure_bucket(self.settings_obj.object_store.landing_bucket)
        self.objects.ensure_bucket(self.settings_obj.object_store.curated_bucket)

    def close_spider(self, spider: Spider) -> None:  # noqa: ARG002 - Scrapy interface
        self.metadata.close()

    def process_item(self, item: Any, spider: Spider) -> Any:  # noqa: ARG002 - Scrapy interface
        return item


class PersistencePipeline(StorageInitPipeline):
    """Writes the document to object storage and the metadata to Mongo.

    Order matters: the file is written **before** the metadata row. If the
    process dies between the two, we are left with an orphaned object in the
    bucket -- harmless, and cleaned up by the deterministic key on the next
    run. The reverse order would leave a metadata record pointing at a file
    that does not exist, which is a genuine data-integrity bug for every
    downstream consumer.
    """

    def process_item(self, item: Any, spider: Spider) -> Any:
        adapter = ItemAdapter(item)
        action = adapter.get("__action__")

        if action == "touch":
            return deferToThread(self._touch, adapter, spider)
        if action == "store":
            return deferToThread(self._store, adapter, spider)

        raise DropItem(f"Unknown item action: {action!r}")

    # -- executed on the Twisted thread pool, never on the reactor thread ----
    def _touch(self, adapter: ItemAdapter, spider: Spider) -> dict[str, Any]:
        self.metadata.touch_last_seen(
            identifier=adapter["identifier"],
            body=adapter["body"],
            run_id=getattr(spider, "run_id", "unknown"),
        )
        return {"identifier": adapter["identifier"], "action": "touched"}

    def _store(self, adapter: ItemAdapter, spider: Spider) -> dict[str, Any]:
        record: DecisionRecord = adapter["record"]
        payload: bytes = adapter["payload"]

        key = record.storage_key()
        uri = self.objects.put_bytes(
            bucket=self.settings_obj.object_store.landing_bucket,
            key=key,
            data=payload,
            content_type=adapter.get("content_type"),
            metadata={
                "identifier": record.identifier,
                "body": record.body,
                "partition-date": record.partition_date.isoformat(),
                "file-hash": record.file_hash or "",
                "run-id": record.run_id,
            },
        )
        record.storage_path = uri

        outcome = self.metadata.upsert_landing(record)

        stats = getattr(spider, "stats_model", None)
        if stats is not None:
            stats.records_scraped += 1
            if outcome == "new":
                stats.records_new += 1
            elif outcome == "updated":
                stats.records_updated += 1

        logger.info(
            "Record persisted",
            extra={
                "event": "record_persisted",
                "run_id": record.run_id,
                "identifier": record.identifier,
                "body": record.body,
                "partition": record.partition_date.isoformat(),
                "document_type": record.document_type,
                "storage_path": uri,
                "file_hash": record.file_hash,
                "bytes": record.file_size_bytes,
                "outcome": outcome,
            },
        )
        return {"identifier": record.identifier, "action": outcome}
