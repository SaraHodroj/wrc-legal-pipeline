"""MongoDB metadata repository.

Why Mongo for this workload
---------------------------
The records are semi-structured and the shape varies by issuing body -- the
Labour Court exposes fields the Equality Tribunal does not, and the site's
schema has visibly drifted over three decades of decisions. A document store
lets us land whatever the source gives us without a migration every time an
upstream field appears, which is exactly what a landing zone should do.

The trade-off, which I'd call out honestly: we give up joins and cross-record
constraints. That's acceptable here because the access pattern is
point-lookup-by-identifier and range-scan-by-partition, both of which are index
-backed single-collection queries.
"""

from __future__ import annotations

from collections.abc import Iterable
from datetime import UTC, date, datetime
from typing import Any

from pymongo import ASCENDING, MongoClient, UpdateOne
from pymongo.collection import Collection
from pymongo.database import Database
from pymongo.errors import BulkWriteError

from ..config import MongoSettings
from ..logging_setup import get_logger
from ..models import DecisionRecord, FailedRecord

logger = get_logger(__name__)


class MetadataStore:
    """Thin repository over the landing and curated collections."""

    def __init__(self, settings: MongoSettings) -> None:
        self._settings = settings
        self._client: MongoClient | None = None

    # ------------------------------------------------------------------ setup
    @property
    def client(self) -> MongoClient:
        if self._client is None:
            self._client = MongoClient(
                self._settings.uri,
                serverSelectionTimeoutMS=self._settings.server_selection_timeout_ms,
                tz_aware=True,
            )
        return self._client

    @property
    def db(self) -> Database:
        return self.client[self._settings.database]

    @property
    def landing(self) -> Collection:
        return self.db[self._settings.landing_collection]

    @property
    def curated(self) -> Collection:
        return self.db[self._settings.curated_collection]

    @property
    def failures(self) -> Collection:
        return self.db["failed_records"]

    def ensure_indexes(self) -> None:
        """Create the indexes the pipeline depends on. Safe to re-run.

        The unique compound index on ``(identifier, body)`` is what makes
        idempotency a *database guarantee* rather than an application
        convention: even if two workers race on the same partition, the second
        write becomes an update, not a duplicate row.
        """
        self.landing.create_index(
            [("identifier", ASCENDING), ("body", ASCENDING)],
            unique=True,
            name="uq_identifier_body",
        )
        self.landing.create_index([("partition_date", ASCENDING)], name="ix_partition_date")
        self.landing.create_index([("file_hash", ASCENDING)], name="ix_file_hash")
        self.landing.create_index([("run_id", ASCENDING)], name="ix_run_id")

        self.curated.create_index(
            [("identifier", ASCENDING), ("body", ASCENDING)],
            unique=True,
            name="uq_identifier_body",
        )
        self.curated.create_index([("partition_date", ASCENDING)], name="ix_partition_date")

        self.failures.create_index([("run_id", ASCENDING)], name="ix_run_id")
        logger.info("Mongo indexes ensured", extra={"event": "indexes_ensured"})

    def ping(self) -> None:
        """Round-trip to the server; raises if it is unreachable.

        Used as a preflight check so a missing/down MongoDB fails before the
        crawl starts, with a readable error, instead of mid-run.
        """
        self.client.admin.command("ping")

    def close(self) -> None:
        if self._client is not None:
            self._client.close()
            self._client = None

    # ------------------------------------------------------- idempotency index
    def load_known_hashes(
        self, start: date, end: date, bodies: Iterable[str] | None = None
    ) -> dict[tuple[str, str], str | None]:
        """Preload ``{(identifier, body): file_hash}`` for a date window.

        This is the single most important performance decision in the crawl.

        The naive approach is a Mongo lookup per record to ask "have I seen
        this before?". At 1,000 records that is 1,000 round-trips interleaved
        with the crawl -- and, worse, each one is a *blocking* call inside
        Scrapy's single-threaded Twisted reactor, which would stall every
        in-flight download.

        Instead we pay for one indexed range query up front and keep the answer
        in a dict. Lookups during the crawl are then O(1) and non-blocking.

        The trade-off is memory: roughly 100 bytes per record, so ~100 MB at
        one million records. That is fine at the assessment's scale and for a
        good way beyond it; see ARCHITECTURE.md for what changes at 1000x.
        """
        query: dict[str, Any] = {"partition_date": {"$gte": _as_dt(start), "$lte": _as_dt(end)}}
        if bodies:
            query["body"] = {"$in": list(bodies)}

        projection = {"identifier": 1, "body": 1, "file_hash": 1, "_id": 0}
        index = {
            (doc["identifier"], doc["body"]): doc.get("file_hash")
            for doc in self.landing.find(query, projection)
        }
        logger.info(
            "Loaded idempotency index",
            extra={"event": "idempotency_index_loaded", "known_records": len(index)},
        )
        return index

    # ------------------------------------------------------------------ writes
    def upsert_landing(self, record: DecisionRecord) -> str:
        """Insert or update one landing record. Returns 'new' | 'updated' | 'unchanged'.

        Note the split between ``$set`` and ``$setOnInsert``: ``first_seen_at``
        must survive re-runs, so it is only written on insert. This is what
        lets us answer "when did this document first appear in our system?"
        even after fifty subsequent runs have touched the row.
        """
        payload = to_bson_safe(record.model_dump(mode="python"))
        first_seen = payload.pop("first_seen_at")

        existing = self.landing.find_one(
            {"identifier": record.identifier, "body": record.body},
            {"file_hash": 1, "_id": 0},
        )

        if existing is None:
            outcome = "new"
        elif existing.get("file_hash") == record.file_hash:
            outcome = "unchanged"
        else:
            outcome = "updated"
            payload["content_changed_at"] = datetime.now(UTC)

        self.landing.update_one(
            {"identifier": record.identifier, "body": record.body},
            {"$set": payload, "$setOnInsert": {"first_seen_at": first_seen}},
            upsert=True,
        )
        return outcome

    def touch_last_seen(self, identifier: str, body: str, run_id: str) -> None:
        """Mark an unchanged record as seen in this run without rewriting it.

        Landing-zone data is immutable, so we never touch the content fields --
        only the audit trail that proves the record still exists upstream.
        """
        self.landing.update_one(
            {"identifier": identifier, "body": body},
            {"$set": {"last_seen_at": datetime.now(UTC), "last_seen_run_id": run_id}},
        )

    def bulk_upsert_curated(self, records: list[dict[str, Any]]) -> int:
        """Write the transform stage output in one round-trip."""
        if not records:
            return 0

        operations = [
            UpdateOne(
                {"identifier": rec["identifier"], "body": rec["body"]},
                {"$set": to_bson_safe(rec)},
                upsert=True,
            )
            for rec in records
        ]
        try:
            result = self.curated.bulk_write(operations, ordered=False)
            return result.upserted_count + result.modified_count
        except BulkWriteError as exc:
            # ordered=False means the good writes still landed; surface the rest.
            logger.error(
                "Partial failure writing curated records",
                extra={"event": "curated_bulk_write_error", "details": exc.details},
            )
            raise

    def record_failure(self, failure: FailedRecord) -> None:
        self.failures.insert_one(to_bson_safe(failure.model_dump(mode="python")))

    # ------------------------------------------------------------------ reads
    def fetch_landing_between(self, start: date, end: date) -> list[dict[str, Any]]:
        """Records whose partition falls in ``[start, end]`` -- transform input."""
        cursor = self.landing.find(
            {"partition_date": {"$gte": _as_dt(start), "$lte": _as_dt(end)}}
        ).sort("identifier", ASCENDING)
        return list(cursor)


def _as_dt(value: date) -> datetime:
    """Mongo stores BSON datetimes; a bare ``date`` will not compare correctly."""
    if isinstance(value, datetime):
        return value
    return datetime(value.year, value.month, value.day, tzinfo=UTC)


def to_bson_safe(value: Any) -> Any:
    """Recursively coerce ``date`` values to ``datetime`` for BSON encoding.

    BSON has no date-only type -- only a 64-bit UTC datetime. ``pymongo`` will
    happily encode a ``datetime`` and hard-fail on a ``date``, which is easy to
    miss because our Pydantic models legitimately use ``date`` for
    ``published_date`` and ``partition_date``.

    Doing the conversion in one place, at the storage boundary, keeps the
    domain models honest (a publication date genuinely has no time component)
    without leaking BSON's limitations into them. The alternative -- typing
    those fields as ``datetime`` everywhere -- would mean carrying a meaningless
    00:00:00 through the whole codebase and inviting timezone bugs at every
    comparison.
    """
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return _as_dt(value)
    if isinstance(value, dict):
        return {key: to_bson_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [to_bson_safe(item) for item in value]
    return value
