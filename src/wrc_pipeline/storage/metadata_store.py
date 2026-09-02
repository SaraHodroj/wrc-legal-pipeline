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

from collections.abc import Iterable, Iterator
from datetime import UTC, date, datetime
from typing import Any

from pymongo import ASCENDING, MongoClient, UpdateOne
from pymongo.collection import Collection
from pymongo.database import Database
from pymongo.errors import BulkWriteError, DuplicateKeyError

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
    def observations(self) -> Collection:
        return self.db[self._settings.observations_collection]

    @property
    def failures(self) -> Collection:
        return self.db[self._settings.failures_collection]

    def ensure_indexes(self) -> None:
        """Create the indexes the pipeline depends on. Safe to re-run.

        The landing zone is append-only and *versioned*: one row per distinct
        content version of a record. The unique compound index on
        ``(source, identifier, body, file_hash)`` is what makes that a
        database guarantee -- an unchanged re-run cannot insert a duplicate
        version even if two workers race, while changed content inserts a new
        version row rather than mutating the old one.
        """
        self.landing.create_index(
            [
                ("source", ASCENDING),
                ("identifier", ASCENDING),
                ("body", ASCENDING),
                ("file_hash", ASCENDING),
            ],
            unique=True,
            name="uq_source_identifier_body_hash",
        )
        self.landing.create_index(
            [("identifier", ASCENDING), ("body", ASCENDING)], name="ix_identifier_body"
        )
        self.landing.create_index([("partition_date", ASCENDING)], name="ix_partition_date")
        self.landing.create_index([("run_id", ASCENDING)], name="ix_run_id")

        self.curated.create_index(
            [("source", ASCENDING), ("identifier", ASCENDING), ("body", ASCENDING)],
            unique=True,
            name="uq_source_identifier_body",
        )
        self.curated.create_index([("partition_date", ASCENDING)], name="ix_partition_date")

        self.observations.create_index(
            [("source", ASCENDING), ("identifier", ASCENDING), ("body", ASCENDING)],
            name="ix_source_identifier_body",
        )
        self.observations.create_index([("run_id", ASCENDING)], name="ix_obs_run_id")

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
        self,
        start: date,
        end: date,
        bodies: Iterable[str] | None = None,
        source: str | None = None,
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

        The trade-off is memory: one dict entry per record -- a few hundred
        bytes each with Python overhead, so on the order of hundreds of MB at
        a million records. Fine at the assessment's scale and beyond; see
        ARCHITECTURE.md for what changes at 1000x (Redis / server-side
        aggregation).
        """
        query: dict[str, Any] = {"partition_date": {"$gte": _as_dt(start), "$lte": _as_dt(end)}}
        if bodies:
            query["body"] = {"$in": list(bodies)}
        if source:
            query["source"] = source

        projection = {"identifier": 1, "body": 1, "file_hash": 1, "first_seen_at": 1}
        # Versions are append-only, so a record may have several rows; the
        # idempotency answer is the LATEST version's hash per (identifier, body).
        index: dict[tuple[str, str], str | None] = {}
        order: dict[tuple[str, str], tuple[Any, str]] = {}
        for doc in self.landing.find(query, projection):
            key = (doc["identifier"], doc["body"])
            rank = _version_order(doc)
            if key not in index or rank > order[key]:
                index[key] = doc.get("file_hash")
                order[key] = rank
        logger.info(
            "Loaded idempotency index",
            extra={"event": "idempotency_index_loaded", "known_records": len(index)},
        )
        return index

    # ------------------------------------------------------------------ writes
    def record_version(self, record: DecisionRecord) -> str:
        """Land one content version. Returns 'new' | 'updated' | 'unchanged'.

        The landing zone is strictly INSERT-only -- existing rows are never
        mutated, not even audit fields.
        - No prior version of (identifier, body): INSERT -> 'new'.
        - A prior version with this exact hash: record a sighting in the
          separate ``record_observations`` collection -> 'unchanged'.
        - Prior versions exist but this hash is new (an amended decision):
          INSERT a fresh version row -> 'updated'. The old row, its object and
          its hash all remain untouched -- free amendment history and an
          auditable past.
        """
        existing_same_hash = self.landing.find_one(
            {
                "source": record.source,
                "identifier": record.identifier,
                "body": record.body,
                "file_hash": record.file_hash,
            },
            {"_id": 1},
        )
        if existing_same_hash is not None:
            self.record_sighting(record.source, record.identifier, record.body, record.run_id)
            return "unchanged"

        any_prior = self.landing.find_one(
            {"source": record.source, "identifier": record.identifier, "body": record.body},
            {"_id": 1},
        )
        payload = to_bson_safe(record.model_dump(mode="python"))
        if any_prior is not None:
            payload["content_changed_at"] = datetime.now(UTC)

        try:
            self.landing.insert_one(payload)
        except DuplicateKeyError:
            # A racing worker landed the identical version between our check
            # and the insert; the unique index kept the zone duplicate-free.
            return "unchanged"
        return "updated" if any_prior is not None else "new"

    def record_sighting(self, source: str, identifier: str, body: str, run_id: str) -> None:
        """Append one 'seen again, unchanged' observation.

        Kept in a SEPARATE collection so that landing rows are never updated
        at all -- not even audit fields. "When was this record last confirmed
        upstream?" is answered by the newest observation, and the landing zone
        stays byte-for-byte what each run originally inserted.
        """
        self.observations.insert_one(
            {
                "source": source,
                "identifier": identifier,
                "body": body,
                "run_id": run_id,
                "observed_at": datetime.now(UTC),
            }
        )

    def bulk_upsert_curated(self, records: list[dict[str, Any]]) -> int:
        """Write the transform stage output in one round-trip."""
        if not records:
            return 0

        operations = [
            UpdateOne(
                {
                    "source": rec.get("source", "wrc"),
                    "identifier": rec["identifier"],
                    "body": rec["body"],
                },
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
    def iter_latest_landing(
        self, start: date, end: date, batch_size: int = 200
    ) -> Iterator[dict[str, Any]]:
        """Stream the LATEST version of each record in ``[start, end]``.

        Document PAYLOADS are bounded: full documents stream in ``batch_size``
        chunks by _id, so the heavy bytes never accumulate. The preceding
        version-resolution pass, however, is honestly O(N) in record COUNT --
        it holds one dict entry (a few hundred bytes with Python overhead) per
        record in the window. Fine for this assessment's scale and well
        beyond; at tens of millions of records per window it should move to a
        server-side aggregation ($sort + $group), per ARCHITECTURE.md.
        """
        window = {"partition_date": {"$gte": _as_dt(start), "$lte": _as_dt(end)}}
        latest: dict[tuple[str, str], tuple[Any, tuple[Any, str]]] = {}  # key -> (_id, rank)
        projection = {"identifier": 1, "body": 1, "first_seen_at": 1}
        for doc in self.landing.find(window, projection):
            key = (doc["identifier"], doc["body"])
            rank = _version_order(doc)
            current = latest.get(key)
            if current is None or rank > current[1]:
                latest[key] = (doc["_id"], rank)

        ids = sorted((v[0] for v in latest.values()), key=str)
        for offset in range(0, len(ids), batch_size):
            chunk = ids[offset : offset + batch_size]
            yield from self.landing.find({"_id": {"$in": chunk}}).sort(
                "identifier", ASCENDING
            )

    def load_curated_hashes(
        self, start: date, end: date
    ) -> dict[tuple[str, str], str | None]:
        """``{(identifier, body): file_hash}`` for curated records in the window.

        Lets the transform verify metadata currency separately from object
        currency -- an existing curated object must never mask an absent or
        stale curated metadata row.
        """
        window = {"partition_date": {"$gte": _as_dt(start), "$lte": _as_dt(end)}}
        projection = {"identifier": 1, "body": 1, "file_hash": 1, "_id": 0}
        return {
            (doc["identifier"], doc["body"]): doc.get("file_hash")
            for doc in self.curated.find(window, projection)
        }

    def count_latest_landing(self, start: date, end: date) -> int:
        """How many distinct records (not versions) fall in the window."""
        window = {"partition_date": {"$gte": _as_dt(start), "$lte": _as_dt(end)}}
        keys = {
            (doc["identifier"], doc["body"])
            for doc in self.landing.find(window, {"identifier": 1, "body": 1})
        }
        return len(keys)


_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


def _version_order(doc: dict[str, Any]) -> tuple[str, str]:
    """Sort rank for version rows: newest ``first_seen_at`` wins, with the
    monotonic ObjectId as tiebreaker (BSON datetimes have only millisecond
    precision, so two versions landed in the same millisecond would otherwise
    tie and resolve arbitrarily). Timestamps compare as ISO strings so naive
    and aware values from different drivers can never raise on comparison.
    """
    stamp = doc.get("first_seen_at") or _EPOCH
    return (stamp.isoformat(), str(doc.get("_id", "")))


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
