"""Domain models.

These are the contract between the spider, the storage layer and the transform
stage. Validating at the boundary means a malformed row is rejected *at the
point it was parsed*, with the offending record logged, rather than silently
written to Mongo and discovered weeks later by a downstream consumer.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator


class DocumentType(StrEnum):
    PDF = "pdf"
    DOC = "doc"
    DOCX = "docx"
    HTML = "html"
    UNKNOWN = "unknown"

    @classmethod
    def from_url_or_content_type(cls, url: str, content_type: str | None) -> DocumentType:
        """Resolve the type from the Content-Type header, falling back to the URL.

        Header first, extension second: this site serves some documents from
        handler URLs with no extension at all, and a wrong extension on disk is
        worse than a slow lookup.
        """
        ct = (content_type or "").lower()
        if "pdf" in ct:
            return cls.PDF
        if "wordprocessingml" in ct or "officedocument" in ct:
            return cls.DOCX
        if "msword" in ct:
            return cls.DOC
        if "html" in ct:
            return cls.HTML

        lowered = url.lower().split("?")[0]
        for candidate in (cls.PDF, cls.DOCX, cls.DOC, cls.HTML):
            if lowered.endswith(f".{candidate.value}"):
                return candidate
        return cls.UNKNOWN


def normalise_identifier(value: str) -> str:
    """Normalise so 'IR - SC - 00001595' and 'IR-SC-00001595' are one record.

    The listing page and the detail page format identifiers differently on
    this site; without normalisation the same decision would be stored twice
    and idempotency would silently fail. A few live slugs also use unicode
    dashes (en/em dash), which must collapse to the same record as their
    ASCII-hyphen siblings. Exposed as a module function because the spider
    needs the same normalisation *before* a record is built, to match listing
    rows against already-ingested identifiers.
    """
    for dash in ("–", "—", "−"):  # en dash, em dash, minus
        value = value.replace(dash, "-")
    collapsed = "-".join(part.strip() for part in value.split("-"))
    return " ".join(collapsed.split()).upper()


class DecisionRecord(BaseModel):
    """One landed *version* of a decision/determination.

    The landing zone is append-only: a record whose content changes upstream
    is stored as a NEW version row (and a new object under a hash-suffixed
    key), never by mutating the previous one. The version identity is
    ``(source, identifier, body, file_hash)``.
    """

    model_config = ConfigDict(use_enum_values=True, extra="forbid")

    # --- Natural key --------------------------------------------------------
    source: str = Field("wrc", description="Which legal source this came from")
    identifier: str = Field(..., description="e.g. ADJ-00054658 -- unique per decision")
    body: str = Field(..., description="Issuing body, e.g. 'Labour Court'")

    # --- Descriptive metadata ----------------------------------------------
    title: str | None = None
    description: str | None = None
    published_date: date | None = None
    source_url: str = Field(..., description="Detail page the record was found on")
    document_url: str | None = Field(None, description="Direct link to the document")

    # --- Lineage ------------------------------------------------------------
    partition_date: date = Field(..., description="Start date of the partition that produced this")
    run_id: str

    # --- Stored artefact ----------------------------------------------------
    document_type: DocumentType = DocumentType.UNKNOWN
    storage_path: str | None = Field(None, description="s3://bucket/key of the stored file")
    file_hash: str | None = Field(None, description="SHA-256 of the stored bytes")
    file_size_bytes: int | None = None

    # --- Audit --------------------------------------------------------------
    first_seen_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    last_seen_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    content_changed_at: datetime | None = None

    @field_validator("identifier")
    @classmethod
    def _normalise_identifier(cls, value: str) -> str:
        return normalise_identifier(value)

    def storage_key(self) -> str:
        """Deterministic, *versioned* object key for the landing zone.

        The content hash is part of the key, so changed content lands under a
        new key and an unchanged re-run resolves to the same key -- the landing
        bucket is therefore append-only (no overwrites) while re-runs stay
        idempotent. Partition-prefixed so the bucket stays browsable and
        lifecycle rules can be applied per period.
        """
        ext = str(self.document_type)
        safe_body = self.body.lower().replace(" ", "-")
        version = (self.file_hash or "unhashed")[:16]
        return (
            f"{safe_body}/{self.partition_date.isoformat()}/"
            f"{self.identifier}/{version}.{ext}"
        )


class FailedRecord(BaseModel):
    """A record we found in the listing but could not fully ingest.

    Requirement: 'every single record from X is logged with the reason'. We
    persist these rather than only logging them, so a failed batch can be
    replayed later without re-crawling the search pages.
    """

    model_config = ConfigDict(extra="forbid")

    identifier: str | None = None
    url: str
    body: str
    partition_date: date
    run_id: str
    reason: str
    http_status: int | None = None
    failed_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class PartitionStats(BaseModel):
    """Reconciliation ledger for one ``(body, partition)`` unit of work.

    The brief demands that a partition holding N records yields N, or N-X with
    every one of the X attributable. That is only checkable if we track, per
    partition: what the source *says* it has, what we discovered in listings,
    and how each discovered record ended (stored, skipped-as-known, or failed).
    ``status`` is derived, never asserted.
    """

    body: str
    partition: str
    source_reported: int | None = None
    rows_discovered: int = 0
    records_succeeded: int = 0
    records_skipped_known: int = 0
    records_failed: int = 0
    pages_fetched: int = 0
    page_cap_hit: bool = False
    search_failed: bool = False

    @property
    def unaccounted(self) -> int:
        return self.rows_discovered - (
            self.records_succeeded + self.records_skipped_known + self.records_failed
        )

    @property
    def status(self) -> str:
        """complete | partial | failed -- derived from the ledger."""
        if self.search_failed or (
            self.rows_discovered == 0 and (self.source_reported or 0) > 0
        ):
            return "failed"
        if self.records_failed > 0 or self.unaccounted > 0 or self.page_cap_hit:
            return "partial"
        return "complete"

    def as_dict(self) -> dict[str, Any]:
        return {
            "body": self.body,
            "partition": self.partition,
            "source_reported": self.source_reported,
            "rows_discovered": self.rows_discovered,
            "records_succeeded": self.records_succeeded,
            "records_skipped_known": self.records_skipped_known,
            "records_failed": self.records_failed,
            "unaccounted": self.unaccounted,
            "pages_fetched": self.pages_fetched,
            "page_cap_hit": self.page_cap_hit,
            "status": self.status,
        }


class RunStats(BaseModel):
    """Per-run counters, emitted as the end-of-run summary log line."""

    run_id: str
    started_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    finished_at: datetime | None = None

    partitions_planned: int = 0
    records_found: int = 0
    records_scraped: int = 0
    records_skipped_unchanged: int = 0
    records_skipped_known: int = 0
    records_updated: int = 0
    records_new: int = 0
    downloads_failed: int = 0
    storage_failures: int = 0
    bytes_downloaded: int = 0

    failures: list[dict[str, Any]] = Field(default_factory=list)
    partitions: dict[str, PartitionStats] = Field(default_factory=dict)

    def partition(self, body: str, partition_key: str) -> PartitionStats:
        """The reconciliation ledger for one (body, partition), created lazily."""
        key = f"{body}|{partition_key}"
        if key not in self.partitions:
            self.partitions[key] = PartitionStats(body=body, partition=partition_key)
        return self.partitions[key]

    @property
    def run_status(self) -> str:
        """complete | partial | failed -- worst partition wins."""
        statuses = {p.status for p in self.partitions.values()}
        if not statuses:
            return "failed"  # nothing was even attempted
        if "failed" in statuses:
            return "failed"
        if "partial" in statuses:
            return "partial"
        return "complete"

    @property
    def success_rate(self) -> float:
        """Honest even at zero: an empty run with failures is 0.0, not 1.0."""
        if not self.records_found:
            return 0.0 if (self.downloads_failed or self.storage_failures) else 1.0
        return round(
            (self.records_scraped + self.records_skipped_known) / self.records_found, 4
        )

    def summary(self) -> dict[str, Any]:
        """Flat dict for the final structured log line."""
        return {
            "event": "run_summary",
            "run_id": self.run_id,
            "run_status": self.run_status,
            "started_at": self.started_at,
            "finished_at": self.finished_at or datetime.now(UTC),
            "partitions_planned": self.partitions_planned,
            "partitions_complete": sum(
                1 for p in self.partitions.values() if p.status == "complete"
            ),
            "records_found": self.records_found,
            "records_scraped": self.records_scraped,
            "records_new": self.records_new,
            "records_updated": self.records_updated,
            "records_skipped_unchanged": self.records_skipped_unchanged,
            "records_skipped_known": self.records_skipped_known,
            "downloads_failed": self.downloads_failed,
            "storage_failures": self.storage_failures,
            "bytes_downloaded": self.bytes_downloaded,
            "success_rate": self.success_rate,
            "partition_reconciliation": [
                p.as_dict() for p in self.partitions.values()
            ],
            "failure_sample": self.failures[:25],
        }
