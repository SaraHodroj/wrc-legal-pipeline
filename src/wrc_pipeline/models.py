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


class DecisionRecord(BaseModel):
    """One decision/determination as stored in the landing metadata collection."""

    model_config = ConfigDict(use_enum_values=True, extra="forbid")

    # --- Natural key --------------------------------------------------------
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
        """Normalise so 'IR - SC - 00001595' and 'IR-SC-00001595' are one record.

        The listing page and the detail page format identifiers differently on
        this site; without normalisation the same decision would be stored
        twice and idempotency would silently fail. A few live slugs also use
        unicode dashes (en/em dash), which must collapse to the same record
        as their ASCII-hyphen siblings.
        """
        for dash in ("–", "—", "−"):  # en dash, em dash, minus
            value = value.replace(dash, "-")
        collapsed = "-".join(part.strip() for part in value.split("-"))
        return " ".join(collapsed.split()).upper()

    def storage_key(self) -> str:
        """Deterministic object key for the landing zone.

        Partition-prefixed so the bucket stays browsable and lifecycle rules
        can be applied per period. Deterministic so a re-run overwrites in
        place rather than accumulating duplicates under new names.
        """
        ext = str(self.document_type)
        safe_body = self.body.lower().replace(" ", "-")
        return f"{safe_body}/{self.partition_date.isoformat()}/{self.identifier}.{ext}"


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


class RunStats(BaseModel):
    """Per-run counters, emitted as the end-of-run summary log line."""

    run_id: str
    started_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    finished_at: datetime | None = None

    partitions_processed: int = 0
    records_found: int = 0
    records_scraped: int = 0
    records_skipped_unchanged: int = 0
    records_updated: int = 0
    records_new: int = 0
    downloads_failed: int = 0
    bytes_downloaded: int = 0

    failures: list[dict[str, Any]] = Field(default_factory=list)

    @property
    def success_rate(self) -> float:
        if not self.records_found:
            return 1.0
        return round(self.records_scraped / self.records_found, 4)

    def summary(self) -> dict[str, Any]:
        """Flat dict for the final structured log line."""
        return {
            "event": "run_summary",
            "run_id": self.run_id,
            "started_at": self.started_at,
            "finished_at": self.finished_at or datetime.now(UTC),
            "partitions_processed": self.partitions_processed,
            "records_found": self.records_found,
            "records_scraped": self.records_scraped,
            "records_new": self.records_new,
            "records_updated": self.records_updated,
            "records_skipped_unchanged": self.records_skipped_unchanged,
            "downloads_failed": self.downloads_failed,
            "bytes_downloaded": self.bytes_downloaded,
            "success_rate": self.success_rate,
            "failure_sample": self.failures[:25],
        }
