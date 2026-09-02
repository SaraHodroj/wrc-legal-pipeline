"""Central configuration.

Every tunable value in the pipeline is resolved here, from environment
variables (optionally loaded from a .env file). Nothing downstream is allowed
to hardcode a connection string, bucket name, partition size or throttle
setting -- modules import ``get_settings()`` instead.

Design note
-----------
We use ``pydantic-settings`` rather than raw ``os.environ`` reads so that
configuration is *validated once at startup* rather than failing deep inside a
crawl. A typo in ``PARTITION_SIZE`` raises immediately with a readable error,
instead of silently producing zero partitions three hours into a backfill.
"""

from __future__ import annotations

from datetime import date
from functools import lru_cache
from typing import Annotated, Literal

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict

PartitionSize = Literal["daily", "weekly", "monthly", "quarterly", "yearly"]

_ENV_FILE = (".env", ".env.local")


class MongoSettings(BaseSettings):
    """Metadata store (NoSQL) connection + collection naming."""

    model_config = SettingsConfigDict(env_prefix="MONGO_", env_file=_ENV_FILE, extra="ignore")

    uri: str = "mongodb://root:example@localhost:27017"
    database: str = "wrc"
    # Landing zone is append-only; curated is written by the transform stage.
    landing_collection: str = "landing_decisions"
    curated_collection: str = "curated_decisions"
    runs_collection: str = "pipeline_runs"
    server_selection_timeout_ms: int = 5_000


class ObjectStoreSettings(BaseSettings):
    """S3-compatible object storage (MinIO locally, S3/GCS in production)."""

    model_config = SettingsConfigDict(env_prefix="S3_", env_file=_ENV_FILE, extra="ignore")

    endpoint_url: str | None = "http://localhost:9000"
    access_key: str = "minioadmin"
    secret_key: str = "minioadmin"
    region: str = "us-east-1"
    landing_bucket: str = "landing-zone"
    curated_bucket: str = "curated-zone"
    # Multipart threshold matters once we hit large PDF bundles.
    multipart_threshold_bytes: int = 8 * 1024 * 1024


class ScrapeSettings(BaseSettings):
    """Crawl behaviour: politeness, retries, and what to crawl."""

    model_config = SettingsConfigDict(env_prefix="SCRAPE_", env_file=_ENV_FILE, extra="ignore")

    #: Which source adapter to crawl with -- must be a key in
    #: ``scraping.search_adapter.SOURCE_ADAPTERS``. Adding a legal source means
    #: registering one adapter class there and selecting it here.
    source: str = "wrc"
    base_url: str = "https://www.workplacerelations.ie"

    # --- Politeness / throughput -------------------------------------------
    # AutoThrottle adapts the delay to the latency the *server* is showing us,
    # which is both faster and kinder than a fixed sleep: it speeds up when the
    # site is healthy and backs off when it starts to struggle.
    concurrent_requests: int = 8
    concurrent_requests_per_domain: int = 4
    download_delay_seconds: float = 0.25
    autothrottle_enabled: bool = True
    autothrottle_target_concurrency: float = 4.0
    autothrottle_max_delay_seconds: float = 30.0
    obey_robots_txt: bool = True
    user_agent: str = (
        "wrc-legal-pipeline/1.0 (+contact: data-engineering@example.com) "
        "Scrapy research crawler"
    )

    # --- Reliability --------------------------------------------------------
    retry_times: int = 4  # retries AFTER the initial attempt (Scrapy semantics)
    retry_http_codes: tuple[int, ...] = (429, 500, 502, 503, 504, 408, 522, 524)
    download_timeout_seconds: int = 60
    download_maxsize_bytes: int = 100 * 1024 * 1024
    download_warnsize_bytes: int = 20 * 1024 * 1024
    #: Development convenience: cache HTTP responses on disk so selector
    #: iteration doesn't re-hit the source. Off by default for real runs.
    httpcache_enabled: bool = False

    # --- Idempotency behaviour ----------------------------------------------
    #: False (default): a record whose (identifier, body) is already landed is
    #: skipped at LISTING time -- its detail page and document are never
    #: re-downloaded, satisfying "running twice must not re-download unchanged
    #: files". True: known records are re-fetched and hash-compared, which
    #: detects silently amended decisions at the cost of re-downloading.
    #: A scheduled amendment sweep runs with this on; ad-hoc re-runs leave it off.
    recheck_known: bool = False

    # --- Scope --------------------------------------------------------------
    bodies: Annotated[tuple[str, ...], NoDecode] = (
        "Workplace Relations Commission",
        "Labour Court",
        "Equality Tribunal",
        "Employment Appeals Tribunal",
    )
    partition_size: PartitionSize = "monthly"

    # Safety valve for smoke tests: cap records per partition (0 = unlimited).
    max_records_per_partition: int = 0
    #: Circuit breaker against a pager that links "next" forever. Hitting it
    #: marks the partition PARTIAL in the reconciliation ledger.
    max_pages_per_partition: int = 500

    @field_validator("bodies", mode="before")
    @classmethod
    def _split_bodies(cls, value: object) -> object:
        """Allow ``SCRAPE_BODIES="Labour Court|Equality Tribunal"`` in env."""
        if isinstance(value, str):
            return tuple(part.strip() for part in value.split("|") if part.strip())
        return value

    @field_validator("source")
    @classmethod
    def _source_is_registered(cls, value: str) -> str:
        """Fail at startup if no adapter is registered for the source."""
        from .scraping.search_adapter import SOURCE_ADAPTERS

        if value not in SOURCE_ADAPTERS:
            raise ValueError(
                f"Unknown source {value!r}; registered sources: {sorted(SOURCE_ADAPTERS)}"
            )
        return value

    @field_validator("bodies")
    @classmethod
    def _bodies_are_known(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        """Fail at startup, not mid-crawl, if a body name has no known ID.

        Imported lazily to avoid a config -> scraping -> config cycle.
        """
        from .scraping.search_adapter import BODY_IDS

        unknown = [b for b in value if b not in BODY_IDS]
        if unknown:
            raise ValueError(
                f"Unknown body name(s) {unknown}; known bodies: {sorted(BODY_IDS)}"
            )
        return value


class TransformSettings(BaseSettings):
    """Transformation-stage behaviour."""

    model_config = SettingsConfigDict(env_prefix="TRANSFORM_", env_file=_ENV_FILE, extra="ignore")

    #: Documents are streamed from Mongo and written to the curated collection
    #: in batches of this size, so memory stays bounded at any corpus size.
    batch_size: int = 200


class RunSettings(BaseSettings):
    """Per-invocation inputs (date window, logging)."""

    model_config = SettingsConfigDict(env_prefix="RUN_", env_file=_ENV_FILE, extra="ignore")

    start_date: date = date(2024, 1, 1)
    end_date: date = date(2025, 1, 1)
    log_level: str = "INFO"
    log_format: Literal["json", "console"] = "json"


class Settings(BaseSettings):
    """Root settings object -- the single import surface for the whole app."""

    model_config = SettingsConfigDict(env_file=_ENV_FILE, extra="ignore")

    mongo: MongoSettings = Field(default_factory=MongoSettings)
    object_store: ObjectStoreSettings = Field(default_factory=ObjectStoreSettings)
    scrape: ScrapeSettings = Field(default_factory=ScrapeSettings)
    transform: TransformSettings = Field(default_factory=TransformSettings)
    run: RunSettings = Field(default_factory=RunSettings)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Cached accessor. Cached so config is parsed once per process."""
    return Settings()
