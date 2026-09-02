# WRC Legal Document Pipeline

A partitioned, idempotent scraping pipeline that ingests decisions and
determinations from the Irish [Workplace Relations
Commission](https://www.workplacerelations.ie/en/cases/) into a landing zone,
then transforms them into a curated zone.

Built with **Scrapy** (crawling), **MongoDB** (metadata), **MinIO/S3** (objects)
and **Dagster** (orchestration).

---

## Quick start

```bash
# 1. Configuration
cp .env.example .env

# 2. Infrastructure (Mongo + MinIO, buckets auto-created)
make up          # or: docker compose up -d mongo minio minio-init

# 3. Install
python -m venv .venv && source .venv/bin/activate
make install     # pip install -e ".[dev]"

# 4. Verify offline
make test        # 108 tests, no infrastructure required

# 5. Verify the live site still matches the adapter (2 polite GETs, no infra)
make check-site  # or: python scripts/check_site_contract.py

# 6. Run the ingestion stage for a date window
make ingest START=2024-01-01 END=2024-03-01

# 7. Run the transformation stage over the same window
make transform START=2024-01-01 END=2024-02-29

# 8. Or drive both through the orchestrator
make dagster     # UI at http://localhost:3000
```

MinIO console: <http://localhost:9001> (`minioadmin` / `minioadmin`).

Requires **Python 3.11+** and a Scrapy from the pinned range (`>=2.11,<3.0`
— the spider implements both the modern `start()` and the legacy
`start_requests()` entry points, so old and new Scrapy releases both work).

### The site contract

The search is a plain server-rendered GET (no XHR):
`/en/search/?decisions=1&body=<id>&from=D/M/YYYY&to=D/M/YYYY` — endpoint,
parameter names, numeric body IDs and the unpadded date format were all
confirmed against the live site and are pinned in one module,
`src/wrc_pipeline/scraping/search_adapter.py`. Every other module is
source-agnostic. `scripts/check_site_contract.py` re-verifies all of those
assumptions (plus robots.txt) against the live site in seconds; run it before
a real crawl and whenever a crawl unexpectedly returns zero rows.

Note on robots.txt: the site disallows `/en/Cases/` (capital C) — the legacy
import directories — while the decisions themselves are served and linked
under lowercase `/en/cases/`, which is permitted. The crawler obeys
robots.txt (`SCRAPE_OBEY_ROBOTS_TXT=true`) and identifies itself with a
contact address.

---

## Design

```
                    ┌──────────────────────┐
                    │  Dagster (monthly    │
                    │  partitioned assets) │
                    └──────────┬───────────┘
                               │ subprocess per partition
                    ┌──────────▼───────────┐
                    │  Scrapy: decisions   │
                    │  search → detail →   │
                    │  document            │
                    └──────────┬───────────┘
                               │
        ┌──────────────────────┴──────────────────────┐
        ▼                                             ▼
┌────────────────────┐                    ┌────────────────────┐
│ MinIO landing-zone │                    │ Mongo landing_     │
│ {body}/{partition}/│◄──storage_path─────│ decisions          │
│ {identifier}.{ext} │                    │ + file_hash        │
└─────────┬──────────┘                    └─────────┬──────────┘
          │              IMMUTABLE — never modified │
          ▼                                         ▼
┌────────────────────┐                    ┌────────────────────┐
│ MinIO curated-zone │                    │ Mongo curated_     │
│ {body}/            │◄───────────────────│ decisions          │
│ {identifier}.{ext} │                    │ + new file_hash    │
└────────────────────┘                    └────────────────────┘
```

### Layout

```
src/wrc_pipeline/
├── config.py              Typed settings — no hardcoded values anywhere
├── logging_setup.py       JSON (JSONL) structured logging
├── models.py              Pydantic domain models + run statistics
├── partitioning.py        Date-window slicing
├── hashing.py             SHA-256 — the basis of idempotency
├── storage/
│   ├── metadata_store.py  Mongo repository, indexes, upserts
│   └── object_store.py    S3-compatible client (MinIO / S3 / GCS / R2)
├── scraping/
│   ├── search_adapter.py  ◄── the ONLY site-specific module
│   ├── spiders/decisions.py
│   ├── pipelines.py       Persistence, off the reactor thread
│   ├── settings.py        Scrapy settings, derived from config.py
│   └── runner.py          CLI entry point
├── transform/
│   ├── html_extract.py    Boilerplate stripping
│   └── job.py             Landing → curated
└── orchestration/
    └── definitions.py     Dagster partitioned assets
```

### Requirements coverage

| # | Requirement | Where |
|---|---|---|
| 1 | Fast crawl without overloading the source | AutoThrottle + per-domain concurrency, `scraping/settings.py` |
| 2 | Crawl each body, partitioned by date | `spiders/decisions.py::start_requests` |
| 3 | `start_date`/`end_date` inputs, iterate by period, `partition_date` field | `partitioning.py`, `models.py` |
| 4 | Extract metadata per record | `search_adapter.py`, `models.DecisionRecord` |
| 5 | Metadata in a NoSQL DB | `storage/metadata_store.py` (MongoDB) |
| 6 | Documents in object storage; PDFs as-is, HTML pages as `.html` | `spiders/decisions.py::parse_detail`, `storage/object_store.py` |
| 7 | `storage_path` on the metadata record | `pipelines.PersistencePipeline._store` |
| 8 | `file_hash` on the metadata record | `hashing.py`, `models.py` |
| 9 | Idempotent re-runs, hash-based change detection | `metadata_store.upsert_landing`, `tests/test_idempotency.py` |
| 10 | Structured JSON logs + end-of-run summary | `logging_setup.py`, `models.RunStats` |
| — | Dockerised storages | `docker-compose.yml` |
| — | Orchestration with dependencies | `orchestration/definitions.py` (Dagster) |
| — | Everything configurable | `config.py`, `.env.example` |
| — | Transformation script | `transform/job.py` |
| — | Architecture write-up | `ARCHITECTURE.md` |

---

## Structured logs

One JSON object per line. Filter a run with `jq`:

```bash
make ingest START=2024-01-01 END=2024-02-01 > run.jsonl

jq 'select(.event == "search_page_parsed")' run.jsonl        # per-partition progress
jq 'select(.event == "request_failed")' run.jsonl            # every failure, with URL + status
jq 'select(.event == "run_summary")' run.jsonl               # found vs scraped, success rate
```

The summary line reports `records_found`, `records_scraped`, `records_new`,
`records_updated`, `records_skipped_unchanged`, `downloads_failed`,
`bytes_downloaded` and `success_rate`. Failures are also persisted to the
`failed_records` collection so a partial batch can be replayed without
re-crawling the search pages.

---

## Idempotency

Running the same window twice writes nothing the second time:

```bash
make ingest START=2024-01-01 END=2024-02-01
make ingest START=2024-01-01 END=2024-02-01   # records_skipped_unchanged == records_found
```

Guaranteed by a unique index on `(identifier, body)`, SHA-256 content
comparison before every write, and deterministic object keys. See
`ARCHITECTURE.md` for the full strategy and its limitations.

---

## Testing

```bash
make test    # 108 tests across every module: partitioning boundaries, hashing,
             # HTML extraction, idempotency, search-adapter parsing, spider
             # callbacks, persistence pipeline, transform job, logging, CLI,
             # and a Dagster definitions smoke test. ~83% line coverage.
make lint    # ruff + mypy
```

The whole suite runs against in-memory doubles (`mongomock` for MongoDB,
`moto` for S3), so it needs no running infrastructure and finishes in seconds.
CI (`.github/workflows/ci.yml`) runs lint, type-check, tests and a Docker
build on Python 3.11 and 3.12 for every push.

---

## Notes on scope

- **PDFs pass through the transform untouched**, per the brief. Text extraction
  and OCR for scanned decisions belong in a separate stage with their own
  failure modes; see `ARCHITECTURE.md`.
- **The landing zone is append-only.** Re-running the transform never re-crawls
  the source.
- **Access should be legitimate.** The crawler identifies itself with a contact
  address and obeys `robots.txt`. For sustained bulk ingestion of a legal
  corpus, the durable path is an agreement with the source rather than
  engineering around access controls — a corpus whose provenance cannot
  withstand a client's due diligence is not an asset.
