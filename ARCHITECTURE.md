# Architecture

## Date partition size: monthly

The source pages ten results at a time, so a busy month is ~20–30 pages per
body — shallow enough to walk reliably, where a yearly partition would mean
hundreds of pages of flaky deep pagination and daily would mean ~1,460
mostly-empty requests/year/body. A partition is also the retry unit (one
failure re-runs a month, not a decade), and decisions get amended after
publication, so the current month needs cheap, frequent re-runs. It's config,
not code (`SCRAPE_PARTITION_SIZE`).

## Retries and rate limiting

Three layers: **per-request retries** (Scrapy, up to 4 retries after the
initial attempt on 429/5xx/timeout; retries are deprioritised — Scrapy's
middleware does not read Retry-After, so backoff comes from AutoThrottle);
**adaptive throttling** (AutoThrottle widens every request's delay as soon as
the server slows or errors, narrows it on recovery); **per-partition retries**
(Dagster, 2 attempts, 60s delay). The crawler identifies itself (User-Agent +
contact), obeys robots.txt and caps per-domain concurrency — a legal corpus
whose provenance can't survive due diligence is worthless.

## Deduplication and immutability

The landing zone is **append-only and hash-versioned**: one Mongo row and one
object per content version, keyed `(source, identifier, body, file_hash)`
under a unique index (identifiers normalised first). Object keys embed the
hash, so nothing is ever overwritten; an amended decision lands as a *new*
version beside the old one — free amendment history. Even "seen again" audit
sightings go to a separate append-only `record_observations` collection, so
landing rows are frozen at insert: "don't update/delete Landing Zone data" is
satisfied literally.

Idempotency is **network-level**: known records are skipped at *listing* time,
before their pages are requested — a re-run downloads nothing. The
`weekly_amendment_sweep` schedule re-fetches known records with
`--recheck-known` and hash-compares them (ETag/If-Modified-Since would be
preferable; this source doesn't serve them dependably). The known-hash index
loads once per run — per-record Mongo calls would block Twisted's reactor.
Every run ends with a per-`(body, partition)` **reconciliation ledger**
(source-reported vs discovered vs succeeded/skipped/failed, plus any listing
gap) that derives a complete/partial/failed status and the process exit code —
missing records can never report green.

## Scaling to 50+ sources

Site-specific logic lives behind one `SearchAdapter` protocol with a source
registry (`SOURCE_ADAPTERS`), and `source` is part of the landing/curated
natural keys — honestly, groundwork rather than a finished platform: curated
object names are flat `identifier.ext` per the brief, and a second source
would need a pass over the remaining `(identifier, body)` lookups. Beyond
that: config becomes a declarative per-source registry the orchestrator
generates assets from; per-source concurrency limits and failure budgets; the
idempotency index moves to Redis and latest-version selection to server-side
aggregation (~100 bytes/record in memory is fine to tens of millions, not
beyond); golden extraction fixtures per source so a redesign fails a test
("records per partition dropped >N%" is the highest-value alert); PDF
text/OCR becomes its own stage.
