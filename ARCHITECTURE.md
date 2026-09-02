# Architecture

## Date partition size: monthly

Monthly balances three pressures. **Result-set depth**: the source's search UI
paginates, and a month across the four bodies is a few hundred records — one
or two pages. A yearly partition would push the busiest bodies into deep,
unreliable pagination; daily would mean ~1,460 mostly-empty requests/year/body.
**Blast radius**: a partition is the retry unit — one failure re-runs a month,
not a decade. **Source behaviour**: decisions get amended after publication,
so the current month is never "final" and needs cheap, frequent re-runs.

It's config, not code (`SCRAPE_PARTITION_SIZE`): weekly for a dense archive
like the Labour Court's, quarterly for a sparse body like the EAT.

## Retries and rate limiting

Three layers: **per-request retries** (Scrapy, up to 4 retries after the
initial attempt, on 429/5xx/timeout; retries are deprioritised, and backoff
comes from AutoThrottle — Scrapy's retry middleware does not read Retry-After);
**adaptive throttling** (AutoThrottle widens the delay for every request as
soon as the server slows or errors, and narrows it when it recovers — faster
and gentler than a fixed `sleep()`); **per-partition retries** (Dagster, 2
attempts, 60s delay) for failures the crawler can't recover from in-process.

We identify the crawler honestly (User-Agent + contact address), obey
`robots.txt`, and cap per-domain concurrency — the output feeds a legal
product, and a corpus whose provenance can't survive due diligence is
worthless.

## Deduplication and immutability

The landing zone is **append-only and hash-versioned**: one Mongo row and one
object per distinct content version, keyed `(source, identifier, body,
file_hash)` under a **unique index** (identifiers normalised first — the site
formats them inconsistently). Object keys embed the content hash, so nothing
is ever overwritten; an amended decision lands as a *new* version beside the
old one (free amendment history), and the previous version is never mutated —
satisfying "don't update/delete Landing Zone data" literally.

Idempotency operates at the **network level**: known `(identifier, body)`
pairs are skipped at *listing* time, before their detail page or document is
requested — a second run of the same window re-downloads nothing
(`SCRAPE_RECHECK_KNOWN=true` flips to re-fetch-and-hash-compare for a
scheduled amendment sweep; ETag/If-Modified-Since would be preferable but this
source doesn't serve them dependably). The known-hash index loads **once per
run** as one range query — a per-record Mongo call would block Twisted's
reactor. Every run ends with a per-`(body, partition)` **reconciliation
ledger** (source-reported vs discovered vs succeeded/skipped/failed, status
complete|partial|failed) that drives the process exit code — a failed
partition can never report green.

## Scaling to 50+ sources

Already structured for this: all site-specific logic lives behind one
`SearchAdapter` protocol with a **source registry** (`SOURCE_ADAPTERS`,
selected by `SCRAPE_SOURCE`), and every record carries a `source` field in its
natural key. Adding a source means registering one adapter, not pipeline
edits. The transform streams Mongo in **bounded batches** (configurable), so
memory is flat at any corpus size.

What changes at that scale: **config becomes data** — a declarative per-source
registry (URL, adapter, rate limits, robots policy, legal basis) that the
orchestrator generates assets from, not hand-written definitions. **Per-source
isolation** — Dagster concurrency-limit tags so one slow site can't starve a
fast one, and a per-source failure budget. **The idempotency index outgrows a
dict** — ~100 bytes/record is fine at a million, uncomfortable at fifty
million; move to Redis keyed by `(source, identifier)`, shared across workers.
**Extraction needs golden fixtures per source** so a redesign fails a test
instead of silently degrading the corpus — the highest-value alert is
"records per partition dropped >N% vs. trailing average." **PDF text
extraction/OCR becomes its own stage**, since scanned-document failure modes
are unrelated to HTML cleaning.
