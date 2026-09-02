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

Three layers: **per-request retries** (Scrapy, 4 attempts on 429/5xx/timeout,
Retry-After honoured); **adaptive throttling** (AutoThrottle targets a
concurrency level by measuring actual server latency, widening the delay when
the site slows and narrowing it when it recovers — faster and gentler than a
fixed `sleep()`); **per-partition retries** (Dagster, 2 attempts, 60s delay)
for failures the crawler can't recover from in-process.

We identify the crawler honestly (User-Agent + contact address), obey
`robots.txt`, and cap per-domain concurrency. This isn't just correctness —
the output feeds a legal product, and a corpus whose provenance can't survive
a client's due diligence is worthless. Bulk access beyond politeness limits
should be a licensed agreement, not an engineering workaround.

## Deduplication strategy

SHA-256 of stored bytes, enforced at three levels: a **unique index** on
`(identifier, body)` makes duplicate prevention a database guarantee, not an
app convention — even racing workers can't produce two rows (identifiers are
normalised first, since the site formats them inconsistently across pages).
**Content comparison** before every write — unchanged skips the S3 PUT and
Mongo rewrite entirely, just touches `last_seen_at`; changed updates in place
and stamps `content_changed_at`, giving free amendment history. **Deterministic
object keys** (no timestamp/run-id) mean a re-run overwrites, never
accumulates.

We dedupe on hash, not URL (unstable, tracking params) or ETag (this source
doesn't set it reliably). Honest limitation: we only detect "unchanged" after
downloading — the saving is on the write path and downstream reprocessing, not
bandwidth. True request-avoidance would need conditional requests this source
doesn't support.

The idempotency index loads **once per run** as a single indexed range query,
not per-record — a per-record Mongo call would block Twisted's single-threaded
reactor and stall every in-flight download.

## Scaling to 50+ sources

Already structured for this: all site-specific logic lives behind one
`SearchAdapter` protocol (`search_adapter.py`); partitioning, hashing,
idempotency, storage and orchestration are source-agnostic. Adding a source
means one new adapter, not pipeline edits.

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
