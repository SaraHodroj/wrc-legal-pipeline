"""The decisions spider.

Flow per ``(body, partition)``:

    search page 1..N  ->  result rows  ->  detail page / document  ->  Item

Two design decisions worth defending in review:

1. **Documents are fetched as Scrapy Requests, not with ``requests`` inside a
   pipeline.** Scrapy runs on a single-threaded Twisted reactor. A synchronous
   HTTP call in a pipeline blocks *every* in-flight request, collapsing
   concurrency to one. Routing downloads back through the scheduler means they
   inherit retries, AutoThrottle, the robots.txt policy and the duplicate
   filter for free.

2. **The idempotency index is loaded once at spider start**, not queried per
   record -- same reasoning. See ``MetadataStore.load_known_hashes``.
"""

from __future__ import annotations

import uuid
from collections.abc import AsyncIterator, Iterator
from datetime import UTC, date, datetime
from typing import Any

import scrapy
from scrapy.http import Response

from ...config import get_settings
from ...hashing import hash_bytes
from ...logging_setup import get_logger
from ...models import DecisionRecord, DocumentType, FailedRecord, RunStats
from ...partitioning import Partition, iter_partitions
from ...storage.metadata_store import MetadataStore
from ..search_adapter import WrcSearchAdapter

logger = get_logger(__name__)


class DecisionsSpider(scrapy.Spider):
    """Crawl WRC decisions for a date window, one partition at a time."""

    name = "decisions"

    custom_settings: dict[str, Any] = {
        # Detail pages are HTML; documents can be many MB. Cap the in-memory
        # response size so one pathological file cannot exhaust the worker.
        "DOWNLOAD_MAXSIZE": 100 * 1024 * 1024,
        "DOWNLOAD_WARNSIZE": 20 * 1024 * 1024,
    }

    def __init__(
        self,
        start_date: str | None = None,
        end_date: str | None = None,
        bodies: str | None = None,
        partition_size: str | None = None,
        run_id: str | None = None,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)
        settings = get_settings()

        self.settings_obj = settings
        self.start_date = _parse_iso(start_date) or settings.run.start_date
        self.end_date = _parse_iso(end_date) or settings.run.end_date
        self.partition_size = partition_size or settings.scrape.partition_size
        self.bodies = (
            tuple(b.strip() for b in bodies.split("|") if b.strip())
            if bodies
            else settings.scrape.bodies
        )
        self.run_id = run_id or f"run-{datetime.now(UTC):%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:8]}"

        self.adapter = WrcSearchAdapter(base_url=settings.scrape.base_url)
        self.stats_model = RunStats(run_id=self.run_id)
        self.known_hashes: dict[tuple[str, str], str | None] = {}
        # Detail URLs already yielded, per (body, partition key) -- the guard
        # that makes the pagination fallback in parse_search self-limiting.
        self._seen_detail_urls: dict[tuple[str, str], set[str]] = {}
        self._partitions: list[Partition] = list(
            iter_partitions(self.start_date, self.end_date, self.partition_size)  # type: ignore[arg-type]
        )
        self._store = MetadataStore(settings.mongo)

    # --------------------------------------------------------------- lifecycle
    async def start(self) -> AsyncIterator[scrapy.Request]:
        """Scrapy >= 2.13 entry point.

        Scrapy 2.13 replaced the synchronous ``start_requests()`` with the
        asynchronous ``start()``, and recent releases no longer call
        ``start_requests()`` at all -- a spider defining only the old method
        "finishes" instantly with zero requests and no error. Defining both,
        with ``start()`` delegating to ``start_requests()``, keeps one code
        path that works on every Scrapy version this project pins.
        """
        for request in self.start_requests():
            yield request

    def start_requests(self) -> Iterator[scrapy.Request]:
        """Preload idempotency state, then fan out across (body, partition)."""
        self._store.ensure_indexes()
        self.known_hashes = self._store.load_known_hashes(
            self.start_date, self.end_date, self.bodies
        )

        logger.info(
            "Crawl starting",
            extra={
                "event": "run_started",
                "run_id": self.run_id,
                "start_date": self.start_date,
                "end_date": self.end_date,
                "partition_size": self.partition_size,
                "partitions": len(self._partitions),
                "bodies": list(self.bodies),
            },
        )

        for partition in self._partitions:
            for body in self.bodies:
                yield self._search_request(body, partition, page=1)

    def _search_request(self, body: str, partition: Partition, page: int) -> scrapy.Request:
        url = self.adapter.build_search_url(body, partition.start, partition.end, page)
        return scrapy.Request(
            url=url,
            callback=self.parse_search,
            errback=self.handle_error,
            cb_kwargs={"body_name": body, "partition": partition, "page": page},
            # Search pages are cheap and high-fan-out; prioritising them keeps
            # the download queue fed rather than draining one branch at a time.
            priority=10,
            meta={"partition_key": partition.key, "body_name": body},
            dont_filter=True,
        )

    # ------------------------------------------------------------------ search
    def parse_search(
        self, response: Response, body_name: str, partition: Partition, page: int
    ) -> Iterator[scrapy.Request]:
        rows = self.adapter.parse_rows(response.text, response.url)
        total = self.adapter.total_results(response.text)

        logger.info(
            "Search page parsed",
            extra={
                "event": "search_page_parsed",
                "run_id": self.run_id,
                "partition": partition.key,
                "body": body_name,
                "page": page,
                "rows_on_page": len(rows),
                "total_reported": total,
            },
        )

        if page == 1 and not rows:
            # Empty is legitimate (a quiet month), but it is also exactly what a
            # broken selector looks like. Log it loudly enough to notice.
            logger.warning(
                "Partition returned no rows",
                extra={
                    "event": "empty_partition",
                    "run_id": self.run_id,
                    "partition": partition.key,
                    "body": body_name,
                },
            )
            return

        # Track which detail URLs this (body, partition) has already produced.
        # This is what makes the constructed-URL pagination fallback below
        # safe: a wrong page parameter just re-serves page 1, yields zero new
        # rows, and pagination stops after one wasted request instead of
        # looping or double-counting.
        seen = self._seen_detail_urls.setdefault((body_name, partition.key), set())
        new_rows = [row for row in rows if row.detail_url not in seen]
        seen.update(row.detail_url for row in rows)

        if page > 1 and not new_rows:
            return  # the site re-served content we already have -- end of pages

        cap = self.settings_obj.scrape.max_records_per_partition
        if cap:
            new_rows = new_rows[: max(0, cap - (len(seen) - len(new_rows)))]

        for row in new_rows:
            self.stats_model.records_found += 1
            yield scrapy.Request(
                url=row.detail_url,
                callback=self.parse_detail,
                errback=self.handle_error,
                cb_kwargs={"row": row, "body_name": body_name, "partition": partition},
                meta={"partition_key": partition.key, "body_name": body_name},
            )

        if cap and len(seen) >= cap:
            return  # smoke-test cap reached; no point fetching further pages

        # Pagination, two strategies in order of trust. First, follow the
        # next-page link the site itself renders. If no link is recognised but
        # the results counter says more records exist than we have seen, fall
        # back to constructing the next page's URL -- the seen-set above turns
        # a wrong construction into a no-op rather than an infinite loop.
        # Page depth is capped as a circuit breaker either way.
        if page >= 500:
            return
        next_url = self.adapter.next_page_url(response.text, response.url, page)
        if not next_url and total and len(seen) < total and new_rows:
            next_url = self.adapter.build_search_url(
                body_name, partition.start, partition.end, page + 1
            )
            logger.info(
                "No next-page link recognised; falling back to constructed URL",
                extra={
                    "event": "pagination_fallback",
                    "run_id": self.run_id,
                    "partition": partition.key,
                    "body": body_name,
                    "page": page + 1,
                    "seen": len(seen),
                    "total_reported": total,
                },
            )
        if next_url:
            yield scrapy.Request(
                url=next_url,
                callback=self.parse_search,
                errback=self.handle_error,
                cb_kwargs={"body_name": body_name, "partition": partition, "page": page + 1},
                priority=10,
                meta={"partition_key": partition.key, "body_name": body_name},
                dont_filter=True,
            )

    # ------------------------------------------------------------------ detail
    def parse_detail(
        self, response: Response, row: Any, body_name: str, partition: Partition
    ) -> Iterator[scrapy.Request | dict[str, Any]]:
        """Turn a detail page into a stored artefact + metadata record.

        The brief distinguishes two cases: a link to a PDF/DOC (store the bytes
        untouched) and a link to an HTML page (store the rendered page as
        ``.html``). Both land here; the branch is on Content-Type.
        """
        content_type = _header(response, "Content-Type")
        doc_type = DocumentType.from_url_or_content_type(response.url, content_type)

        if doc_type is DocumentType.HTML or doc_type is DocumentType.UNKNOWN:
            # The detail page may itself embed a link to a PDF. If so, follow
            # it -- the primary document beats the HTML wrapper around it.
            document_href = self._document_link(response)
            if document_href:
                yield response.follow(
                    document_href,
                    callback=self.parse_document,
                    errback=self.handle_error,
                    cb_kwargs={
                        "row": row,
                        "body_name": body_name,
                        "partition": partition,
                        "source_url": response.url,
                        "title": self._extract_title(response),
                    },
                    meta={"partition_key": partition.key, "body_name": body_name},
                )
                return

        # No linked document: the page itself is the record.
        yield from self._emit(
            row=row,
            body_name=body_name,
            partition=partition,
            source_url=response.url,
            document_url=response.url,
            payload=response.body,
            doc_type=DocumentType.HTML if doc_type is DocumentType.UNKNOWN else doc_type,
            content_type=content_type,
            title=self._extract_title(response),
        )

    def parse_document(
        self,
        response: Response,
        row: Any,
        body_name: str,
        partition: Partition,
        source_url: str,
        title: str | None,
    ) -> Iterator[dict[str, Any]]:
        content_type = _header(response, "Content-Type")
        yield from self._emit(
            row=row,
            body_name=body_name,
            partition=partition,
            source_url=source_url,
            document_url=response.url,
            payload=response.body,
            doc_type=DocumentType.from_url_or_content_type(response.url, content_type),
            content_type=content_type,
            title=title,
        )

    # -------------------------------------------------------------- emit / skip
    def _emit(
        self,
        row: Any,
        body_name: str,
        partition: Partition,
        source_url: str,
        document_url: str,
        payload: bytes,
        doc_type: DocumentType,
        content_type: str | None,
        title: str | None,
    ) -> Iterator[dict[str, Any]]:
        """Hash, compare against known state, and emit only if work is needed.

        This is the idempotency gate. We have already paid for the download by
        the time we get here -- the saving is on the *write* side (no object
        storage PUT, no metadata rewrite) plus a clean audit trail showing the
        content was verified unchanged rather than skipped blindly.

        Avoiding the download itself would need a conditional request
        (If-None-Match / If-Modified-Since); this source does not set those
        headers dependably, so content hashing is the honest mechanism.
        """
        file_hash = hash_bytes(payload)
        record = DecisionRecord(
            identifier=row.identifier,
            body=body_name,
            title=title,
            description=row.description,
            published_date=row.published_date,
            source_url=source_url,
            document_url=document_url,
            partition_date=partition.start,
            run_id=self.run_id,
            document_type=doc_type,
            file_hash=file_hash,
            file_size_bytes=len(payload),
        )

        key = (record.identifier, record.body)
        if self.known_hashes.get(key) == file_hash:
            self.stats_model.records_skipped_unchanged += 1
            self.stats_model.records_scraped += 1
            logger.debug(
                "Unchanged, skipping write",
                extra={
                    "event": "record_unchanged",
                    "run_id": self.run_id,
                    "identifier": record.identifier,
                    "partition": partition.key,
                },
            )
            # Still refresh the audit trail so 'last_seen' stays truthful.
            yield {"__action__": "touch", "identifier": record.identifier, "body": record.body}
            return

        self.stats_model.bytes_downloaded += len(payload)
        yield {
            "__action__": "store",
            "record": record,
            "payload": payload,
            "content_type": content_type,
        }

    # ------------------------------------------------------------------ errors
    def handle_error(self, failure: Any) -> None:
        """Record a failed fetch instead of letting it vanish into the log.

        The requirement is explicit: if a partition holds 200 records we scrape
        200, or 200-X with every one of the X logged and attributable. That is
        only possible if failures are captured as data.
        """
        request = failure.request
        status = getattr(getattr(failure.value, "response", None), "status", None)
        reason = failure.getErrorMessage()

        self.stats_model.downloads_failed += 1
        entry = {
            "url": request.url,
            "reason": reason,
            "http_status": status,
            "partition": request.meta.get("partition_key"),
            "body": request.meta.get("body_name"),
        }
        self.stats_model.failures.append(entry)

        logger.error(
            "Request failed",
            extra={"event": "request_failed", "run_id": self.run_id, **entry},
        )

        try:
            self._store.record_failure(
                FailedRecord(
                    url=request.url,
                    body=request.meta.get("body_name", "unknown"),
                    partition_date=_parse_iso(request.meta.get("partition_key"))
                    or self.start_date,
                    run_id=self.run_id,
                    reason=reason,
                    http_status=status,
                )
            )
        except Exception:  # pragma: no cover - never let bookkeeping kill a crawl
            logger.exception("Could not persist failure record")

    def closed(self, reason: str) -> None:
        """Emit the end-of-run summary and release connections."""
        self.stats_model.finished_at = datetime.now(tz=None).astimezone()
        self.stats_model.partitions_processed = len(self._partitions)
        logger.info("Run complete", extra={**self.stats_model.summary(), "close_reason": reason})
        self._store.close()

    # ----------------------------------------------------------------- helpers
    #: Link-target fragments that mark site furniture, never the decision
    #: document. Learned from a live run: every WRC detail page links the
    #: cookie-policy PDF from its consent banner, and a naive "first PDF link
    #: on the page" selector routed all ten records of the smoke test to
    #: /en/privacy-policy/cookie_policy.pdf (a 404) instead of the decision.
    CHROME_LINK_FRAGMENTS = ("cookie", "privacy-policy", "privacy_policy")

    _DOC_LINK_CSS = (
        "a[href$='.pdf']::attr(href), a[href$='.doc']::attr(href), "
        "a[href$='.docx']::attr(href)"
    )

    @classmethod
    def _document_link(cls, response: Response) -> str | None:
        """First document link that plausibly IS the decision.

        Prefer links inside the main content container (the page chrome lives
        outside it), fall back to the whole page for legacy markup, and in
        both cases refuse links that are clearly site furniture.
        """
        main = response.css("main")
        candidates: list[str] = list(main.css(cls._DOC_LINK_CSS).getall()) if main else []
        candidates += response.css(cls._DOC_LINK_CSS).getall()
        for href in candidates:
            if not any(frag in href.lower() for frag in cls.CHROME_LINK_FRAGMENTS):
                return str(href)
        return None

    @staticmethod
    def _extract_title(response: Response) -> str | None:
        for css in ("h1::text", "h2::text", "title::text"):
            value = response.css(css).get()
            if value and value.strip():
                return " ".join(value.split())
        return None


def _header(response: Response, name: str) -> str | None:
    raw = response.headers.get(name)
    return raw.decode("utf-8", errors="ignore") if raw else None


def _parse_iso(value: str | None) -> date | None:
    if not value:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"Dates must be ISO format (YYYY-MM-DD); got {value!r}") from exc
