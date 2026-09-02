"""Spider unit tests.

Scrapy spiders resist testing when their logic is buried in callbacks that
touch the network. Ours keep the callbacks pure-ish -- take a Response, yield
Requests/items -- so we can drive them with hand-built ``HtmlResponse`` objects
and assert on what comes out. No sockets are opened anywhere in this file.
"""

from datetime import date

import mongomock
import pytest
from scrapy.http import HtmlResponse, Request, TextResponse

from wrc_pipeline.config import MongoSettings
from wrc_pipeline.models import DocumentType
from wrc_pipeline.partitioning import Partition
from wrc_pipeline.scraping.search_adapter import SearchResultRow
from wrc_pipeline.scraping.spiders.decisions import DecisionsSpider
from wrc_pipeline.storage.metadata_store import MetadataStore

PARTITION = Partition(start=date(2025, 7, 1), end=date(2025, 7, 31))


@pytest.fixture()
def spider():
    s = DecisionsSpider(
        start_date="2025-07-01",
        end_date="2025-08-01",
        bodies="Workplace Relations Commission",
        partition_size="monthly",
        run_id="test-run",
    )
    # Swap the real Mongo client for an in-memory one; nothing else changes.
    s._store = MetadataStore(MongoSettings(uri="mongodb://localhost", database="t"))
    s._store._client = mongomock.MongoClient()
    return s


def response(url: str, body: str, content_type: str = "text/html") -> HtmlResponse:
    return HtmlResponse(
        url=url,
        body=body.encode(),
        encoding="utf-8",
        headers={"Content-Type": content_type},
        request=Request(url),
    )


ROW = SearchResultRow(
    identifier="ADJ-00054658",
    description="Declan Holden V Ger Brennan Construction",
    published_date=date(2025, 7, 17),
    detail_url="https://example.ie/en/cases/2025/july/adj-00054658.html",
)

SEARCH_PAGE = """
<html><body>
  <p>Shows 1 to 1 of 1 results</p>
  <div>
    <div><h3>ADJ-00054658</h3><span>17/07/2025</span></div>
    <p>Declan Holden V Ger Brennan Construction</p>
    <div>Ref no: ADJ-00054658
      <a href="/en/cases/2025/july/adj-00054658.html">View Page</a></div>
  </div>
</body></html>
"""

DETAIL_WITH_PDF = """
<html><body><main id="main">
  <h1>ADJ-00054658</h1>
  <a href="/en/docs/adj-00054658.pdf">Download decision</a>
</main></body></html>
"""

DETAIL_SELF_CONTAINED = """
<html><body><main id="main">
  <h1>ADJ-00054658</h1>
  <p>ADJUDICATION OFFICER Decision. The complaint is well founded.</p>
</main></body></html>
"""


# ----------------------------------------------------------------- search page
def test_search_page_yields_one_detail_request_per_row(spider):
    resp = response("https://example.ie/en/cases/?page=1", SEARCH_PAGE)
    requests = list(spider.parse_search(resp, "Workplace Relations Commission", PARTITION, 1))

    detail = [r for r in requests if "adj-00054658" in r.url]
    assert len(detail) == 1
    assert spider.stats_model.records_found == 1


def test_no_next_link_means_no_pagination_request(spider):
    resp = response("https://example.ie/en/search/?page=1", SEARCH_PAGE)
    requests = list(spider.parse_search(resp, "Workplace Relations Commission", PARTITION, 1))
    assert all("page=2" not in r.url for r in requests)


def test_next_link_is_followed_verbatim(spider):
    paged = SEARCH_PAGE.replace(
        "</body>",
        '<a href="/en/search/?decisions=1&amp;body=15376&amp;page=2">Next</a></body>',
    )
    resp = response("https://example.ie/en/search/?page=1", paged)
    requests = list(spider.parse_search(resp, "Workplace Relations Commission", PARTITION, 1))
    next_requests = [r for r in requests if "page=2" in r.url]
    assert len(next_requests) == 1
    assert next_requests[0].cb_kwargs["page"] == 2


def test_empty_partition_yields_nothing_and_does_not_crash(spider):
    resp = response("https://example.ie/en/cases/?page=1", "<html><body></body></html>")
    assert list(spider.parse_search(resp, "Workplace Relations Commission", PARTITION, 1)) == []


def test_record_cap_limits_yielded_requests(spider):
    row_template = """
    <div>
      <div><h3>ADJ-0000000{n}</h3><span>17/07/2025</span></div>
      <div>Ref no: ADJ-0000000{n}
        <a href="/en/cases/2025/july/adj-0000000{n}.html">View Page</a></div>
    </div>
    """
    page = (
        "<html><body>"
        + "".join(row_template.format(n=i) for i in range(1, 4))
        + "</body></html>"
    )
    resp = response("https://example.ie/en/search/?page=1", page)

    object.__setattr__(spider.settings_obj.scrape, "max_records_per_partition", 2)
    try:
        requests = list(spider.parse_search(resp, "WRC", PARTITION, 1))
        assert len([r for r in requests if "/en/cases/" in r.url]) == 2
    finally:
        object.__setattr__(spider.settings_obj.scrape, "max_records_per_partition", 0)


# ----------------------------------------------------------------- detail page
def test_detail_page_with_pdf_link_follows_the_pdf(spider):
    resp = response(ROW.detail_url, DETAIL_WITH_PDF)
    out = list(spider.parse_detail(resp, ROW, "Workplace Relations Commission", PARTITION))

    assert len(out) == 1
    assert isinstance(out[0], Request)
    assert out[0].url.endswith(".pdf")


def test_self_contained_detail_page_is_stored_as_html(spider):
    resp = response(ROW.detail_url, DETAIL_SELF_CONTAINED)
    out = list(spider.parse_detail(resp, ROW, "Workplace Relations Commission", PARTITION))

    assert len(out) == 1
    item = out[0]
    assert item["__action__"] == "store"
    assert item["record"].document_type == DocumentType.HTML
    assert item["record"].identifier == "ADJ-00054658"
    assert item["record"].partition_date == PARTITION.start
    assert item["record"].file_hash is not None


def test_pdf_response_is_typed_from_content_type(spider):
    pdf = TextResponse(
        url="https://example.ie/en/docs/adj-00054658.ashx",
        body=b"%PDF-1.4 fake",
        headers={"Content-Type": "application/pdf"},
        request=Request("https://example.ie/en/docs/adj-00054658.ashx"),
    )
    out = list(
        spider.parse_document(
            pdf, ROW, "Workplace Relations Commission", PARTITION, ROW.detail_url, "title"
        )
    )
    assert out[0]["record"].document_type == DocumentType.PDF


# ---------------------------------------------------------------- idempotency
def test_unchanged_hash_yields_touch_not_store(spider):
    resp = response(ROW.detail_url, DETAIL_SELF_CONTAINED)
    first = list(spider.parse_detail(resp, ROW, "Workplace Relations Commission", PARTITION))
    known_hash = first[0]["record"].file_hash

    spider.known_hashes[("ADJ-00054658", "Workplace Relations Commission")] = known_hash
    second = list(spider.parse_detail(resp, ROW, "Workplace Relations Commission", PARTITION))

    assert second[0]["__action__"] == "touch"
    assert spider.stats_model.records_skipped_unchanged == 1


def test_changed_content_is_stored_again(spider):
    spider.known_hashes[("ADJ-00054658", "Workplace Relations Commission")] = "old-hash"
    resp = response(ROW.detail_url, DETAIL_SELF_CONTAINED)
    out = list(spider.parse_detail(resp, ROW, "Workplace Relations Commission", PARTITION))
    assert out[0]["__action__"] == "store"


# --------------------------------------------------------------------- errors
class FakeFailure:
    """Minimal stand-in for a Twisted Failure."""

    def __init__(self, url: str, status: int | None = None):
        self.request = Request(
            url, meta={"partition_key": "2025-07-01", "body_name": "Labour Court"}
        )
        self.value = type("E", (), {"response": type("R", (), {"status": status})()})()

    def getErrorMessage(self) -> str:  # noqa: N802 - Twisted's casing
        return "503 Service Unavailable"


def test_failed_request_is_counted_and_persisted(spider):
    spider.handle_error(FakeFailure("https://example.ie/en/docs/broken.pdf", 503))

    assert spider.stats_model.downloads_failed == 1
    assert spider.stats_model.failures[0]["http_status"] == 503
    stored = list(spider._store.failures.find({}))
    assert len(stored) == 1
    assert stored[0]["reason"] == "503 Service Unavailable"
    assert stored[0]["url"].endswith("broken.pdf")


def test_run_summary_reconciles_found_vs_scraped(spider):
    """The requirement: 200 found -> 200 scraped, or 200-X with X logged."""
    spider.stats_model.records_found = 200
    spider.stats_model.records_scraped = 197
    spider.stats_model.downloads_failed = 3

    summary = spider.stats_model.summary()
    assert summary["records_found"] - summary["records_scraped"] == summary["downloads_failed"]
    assert summary["success_rate"] == 0.985


# --------------------------------------------------------------------------
# Crawl entry points.
#
# Scrapy 2.13 replaced the synchronous ``start_requests()`` entry point with
# the asynchronous ``start()``, and recent releases no longer call
# ``start_requests()`` at all. A spider defining only the legacy method
# "finishes" in milliseconds with zero requests and **no error** -- the
# nastiest possible failure mode for a scheduled pipeline. These tests pin
# both entry points so neither can silently rot.
# --------------------------------------------------------------------------
def test_start_requests_fans_out_one_search_request_per_body_partition(spider):
    requests = list(spider.start_requests())

    assert len(requests) == 1  # one body x one monthly partition
    url = requests[0].url
    assert "/en/search/" in url
    assert "decisions=1" in url
    assert "body=15376" in url  # Workplace Relations Commission
    assert requests[0].callback == spider.parse_search


def test_async_start_yields_the_same_requests_as_start_requests(spider):
    """The Scrapy >= 2.13 bridge: ``start()`` must delegate to ``start_requests()``."""
    import asyncio

    async def collect():
        return [request async for request in spider.start()]

    requests = asyncio.run(collect())
    assert [r.url for r in requests] == [r.url for r in spider.start_requests()]


# --------------------------------------------------------------------------
# Regressions caught by the first live run against workplacerelations.ie.
# --------------------------------------------------------------------------
DETAIL_WITH_COOKIE_BANNER = """
<html><body>
  <div class="consent">
    Our website uses cookies.
    <a href="/en/privacy-policy/cookie_policy.pdf">Cookie Policy</a>
  </div>
  <main id="main">
    <h1>ADJ-00054658</h1>
    <p>ADJUDICATION OFFICER Decision. The complaint is well founded.</p>
  </main>
</body></html>
"""


def test_cookie_policy_pdf_is_not_mistaken_for_the_decision(spider):
    """Live-run regression: every WRC detail page links cookie_policy.pdf from
    its consent banner; a naive first-PDF-link selector sent every record
    there (a 404) and the smoke test stored nothing."""
    resp = response(
        "https://example.ie/en/cases/2025/july/adj-00054658.html", DETAIL_WITH_COOKIE_BANNER
    )
    results = list(spider.parse_detail(resp, ROW, "Workplace Relations Commission", PARTITION))

    # No request chased the cookie PDF; the page itself became the record.
    assert all(not isinstance(r, type(resp.request)) or "cookie" not in r.url for r in results)
    stores = [r for r in results if isinstance(r, dict) and r.get("__action__") == "store"]
    assert len(stores) == 1
    assert stores[0]["record"].document_type == "html"


def test_real_document_link_still_wins_over_the_html_page(spider):
    banner_plus_doc = DETAIL_WITH_COOKIE_BANNER.replace(
        "</main>", '<a href="/en/docs/adj-00054658.pdf">Download decision</a></main>'
    )
    resp = response(
        "https://example.ie/en/cases/2025/july/adj-00054658.html", banner_plus_doc
    )
    results = list(spider.parse_detail(resp, ROW, "Workplace Relations Commission", PARTITION))

    assert len(results) == 1
    assert results[0].url.endswith("/en/docs/adj-00054658.pdf")


PAGE_ONE_OF_MANY = """
<html><body>
  <p>Shows 1 to 1 of 30 results</p>
  <div>
    <div><h3>ADJ-00054658</h3><span>17/07/2025</span></div>
    <div>Ref no: ADJ-00054658
      <a href="/en/cases/2025/july/adj-00054658.html">View Page</a></div>
  </div>
</body></html>
"""


def test_pagination_falls_back_to_a_constructed_url(spider):
    """Live-run regression: the site reported 234 results but rendered no
    next-page link our matcher recognised, so the crawl silently stopped at
    page 1. When the counter says more records exist, the spider must
    construct the next page's URL itself."""
    resp = response("https://example.ie/en/search/?decisions=1", PAGE_ONE_OF_MANY)
    requests = list(spider.parse_search(resp, "Workplace Relations Commission", PARTITION, 1))

    page_two = [r for r in requests if "pageNumber=2" in r.url]
    assert len(page_two) == 1
    assert page_two[0].cb_kwargs["page"] == 2


def test_constructed_pagination_stops_when_a_page_repeats(spider):
    """The safety net for the fallback: if page 2 re-serves page 1's rows
    (i.e. our page parameter guess was wrong), pagination must stop instead
    of looping to page 500."""
    resp1 = response("https://example.ie/en/search/?decisions=1", PAGE_ONE_OF_MANY)
    list(spider.parse_search(resp1, "Workplace Relations Commission", PARTITION, 1))

    resp2 = response("https://example.ie/en/search/?decisions=1&page=2", PAGE_ONE_OF_MANY)
    requests = list(spider.parse_search(resp2, "Workplace Relations Commission", PARTITION, 2))
    assert requests == []


def test_relative_next_page_hrefs_are_recognised(spider):
    """A pager that emits `?page=2` (no path) must still be followed."""
    paged = PAGE_ONE_OF_MANY.replace(
        "</body>", '<a href="?decisions=1&amp;page=2">2</a></body>'
    )
    resp = response("https://example.ie/en/search/?decisions=1", paged)
    requests = list(spider.parse_search(resp, "Workplace Relations Commission", PARTITION, 1))
    page_two = [r for r in requests if "page=2" in r.url and r.cb_kwargs.get("page") == 2]
    assert len(page_two) == 1
