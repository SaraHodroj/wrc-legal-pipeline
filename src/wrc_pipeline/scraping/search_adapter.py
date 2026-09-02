"""Site-specific adapter for the WRC 'Decisions and Determinations' search.

This module is deliberately the *only* place in the codebase that knows how the
Workplace Relations site is shaped. Everything else -- partitioning, hashing,
idempotency, storage, orchestration -- is source-agnostic.

That boundary is the whole design. The brief says this pipeline should scale to
50+ legal sources; that is only tractable if adding a source means writing one
new adapter against the ``SearchAdapter`` protocol, not editing the spider, the
pipelines and the storage layer for every jurisdiction.

--------------------------------------------------------------------------
IMPORTANT -- verify the request shape before running
--------------------------------------------------------------------------
The search results on this site are populated by an XHR call, not by the
server-rendered HTML at ``/en/cases/``. Fetching that URL returns the page
chrome and no results, so the selectors below must be confirmed against the
real request:

1. Open https://www.workplacerelations.ie/en/cases/ with DevTools -> Network,
   filtered to Fetch/XHR.
2. Run a search with a Body and a date range set.
3. Inspect the request that carries the results. Note its method, path, the
   exact parameter names for body / start date / finish date / page, and
   whether the response is JSON or an HTML fragment.
4. Update ``PARAM_MAP``, ``SEARCH_ENDPOINT`` and the selectors below to match.

Pinning those details in one small, well-named module -- instead of scattering
magic strings through the spider -- is what keeps that a five-minute change.
"""

from __future__ import annotations

import re
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import date, datetime
from typing import Protocol
from urllib.parse import urljoin

from parsel import Selector

from ..logging_setup import get_logger

logger = get_logger(__name__)

# CONFIRMED against the live site (2026-09): a plain GET, full-page reload --
# not an XHR call, and not the /en/cases/ page. Example real request:
#   /en/search/?decisions=1&from=1/1/2024&to=31/1/2024&body=15376
SEARCH_ENDPOINT = "/en/search/"

#: The site keys bodies by a numeric ID, not by name. Confirmed by checking
#: each Body filter individually and reading the resulting `body=` param.
BODY_IDS: dict[str, int] = {
    "Equality Tribunal": 1,
    "Employment Appeals Tribunal": 2,
    "Labour Court": 3,
    "Workplace Relations Commission": 15376,
}

#: Maps our internal concepts to the site's query parameter names.
PARAM_MAP = {
    "decisions": "decisions",  # fixed flag, always "1" -- confirmed live
    "body": "body",
    "start_date": "from",
    "finish_date": "to",
    # Confirmed live (2026-09-02): the site's own next-page link reads
    # `...&pageNumber=2` -- the parameter is `pageNumber`, not `page`.
    "page": "pageNumber",
}

#: The site renders dates as D/M/YYYY -- NOT zero-padded (e.g. "1/1/2024",
#: not "01/01/2024"). strftime always zero-pads, so build these by hand
#: instead of trusting %d/%m/%Y, which would silently send a wrong-but-similar
#: value the site might mishandle.
def _format_site_date(value: date) -> str:
    return f"{value.day}/{value.month}/{value.year}"

# Row parsing strategy -- anchored on structure, not CSS classes.
#
# We could not lift the site's exact class names (the page's giant filter
# dropdowns defeat text extraction), and guessing classes is how scrapers rot.
# Instead we anchor on the one invariant the site cannot change without
# breaking itself: every result links to a detail page under ``/en/cases/``.
# From each such link we walk up to its enclosing block and pull the fields
# out by *shape*: the "Ref no:" label, a d/m/yyyy date, the description line.
# A cosmetic re-skin (new classes, new wrappers) leaves all of that intact.
DETAIL_HREF_FRAGMENT = "/en/cases/"

DATE_RE = re.compile(r"\b(\d{1,2}/\d{1,2}/\d{4})\b")
REF_NO_RE = re.compile(
    r"Ref\s*no\.?:?\s*([A-Za-z0-9]+(?:\s*[-/.]\s*[A-Za-z0-9]+)*)", re.IGNORECASE
)
#: Text fragments that are row furniture, never the description.
ROW_FURNITURE = ("view page", "ref no")


@dataclass(frozen=True, slots=True)
class SearchResultRow:
    """One row of the search listing, before the detail page is fetched."""

    identifier: str
    description: str | None
    published_date: date | None
    detail_url: str


class SearchAdapter(Protocol):
    """Contract every source adapter must satisfy.

    Add a new legal source by implementing this against its site; the rest of
    the pipeline neither knows nor cares which jurisdiction it is crawling.
    """

    def build_search_url(self, body: str, start: date, end: date, page: int) -> str: ...

    def parse_rows(self, html: str, base_url: str) -> list[SearchResultRow]: ...

    def total_results(self, html: str) -> int | None: ...

    def next_page_url(self, html: str, base_url: str, current_page: int) -> str | None: ...


class WrcSearchAdapter:
    """Adapter for workplacerelations.ie."""

    def __init__(self, base_url: str) -> None:
        # No page-size knob: the site exposes no page-size parameter, so the
        # adapter takes none. (An earlier draft carried a dead
        # ``results_per_page`` setting; it was removed rather than shipped.)
        self._base_url = base_url.rstrip("/")

    # ------------------------------------------------------------- requests
    def build_search_url(self, body: str, start: date, end: date, page: int) -> str:
        """Compose the date-filtered, body-filtered search URL.

        Raises:
            ValueError: if ``body`` isn't a known body name -- fail loudly at
                URL-construction time rather than silently querying the wrong
                (or a nonexistent) body ID.
        """
        from urllib.parse import urlencode

        try:
            body_id = BODY_IDS[body]
        except KeyError:
            raise ValueError(
                f"Unknown body {body!r}. Known bodies: {sorted(BODY_IDS)}"
            ) from None

        params: dict[str, str | int] = {
            PARAM_MAP["decisions"]: 1,
            PARAM_MAP["body"]: body_id,
            PARAM_MAP["start_date"]: _format_site_date(start),
            PARAM_MAP["finish_date"]: _format_site_date(end),
        }
        # The confirmed URL carries no page param on page 1; only add it when
        # paginating, so the first request matches what the site actually sent.
        if page > 1:
            params[PARAM_MAP["page"]] = page

        return f"{self._base_url}{SEARCH_ENDPOINT}?{urlencode(params)}"

    # -------------------------------------------------------------- parsing
    def parse_rows(self, html: str, base_url: str) -> list[SearchResultRow]:
        """Extract result rows by anchoring on detail-page links.

        For every anchor pointing under ``/en/cases/`` we locate its enclosing
        result block and read the fields out of the block's text by shape.
        The identifier has two sources, tried in order: the explicit
        "Ref no:" label, then the URL slug itself (``adj-00045701.html`` ->
        ``ADJ-00045701``) -- so even a row with no readable text still yields
        a usable, correctly-keyed record.
        """
        selector = Selector(text=html)
        rows: dict[str, SearchResultRow] = {}

        for link in selector.xpath(f'//a[contains(@href, "{DETAIL_HREF_FRAGMENT}")]'):
            href = (link.attrib.get("href") or "").strip()
            # Skip the bare /en/cases/ index link and non-document anchors.
            if not href or href.rstrip("/").endswith("/en/cases"):
                continue
            detail_url = urljoin(base_url, href)
            if detail_url in rows:
                continue  # heading link + "View Page" both point at the same doc

            block_text = self._enclosing_block_text(link)

            identifier = self._identifier_from(block_text, href)
            if not identifier:
                logger.warning(
                    "Skipping row with no derivable identifier",
                    extra={"event": "row_parse_failed", "detail_url": detail_url},
                )
                continue

            date_match = DATE_RE.search(block_text)
            rows[detail_url] = SearchResultRow(
                identifier=identifier,
                description=self._description_from(block_text, identifier),
                published_date=self._parse_date(date_match.group(1) if date_match else None),
                detail_url=detail_url,
            )

        return list(rows.values())

    def total_results(self, html: str) -> int | None:
        """Read the 'Shows 1 to 10 of 62,789 results' counter, if present.

        Worth parsing: comparing it against what we actually ingested is how we
        detect a partition that silently under-returned.
        """
        selector = Selector(text=html)
        text = " ".join(selector.css("*::text").getall())
        match = re.search(r"of\s+([\d,]+)\s+results", text, flags=re.IGNORECASE)
        if not match:
            return None
        return int(match.group(1).replace(",", ""))

    def next_page_url(self, html: str, base_url: str, current_page: int) -> str | None:
        """Return the next page's URL as the site advertises it, if we can.

        We prefer the link the site itself renders over constructing one:
        following real markup can't guess a parameter name wrong. Anchors are
        resolved against the page URL *before* matching, because a pager
        typically emits relative hrefs (``?page=2``) that carry no path at
        all -- matching on the raw href would miss every one of them.

        The spider holds the safety net: when no link is recognised here but
        the results counter says more records exist, it constructs the next
        URL itself and relies on its seen-set to stop if that guess re-serves
        the same page.
        """
        selector = Selector(text=html)
        next_labels = {"next", "\u00bb", "\u203a", "next page", ">", ">>"}
        numeric_target = str(current_page + 1)

        for anchor in selector.xpath("//a[@href]"):
            href = (anchor.attrib.get("href") or "").strip()
            if not href or href.startswith(("#", "javascript:", "mailto:")):
                continue
            resolved = urljoin(base_url, href)
            if "/en/search" not in resolved:
                continue
            rel = (anchor.attrib.get("rel") or "").strip().lower()
            label = " ".join(anchor.xpath(".//text()").getall()).strip().lower()
            if rel == "next" or label in next_labels or label == numeric_target:
                return resolved
        return None

    # --------------------------------------------------------------- helpers
    @staticmethod
    def _enclosing_block_text(link: Selector) -> str:
        """Text of the nearest ancestor that looks like a full result block.

        Climb at most four levels: far enough to escape a button wrapper,
        near enough not to swallow the whole results list.
        """
        ref_only = ""
        best = ""
        node = link
        for _ in range(4):
            parent = node.xpath("..")
            if not parent:
                break
            node = parent[0]
            text = " ".join(t.strip() for t in node.xpath(".//text()").getall() if t.strip())
            if DATE_RE.search(text):
                # The full row block: it carries the published date (and, in
                # practice, the description). Stop climbing here rather than
                # swallowing the whole results list.
                return text
            if REF_NO_RE.search(text) and not ref_only:
                ref_only = text  # a keyable but incomplete wrapper -- keep looking
            best = text or best
        return ref_only or best

    def _identifier_from(self, block_text: str, href: str) -> str | None:
        match = REF_NO_RE.search(block_text)
        if match:
            return match.group(1).strip()
        # Fallback: the URL slug is the identifier in lowercase.
        slug = href.rstrip("/").rsplit("/", 1)[-1]
        slug = slug.split("?")[0]
        if slug.endswith(".html"):
            slug = slug[: -len(".html")]
        return slug.upper() if slug else None

    @staticmethod
    def _description_from(block_text: str, identifier: str) -> str | None:
        """The description is what remains once the known furniture is removed."""
        text = block_text
        text = DATE_RE.sub(" ", text)
        text = REF_NO_RE.sub(" ", text)
        text = re.sub(re.escape(identifier), " ", text, flags=re.IGNORECASE)
        for noise in ROW_FURNITURE:
            text = re.sub(noise, " ", text, flags=re.IGNORECASE)
        cleaned = " ".join(text.split()).strip(" -|:")
        return cleaned or None

    @staticmethod
    def _parse_date(value: str | None) -> date | None:
        """Accept the handful of date formats this site mixes in its markup."""
        if not value:
            return None
        raw = value.strip()
        for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d %B %Y", "%d-%m-%Y", "%Y-%m-%dT%H:%M:%S"):
            try:
                return datetime.strptime(raw[:19] if "T" in raw else raw, fmt).date()
            except ValueError:
                continue
        logger.warning(
            "Unrecognised date format", extra={"event": "date_parse_failed", "value": raw}
        )
        return None


def iter_pages(max_pages: int = 500) -> Iterator[int]:
    """Bounded page counter.

    An unbounded ``while True`` on pagination is how scrapers end up looping
    forever against a site that returns the same page for any out-of-range
    page number. The cap is a circuit breaker, not an expectation.
    """
    yield from range(1, max_pages + 1)
