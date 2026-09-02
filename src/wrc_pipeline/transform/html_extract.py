"""Extract the substantive decision text from a stored HTML page.

The landing zone holds the page exactly as served: navigation, cookie banner,
header, footer, social links and all. None of that is part of the decision, and
leaving it in would poison anything downstream -- a search index would match on
boilerplate, and an LLM fed the raw page would spend most of its context window
reading the site's menu.

Strategy: strip the elements we know are chrome, then select the main content
container using an ordered list of candidate selectors, falling back to a
heuristic if none match. The fallback matters because this site's markup has
changed across the decades of decisions it hosts, and a single hardcoded
selector would silently return empty strings for older pages.
"""

from __future__ import annotations

from bs4 import BeautifulSoup, Tag

from ..logging_setup import get_logger

logger = get_logger(__name__)

#: Elements that are never part of a decision.
CHROME_TAGS = ("script", "style", "noscript", "nav", "header", "footer", "iframe", "svg", "form")

#: Class/id fragments that mark site furniture on this domain.
CHROME_PATTERNS = (
    "cookie", "banner", "breadcrumb", "skip-link", "site-header", "site-footer",
    "social", "share", "navigation", "menu", "sidebar", "search-form",
    "back-to-top", "print-page", "my-documents", "binder",
    "return-to-search", "returntosearch", "language-toggle", "font-size",
)

#: Link texts that are navigation, not decision content, wherever they appear.
CHROME_LINK_TEXTS = ("return to search", "back to search")

#: Below this many characters of text we assume extraction matched an empty
#: wrapper rather than the decision, and keep looking. Shared by the selector
#: path and the density fallback so the two cannot drift apart.
MIN_CONTENT_LENGTH = 200

#: Ordered candidates for the container holding the decision itself. The
#: site's real decision container (``div.content``, verified against a live
#: page) comes before the generic fallbacks so a match is tight, not a
#: whole-page wrapper.
CONTENT_SELECTORS = (
    "div.content",
    "main#main",
    "main",
    "div.case-content",
    "div.decision-content",
    "div#content",
    "article",
    "div.container div.row",
)


def extract_main_content(html: str, *, min_length: int = MIN_CONTENT_LENGTH) -> str:
    """Return cleaned HTML containing only the decision body.

    Args:
        html: The raw stored page.
        min_length: Below this many characters we treat the extraction as
            having failed and fall back, rather than returning a fragment that
            happened to match an empty wrapper div.

    Returns:
        An HTML fragment. We keep HTML rather than flattening to text because
        the decisions carry meaningful structure -- the parties table, the
        headings, the numbered findings -- and discarding it here would be
        lossy in a way we could not undo later.
    """
    soup = BeautifulSoup(html, "lxml")

    for tag_name in CHROME_TAGS:
        for element in soup.find_all(tag_name):
            element.decompose()

    # NB: find_all() materialises its result list up front, but decompose()
    # destroys the element AND all its descendants (BeautifulSoup >= 4.13
    # clears their attribute dicts entirely). Descendants of a decomposed
    # chrome element therefore still appear later in this loop as husks whose
    # ``attrs`` is gone -- touching them raised AttributeError on every real
    # page (caught in a live transform run over 893 documents; the offline
    # fixtures had no nested tags inside chrome elements). Skip them.
    for element in soup.find_all(True):
        if getattr(element, "decomposed", False):
            continue
        if _is_chrome(element):
            element.decompose()

    # Navigation links identified by their text ("Return to Search") -- on the
    # live site they carry no usable class, so class-based stripping misses
    # them (verified against a real decision page in review).
    for anchor in soup.find_all("a"):
        if getattr(anchor, "decomposed", False):
            continue
        label = anchor.get_text(strip=True).lower()
        if label in CHROME_LINK_TEXTS:
            anchor.decompose()

    for selector in CONTENT_SELECTORS:
        container = soup.select_one(selector)
        if container and len(container.get_text(strip=True)) >= min_length:
            return _tidy(container)

    # Fallback: the single element with the most text is almost always the
    # decision body once the chrome is gone.
    candidate = _densest_block(soup, min_length)
    if candidate is not None:
        logger.warning(
            "Fell back to text-density extraction",
            extra={"event": "content_selector_fallback"},
        )
        return _tidy(candidate)

    logger.error("Could not locate main content", extra={"event": "content_extraction_failed"})
    return ""


def extract_plain_text(html: str) -> str:
    """Whitespace-normalised text, for indexing and length checks."""
    soup = BeautifulSoup(html, "lxml")
    return " ".join(soup.get_text(separator=" ", strip=True).split())


# ------------------------------------------------------------------ internals
def _is_chrome(element: Tag) -> bool:
    # A decomposed element's ``attrs`` can be None (bs4 >= 4.13 destroys it);
    # read defensively so a husk can never crash the extraction.
    attrs = getattr(element, "attrs", None) or {}
    raw_classes = attrs.get("class")
    classes: list[str] = []
    if isinstance(raw_classes, str):  # bs4 may return a bare string for some attrs
        classes = [raw_classes]
    elif raw_classes:
        classes = [str(c) for c in raw_classes]
    element_id = attrs.get("id")
    parts = [*classes, element_id if isinstance(element_id, str) else ""]
    identifiers = " ".join(part for part in parts if part).lower()
    if not identifiers:
        return False
    return any(pattern in identifiers for pattern in CHROME_PATTERNS)


def _densest_block(soup: BeautifulSoup, min_length: int = MIN_CONTENT_LENGTH) -> Tag | None:
    """The deepest element carrying the bulk of the page's text."""
    best: Tag | None = None
    best_length = 0
    for element in soup.find_all(["div", "section", "article", "main", "td"]):
        length = len(element.get_text(strip=True))
        # Prefer deeper elements at equal length: a parent always contains at
        # least as much text as its child, so ties should resolve downward.
        if length > best_length:
            best, best_length = element, length
    return best if best_length >= min_length else None


def _tidy(container: Tag) -> str:
    """Collapse redundant whitespace without disturbing the markup.

    Boundary whitespace is *preserved as a single space*, not stripped: the
    text node after ``<b>Reference:</b>`` begins with a space that separates
    the label from the value, and dropping it fuses them into
    ``Reference:ADJ-...`` (caught against a real page in review).
    """
    for element in container.find_all(string=True):
        if element.strip() == "":
            continue
        lead = " " if element[:1].isspace() else ""
        trail = " " if element[-1:].isspace() else ""
        replacement = f"{lead}{' '.join(element.split())}{trail}"
        if replacement != element:
            element.replace_with(replacement)
    return str(container)
