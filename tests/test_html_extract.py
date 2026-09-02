"""Content extraction decides what a downstream search index or LLM actually
sees. Boilerplate leaking through is a silent quality failure, not a crash."""

from wrc_pipeline.transform.html_extract import extract_main_content, extract_plain_text

DECISION_BODY = (
    "ADJUDICATION OFFICER Recommendation on dispute under Industrial Relations Act 1969. "
    "The Complainant submitted that the Respondent failed to apply the agreed procedure. "
    "Having considered the written and oral submissions of both parties, I find that the "
    "dispute is well founded and recommend that the Respondent pay compensation."
)

PAGE = f"""
<html>
  <head><title>IR-SC-00001595</title><style>body {{ color: red; }}</style></head>
  <body>
    <div class="cookie-banner">We use cookies. I accept cookies from this site.</div>
    <header class="site-header"><a href="/">WRC</a></header>
    <nav class="navigation"><ul><li>Cases</li><li>Contact us</li></ul></nav>
    <main id="main">
      <h1>IR - SC - 00001595</h1>
      <p>{DECISION_BODY}</p>
    </main>
    <footer class="site-footer">Accessibility | Data Protection | Sitemap</footer>
    <script>trackPageView();</script>
  </body>
</html>
"""


def test_chrome_is_stripped():
    cleaned = extract_main_content(PAGE)
    for boilerplate in ("cookie", "Sitemap", "trackPageView", "color: red"):
        assert boilerplate not in cleaned


def test_decision_text_survives():
    cleaned = extract_main_content(PAGE)
    assert "Industrial Relations Act 1969" in cleaned
    assert "recommend that the Respondent pay compensation" in cleaned


def test_structure_is_preserved():
    """We keep HTML rather than flattening: the headings and tables carry meaning."""
    assert "<h1" in extract_main_content(PAGE)


def test_falls_back_when_no_known_container_matches():
    """Older pages on this site predate the current markup entirely."""
    legacy = f"<html><body><table><tr><td>{DECISION_BODY}</td></tr></table></body></html>"
    cleaned = extract_main_content(legacy)
    assert "dispute is well founded" in cleaned


def test_plain_text_normalises_whitespace():
    text = extract_plain_text("<p>a   b\n\n\tc</p>")
    assert text == "a b c"


def test_nested_tags_inside_chrome_do_not_crash_extraction():
    """Live-run regression (893/893 records failed): BeautifulSoup >= 4.13's
    decompose() destroys descendants' attribute dicts, and descendants of a
    decomposed chrome element still appear later in the find_all sweep as
    husks. Real pages always nest tags inside their cookie banner; the
    original fixtures did not, so the suite never caught it."""
    from wrc_pipeline.transform.html_extract import extract_main_content

    body = "The adjudication officer finds the complaint well founded. " * 10
    html = f"""
    <html><body>
      <div class="cookie-banner">
        <p>We use <a href="/cookies">cookies</a> to <b>improve</b> things.</p>
        <span id="cookie-accept"><button>Accept</button></span>
      </div>
      <main id="main"><h1>ADJ-00000001</h1><p>{body}</p></main>
    </body></html>
    """
    result = extract_main_content(html)
    assert "well founded" in result
    assert "cookie" not in result.lower()


def test_identifier_normalisation_collapses_unicode_dashes():
    """Live-run regression: one slug used a percent-encoded en dash, producing
    the identifier 'IR-SC – 00001494' alongside its ASCII sibling."""
    from datetime import date

    from wrc_pipeline.models import DecisionRecord

    record = DecisionRecord(
        identifier="IR-SC – 00001494",
        body="Workplace Relations Commission",
        source_url="https://example.ie/x",
        partition_date=date(2024, 3, 1),
        run_id="t",
    )
    assert record.identifier == "IR-SC-00001494"


def test_return_to_search_navigation_is_stripped():
    """Review regression: the live page's 'Return to Search' link carries no
    class, so class-based chrome stripping missed it."""
    from wrc_pipeline.transform.html_extract import extract_main_content

    body = "The complaint is well founded. " * 20
    html = f"""
    <html><body><div class="content">
      <a href="/en/search/?advance=true">Return to Search</a>
      <h1>ADJ-00000001</h1><p>{body}</p>
    </div></body></html>
    """
    result = extract_main_content(html)
    assert "Return to Search" not in result
    assert "well founded" in result


def test_inline_tag_boundary_whitespace_is_preserved():
    """Review regression: collapsing whitespace stripped the space after
    '<b>Reference:</b>', fusing label and value into 'Reference:ADJ-...'."""
    from wrc_pipeline.transform.html_extract import extract_main_content, extract_plain_text

    body = "Filler decision text to clear the minimum length. " * 10
    html = f"""
    <html><body><div class="content">
      <p><b>Adjudication Reference:</b> ADJ-00045701</p><p>{body}</p>
    </div></body></html>
    """
    text = extract_plain_text(extract_main_content(html))
    assert "Reference: ADJ-00045701" in text
    assert "Reference:ADJ" not in text
