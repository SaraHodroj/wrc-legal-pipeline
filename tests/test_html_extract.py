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
