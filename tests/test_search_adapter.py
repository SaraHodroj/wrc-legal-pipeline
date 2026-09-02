"""Search adapter tests.

The adapter is the only module coupled to the site's markup, which makes these
tests the repo's early-warning system: when the site is redesigned, this file
is what fails, pointing at exactly the selectors to update.
"""

from datetime import date

import pytest

from wrc_pipeline.scraping.search_adapter import WrcSearchAdapter

ADAPTER = WrcSearchAdapter(base_url="https://www.workplacerelations.ie")

LISTING = """
<html><body>
  <p>Shows 1 to 10 of 234 results</p>
  <div>
    <div><h3>ADJ-00045701</h3><span>31/01/2024</span></div>
    <p>Nicholas Glynn V Health Service Executive</p>
    <div>Ref no: ADJ-00045701
      <a href="/en/cases/2024/january/adj-00045701.html">View Page</a></div>
  </div>
  <div>
    <div><h3>ADJ-00046159</h3><span>31/01/2024</span></div>
    <p>Jessica Davis V St. Vincent's Private Hospital</p>
    <div>Ref no: ADJ-00046159
      <a href="/en/cases/2024/january/adj-00046159.html">View Page</a></div>
  </div>
  <a href="/en/search/?decisions=1&amp;body=15376&amp;page=2">Next</a>
</body></html>
"""

BASE = "https://www.workplacerelations.ie/en/search/"


def test_parses_all_rows_without_any_css_classes():
    """The real page's classes were never confirmed, so the parser must not
    need them: rows are anchored purely on /en/cases/ links + text shape."""
    rows = ADAPTER.parse_rows(LISTING, BASE)
    assert [r.identifier for r in rows] == ["ADJ-00045701", "ADJ-00046159"]


def test_detail_urls_are_absolutised():
    rows = ADAPTER.parse_rows(LISTING, BASE)
    assert rows[0].detail_url == (
        "https://www.workplacerelations.ie/en/cases/2024/january/adj-00045701.html"
    )


def test_dates_are_parsed_from_site_format():
    rows = ADAPTER.parse_rows(LISTING, BASE)
    assert rows[0].published_date == date(2024, 1, 31)


def test_description_survives_furniture_removal():
    rows = ADAPTER.parse_rows(LISTING, BASE)
    assert rows[0].description == "Nicholas Glynn V Health Service Executive"


def test_total_results_reads_the_counter():
    assert ADAPTER.total_results(LISTING) == 234


def test_identifier_falls_back_to_url_slug():
    """A row with no 'Ref no:' text still keys correctly off its URL."""
    bare = '<div><a href="/en/cases/2024/march/udd-2412.html">View Page</a></div>'
    rows = ADAPTER.parse_rows(bare, BASE)
    assert len(rows) == 1
    assert rows[0].identifier == "UDD-2412"


def test_heading_link_and_view_page_link_do_not_duplicate_the_row():
    doubled = """
    <div>
      <h3><a href="/en/cases/2024/january/adj-00045701.html">ADJ-00045701</a></h3>
      <a href="/en/cases/2024/january/adj-00045701.html">View Page</a>
    </div>
    """
    assert len(ADAPTER.parse_rows(doubled, BASE)) == 1


def test_bare_cases_index_link_is_not_a_row():
    nav = '<nav><a href="/en/cases/">Decisions</a></nav>'
    assert ADAPTER.parse_rows(nav, BASE) == []


def test_legacy_table_layout_still_parses():
    """Older renderings of this site used a results table, not divs."""
    legacy = """
    <table><tbody>
      <tr><td>Ref no: DEC-E2002-001</td><td>Anne Harrington -v- East Coast Area Health Board</td>
          <td>15/03/2002</td><td><a href="/en/cases/dec-e2002-001.html">View</a></td></tr>
    </tbody></table>
    """
    rows = ADAPTER.parse_rows(legacy, BASE)
    assert len(rows) == 1
    assert rows[0].identifier == "DEC-E2002-001"
    assert rows[0].published_date == date(2002, 3, 15)


def test_next_page_url_follows_the_sites_own_link():
    url = ADAPTER.next_page_url(LISTING, BASE, current_page=1)
    assert url == (
        "https://www.workplacerelations.ie/en/search/?decisions=1&body=15376&page=2"
    )


def test_no_next_link_means_no_next_page():
    last_page = LISTING.replace(
        '<a href="/en/search/?decisions=1&amp;body=15376&amp;page=2">Next</a>', ""
    )
    assert ADAPTER.next_page_url(last_page, BASE, current_page=24) is None


def test_numbered_pagination_also_works():
    numbered = LISTING.replace(">Next</a>", ">2</a>")
    url = ADAPTER.next_page_url(numbered, BASE, current_page=1)
    assert url is not None and "page=2" in url


def test_search_url_matches_the_confirmed_live_request_shape():
    """Compared against a URL actually captured from the live site via
    DevTools: /en/search/?decisions=1&from=1/1/2024&to=31/1/2024&body=15376
    Comparing parsed query params rather than a raw string, since param
    order in urlencode() is an implementation detail, not part of the
    contract with the site.
    """
    from urllib.parse import parse_qs, urlparse

    url = ADAPTER.build_search_url(
        "Workplace Relations Commission", date(2024, 1, 1), date(2024, 1, 31), page=1
    )
    parsed = urlparse(url)
    assert parsed.scheme == "https"
    assert parsed.netloc == "www.workplacerelations.ie"
    assert parsed.path == "/en/search/"

    params = parse_qs(parsed.query)
    assert params == {
        "decisions": ["1"],
        "body": ["15376"],
        "from": ["1/1/2024"],
        "to": ["31/1/2024"],
    }


def test_search_url_dates_are_not_zero_padded():
    """The live site sent '1/1/2024', not '01/01/2024' -- strftime would get
    this wrong silently, which is why dates are hand-formatted."""
    url = ADAPTER.build_search_url(
        "Labour Court", date(2024, 1, 1), date(2024, 1, 31), page=1
    )
    assert "from=1%2F1%2F2024" in url  # urlencoded "1/1/2024"
    assert "to=31%2F1%2F2024" in url  # urlencoded "31/1/2024"
    assert "from=01" not in url


def test_search_url_uses_numeric_body_id_not_name():
    labour_court = ADAPTER.build_search_url(
        "Labour Court", date(2024, 1, 1), date(2024, 1, 31), page=1
    )
    equality = ADAPTER.build_search_url(
        "Equality Tribunal", date(2024, 1, 1), date(2024, 1, 31), page=1
    )
    wrc = ADAPTER.build_search_url(
        "Workplace Relations Commission", date(2024, 1, 1), date(2024, 1, 31), page=1
    )
    assert "body=3" in labour_court
    assert "body=1" in equality
    assert "body=15376" in wrc


def test_search_url_omits_page_param_on_first_page():
    """The confirmed URL carries no page param at all for page 1."""
    url = ADAPTER.build_search_url(
        "Labour Court", date(2024, 1, 1), date(2024, 1, 31), page=1
    )
    assert "page=" not in url


def test_search_url_includes_page_param_when_paginating():
    url = ADAPTER.build_search_url(
        "Labour Court", date(2024, 1, 1), date(2024, 1, 31), page=2
    )
    assert "pageNumber=2" in url  # param name confirmed from the site's own pager link


def test_unknown_body_name_raises_immediately():
    with pytest.raises(ValueError, match="Unknown body"):
        ADAPTER.build_search_url("Not A Real Body", date(2024, 1, 1), date(2024, 1, 31), 1)


