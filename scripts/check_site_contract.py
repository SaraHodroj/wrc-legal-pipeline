"""Live contract check: does the site still match what the adapter expects?

Run this before a real crawl (and any time the crawl starts returning zero
rows). It needs no Docker, no MongoDB, no MinIO -- just network access:

    python scripts/check_site_contract.py

It performs exactly two polite GET requests (robots.txt + one search page)
and verifies, against the real site, every assumption the pipeline makes:

  1. robots.txt permits the paths we crawl (/en/search/, /en/cases/...).
  2. The search URL the adapter builds returns HTTP 200.
  3. The adapter parses result rows out of the real markup.
  4. The "of N results" counter is readable.
  5. Pagination: a next-page link is found whenever the counter says there
     is more than one page.

Exit code 0 = the contract holds. Non-zero = the site changed (or you are
offline / blocked); the output says which assumption broke.
"""

from __future__ import annotations

import ssl
import sys
import urllib.robotparser
from datetime import date
from pathlib import Path
from urllib.request import Request, urlopen

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from wrc_pipeline.config import get_settings  # noqa: E402
from wrc_pipeline.scraping.search_adapter import WrcSearchAdapter  # noqa: E402

# macOS python.org installs ship without system CA certificates wired up, so a
# bare urlopen dies with CERTIFICATE_VERIFY_FAILED. Prefer certifi's bundle
# when available (it ships with this project's dependencies); otherwise fall
# back to the system default and, if that fails too, tell the user the fix.
try:
    import certifi

    SSL_CONTEXT = ssl.create_default_context(cafile=certifi.where())
except ImportError:  # pragma: no cover
    SSL_CONTEXT = ssl.create_default_context()

CERT_HINT = (
    "hint: on macOS run 'open \"/Applications/Python 3.13/Install "
    "Certificates.command\"' (adjust the version) or 'pip install certifi'."
)

# A window known to be busy: January 2024, Workplace Relations Commission.
BODY = "Workplace Relations Commission"
START, END = date(2024, 1, 1), date(2024, 1, 31)
SAMPLE_DETAIL_PATH = "/en/cases/2024/january/adj-00045701.html"


def fetch(url: str, user_agent: str) -> str:
    request = Request(url, headers={"User-Agent": user_agent})
    try:
        with urlopen(request, timeout=30, context=SSL_CONTEXT) as response:  # noqa: S310
            if response.status != 200:
                raise RuntimeError(f"HTTP {response.status} for {url}")
            return response.read().decode("utf-8", errors="replace")
    except OSError as exc:  # URLError subclasses OSError; SSL errors hide inside
        if "CERTIFICATE_VERIFY_FAILED" in str(exc):
            raise RuntimeError(f"{exc} -- {CERT_HINT}") from exc
        raise


def main() -> int:
    settings = get_settings()
    base = settings.scrape.base_url
    ua = settings.scrape.user_agent
    adapter = WrcSearchAdapter(base_url=base)
    failures: list[str] = []

    # 1. robots.txt --------------------------------------------------------
    parser = urllib.robotparser.RobotFileParser()
    try:
        parser.parse(fetch(f"{base}/robots.txt", ua).splitlines())
    except Exception as exc:
        print(f"FAIL robots.txt unreachable: {exc}")
        return 1
    for path in ("/en/search/", SAMPLE_DETAIL_PATH):
        allowed = parser.can_fetch(ua, f"{base}{path}")
        print(f"{'ok  ' if allowed else 'FAIL'} robots.txt allows {path}: {allowed}")
        if not allowed:
            failures.append(f"robots.txt disallows {path}")

    # 2. search URL --------------------------------------------------------
    url = adapter.build_search_url(BODY, START, END, page=1)
    print(f"ok   search URL built: {url}")
    try:
        html = fetch(url, ua)
    except Exception as exc:
        print(f"FAIL search page fetch: {exc}")
        return 1
    print(f"ok   search page fetched ({len(html):,} bytes)")

    # 3. row parsing -------------------------------------------------------
    rows = adapter.parse_rows(html, url)
    print(f"{'ok  ' if rows else 'FAIL'} rows parsed: {len(rows)}")
    if rows:
        sample = rows[0]
        print(f"     sample: {sample.identifier} | {sample.published_date} | "
              f"{(sample.description or '')[:60]}")
        print(f"     detail: {sample.detail_url}")
    else:
        failures.append("parse_rows returned no rows -- selectors need re-pinning")

    # 4. total counter -----------------------------------------------------
    total = adapter.total_results(html)
    print(f"{'ok  ' if total is not None else 'FAIL'} total counter: {total}")
    if total is None:
        failures.append("total_results counter not found")

    # 5. pagination --------------------------------------------------------
    next_url = adapter.next_page_url(html, url, current_page=1)
    multi_page = total is not None and rows and total > len(rows)
    if multi_page and not next_url:
        print(f"FAIL {total} results but no next-page link found")
        failures.append("next_page_url found nothing on a multi-page result set")
    else:
        print(f"ok   next page: {next_url or 'n/a (single page)'}")

    if failures:
        print("\nCONTRACT BROKEN:")
        for failure in failures:
            print(f"  - {failure}")
        return 1
    print("\nAll site-contract checks passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
