"""Scrapy settings, derived from the typed config rather than hardcoded.

Everything here that could reasonably differ between environments is read from
``get_settings()``. The Scrapy settings module is a global, so this is the one
place where we bridge our config object into the framework's expectations.
"""

from __future__ import annotations

from ..config import get_settings

_s = get_settings()

BOT_NAME = "wrc_pipeline"
SPIDER_MODULES = ["wrc_pipeline.scraping.spiders"]
NEWSPIDER_MODULE = "wrc_pipeline.scraping.spiders"

# --- Politeness -------------------------------------------------------------
# We identify ourselves honestly and obey robots.txt. Beyond being the correct
# default for any crawler, it matters commercially here: the output feeds a
# legal product, and provenance that cannot survive scrutiny is worthless.
USER_AGENT = _s.scrape.user_agent
ROBOTSTXT_OBEY = _s.scrape.obey_robots_txt

CONCURRENT_REQUESTS = _s.scrape.concurrent_requests
CONCURRENT_REQUESTS_PER_DOMAIN = _s.scrape.concurrent_requests_per_domain
DOWNLOAD_DELAY = _s.scrape.download_delay_seconds

# AutoThrottle targets a *latency*, not a fixed rate: it speeds up while the
# server responds quickly and backs off as soon as it slows down. That is both
# faster overall and gentler than a hardcoded sleep.
AUTOTHROTTLE_ENABLED = _s.scrape.autothrottle_enabled
AUTOTHROTTLE_START_DELAY = _s.scrape.download_delay_seconds
AUTOTHROTTLE_MAX_DELAY = _s.scrape.autothrottle_max_delay_seconds
AUTOTHROTTLE_TARGET_CONCURRENCY = _s.scrape.autothrottle_target_concurrency
AUTOTHROTTLE_DEBUG = False

# --- Reliability ------------------------------------------------------------
RETRY_ENABLED = True
# Scrapy semantics: RETRY_TIMES counts retries AFTER the initial attempt, so
# 4 here means up to 5 total attempts per request.
RETRY_TIMES = _s.scrape.retry_times
RETRY_HTTP_CODES = list(_s.scrape.retry_http_codes)
DOWNLOAD_TIMEOUT = _s.scrape.download_timeout_seconds

# Retries are DEPRIORITISED, not delayed: Scrapy's RetryMiddleware does not
# read the Retry-After header. The effective backoff comes from AutoThrottle,
# which widens the delay for every request as soon as the server slows down or
# errors -- honest description, since claiming Retry-After support here would
# be wrong.
RETRY_PRIORITY_ADJUST = -1

# Detail pages are HTML; documents can be many MB. Cap the in-memory response
# size so one pathological file cannot exhaust the worker. Configurable, like
# every other tunable.
DOWNLOAD_MAXSIZE = _s.scrape.download_maxsize_bytes
DOWNLOAD_WARNSIZE = _s.scrape.download_warnsize_bytes

# Development convenience, off by default: cache responses on disk so selector
# iteration doesn't re-hit the source. Enable with SCRAPE_HTTPCACHE_ENABLED=true.
HTTPCACHE_ENABLED = _s.scrape.httpcache_enabled
HTTPCACHE_EXPIRATION_SECS = 86400
HTTPCACHE_DIR = "httpcache"
HTTPCACHE_IGNORE_HTTP_CODES = [429, 500, 502, 503, 504]

# --- Pipelines --------------------------------------------------------------
ITEM_PIPELINES = {
    "wrc_pipeline.scraping.pipelines.PersistencePipeline": 300,
}

# --- Misc -------------------------------------------------------------------
# Scrapy's own log config would fight our JSON formatter; we configure the root
# logger ourselves in the runner and let Scrapy inherit it.
LOG_ENABLED = False
TELNETCONSOLE_ENABLED = False
REQUEST_FINGERPRINTER_IMPLEMENTATION = "2.7"
TWISTED_REACTOR = "twisted.internet.asyncioreactor.AsyncioSelectorReactor"
FEED_EXPORT_ENCODING = "utf-8"
