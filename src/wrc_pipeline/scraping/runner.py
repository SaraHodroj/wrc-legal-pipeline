"""CLI entry point for the ingestion stage.

Kept separate from the orchestrator so the crawl can be run standalone -- which
matters for debugging a single bad partition without spinning up Dagster.
"""

from __future__ import annotations

import argparse
import sys
from datetime import date
from typing import Any

from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

from ..config import get_settings
from ..logging_setup import configure_logging, get_logger

logger = get_logger(__name__)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Ingest WRC decisions into the landing zone.")
    parser.add_argument("--start-date", type=date.fromisoformat, help="ISO date, inclusive")
    parser.add_argument("--end-date", type=date.fromisoformat, help="ISO date, exclusive")
    parser.add_argument(
        "--bodies",
        help="Pipe-separated body names, e.g. 'Labour Court|Equality Tribunal'",
    )
    parser.add_argument(
        "--partition-size",
        choices=["daily", "weekly", "monthly", "quarterly", "yearly"],
    )
    parser.add_argument("--run-id", help="Override the generated run id (for replays)")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    settings = get_settings()
    configure_logging(settings.run.log_level, settings.run.log_format)

    start = args.start_date or settings.run.start_date
    end = args.end_date or settings.run.end_date
    if start >= end:
        logger.error(
            "Invalid date window",
            extra={"event": "invalid_arguments", "start_date": start, "end_date": end},
        )
        return 2

    # Fail fast, with a readable error, if the storages are unreachable.
    # Without this the failure surfaces as a raw traceback from deep inside
    # the crawl (or worse, a crawl that silently does nothing).
    if not _preflight(settings):
        return 2

    process = CrawlerProcess(get_project_settings())
    crawler = process.create_crawler("decisions")
    process.crawl(
        crawler,
        start_date=start.isoformat(),
        end_date=end.isoformat(),
        bodies=args.bodies,
        partition_size=args.partition_size,
        run_id=args.run_id,
    )
    # Blocks until the crawl finishes. Note: Twisted's reactor cannot be
    # restarted within a process, which is why the orchestrator launches this
    # module as a subprocess per partition rather than calling it in-process.
    process.start()

    # A crawl that never sent a single request is an infrastructure or
    # configuration failure, not a quiet month -- even an empty partition
    # costs at least one search-page request. Exit non-zero so the
    # orchestrator retries instead of marking the partition green.
    stats = crawler.stats.get_stats() if crawler.stats else {}
    if not stats.get("downloader/request_count"):
        logger.error(
            "Crawl finished without sending a single request",
            extra={
                "event": "no_requests_made",
                "finish_reason": stats.get("finish_reason"),
                "hint": (
                    "Check network connectivity and that the installed Scrapy "
                    "version is supported (pip show scrapy)."
                ),
            },
        )
        return 1

    # The exit code reflects the reconciliation ledger, not vibes:
    #   0 -- every (body, partition) is COMPLETE;
    #   1 -- at least one partition FAILED outright (search never returned, or
    #        the source reported records we discovered none of) -- retryable;
    #   3 -- PARTIAL: some records failed and each one is logged/attributed
    #        (the brief's "N-X with X logged" case), distinct from success so
    #        an orchestrator or operator can decide to replay.
    run_stats = getattr(crawler.spider, "stats_model", None)
    status = run_stats.run_status if run_stats is not None else "failed"
    if status == "failed":
        return 1
    if status == "partial":
        return 3
    return 0


def _preflight(settings: Any) -> bool:
    """Verify Mongo and the object store are reachable before starting Twisted."""
    from ..storage.metadata_store import MetadataStore
    from ..storage.object_store import ObjectStore

    store = MetadataStore(settings.mongo)
    try:
        store.ping()
    except Exception as exc:
        logger.error(
            "Cannot reach MongoDB -- is `docker compose up -d mongo` running?",
            extra={"event": "preflight_failed", "target": "mongodb", "reason": str(exc)},
        )
        return False
    finally:
        store.close()

    objects = ObjectStore(settings.object_store)
    try:
        objects.ensure_bucket(settings.object_store.landing_bucket)
    except Exception as exc:
        logger.error(
            "Cannot reach object storage -- is `docker compose up -d minio` running?",
            extra={"event": "preflight_failed", "target": "object_store", "reason": str(exc)},
        )
        return False
    return True


if __name__ == "__main__":
    sys.exit(main())
