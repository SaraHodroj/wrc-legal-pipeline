"""Dagster orchestration.

Two partitioned assets with an explicit dependency:

    landing_decisions  ->  curated_decisions

Why Dagster rather than Airflow here
------------------------------------
Airflow's unit of work is a *task*; Dagster's is an *asset* -- a named piece of
data with a partition key. That maps directly onto this problem, where the
whole design is "the January 2024 slice of the landing zone exists and is
current". Concretely it buys us three things Airflow would need custom work to
match: partition status is visible per month in the UI, a backfill of 120 months
is a first-class operation rather than a hand-rolled loop, and
``curated_decisions`` inherits its partitioning from ``landing_decisions``, so
Dagster will not let the transform run for a month that was never ingested.

The one thing that genuinely constrains the implementation
----------------------------------------------------------
Scrapy runs on Twisted, whose reactor **cannot be restarted inside a process**.
Calling ``CrawlerProcess.start()`` a second time in the same interpreter raises
``ReactorNotRestartable``. Since Dagster reuses its process across materialisations,
running the spider in-process would work for exactly one partition and then fail
for every subsequent one.

So each partition is launched as a subprocess. That is not a workaround -- it is
the right isolation boundary anyway: a crawl that leaks memory or segfaults on a
malformed PDF takes down one partition, not the whole run.

Note the deliberate absence of ``from __future__ import annotations`` in this
module: PEP 563 turns every annotation into a string, and Dagster resolves the
``context`` parameter's annotation *by identity at runtime*, so stringified
annotations make it reject a perfectly valid ``AssetExecutionContext``. Caught
by the definitions smoke test.
"""

import subprocess
import sys
from datetime import date, datetime, timedelta

from dagster import (
    AssetExecutionContext,
    Definitions,
    MonthlyPartitionsDefinition,
    RetryPolicy,
    ScheduleDefinition,
    asset,
    define_asset_job,
)

from ..config import get_settings
from ..transform.job import transform_window

settings = get_settings()

# Partition definition drives both the UI and the backfill machinery.
# Start date is the earliest period we intend to hold; extend it to widen the
# historical backfill without touching any other code.
monthly_partitions = MonthlyPartitionsDefinition(start_date="2015-10-01")

# Transient network failures are the normal case for a crawler, not the
# exception. Exponential backoff on the partition as a whole complements the
# per-request retries Scrapy already performs.
CRAWL_RETRY = RetryPolicy(max_retries=2, delay=60, backoff=None)


def _partition_window(context: AssetExecutionContext) -> tuple[date, date]:
    """Resolve the partition key into an ``[start, end)`` window."""
    start = datetime.strptime(context.partition_key, "%Y-%m-%d").date()
    # First day of the following month; end is exclusive for the crawler and
    # inclusive-1 for the transform, matching iter_partitions' contract.
    next_month = (start.replace(day=28) + timedelta(days=4)).replace(day=1)
    return start, next_month


@asset(
    partitions_def=monthly_partitions,
    retry_policy=CRAWL_RETRY,
    group_name="ingestion",
    description="Raw documents + metadata for one month, written to the landing zone.",
)
def landing_decisions(context: AssetExecutionContext) -> dict:
    """Run the Scrapy crawl for a single monthly partition, out of process."""
    start, end = _partition_window(context)
    run_id = f"dagster-{context.run_id[:8]}-{context.partition_key}"

    command = [
        sys.executable,
        "-m",
        "wrc_pipeline.scraping.runner",
        "--start-date", start.isoformat(),
        "--end-date", end.isoformat(),
        "--partition-size", "monthly",
        "--run-id", run_id,
    ]
    context.log.info("Launching crawl subprocess: %s", " ".join(command))

    completed = subprocess.run(
        command,
        capture_output=True,
        text=True,
        check=False,
        # A single month should never take this long; if it does, something is
        # wrong upstream and we would rather fail loudly than hang the backfill.
        timeout=60 * 60,
    )

    # The child writes JSONL to stdout; surface it in the Dagster run log so
    # the structured records are not lost behind a process boundary.
    for line in completed.stdout.splitlines():
        context.log.info(line)
    if completed.returncode != 0:
        context.log.error(completed.stderr)
        raise RuntimeError(
            f"Crawl failed for partition {context.partition_key} "
            f"(exit code {completed.returncode})"
        )

    return {"partition": context.partition_key, "run_id": run_id}


@asset(
    partitions_def=monthly_partitions,
    deps=[landing_decisions],
    group_name="transformation",
    description="Cleaned documents + metadata for one month, written to the curated zone.",
)
def curated_decisions(context: AssetExecutionContext) -> dict:
    """Transform one month of landing data.

    Runs in-process: unlike the crawl there is no Twisted reactor involved, and
    the work is I/O-bound against Mongo and object storage rather than against
    a third-party site we need to isolate ourselves from.
    """
    start, end = _partition_window(context)
    summary = transform_window(start=start, end=end - timedelta(days=1))
    context.log.info("Transform summary: %s", summary)

    if summary["failed"]:
        context.log.warning(
            "%s record(s) failed to transform in partition %s",
            summary["failed"],
            context.partition_key,
        )
    return summary


# Partitioning is inferred from the selected assets; passing partitions_def
# here is deprecated in Dagster 1.x and will be removed in 2.0.
ingest_and_transform = define_asset_job(
    name="ingest_and_transform",
    selection=[landing_decisions, curated_decisions],
    description="Ingest a month of decisions, then curate it.",
)

# Decisions are published continuously, so we re-run the current month nightly
# rather than assuming a month is final the moment it ends. Idempotency is what
# makes that safe: a nightly re-run of an unchanged month writes nothing.
daily_refresh = ScheduleDefinition(
    name="daily_current_month_refresh",
    job=ingest_and_transform,
    cron_schedule="0 2 * * *",
    execution_timezone="Europe/Dublin",
)

defs = Definitions(
    assets=[landing_decisions, curated_decisions],
    jobs=[ingest_and_transform],
    schedules=[daily_refresh],
)
