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
    RunRequest,
    ScheduleEvaluationContext,
    asset,
    define_asset_job,
    schedule,
)

from ..config import get_settings
from ..transform.job import transform_window

settings = get_settings()

# Partition definition drives both the UI and the backfill machinery.
# Start date is config (ORCH_PARTITION_START_DATE): widen the historical
# backfill without touching code.
monthly_partitions = MonthlyPartitionsDefinition(
    start_date=settings.orchestration.partition_start_date
)

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
    # The weekly amendment sweep launches this same asset with a run tag; the
    # flag re-fetches KNOWN records so silently amended decisions get
    # hash-compared (default runs never re-download known records).
    if context.run.tags.get("recheck_known") == "true":
        command.append("--recheck-known")
    context.log.info("Launching crawl subprocess: %s", " ".join(command))

    completed = subprocess.run(
        command,
        capture_output=True,
        text=True,
        check=False,
        # A single month should never take longer than this configurable
        # ceiling; if it does, fail loudly rather than hang the backfill.
        timeout=settings.orchestration.crawl_timeout_seconds,
    )

    # The child writes JSONL to stdout; surface it in the Dagster run log so
    # the structured records are not lost behind a process boundary.
    for line in completed.stdout.splitlines():
        context.log.info(line)
    # Runner exit codes: 0 complete, 3 partial (every miss logged/attributed),
    # anything else failed. Partial and failed both fail the materialization --
    # an asset that silently accepted missing records would defeat the ledger.
    if completed.returncode != 0:
        context.log.error(completed.stderr)
        label = "partial" if completed.returncode == 3 else "failed"
        raise RuntimeError(
            f"Crawl {label} for partition {context.partition_key} "
            f"(exit code {completed.returncode}); see the run_summary log line "
            "for the per-partition reconciliation."
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
        # A partially-transformed month must not present as a healthy asset:
        # every failure is already logged per-record, so fail the
        # materialization and let a retry (or a fix) produce a complete one.
        raise RuntimeError(
            f"{summary['failed']} record(s) failed to transform in partition "
            f"{context.partition_key}; see transform_record_failed log lines."
        )
    return summary


# Partitioning is inferred from the selected assets; passing partitions_def
# here is deprecated in Dagster 1.x and will be removed in 2.0.
ingest_and_transform = define_asset_job(
    name="ingest_and_transform",
    selection=[landing_decisions, curated_decisions],
    description="Ingest a month of decisions, then curate it.",
)


# Decisions are published continuously, so we re-run the CURRENT month nightly
# rather than assuming a month is final the moment it ends. Idempotency is what
# makes that safe: a nightly re-run of an unchanged month writes nothing.
# The job is partitioned, so the schedule must name a partition explicitly --
# a bare ScheduleDefinition would emit a RunRequest with no partition_key and
# could not target the current month (caught in review; pinned by a test).
@schedule(
    name="daily_current_month_refresh",
    job=ingest_and_transform,
    cron_schedule=settings.orchestration.refresh_cron,
    execution_timezone=settings.orchestration.timezone,
)
def daily_refresh(context: ScheduleEvaluationContext) -> RunRequest:
    """Materialize the month the scheduled tick falls in."""
    today = context.scheduled_execution_time.date()
    partition_key = today.replace(day=1).isoformat()
    return RunRequest(
        run_key=f"refresh-{partition_key}-{today.isoformat()}",
        partition_key=partition_key,
    )


@schedule(
    name="weekly_amendment_sweep",
    job=ingest_and_transform,
    cron_schedule=settings.orchestration.sweep_cron,
    execution_timezone=settings.orchestration.timezone,
)
def weekly_sweep(context: ScheduleEvaluationContext) -> RunRequest:
    """Re-check KNOWN records of the current month for silent amendments.

    Unlike the nightly refresh (which only picks up NEW records and never
    re-downloads known ones), this run carries the ``recheck_known`` tag, so
    the crawl subprocess gets ``--recheck-known``: every known record is
    re-fetched and hash-compared, and an amended decision lands as a new
    immutable version. Sweep older windows by launching a backfill of this
    job with the same tag.
    """
    today = context.scheduled_execution_time.date()
    partition_key = today.replace(day=1).isoformat()
    return RunRequest(
        run_key=f"sweep-{partition_key}-{today.isoformat()}",
        partition_key=partition_key,
        tags={"recheck_known": "true"},
    )

defs = Definitions(
    assets=[landing_decisions, curated_decisions],
    jobs=[ingest_and_transform],
    schedules=[daily_refresh, weekly_sweep],
)
