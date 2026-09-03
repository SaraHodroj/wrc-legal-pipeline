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


# Decisions are published continuously, so we rerun the current month nightly
# rather than assuming a month is final when it ends.
# Idempotency makes this safe: an unchanged nightly rerun writes nothing.
# The job is partitioned, so the schedule must name a partition explicitly.
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
