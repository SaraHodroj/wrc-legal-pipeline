"""Tests for the runtime glue: logging, CLI parsing, the transform batch loop,
and the orchestration definitions.

Glue code fails differently from logic code -- not with wrong answers but with
a run that dies at startup or a summary that lies. These tests target exactly
those modes.
"""

import json
import logging
from datetime import UTC, date, datetime

import mongomock
import pytest
from moto import mock_aws

from wrc_pipeline.config import MongoSettings, ObjectStoreSettings, get_settings
from wrc_pipeline.hashing import hash_bytes
from wrc_pipeline.logging_setup import JsonFormatter
from wrc_pipeline.scraping.runner import build_parser
from wrc_pipeline.storage.metadata_store import MetadataStore
from wrc_pipeline.storage.object_store import ObjectStore
from wrc_pipeline.transform import job as transform_job


# -------------------------------------------------------------------- logging
def make_record(**extra) -> logging.LogRecord:
    record = logging.LogRecord(
        name="test", level=logging.INFO, pathname=__file__, lineno=1,
        msg="hello %s", args=("world",), exc_info=None,
    )
    for key, value in extra.items():
        setattr(record, key, value)
    return record


def test_log_line_is_valid_json_with_interpolated_message():
    line = JsonFormatter().format(make_record())
    payload = json.loads(line)
    assert payload["message"] == "hello world"
    assert payload["level"] == "INFO"


def test_extra_fields_survive_into_the_json():
    line = JsonFormatter().format(
        make_record(event="record_persisted", partition="2024-01-01", records_found=42)
    )
    payload = json.loads(line)
    assert payload["event"] == "record_persisted"
    assert payload["records_found"] == 42


def test_non_serialisable_extras_do_not_crash_the_formatter():
    """A log call must never take down the pipeline it is reporting on."""
    line = JsonFormatter().format(make_record(when=date(2024, 1, 1)))
    assert json.loads(line)["when"] == "2024-01-01"


def test_exceptions_are_captured():
    try:
        raise ValueError("boom")
    except ValueError:
        import sys
        record = make_record()
        record.exc_info = sys.exc_info()
    payload = json.loads(JsonFormatter().format(record))
    assert "ValueError: boom" in payload["exception"]


# ------------------------------------------------------------------------ CLI
def test_cli_parses_iso_dates():
    args = build_parser().parse_args(["--start-date", "2024-01-01", "--end-date", "2024-06-01"])
    assert args.start_date == date(2024, 1, 1)
    assert args.end_date == date(2024, 6, 1)


def test_cli_rejects_malformed_dates():
    with pytest.raises(SystemExit):
        build_parser().parse_args(["--start-date", "01/01/2024"])


def test_cli_accepts_body_filter_and_partition_size():
    args = build_parser().parse_args(
        ["--bodies", "Labour Court|Equality Tribunal", "--partition-size", "weekly"]
    )
    assert args.bodies == "Labour Court|Equality Tribunal"
    assert args.partition_size == "weekly"


def test_cli_rejects_unknown_partition_size():
    with pytest.raises(SystemExit):
        build_parser().parse_args(["--partition-size", "hourly"])


# ------------------------------------------------------- transform batch loop
HTML_PAGE = (
    '<html><body><main id="main"><h1>ADJ-1</h1>'
    "<p>ADJUDICATION OFFICER Decision under the Industrial Relations Act 1969. "
    "The Complainant submitted that the Respondent failed to apply the agreed "
    "grievance procedure prior to the disciplinary sanction. Having considered "
    "the written and oral submissions of both parties, I find the complaint is "
    "well founded and recommend that the Respondent pay compensation "
    "accordingly.</p></main></body></html>"
)


@pytest.fixture()
def wired_transform(monkeypatch):
    """Run transform_window against in-memory Mongo + S3."""
    with mock_aws():
        settings = get_settings()

        metadata = MetadataStore(MongoSettings(uri="mongodb://localhost", database="t"))
        metadata._client = mongomock.MongoClient()
        # transform_window closes its store on exit (correct in production);
        # the lazy reconnect would then hit a real localhost Mongo instead of
        # the mock. Pin the double open for the lifetime of the test.
        metadata.close = lambda: None  # type: ignore[method-assign]
        objects = ObjectStore(
            ObjectStoreSettings(endpoint_url=None, access_key="t", secret_key="t")
        )
        objects.ensure_bucket(settings.object_store.landing_bucket)
        objects.ensure_bucket(settings.object_store.curated_bucket)

        monkeypatch.setattr(transform_job, "MetadataStore", lambda _s: metadata)
        monkeypatch.setattr(transform_job, "ObjectStore", _patched_object_store(objects))
        yield metadata, objects, settings


def _patched_object_store(instance):
    class Factory:
        parse_uri = staticmethod(ObjectStore.parse_uri)

        def __new__(cls, _settings):
            return instance

    return Factory


def _seed_landing(metadata, objects, settings, identifier: str, payload: bytes, doc_type: str):
    key = f"wrc/2025-07-01/{identifier}.{doc_type}"
    uri = objects.put_bytes(settings.object_store.landing_bucket, key, payload)
    metadata.landing.insert_one(
        {
            "identifier": identifier,
            "body": "Workplace Relations Commission",
            "document_type": doc_type,
            "partition_date": datetime(2025, 7, 1, tzinfo=UTC),
            "storage_path": uri,
            "file_hash": hash_bytes(payload),
            "source_url": "https://example.ie/x",
            "document_url": "https://example.ie/x",
        }
    )


def test_transform_window_processes_the_batch(wired_transform):
    metadata, objects, settings = wired_transform
    _seed_landing(metadata, objects, settings, "ADJ-1", HTML_PAGE.encode(), "html")
    _seed_landing(metadata, objects, settings, "LCR-2", b"%PDF-1.4 bytes", "pdf")

    summary = transform_job.transform_window(date(2025, 7, 1), date(2025, 7, 31))

    assert summary["records_found"] == 2
    assert summary["transformed"] == 1
    assert summary["passthrough"] == 1
    assert summary["failed"] == 0
    assert metadata.curated.count_documents({}) == 2


def test_one_bad_record_does_not_kill_the_batch(wired_transform):
    """The failure contract: process everything, count and attribute the losses."""
    metadata, objects, settings = wired_transform
    _seed_landing(metadata, objects, settings, "ADJ-1", HTML_PAGE.encode(), "html")
    _seed_landing(metadata, objects, settings, "BAD-1", b"<html></html>", "html")  # empty body

    summary = transform_job.transform_window(date(2025, 7, 1), date(2025, 7, 31))

    assert summary["failed"] == 1
    assert summary["transformed"] == 1
    assert summary["failure_sample"][0]["identifier"] == "BAD-1"
    assert metadata.curated.count_documents({}) == 1


def test_rerun_skips_unchanged_curated_output(wired_transform):
    metadata, objects, settings = wired_transform
    _seed_landing(metadata, objects, settings, "ADJ-1", HTML_PAGE.encode(), "html")

    first = transform_job.transform_window(date(2025, 7, 1), date(2025, 7, 31))
    second = transform_job.transform_window(date(2025, 7, 1), date(2025, 7, 31))

    assert first["transformed"] == 1
    assert second["skipped_unchanged"] == 1
    assert second["transformed"] == 0


# ------------------------------------------------------------- orchestration
def test_dagster_definitions_load_and_wire_the_dependency():
    """Smoke test: the asset graph must at least build.

    A typo in the Dagster wiring otherwise only surfaces when someone opens the
    UI. Loading Definitions validates asset names, partition defs and deps.
    """
    pytest.importorskip("dagster")
    from wrc_pipeline.orchestration.definitions import defs

    graph = {a.key.to_user_string() for a in defs.assets}
    assert graph == {"landing_decisions", "curated_decisions"}

    curated = next(a for a in defs.assets if a.key.to_user_string() == "curated_decisions")
    dep_names = {k.to_user_string() for k in curated.dependency_keys}
    assert "landing_decisions" in dep_names
    assert defs.resolve_job_def("ingest_and_transform") is not None


# --------------------------------------------------------------- config bug
def test_pipe_separated_bodies_env_var_parses_without_json_error(tmp_path, monkeypatch):
    """Regression test for a real bug: pydantic-settings attempts to
    JSON-decode any 'complex' field type (tuple, list, dict) read from an env
    var *before* custom validators run. SCRAPE_BODIES uses '|' as a separator
    (not JSON), which raised SettingsError: 'Expecting value: line 1 column 1'
    -- the JSON decoder choked before _split_bodies ever got the string.

    Fixed by annotating the field with pydantic_settings.NoDecode, which
    tells the settings source to pass the raw string through untouched and
    let the field_validator(mode="before") do the splitting instead.
    """
    env_file = tmp_path / ".env"
    env_file.write_text(
        "SCRAPE_BODIES=Workplace Relations Commission|Labour Court|"
        "Equality Tribunal|Employment Appeals Tribunal\n"
    )
    monkeypatch.chdir(tmp_path)

    from wrc_pipeline.config import ScrapeSettings

    settings = ScrapeSettings(_env_file=str(env_file))  # type: ignore[call-arg]
    assert settings.bodies == (
        "Workplace Relations Commission",
        "Labour Court",
        "Equality Tribunal",
        "Employment Appeals Tribunal",
    )


def test_unknown_body_in_env_var_fails_at_startup_not_mid_crawl(tmp_path, monkeypatch):
    """The config-level validator that cross-checks against BODY_IDS."""
    env_file = tmp_path / ".env"
    env_file.write_text("SCRAPE_BODIES=Not A Real Body\n")
    monkeypatch.chdir(tmp_path)

    from pydantic import ValidationError

    from wrc_pipeline.config import ScrapeSettings

    with pytest.raises(ValidationError, match="Unknown body"):
        ScrapeSettings(_env_file=str(env_file))  # type: ignore[call-arg]


def test_daily_schedule_targets_the_current_month_partition():
    """Review regression: a bare ScheduleDefinition on a partitioned job emits
    a RunRequest with no partition_key and cannot refresh the current month.
    The schedule must resolve the month the tick falls in."""
    from datetime import UTC, datetime

    from dagster import RunRequest, build_schedule_context

    from wrc_pipeline.orchestration.definitions import daily_refresh

    context = build_schedule_context(
        scheduled_execution_time=datetime(2024, 3, 15, 2, 0, tzinfo=UTC)
    )
    request = daily_refresh(context)
    assert isinstance(request, RunRequest)
    assert request.partition_key == "2024-03-01"


def test_partial_transform_fails_the_curated_materialization(monkeypatch):
    """A month with failed transforms must not present as a healthy asset."""
    import pytest as _pytest

    from wrc_pipeline.orchestration import definitions as defs_mod

    monkeypatch.setattr(
        defs_mod,
        "transform_window",
        lambda **_kwargs: {"failed": 3, "transformed": 7},
    )

    from dagster import build_asset_context

    context = build_asset_context(partition_key="2024-03-01")
    with _pytest.raises(RuntimeError, match="3 record"):
        defs_mod.curated_decisions(context)
