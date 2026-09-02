"""Object store tests against moto's in-memory S3.

These prove the storage semantics the pipeline relies on -- existence checks,
round-tripping bytes, URI parsing -- without a running MinIO.
"""

import pytest
from moto import mock_aws

from wrc_pipeline.config import ObjectStoreSettings
from wrc_pipeline.storage.object_store import ObjectStore


@pytest.fixture()
def store():
    with mock_aws():
        settings = ObjectStoreSettings(
            endpoint_url=None,  # moto intercepts the default AWS endpoint
            access_key="testing",
            secret_key="testing",
        )
        s = ObjectStore(settings)
        s.ensure_bucket("landing-zone")
        yield s


def test_put_returns_canonical_uri(store):
    uri = store.put_bytes("landing-zone", "labour-court/2024-01-01/LCR-1.pdf", b"%PDF-1.4")
    assert uri == "s3://landing-zone/labour-court/2024-01-01/LCR-1.pdf"


def test_bytes_round_trip_exactly(store):
    payload = b"\x00\x01binary bytes \xff"
    store.put_bytes("landing-zone", "k", payload)
    assert store.get_bytes("landing-zone", "k") == payload


def test_exists_is_true_after_put_false_before(store):
    assert store.exists("landing-zone", "nope") is False
    store.put_bytes("landing-zone", "yep", b"x")
    assert store.exists("landing-zone", "yep") is True


def test_overwrite_replaces_rather_than_duplicates(store):
    """Deterministic keys mean re-runs must overwrite in place."""
    store.put_bytes("landing-zone", "k", b"v1")
    store.put_bytes("landing-zone", "k", b"v2")
    assert store.get_bytes("landing-zone", "k") == b"v2"


def test_object_metadata_is_attached(store):
    store.put_bytes(
        "landing-zone", "k", b"x", metadata={"identifier": "ADJ-1", "file-hash": "abc"}
    )
    head = store.client.head_object(Bucket="landing-zone", Key="k")
    assert head["Metadata"]["identifier"] == "ADJ-1"


def test_ensure_bucket_is_idempotent(store):
    store.ensure_bucket("landing-zone")  # second call must not raise
    store.ensure_bucket("landing-zone")


def test_parse_uri_round_trips():
    assert ObjectStore.parse_uri("s3://bucket/a/b/c.pdf") == ("bucket", "a/b/c.pdf")


@pytest.mark.parametrize("bad", ["http://x/y", "s3://only-bucket", "s3:///no-bucket", ""])
def test_parse_uri_rejects_malformed(bad):
    with pytest.raises(ValueError):
        ObjectStore.parse_uri(bad)


# ---------------------------------------------------- the real 403 bug, live
def test_ensure_bucket_creates_on_403_not_just_404(monkeypatch):
    """Regression test for a real bug hit while running the pipeline live
    against actual MinIO: HeadBucket returned 403 Forbidden for a bucket that
    genuinely didn't exist yet (MinIO's security-through-obscurity behavior,
    not a real permissions problem). The old code only treated 404 as
    'create it' and crashed the whole spider on startup for 403.

    moto (our test double) always returns a clean 404, so it can't reproduce
    this by itself -- we simulate the exact 403 response MinIO sent.
    """
    from unittest.mock import MagicMock

    from botocore.exceptions import ClientError

    from wrc_pipeline.config import ObjectStoreSettings
    from wrc_pipeline.storage.object_store import ObjectStore

    store = ObjectStore(ObjectStoreSettings(endpoint_url=None, access_key="t", secret_key="t"))

    fake_client = MagicMock()
    fake_client.head_bucket.side_effect = ClientError(
        {"Error": {"Code": "403", "Message": "Forbidden"},
         "ResponseMetadata": {"HTTPStatusCode": 403}},
        "HeadBucket",
    )
    monkeypatch.setattr(ObjectStore, "client", property(lambda _self: fake_client))

    store.ensure_bucket("landing-zone")  # must NOT raise

    fake_client.create_bucket.assert_called_once_with(Bucket="landing-zone")


def test_ensure_bucket_swallows_bucket_already_owned_on_create(monkeypatch):
    """If create_bucket races with another process that just made it, that's
    success, not failure."""
    from unittest.mock import MagicMock

    from botocore.exceptions import ClientError

    from wrc_pipeline.config import ObjectStoreSettings
    from wrc_pipeline.storage.object_store import ObjectStore

    store = ObjectStore(ObjectStoreSettings(endpoint_url=None, access_key="t", secret_key="t"))

    fake_client = MagicMock()
    fake_client.head_bucket.side_effect = ClientError(
        {"Error": {"Code": "404"}, "ResponseMetadata": {"HTTPStatusCode": 404}}, "HeadBucket"
    )
    fake_client.create_bucket.side_effect = ClientError(
        {"Error": {"Code": "BucketAlreadyOwnedByYou"}}, "CreateBucket"
    )
    monkeypatch.setattr(ObjectStore, "client", property(lambda _self: fake_client))

    store.ensure_bucket("landing-zone")  # must NOT raise


def test_ensure_bucket_still_raises_on_a_genuinely_unexpected_error(monkeypatch):
    """A real network/config problem must NOT be silently swallowed just
    because we widened the 403 handling."""
    from unittest.mock import MagicMock

    from botocore.exceptions import EndpointConnectionError

    from wrc_pipeline.config import ObjectStoreSettings
    from wrc_pipeline.storage.object_store import ObjectStore

    store = ObjectStore(ObjectStoreSettings(endpoint_url=None, access_key="t", secret_key="t"))

    fake_client = MagicMock()
    fake_client.head_bucket.side_effect = EndpointConnectionError(endpoint_url="http://bad:9000")
    monkeypatch.setattr(ObjectStore, "client", property(lambda _self: fake_client))

    with pytest.raises(EndpointConnectionError):
        store.ensure_bucket("landing-zone")
