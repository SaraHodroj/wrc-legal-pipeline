"""S3-compatible object storage.

We talk S3, not the MinIO SDK. MinIO is the local Docker stand-in; the same
code runs unchanged against AWS S3, GCS (via its S3 interoperability layer) or
Cloudflare R2 by changing one environment variable. Coupling the pipeline to a
vendor SDK for the sake of a local container would be a poor trade.
"""

from __future__ import annotations

import io
from typing import Any

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError

from ..config import ObjectStoreSettings
from ..logging_setup import get_logger

logger = get_logger(__name__)


class ObjectStore:
    """Minimal wrapper over the S3 API surface this pipeline actually uses."""

    def __init__(self, settings: ObjectStoreSettings) -> None:
        self._settings = settings
        self._client: Any = None

    @property
    def client(self) -> Any:
        if self._client is None:
            self._client = boto3.client(
                "s3",
                endpoint_url=self._settings.endpoint_url,
                aws_access_key_id=self._settings.access_key,
                aws_secret_access_key=self._settings.secret_key,
                region_name=self._settings.region,
                # MinIO requires path-style addressing; virtual-host style
                # assumes DNS records that do not exist for a local container.
                config=Config(s3={"addressing_style": "path"}, retries={"max_attempts": 5}),
            )
        return self._client

    def ensure_bucket(self, bucket: str) -> None:
        """Create the bucket if absent. Safe to call on every startup.

        MinIO has a well-documented quirk: ``HeadBucket`` on a bucket it can't
        confirm the caller owns returns **403 Forbidden**, not 404 Not Found --
        deliberate security-through-obscurity, so an unauthorized caller can't
        even learn whether a bucket exists. Treating only 404 as "go ahead and
        create it" means a legitimate first-run case (the bucket genuinely
        doesn't exist yet) crashes the whole spider on startup instead of
        creating the bucket like it's meant to.

        Both cases route to the same recovery: attempt ``create_bucket``. If
        the bucket already exists and we own it, MinIO/S3 treat that as a
        harmless no-op (or raise ``BucketAlreadyOwnedByYou``, caught below). If
        we genuinely lack permission for a *different* reason, ``create_bucket``
        itself will fail with its own clear error -- so this doesn't silently
        swallow a real permissions problem, it just stops assuming 403 always
        means "real problem, crash immediately."
        """
        try:
            self.client.head_bucket(Bucket=bucket)
            return
        except ClientError as exc:
            error = exc.response.get("Error", {})
            code = error.get("Code", "")
            status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")

            if code not in {"404", "NoSuchBucket", "403", "AccessDenied"} and status != 403:
                # A genuinely unexpected failure (network issue, wrong
                # endpoint, etc.) -- don't mask it, let it surface loudly.
                raise

            logger.info(
                "head_bucket inconclusive, attempting create",
                extra={
                    "event": "bucket_head_inconclusive",
                    "bucket": bucket,
                    "error_code": code,
                    "http_status": status,
                },
            )

        try:
            self.client.create_bucket(Bucket=bucket)
            logger.info("Created bucket", extra={"event": "bucket_created", "bucket": bucket})
        except ClientError as exc:
            already_owned = exc.response.get("Error", {}).get("Code") in {
                "BucketAlreadyOwnedByYou",
                "BucketAlreadyExists",
            }
            if not already_owned:
                raise
            logger.debug(
                "Bucket already exists and is owned by us",
                extra={"event": "bucket_already_exists", "bucket": bucket},
            )

    def put_bytes(
        self,
        bucket: str,
        key: str,
        data: bytes,
        content_type: str | None = None,
        metadata: dict[str, str] | None = None,
    ) -> str:
        """Store an object and return its ``s3://`` URI.

        Metadata is attached to the object itself as well as being written to
        Mongo. That redundancy is deliberate: if the metadata store is ever
        lost or rebuilt, the bucket alone still carries enough provenance to
        reconstruct it.
        """
        self.client.put_object(
            Bucket=bucket,
            Key=key,
            Body=data,
            ContentType=content_type or "application/octet-stream",
            Metadata=metadata or {},
        )
        return f"s3://{bucket}/{key}"

    def get_bytes(self, bucket: str, key: str) -> bytes:
        response = self.client.get_object(Bucket=bucket, Key=key)
        return bytes(response["Body"].read())

    def get_stream(self, bucket: str, key: str) -> io.BytesIO:
        return io.BytesIO(self.get_bytes(bucket, key))

    def exists(self, bucket: str, key: str) -> bool:
        try:
            self.client.head_object(Bucket=bucket, Key=key)
            return True
        except ClientError as exc:
            if exc.response["Error"]["Code"] in {"404", "NoSuchKey"}:
                return False
            raise

    @staticmethod
    def parse_uri(uri: str) -> tuple[str, str]:
        """``s3://bucket/a/b.pdf`` -> ``('bucket', 'a/b.pdf')``."""
        if not uri.startswith("s3://"):
            raise ValueError(f"Not an s3 URI: {uri!r}")
        bucket, _, key = uri[len("s3://") :].partition("/")
        if not bucket or not key:
            raise ValueError(f"Malformed s3 URI: {uri!r}")
        return bucket, key
