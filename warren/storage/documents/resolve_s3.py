"""S3 document resolver.

Fetches document bytes from Amazon S3. Follows the same contract as
resolve_gcs — standalone async function, bound via functools.partial
at wiring time.

botocore is an optional dependency: it is imported lazily inside the
function body so that importing this module does not require boto3/botocore
to be installed.
"""

from typing import Any, cast

import asyncio

from warren.storage.documents.interface import DocumentNotFoundError
from warren.storage.documents.location import (
    DocumentCloudLocation,
    DocumentLocation,
)


_NOT_FOUND_CODES = {"NoSuchKey", "NoSuchBucket", "404"}


async def resolve_s3(
    location: DocumentLocation,
    *,
    client: Any,
) -> bytes:
    """Read document bytes from Amazon S3.

    :param location: Must be a DocumentCloudLocation with provider="s3".
    :param client: boto3 S3 client instance, bound via functools.partial
        at wiring time.

    :return: Raw document bytes.

    :raises DocumentNotFoundError: If the object or bucket does not exist.
    """
    from botocore.exceptions import ClientError

    cloud_location = cast("DocumentCloudLocation", location)

    def _get() -> bytes:
        try:
            response = client.get_object(
                Bucket=cloud_location.bucket,
                Key=cloud_location.key,
            )
            return response["Body"].read()
        except ClientError as exc:
            code = exc.response["Error"]["Code"]
            if code in _NOT_FOUND_CODES:
                msg = (
                    f"Document not found in S3: "
                    f"s3://{cloud_location.bucket}/{cloud_location.key}"
                )
                raise DocumentNotFoundError(msg) from exc
            raise

    return await asyncio.to_thread(_get)
