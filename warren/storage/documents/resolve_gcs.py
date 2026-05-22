"""GCS document resolver.

Fetches document bytes from Google Cloud Storage. Follows the same
contract as resolve_path in resolvers.py — standalone async function,
bound via functools.partial at wiring time.
"""

from typing import cast

import asyncio

from google.api_core.exceptions import NotFound
from google.cloud import storage

from document_processing.distributed.warren.storage.documents.interface import (
    DocumentNotFoundError,
)
from document_processing.distributed.warren.storage.documents.location import (
    DocumentCloudLocation,
    DocumentLocation,
)


async def resolve_gcs(
    location: DocumentLocation,
    *,
    client: storage.Client,
) -> bytes:
    """Read document bytes from Google Cloud Storage.

    :param location: Must be a DocumentCloudLocation with provider="gcs".
    :param client: GCS client instance, bound via functools.partial
        at wiring time.

    :return: Raw document bytes.

    :raises DocumentNotFoundError: If the object does not exist.
    """
    cloud_location = cast(DocumentCloudLocation, location)
    bucket = client.bucket(cloud_location.bucket)
    blob = bucket.blob(cloud_location.key)

    try:
        return await asyncio.to_thread(blob.download_as_bytes)
    except NotFound as e:
        raise DocumentNotFoundError(
            f"Document not found in GCS: "
            f"gs://{cloud_location.bucket}/{cloud_location.key}"
        ) from e
