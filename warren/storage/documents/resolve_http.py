"""HTTP(S) document resolver.

Fetches document bytes from an HTTP(S) URL — e.g. a short-lived presigned
GET URL. Follows the same contract as resolve_s3 / resolve_gcs: a standalone
async function, bound to an httpx.AsyncClient via functools.partial at wiring
time.

httpx is an optional dependency (the ``http`` extra). This module is only
imported once httpx is confirmed importable (see runner._create_resolvers),
so httpx is imported at module top level here — mirroring resolve_gcs.

This is the "a file happens to live at a URL" resolver (PDF/XML/office/…),
distinct from WebFetchWorker's browser-crawl path.
"""

from typing import cast

import httpx

from warren.storage.documents.interface import DocumentNotFoundError
from warren.storage.documents.location import (
    DocumentLocation,
    DocumentURLLocation,
)


async def resolve_http(
    location: DocumentLocation,
    *,
    client: httpx.AsyncClient,
) -> bytes:
    """Read document bytes from an HTTP(S) URL.

    :param location: Must be a DocumentURLLocation.
    :param client: httpx.AsyncClient instance, bound via functools.partial
        at wiring time.

    :return: Raw document bytes.

    :raises DocumentNotFoundError: If the server responds 404 (hard failure).
    """
    url_location = cast("DocumentURLLocation", location)

    response = await client.get(url_location.url)
    if response.status_code == httpx.codes.NOT_FOUND:
        msg = f"Document not found at URL: {url_location.url}"
        raise DocumentNotFoundError(msg)
    response.raise_for_status()
    return response.content
