"""
Resolver functions for document location types.

Each resolver fetches bytes from a specific location type. Resolvers
are standalone async functions — bind configuration (e.g., base_dir)
via ``functools.partial`` at wiring time::

    from functools import partial
    resolvers = {"path": partial(resolve_path, base_dir=Path("/data"))}

Resolvers raise ``DocumentNotFoundError`` for permanent issues (file
missing, HTTP 404) and let transient exceptions propagate — the
fetcher wraps them in ``DocumentResolutionError``.

The HTTP(S) URL resolver lives in ``resolve_http.py`` (httpx-based,
optional ``http`` extra) — for "a file happens to live at a URL"
(PDF/XML/…), distinct from WebFetchWorker's browser-crawl path.
"""

from typing import cast

import asyncio
from pathlib import Path

from warren.storage.documents.interface import (
    DocumentNotFoundError,
)
from warren.storage.documents.location import (
    DocumentLocation,
    DocumentPathLocation,
)


async def resolve_path(
    location: DocumentLocation,
    *,
    base_dir: Path = Path(),
) -> bytes:
    """Read document bytes from the local filesystem.

    :param location: Must be a DocumentPathLocation.
    :param base_dir: Base directory prepended to relative_path.
        Defaults to current working directory.

    :return: Raw document bytes.

    :raises DocumentNotFoundError: If the resolved path does not exist.
    """
    path_location = cast("DocumentPathLocation", location)
    path = base_dir / path_location.relative_path

    try:
        return await asyncio.to_thread(path.read_bytes)
    except FileNotFoundError as e:
        msg = f"Document not found at path: {path}"
        raise DocumentNotFoundError(msg) from e
