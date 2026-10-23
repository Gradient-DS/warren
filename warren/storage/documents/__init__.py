"""Documents: document locations, resolvers, the cached fetcher and source discovery.

`resolve_gcs` needs the `warren[gcs]` extra, and the HTTP names need the
`warren[http]` extra. The package imports them only when you use them, so
the package imports without these extras.
"""

from typing import TYPE_CHECKING, Any

import importlib

from warren.storage.documents.factories import (
    DEFAULT_TTL_SECONDS,
    create_cached_document_fetcher,
)
from warren.storage.documents.fetcher import (
    CachedDocumentFetcher,
    build_document_cache_key,
)
from warren.storage.documents.interface import (
    DocumentNotFoundError,
    DocumentResolutionError,
    DocumentThrottledError,
    GetDocumentFunc,
    ResolveDocumentFunc,
    UnknownLocationTypeError,
)
from warren.storage.documents.location import (
    AnyDocumentLocation,
    DocumentCloudLocation,
    DocumentLocation,
    DocumentPathLocation,
    DocumentURLLocation,
)
from warren.storage.documents.resolve_s3 import resolve_s3
from warren.storage.documents.resolvers import resolve_path
from warren.storage.documents.sources import (
    discover_gcs_pdfs,
    discover_local_pdfs,
)


if TYPE_CHECKING:
    from warren.storage.documents.resolve_gcs import resolve_gcs
    from warren.storage.documents.resolve_http import (
        DEFAULT_FOLLOW_REDIRECTS,
        DEFAULT_MAX_REDIRECTS,
        DEFAULT_TIMEOUT_S,
        build_client,
        parse_retry_after,
        resolve_http,
    )


_LAZY = {
    "resolve_gcs": "warren.storage.documents.resolve_gcs",
    "DEFAULT_FOLLOW_REDIRECTS": "warren.storage.documents.resolve_http",
    "DEFAULT_MAX_REDIRECTS": "warren.storage.documents.resolve_http",
    "DEFAULT_TIMEOUT_S": "warren.storage.documents.resolve_http",
    "build_client": "warren.storage.documents.resolve_http",
    "parse_retry_after": "warren.storage.documents.resolve_http",
    "resolve_http": "warren.storage.documents.resolve_http",
}


def __getattr__(name: str) -> Any:
    """Import a name that needs an optional extra when it is first used."""
    if name in _LAZY:
        return getattr(importlib.import_module(_LAZY[name]), name)
    msg = f"module {__name__!r} has no attribute {name!r}"
    raise AttributeError(msg)


__all__ = [
    "DEFAULT_FOLLOW_REDIRECTS",
    "DEFAULT_MAX_REDIRECTS",
    "DEFAULT_TIMEOUT_S",
    "DEFAULT_TTL_SECONDS",
    "AnyDocumentLocation",
    "CachedDocumentFetcher",
    "DocumentCloudLocation",
    "DocumentLocation",
    "DocumentNotFoundError",
    "DocumentPathLocation",
    "DocumentResolutionError",
    "DocumentThrottledError",
    "DocumentURLLocation",
    "GetDocumentFunc",
    "ResolveDocumentFunc",
    "UnknownLocationTypeError",
    "build_client",
    "build_document_cache_key",
    "create_cached_document_fetcher",
    "discover_gcs_pdfs",
    "discover_local_pdfs",
    "parse_retry_after",
    "resolve_gcs",
    "resolve_http",
    "resolve_path",
    "resolve_s3",
]
