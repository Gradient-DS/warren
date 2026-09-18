"""
Protocols, type aliases, and exceptions for document fetching.

Defines the contract between workers and the payload-fetching layer (the
claim-check pattern: messages carry a location, not bytes). Workers depend
on ``GetDocumentFunc`` — they don't know about caching, resolvers, or
storage backends.

Exception hierarchy for error handling in workers::

    DocumentResolutionError          # base — soft failure (transient/unknown)
    ├── DocumentNotFoundError        # hard failure (document doesn't exist)
    ├── UnknownLocationTypeError     # hard failure (no resolver registered)
    └── DocumentThrottledError       # soft failure; the source asked for a delay
"""

from typing import Protocol

from collections.abc import Awaitable, Callable

from warren.exceptions import WarrenError
from warren.storage.documents.location import (
    DocumentLocation,
)


class DocumentResolutionError(WarrenError):
    """Base exception for document resolution failures.

    Treated as a soft failure (retryable) by workers unless a more
    specific subclass indicates otherwise.

    :param message: Error description.
    :param doc_id: Document identifier, if known.
    """

    def __init__(
        self,
        message: str,
        *,
        doc_id: str | None = None,
    ) -> None:
        self.doc_id = doc_id
        super().__init__(message)


class DocumentNotFoundError(DocumentResolutionError):
    """Document does not exist at the specified location.

    Treated as a hard failure (non-retryable) by workers — the
    document won't appear on retry.
    """


class UnknownLocationTypeError(DocumentResolutionError):
    """No resolver registered for the given location type.

    Treated as a hard failure (non-retryable) by workers — a missing
    resolver is a configuration error, not a transient issue.
    """


class DocumentThrottledError(DocumentResolutionError):
    """The source asked us to slow down: HTTP 429, or 503 with ``Retry-After``.

    Soft failure. Workers that defer the document instead of spending a
    retry slot read ``retry_after`` — see ``warren/docs/retry_design.md``,
    "Deferring on throttling".

    :param message: Error description.
    :param retry_after: Server-requested delay in seconds, or None when the
        response carried no usable ``Retry-After``.
    :param status_code: The HTTP status that carried the request.
    :param doc_id: Document identifier, if known.
    """

    def __init__(
        self,
        message: str,
        *,
        retry_after: float | None,
        status_code: int,
        doc_id: str | None = None,
    ) -> None:
        super().__init__(message, doc_id=doc_id)
        self.retry_after = retry_after
        self.status_code = status_code


ResolveDocumentFunc = Callable[[DocumentLocation], Awaitable[bytes]]
"""Async function that fetches bytes for a specific location type.

One resolver per ``location_type``. Bound with config (e.g., base_dir)
via ``functools.partial`` at wiring time.

Resolvers should raise:
- ``DocumentNotFoundError`` for permanent issues (file missing, HTTP 404).
- Let transient exceptions propagate (connection errors, timeouts) —
  the fetcher wraps them in ``DocumentResolutionError``.
"""


class GetDocumentFunc(Protocol):
    async def __call__(
        self,
        doc_id: str,
        document_location: DocumentLocation,
        *,
        job_id: str | None = None,
    ) -> bytes:
        """Fetch document bytes by location, with transparent caching.

        Workers call this to obtain raw document bytes. The implementation
        handles cache lookup, resolution dispatch, and cache population.

        :param doc_id: Document identifier (used as cache key).
        :param document_location: Where the document lives.
        :param job_id: Job identifier scoping the cache entry. Workers
            should pass ``message.job_id`` so a re-submitted ``doc_id``
            with updated content never reads bytes cached by a previous
            job. ``None`` falls back to the legacy shared per-doc key.

        :return: Raw document bytes.

        :raises DocumentNotFoundError: If the document doesn't exist (hard failure).
        :raises UnknownLocationTypeError: If no resolver for the location type (hard failure).
        :raises DocumentResolutionError: For transient resolution failures (soft failure).
        """
        ...
