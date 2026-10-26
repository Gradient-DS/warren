"""
Protocols, type aliases, and exceptions for document fetching.

Defines the contract between workers and the payload-fetching layer (the
claim-check pattern: messages carry a location, not bytes). Workers depend
on ``GetDocumentFunc`` — they don't know about caching, resolvers, or
storage backends.

Exception hierarchy for error handling in workers::

    DocumentResolutionError          # base (transient or unknown failure)
    ├── DocumentNotFoundError        # the document does not exist
    ├── UnknownLocationTypeError     # no resolver registered
    └── DocumentThrottledError       # the source asked for a delay

None of these errors is a ``SoftFailureException``, so the consumer treats
each of them as a hard failure. To retry, catch the error in the handler and
raise ``SoftFailureException``.
"""

from typing import Protocol

from collections.abc import Awaitable, Callable

from warren.exceptions import WarrenError
from warren.storage.documents.location import (
    DocumentLocation,
)


class DocumentResolutionError(WarrenError):
    """Base exception for document resolution failures.

    Not retried by itself: the consumer treats any exception other than
    ``SoftFailureException`` as a hard failure. To retry, catch this error
    in the handler and raise ``SoftFailureException``.

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

    Do not retry it: the document will not appear on retry.

    Raised by resolvers and the fetcher. Document stores raise a different
    class, ``warren.storage.document_store.DocumentNotFoundError``. A handler
    that catches one class does not catch the other.
    """


class UnknownLocationTypeError(DocumentResolutionError):
    """No resolver registered for the given location type.

    Do not retry it: a missing resolver is a configuration error, not a
    transient issue.
    """


class DocumentThrottledError(DocumentResolutionError):
    """The source asked us to slow down: HTTP 429, or 503 with ``Retry-After``.

    Not retried by itself, as for ``DocumentResolutionError``. To retry
    after the delay that the source asked for, catch this error in the
    handler and raise ``SoftFailureException`` with ``retry_after``.

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

        :raises DocumentNotFoundError: If the document does not exist.
        :raises UnknownLocationTypeError: If no resolver for the location type.
        :raises DocumentResolutionError: For transient resolution failures. The
            consumer does not retry it: raise ``SoftFailureException`` to retry.
        """
        ...
