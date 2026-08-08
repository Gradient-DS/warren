"""
Cached document fetcher implementation.

Warren's take on the **claim-check pattern**: messages stay small and carry
only a payload *location*; workers resolve the actual bytes on demand
through this fetcher. Composes a binary cache with a resolver dispatch
table to provide transparent cache-aside fetching. Workers receive this as
a ``GetDocumentFunc`` — they don't know about caching or resolvers.
"""

from collections.abc import Mapping

from basics.base import Base

from warren.storage.cache.interface import (
    CacheInterface,
    get_or_set,
)
from warren.storage.documents.interface import (
    DocumentResolutionError,
    ResolveDocumentFunc,
    UnknownLocationTypeError,
)
from warren.storage.documents.location import (
    DocumentLocation,
)


def build_document_cache_key(doc_id: str, job_id: str | None = None) -> str:
    """Compute the cache key a document's bytes live under.

    Exposed at module level so upstream writers (e.g. ``WebFetchWorker``
    via ``BinaryResultsStore``) can pre-populate the cache with the same
    key the downstream ``CachedDocumentFetcher`` reads from — avoiding a
    resolver call for bytes that have already been fetched by an earlier
    worker.

    When ``job_id`` is given the key is job-scoped
    (``doc:{doc_id}:{job_id}``): retries of the same job still hit the
    cache, while a re-submission of the same ``doc_id`` with new content
    (a new job) always fetches fresh bytes instead of serving stale
    cached ones. Without ``job_id`` the legacy shared key
    (``doc:{doc_id}``) is used — subject to cross-job staleness.
    """
    if job_id is None:
        return f"doc:{doc_id}"
    return f"doc:{doc_id}:{job_id}"


class CachedDocumentFetcher(Base):
    """Fetches document bytes with cache-aside pattern.

    Uses a binary cache (e.g., RedisBinaryCache) for caching and a
    dispatch table of resolver functions for location-type-specific
    fetching.

    :param cache: Binary cache instance for storing resolved bytes.
    :param resolvers: Mapping of location_type to resolver function.
    :param name: Optional name for logging.
    """

    def __init__(
        self,
        *,
        cache: CacheInterface[bytes],
        resolvers: Mapping[str, ResolveDocumentFunc],
        name: str | None = None,
    ) -> None:
        super().__init__(pybase_logger_name=name)
        self._cache = cache
        self._resolvers = resolvers

    async def __call__(
        self,
        doc_id: str,
        document_location: DocumentLocation,
        *,
        job_id: str | None = None,
    ) -> bytes:
        """Fetch document bytes, using cache when available.

        :param doc_id: Document identifier (used as cache key).
        :param document_location: Where the document lives.
        :param job_id: Job identifier scoping the cache entry. Pass it so
            a re-submitted ``doc_id`` with updated content (a new job)
            never hits bytes cached by a previous job; ``None`` keeps the
            legacy shared ``doc:{doc_id}`` key.

        :return: Raw document bytes.

        :raises UnknownLocationTypeError: If no resolver for the location type.
        :raises DocumentNotFoundError: If the document doesn't exist at the location.
        :raises DocumentResolutionError: For transient resolution failures.
        """
        cache_key = self._build_cache_key(doc_id, job_id)
        return await get_or_set(
            self._cache,
            cache_key,
            factory=lambda: self._resolve(doc_id, document_location),
        )

    async def _resolve(
        self,
        doc_id: str,
        document_location: DocumentLocation,
    ) -> bytes:
        """Dispatch to the resolver for the given location type.

        Raises ``UnknownLocationTypeError`` if no resolver is registered.
        Wraps unexpected exceptions in ``DocumentResolutionError`` so
        workers always receive a typed exception from the resolution layer.
        """
        location_type = document_location.location_type
        resolver = self._resolvers.get(location_type)
        if resolver is None:
            msg = f"No resolver registered for location type: '{location_type}'"
            raise UnknownLocationTypeError(
                msg,
                doc_id=doc_id,
            )

        try:
            return await resolver(document_location)
        except DocumentResolutionError:
            raise
        except Exception as e:
            msg = (
                f"Failed to resolve document '{doc_id}' "
                f"from {location_type} location: {e}"
            )
            raise DocumentResolutionError(
                msg,
                doc_id=doc_id,
            ) from e

    def _build_cache_key(self, doc_id: str, job_id: str | None) -> str:
        """Build cache key from document identity."""
        return build_document_cache_key(doc_id, job_id)
