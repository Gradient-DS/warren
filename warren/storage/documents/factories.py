"""
Factory functions for document fetching components.
"""

from collections.abc import Mapping

from redis.asyncio import Redis

from warren.storage.cache.redis import (
    RedisBinaryCache,
)
from warren.storage.documents.fetcher import (
    CachedDocumentFetcher,
)
from warren.storage.documents.interface import (
    ResolveDocumentFunc,
)


DEFAULT_TTL_SECONDS: int = 86400  # 24 hours


def create_cached_document_fetcher(
    *,
    redis_client: Redis,
    resolvers: Mapping[str, ResolveDocumentFunc],
    cache_base_key: str = "documents",
    default_ttl_seconds: int = DEFAULT_TTL_SECONDS,
) -> CachedDocumentFetcher:
    """Create a CachedDocumentFetcher with a RedisBinaryCache backend.

    :param redis_client: Async Redis client instance.
    :param resolvers: Mapping of location_type to resolver function.
    :param cache_base_key: Redis key namespace for document cache.
    :param default_ttl_seconds: Default TTL for cached document bytes.

    :return: Configured CachedDocumentFetcher instance.
    """
    cache = RedisBinaryCache(
        client=redis_client,
        base_key=cache_base_key,
        default_ttl_seconds=default_ttl_seconds,
    )
    return CachedDocumentFetcher(cache=cache, resolvers=resolvers)
