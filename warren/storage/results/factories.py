"""
Factory functions for creating ResultsStore instances.
"""

from pymongo import AsyncMongoClient
from redis.asyncio import Redis

from document_processing.distributed.warren.storage.cache.redis import (
    RedisBinaryCache,
    RedisDictCache,
)
from document_processing.distributed.warren.storage.document_store.mongodb import (
    MongoDBDocumentStore,
)
from document_processing.distributed.warren.storage.results.binary import (
    BinaryResultsStore,
)
from document_processing.distributed.warren.storage.results.default import (
    DefaultResultsStore,
)


async def create_default_results_store(
    collection_name: str,
    mongo_client: AsyncMongoClient,
    redis_client: Redis | None = None,
    *,
    database_name: str = "document_processing",
    doc_id_field: str = "result_id",
    cache_base_key_prefix: str = "results",
    cache_ttl_seconds: int = 3600,
) -> DefaultResultsStore:
    """
    Create a DefaultResultsStore with MongoDB persistence and optional Redis cache.

    Returns a fully initialized store (setup() already called on both
    the document store and the results store).

    :param collection_name: MongoDB collection and result_type identifier.
    :param mongo_client: Injected async MongoClient instance.
    :param redis_client: Injected async Redis instance. If None, no caching.
    :param database_name: MongoDB database name.
    :param doc_id_field: Field name for document IDs.
    :param cache_base_key_prefix: Prefix for Redis cache keys.
    :param cache_ttl_seconds: Default TTL for cached results.
    """
    doc_store = MongoDBDocumentStore(
        client=mongo_client,
        database_name=database_name,
        collection_name=collection_name,
        doc_id_field=doc_id_field,
        unique_indexes=[("doc_id", "job_id", "part_idx")],
    )
    await doc_store.setup()

    cache = None
    if redis_client is not None:
        cache = RedisDictCache(
            client=redis_client,
            base_key=f"{cache_base_key_prefix}:{collection_name}",
            default_ttl_seconds=cache_ttl_seconds,
        )

    store = DefaultResultsStore(
        document_store=doc_store,
        cache=cache,
        result_type=collection_name,
    )
    await store.setup()

    return store


async def create_binary_results_store(
    collection_name: str,
    mongo_client: AsyncMongoClient,
    redis_client: Redis | None = None,
    *,
    database_name: str = "document_processing",
    doc_id_field: str = "result_id",
    cache_base_key: str = "documents",
    cache_ttl_seconds: int | None = None,
) -> BinaryResultsStore:
    """Create a ``BinaryResultsStore`` wired with MongoDB + RedisBinaryCache.

    :param collection_name: MongoDB collection + result_type identifier.
    :param mongo_client: Async MongoDB client.
    :param redis_client: Async Redis client. ``None`` disables caching.
    :param database_name: MongoDB database name.
    :param doc_id_field: Field name for the per-result primary key.
    :param cache_base_key: Redis namespace. Defaults to ``"documents"``
        so the bytes land under the same cache namespace
        ``CachedDocumentFetcher`` reads from — downstream workers that
        use ``GetDocumentFunc`` hit these bytes transparently.
    :param cache_ttl_seconds: Default TTL. ``None`` = no expiry.
    """
    doc_store = MongoDBDocumentStore(
        client=mongo_client,
        database_name=database_name,
        collection_name=collection_name,
        doc_id_field=doc_id_field,
        unique_indexes=[("doc_id", "job_id", "part_idx")],
    )
    await doc_store.setup()

    cache: RedisBinaryCache | None = None
    if redis_client is not None:
        cache = RedisBinaryCache(
            client=redis_client,
            base_key=cache_base_key,
            default_ttl_seconds=cache_ttl_seconds,
        )

    store = BinaryResultsStore(
        document_store=doc_store,
        cache=cache,
        result_type=collection_name,
    )
    await store.setup()

    return store
