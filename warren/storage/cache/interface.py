from typing import Protocol

from collections.abc import Awaitable, Callable

from warren.exceptions import WarrenError


class CacheOperationError(WarrenError):
    """
    Raised when a cache operation fails.

    :param message: Optional error message with details.
    :param operation: Optional operation name (e.g., "get", "set", "delete")
        to make the error message more specific.
    """

    def __init__(
        self,
        message: str | None = None,
        operation: str | None = None,
    ) -> None:
        self.operation = operation

        if operation is not None and message is not None:
            full_message = f"Cache operation '{operation}' failed: {message}"
        elif operation is not None:
            full_message = f"Cache operation '{operation}' failed"
        elif message is not None:
            full_message = message
        else:
            full_message = "Cache operation failed"

        super().__init__(full_message)


class CacheInterface[T](Protocol):
    """
    Generic async cache interface for storing typed values.

    All methods are async — implementations must use non-blocking I/O
    (e.g., redis.asyncio) to avoid blocking the event loop.

    Implementations should be specialized for specific value types
    (e.g., dict values or binary values).

    A cache is one namespace: the Redis caches prefix each key with a
    ``base_key``. Each ``MemoryCache`` instance is its own namespace.

    Key hierarchy convention: keys can be hierarchical, with ":" as the
    separator (e.g., "doc123:0" and "doc123:1" for the parts of document
    doc123). For the prefix operations (get_by_key_prefix,
    delete_by_key_prefix), include the trailing separator to match only
    sub-keys:

    - "doc123" matches: doc123, doc123:0, doc1234:0 (can be too broad)
    - "doc123:" matches: doc123:0, doc123:1 only (sub-keys only)
    """

    async def get(self, key: str) -> T | None:
        """
        Retrieve value from cache.

        :param key: Cache key.

        :return: Cached value, or None if not found.
        """
        ...

    async def set(self, key: str, value: T, ttl_seconds: int | None = None) -> None:
        """
        Store value in cache.

        :param key: Cache key.
        :param value: Value to cache.
        :param ttl_seconds: Time-to-live in seconds. None uses default TTL.
        """
        ...

    async def delete(self, key: str) -> bool:
        """
        Delete key from cache.

        :param key: Cache key.

        :return: True if key existed and was deleted, False otherwise.
        """
        ...

    async def exists(self, key: str) -> bool:
        """
        Check if key exists in cache.

        :param key: Cache key.

        :return: True if key exists.
        """
        ...

    async def get_by_key_prefix(self, key_prefix: str) -> dict[str, T]:
        """
        Get all items where key starts with key_prefix.

        IMPORTANT: The prefix is matched literally with a wildcard appended.
        To match only sub-keys, include the hierarchy separator (":") at the
        end. See class docstring for key hierarchy convention.

        :param key_prefix: Key prefix to match.

        :return: Dictionary mapping matching keys to their values.
        """
        ...

    async def set_many(
        self, items: dict[str, T], ttl_seconds: int | None = None
    ) -> None:
        """
        Store multiple key-value pairs.

        :param items: Dictionary of key-value pairs to cache.
        :param ttl_seconds: Time-to-live in seconds. None uses default TTL.
        """
        ...

    async def delete_by_key_prefix(self, key_prefix: str) -> int:
        """
        Delete all items where key starts with key_prefix.

        IMPORTANT: The prefix is matched literally with a wildcard appended.
        To delete only sub-keys, include the hierarchy separator (":") at the
        end. See class docstring for key hierarchy convention.

        :param key_prefix: Key prefix to match.

        :return: Number of keys deleted.
        """
        ...

    async def clear(self) -> None:
        """Clear all entries from this cache namespace."""
        ...


async def get_or_set[T](
    cache: CacheInterface[T],
    key: str,
    factory: Callable[[], Awaitable[T]],
    ttl_seconds: int | None = None,
) -> T:
    """
    Return cached value, or compute via async factory, cache it, and return.

    This implements the cache-aside pattern: if the key exists in cache,
    return the cached value. Otherwise, call factory() to compute the value,
    store it in cache, and return it.

    Example with ``functools.partial``:

    ```python
    import asyncio
    from functools import partial
    from warren.storage.cache import MemoryCache, get_or_set

    async def load(key: str) -> dict:
        return {"key": key}

    async def main() -> None:
        cached = partial(get_or_set, MemoryCache[dict]())
        print(await cached("doc-1", lambda: load("doc-1")))

    asyncio.run(main())
    ```

    :param cache: Cache instance to use.
    :param key: Cache key.
    :param factory: Async callable that produces the value if not cached.
    :param ttl_seconds: Time-to-live in seconds. None uses cache default.

    :return: Cached or newly computed value.
    """
    value = await cache.get(key)
    if value is not None:
        return value

    value = await factory()
    await cache.set(key, value, ttl_seconds)
    return value
