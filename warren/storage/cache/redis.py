from typing import TypeVar

import json
from abc import ABC, abstractmethod

from basics.base import Base
from redis.asyncio import Redis
from redis.exceptions import RedisError

from warren.storage.cache.interface import (
    CacheInterface,
    CacheOperationError,
)


T = TypeVar("T")


class RedisCacheBase(Base, ABC, CacheInterface[T]):
    """
    Abstract base class for async Redis-backed cache implementations.

    Uses redis.asyncio for non-blocking I/O. Handles Redis connection,
    key namespacing, TTL management, and error handling.
    Subclasses must implement _serialize and _deserialize for their specific types.
    """

    def __init__(
        self,
        client: Redis,
        *,
        base_key: str,
        default_ttl_seconds: int | None = None,
        key_separator: str = ":",
        name: str | None = None,
    ) -> None:
        """
        Initialize the Redis cache.

        :param client: Async Redis client instance (injected, not created internally).
        :param base_key: Namespace prefix for all keys in this cache.
        :param default_ttl_seconds: Default TTL for cached values. None means
            no expiration.
        :param key_separator: Separator between base_key and key.
            Defaults to ":" (Redis convention). Colon is preferred over dot
            because dots commonly appear in data (filenames, versions).
        :param name: optional name for logging purposes
        """
        super().__init__(pybase_logger_name=name)

        self._client = client
        self._base_key = base_key
        self._default_ttl_seconds = default_ttl_seconds
        self._key_separator = key_separator

    async def get(self, key: str) -> T | None:
        """
        Retrieve value from cache.

        :param key: Cache key.

        :return: Cached value, or None if not found.

        :raises CacheOperationError: If Redis operation fails.
        """
        try:
            data = await self._client.get(self._full_key(key))
            if data is None:
                return None
            return self._deserialize(data)
        except (RedisError, ValueError, TypeError) as e:
            raise CacheOperationError(operation="get") from e

    async def set(self, key: str, value: T, ttl_seconds: int | None = None) -> None:
        """
        Store value in cache.

        :param key: Cache key.
        :param value: Value to cache.
        :param ttl_seconds: Time-to-live in seconds. None uses default TTL.

        :raises CacheOperationError: If Redis operation fails.
        """
        try:
            serialized = self._serialize(value)
            await self._client.set(
                self._full_key(key), serialized, ex=self._get_ttl(ttl_seconds)
            )
        except (RedisError, ValueError, TypeError) as e:
            raise CacheOperationError(operation="set") from e

    async def delete(self, key: str) -> bool:
        """
        Delete key from cache.

        :param key: Cache key.

        :return: True if key existed and was deleted, False otherwise.

        :raises CacheOperationError: If Redis operation fails.
        """
        try:
            result = await self._client.delete(self._full_key(key))
            return result > 0
        except RedisError as e:
            raise CacheOperationError(operation="delete") from e

    async def exists(self, key: str) -> bool:
        """
        Check if key exists in cache.

        :param key: Cache key.

        :return: True if key exists.

        :raises CacheOperationError: If Redis operation fails.
        """
        try:
            return await self._client.exists(self._full_key(key)) > 0
        except RedisError as e:
            raise CacheOperationError(operation="exists") from e

    async def get_by_key_prefix(self, key_prefix: str) -> dict[str, T]:
        """
        Get all items where key starts with key_prefix.

        IMPORTANT: The prefix is matched literally with a wildcard appended.
        To match only sub-keys of a hierarchical key, include the hierarchy
        separator in the prefix. Example with keys "doc123:0", "doc123:1", "doc1234:0":
        - key_prefix="doc123" matches: doc123:0, doc123:1, doc1234:0 (too broad!)
        - key_prefix="doc123:" matches: doc123:0, doc123:1 only (sub-keys only)

        :param key_prefix: Key prefix to match.

        :return: Dictionary mapping matching keys to their values.

        :raises CacheOperationError: If Redis operation fails.
        """
        try:
            pattern = f"{self._full_key(key_prefix)}*"
            keys = await self._scan_keys(pattern)

            if not keys:
                return {}

            values = await self._client.mget(keys)
            result: dict[str, T] = {}
            for full_key, data in zip(keys, values, strict=False):
                if data is not None:
                    result[self._strip_base_key(full_key)] = self._deserialize(data)

            return result
        except (RedisError, ValueError, TypeError) as e:
            raise CacheOperationError(operation="get_by_key_prefix") from e

    async def set_many(
        self, items: dict[str, T], ttl_seconds: int | None = None
    ) -> None:
        """
        Store multiple key-value pairs.

        :param items: Dictionary of key-value pairs to cache.
        :param ttl_seconds: Time-to-live in seconds. None uses default TTL.

        :raises CacheOperationError: If Redis operation fails.
        """
        if not items:
            return

        try:
            ttl = self._get_ttl(ttl_seconds)
            pipeline = self._client.pipeline()

            for key, value in items.items():
                serialized = self._serialize(value)
                await pipeline.set(self._full_key(key), serialized, ex=ttl)

            await pipeline.execute()
        except (RedisError, ValueError, TypeError) as e:
            raise CacheOperationError(operation="set_many") from e

    async def delete_by_key_prefix(self, key_prefix: str) -> int:
        """
        Delete all items where key starts with key_prefix.

        IMPORTANT: The prefix is matched literally with a wildcard appended.
        To delete only sub-keys of a hierarchical key, include the hierarchy
        separator in the prefix. Example with keys "doc123:0", "doc123:1", "doc1234:0":
        - key_prefix="doc123" deletes: doc123:0, doc123:1, doc1234:0 (too broad!)
        - key_prefix="doc123:" deletes: doc123:0, doc123:1 only (sub-keys only)

        :param key_prefix: Key prefix to match.

        :return: Number of keys deleted.

        :raises CacheOperationError: If Redis operation fails.
        """
        try:
            pattern = f"{self._full_key(key_prefix)}*"
            keys = await self._scan_keys(pattern)

            if not keys:
                return 0

            return await self._client.delete(*keys)
        except RedisError as e:
            raise CacheOperationError(operation="delete_by_key_prefix") from e

    async def clear(self) -> None:
        """
        Clear all entries from this cache namespace.

        :raises CacheOperationError: If Redis operation fails.
        """
        try:
            pattern = f"{self._base_key}{self._key_separator}*"
            keys = await self._scan_keys(pattern)

            if keys:
                await self._client.delete(*keys)
        except RedisError as e:
            raise CacheOperationError(operation="clear") from e

    @abstractmethod
    def _serialize(self, value: T) -> bytes:
        """Serialize value to bytes for storage."""
        ...

    @abstractmethod
    def _deserialize(self, data: bytes) -> T:
        """Deserialize bytes from storage to value."""
        ...

    def _full_key(self, key: str) -> str:
        """Build full Redis key with namespace prefix."""
        return f"{self._base_key}{self._key_separator}{key}"

    def _strip_base_key(self, full_key: str) -> str:
        """Strip namespace prefix from full Redis key."""
        prefix = f"{self._base_key}{self._key_separator}"
        if full_key.startswith(prefix):
            return full_key[len(prefix) :]

        self._log.warning(f"Redis key '{full_key}' does not start with '{prefix}'.")

        return full_key

    def _get_ttl(self, ttl_seconds: int | None) -> int | None:
        """Return TTL to use: explicit value or default."""
        if ttl_seconds is not None:
            return ttl_seconds
        return self._default_ttl_seconds

    async def _scan_keys(self, pattern: str) -> list[str]:
        """
        Scan for keys matching pattern.

        Uses Redis glob-style patterns:
        - * matches any sequence of characters
        - ? matches a single character
        - [abc] matches any character in the set
        """
        keys: list[str] = []
        cursor = 0
        while True:
            cursor, batch = await self._client.scan(cursor=cursor, match=pattern)
            keys.extend(k.decode("utf-8") if isinstance(k, bytes) else k for k in batch)
            if cursor == 0:
                break
        return keys


class RedisDictCache(RedisCacheBase[dict]):
    """
    Async Redis cache for Dict values using JSON serialization.

    Suitable for caching document metadata, extracted text, processing results,
    and other JSON-serializable data.
    """

    def _serialize(self, value: dict) -> bytes:
        """Serialize Dict to JSON bytes."""
        return json.dumps(value).encode("utf-8")

    def _deserialize(self, data: bytes) -> dict:
        """Deserialize JSON bytes to Dict."""
        return json.loads(data.decode("utf-8"))


class RedisBinaryCache(RedisCacheBase[bytes]):
    """
    Async Redis cache for binary data (pass-through, no serialization).

    Suitable for caching PDFs, images, and other binary files.
    """

    def _serialize(self, value: bytes) -> bytes:
        """Pass-through for binary data."""
        return value

    def _deserialize(self, data: bytes) -> bytes:
        """Pass-through for binary data."""
        return data
