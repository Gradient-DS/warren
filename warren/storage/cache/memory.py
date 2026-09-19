"""In-process ``CacheInterface`` implementation."""

import copy
import time
from collections.abc import Callable

from basics.base import Base

from warren.storage.cache.interface import CacheInterface


class MemoryCache[T](Base, CacheInterface[T]):
    """Cache held in a dict, with TTLs honoured lazily on access.

    Each instance is its own namespace, so there is no ``base_key``: two
    caches never share keys. Expiry is checked when a key is read, not by a
    timer, which keeps the class free of background tasks.

    :param default_ttl_seconds: TTL applied when ``set`` gets none. ``None``
        means entries never expire.
    :param clock: Monotonic clock, injectable so tests can advance time.
    :param name: Optional name for logging.
    """

    def __init__(
        self,
        *,
        default_ttl_seconds: int | None = None,
        clock: Callable[[], float] = time.monotonic,
        name: str | None = None,
    ) -> None:
        super().__init__(pybase_logger_name=name)
        self._default_ttl_seconds = default_ttl_seconds
        self._clock = clock
        # key -> (value, expires_at or None)
        self._entries: dict[str, tuple[T, float | None]] = {}

    async def get(self, key: str) -> T | None:
        if not self._alive(key):
            return None
        return copy.deepcopy(self._entries[key][0])

    async def set(self, key: str, value: T, ttl_seconds: int | None = None) -> None:
        ttl = ttl_seconds if ttl_seconds is not None else self._default_ttl_seconds
        expires_at = self._clock() + ttl if ttl is not None else None
        self._entries[key] = (copy.deepcopy(value), expires_at)

    async def delete(self, key: str) -> bool:
        alive = self._alive(key)
        self._entries.pop(key, None)
        return alive

    async def exists(self, key: str) -> bool:
        return self._alive(key)

    async def get_by_key_prefix(self, key_prefix: str) -> dict[str, T]:
        return {
            key: copy.deepcopy(self._entries[key][0])
            for key in self._alive_keys(key_prefix)
        }

    async def set_many(
        self,
        items: dict[str, T],
        ttl_seconds: int | None = None,
    ) -> None:
        for key, value in items.items():
            await self.set(key, value, ttl_seconds)

    async def delete_by_key_prefix(self, key_prefix: str) -> int:
        keys = self._alive_keys(key_prefix)
        for key in keys:
            del self._entries[key]
        return len(keys)

    async def clear(self) -> None:
        self._entries.clear()

    def _alive(self, key: str) -> bool:
        entry = self._entries.get(key)
        if entry is None:
            return False
        expires_at = entry[1]
        if expires_at is not None and self._clock() >= expires_at:
            del self._entries[key]
            return False
        return True

    def _alive_keys(self, key_prefix: str) -> list[str]:
        return [
            key
            for key in list(self._entries)
            if key.startswith(key_prefix) and self._alive(key)
        ]
