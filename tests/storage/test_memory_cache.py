"""Contract tests for ``CacheInterface``, run against the memory cache."""

import asyncio

from warren.storage.cache.memory import MemoryCache


class _Clock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


def test_set_get_exists_delete() -> None:
    async def scenario() -> tuple:
        cache: MemoryCache[dict] = MemoryCache()
        await cache.set("a", {"x": 1})
        return (
            await cache.get("a"),
            await cache.exists("a"),
            await cache.delete("a"),
            await cache.get("a"),
            await cache.delete("a"),
        )

    assert asyncio.run(scenario()) == ({"x": 1}, True, True, None, False)


def test_explicit_ttl_expires() -> None:
    clock = _Clock()

    async def scenario() -> tuple:
        cache: MemoryCache[bytes] = MemoryCache(clock=clock)
        await cache.set("a", b"x", ttl_seconds=10)
        clock.now += 9
        alive = await cache.get("a")
        clock.now += 2
        return alive, await cache.get("a"), await cache.exists("a")

    assert asyncio.run(scenario()) == (b"x", None, False)


def test_default_ttl_applies_and_none_means_forever() -> None:
    clock = _Clock()

    async def scenario() -> tuple:
        expiring: MemoryCache[bytes] = MemoryCache(default_ttl_seconds=5, clock=clock)
        forever: MemoryCache[bytes] = MemoryCache(clock=clock)
        await expiring.set("a", b"x")
        await forever.set("a", b"x")
        clock.now += 1_000_000
        return await expiring.get("a"), await forever.get("a")

    assert asyncio.run(scenario()) == (None, b"x")


def test_prefix_operations_match_literally() -> None:
    async def scenario() -> tuple:
        cache: MemoryCache[dict] = MemoryCache()
        await cache.set_many(
            {"doc123:0": {"i": 0}, "doc123:1": {"i": 1}, "doc1234:0": {"i": 9}}
        )
        narrow = await cache.get_by_key_prefix("doc123:")
        broad = await cache.get_by_key_prefix("doc123")
        deleted = await cache.delete_by_key_prefix("doc123:")
        return (
            sorted(narrow),
            sorted(broad),
            deleted,
            sorted(await cache.get_by_key_prefix("")),
        )

    # "4" sorts before ":", so "doc1234:0" comes first in the broad match
    assert asyncio.run(scenario()) == (
        ["doc123:0", "doc123:1"],
        ["doc1234:0", "doc123:0", "doc123:1"],
        2,
        ["doc1234:0"],
    )


def test_clear_empties_the_cache() -> None:
    async def scenario() -> dict:
        cache: MemoryCache[dict] = MemoryCache()
        await cache.set_many({"a": {"x": 1}, "b": {"x": 2}})
        await cache.clear()
        return await cache.get_by_key_prefix("")

    assert asyncio.run(scenario()) == {}


def test_values_are_isolated_from_callers() -> None:
    async def scenario() -> dict | None:
        cache: MemoryCache[dict] = MemoryCache()
        value = {"x": {"y": 1}}
        await cache.set("a", value)
        value["x"]["y"] = 99
        (await cache.get("a"))["x"]["y"] = 42  # type: ignore[index]
        return await cache.get("a")

    assert asyncio.run(scenario()) == {"x": {"y": 1}}
