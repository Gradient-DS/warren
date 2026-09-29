import ast
import asyncio
from pathlib import Path

import pytest

from tests.storage.results.doubles import RedisClient
from warren.storage.cache.interface import CacheOperationError
from warren.storage.cache.redis import RedisDictCache


def _writes_without_ttl(source: str) -> list[int]:
    missing = []
    for node in ast.walk(ast.parse(source)):
        if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
            continue
        method = node.func.attr
        if method not in {"set", "set_many"}:
            continue
        # Event.set() has no payload and is not a cache write.
        if method == "set" and not node.args and not node.keywords:
            continue
        # Scope context assignment is not a cache write.
        if (
            isinstance(node.func.value, ast.Name)
            and node.func.value.id == "current_scope"
        ):
            continue
        position = 2 if method == "set" else 1
        ttl = node.args[position] if len(node.args) > position else None
        for keyword in node.keywords:
            if keyword.arg in {"ttl_seconds", "ex", "px", "exat", "pxat"}:
                ttl = keyword.value
        if ttl is None or (isinstance(ttl, ast.Constant) and ttl.value is None):
            missing.append(node.lineno)
    return missing


def test_framework_cache_writes_pass_a_ttl() -> None:
    root = Path(__file__).resolve().parents[3] / "warren"
    missing = [
        f"{path.relative_to(root)}:{line}"
        for path in sorted(root.rglob("*.py"))
        for line in _writes_without_ttl(path.read_text())
    ]
    assert not missing, "Cache writes without TTL: " + ", ".join(missing)


@pytest.mark.parametrize(
    "call",
    [
        "cache.set('key', value)",
        "cache.set_many(items)",
        "cache.set('key', value, ttl_seconds=None)",
        "cache.set_many(items, None)",
        "client.set('key', value, ex=None)",
    ],
)
def test_guard_rejects_missing_or_null_ttls(call: str) -> None:
    assert _writes_without_ttl(call) == [1]


@pytest.mark.parametrize(
    "call",
    [
        "cache.set('key', value, ttl_seconds=30)",
        "cache.set_many(items, ttl)",
        "client.set('key', value, ex=ttl)",
        "event.set()",
        "current_scope.set(value)",
    ],
)
def test_guard_accepts_explicit_ttls_and_events(call: str) -> None:
    assert _writes_without_ttl(call) == []


@pytest.mark.parametrize("default", [None, 120])
def test_redis_writes_always_have_a_finite_expiry(default: int | None) -> None:
    async def run() -> None:
        client = RedisClient()
        cache = RedisDictCache(client, base_key="cache", default_ttl_seconds=default)
        await cache.set("a", {})
        await cache.set_many({"b": {}, "c": {}})
        await cache.set("d", {}, ttl_seconds=17)
        await cache.set_many({"e": {}}, ttl_seconds=19)
        expected = 3600 if default is None else default
        assert client.ttls == {
            "cache:a": expected,
            "cache:b": expected,
            "cache:c": expected,
            "cache:d": 17,
            "cache:e": 19,
        }

    asyncio.run(run())


@pytest.mark.parametrize("ttl", [0, -1])
def test_non_positive_ttls_are_rejected(ttl: int) -> None:
    async def run() -> None:
        client = RedisClient()
        with pytest.raises(ValueError, match="must be positive"):
            RedisDictCache(client, base_key="cache", default_ttl_seconds=ttl)
        cache = RedisDictCache(client, base_key="cache")
        with pytest.raises(CacheOperationError):
            await cache.set("a", {}, ttl_seconds=ttl)
        with pytest.raises(CacheOperationError):
            await cache.set_many({"b": {}}, ttl_seconds=ttl)
        assert not client.entries

    asyncio.run(run())
