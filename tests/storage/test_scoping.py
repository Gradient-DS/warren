import asyncio

import pytest

from tests.storage.scoping_doubles import MongoClient, RedisClient
from warren.common import HardFailureException
from warren.storage.cache.redis import RedisDictCache
from warren.storage.document_store.interface import DocumentNotFoundError
from warren.storage.document_store.mongodb import MongoDBDocumentStore
from warren.storage.documents.factories import create_cached_document_fetcher
from warren.storage.documents.location import DocumentLocation
from warren.storage.job_results.mongodb import MongoDBJobResultsStore
from warren.storage.jobs.mongodb import MongoDBJobStore
from warren.storage.publishing_tracker.mongodb import MongoDBPublishingTracker
from warren.storage.results.factories import (
    create_binary_results_store,
    create_default_results_store,
)
from warren.storage.results.interface import ResultItem, ResultNotFound
from warren.storage.scoping import ScopedDatabase, current_scope


@pytest.mark.parametrize("raw", [None, "", "A", "a_b", "a" * 41, "a\n", 12, {}, []])
@pytest.mark.parametrize("required", [False, True])
def test_scope_is_validated_only_on_access(raw: object, required: bool) -> None:
    async def run() -> None:
        mongo, redis = MongoClient(), RedisClient()
        resolver = ScopedDatabase(mongo, required=required, default_database="control")
        results = await create_default_results_store(
            "results", mongo, redis, scoped_database=resolver
        )
        assert not mongo.databases
        token = current_scope.set(raw)
        try:
            cache = RedisDictCache(
                redis, base_key="cache", scoping_enabled=True, scope_required=required
            )
            if raw is None and not required:
                assert resolver().name == "control"
                await cache.set("key", {})
                assert "cache:key" in redis.entries
            else:
                reason = "missing scope" if raw is None else "malformed scope"
                with pytest.raises(HardFailureException, match=f"^{reason}$"):
                    await results.store({}, "item")
                with pytest.raises(HardFailureException, match=f"^{reason}$"):
                    await cache.get("key")
                assert not mongo.databases
                assert not redis.entries
        finally:
            current_scope.reset(token)

    asyncio.run(run())


def test_content_and_caches_are_isolated() -> None:
    async def run() -> None:
        mongo, redis = MongoClient(), RedisClient()
        resolver = ScopedDatabase(mongo)
        results = await create_default_results_store(
            "results", mongo, redis, scoped_database=resolver
        )
        binary = await create_binary_results_store(
            "binary", mongo, redis, scoped_database=resolver
        )
        registry = MongoDBDocumentStore(
            mongo,
            database_name="control",
            collection_name="documents",
            doc_id_field="doc_id",
            scoped_database=resolver,
        )
        await registry.setup()
        fetched: list[str] = []

        async def resolve(location: DocumentLocation) -> bytes:
            fetched.append(current_scope.get())
            return current_scope.get().encode()

        fetcher = create_cached_document_fetcher(
            redis_client=redis, resolvers={"path": resolve}, scoping_enabled=True
        )
        location = DocumentLocation(location_type="path", path="unused")
        token = current_scope.set("a")
        try:
            await results.store_many([ResultItem({"value": "a"}, "item", 0, "job")])
            await registry.insert({"doc_id": "item", "value": "a"})
            await binary.store_bytes(b"a", doc_id="item", job_id="job")
            assert await fetcher("fetched", location) == b"a"
            current_scope.set("b")
            with pytest.raises(ResultNotFound):
                await results.get_result("item", job_id="job")
            with pytest.raises(DocumentNotFoundError):
                await registry.get_document("item")
            with pytest.raises(ResultNotFound):
                await binary.get_bytes("item", job_id="job")
            assert await fetcher("fetched", location) == b"b"
            await results.store({"value": "b"}, "item", job_id="job")
            await binary.store_bytes(b"b", doc_id="item", job_id="job")
            current_scope.set("a")
            assert (await results.get_result("item", job_id="job")).result == {
                "value": "a"
            }
            assert (await registry.get_document("item"))["value"] == "a"
            assert await binary.get_bytes("item", job_id="job") == b"a"
            assert await fetcher("fetched", location) == b"a"
            redis.entries.clear()
            assert (await results.get_result("item", job_id="job")).result == {
                "value": "a"
            }
            assert await binary.get_bytes("item", job_id="job") == b"a"
            assert fetched == ["a", "b"]
            assert set(mongo.databases) == {"wr_a", "wr_b"}
            assert all(key.startswith("s:a:") for key in redis.entries)
        finally:
            current_scope.reset(token)

    asyncio.run(run())


def test_indexes_are_lazy_and_serialized_per_database() -> None:
    async def run() -> None:
        mongo = MongoClient()
        resolver = ScopedDatabase(mongo)
        stores = [
            await create_default_results_store(
                "results", mongo, scoped_database=resolver
            )
            for _ in range(2)
        ]
        assert not mongo.databases

        async def write(scope: str, index: int) -> None:
            token = current_scope.set(scope)
            try:
                await stores[index % 2].store({"value": scope}, str(index))
            finally:
                current_scope.reset(token)

        await asyncio.gather(
            *(write(scope, i) for scope in ("a", "b") for i in range(10))
        )
        for scope in ("a", "b"):
            collection = mongo[f"wr_{scope}"]["results"]
            assert len(collection.rows) == 10
            assert collection.index_calls == 2
            assert all(row["result"] == {"value": scope} for row in collection.rows)

    asyncio.run(run())


@pytest.mark.parametrize("scope", [None, "a", "INVALID", {}, 1])
@pytest.mark.parametrize("enabled", [False, True])
def test_control_rows_record_only_valid_scopes(scope: object, enabled: bool) -> None:
    async def run() -> None:
        mongo = MongoClient()
        jobs = MongoDBJobStore(mongo, database_name="control", scoping_enabled=enabled)
        results = MongoDBJobResultsStore(
            mongo, database_name="control", scoping_enabled=enabled
        )
        tracker = MongoDBPublishingTracker(
            mongo, database_name="control", scoping_enabled=enabled
        )
        token = current_scope.set(scope)
        try:
            await jobs.setup()
            await results.setup()
            await tracker.setup()
            await jobs.create_job("output")
            await results.record_success("job", "output", "item", "worker", "1")
            await results.record_soft_failure(
                "job", "output", "item", "worker", "1", "worker", "2", 1, "retry"
            )
            await results.record_hard_failure(
                "job", "output", "item", "worker", "1", "worker", "2", "failed"
            )
            await tracker.record_success("job", "item")
            await tracker.record_failure("job", None, "source", "failed", "load")
            for collection in mongo["control"].collections.values():
                for row in collection.rows:
                    assert ({"scope": row["scope"]} if "scope" in row else {}) == (
                        {"scope": "a"} if enabled and scope == "a" else {}
                    )
                assert (
                    any(
                        index["key"] == [("scope", 1)]
                        for index in collection.indexes.values()
                    )
                    == enabled
                )
            job_id = await jobs.create_job("output", scope="explicit")
            assert (await jobs.get_job(job_id)).get("scope") == (
                "explicit" if enabled else None
            )
        finally:
            current_scope.reset(token)

    asyncio.run(run())


def test_cache_namespace_operations_stay_in_scope() -> None:
    async def run() -> None:
        redis = RedisClient()
        cache = RedisDictCache(redis, base_key="results", scoping_enabled=True)
        token = current_scope.set("a")
        try:
            await cache.set_many({"part:0": {"value": "a"}, "index": {"parts": [0]}})
            current_scope.set("b")
            await cache.set("part:0", {"value": "b"})
            assert await cache.get_by_key_prefix("part:") == {"part:0": {"value": "b"}}
            await cache.clear()
            assert not await cache.exists("part:0")
            current_scope.set("a")
            assert await cache.exists("part:0")
            assert await cache.delete_by_key_prefix("part:") == 1
            assert await cache.get("index") == {"parts": [0]}
        finally:
            current_scope.reset(token)

    asyncio.run(run())


def test_disabled_storage_ignores_raw_scope() -> None:
    async def run() -> None:
        mongo, redis = MongoClient(), RedisClient()
        store = await create_default_results_store(
            "results", mongo, redis, database_name="control"
        )
        token = current_scope.set("INVALID")
        try:
            await store.store({"value": 1}, "item")
            current_scope.set("a")
            assert (await store.get_result("item")).result == {"value": 1}
            assert set(mongo.databases) == {"control"}
            assert all(key.startswith("results:") for key in redis.entries)
        finally:
            current_scope.reset(token)

    asyncio.run(run())


def test_scope_failure_is_not_swallowed_as_a_cache_miss() -> None:
    async def run() -> None:
        mongo, redis = MongoClient(), RedisClient()
        resolver = ScopedDatabase(mongo)
        results = await create_default_results_store(
            "results", mongo, redis, scoped_database=resolver
        )
        binary = await create_binary_results_store(
            "binary", mongo, redis, scoped_database=resolver
        )
        fetcher = create_cached_document_fetcher(
            redis_client=redis, resolvers={}, scoping_enabled=True
        )
        with pytest.raises(HardFailureException, match="missing scope"):
            await results.get_result("item")
        with pytest.raises(HardFailureException, match="missing scope"):
            await binary.get_bytes("item")
        with pytest.raises(HardFailureException, match="missing scope"):
            await fetcher("item", DocumentLocation(location_type="path", path="unused"))
        assert not mongo.databases

    asyncio.run(run())
