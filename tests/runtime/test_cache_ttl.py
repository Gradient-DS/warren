import asyncio
from pathlib import Path

import pytest
from pydantic import ValidationError

from tests.storage.results.doubles import MongoClient, RedisClient
from warren.common import MessageConsumerInterface
from warren.pubsub.rabbitmq.config import RMQExchangeConfig
from warren.retry_management.retry_worker import RetryWorker
from warren.retry_management.retry_worker_runner import RetryWorkerRunner
from warren.runtime.config import RuntimeConfig
from warren.runtime.infrastructure import RuntimeInfra
from warren.runtime.runner import DefaultWorkerRunner
from warren.runtime.spec import WorkerFactoryContext, WorkerSpec
from warren.storage.documents.interface import ResolveDocumentFunc
from warren.storage.documents.location import DocumentLocation
from warren.storage.results import ResultItem
from warren.storage.results.factories import create_binary_results_store


async def _factory(context: WorkerFactoryContext) -> MessageConsumerInterface:
    msg = "Worker construction is not needed"
    raise AssertionError(msg)


async def _resolve(location: DocumentLocation) -> bytes:
    return b"payload"


class Runner(DefaultWorkerRunner):
    def _create_resolvers(self) -> dict[str, ResolveDocumentFunc]:
        return {"test": _resolve}


def test_cache_defaults() -> None:
    config = RuntimeConfig()
    assert config.documents.cache_ttl_seconds == 86400
    assert config.results.cache_ttl_seconds == 3600


@pytest.mark.parametrize("section", ["documents", "results"])
@pytest.mark.parametrize("ttl", [0, -1, None, float("inf")])
def test_invalid_cache_ttls(section: str, ttl: object) -> None:
    with pytest.raises(ValidationError):
        RuntimeConfig.model_validate({section: {"cache_ttl_seconds": ttl}})


def test_runtime_ttls_reach_single_batch_and_fetcher_cache_writes(
    tmp_path: Path,
) -> None:
    path = tmp_path / "config.yaml"
    path.write_text(
        "documents: {cache_ttl_seconds: 123}\nresults: {cache_ttl_seconds: 45}\n"
    )
    config = RuntimeConfig.from_yaml(path)

    async def run() -> None:
        mongo, redis = MongoClient(), RedisClient()
        runner = Runner(
            config,
            "test",
            worker_type="test",
            worker_spec=WorkerSpec(collections={"write": "results"}, factory=_factory),
            exchange=RMQExchangeConfig(name="jobs"),
            infra=RuntimeInfra(mongo, redis, None),
        )
        store = (await runner._create_default_results_stores())["write"]
        await store.store({}, "a")
        await store.store_many([ResultItem({}, "b"), ResultItem({}, "c")])
        assert len(redis.entries) == 3
        assert set(redis.ttls.values()) == {45}
        redis.entries.clear()
        redis.ttls.clear()
        await store.get_result("a")
        assert list(redis.ttls.values()) == [45]
        fetcher = runner._create_default_document_fetcher()
        location = DocumentLocation(location_type="test")
        for job_id in (None, "j"):
            assert await fetcher("a", location, job_id=job_id) == b"payload"
        assert redis.ttls["documents:doc:a"] == 123
        assert redis.ttls["documents:doc:a:j"] == 123

    asyncio.run(run())


@pytest.mark.parametrize("ttl", [3600, 17])
def test_binary_cache_write_and_read_through_expire(ttl: int) -> None:
    async def run() -> None:
        mongo, redis = MongoClient(), RedisClient()
        options = {} if ttl == 3600 else {"cache_ttl_seconds": ttl}
        store = await create_binary_results_store("results", mongo, redis, **options)
        await store.store_bytes(b"payload", doc_id="a", job_id="j")
        assert redis.ttls == {"documents:doc:a:j": ttl}
        redis.entries.clear()
        redis.ttls.clear()
        assert await store.get_bytes("a", job_id="j") == b"payload"
        assert redis.ttls == {"documents:doc:a:j": ttl}

    asyncio.run(run())


@pytest.mark.parametrize("cap", [300, 7200])
def test_retry_cache_outlives_the_delay_cap(cap: int) -> None:
    async def run() -> None:
        config = RuntimeConfig.model_validate(
            {"retry": {"policy": {"max_delay_cap": cap}}}
        )
        mongo, redis = MongoClient(), RedisClient()
        runner = RetryWorkerRunner(
            config,
            "retry",
            exchange=RMQExchangeConfig(name="jobs"),
            infra=RuntimeInfra(mongo, redis, None),
        )
        store = await runner._create_default_retry_store()
        await store.insert(
            {RetryWorker.REQUIRED_DOC_ID_FIELD: "retry-1"}, overwrite_existing=True
        )
        assert redis.ttls == {"retry:retries:retry-1": cap + 60}

    asyncio.run(run())
