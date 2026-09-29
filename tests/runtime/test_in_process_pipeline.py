"""A whole pipeline in one process: memory broker, memory stores, real runners."""

import asyncio
import json

import pytest

from warren.common import MessageConsumerInterface, SoftFailureException
from warren.pubsub.common import RetryConfig
from warren.pubsub.rabbitmq.config import RMQExchangeConfig
from warren.pubsub.routing import MessageFieldRouter, observer_route_func
from warren.runtime import backends
from warren.runtime.config import RuntimeConfig, RuntimeRetryConfig
from warren.runtime.in_process import create_in_process_runners, run_in_process
from warren.runtime.infrastructure import (
    close_runtime_infrastructure,
    create_runtime_infrastructure,
)
from warren.runtime.spec import (
    PipelineSpec,
    PublishSpec,
    WorkerFactoryContext,
    WorkerSpec,
)
from warren.storage.memory_registry import MemoryStoreRegistry
from warren.workers.health import HealthConfig
from warren.workers.workers import FilteringWorkerBase


class _Upper(FilteringWorkerBase):
    """Stage 1: ``raw`` -> ``upper``."""

    def __init__(self, ctx: WorkerFactoryContext) -> None:
        super().__init__(ctx.worker_name, worker_type=ctx.worker_type)
        self._write = ctx.stores["write"]

    def should_process(self, message: dict) -> bool:
        return message.get("data_type") == "raw"

    async def process(self, message: dict) -> dict | None:
        doc_id, job_id = message["data"]["doc_id"], message["job_id"]
        await self._write.store(
            result={"text": message["data"]["text"].upper()},
            doc_id=doc_id,
            job_id=job_id,
        )
        return {
            "data_type": "upper",
            "data": {"doc_id": doc_id},
            "job_id": job_id,
            "origin": {"type": self.type, "name": self.name},
        }


class _Exclaim(FilteringWorkerBase):
    """Stage 2: ``upper`` -> ``final``. Optionally soft-fails once per document."""

    def __init__(self, ctx: WorkerFactoryContext, *, flaky: bool) -> None:
        super().__init__(ctx.worker_name, worker_type=ctx.worker_type)
        self._read, self._write = ctx.stores["read"], ctx.stores["write"]
        self._flaky = flaky
        self._failed_once: set[str] = set()

    def should_process(self, message: dict) -> bool:
        return message.get("data_type") == "upper"

    async def process(self, message: dict) -> dict | None:
        doc_id, job_id = message["data"]["doc_id"], message["job_id"]
        if self._flaky and doc_id not in self._failed_once:
            self._failed_once.add(doc_id)
            reason = "first attempt always fails"
            raise SoftFailureException(reason)
        text = (await self._read.get_result(doc_id=doc_id, job_id=job_id)).result[
            "text"
        ]
        await self._write.store(
            result={"text": text + "!"}, doc_id=doc_id, job_id=job_id
        )
        return {
            "data_type": "final",
            "data": {"doc_id": doc_id},
            "job_id": job_id,
            "origin": {"type": self.type, "name": self.name},
        }


def _pipeline(exchange: RMQExchangeConfig, *, flaky: bool) -> PipelineSpec:
    async def upper(ctx: WorkerFactoryContext) -> MessageConsumerInterface:
        return _Upper(ctx)

    async def exclaim(ctx: WorkerFactoryContext) -> MessageConsumerInterface:
        return _Exclaim(ctx, flaky=flaky)

    routed = exchange.type != "fanout"
    publish = PublishSpec(route_func=MessageFieldRouter()) if routed else PublishSpec()
    return PipelineSpec(
        exchange=exchange,
        workers={
            "upper": WorkerSpec(
                collections={"write": "uppered"},
                factory=upper,
                publish=publish,
                binding_key="raw" if routed else None,
            ),
            "exclaim": WorkerSpec(
                collections={"read": "uppered", "write": "finals"},
                factory=exclaim,
                publish=publish,
                binding_key="upper" if routed else None,
            ),
        },
        result_collections=["uppered", "finals"],
        reference_collection="uppered",
        completion_collection="finals",
        final_data_type="final",
    )


async def _wait_until_completed(stores: MemoryStoreRegistry, job_id: str) -> None:
    """Poll the job store: completion is a stored fact, there is no event to await."""
    while True:
        if (await stores.job_store().get_status(job_id))["completed"]:
            return
        await asyncio.sleep(0.01)


def _run(
    exchange: RMQExchangeConfig, *, flaky: bool
) -> tuple[dict, list[dict], list[str]]:
    docs = {"d1": "hello", "d2": "world"}
    config = RuntimeConfig(
        backend="memory",
        health=HealthConfig(enabled=False),
        retry=RuntimeRetryConfig(
            enabled=True,
            policy=RetryConfig(initial_delay=0, jitter=False),
        ),
    )
    pipeline = _pipeline(exchange, flaky=flaky)

    async def scenario() -> tuple[dict, list[dict], list[str]]:
        infra = await create_runtime_infrastructure(config)
        stores = MemoryStoreRegistry()
        try:
            runners = await create_in_process_runners(
                config, pipeline, infra=infra, stores=stores
            )
            job_id = await stores.job_store().create_job(
                final_data_type=pipeline.final_data_type, num_documents=len(docs)
            )

            async def drive() -> None:
                publisher = backends.create_publisher(
                    config,
                    infra.pubsub_connection_manager,
                    exchange=pipeline.exchange,
                    route_func=observer_route_func(pipeline.exchange)
                    if pipeline.exchange.type == "fanout"
                    else MessageFieldRouter(),
                )
                await publisher.setup()
                for doc_id, text in docs.items():
                    await publisher(
                        {
                            "data_type": "raw",
                            "data": {"doc_id": doc_id, "text": text},
                            "job_id": job_id,
                            "origin": {"type": "publisher", "name": "test"},
                        }
                    )
                await _wait_until_completed(stores, job_id)

            await asyncio.wait_for(run_in_process(runners, until=drive), timeout=10)

            finals = await stores.results_store("finals")
            texts = sorted(
                [
                    (await finals.get_result(doc_id=d, job_id=job_id)).result["text"]
                    for d in docs
                ]
            )
            return (
                await stores.job_store().get_status(job_id),
                await stores.job_results_store().get_stage_counts(job_id),
                texts,
            )
        finally:
            await close_runtime_infrastructure(infra)

    return asyncio.run(scenario())


def test_fanout_pipeline_runs_to_completion() -> None:
    status, stages, texts = _run(
        RMQExchangeConfig(name="jobs", type="fanout"), flaky=False
    )

    assert status["completed"] is True
    assert status["with_failures"] is False
    assert texts == ["HELLO!", "WORLD!"]
    final = next(s for s in stages if s["data_type"] == "final")
    assert final["succeeded"] == 2


def test_soft_failure_recovers_through_the_retry_runner() -> None:
    status, _, texts = _run(RMQExchangeConfig(name="jobs", type="fanout"), flaky=True)

    assert status["completed"] is True
    assert status["with_failures"] is False
    assert texts == ["HELLO!", "WORLD!"]


def test_topic_pipeline_runs_to_completion() -> None:
    status, _, texts = _run(RMQExchangeConfig(name="jobs", type="topic"), flaky=False)

    assert status["completed"] is True
    assert texts == ["HELLO!", "WORLD!"]


def test_first_runner_failure_stops_the_run_and_is_raised() -> None:
    class _Boom:
        async def setup(self) -> None: ...
        async def run(self) -> None:
            msg = "boom"
            raise RuntimeError(msg)

        async def teardown(self) -> None:
            self.torn_down = True

    class _Forever:
        async def setup(self) -> None: ...
        async def run(self) -> None:
            await asyncio.Event().wait()

        async def teardown(self) -> None:
            self.torn_down = True

    boom, forever = _Boom(), _Forever()

    with pytest.raises(RuntimeError, match="boom"):
        asyncio.run(run_in_process([forever, boom]))  # type: ignore[list-item]
    assert boom.torn_down
    assert forever.torn_down


def test_instances_share_queue_and_process_each_item_once() -> None:
    async def scenario() -> None:
        seen: dict[str, list[int]] = {}
        started = asyncio.Event()
        release = asyncio.Event()

        class Worker(FilteringWorkerBase):
            def should_process(self, message: dict) -> bool:
                return True

            async def process(self, message: dict) -> dict | None:
                seen.setdefault(self.name, []).append(message["item"])
                if len(seen) == 3:
                    started.set()
                await release.wait()
                return None

        async def factory(ctx: WorkerFactoryContext) -> MessageConsumerInterface:
            return Worker(ctx.worker_name, worker_type=ctx.worker_type)

        exchange = RMQExchangeConfig(name="jobs", type="fanout")
        pipeline = _pipeline(exchange, flaky=False)
        pipeline.workers.clear()
        pipeline.workers["worker"] = WorkerSpec({}, factory)
        config = RuntimeConfig(backend="memory", health=HealthConfig(enabled=False))
        infra = await create_runtime_infrastructure(config)
        runners = []
        try:
            runners = await create_in_process_runners(
                config,
                pipeline,
                infra=infra,
                stores=MemoryStoreRegistry(),
                instances={"worker": 3},
            )
            assert [r._worker_name for r in runners[:3]] == [
                "worker-0",
                "worker-1",
                "worker-2",
            ]
            for runner in runners:
                await runner.setup()
            managers = [r._consumer_manager for r in runners[:3]]
            assert len({id(m._queue) for m in managers}) == 1
            for manager in managers:
                await manager.start_consuming()
            broker = infra.pubsub_connection_manager.broker
            for i in range(3):
                broker.publish(exchange, "", json.dumps({"item": i}).encode())
            async with asyncio.timeout(1):
                await started.wait()
            assert sorted(item for items in seen.values() for item in items) == [
                0,
                1,
                2,
            ]
        finally:
            release.set()
            for runner in reversed(runners):
                await runner.teardown()
            await close_runtime_infrastructure(infra)

    asyncio.run(scenario())


@pytest.mark.parametrize("instances", [{"upper": 0}, {"upper": -1}, {"missing": 1}])
def test_invalid_instance_counts_fail_before_setup(instances: dict[str, int]) -> None:
    async def scenario() -> None:
        config = RuntimeConfig(backend="memory")
        infra = await create_runtime_infrastructure(config)
        try:
            with pytest.raises(ValueError, match=r"Instance count|Unknown worker type"):
                await create_in_process_runners(
                    config,
                    _pipeline(
                        RMQExchangeConfig(name="jobs", type="fanout"), flaky=False
                    ),
                    infra=infra,
                    stores=MemoryStoreRegistry(),
                    instances=instances,
                )
        finally:
            await close_runtime_infrastructure(infra)

    asyncio.run(scenario())
