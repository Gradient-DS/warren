"""Pins what ``DefaultWorkerRunner`` hands to the backend factory.

No infrastructure: the factory is replaced by a recorder and the runner's
``_infra`` by a namespace carrying the one attribute the wiring reads.
"""

import asyncio
from types import SimpleNamespace

import pytest
from basics.logging_utils import summarize_exception_chain

from warren.common import MessageConsumerInterface
from warren.exceptions import WarrenError
from warren.pubsub.common import RetryConfig
from warren.pubsub.rabbitmq.config import RMQExchangeConfig
from warren.runtime import backends
from warren.runtime.config import RuntimeConfig, RuntimeRetryConfig
from warren.runtime.infrastructure import (
    RuntimeInfra,
    close_runtime_infrastructure,
    create_runtime_infrastructure,
)
from warren.runtime.runner import DefaultWorkerRunner
from warren.runtime.spec import WorkerFactoryContext, WorkerSpec
from warren.storage.memory_registry import MemoryStoreRegistry
from warren.workers.health import HealthConfig
from warren.workers.workers import FilteringWorkerBase


class _FakeWorker:
    name = "worker-1"
    type = "test_worker"

    async def __call__(self, message: dict) -> dict | None:
        return None


async def _factory(ctx):
    return _FakeWorker()


def test_runner_passes_retry_policy_to_consumer_manager(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict = {}

    def fake_create(config, connection_manager, **kwargs):
        captured.update(kwargs)
        return object()

    monkeypatch.setattr(backends, "create_consumer_manager", fake_create)
    config = RuntimeConfig(
        retry=RuntimeRetryConfig(policy=RetryConfig(max_delay_cap=900))
    )
    runner = DefaultWorkerRunner(
        config,
        "worker-1",
        worker_type="test_worker",
        worker_spec=WorkerSpec(collections={}, factory=_factory),
        exchange=RMQExchangeConfig(name="jobs", type="fanout"),
    )
    runner._infra = SimpleNamespace(pubsub_connection_manager=object())

    runner._create_consumer_manager(_FakeWorker(), None, None, None)

    assert captured["retry_config"].max_delay_cap == 900


def test_runner_takes_health_config_from_runtime_config() -> None:
    config = RuntimeConfig(health=HealthConfig(port=9090))
    runner = DefaultWorkerRunner(
        config,
        "worker-1",
        worker_type="test_worker",
        worker_spec=WorkerSpec(collections={}, factory=_factory),
        exchange=RMQExchangeConfig(name="jobs", type="fanout"),
    )

    assert runner._health.port == 9090


# ---------------------------------------------------------------------------
# Memory backend: injected infrastructure
# ---------------------------------------------------------------------------


def _memory_config() -> RuntimeConfig:
    return RuntimeConfig(backend="memory", health=HealthConfig(enabled=False))


class _NoopWorker(FilteringWorkerBase):
    def should_process(self, message: dict) -> bool:
        return False

    async def process(self, message: dict) -> dict | None:
        return None


async def _noop_factory(ctx: WorkerFactoryContext) -> MessageConsumerInterface:
    assert ctx.mongo_client is None
    assert ctx.redis_client is None
    return _NoopWorker(ctx.worker_name, worker_type=ctx.worker_type)


_SPEC = WorkerSpec(collections={"write": "out"}, factory=_noop_factory)
_EXCHANGE = RMQExchangeConfig(name="jobs", type="fanout")


def test_memory_infrastructure_builds_no_clients() -> None:
    async def scenario() -> RuntimeInfra:
        infra = await create_runtime_infrastructure(_memory_config())
        await close_runtime_infrastructure(infra)
        return infra

    infra = asyncio.run(scenario())
    assert infra.mongo_client is None
    assert infra.redis_client is None


def test_injected_infra_is_used_and_not_closed_by_the_runner() -> None:
    async def scenario() -> int:
        config = _memory_config()
        infra = await create_runtime_infrastructure(config)
        stores = MemoryStoreRegistry()
        runner = DefaultWorkerRunner(
            config,
            "w-0",
            worker_type="w",
            worker_spec=_SPEC,
            exchange=_EXCHANGE,
            infra=infra,
            results_stores={"write": await stores.results_store("out")},
        )
        await runner.setup()
        await runner.teardown()
        # Still usable: the runner did not tear down what it did not create.
        delivered = infra.pubsub_connection_manager.broker.publish(_EXCHANGE, "", b"{}")
        await close_runtime_infrastructure(infra)
        return delivered

    assert asyncio.run(scenario()) == 1


def test_memory_runner_without_injected_stores_fails_with_a_pointer() -> None:
    async def scenario() -> None:
        config = _memory_config()
        infra = await create_runtime_infrastructure(config)
        runner = DefaultWorkerRunner(
            config,
            "w-0",
            worker_type="w",
            worker_spec=_SPEC,
            exchange=_EXCHANGE,
            infra=infra,
        )
        try:
            await runner.setup()
        finally:
            await runner.teardown()
            await close_runtime_infrastructure(infra)

    with pytest.raises(WarrenError) as excinfo:
        asyncio.run(scenario())
    assert "create_in_process_runners" in summarize_exception_chain(excinfo.value)
