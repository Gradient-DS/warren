import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from tests.pubsub.rabbitmq.fakes import FakeWorker
from warren.jobs.publishing.job_publication_worker_runner import JobPublicationWorkerRunner
from warren.jobs.status.job_status_worker_runner import JobStatusWorkerRunner
from warren.pubsub.common import PubSubSetupError, Route
from warren.pubsub.rabbitmq.aio_pika.consumer import RMQConsumerManager
from warren.pubsub.rabbitmq.aio_pika.publisher import RMQPublisher
from warren.pubsub.rabbitmq.aio_pika.topology import declare_exchange
from warren.pubsub.rabbitmq.config import (
    RMQConsumerConfig,
    RMQConsumerManagerConfig,
    RMQExchangeConfig,
    RMQQueueConfig,
)
from warren.pubsub.routing import observer_exchange
from warren.retry_management.retry_worker_runner import RetryWorkerRunner
from warren.runtime.config import RuntimeConfig
from warren.runtime.infrastructure import RuntimeInfra
from warren.runtime.runner import DefaultWorkerRunner
from warren.runtime.spec import PublishSpec, WorkerSpec


def connection_for(exchanges: set[str]) -> AsyncMock:
    connection = AsyncMock()

    async def declare(name: str, exchange_type: str, *, durable: bool, passive: bool = False) -> SimpleNamespace:
        if not passive:
            reason = "exchange configuration denied"
            raise PermissionError(reason)
        if name not in exchanges:
            reason = "exchange missing"
            raise LookupError(reason)
        return SimpleNamespace(name=name)

    connection.create_channel.return_value.declare_exchange.side_effect = declare
    return connection


def endpoint(kind: str, exchange: RMQExchangeConfig, connection: AsyncMock) -> RMQPublisher | RMQConsumerManager:
    if kind == "publisher":
        return RMQPublisher(connection, exchange, route=None if exchange.type == "fanout" else Route("raw"))
    return RMQConsumerManager(
        RMQConsumerManagerConfig(
            exchange=exchange,
            queue=RMQQueueConfig(name="q", binding_keys=("raw",)),
            consumer=RMQConsumerConfig(),
        ),
        connection,
        FakeWorker(),
    )


@pytest.mark.parametrize("exchange_type", ["fanout", "topic", "direct"])
@pytest.mark.parametrize("kind", ["publisher", "consumer"])
def test_passive_setup_without_exchange_configure_rights(exchange_type: str, kind: str) -> None:
    connection = connection_for({"jobs"})
    exchange = RMQExchangeConfig(name="jobs", type=exchange_type, passive=True)
    asyncio.run(endpoint(kind, exchange, connection).setup())
    channel = connection.create_channel.return_value
    assert channel.declare_exchange.await_args.kwargs["passive"] is True
    if kind == "consumer":
        channel.declare_queue.return_value.bind.assert_awaited_once_with(
            "jobs", routing_key="" if exchange_type == "fanout" else "raw"
        )


@pytest.mark.parametrize("kind", ["publisher", "consumer"])
def test_missing_passive_exchange_raises_setup_error(kind: str) -> None:
    connection = connection_for(set())
    exchange = RMQExchangeConfig(name="missing", type="fanout", passive=True)
    with pytest.raises(PubSubSetupError) as error:
        asyncio.run(endpoint(kind, exchange, connection).setup())
    cause = error.value
    while cause.__cause__ is not None:
        cause = cause.__cause__
    assert isinstance(cause, LookupError)
    connection.create_channel.return_value.declare_queue.assert_not_awaited()


def test_default_exchange_declaration_remains_active() -> None:
    exchange = RMQExchangeConfig(name="jobs")
    assert exchange.passive is False
    channel = AsyncMock()
    asyncio.run(declare_exchange(channel, exchange))
    channel.declare_exchange.assert_awaited_once_with("jobs", "topic", durable=True, passive=False)


@pytest.mark.parametrize("exchange_type", ["fanout", "topic", "direct"])
@pytest.mark.parametrize("passive", [False, True])
def test_observer_preserves_exchange_declaration_mode(exchange_type: str, passive: bool) -> None:
    exchange = RMQExchangeConfig(name="jobs", type=exchange_type, durable=False, passive=passive)
    restored = RMQExchangeConfig.model_validate(exchange.model_dump())
    observer = observer_exchange(restored)
    assert observer.passive is passive
    assert observer.durable is False


@pytest.mark.parametrize("exchange_type", ["fanout", "topic", "direct"])
@pytest.mark.parametrize("kind", ["processing", "status", "retry", "publication"])
def test_runners_preserve_passive_exchanges(exchange_type: str, kind: str) -> None:
    async def run() -> None:
        exchange = RMQExchangeConfig(name="jobs", type=exchange_type, passive=True)
        observer = observer_exchange(exchange)
        config = RuntimeConfig()
        connection = connection_for({exchange.name, observer.name})
        infra = RuntimeInfra(None, None, connection)
        if kind == "processing":
            spec = WorkerSpec(
                collections={},
                factory=AsyncMock(),
                binding_keys=("raw",),
                publish=PublishSpec(route=None if exchange_type == "fanout" else Route("next")),
            )
            runner = DefaultWorkerRunner(
                config, "worker", worker_type="worker", worker_spec=spec, exchange=exchange, infra=infra
            )
            manager = runner._create_consumer_manager(
                FakeWorker(),
                runner._create_data_publisher(),
                runner._create_control_publisher(),
                runner._create_observer_publisher(),
            )
        else:
            if kind == "status":
                runner = JobStatusWorkerRunner(config, "status", exchange=observer, infra=infra)
            elif kind == "retry":
                runner = RetryWorkerRunner(config, "retry", exchange=observer, republish_exchange=exchange, infra=infra)
            else:
                runner = JobPublicationWorkerRunner(
                    config,
                    "publication",
                    exchange=observer,
                    publish_exchange=exchange,
                    documents_publisher_factory=AsyncMock(),
                )
                runner._infra = infra
            publisher = runner._create_default_publisher()
            if kind == "retry":
                await publisher.setup()
            else:
                runner._publisher = publisher
            manager = runner._create_default_consumer_factory()(FakeWorker())
        await manager.setup()
        calls = connection.create_channel.return_value.declare_exchange.await_args_list
        assert calls
        assert all(call.kwargs["passive"] is True for call in calls)
        expected_exchange = exchange if kind == "processing" else observer
        binding = "" if expected_exchange.type == "fanout" else "raw" if kind == "processing" else "#"
        connection.create_channel.return_value.declare_queue.return_value.bind.assert_awaited_once_with(
            expected_exchange.name, routing_key=binding
        )

    asyncio.run(run())
