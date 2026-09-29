import asyncio
import json
from unittest.mock import AsyncMock

import pytest

from tests.pubsub.rabbitmq.fakes import (
    BODY,
    FakeIncomingMessage,
    FakePublisher,
    FakeWorker,
    make_manager,
)
from warren.common import SoftFailureException
from warren.pubsub.rabbitmq.aio_pika.publisher import RMQPublisher
from warren.pubsub.rabbitmq.config import RMQConsumerConfig, RMQExchangeConfig
from warren.pubsub.routing import (
    DELIVERY_COUNT_FIELD,
    REPLAY_ROUTING_KEY_FIELD,
    ReplayRouter,
)
from warren.retry_management.retry_worker import RetryWorker
from warren.storage.document_store.memory import MemoryDocumentStore


@pytest.mark.parametrize("redelivered", [False, True])
@pytest.mark.parametrize("recovered", [False, True])
@pytest.mark.parametrize("priority", [0, 2])
def test_retry_replays_original_lane_key_and_amqp_priority(
    redelivered: bool, recovered: bool, priority: int
) -> None:
    async def scenario() -> None:
        control = FakePublisher()
        manager = make_manager(
            FakeWorker(error=SoftFailureException("later")),
            control_publisher=control,
            consumer_config=RMQConsumerConfig(max_deliveries=3),
        )
        body = {**BODY, "lane": "interactive", "priority": priority}
        key = "interactive.parser"
        incoming = FakeIncomingMessage(body, routing_key=key, redelivered=redelivered)
        await manager._process_message(incoming)
        assert incoming.acked
        failed = control.published[0]["data"]
        assert failed["lane"] == "interactive"
        assert failed["priority"] == priority
        assert failed[REPLAY_ROUTING_KEY_FIELD] == key
        if redelivered:
            assert failed[DELIVERY_COUNT_FIELD] == 2

        connection = AsyncMock()
        publisher = RMQPublisher(
            connection,
            RMQExchangeConfig(name="jobs", type="topic"),
            route_func=ReplayRouter(),
        )
        await publisher.setup()
        store = MemoryDocumentStore(collection_name="retries", doc_id_field="retry_key")
        retry = RetryWorker(
            "retry", retry_store=store, republish_publisher=publisher, clock=lambda: 0
        )
        await retry(json.loads(json.dumps(control.published[0])))
        row = await anext(store.query({}))
        if recovered:
            await retry.shutdown()
            retry = RetryWorker(
                "retry",
                retry_store=store,
                republish_publisher=publisher,
                clock=lambda: 10000,
            )
            await retry.schedule_pending()
        else:
            retry._on_timer_fire(row["retry_key"], row["generation"])
        await asyncio.gather(*retry._republish_tasks)
        exchange = connection.create_channel.return_value.declare_exchange.return_value
        exchange.publish.assert_awaited_once()
        call = exchange.publish.call_args
        assert call.kwargs["routing_key"] == key
        assert call.args[0].priority == priority
        assert json.loads(call.args[0].body) == failed
        assert [row async for row in store.query({})] == []
        await retry.shutdown()
        await publisher.teardown()

    asyncio.run(scenario())
