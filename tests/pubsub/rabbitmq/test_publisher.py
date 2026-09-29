import asyncio
import json
from unittest.mock import AsyncMock, patch

import aio_pika
import pytest

from warren.pubsub.rabbitmq.aio_pika.publisher import RMQPublisher
from warren.pubsub.rabbitmq.config import RMQExchangeConfig


@pytest.mark.parametrize(
    "fields",
    [
        {},
        {"priority": None},
        {"priority": True},
        {"priority": "2"},
        {"priority": 0},
        {"priority": 2},
        {"priority": 255},
    ],
)
def test_publisher_sets_only_integer_priority(fields: dict) -> None:
    async def scenario() -> None:
        connection = AsyncMock()
        publisher = RMQPublisher(
            connection, RMQExchangeConfig(name="jobs", type="fanout")
        )
        await publisher.setup()
        body = {"data_type": "raw_document", **fields}
        with patch(
            "warren.pubsub.rabbitmq.aio_pika.publisher.aio_pika.Message",
            wraps=aio_pika.Message,
        ) as constructor:
            await publisher(body)
        priority = fields.get("priority")
        if type(priority) is int:
            assert constructor.call_args.kwargs["priority"] == priority
        else:
            assert "priority" not in constructor.call_args.kwargs
        exchange = connection.create_channel.return_value.declare_exchange.return_value
        message = exchange.publish.call_args.args[0]
        assert json.loads(message.body) == body
        if type(priority) is int:
            assert message.priority == priority
        await publisher.teardown()

    asyncio.run(scenario())
