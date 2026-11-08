import asyncio
from unittest.mock import AsyncMock, Mock

import pytest

from tests.pubsub.kafka.test_consumer_manager import _manager, _msg
from tests.pubsub.rabbitmq.fakes import FakeIncomingMessage, FakePublisher, FakeWorker, make_manager
from warren.common import HardFailureException, SoftFailureException


@pytest.mark.parametrize("backend", ["rabbitmq", "kafka"])
@pytest.mark.parametrize("raw", [b"[]", b"null", b'{"data":null}', b"\xff", b"[" * 2000 + b"]" * 2000])
def test_malformed_delivery_is_rejected_and_next_delivery_runs(backend: str, raw: bytes) -> None:
    worker = FakeWorker()
    if backend == "rabbitmq":
        manager = make_manager(worker)
        bad, good = FakeIncomingMessage(raw), FakeIncomingMessage()
    else:
        manager, connection = _manager(worker)
        transport = connection.kafka_consumer
        manager._kafka_consumer = transport
        bad, good = _msg(raw), _msg({"data": {}}, offset=8)

    async def run() -> None:
        await manager._process_message(bad)
        assert worker.calls == []
        await manager._process_message(good)

    asyncio.run(run())
    assert len(worker.calls) == 1
    if backend == "rabbitmq":
        assert bad.rejected is False
        assert good.acked
    else:
        assert len(transport.commits) == 2


@pytest.mark.parametrize("backend", ["rabbitmq", "kafka"])
@pytest.mark.parametrize("failure", [SoftFailureException("later"), HardFailureException("bad")])
def test_failure_handler_exception_still_rejects(backend: str, failure: Exception) -> None:
    worker = FakeWorker(error=failure)
    if backend == "rabbitmq":
        manager = make_manager(worker, control_publisher=FakePublisher())
        message = FakeIncomingMessage()
    else:
        manager, connection = _manager(worker)
        transport = connection.kafka_consumer
        manager._kafka_consumer = transport
        message = _msg({"data": {}})
    manager._handle_soft_failure = AsyncMock(side_effect=ValueError("invalid retry"))
    manager._handle_hard_failure = AsyncMock(side_effect=ValueError("invalid retry"))
    asyncio.run(manager._process_message(message))
    if backend == "rabbitmq":
        assert message.rejected is False
    else:
        assert len(transport.commits) == 1


@pytest.mark.parametrize("backend", ["rabbitmq", "kafka"])
def test_identity_exception_still_rejects(backend: str) -> None:
    worker = FakeWorker()
    if backend == "rabbitmq":
        manager = make_manager(worker)
        message = FakeIncomingMessage()
    else:
        manager, connection = _manager(worker)
        transport = connection.kafka_consumer
        manager._kafka_consumer = transport
        message = _msg({"data": {}})
    manager._extract_identity = Mock(side_effect=ValueError("invalid identity"))
    asyncio.run(manager._process_message(message))
    assert worker.calls == []
    if backend == "rabbitmq":
        assert message.rejected is False
    else:
        assert len(transport.commits) == 1


@pytest.mark.parametrize("retry", [None, [], "bad", {"count": []}])
@pytest.mark.parametrize("backend", ["rabbitmq", "kafka"])
def test_malformed_retry_cannot_escape_failure_handler(backend: str, retry: object) -> None:
    worker = FakeWorker(error=SoftFailureException("later"))
    body = {"data": {}, "retry": retry}
    if backend == "rabbitmq":
        manager = make_manager(worker, control_publisher=FakePublisher())
        message = FakeIncomingMessage(body)
    else:
        manager, connection = _manager(worker, control_publisher=FakePublisher())
        transport = connection.kafka_consumer
        manager._kafka_consumer = transport
        message = _msg(body)
    asyncio.run(manager._process_message(message))
    if backend == "rabbitmq":
        assert message.rejected is False
    else:
        assert len(transport.commits) == 1


def test_reject_exception_closes_delivery_channel() -> None:
    manager = make_manager(FakeWorker())
    message = Mock(body=b"null")
    message.reject = AsyncMock(side_effect=RuntimeError("cannot reject"))
    message.channel.close = AsyncMock()
    asyncio.run(manager._process_message(message))
    message.channel.close.assert_awaited_once()
