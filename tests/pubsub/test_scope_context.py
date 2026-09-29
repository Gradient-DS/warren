import asyncio

import pytest

from tests.pubsub.rabbitmq.fakes import FakePublisher, FakeWorker
from tests.pubsub.test_handler_timeout import make_consumer
from warren.common import HardFailureException, SoftFailureException
from warren.runtime.config import RuntimeConfig
from warren.storage.scoping import current_scope, get_current_scope
from warren.workers.messages import MessageOrigin, ProcessingMessage


def test_message_scope_roundtrip_and_derive() -> None:
    message = ProcessingMessage(
        "input", {}, "job-1", MessageOrigin("source", "1"), scope="a"
    )
    raw = {**message.to_dict(), "future_field": True}
    derived = ProcessingMessage.create_from(raw).derive(
        data_type="output", data={}, origin=MessageOrigin("worker", "1")
    )
    assert derived.to_dict()["scope"] == "a"
    raw.pop("scope")
    assert "scope" not in ProcessingMessage.create_from(raw).to_dict()
    assert RuntimeConfig().scoping.model_dump() == {
        "enabled": False,
        "required": True,
        "database_prefix": "wr_",
    }


@pytest.mark.parametrize("backend", ["memory", "rabbitmq", "kafka"])
@pytest.mark.parametrize("scope", [None, "a", "INVALID", {"raw": True}])
@pytest.mark.parametrize("sync", [False, True])
def test_handler_receives_raw_scope_and_resets(
    backend: str, scope: object, sync: bool
) -> None:
    class SyncWorker(FakeWorker):
        def __call__(self, message: dict) -> dict:
            assert get_current_scope() == scope
            return {"data_type": "output"}

    class AsyncWorker(FakeWorker):
        async def __call__(self, message: dict) -> dict:
            await asyncio.sleep(0)
            assert get_current_scope() == scope
            return {"data_type": "output"}

    async def run() -> None:
        token = current_scope.set("outer")
        try:
            publisher = FakePublisher()
            worker = SyncWorker() if sync else AsyncWorker()
            manager, process = await make_consumer(backend, worker, publisher, None)
            await process({"data_type": "input", "scope": scope})
            assert get_current_scope() == "outer"
            assert publisher.published == [{"data_type": "output", "scope": scope}]
            await manager.stop_consuming()
        finally:
            current_scope.reset(token)

    asyncio.run(run())


@pytest.mark.parametrize("backend", ["memory", "rabbitmq", "kafka"])
@pytest.mark.parametrize("soft", [False, True])
def test_failure_envelopes_preserve_scope(backend: str, soft: bool) -> None:
    class FailingWorker(FakeWorker):
        async def __call__(self, message: dict) -> None:
            assert get_current_scope() == "a"
            reason = "failed"
            if soft:
                raise SoftFailureException(reason)
            raise HardFailureException(reason)

    async def run() -> None:
        publisher = FakePublisher()
        manager, process = await make_consumer(
            backend, FailingWorker(), publisher, None
        )
        await process({"data_type": "input", "scope": "a"})
        assert get_current_scope() is None
        envelope = publisher.published[0]
        assert envelope["scope"] == envelope["data"]["scope"] == "a"
        assert envelope["data_type"] == ("soft-failure" if soft else "hard-failure")
        await manager.stop_consuming()

    asyncio.run(run())
