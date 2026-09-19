"""Unit tests for ``MemoryConsumerManager``.

The real ``MemoryBroker`` is the transport here, so no transport double is
needed. The decision matrix mirrors ``tests/pubsub/kafka/
test_consumer_manager.py``: ack means the message is gone, requeue means
it is back on the queue.
"""

import asyncio
import json

import pytest

from warren.common import HardFailureException, SoftFailureException
from warren.pubsub.common import PublishFailureException, RetryConfig
from warren.pubsub.memory.broker import Delivery
from warren.pubsub.memory.connection import MemoryConnectionManager
from warren.pubsub.memory.consumer import MemoryConsumerManager
from warren.pubsub.rabbitmq.config import RMQExchangeConfig
from warren.pubsub.routing import REPLAY_ROUTING_KEY_FIELD


EXCHANGE = RMQExchangeConfig(name="jobs", type="fanout")
QUEUE = "jobs.test_worker"
_BODY = {"data_type": "x", "data": {"doc_id": "d1"}, "job_id": "job-1"}


class _FakeWorker:
    """Async MessageConsumerInterface stand-in with scripted behavior."""

    def __init__(self, *, result: dict | None = None, error: Exception | None = None):
        self.calls: list[dict] = []
        self.result = result
        self.error = error

    @property
    def name(self) -> str:
        return "worker-1"

    @property
    def type(self) -> str:
        return "test_worker"

    async def __call__(self, message: dict) -> dict | None:
        self.calls.append(message)
        if self.error is not None:
            raise self.error
        return self.result


class _FakePublisher:
    """PublisherInterface stand-in recording published messages."""

    def __init__(self, *, error: Exception | None = None) -> None:
        self.published: list[dict] = []
        self.setup_called = False
        self.teardown_called = False
        self.error = error

    async def setup(self) -> None:
        self.setup_called = True

    async def __call__(self, message: dict) -> None:
        if self.error is not None:
            raise self.error
        self.published.append(message)

    async def teardown(self) -> None:
        self.teardown_called = True


def _retry_config(**overrides) -> RetryConfig:
    # jitter off for determinism; no real sleeps in tests
    defaults = {"jitter": False, "fallback_requeue_delay": 0.0}
    return RetryConfig(**{**defaults, **overrides})


def _delivery(body=_BODY, *, routing_key: str = "") -> Delivery:
    raw = body if isinstance(body, bytes) else json.dumps(body).encode()
    return Delivery(body=raw, routing_key=routing_key)


async def _manager(worker, **kwargs) -> MemoryConsumerManager:
    conn = MemoryConnectionManager()
    await conn.setup()
    kwargs.setdefault("retry_config", _retry_config())
    manager = MemoryConsumerManager(
        conn,
        worker,  # type: ignore[arg-type]
        exchange=EXCHANGE,
        queue_name=QUEUE,
        **kwargs,
    )
    await manager.setup()
    return manager


def _process(worker, delivery: Delivery, **kwargs) -> int:
    """Run one delivery through the decision matrix; return the queue depth after."""

    async def scenario() -> int:
        manager = await _manager(worker, **kwargs)
        await manager._process_message(delivery)
        return manager._queue.qsize()

    return asyncio.run(scenario())


async def _eventually(predicate) -> None:
    deadline = asyncio.get_event_loop().time() + 2.0
    while not predicate():
        assert asyncio.get_event_loop().time() < deadline, "condition not met in time"
        await asyncio.sleep(0.005)


# --- setup -----------------------------------------------------------------


def test_setup_sets_up_publishers_and_binds_the_queue() -> None:
    publisher = _FakePublisher()

    async def scenario() -> int:
        manager = await _manager(_FakeWorker(), data_publisher=publisher)
        return manager._connection_manager.broker.publish(EXCHANGE, "", b"{}")

    assert asyncio.run(scenario()) == 1
    assert publisher.setup_called


def test_start_consuming_requires_setup() -> None:
    async def scenario() -> None:
        conn = MemoryConnectionManager()
        await conn.setup()
        manager = MemoryConsumerManager(
            conn,
            _FakeWorker(),
            exchange=EXCHANGE,
            queue_name=QUEUE,  # type: ignore[arg-type]
        )
        await manager.start_consuming()

    with pytest.raises(RuntimeError, match=r"setup\(\)"):
        asyncio.run(scenario())


# --- success ---------------------------------------------------------------


def test_success_publishes_result_and_acks() -> None:
    worker = _FakeWorker(result={"data_type": "y"})
    data, observer = _FakePublisher(), _FakePublisher()

    depth = _process(
        worker, _delivery(), data_publisher=data, observer_publisher=observer
    )

    assert worker.calls == [_BODY]
    assert data.published == [{"data_type": "y"}]
    assert observer.published == [{"data_type": "y"}]
    assert depth == 0


def test_none_result_acks_without_publishing() -> None:
    data = _FakePublisher()

    depth = _process(_FakeWorker(result=None), _delivery(), data_publisher=data)

    assert data.published == []
    assert depth == 0


def test_undecodable_message_is_dropped_without_calling_worker() -> None:
    worker = _FakeWorker()

    depth = _process(worker, _delivery(b"not json"))

    assert worker.calls == []
    assert depth == 0


# --- soft failure ----------------------------------------------------------


def test_soft_failure_without_control_publisher_requeues() -> None:
    depth = _process(_FakeWorker(error=SoftFailureException("transient")), _delivery())

    assert depth == 1


def test_soft_failure_publishes_envelope_and_acks() -> None:
    control = _FakePublisher()

    depth = _process(
        _FakeWorker(error=SoftFailureException("transient")),
        _delivery(routing_key="doc.pdf"),
        control_publisher=control,
    )

    assert control.published == [
        {
            "data_type": "soft-failure",
            "data": {
                **_BODY,
                # initial_delay 30 * backoff 2.0^(1-1), jitter off; max 5
                "retry": {"count": 1, "after": 30, "reason": "transient", "max": 5},
                # The delivery's key, so ReplayRouter lands the retry back
                # on the worker that failed on topic/direct pipelines.
                REPLAY_ROUTING_KEY_FIELD: "doc.pdf",
            },
            "job_id": "job-1",
            "origin": {"type": "test_worker", "name": "worker-1"},
        }
    ]
    assert depth == 0


def test_soft_failure_retry_math_on_re_retry() -> None:
    control = _FakePublisher()
    body = {**_BODY, "retry": {"count": 2, "after": 60, "reason": "old", "max": 5}}

    _process(
        _FakeWorker(error=SoftFailureException("again")),
        _delivery(body),
        control_publisher=control,
    )

    # attempt 3: 30 * 2^(3-1) = 120
    assert control.published[0]["data"]["retry"] == {
        "count": 3,
        "after": 120,
        "reason": "again",
        "max": 5,
    }


def test_soft_failure_honors_worker_intent_with_caps() -> None:
    control = _FakePublisher()
    error = SoftFailureException("x", retry_after=1000, retry_max=99)

    _process(_FakeWorker(error=error), _delivery(), control_publisher=control)

    retry = control.published[0]["data"]["retry"]
    assert retry["after"] == 300  # max_delay_cap
    assert retry["max"] == 10  # max_retries_cap


def test_soft_failure_max_retries_exceeded_becomes_hard_failure() -> None:
    control = _FakePublisher()
    body = {**_BODY, "retry": {"count": 5, "after": 30, "reason": "old", "max": 5}}

    depth = _process(
        _FakeWorker(error=SoftFailureException("again")),
        _delivery(body),
        control_publisher=control,
    )

    assert control.published[0]["data_type"] == "hard-failure"
    assert depth == 0


def test_soft_failure_without_consuming_a_retry_slot() -> None:
    control = _FakePublisher()
    error = SoftFailureException("polling", retry_after=7, retry_count_consumed=False)
    body = {**_BODY, "retry": {"count": 5, "after": 30, "reason": "old", "max": 5}}

    _process(_FakeWorker(error=error), _delivery(body), control_publisher=control)

    envelope = control.published[0]
    assert envelope["data_type"] == "soft-failure"
    assert envelope["data"]["retry"]["count"] == 5


def test_first_deferral_without_retry_slot_is_not_halved() -> None:
    control = _FakePublisher()
    error = SoftFailureException("polling", retry_after=8, retry_count_consumed=False)

    _process(_FakeWorker(error=error), _delivery(), control_publisher=control)

    assert control.published[0]["data"]["retry"]["after"] == 8


def test_soft_failure_envelope_publish_failure_requeues() -> None:
    control = _FakePublisher(error=PublishFailureException("down"))

    depth = _process(
        _FakeWorker(error=SoftFailureException("transient")),
        _delivery(),
        control_publisher=control,
    )

    assert depth == 1


# --- publish failure -------------------------------------------------------


def test_result_publish_failure_requeues() -> None:
    data = _FakePublisher(error=PublishFailureException("down"))

    depth = _process(
        _FakeWorker(result={"data_type": "y"}), _delivery(), data_publisher=data
    )

    assert depth == 1


# --- hard failure ----------------------------------------------------------


def test_hard_failure_publishes_envelope_and_drops_the_message() -> None:
    control = _FakePublisher()

    depth = _process(
        _FakeWorker(error=HardFailureException("permanent")),
        _delivery(),
        control_publisher=control,
    )

    envelope = control.published[0]
    assert envelope["data_type"] == "hard-failure"
    assert envelope["data"] == _BODY
    assert envelope["job_id"] == "job-1"
    assert envelope["origin"] == {"type": "test_worker", "name": "worker-1"}
    assert "permanent" in envelope["error"]
    assert depth == 0


def test_unexpected_exception_is_a_hard_failure() -> None:
    control = _FakePublisher()

    _process(
        _FakeWorker(error=KeyError("boom")), _delivery(), control_publisher=control
    )

    assert control.published[0]["data_type"] == "hard-failure"


def test_hard_failure_envelope_suppressed_when_disabled() -> None:
    control = _FakePublisher()

    depth = _process(
        _FakeWorker(error=HardFailureException("permanent")),
        _delivery(),
        control_publisher=control,
        publish_hard_failures=False,
    )

    assert control.published == []
    assert depth == 0


def test_hard_failure_envelope_publish_is_best_effort() -> None:
    control = _FakePublisher(error=RuntimeError("down"))

    depth = _process(
        _FakeWorker(error=HardFailureException("permanent")),
        _delivery(),
        control_publisher=control,
    )

    assert depth == 0


def test_extract_identity_func_is_used() -> None:
    seen: list[dict] = []

    def identity(message: dict) -> str:
        seen.append(message)
        return "custom"

    _process(
        _FakeWorker(error=HardFailureException("permanent")),
        _delivery(),
        extract_identity_func=identity,
    )

    assert seen == [_BODY]


# --- consume loop ----------------------------------------------------------


def test_consume_loop_processes_one_message_at_a_time() -> None:
    running: list[int] = []
    peak: list[int] = []

    class _SlowWorker(_FakeWorker):
        async def __call__(self, message: dict) -> dict | None:
            running.append(1)
            peak.append(len(running))
            await asyncio.sleep(0.01)
            running.pop()
            return await super().__call__(message)

    async def scenario() -> int:
        worker = _SlowWorker()
        manager = await _manager(worker)
        for _ in range(3):
            manager._connection_manager.broker.publish(
                EXCHANGE, "", json.dumps(_BODY).encode()
            )
        await manager.start_consuming()
        await _eventually(lambda: len(worker.calls) == 3)
        await manager.stop_consuming()
        return max(peak)

    assert asyncio.run(scenario()) == 1


def test_health_reflects_the_consume_loop() -> None:
    async def scenario() -> tuple[str, str, str]:
        manager = await _manager(_FakeWorker())
        before = (await manager.health()).state
        await manager.start_consuming()
        during = (await manager.health()).state
        await manager.stop_consuming()
        after = (await manager.health()).state
        return before, during, after

    assert asyncio.run(scenario()) == ("consumer_lost", "ready", "consumer_lost")


def test_shutdown_drains_the_in_flight_message_and_tears_down_publishers() -> None:
    release = asyncio.Event  # constructed inside the loop below
    data = _FakePublisher()

    async def scenario() -> list[dict]:
        gate = release()

        class _GatedWorker(_FakeWorker):
            async def __call__(self, message: dict) -> dict | None:
                await gate.wait()
                return {"data_type": "y"}

        worker = _GatedWorker()
        manager = await _manager(worker, data_publisher=data)
        manager._connection_manager.broker.publish(
            EXCHANGE, "", json.dumps(_BODY).encode()
        )
        await manager.start_consuming()
        await _eventually(lambda: manager._in_flight_task is not None)
        stopping = asyncio.create_task(manager.stop_consuming())
        await asyncio.sleep(0.01)
        gate.set()
        await stopping
        return data.published

    assert asyncio.run(scenario()) == [{"data_type": "y"}]
    assert data.teardown_called
