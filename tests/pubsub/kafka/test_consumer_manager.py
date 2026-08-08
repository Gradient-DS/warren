"""Unit tests for ``KafkaConsumerManager``.

Fake-driven (no broker): a hand-rolled aiokafka consumer stands in for
the real one, injected through a fake connection manager — the same seam
the real ``KafkaConnectionManager`` provides via its factory helpers.

Covered here:

- the commit/seek decision per failure class (the Kafka mapping of the
  RabbitMQ ack/nack/reject matrix),
- soft-failure envelope contents and retry math, including byte-for-byte
  parity with the envelopes the RMQ consumer manager produces for the
  same inputs,
- setup (group derivation, manual-commit settings, topic verification),
- shutdown drain of the in-flight message.
"""

import asyncio
import copy
import json
from types import SimpleNamespace

from aiokafka import TopicPartition

from warren.common import HardFailureException, SoftFailureException
from warren.pubsub.common import (
    PublishFailureException,
    PubSubSetupError,
    RetryConfig,
)
from warren.pubsub.kafka.aiokafka.consumer import KafkaConsumerManager
from warren.pubsub.kafka.config import (
    KafkaConsumerConfig,
    KafkaConsumerManagerConfig,
    KafkaTopicConfig,
)
from warren.pubsub.rabbitmq.aio_pika.consumer import RMQConsumerManager
from warren.pubsub.rabbitmq.config import (
    RMQConsumerConfig,
    RMQConsumerManagerConfig,
    RMQExchangeConfig,
    RMQQueueConfig,
)
from warren.pubsub.routing import REPLAY_ROUTING_KEY_FIELD


TOPIC = "jobs"
TP = TopicPartition(TOPIC, 0)

_BODY = {
    "data_type": "raw_document",
    "job_id": "job-1",
    "data": {"doc_id": "doc-1", "part_idx": 0},
    "origin": {"type": "api", "name": "api-1"},
}


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------


class _FakeKafkaMessage:
    """ConsumerRecord stand-in."""

    def __init__(self, value, *, offset: int = 7) -> None:
        self.topic = TOPIC
        self.partition = 0
        self.offset = offset
        self.value = value


def _msg(body, *, offset: int = 7) -> _FakeKafkaMessage:
    value = body if isinstance(body, bytes) else json.dumps(body).encode()
    return _FakeKafkaMessage(value, offset=offset)


class _FakeKafkaConsumer:
    """AIOKafkaConsumer stand-in recording commits/seeks; fed via a queue."""

    def __init__(self, *, start_error: Exception | None = None) -> None:
        self.started = False
        self.stopped = False
        self.commits: list[dict] = []
        self.seeks: list[tuple] = []
        self._queue: asyncio.Queue = asyncio.Queue()
        self._start_error = start_error

    def feed(self, message: _FakeKafkaMessage) -> None:
        self._queue.put_nowait(message)

    async def start(self) -> None:
        if self._start_error is not None:
            raise self._start_error
        self.started = True

    async def stop(self) -> None:
        self.stopped = True

    async def getone(self) -> _FakeKafkaMessage:
        return await self._queue.get()

    async def commit(self, offsets=None) -> None:
        self.commits.append(offsets)

    def seek(self, partition, offset) -> None:
        self.seeks.append((partition, offset))


class _FakeAdmin:
    def __init__(self, *, topics: list[str] | None = None) -> None:
        self.topics = list(topics if topics is not None else [TOPIC])

    async def list_topics(self) -> list[str]:
        return list(self.topics)

    async def create_topics(self, new_topics, timeout_ms=None, validate_only=False):
        self.topics.extend(nt.name for nt in new_topics)
        return SimpleNamespace(topic_errors=[(nt.name, 0, None) for nt in new_topics])


class _FakeConnectionManager:
    """KafkaConnectionManager stand-in: exposes ``admin`` + factory helpers."""

    def __init__(
        self,
        *,
        admin: _FakeAdmin | None = None,
        kafka_consumer: _FakeKafkaConsumer | None = None,
    ) -> None:
        self.admin = admin or _FakeAdmin()
        self.kafka_consumer = kafka_consumer or _FakeKafkaConsumer()
        self.consumer_topics: tuple | None = None
        self.consumer_kwargs: dict | None = None

    def create_consumer(self, *topics, **kwargs):
        self.consumer_topics = topics
        self.consumer_kwargs = kwargs
        return self.kafka_consumer


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


# ---------------------------------------------------------------------------
# Builders
# ---------------------------------------------------------------------------


def _retry_config(**overrides) -> RetryConfig:
    # jitter off for determinism; no real sleeps in tests
    defaults = {"jitter": False, "fallback_requeue_delay": 0.0}
    return RetryConfig(**{**defaults, **overrides})


def _manager(
    worker: _FakeWorker,
    *,
    data_publisher: _FakePublisher | None = None,
    control_publisher: _FakePublisher | None = None,
    retry_config: RetryConfig | None = None,
    conn: _FakeConnectionManager | None = None,
    group_id: str | None = None,
    create_if_missing: bool = False,
    publish_hard_failures: bool = True,
    on_shutdown_timeout: float = 1.0,
) -> tuple[KafkaConsumerManager, _FakeConnectionManager]:
    config = KafkaConsumerManagerConfig(
        topic=KafkaTopicConfig(name=TOPIC, create_if_missing=create_if_missing),
        consumer=KafkaConsumerConfig(
            group_id=group_id,
            on_shutdown_timeout=on_shutdown_timeout,
        ),
    )
    conn = conn or _FakeConnectionManager()
    manager = KafkaConsumerManager(
        config,
        conn,  # type: ignore[arg-type]
        worker,  # type: ignore[arg-type]
        data_publisher=data_publisher,  # type: ignore[arg-type]
        control_publisher=control_publisher,  # type: ignore[arg-type]
        retry_config=retry_config or _retry_config(),
        publish_hard_failures=publish_hard_failures,
    )
    return manager, conn


def _process(
    manager: KafkaConsumerManager,
    message: _FakeKafkaMessage,
) -> None:
    """Set up the manager and run a single message through the decision matrix."""

    async def scenario() -> None:
        await manager.setup()
        await manager._process_message(message)

    asyncio.run(scenario())


async def _eventually(predicate) -> None:
    deadline = asyncio.get_event_loop().time() + 2.0
    while not predicate():
        assert asyncio.get_event_loop().time() < deadline, "condition not met in time"
        await asyncio.sleep(0.005)


# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------


def test_setup_creates_manual_commit_consumer_with_derived_group() -> None:
    worker = _FakeWorker()
    manager, conn = _manager(worker)

    asyncio.run(manager.setup())

    assert conn.kafka_consumer.started
    assert conn.consumer_topics == (TOPIC,)
    assert conn.consumer_kwargs == {
        "group_id": f"{TOPIC}.test_worker",  # "<topic>.<worker_type>"
        "enable_auto_commit": False,
        "auto_offset_reset": "earliest",
        "max_poll_interval_ms": 600_000,
        "session_timeout_ms": 45_000,
        "isolation_level": "read_committed",
    }


def test_setup_uses_explicit_group_id() -> None:
    worker = _FakeWorker()
    manager, conn = _manager(worker, group_id="dsh-assigned-group")

    asyncio.run(manager.setup())

    assert conn.consumer_kwargs is not None
    assert conn.consumer_kwargs["group_id"] == "dsh-assigned-group"


def test_setup_sets_up_publishers_first() -> None:
    worker = _FakeWorker()
    publisher = _FakePublisher()
    conn = _FakeConnectionManager(admin=_FakeAdmin(topics=[]))  # topic missing
    manager, _ = _manager(worker, control_publisher=publisher, conn=conn)

    raised = False
    try:
        asyncio.run(manager.setup())
    except PubSubSetupError:
        raised = True

    assert raised  # missing topic, create_if_missing=False
    assert publisher.setup_called  # publishers were set up before the topic check


def test_setup_creates_missing_topic_when_configured() -> None:
    worker = _FakeWorker()
    conn = _FakeConnectionManager(admin=_FakeAdmin(topics=[]))
    manager, conn = _manager(worker, conn=conn, create_if_missing=True)

    asyncio.run(manager.setup())

    assert TOPIC in conn.admin.topics
    assert conn.kafka_consumer.started


def test_setup_failure_is_wrapped_as_setup_error() -> None:
    worker = _FakeWorker()
    conn = _FakeConnectionManager(
        kafka_consumer=_FakeKafkaConsumer(start_error=RuntimeError("no brokers"))
    )
    manager, _ = _manager(worker, conn=conn)

    raised = False
    try:
        asyncio.run(manager.setup())
    except PubSubSetupError:
        raised = True

    assert raised


def test_start_consuming_requires_setup() -> None:
    worker = _FakeWorker()
    manager, _ = _manager(worker)

    raised = False
    try:
        asyncio.run(manager.start_consuming())
    except RuntimeError:
        raised = True

    assert raised


# ---------------------------------------------------------------------------
# Commit/seek decision matrix
# ---------------------------------------------------------------------------


def test_success_publishes_result_and_commits() -> None:
    worker = _FakeWorker(result={"data_type": "parsed_document"})
    publisher = _FakePublisher()
    manager, conn = _manager(worker, data_publisher=publisher)

    _process(manager, _msg(_BODY, offset=7))

    assert worker.calls == [_BODY]
    assert publisher.published == [{"data_type": "parsed_document"}]
    assert conn.kafka_consumer.commits == [{TP: 8}]  # offset + 1 ≙ ack
    assert conn.kafka_consumer.seeks == []


def test_success_without_publishers_commits() -> None:
    worker = _FakeWorker(result={"data_type": "parsed_document"})
    manager, conn = _manager(worker)

    _process(manager, _msg(_BODY, offset=7))

    assert conn.kafka_consumer.commits == [{TP: 8}]


def test_none_result_commits_without_publishing() -> None:
    worker = _FakeWorker(result=None)
    publisher = _FakePublisher()
    manager, conn = _manager(worker, data_publisher=publisher)

    _process(manager, _msg(_BODY, offset=7))

    assert publisher.published == []
    assert conn.kafka_consumer.commits == [{TP: 8}]


def test_undecodable_message_commits_without_calling_worker() -> None:
    worker = _FakeWorker()
    manager, conn = _manager(worker)

    _process(manager, _msg(b"not json {", offset=7))

    assert worker.calls == []
    assert conn.kafka_consumer.commits == [{TP: 8}]  # ≙ reject(requeue=False)
    assert conn.kafka_consumer.seeks == []


def test_soft_failure_without_control_publisher_seeks_back() -> None:
    worker = _FakeWorker(error=SoftFailureException("transient"))
    manager, conn = _manager(worker)

    _process(manager, _msg(_BODY, offset=7))

    assert conn.kafka_consumer.commits == []  # ≙ nack(requeue=True)
    assert conn.kafka_consumer.seeks == [(TP, 7)]


def test_soft_failure_publishes_envelope_and_commits() -> None:
    worker = _FakeWorker(error=SoftFailureException("transient"))
    publisher = _FakePublisher()
    manager, conn = _manager(worker, control_publisher=publisher)

    _process(manager, _msg(_BODY, offset=7))

    assert len(publisher.published) == 1
    envelope = publisher.published[0]
    assert envelope == {
        "data_type": "soft-failure",
        "data": {
            **_BODY,
            # initial_delay 30 * backoff 2.0^(1-1), jitter off; max 5 (capped at 10)
            "retry": {"count": 1, "after": 30, "reason": "transient", "max": 5},
            # Kafka messages carry no routing key; "" replays as a fanout key.
            REPLAY_ROUTING_KEY_FIELD: "",
        },
        "job_id": "job-1",
        "origin": {"type": "test_worker", "name": "worker-1"},
    }
    assert conn.kafka_consumer.commits == [{TP: 8}]  # ≙ ack
    assert conn.kafka_consumer.seeks == []


def test_soft_failure_retry_math_on_re_retry() -> None:
    worker = _FakeWorker(error=SoftFailureException("still transient"))
    publisher = _FakePublisher()
    manager, conn = _manager(worker, control_publisher=publisher)
    body = {**_BODY, "retry": {"count": 2, "after": 60, "reason": "old", "max": 5}}

    _process(manager, _msg(body, offset=7))

    retry = publisher.published[0]["data"]["retry"]
    # count incremented, delay = 30 * 2.0^(3-1) = 120, capped at 300
    assert retry == {"count": 3, "after": 120, "reason": "still transient", "max": 5}
    assert conn.kafka_consumer.commits == [{TP: 8}]


def test_soft_failure_honors_worker_retry_intent_with_caps() -> None:
    worker = _FakeWorker(
        error=SoftFailureException(
            "transient",
            retry_after=200,
            retry_max=99,  # capped by max_retries_cap=10
            backoff_base=3.0,
        )
    )
    publisher = _FakePublisher()
    manager, _ = _manager(worker, control_publisher=publisher)
    body = {**_BODY, "retry": {"count": 1, "after": 200, "reason": "old", "max": 10}}

    _process(manager, _msg(body, offset=7))

    retry = publisher.published[0]["data"]["retry"]
    # delay = 200 * 3.0^(2-1) = 600 -> capped at max_delay_cap=300
    assert retry == {"count": 2, "after": 300, "reason": "transient", "max": 10}


def test_soft_failure_max_retries_exceeded_becomes_hard_failure() -> None:
    worker = _FakeWorker(error=SoftFailureException("transient"))
    publisher = _FakePublisher()
    manager, conn = _manager(worker, control_publisher=publisher)
    body = {**_BODY, "retry": {"count": 5, "after": 300, "reason": "old", "max": 5}}

    _process(manager, _msg(body, offset=7))

    assert len(publisher.published) == 1
    envelope = publisher.published[0]
    assert envelope["data_type"] == "hard-failure"
    assert envelope["job_id"] == "job-1"
    assert envelope["origin"] == {"type": "test_worker", "name": "worker-1"}
    assert "error" in envelope
    assert conn.kafka_consumer.commits == [{TP: 8}]  # ≙ reject(requeue=False)


def test_soft_failure_without_consuming_retry_slot() -> None:
    worker = _FakeWorker(
        error=SoftFailureException("poll again", retry_count_consumed=False)
    )
    publisher = _FakePublisher()
    manager, conn = _manager(worker, control_publisher=publisher)
    # count already at max — no slot consumed, so no hard-failure escalation
    body = {**_BODY, "retry": {"count": 5, "after": 300, "reason": "old", "max": 5}}

    _process(manager, _msg(body, offset=7))

    envelope = publisher.published[0]
    assert envelope["data_type"] == "soft-failure"
    assert envelope["data"]["retry"]["count"] == 5  # not incremented
    assert conn.kafka_consumer.commits == [{TP: 8}]


def test_soft_failure_envelope_publish_failure_seeks_back() -> None:
    worker = _FakeWorker(error=SoftFailureException("transient"))
    publisher = _FakePublisher(error=PublishFailureException("broker gone"))
    manager, conn = _manager(worker, control_publisher=publisher)

    _process(manager, _msg(_BODY, offset=7))

    assert conn.kafka_consumer.commits == []  # ≙ nack(requeue=True)
    assert conn.kafka_consumer.seeks == [(TP, 7)]


def test_result_publish_failure_seeks_back() -> None:
    worker = _FakeWorker(result={"data_type": "parsed_document"})
    publisher = _FakePublisher(error=PublishFailureException("broker gone"))
    manager, conn = _manager(worker, data_publisher=publisher)

    _process(manager, _msg(_BODY, offset=7))

    assert conn.kafka_consumer.commits == []  # ≙ nack(requeue=True)
    assert conn.kafka_consumer.seeks == [(TP, 7)]


def test_hard_failure_publishes_envelope_and_commits() -> None:
    worker = _FakeWorker(error=HardFailureException("unparseable"))
    publisher = _FakePublisher()
    manager, conn = _manager(worker, control_publisher=publisher)

    _process(manager, _msg(_BODY, offset=7))

    envelope = publisher.published[0]
    assert envelope["data_type"] == "hard-failure"
    assert envelope["data"] == _BODY
    assert envelope["job_id"] == "job-1"
    assert envelope["origin"] == {"type": "test_worker", "name": "worker-1"}
    assert "unparseable" in envelope["error"]
    assert conn.kafka_consumer.commits == [{TP: 8}]  # ≙ reject(requeue=False)
    assert conn.kafka_consumer.seeks == []


def test_unexpected_exception_is_treated_as_hard_failure() -> None:
    worker = _FakeWorker(error=ValueError("bug"))
    publisher = _FakePublisher()
    manager, conn = _manager(worker, control_publisher=publisher)

    _process(manager, _msg(_BODY, offset=7))

    assert publisher.published[0]["data_type"] == "hard-failure"
    assert conn.kafka_consumer.commits == [{TP: 8}]


def test_hard_failure_envelope_suppressed_when_disabled() -> None:
    worker = _FakeWorker(error=HardFailureException("unparseable"))
    publisher = _FakePublisher()
    manager, conn = _manager(
        worker, control_publisher=publisher, publish_hard_failures=False
    )

    _process(manager, _msg(_BODY, offset=7))

    assert publisher.published == []
    assert conn.kafka_consumer.commits == [{TP: 8}]  # still dropped


def test_hard_failure_envelope_publish_is_best_effort() -> None:
    worker = _FakeWorker(error=HardFailureException("unparseable"))
    publisher = _FakePublisher(error=RuntimeError("broker gone"))
    manager, conn = _manager(worker, control_publisher=publisher)

    _process(manager, _msg(_BODY, offset=7))

    assert conn.kafka_consumer.commits == [{TP: 8}]  # committed anyway


# ---------------------------------------------------------------------------
# Envelope parity with the RabbitMQ consumer manager
# ---------------------------------------------------------------------------


class _FakeRMQMessage:
    """AbstractIncomingMessage stand-in (ack/nack/reject recording)."""

    # Arrival routing key stamped into soft-failure envelopes; "" is what a
    # fanout publish carries, matching the Kafka consumer's stamp.
    routing_key = ""

    def __init__(self) -> None:
        self.acked = False
        self.nacked: bool | None = None
        self.rejected: bool | None = None

    async def ack(self) -> None:
        self.acked = True

    async def nack(self, requeue: bool = True) -> None:
        self.nacked = requeue

    async def reject(self, requeue: bool = False) -> None:
        self.rejected = requeue


def _rmq_manager(
    worker: _FakeWorker,
    publisher: _FakePublisher,
    retry_config: RetryConfig,
) -> RMQConsumerManager:
    config = RMQConsumerManagerConfig(
        exchange=RMQExchangeConfig(name=TOPIC, type="fanout"),
        queue=RMQQueueConfig(name=f"{TOPIC}.test_worker"),
        consumer=RMQConsumerConfig(),
    )
    return RMQConsumerManager(
        config,
        object(),  # type: ignore[arg-type]  # connection manager unused here
        worker,  # type: ignore[arg-type]
        control_publisher=publisher,  # type: ignore[arg-type]
        retry_config=retry_config,
    )


def _parity_envelopes(
    body: dict,
    error: SoftFailureException,
) -> tuple[list[dict], list[dict], _FakeRMQMessage, _FakeKafkaConsumer]:
    """Run the same soft failure through both backends; return the envelopes."""
    retry_config = _retry_config()

    kafka_worker = _FakeWorker(error=error)
    kafka_publisher = _FakePublisher()
    manager, conn = _manager(
        kafka_worker, control_publisher=kafka_publisher, retry_config=retry_config
    )

    rmq_worker = _FakeWorker(error=error)
    rmq_publisher = _FakePublisher()
    rmq_manager = _rmq_manager(rmq_worker, rmq_publisher, retry_config)
    rmq_message = _FakeRMQMessage()

    async def scenario() -> None:
        await manager.setup()
        await manager._handle_soft_failure(
            _msg(body, offset=7),  # type: ignore[arg-type]
            copy.deepcopy(body),
            error,
        )
        await rmq_manager._handle_soft_failure(
            rmq_message,  # type: ignore[arg-type]
            copy.deepcopy(body),
            error,
        )

    asyncio.run(scenario())

    return (
        kafka_publisher.published,
        rmq_publisher.published,
        rmq_message,
        conn.kafka_consumer,
    )


def test_soft_failure_envelope_matches_rmq_path() -> None:
    error = SoftFailureException("transient")

    kafka_envelopes, rmq_envelopes, rmq_message, kafka_consumer = _parity_envelopes(
        _BODY, error
    )

    assert kafka_envelopes == rmq_envelopes  # byte-for-byte identical envelopes
    assert json.dumps(kafka_envelopes) == json.dumps(rmq_envelopes)
    # and the terminal action agrees: RMQ acked ≙ Kafka committed
    assert rmq_message.acked
    assert kafka_consumer.commits == [{TP: 8}]


def test_soft_failure_re_retry_envelope_matches_rmq_path() -> None:
    error = SoftFailureException("still transient", retry_after=45, backoff_base=2.0)
    body = {**_BODY, "retry": {"count": 2, "after": 90, "reason": "old", "max": 5}}

    kafka_envelopes, rmq_envelopes, rmq_message, kafka_consumer = _parity_envelopes(
        body, error
    )

    assert kafka_envelopes == rmq_envelopes
    assert json.dumps(kafka_envelopes) == json.dumps(rmq_envelopes)
    assert rmq_message.acked
    assert kafka_consumer.commits == [{TP: 8}]


def test_max_retries_hard_failure_envelope_matches_rmq_path() -> None:
    error = SoftFailureException("transient")
    body = {**_BODY, "retry": {"count": 5, "after": 300, "reason": "old", "max": 5}}

    kafka_envelopes, rmq_envelopes, rmq_message, kafka_consumer = _parity_envelopes(
        body, error
    )

    assert kafka_envelopes == rmq_envelopes  # identical hard-failure envelopes
    assert json.dumps(kafka_envelopes) == json.dumps(rmq_envelopes)
    # RMQ rejected without requeue ≙ Kafka committed
    assert rmq_message.rejected is False
    assert kafka_consumer.commits == [{TP: 8}]


# ---------------------------------------------------------------------------
# Poll loop and shutdown
# ---------------------------------------------------------------------------


def test_poll_loop_processes_messages_sequentially() -> None:
    worker = _FakeWorker(result=None)
    manager, conn = _manager(worker)

    async def scenario() -> None:
        conn.kafka_consumer.feed(_msg(_BODY, offset=7))
        conn.kafka_consumer.feed(_msg(_BODY, offset=8))
        await manager.setup()
        await manager.start_consuming()
        await _eventually(lambda: len(conn.kafka_consumer.commits) == 2)
        await manager.stop_consuming()

    asyncio.run(scenario())

    assert conn.kafka_consumer.commits == [{TP: 8}, {TP: 9}]  # in order
    assert conn.kafka_consumer.stopped


def test_shutdown_drains_in_flight_message() -> None:
    started = asyncio.Event()
    release = asyncio.Event()

    class _BlockingWorker(_FakeWorker):
        async def __call__(self, message: dict) -> dict | None:
            started.set()
            await release.wait()
            return None

    publisher = _FakePublisher()
    manager, conn = _manager(_BlockingWorker(), data_publisher=publisher)

    async def scenario() -> None:
        conn.kafka_consumer.feed(_msg(_BODY, offset=7))
        await manager.setup()
        await manager.start_consuming()
        await asyncio.wait_for(started.wait(), timeout=1.0)

        stop_task = asyncio.create_task(manager.stop_consuming())
        await asyncio.sleep(0.01)
        assert not stop_task.done()  # waiting on the in-flight message

        release.set()
        await asyncio.wait_for(stop_task, timeout=2.0)

    asyncio.run(scenario())

    assert conn.kafka_consumer.commits == [{TP: 8}]  # in-flight completed + committed
    assert conn.kafka_consumer.stopped
    assert publisher.teardown_called


def test_shutdown_timeout_leaves_offset_uncommitted() -> None:
    started = asyncio.Event()

    class _StuckWorker(_FakeWorker):
        async def __call__(self, message: dict) -> dict | None:
            started.set()
            await asyncio.Event().wait()  # never completes
            return None

    manager, conn = _manager(_StuckWorker(), on_shutdown_timeout=0.05)

    async def scenario() -> None:
        conn.kafka_consumer.feed(_msg(_BODY, offset=7))
        await manager.setup()
        await manager.start_consuming()
        await asyncio.wait_for(started.wait(), timeout=1.0)
        await asyncio.wait_for(manager.stop_consuming(), timeout=2.0)

    asyncio.run(scenario())

    # Nothing committed — the offset redelivers to the group (at-least-once).
    assert conn.kafka_consumer.commits == []
    assert conn.kafka_consumer.stopped


def test_poll_loop_survives_transient_fetch_error() -> None:
    """A getone() that raises a transient error must not kill the loop.

    The loop logs, backs off, and goes on to process the next message —
    the fetch-error-is-recoverable policy.
    """
    worker = _FakeWorker(result=None)
    manager, conn = _manager(worker)

    # One transient fetch failure, then a normal message.
    real_getone = conn.kafka_consumer.getone
    raised = {"done": False}

    async def flaky_getone() -> _FakeKafkaMessage:
        if not raised["done"]:
            raised["done"] = True
            msg = "transient KafkaError during metadata refresh"
            raise RuntimeError(msg)
        return await real_getone()

    conn.kafka_consumer.getone = flaky_getone  # type: ignore[method-assign]

    async def scenario() -> None:
        conn.kafka_consumer.feed(_msg(_BODY, offset=7))
        await manager.setup()
        manager._fetch_error_backoff = 0.0  # keep the test fast
        await manager.start_consuming()
        await _eventually(lambda: len(conn.kafka_consumer.commits) == 1)
        await manager.stop_consuming()

    asyncio.run(scenario())

    assert raised["done"]  # the transient fetch error did fire
    # The loop survived the fetch error and processed the next message —
    # proven by the commit landing after the error fired.
    assert conn.kafka_consumer.commits == [{TP: 8}]


def test_poll_loop_stops_on_cancellation() -> None:
    """CancelledError still stops the loop — only cancellation does."""
    worker = _FakeWorker(result=None)
    manager, conn = _manager(worker)

    async def scenario() -> None:
        await manager.setup()
        await manager.start_consuming()
        # The loop is parked in getone(); stop_consuming cancels it.
        await manager.stop_consuming()

    asyncio.run(scenario())

    assert manager._poll_task.done()
    assert conn.kafka_consumer.stopped


def test_shutdown_tears_down_publishers_best_effort() -> None:
    class _ExplodingPublisher(_FakePublisher):
        async def teardown(self) -> None:
            msg = "already closed"
            raise RuntimeError(msg)

    ok_publisher = _FakePublisher()
    manager, conn = _manager(
        _FakeWorker(),
        data_publisher=_ExplodingPublisher(),
        control_publisher=ok_publisher,
    )

    async def scenario() -> None:
        await manager.setup()
        await manager.start_consuming()
        await manager.stop_consuming()  # must not raise

    asyncio.run(scenario())

    assert ok_publisher.teardown_called
    assert conn.kafka_consumer.stopped
