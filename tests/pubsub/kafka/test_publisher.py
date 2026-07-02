"""Unit tests for ``KafkaPublisher`` and the ``ensure_topic`` topology helper.

Fake-driven (no broker): a hand-rolled admin client and producer stand in
for aiokafka, injected through a fake connection manager — the same seam
the real ``KafkaConnectionManager`` provides via its factory helpers.
"""

import asyncio
import json
from types import SimpleNamespace

from aiokafka.errors import TopicAlreadyExistsError

from warren.pubsub.common import PublishFailureException, PubSubSetupError, Route
from warren.pubsub.kafka.aiokafka.publisher import KafkaPublisher
from warren.pubsub.kafka.aiokafka.topology import ensure_topic
from warren.pubsub.kafka.config import KafkaTopicConfig


class _FakeAdmin:
    """AIOKafkaAdminClient stand-in: existing topics + scripted create results."""

    def __init__(
        self,
        *,
        topics: list[str] | None = None,
        create_error: Exception | None = None,
        create_error_code: int = 0,
    ) -> None:
        self.topics = list(topics or [])
        self.created: list = []
        self._create_error = create_error
        self._create_error_code = create_error_code

    async def list_topics(self) -> list[str]:
        return list(self.topics)

    async def create_topics(self, new_topics, timeout_ms=None, validate_only=False):
        if self._create_error is not None:
            raise self._create_error
        self.created.extend(new_topics)
        return SimpleNamespace(
            topic_errors=[(nt.name, self._create_error_code, None) for nt in new_topics]
        )


class _FakeProducer:
    """AIOKafkaProducer stand-in recording sends; can be told to fail."""

    def __init__(
        self,
        *,
        start_error: Exception | None = None,
        send_error: Exception | None = None,
    ) -> None:
        self.started = False
        self.stopped = False
        self.sent: list[tuple] = []
        self._start_error = start_error
        self._send_error = send_error

    async def start(self) -> None:
        if self._start_error is not None:
            raise self._start_error
        self.started = True

    async def stop(self) -> None:
        self.stopped = True

    async def send_and_wait(self, topic, value=None, key=None, headers=None):
        if self._send_error is not None:
            raise self._send_error
        self.sent.append((topic, value, key, headers))


class _FakeConnectionManager:
    """KafkaConnectionManager stand-in: exposes ``admin`` + factory helpers."""

    def __init__(
        self,
        *,
        admin: _FakeAdmin | None = None,
        producer: _FakeProducer | None = None,
    ) -> None:
        self.admin = admin or _FakeAdmin(topics=["jobs"])
        self.producer = producer or _FakeProducer()
        self.producer_kwargs: dict | None = None

    def create_producer(self, **kwargs):
        self.producer_kwargs = kwargs
        return self.producer


def _publisher(
    conn: _FakeConnectionManager,
    *,
    topic_config: KafkaTopicConfig | None = None,
) -> KafkaPublisher:
    return KafkaPublisher(
        conn,  # type: ignore[arg-type]
        topic_config or KafkaTopicConfig(name="jobs"),
        name="test-publisher",
    )


# ---------------------------------------------------------------------------
# Route rejection (fanout-only parity)
# ---------------------------------------------------------------------------


def test_route_is_rejected() -> None:
    conn = _FakeConnectionManager()

    raised = False
    try:
        KafkaPublisher(
            conn,  # type: ignore[arg-type]
            KafkaTopicConfig(name="jobs"),
            route=Route(key="some.key"),
        )
    except ValueError:
        raised = True

    assert raised


def test_route_func_is_rejected() -> None:
    conn = _FakeConnectionManager()

    async def route_func(message: dict):
        return [Route(key="some.key")]

    raised = False
    try:
        KafkaPublisher(
            conn,  # type: ignore[arg-type]
            KafkaTopicConfig(name="jobs"),
            route_func=route_func,
        )
    except ValueError:
        raised = True

    assert raised


# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------


def test_setup_starts_idempotent_all_acks_producer() -> None:
    conn = _FakeConnectionManager()
    publisher = _publisher(conn)

    asyncio.run(publisher.setup())

    assert conn.producer.started
    assert conn.producer_kwargs == {"acks": "all", "enable_idempotence": True}


def test_setup_missing_topic_without_create_raises_setup_error() -> None:
    conn = _FakeConnectionManager(admin=_FakeAdmin(topics=[]))
    publisher = _publisher(conn)

    raised = False
    try:
        asyncio.run(publisher.setup())
    except PubSubSetupError:
        raised = True

    assert raised
    assert conn.admin.created == []  # never attempted creation


def test_setup_creates_missing_topic_when_configured() -> None:
    conn = _FakeConnectionManager(admin=_FakeAdmin(topics=[]))
    topic_config = KafkaTopicConfig(
        name="jobs",
        num_partitions=12,
        replication_factor=3,
        create_if_missing=True,
    )
    publisher = _publisher(conn, topic_config=topic_config)

    asyncio.run(publisher.setup())

    assert len(conn.admin.created) == 1
    new_topic = conn.admin.created[0]
    assert new_topic.name == "jobs"
    assert new_topic.num_partitions == 12
    assert new_topic.replication_factor == 3
    assert conn.producer.started


def test_setup_skips_creation_when_topic_exists() -> None:
    conn = _FakeConnectionManager(admin=_FakeAdmin(topics=["jobs"]))
    topic_config = KafkaTopicConfig(name="jobs", create_if_missing=True)
    publisher = _publisher(conn, topic_config=topic_config)

    asyncio.run(publisher.setup())

    assert conn.admin.created == []


def test_setup_tolerates_concurrent_topic_creation() -> None:
    # Raised by the client...
    conn = _FakeConnectionManager(
        admin=_FakeAdmin(topics=[], create_error=TopicAlreadyExistsError())
    )
    topic_config = KafkaTopicConfig(name="jobs", create_if_missing=True)

    asyncio.run(_publisher(conn, topic_config=topic_config).setup())
    assert conn.producer.started

    # ...or reported in the response body.
    conn = _FakeConnectionManager(
        admin=_FakeAdmin(topics=[], create_error_code=TopicAlreadyExistsError.errno)
    )

    asyncio.run(_publisher(conn, topic_config=topic_config).setup())
    assert conn.producer.started


def test_ensure_topic_surfaces_response_errors() -> None:
    conn = _FakeConnectionManager(
        admin=_FakeAdmin(topics=[], create_error_code=41)  # NOT_CONTROLLER
    )
    topic_config = KafkaTopicConfig(name="jobs", create_if_missing=True)

    raised = False
    try:
        asyncio.run(ensure_topic(conn, topic_config))  # type: ignore[arg-type]
    except PubSubSetupError:
        raised = True

    assert raised


def test_setup_failure_is_wrapped_as_setup_error() -> None:
    conn = _FakeConnectionManager(
        producer=_FakeProducer(start_error=RuntimeError("broker down"))
    )
    publisher = _publisher(conn)

    raised = False
    try:
        asyncio.run(publisher.setup())
    except PubSubSetupError:
        raised = True

    assert raised


# ---------------------------------------------------------------------------
# Publishing
# ---------------------------------------------------------------------------


def test_publish_sends_json_to_topic_without_key() -> None:
    conn = _FakeConnectionManager()
    publisher = _publisher(conn)
    message = {"data_type": "raw_document", "job_id": "j1", "data": {"doc_id": "d1"}}

    async def scenario() -> None:
        await publisher.setup()
        await publisher(message)

    asyncio.run(scenario())

    assert len(conn.producer.sent) == 1
    topic, value, key, headers = conn.producer.sent[0]
    assert topic == "jobs"
    assert json.loads(value) == message
    assert key is None  # round-robin partitioning (fanout parity)
    assert ("content_type", b"application/json") in headers


def test_publish_failure_is_wrapped() -> None:
    conn = _FakeConnectionManager(
        producer=_FakeProducer(send_error=RuntimeError("leader not available"))
    )
    publisher = _publisher(conn)

    async def scenario() -> None:
        await publisher.setup()
        await publisher({"data_type": "raw_document"})

    raised = False
    try:
        asyncio.run(scenario())
    except PublishFailureException:
        raised = True

    assert raised


def test_publish_before_setup_raises() -> None:
    publisher = _publisher(_FakeConnectionManager())

    raised = False
    try:
        asyncio.run(publisher({"data_type": "raw_document"}))
    except RuntimeError:
        raised = True

    assert raised


# ---------------------------------------------------------------------------
# Teardown
# ---------------------------------------------------------------------------


def test_teardown_stops_producer() -> None:
    conn = _FakeConnectionManager()
    publisher = _publisher(conn)

    async def scenario() -> None:
        await publisher.setup()
        await publisher.teardown()

    asyncio.run(scenario())

    assert conn.producer.stopped


def test_teardown_is_best_effort() -> None:
    class _ExplodingProducer(_FakeProducer):
        async def stop(self) -> None:
            msg = "already closed"
            raise RuntimeError(msg)

    conn = _FakeConnectionManager(producer=_ExplodingProducer())
    publisher = _publisher(conn)

    async def scenario() -> None:
        await publisher.setup()
        await publisher.teardown()  # must not raise

    asyncio.run(scenario())


def test_teardown_before_setup_is_noop() -> None:
    publisher = _publisher(_FakeConnectionManager())
    asyncio.run(publisher.teardown())  # must not raise
