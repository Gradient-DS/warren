"""Pins ``RMQConsumerManager.health()`` — the health matrix and the one
non-ABC aiormq attribute it depends on (``Channel.consumers``).
"""

import asyncio
import inspect

import aiormq

from warren.pubsub.common import ConsumerHealth

from .fakes import (
    FakeChannel,
    FakeConnection,
    FakeConnectionManager,
    FakeTransport,
    FakeUnderlayChannel,
    FakeWorker,
    make_manager,
)


TAG = "ctag-1"


def _health(
    *,
    connection: FakeConnection | None = None,
    channel: FakeChannel | None = None,
    tag: str | None = TAG,
) -> ConsumerHealth:
    manager = make_manager(
        FakeWorker(), connection_manager=FakeConnectionManager(connection)
    )
    manager._channel = channel  # type: ignore[assignment]
    manager._consumer_tag = tag
    return asyncio.run(manager.health(probe_timeout=0.05))


def _live_channel() -> FakeChannel:
    return FakeChannel(underlay=FakeUnderlayChannel({TAG: object()}))


def test_ready_when_connected_open_and_registered() -> None:
    health = _health(connection=FakeConnection(), channel=_live_channel())

    assert health.ready
    assert health.state == "ready"
    assert not health.consumer_lost


def test_no_connection_is_reconnecting() -> None:
    health = _health(connection=None, channel=_live_channel())

    assert not health.connected
    assert health.state == "reconnecting"
    assert not health.consumer_lost


def test_transport_gone_is_reconnecting() -> None:
    connection = FakeConnection()
    connection.transport = None

    health = _health(connection=connection, channel=_live_channel())

    assert health.state == "reconnecting"


def test_blocked_when_transport_ready_does_not_complete() -> None:
    connection = FakeConnection(transport=FakeTransport(blocked=True))

    health = _health(connection=connection, channel=_live_channel())

    assert health.connected
    assert health.blocked
    assert health.state == "blocked"
    assert not health.consumer_lost  # a restart cannot help a blocked connection


def test_closed_channel_is_consumer_lost() -> None:
    health = _health(connection=FakeConnection(), channel=FakeChannel(is_closed=True))

    assert not health.channel_open
    assert health.consumer_lost
    assert health.state == "consumer_lost"


def test_missing_tag_is_consumer_lost() -> None:
    channel = FakeChannel(underlay=FakeUnderlayChannel({"someone-else": object()}))

    health = _health(connection=FakeConnection(), channel=channel)

    assert health.consumer_registered is False
    assert health.consumer_lost
    assert TAG in health.detail


def test_underlay_probe_timeout_is_unknown_not_lost() -> None:
    health = _health(connection=FakeConnection(), channel=FakeChannel(hangs=True))

    assert health.consumer_registered is None
    assert not health.consumer_lost
    assert health.state == "unknown"


def test_underlay_without_consumers_attribute_is_unknown() -> None:
    channel = FakeChannel(underlay=FakeUnderlayChannel(None))

    health = _health(connection=FakeConnection(), channel=channel)

    assert health.consumer_registered is None
    assert not health.consumer_lost


def test_aiormq_channel_defines_consumers() -> None:
    """health() reads ``aiormq.Channel.consumers``, a plain attribute that is
    not on aiormq's ABC. Fail here, not in production, if an upgrade drops it."""
    assert "self.consumers" in inspect.getsource(aiormq.Channel.__init__)
