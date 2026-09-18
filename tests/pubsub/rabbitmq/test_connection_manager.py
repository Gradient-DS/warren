"""Pins what ``RMQConnectionManager`` hands to ``aio_pika.connect_robust``.

No broker: ``connect_robust`` is replaced by a recorder. The heartbeat is
the load-bearing kwarg — aiormq reads it from the URL query and RabbitMQ
takes the client's Tune-Ok value as-is, so this kwarg alone decides the
negotiated timeout.
"""

import asyncio

import pytest
from aio_pika.connection import make_url
from pydantic import ValidationError

from warren.pubsub.rabbitmq.aio_pika import connection as connection_module
from warren.pubsub.rabbitmq.aio_pika.connection import RMQConnectionManager
from warren.pubsub.rabbitmq.config import RMQConnectionConfig


class _FakeConnection:
    is_closed = False

    async def close(self) -> None:
        pass


def _connect(monkeypatch: pytest.MonkeyPatch, config: RMQConnectionConfig) -> dict:
    captured: dict = {}

    async def fake_connect_robust(**kwargs):
        captured.update(kwargs)
        return _FakeConnection()

    monkeypatch.setattr(
        connection_module.aio_pika, "connect_robust", fake_connect_robust
    )
    asyncio.run(RMQConnectionManager(config).setup())
    return captured


def test_setup_passes_heartbeat_to_connect_robust(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured = _connect(monkeypatch, RMQConnectionConfig(heartbeat=600))

    assert captured["heartbeat"] == 600


def test_setup_omits_heartbeat_when_unset(monkeypatch: pytest.MonkeyPatch) -> None:
    captured = _connect(monkeypatch, RMQConnectionConfig())

    assert captured["heartbeat"] is None
    # aio-pika's URL builder drops None, so aiormq sees no query parameter
    # and keeps its own default (60) — proven with the real make_url.
    # Defaults for the credentials: a literal password trips ruff S106.
    url = make_url(host="h", heartbeat=captured["heartbeat"])
    assert "heartbeat" not in url.query


@pytest.mark.parametrize("value", [-1, 65535])
def test_heartbeat_rejects_out_of_range(value: int) -> None:
    with pytest.raises(ValidationError):
        RMQConnectionConfig(heartbeat=value)
