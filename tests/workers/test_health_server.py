"""Pins the readiness endpoint: status codes per path, the JSON body, a
bind failure that is reported rather than raised, and a clean stop."""

import asyncio
import json
import logging

import pytest

from warren.pubsub.common import ConsumerHealth
from warren.workers.health import HealthConfig, HealthServer


READY = ConsumerHealth(
    connected=True, blocked=False, channel_open=True, consumer_registered=True
)
LOST = ConsumerHealth(
    connected=True,
    blocked=False,
    channel_open=False,
    consumer_registered=None,
    detail="closed",
)


async def _get(port: int, path: str) -> tuple[int, dict]:
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    writer.write(f"GET {path} HTTP/1.0\r\n\r\n".encode())
    await writer.drain()
    raw = await reader.read()
    writer.close()
    head, _, body = raw.partition(b"\r\n\r\n")
    status = int(head.split(b" ")[1])
    return status, json.loads(body or b"{}")


def _serve(sample: ConsumerHealth | None, path: str) -> tuple[int, dict]:
    async def run() -> tuple[int, dict]:
        server = HealthServer(lambda: sample, host="127.0.0.1", port=0)
        assert await server.start()
        try:
            return await _get(server.port, path)
        finally:
            await server.stop()

    return asyncio.run(run())


def test_ready_200_when_sample_ready() -> None:
    status, body = _serve(READY, "/ready")

    assert status == 200
    assert body["consumer_registered"] is True


def test_ready_503_when_consumer_lost() -> None:
    status, body = _serve(LOST, "/ready")

    assert status == 503
    assert body["detail"] == "closed"


def test_ready_503_before_first_sample() -> None:
    status, body = _serve(None, "/ready")

    assert status == 503
    assert "no health sample" in body["detail"]


def test_live_200_regardless_of_sample() -> None:
    assert _serve(LOST, "/live")[0] == 200


def test_unknown_path_404() -> None:
    assert _serve(READY, "/metrics")[0] == 404


def test_bind_failure_is_logged_not_raised(caplog: pytest.LogCaptureFixture) -> None:
    async def run() -> bool:
        first = HealthServer(lambda: READY, host="127.0.0.1", port=0)
        assert await first.start()
        second = HealthServer(lambda: READY, host="127.0.0.1", port=first.port)
        try:
            return await second.start()
        finally:
            await first.stop()

    with caplog.at_level(logging.ERROR):
        assert asyncio.run(run()) is False
    assert "Health endpoint not started" in caplog.text


def test_health_config_defaults() -> None:
    config = HealthConfig()

    assert config.enabled is True
    assert config.port == 8080
    assert config.consumer_lost_grace_s == 60.0
