"""Worker health: the watchdog's settings and a readiness endpoint.

Lives beside the runner rather than under ``warren/runtime`` because
``runtime`` imports ``workers.runners``; the reverse import would be
circular. The endpoint is stdlib only (``asyncio.start_server``): it serves
the watchdog's last sample and never probes on its own, so an HTTP request
can never cost a probe timeout. HTTP/1.0 with ``Connection: close`` keeps
the protocol handling to one request line.
"""

from typing import Any

import asyncio
import dataclasses
import json
import logging
from collections.abc import Callable
from contextlib import suppress

from basics.logging import get_logger
from pydantic import BaseModel, Field

from warren.pubsub.common import ConsumerHealth


module_logger: logging.Logger = get_logger(__name__)


class HealthConfig(BaseModel):
    """Watchdog and endpoint settings.

    :param enabled: Serve ``/ready`` and ``/live``. On by default; a failed
        bind is logged and the worker keeps consuming without the endpoint.
    :param host: Bind address.
    :param port: Bind port; 0 picks a free one (tests).
    :param check_interval_s: Seconds between watchdog observations.
    :param probe_timeout_s: Upper bound on each observation's waits.
    :param consumer_lost_grace_s: How long the consumer may be lost, with a
        live connection, before the process exits so the orchestrator
        restarts it.
    """

    enabled: bool = True
    host: str = "0.0.0.0"  # noqa: S104 — a probe target must be reachable from the kubelet
    port: int = Field(default=8080, ge=0, le=65535)
    check_interval_s: float = Field(default=5.0, gt=0)
    probe_timeout_s: float = Field(default=1.0, gt=0)
    consumer_lost_grace_s: float = Field(default=60.0, ge=0)


class HealthServer:
    """Minimal HTTP readiness endpoint.

    ``GET /ready`` → 200 while the last sample says ready, else 503;
    ``GET /live`` → 200 while the process runs; anything else → 404. The
    body is the sample as JSON.

    :param get_health: Returns the last observed health, or None before the
        first observation.
    :param host: Bind address.
    :param port: Bind port; 0 picks a free one.
    """

    def __init__(
        self,
        get_health: Callable[[], ConsumerHealth | None],
        *,
        host: str,
        port: int,
    ) -> None:
        self._get_health = get_health
        self._host = host
        self._port = port
        self._server: asyncio.AbstractServer | None = None

    @property
    def port(self) -> int | None:
        """The bound port, or None while not serving."""
        if self._server is None or not self._server.sockets:
            return None
        return int(self._server.sockets[0].getsockname()[1])

    async def start(self) -> bool:
        """Bind and serve. A failed bind is reported, not raised.

        :return: True if serving.
        """
        try:
            self._server = await asyncio.start_server(
                self._handle, self._host, self._port
            )
        except OSError as e:
            module_logger.error(
                f"Health endpoint not started on {self._host}:{self._port}: {e}; "
                f"the worker keeps consuming without it"
            )
            return False
        module_logger.info(f"Health endpoint serving on {self._host}:{self.port}")
        return True

    async def stop(self) -> None:
        if self._server is None:
            return
        self._server.close()
        await self._server.wait_closed()
        self._server = None

    async def _handle(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        try:
            request_line = await asyncio.wait_for(reader.readline(), timeout=5.0)
            parts = request_line.decode("latin-1").split()
            path = parts[1] if len(parts) >= 2 else ""
            status, body = self._respond(path)
            writer.write(
                f"HTTP/1.0 {status}\r\nContent-Type: application/json\r\n"
                f"Content-Length: {len(body)}\r\nConnection: close\r\n\r\n".encode()
                + body
            )
            await writer.drain()
        except (TimeoutError, OSError):
            # A probe that disconnects early is not worth a log line.
            pass
        finally:
            writer.close()
            with suppress(OSError):
                await writer.wait_closed()

    def _respond(self, path: str) -> tuple[str, bytes]:
        health = self._get_health()
        payload: dict[str, Any] = (
            {**dataclasses.asdict(health), "state": health.state}
            if health is not None
            else {"detail": "no health sample yet"}
        )
        body = json.dumps(payload).encode()
        if path == "/live":
            return "200 OK", body
        if path == "/ready":
            ready = health is not None and health.ready
            return ("200 OK" if ready else "503 Service Unavailable"), body
        return "404 Not Found", b"{}"
