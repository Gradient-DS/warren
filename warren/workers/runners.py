"""
Base class for running a worker process.

Defines a transport-independent lifecycle for worker processes. Subclasses
implement ``setup()`` to wire their specific components and set
``self._consumer_manager`` before calling ``run()``.

Lifecycle: setup() -> run() -> teardown()
- setup() initializes connections, creates worker/consumer (subclass-specific)
- run() starts consuming and waits for shutdown signal
- teardown() cleans up resources

``run()`` also runs a watchdog: it polls ``consumer_manager.health()`` and,
when the consumer has been lost with a live connection for longer than
``HealthConfig.consumer_lost_grace_s``, ends the run with ``WarrenError`` so
the process exits non-zero and the orchestrator restarts it. Blocked and
reconnecting states are logged and left to the transport. The last sample
is served on the readiness endpoint when ``HealthConfig.enabled``.
"""

import asyncio
import logging
import signal
from abc import ABCMeta, abstractmethod
from collections.abc import Callable, Iterator
from contextlib import contextmanager, suppress

from basics.base import Base
from basics.logging_utils import summarize_exception_chain

from warren.common import MessageConsumerInterface
from warren.exceptions import WarrenError
from warren.pubsub.common import ConsumerHealth, ConsumerManagerInterface
from warren.workers.health import HealthConfig, HealthServer


ConsumerManagerFactory = Callable[
    [MessageConsumerInterface],
    ConsumerManagerInterface,
]
"""Creates a consumer manager for the given consumer.

Signature: ``(consumer) -> ConsumerManagerInterface``

The factory encapsulates transport-specific wiring (exchange, queue,
connection) so runners stay transport-agnostic. The consumer carries
its own identity (name and type) for use on failure envelopes.
"""


# Only consumer_lost is grounds for exiting; blocked is the broker's back-pressure.
_HEALTH_LOG_LEVEL: dict[str, int] = {
    "ready": logging.INFO,
    "reconnecting": logging.INFO,
    "blocked": logging.WARNING,
    "consumer_lost": logging.ERROR,
}


class WorkerRunnerBase(Base, metaclass=ABCMeta):
    """Base class for running a worker process.

    Provides the generic lifecycle: signal handling, consume loop, and
    graceful shutdown. Subclasses implement ``setup()`` to wire their
    specific components (connections, stores, worker, consumer manager)
    and must set ``self._consumer_manager`` before ``setup()`` returns.

    Override ``_on_teardown()`` for subclass-specific cleanup (e.g.,
    closing connections, cancelling timers). It runs after the consumer
    is stopped but before state is reset.

    Usage::

        runner = MyRunner(...)
        await runner.setup()    # subclass wires components
        await runner.run()      # consume until shutdown signal
        await runner.teardown() # cleanup
    """

    def __init__(
        self,
        *,
        name: str | None = None,
        health: HealthConfig | None = None,
    ) -> None:
        super().__init__(pybase_logger_name=name)
        self._consumer_manager: ConsumerManagerInterface | None = None
        self._setup_succeeded: bool = False
        self._health = health or HealthConfig()
        self._last_health: ConsumerHealth | None = None
        self._fatal_health: str | None = None

    @abstractmethod
    async def setup(self) -> None:
        """Initialize all resources and wire components.

        Subclasses must set ``self._consumer_manager`` and call
        ``self._mark_setup_succeeded()`` at the end.
        """
        ...

    async def run(self) -> None:
        """Start consuming and wait for shutdown signal (SIGINT/SIGTERM).

        :raises WarrenError: If the watchdog declared the consumer lost.
        """
        if not self._setup_succeeded:
            msg = "Must call setup() successfully before run()"
            raise RuntimeError(msg)

        shutdown_event = asyncio.Event()

        def _signal_handler(sig: int, frame: object) -> None:
            self._log.info(f"Received signal {sig}, initiating graceful shutdown...")
            shutdown_event.set()

        signal.signal(signal.SIGINT, _signal_handler)
        signal.signal(signal.SIGTERM, _signal_handler)

        server: HealthServer | None = None
        if self._health.enabled:
            server = HealthServer(
                lambda: self._last_health,
                host=self._health.host,
                port=self._health.port,
            )
            await server.start()  # a failed bind is logged inside; never fatal

        await self._consumer_manager.start_consuming()
        self._log.info("Worker started, waiting for messages...")
        watchdog = asyncio.create_task(self._watch_health(shutdown_event))
        try:
            await shutdown_event.wait()
        finally:
            watchdog.cancel()
            with suppress(asyncio.CancelledError):
                await watchdog
            if server is not None:
                await server.stop()

        if self._fatal_health is not None:
            raise WarrenError(self._fatal_health)

    async def _watch_health(self, shutdown_event: asyncio.Event) -> None:
        """Poll consumer health; end the run when the consumer is lost.

        Only ``consumer_lost`` (connection alive and unblocked, consumer
        gone) is grounds for exiting: a blocked or reconnecting connection
        is the transport's to recover and a restart would not help it.
        """
        loop = asyncio.get_running_loop()
        lost_since: float | None = None
        previous: ConsumerHealth | None = None
        while True:
            await asyncio.sleep(self._health.check_interval_s)
            try:
                health = await self._consumer_manager.health(
                    probe_timeout=self._health.probe_timeout_s
                )
            except Exception as e:
                self._log.error(f"Health check failed: {summarize_exception_chain(e)}")
                continue
            self._last_health = health
            self._log_health_transition(previous, health)
            previous = health
            if not health.consumer_lost:
                lost_since = None
                continue
            if lost_since is None:
                lost_since = loop.time()
            if loop.time() - lost_since >= self._health.consumer_lost_grace_s:
                self._fatal_health = (
                    f"Consumer lost for {self._health.consumer_lost_grace_s:g}s with a "
                    f"live connection ({health.detail}); exiting so the orchestrator "
                    f"restarts this worker"
                )
                self._log.error(self._fatal_health)
                shutdown_event.set()
                return

    def _log_health_transition(
        self, previous: ConsumerHealth | None, current: ConsumerHealth
    ) -> None:
        if previous is not None and previous.state == current.state:
            return
        level = _HEALTH_LOG_LEVEL.get(current.state, logging.WARNING)
        detail = f" ({current.detail})" if current.detail else ""
        self._log.log(level, f"Consumer state: {current.state}{detail}")

    async def teardown(self) -> None:
        """Stop consuming, run subclass cleanup, and reset state.

        Idempotent and safe after a partial or failed ``setup()``: tears
        down whatever exists, best-effort, regardless of whether
        ``setup()`` ran to completion. ``_setup_succeeded`` gates
        ``run()`` only — never cleanup.
        """
        if self._consumer_manager is not None:
            try:
                await self._consumer_manager.stop_consuming()
            except Exception as e:
                self._log.warning(
                    f"Error stopping consumer: {summarize_exception_chain(e)}"
                )

        try:
            await self._on_teardown()
        except Exception as e:
            self._log.warning(f"Error during teardown: {summarize_exception_chain(e)}")

        self._consumer_manager = None
        self._setup_succeeded = False
        self._log.info("Worker stopped.")

    async def _on_teardown(self) -> None:
        """Subclass cleanup hook.

        Called after the consumer is stopped but before state is reset.
        Override to close connections, cancel timers, etc.
        """
        pass

    def _mark_setup_succeeded(self) -> None:
        """Mark setup as complete. Call at the end of ``setup()``."""
        self._setup_succeeded = True

    @contextmanager
    def _exception_wrapping(self, description: str) -> Iterator[None]:
        """Wrap a setup phase so a failure carries phase + worker context.

        Use around calls to overridable methods/hooks or externally
        injected callables — neither can be trusted to contextualise
        their own errors. The original exception is chained, so
        ``summarize_exception_chain`` renders the full layered trace
        (phase -> inner detail -> root cause).

        A synchronous context manager intentionally — it correctly
        wraps exceptions raised by ``await`` expressions inside its
        ``with`` body.

        :param description: Human-readable name of the phase being run.
        """
        try:
            yield
        except Exception as e:
            worker = getattr(self, "_worker_name", "?")
            msg = f"{description} failed for worker '{worker}'"
            raise WarrenError(msg) from e
