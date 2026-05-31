"""
Base class for running a worker process.

Defines a transport-independent lifecycle for worker processes. Subclasses
implement ``setup()`` to wire their specific components and set
``self._consumer_manager`` before calling ``run()``.

Lifecycle: setup() -> run() -> teardown()
- setup() initializes connections, creates worker/consumer (subclass-specific)
- run() starts consuming and waits for shutdown signal
- teardown() cleans up resources
"""

from typing import Callable, Iterator, Optional

from abc import ABCMeta, abstractmethod
import asyncio
from contextlib import contextmanager
import signal

from basics.base import Base
from basics.logging_utils import summarize_exception_chain

from document_processing.distributed.warren.common import MessageConsumerInterface
from document_processing.distributed.warren.exceptions import WarrenError
from document_processing.distributed.warren.pubsub.common import (
    ConsumerManagerInterface,
)


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

    def __init__(self, *, name: Optional[str] = None) -> None:
        super().__init__(pybase_logger_name=name)
        self._consumer_manager: Optional[ConsumerManagerInterface] = None
        self._setup_succeeded: bool = False

    @abstractmethod
    async def setup(self) -> None:
        """Initialize all resources and wire components.

        Subclasses must set ``self._consumer_manager`` and call
        ``self._mark_setup_succeeded()`` at the end.
        """
        ...

    async def run(self) -> None:
        """Start consuming and wait for shutdown signal
        (SIGINT/SIGTERM)."""
        if not self._setup_succeeded:
            raise RuntimeError("Must call setup() successfully before run()")

        shutdown_event = asyncio.Event()

        def _signal_handler(sig: int, frame: object) -> None:
            self._log.info(f"Received signal {sig}, initiating graceful shutdown...")
            shutdown_event.set()

        signal.signal(signal.SIGINT, _signal_handler)
        signal.signal(signal.SIGTERM, _signal_handler)

        await self._consumer_manager.start_consuming()
        self._log.info("Worker started, waiting for messages...")
        await shutdown_event.wait()

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
            self._log.warning(
                f"Error during teardown: {summarize_exception_chain(e)}"
            )

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
            raise WarrenError(
                f"{description} failed for worker '{worker}'"
            ) from e
