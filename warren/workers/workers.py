import asyncio
import re
from abc import ABCMeta, abstractmethod

from basics.base import Base

from document_processing.distributed.warren.common import (
    AsyncMessageConsumerInterface,
    SyncMessageConsumerInterface,
)


def _to_snake_case(name: str) -> str:
    """Convert PascalCase/camelCase to snake_case."""
    s = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", name)
    s = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s)
    return s.lower()


# Base must precede Protocol classes in MRO so that super().__init__()
# reaches Base.__init__ — Protocol/Generic do not propagate
# super().__init__() with keyword arguments.


class AsyncProcessingWorkerBase(Base, AsyncMessageConsumerInterface, metaclass=ABCMeta):
    """
    Abstract base class for async processing workers.

    Satisfies the ``AsyncMessageConsumerInterface`` protocol. Workers
    receive plain dictionaries and return optional dictionaries — they
    never interact with RabbitMQ or any other message transport directly.

    Heartbeat Safety
    ----------------
    This worker runs on the main event loop alongside the RabbitMQ
    connection. Every ``await`` yields control so heartbeats can be
    processed.

    - Pure async I/O (HTTP clients, DB queries) is safe.
    - CPU-bound work MUST be offloaded via
      ``asyncio.get_event_loop().run_in_executor()``.
    - If your worker is primarily CPU-bound or uses sync libraries,
      use ``SyncProcessingWorkerBase`` instead — the framework handles
      threading automatically.

    When to Use
    -----------
    - Worker needs async I/O with shared connection pools (aiohttp,
      async DB clients).
    - Worker combines async I/O with some CPU work (offload CPU parts
      via ``run_in_executor``).
    """

    def __init__(
        self,
        worker_name: str,
        *,
        worker_type: str | None = None,
    ) -> None:
        classname = type(self).__name__
        super().__init__(pybase_logger_name=f"[{classname}] {worker_name}")
        self._worker_name = worker_name
        self._worker_type = worker_type or _to_snake_case(classname)

    @property
    def name(self) -> str:
        """Unique instance identifier for this worker."""
        return self._worker_name

    @property
    def type(self) -> str:
        """Identifies the kind of worker (e.g., ``"parser_worker"``)."""
        return self._worker_type

    async def setup(self) -> None:
        """Start any owned async resources.

        Called by the runner after the worker is constructed and before
        the consumer begins delivering messages. Override to start
        long-lived async resources (browser subprocesses, HTTP client
        pools, connection warm-ups) that cannot be safely initialised
        in ``__init__`` because they need the running event loop.

        Default: no-op.
        """
        pass

    async def teardown(self) -> None:
        """Close any owned async resources.

        Called by the runner after the consumer has stopped delivering
        messages and before infrastructure (MongoDB, Redis, RMQ) is torn
        down. Override to close resources that require an explicit async
        moment — httpx/AsyncOpenAI pools (cannot close from ``__del__``)
        and browser subprocesses (GC does not reap them).

        Default: no-op. Implementations should be best-effort: log and
        continue rather than raise, so one resource's failure does not
        strand another.
        """
        pass

    @abstractmethod
    async def __call__(self, message: dict) -> dict | None:
        """
        Process a message from the queue.

        :param message: Message body as a dictionary.

        :return: Optional result to publish downstream, or None if no
            downstream message is needed.

        :raises SoftFailureException: When failure is retryable. Message
            will be routed to retry queue with back-off.
        :raises HardFailureException: When failure is permanent. Message
            will be dropped.
        """
        ...


class SyncProcessingWorkerBase(Base, SyncMessageConsumerInterface, metaclass=ABCMeta):
    """
    Abstract base class for sync processing workers.

    Satisfies the ``SyncMessageConsumerInterface`` protocol. The framework
    automatically runs ``__call__`` in a ``ThreadPoolExecutor`` via
    ``run_in_executor()``, keeping the main event loop free for
    RabbitMQ heartbeats.

    Threading Behavior
    ------------------
    Each invocation runs in a thread from the default
    ``ThreadPoolExecutor`` (sized ``min(32, os.cpu_count() + 4)``).
    The worker may spawn its own ``ProcessPoolExecutor`` or
    ``multiprocessing.Pool`` internally for CPU parallelism across
    cores.

    - CPU-bound work and sync library calls (``requests``, file I/O,
      sync DB drivers) work naturally.
    - Do NOT use ``asyncio.run()`` or ``asyncio.get_event_loop()`` —
      async objects from the main loop cannot be used from a different
      thread due to event loop affinity.
    - If you need async I/O (aiohttp, async DB clients), use
      ``AsyncProcessingWorkerBase`` instead.

    When to Use
    -----------
    - Worker does CPU-bound work (parsing, extraction, computation).
    - Worker uses sync libraries (``requests``, file I/O, sync DB
      drivers).
    - Worker manages its own multiprocessing internally.
    """

    def __init__(
        self,
        worker_name: str,
        *,
        worker_type: str | None = None,
    ) -> None:
        classname = type(self).__name__
        super().__init__(pybase_logger_name=f"[{classname}] {worker_name}")
        self._worker_name = worker_name
        self._worker_type = worker_type or _to_snake_case(classname)

    @property
    def name(self) -> str:
        """Unique instance identifier for this worker."""
        return self._worker_name

    @property
    def type(self) -> str:
        """Identifies the kind of worker (e.g., ``"parser_worker"``)."""
        return self._worker_type

    async def setup(self) -> None:
        """Start any owned async resources. Default: no-op.

        See ``AsyncProcessingWorkerBase.setup`` for semantics. Sync
        workers can still own async resources at the process level (the
        runner operates on the event loop even when ``__call__`` runs in
        a thread).
        """
        pass

    async def teardown(self) -> None:
        """Close any owned async resources. Default: no-op.

        See ``AsyncProcessingWorkerBase.teardown`` for semantics.
        """
        pass

    @abstractmethod
    def __call__(self, message: dict) -> dict | None:
        """
        Process a message from the queue.

        :param message: Message body as a dictionary.

        :return: Optional result to publish downstream, or None if no
            downstream message is needed.

        :raises SoftFailureException: When failure is retryable. Message
            will be routed to retry queue with back-off.
        :raises HardFailureException: When failure is permanent. Message
            will be dropped.
        """
        ...


class FilteringWorkerBase(AsyncProcessingWorkerBase):
    """
    Async worker that self-selects messages via should_process().

    Subclasses implement should_process() to decide whether a message
    is relevant, and process() for the actual work. When should_process()
    returns False, the message is ACKed with no downstream publish.
    """

    def __init__(
        self,
        worker_name: str,
        *,
        worker_type: str | None = None,
    ) -> None:
        super().__init__(worker_name, worker_type=worker_type)

    @abstractmethod
    def should_process(self, message: dict) -> bool:
        """Decide whether this worker should handle the message.

        :param message: Message body as a dictionary.
        :return: True if this worker should process the message.
        """
        ...

    @abstractmethod
    async def process(self, message: dict) -> dict | None:
        """Process a message that passed should_process().

        :param message: Message body as a dictionary.

        :return: Optional result to publish downstream, or None if no
            downstream message is needed (terminal worker).

        :raises SoftFailureException: When failure is retryable.
        :raises HardFailureException: When failure is permanent.
        """
        ...

    async def __call__(self, message: dict) -> dict | None:
        if not self.should_process(message):
            return None
        return await self.process(message)


# Example workers for testing and demonstration purposes.
class EchoWorker(AsyncProcessingWorkerBase):
    def __init__(self, worker_name: str):
        super().__init__(worker_name)

    async def __call__(self, message: dict) -> dict | None:
        self._log.info(f"Processing: {message}")
        await asyncio.sleep(1)  # simulate work
        return {"source": self._worker_name, "result": message}
