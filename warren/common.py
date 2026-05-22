"""
Shared contracts for the distributed processing system.

Defines the message consumer protocols and failure exceptions that form
the contract between pubsub (consumer managers) and workers. These live
at the distributed/ level because both packages depend on them.

Protocol hierarchy:

- ``MessageConsumerIdentity`` — base protocol providing consumer
  identity (``name`` and ``type`` properties).

- ``AsyncMessageConsumerInterface`` — async consumer that runs on the
  main event loop.  The consumer must yield control regularly (via
  ``await``) so that RabbitMQ heartbeats are not starved.

- ``SyncMessageConsumerInterface`` — sync consumer that the framework
  automatically dispatches to a thread pool, keeping the event loop
  free for heartbeats.

- ``MessageConsumerInterface`` — union of both, accepted by consumer
  managers.
"""

from typing import Protocol, Union

from document_processing.distributed.warren.exceptions import WarrenError


class SoftFailureException(WarrenError):
    """Retryable failure — message is retried with back-off.

    Workers raise this to signal a transient failure. The consumer manager
    catches it and routes the message to retry infrastructure with
    exponential backoff.

    :param reason: Human-readable description of the failure.
    :param retry_after: Suggested initial delay in seconds before retry.
        If None, the consumer manager's default is used.
    :param retry_max: Suggested maximum retry attempts.
        If None, the consumer manager's default is used.
    :param backoff_base: Base for exponential backoff calculation.
        Delay on attempt N = retry_after * backoff_base^(N-1).
        If None, the consumer manager's default is used.
    :param jitter: Whether to add random jitter to the delay.
        If None, the consumer manager's default is used.
    :param cause: Optional exception that caused this failure.
        Sets ``__cause__`` for exception chaining.
    :param retry_count_consumed: If True (default), incrementing the
        message's retry counter and checking against ``retry_max``
        happens normally — the typical "this is a transient failure;
        try again, but give up after N attempts" semantics. If False,
        the consumer manager redelivers after the delay without
        incrementing the counter or enforcing the max — used when the
        worker is using the retry mechanism as generic delayed
        redelivery (e.g. polling a long-running external operation
        like a Weaviate backup) rather than recovering from a failure.
        Workers must guarantee progress by their own means in that
        case (e.g. eventually succeeding or raising HardFailure).
    """

    def __init__(
        self,
        reason: str,
        *,
        retry_after: int | None = None,
        retry_max: int | None = None,
        backoff_base: float | None = None,
        jitter: bool | None = None,
        cause: Exception | None = None,
        retry_count_consumed: bool = True,
    ) -> None:
        super().__init__(reason)
        self.reason = reason
        self.retry_after = retry_after
        self.retry_max = retry_max
        self.backoff_base = backoff_base
        self.jitter = jitter
        self.retry_count_consumed = retry_count_consumed
        if cause is not None:
            self.__cause__ = cause


class HardFailureException(WarrenError):
    """Permanent failure — message is dropped. No retry."""

    pass


class MessageConsumerIdentity(Protocol):
    """Identity for a message consumer — name and type.

    Base protocol bundling identity with the consumer. Both
    async and sync consumer interfaces inherit from this,
    ensuring that any callable consumer also carries its
    identity.
    """

    @property
    def name(self) -> str:
        """Unique instance identifier."""
        ...

    @property
    def type(self) -> str:
        """Identifies the kind of consumer (e.g., ``"parser_worker"``)."""
        ...


class AsyncMessageConsumerInterface(MessageConsumerIdentity, Protocol):
    """
    Async consumer with identity — runs on the main event loop.

    Heartbeat Safety Contract
    -------------------------
    This consumer runs on the same event loop as the RabbitMQ connection.
    The event loop must not be blocked, or heartbeats will fail and the
    connection will be dropped.

    Rules:
    - Pure async I/O (HTTP clients, DB queries) is safe — every
      ``await`` yields control for heartbeats.
    - CPU-bound or blocking I/O work MUST be offloaded to a thread or
      process pool using ``asyncio.get_event_loop().run_in_executor()``.
    - If your consumer is primarily CPU-bound or uses sync libraries, use
      ``SyncMessageConsumerInterface`` instead — the framework handles
      threading automatically.
    """

    async def __call__(self, message: dict) -> dict | None:
        """
        Process a message.

        Example — async consumer with CPU offloading::

            async def __call__(self, message: Dict) -> Optional[Dict]:
                async with aiohttp.ClientSession() as session:
                    resp = await session.get("https://api.example.com/data")
                    data = await resp.json()

                loop = asyncio.get_event_loop()
                result = await loop.run_in_executor(None, cpu_heavy_parse, data)
                return {"parsed": result}


        :param message: Message body as a dictionary.

        :return: Optional result to publish downstream, or None if no
            downstream message is needed.

        :raises SoftFailureException: When failure is retryable. Message
            will be routed to retry queue with back-off.
        :raises HardFailureException: When failure is permanent. Message
            will be dropped.
        """
        ...


class SyncMessageConsumerInterface(MessageConsumerIdentity, Protocol):
    """
    Sync consumer with identity — framework runs this in a thread pool.

    The framework automatically executes this consumer in a
    ``ThreadPoolExecutor`` via ``run_in_executor()``, keeping the main
    event loop free for RabbitMQ heartbeats.

    Rules:
    - CPU-bound work and sync library calls (``requests``, file I/O,
      sync DB drivers) work naturally — no special handling needed.
    - The consumer may spawn its own ``ProcessPoolExecutor`` or
      ``multiprocessing.Pool`` internally for CPU parallelism.
    - Do NOT use ``asyncio.run()`` or ``asyncio.get_event_loop()`` —
      async objects from the main loop cannot be used from a different
      thread due to event loop affinity.
    - If you need async I/O (aiohttp, async DB clients), use
      ``AsyncMessageConsumerInterface`` instead.
    """

    def __call__(self, message: dict) -> dict | None:
        """
        Process a message.

        Example — sync consumer with HTTP calls::

            def __call__(self, message: Dict) -> Optional[Dict]:
                parsed = heavy_parse(message["payload"])

                resp = requests.post("https://api.example.com", json=parsed)
                resp.raise_for_status()
                return resp.json()

        :param message: Message body as a dictionary.

        :return: Optional result to publish downstream, or None if no
            downstream message is needed.

        :raises SoftFailureException: When failure is retryable. Message
            will be routed to retry queue with back-off.
        :raises HardFailureException: When failure is permanent. Message
            will be dropped.

        """
        ...


MessageConsumerInterface = Union[
    AsyncMessageConsumerInterface,
    SyncMessageConsumerInterface,
]
"""Union type accepted by the consumer manager — either sync or async."""
