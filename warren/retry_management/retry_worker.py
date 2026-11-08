"""Retry worker -- persists messages and schedules delayed republishing.

Consumes ``data_type: "soft-failure"`` messages from the observer
exchange, unwraps the failed message, stores it durably, and uses
``asyncio.call_later`` to trigger republishing after the configured
delay. On startup, ``schedule_pending`` loads persisted messages and
reschedules remaining delays. Distributed runners use a lease so only
one worker schedules and republishes at a time.
"""

import asyncio
import time
import uuid
from collections.abc import Awaitable, Callable
from contextlib import suppress
from datetime import UTC, datetime

from basics.logging_utils import summarize_exception_chain

from warren._retry_policy import nonnegative_number, retry_metadata
from warren.pubsub.common import (
    PublisherInterface,
    PublishFailureException,
    RetryConfig,
)
from warren.retry_management.lease import RetryLeaseInterface
from warren.storage.document_store.interface import (
    DocumentAlreadyExistsError,
    DocumentNotFoundError,
    DocumentStoreInterface,
)
from warren.storage.scoping import scope_fields
from warren.workers.messages import (
    build_message_key,
    extract_message_identity,
    validate_message,
)
from warren.workers.workers import (
    AsyncProcessingWorkerBase,
)


class RetryWorker(AsyncProcessingWorkerBase):
    """Worker that manages retry scheduling for soft-failed messages.

    Observes the observer exchange and acts only on ``soft-failure``
    envelopes. It ignores all other messages. For each envelope:

    1. Persists to store (durability).
    2. Schedules ``asyncio.call_later`` for delayed republishing.
    3. On timer: fetches message, republishes, deletes from store.

    On startup (via ``schedule_pending``), loads scheduling metadata for
    persisted messages and reschedules remaining delays. With a lease, call
    ``start`` to acquire ownership and poll for takeover. Standbys only persist.
    Omitting the lease is intended for single-process use.

    A generation is the store-issued UUID of one accepted pending row, not
    the untrusted retry count. A pending generation is immutable. Before
    publishing, its row is conditionally marked as republishing so an immediate
    failure can replace it. Cleanup matches the old generation and preserves
    that replacement. Marked rows remain recoverable after an interrupted publish.
    Legacy rows without a generation use the same transitions.

    :param worker_name: Worker identifier.
    :param retry_store: Persistent store for messages awaiting retry.
        Must use ``"retry_key"`` as its ``doc_id_field``.
    :param republish_publisher: Publisher targeting the processing
        exchange.
    :param message_key_func: Function to extract a composite key from
        a message dict. Defaults to building key from
        job_id/doc_id/part_idx using ``build_message_key``.
    :param retry_config: Delay defaults and caps, shared with the consumers.
    :param scoping_enabled: Prefix the retry key with the scope of the
        message (``s:<scope>:``) when the message has a valid scope.
    :param lease: Lease that lets one instance schedule and republish at a
        time. None means single-process use: this instance always
        schedules.
    :param lease_ttl_seconds: Lifetime in seconds of the lease.
    :param clock: Current time in seconds. Replace it in tests.
    :param sleep: Delay function for the lease loop. Replace it in tests.

    :raises ValueError: If retry_store's ``doc_id_field`` is not
        ``"retry_key"``, or if ``lease_ttl_seconds`` is 0 or less.
    """

    REQUIRED_DOC_ID_FIELD: str = "retry_key"

    def __init__(
        self,
        worker_name: str,
        *,
        retry_store: DocumentStoreInterface,
        republish_publisher: PublisherInterface,
        message_key_func: Callable[[dict], str] | None = None,
        scoping_enabled: bool = False,
        retry_config: RetryConfig | None = None,
        lease: RetryLeaseInterface | None = None,
        lease_ttl_seconds: int = 30,
        clock: Callable[[], float] = time.time,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        super().__init__(worker_name)

        doc_id_field = retry_store.get_doc_id_field()
        if doc_id_field != self.REQUIRED_DOC_ID_FIELD:
            msg = f"retry_store doc_id_field must be '{self.REQUIRED_DOC_ID_FIELD}', got '{doc_id_field}'"
            raise ValueError(msg)

        if lease_ttl_seconds <= 0:
            msg = "lease_ttl_seconds must be positive"
            raise ValueError(msg)
        self._retry_config = retry_config or RetryConfig()
        self._lease = lease
        self._lease_ttl = lease_ttl_seconds
        self._clock = clock
        self._sleep = sleep
        self._holder_id = str(uuid.uuid4())
        self._lease_expires_at = 0.0
        self._lease_task: asyncio.Task[None] | None = None
        self._lease_timer: asyncio.TimerHandle | None = None
        self._republishing_keys: set[str] = set()
        self._scoping_enabled = scoping_enabled
        self._retry_store = retry_store
        self._republish_publisher = republish_publisher
        self._message_key_func = message_key_func or self._default_message_key
        self._pending_timers: dict[str, asyncio.TimerHandle] = {}

        # Strong references to in-flight republish tasks. Without this,
        # asyncio only holds a weak reference to a bare create_task()
        # result, so the task can be garbage-collected mid-flight. Tasks
        # remove themselves on completion via add_done_callback.
        self._republish_tasks: set[asyncio.Task[None]] = set()

        # Serialize local writes with fetch/publish/delete.
        self._retry_lock: asyncio.Lock = asyncio.Lock()

    async def __call__(self, message: dict) -> dict | None:
        """Unwrap soft-failure envelope, persist, and schedule.

        :param message: Soft-failure wrapper with
            ``data_type: "soft-failure"`` and the failed message in
            ``data``.

        :return: None -- no downstream publish.
        """
        validate_message(message)
        if message.get("data_type") != "soft-failure":
            return None

        failed_message: dict = message["data"]
        retry_info = retry_metadata(failed_message.get("retry", {}))
        fields = scope_fields(self._scoping_enabled, failed_message.get("scope"))
        retry_key = self._message_key_func(failed_message)
        if fields:
            retry_key = f"s:{fields['scope']}:{retry_key}"
        delay_seconds = min(retry_info.get("after", self._retry_config.initial_delay), self._retry_config.max_delay_cap)

        generation = str(uuid.uuid4())

        fire_at = self._clock() + delay_seconds
        envelope = {
            **fields,
            self.REQUIRED_DOC_ID_FIELD: retry_key,
            "message": failed_message,
            "fire_at": fire_at,
            "fire_at_date": datetime.fromtimestamp(fire_at, UTC),
            "retry_after_seconds": delay_seconds,
            "generation": generation,
            "republishing": False,
        }

        identity = extract_message_identity(failed_message)
        self._log.info(
            f"[{identity}] Scheduling retry in {delay_seconds}s "
            f"(attempt {retry_info.get('count', '?')}/"
            f"{retry_info.get('max', '?')})"
        )

        async with self._retry_lock:
            while True:
                try:
                    await self._retry_store.insert(envelope)
                    break
                except DocumentAlreadyExistsError:
                    # Read authoritative state: a different process may have changed it.
                    current = await anext(self._retry_store.query({self.REQUIRED_DOC_ID_FIELD: retry_key}), None)
                    if current is None:
                        continue
                    if current.get("republishing") is not True:
                        self._log.info(f"[{identity}] Retry already pending; preserving its generation")
                        return None
                    if await self._retry_store.update_if(
                        retry_key,
                        envelope,
                        expected={"generation": current.get("generation"), "republishing": True},
                    ):
                        break
            self._schedule_republish(retry_key, delay_seconds, generation)

        return None

    async def schedule_pending(self) -> None:
        """Load persisted retries and schedule timers.

        Called on startup after setup. Queries the retry store for all
        persisted messages, computes remaining delay, and schedules
        timers. Messages past their fire time are republished
        immediately.
        """
        if not self._can_publish():
            return
        now = self._clock()
        immediate_count = 0
        scheduled_count = 0
        skipped_count = 0

        async with self._retry_lock:
            async for envelope in self._retry_store.query({}):
                if not self._can_publish():
                    return
                try:
                    retry_key = envelope[self.REQUIRED_DOC_ID_FIELD]
                    validate_message(envelope["message"])
                    fire_at = nonnegative_number(envelope["fire_at"], "retry fire_at")
                    fire_at_date = datetime.fromtimestamp(fire_at, UTC)
                except (KeyError, ValueError, OverflowError, OSError) as e:
                    # A single malformed persisted envelope must not abort
                    # recovery of the rest.
                    skipped_count += 1
                    self._log.warning(f"Skipping malformed persisted retry envelope: {summarize_exception_chain(e)}")
                    continue

                if "fire_at_date" not in envelope:
                    try:
                        await self._retry_store.update(retry_key, {"fire_at_date": fire_at_date})
                    except DocumentNotFoundError:
                        continue

                if retry_key in self._republishing_keys:
                    continue
                generation = envelope.get("generation")
                if fire_at <= now:
                    existing = self._pending_timers.pop(retry_key, None)
                    if existing is not None:
                        existing.cancel()
                    self._spawn_republish(retry_key, generation)
                    immediate_count += 1
                else:
                    remaining = fire_at - now
                    self._schedule_republish(retry_key, remaining, generation)
                    scheduled_count += 1

        self._log.info(
            f"Scheduled {scheduled_count} pending retries, "
            f"{immediate_count} republished immediately"
            + (f", {skipped_count} malformed skipped" if skipped_count else "")
        )

    async def start(self) -> None:
        """Start lease polling and recover persisted retries."""
        if self._lease is None:
            await self.schedule_pending()
        elif self._lease_task is None:
            await self._refresh_lease()
            self._lease_task = asyncio.create_task(self._maintain_lease())

    async def _maintain_lease(self) -> None:
        while True:
            await self._sleep(self._lease_ttl / 3)
            await self._refresh_lease()

    async def _refresh_lease(self) -> None:
        if not self._can_publish():
            self._lose_lease()
        now = self._clock()
        expires_at = int((now + self._lease_ttl) * 1000) / 1000
        try:
            acquired = await self._lease.acquire_or_renew(self._holder_id, now, expires_at)
            if not acquired or self._clock() >= expires_at:
                self._lose_lease()
                return
            self._lease_expires_at = expires_at
            if self._lease_timer is not None:
                self._lease_timer.cancel()
            self._lease_timer = asyncio.get_running_loop().call_later(expires_at - self._clock(), self._lose_lease)
            await self.schedule_pending()
        except Exception as exc:
            self._lose_lease()
            self._log.warning(f"Retry lease refresh failed: {summarize_exception_chain(exc)}")

    def _can_publish(self) -> bool:
        return self._lease is None or self._clock() < self._lease_expires_at

    def _lose_lease(self) -> None:
        self._lease_expires_at = 0.0
        if self._lease_timer is not None:
            self._lease_timer.cancel()
            self._lease_timer = None
        for handle in self._pending_timers.values():
            handle.cancel()
        self._pending_timers.clear()
        for task in self._republish_tasks:
            task.cancel()

    async def shutdown(self) -> None:
        """Stop lease polling and cancel retries, leaving persisted work intact."""
        if self._lease_task is not None:
            self._lease_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._lease_task
            self._lease_task = None
        self._lose_lease()
        await asyncio.gather(*self._republish_tasks, return_exceptions=True)

    def _schedule_republish(
        self,
        retry_key: str,
        delay_seconds: float,
        expected_generation: str | int | None = None,
    ) -> None:
        """Schedule a republish callback via ``asyncio.call_later``.

        Cancels any existing timer for the same retry_key before
        scheduling.

        :param retry_key: Composite key identifying the retry envelope.
        :param delay_seconds: Seconds until republish.
        :param expected_generation: The envelope generation at
            scheduling time. Passed through to ``_republish`` so it
            can detect stale timers superseded by a newer envelope.
            ``None`` for legacy envelopes without a generation.
        """
        if not self._can_publish():
            return
        existing = self._pending_timers.pop(retry_key, None)
        if existing is not None:
            existing.cancel()

        loop = asyncio.get_running_loop()
        handle = loop.call_later(
            delay_seconds,
            self._on_timer_fire,
            retry_key,
            expected_generation,
        )
        self._pending_timers[retry_key] = handle

    def _on_timer_fire(
        self,
        retry_key: str,
        expected_generation: str | int | None,
    ) -> None:
        """Synchronous callback for ``call_later`` -- creates async
        republish task.
        """
        self._pending_timers.pop(retry_key, None)
        self._spawn_republish(retry_key, expected_generation)

    def _spawn_republish(
        self,
        retry_key: str,
        expected_generation: str | int | None = None,
    ) -> None:
        """Create a tracked ``_republish`` task.

        Keeps a strong reference in ``_republish_tasks`` until the task
        completes so the event loop cannot collect it mid-flight.
        """
        if not self._can_publish() or retry_key in self._republishing_keys:
            return
        self._republishing_keys.add(retry_key)
        task = asyncio.create_task(self._republish(retry_key, expected_generation))
        self._republish_tasks.add(task)

        def done(task: asyncio.Task[None]) -> None:
            self._republish_tasks.discard(task)
            self._republishing_keys.discard(retry_key)
            if not task.cancelled() and (exc := task.exception()) is not None:
                self._log.error(f"Retry failed: {summarize_exception_chain(exc)}")

        task.add_done_callback(done)

    async def _republish(
        self,
        retry_key: str,
        expected_generation: str | int | None = None,
    ) -> None:
        """Fetch message from store, republish, and delete from store.

        Holds ``_retry_lock`` to prevent interleaving with ``__call__``
        for the same key. On publish failure, the message remains in
        store for recovery on next ``schedule_pending`` call.

        :param retry_key: Composite key identifying the retry envelope.
        :param expected_generation: The envelope generation this timer
            was scheduled for. If the envelope's generation differs,
            a newer envelope has superseded it and this timer skips.
            ``None`` disables the staleness check for legacy envelopes.
        """
        async with self._retry_lock:
            if not self._can_publish():
                return
            try:
                if self._lease is None:
                    envelope = await self._retry_store.get_document(retry_key)
                else:
                    # A standby may have replaced the persisted row before its cache write.
                    envelope = await anext(
                        self._retry_store.query({self.REQUIRED_DOC_ID_FIELD: retry_key}),
                        None,
                    )
                    if envelope is None:
                        return
            except DocumentNotFoundError:
                self._log.debug(
                    f"Retry envelope not found in store (key={retry_key}), already republished by another timer"
                )
                return

            if expected_generation is not None and envelope.get("generation") != expected_generation:
                self._log.debug(
                    f"Stale timer for key={retry_key} "
                    f"(expected generation={expected_generation}, "
                    f"envelope generation={envelope.get('generation')}), "
                    f"skipping superseded retry"
                )
                return

            if not self._can_publish():
                return
            if not await self._retry_store.update_if(
                retry_key, {"republishing": True}, expected={"generation": envelope.get("generation")}
            ):
                return
            if not self._can_publish():
                return
            message = envelope["message"]
            identity = extract_message_identity(message)
            retry_info = message.get("retry", {})

            try:
                await self._republish_publisher(message)
            except PublishFailureException as e:
                await self._retry_store.update_if(
                    retry_key, {"republishing": False}, expected={"generation": envelope.get("generation")}
                )
                self._log.error(
                    f"[{identity}] Failed to republish, "
                    f"will recover on the next pending scan: "
                    f"{summarize_exception_chain(e)}"
                )
                return

            if self._can_publish():
                await self._retry_store.delete(retry_key, expected={"generation": envelope.get("generation")})

            self._log.info(f"[{identity}] Republished retry message (attempt {retry_info.get('count', '?')})")

    def _default_message_key(self, message: dict) -> str:
        """Build message key from standard identity fields."""
        job_id = message.get("job_id")
        doc_id = message.get("data", {}).get("doc_id")
        part_idx = message.get("data", {}).get("part_idx", 0)
        return build_message_key(job_id, doc_id, part_idx)
