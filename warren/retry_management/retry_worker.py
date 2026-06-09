"""
Retry worker -- persists messages and schedules delayed republishing.

Consumes ``data_type: "soft-failure"`` messages from the processing
exchange, unwraps the failed message, stores it durably, and uses
``asyncio.call_later`` to trigger republishing after the configured
delay. On startup, ``schedule_pending`` loads persisted messages and
reschedules remaining delays.
"""

import asyncio
import time
from collections.abc import Callable

from basics.logging_utils import summarize_exception_chain

from warren.pubsub.common import (
    PublisherInterface,
    PublishFailureException,
)
from warren.storage.document_store.interface import (
    DocumentNotFoundError,
    DocumentStoreInterface,
)
from warren.workers.messages import (
    build_message_key,
    extract_message_identity,
)
from warren.workers.workers import (
    AsyncProcessingWorkerBase,
)


class RetryWorker(AsyncProcessingWorkerBase):
    """Worker that manages retry scheduling for soft-failed messages.

    Consumes messages from the retry queue. For each message:
    1. Persists to store (durability).
    2. Schedules ``asyncio.call_later`` for delayed republishing.
    3. On timer: fetches message, republishes, deletes from store.

    On startup (via ``schedule_pending``), loads scheduling metadata for
    persisted messages and reschedules remaining delays.

    :param worker_name: Worker identifier.
    :param retry_store: Persistent store for messages awaiting retry.
        Must use ``"retry_key"`` as its ``doc_id_field``.
    :param republish_publisher: Publisher targeting the processing
        exchange.
    :param message_key_func: Function to extract a composite key from
        a message dict. Defaults to building key from
        job_id/doc_id/part_idx using ``build_message_key``.

    :raises ValueError: If retry_store's ``doc_id_field`` is not
        ``"retry_key"``.
    """

    REQUIRED_DOC_ID_FIELD: str = "retry_key"

    def __init__(
        self,
        worker_name: str,
        *,
        retry_store: DocumentStoreInterface,
        republish_publisher: PublisherInterface,
        message_key_func: Callable[[dict], str] | None = None,
    ) -> None:
        super().__init__(worker_name)

        doc_id_field = retry_store.get_doc_id_field()
        if doc_id_field != self.REQUIRED_DOC_ID_FIELD:
            msg = (
                f"retry_store doc_id_field must be "
                f"'{self.REQUIRED_DOC_ID_FIELD}', "
                f"got '{doc_id_field}'"
            )
            raise ValueError(msg)

        self._retry_store = retry_store
        self._republish_publisher = republish_publisher
        self._message_key_func = message_key_func or self._default_message_key
        self._pending_timers: dict[str, asyncio.TimerHandle] = {}

        # Strong references to in-flight republish tasks. Without this,
        # asyncio only holds a weak reference to a bare create_task()
        # result, so the task can be garbage-collected mid-flight. Tasks
        # remove themselves on completion via add_done_callback.
        self._republish_tasks: set[asyncio.Task[None]] = set()

        # Monotonic generation counter per retry key. Incremented each
        # time __call__ stores a new envelope for the same key. Passed
        # through call_later → _on_timer_fire → _republish so that a
        # stale timer (whose generation no longer matches the envelope)
        # can detect it was superseded and skip without side effects.
        # Integer comparison is immune to float-precision issues that
        # would affect comparing fire_at timestamps after a JSON/BSON
        # round-trip.
        self._envelope_generation: dict[str, int] = {}

        # Single lock serializing store mutations and republish for all
        # keys. Prevents the TOCTOU race where _republish (fetch →
        # publish → delete) interleaves with __call__ (store → schedule)
        # for the same key at await points, causing the delete to remove
        # a newer envelope. The generation counter above solves a
        # *different* problem: stale timers that fire after a newer
        # envelope was stored but before the stale task acquires the
        # lock.
        #
        # A per-key lock would allow independent keys to proceed
        # concurrently, but adds lifecycle complexity (ref-counted
        # creation/cleanup to avoid stale lock objects). Since retries
        # are the exceptional path and the critical section is short
        # (one store op + one RMQ publish), a single lock is sufficient.
        # Upgrade to per-key if retry volume proves this a bottleneck.
        self._retry_lock: asyncio.Lock = asyncio.Lock()

    async def __call__(self, message: dict) -> dict | None:
        """Unwrap soft-failure envelope, persist, and schedule.

        :param message: Soft-failure wrapper with
            ``data_type: "soft-failure"`` and the failed message in
            ``data``.

        :return: None -- no downstream publish.
        """
        if message.get("data_type") != "soft-failure":
            return None

        failed_message: dict = message["data"]
        retry_info = failed_message.get("retry", {})
        retry_key = self._message_key_func(failed_message)
        delay_seconds = retry_info.get("after", 30)

        generation = self._envelope_generation.get(retry_key, 0) + 1
        self._envelope_generation[retry_key] = generation

        envelope = {
            self.REQUIRED_DOC_ID_FIELD: retry_key,
            "message": failed_message,
            "fire_at": time.time() + delay_seconds,
            "retry_after_seconds": delay_seconds,
            "generation": generation,
        }

        identity = extract_message_identity(failed_message)
        self._log.info(
            f"[{identity}] Scheduling retry in {delay_seconds}s "
            f"(attempt {retry_info.get('count', '?')}/"
            f"{retry_info.get('max', '?')})"
        )

        async with self._retry_lock:
            await self._retry_store.insert(envelope, overwrite_existing=True)
            self._schedule_republish(retry_key, delay_seconds, generation)

        return None

    async def schedule_pending(self) -> None:
        """Load persisted retries and schedule timers.

        Called on startup after setup. Queries the retry store for all
        persisted messages, computes remaining delay, and schedules
        timers. Messages past their fire time are republished
        immediately.
        """
        now = time.time()
        immediate_count = 0
        scheduled_count = 0
        skipped_count = 0

        async for envelope in self._retry_store.query({}):
            try:
                retry_key = envelope[self.REQUIRED_DOC_ID_FIELD]
                fire_at = envelope["fire_at"]
            except KeyError as e:
                # A single malformed persisted envelope must not abort
                # recovery of the rest.
                skipped_count += 1
                self._log.warning(
                    f"Skipping malformed persisted retry envelope: "
                    f"{summarize_exception_chain(e)}"
                )
                continue

            if fire_at <= now:
                self._spawn_republish(retry_key)
                immediate_count += 1
            else:
                remaining = fire_at - now
                self._schedule_republish(retry_key, remaining)
                scheduled_count += 1

        self._log.info(
            f"Scheduled {scheduled_count} pending retries, "
            f"{immediate_count} republished immediately"
            + (f", {skipped_count} malformed skipped" if skipped_count else "")
        )

    async def shutdown(self) -> None:
        """Cancel all pending timers.

        Persisted messages remain in store for recovery on next
        startup.
        """
        count = len(self._pending_timers)

        for handle in self._pending_timers.values():
            handle.cancel()
        self._pending_timers.clear()

        if count > 0:
            self._log.info(f"Cancelled {count} pending retry timers")

    def _schedule_republish(
        self,
        retry_key: str,
        delay_seconds: float,
        expected_generation: int | None = None,
    ) -> None:
        """Schedule a republish callback via ``asyncio.call_later``.

        Cancels any existing timer for the same retry_key before
        scheduling.

        :param retry_key: Composite key identifying the retry envelope.
        :param delay_seconds: Seconds until republish.
        :param expected_generation: The envelope generation at
            scheduling time. Passed through to ``_republish`` so it
            can detect stale timers superseded by a newer envelope.
            ``None`` for startup recovery (skip staleness check).
        """
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
        expected_generation: int | None,
    ) -> None:
        """Synchronous callback for ``call_later`` -- creates async
        republish task."""
        self._pending_timers.pop(retry_key, None)
        self._spawn_republish(retry_key, expected_generation)

    def _spawn_republish(
        self,
        retry_key: str,
        expected_generation: int | None = None,
    ) -> None:
        """Create a tracked ``_republish`` task.

        Keeps a strong reference in ``_republish_tasks`` until the task
        completes so the event loop cannot collect it mid-flight.
        """
        task = asyncio.create_task(self._republish(retry_key, expected_generation))
        self._republish_tasks.add(task)
        task.add_done_callback(self._republish_tasks.discard)

    async def _republish(
        self,
        retry_key: str,
        expected_generation: int | None = None,
    ) -> None:
        """Fetch message from store, republish, and delete from store.

        Holds ``_retry_lock`` to prevent interleaving with ``__call__``
        for the same key. On publish failure, the message remains in
        store for recovery on next ``schedule_pending`` call.

        :param retry_key: Composite key identifying the retry envelope.
        :param expected_generation: The envelope generation this timer
            was scheduled for. If the envelope's generation differs,
            a newer envelope has superseded it and this timer skips.
            ``None`` disables the staleness check (used by startup
            recovery in ``schedule_pending``).
        """
        async with self._retry_lock:
            try:
                envelope = await self._retry_store.get_document(retry_key)
            except DocumentNotFoundError:
                self._log.debug(
                    f"Retry envelope not found in store (key={retry_key}), "
                    f"already republished by another timer"
                )
                return

            if (
                expected_generation is not None
                and envelope.get("generation") != expected_generation
            ):
                self._log.debug(
                    f"Stale timer for key={retry_key} "
                    f"(expected generation={expected_generation}, "
                    f"envelope generation={envelope.get('generation')}), "
                    f"skipping — newer timer will handle it"
                )
                return

            message = envelope["message"]
            identity = extract_message_identity(message)
            retry_info = message.get("retry", {})

            try:
                await self._republish_publisher(message)
            except PublishFailureException as e:
                self._log.error(
                    f"[{identity}] Failed to republish, "
                    f"will recover on next startup: "
                    f"{summarize_exception_chain(e)}"
                )
                return

            await self._retry_store.delete(retry_key)

            self._log.info(
                f"[{identity}] Republished retry message "
                f"(attempt {retry_info.get('count', '?')})"
            )

    def _default_message_key(self, message: dict) -> str:
        """Build message key from standard identity fields."""
        job_id = message.get("job_id")
        doc_id = message.get("data", {}).get("doc_id")
        part_idx = message.get("data", {}).get("part_idx", 0)
        return build_message_key(job_id, doc_id, part_idx)
