from typing import TYPE_CHECKING

import asyncio
import inspect
import json
import random

from aiokafka import ConsumerRecord, TopicPartition
from basics.logging_utils import summarize_exception_chain

from warren.common import (
    HardFailureException,
    MessageConsumerInterface,
    SoftFailureException,
)
from warren.pubsub.base import ConsumerManagerBase
from warren.pubsub.common import (
    PublisherInterface,
    PublishFailureException,
    PubSubSetupError,
    RetryConfig,
)
from warren.pubsub.kafka.aiokafka.connection import (
    KafkaConnectionManager,
)
from warren.pubsub.kafka.aiokafka.topology import (
    ensure_topic,
)
from warren.pubsub.kafka.config import (
    KafkaConsumerManagerConfig,
)
from warren.workers.messages import (
    ExtractMessageIdentityFunc,
    extract_message_identity,
)


if TYPE_CHECKING:
    from aiokafka import AIOKafkaConsumer


class KafkaConsumerManager(ConsumerManagerBase):
    """
    Kafka consumer manager that bridges message queue operations with application workers.

    This class handles all Kafka-specific concerns (consumer lifecycle, offset
    management, retry logic) while delegating business logic to a worker function
    that remains agnostic of the underlying message transport.

    The consumer receives plain dictionaries and returns optional
    response dictionaries—it never interacts with Kafka directly.

    Offset semantics — the Kafka mapping of RabbitMQ's ack/nack/reject:

    - commit after processing       ≙ ack / reject(requeue=False)
    - seek back without committing  ≙ nack(requeue=True)

    Messages are processed sequentially per consumer (one in flight at a
    time) — the faithful mapping of ``prefetch_count: 1`` — which keeps
    offset management trivially correct.
    """

    def __init__(
        self,
        config: KafkaConsumerManagerConfig,
        connection_manager: KafkaConnectionManager,
        consumer: MessageConsumerInterface,
        *,
        publishers: list[PublisherInterface] | None = None,
        retry_config: RetryConfig | None = None,
        extract_identity_func: ExtractMessageIdentityFunc | None = None,
        publish_hard_failures: bool = True,
    ) -> None:
        super().__init__(
            consumer,
            publishers=publishers,
        )

        self._config = config
        self._connection_manager: KafkaConnectionManager = connection_manager
        self._retry_config = retry_config or RetryConfig()
        self._extract_identity = extract_identity_func or extract_message_identity
        self._publish_hard_failures = publish_hard_failures

        self._kafka_consumer: AIOKafkaConsumer | None = None
        self._poll_task: asyncio.Task | None = None

        # Track the in-flight message task for graceful shutdown.
        # Processing is sequential, so there is at most one.
        self._in_flight_task: asyncio.Task | None = None
        self._shutting_down: bool = False

    async def setup(self) -> None:
        """
        Initialize Kafka resources for consumption.

        - Sets up all publishers.
        - Ensures the configured topic exists.
        - Creates and starts the Kafka consumer with manual offset
          commits (the ack/nack seam).
        - Must be called before start_consuming().
        """
        # Publishers self-contextualise their own setup failures (topic
        # identity), so the loop is left unwrapped.
        for publisher in self._publishers:
            await publisher.setup()

        # ensure_topic self-contextualises (topic identity), so it is
        # left unwrapped.
        await ensure_topic(self._connection_manager, self._config.topic)

        # Default group: one consumer group per worker type on the topic,
        # so every worker type sees every message (fanout parity) while
        # instances of the same type share the partitions. Deployments
        # with platform-assigned group names set group_id explicitly.
        group_id = (
            self._config.consumer.group_id
            or f"{self._config.topic.name}.{self._consumer.type}"
        )

        try:
            kafka_consumer = self._connection_manager.create_consumer(
                self._config.topic.name,
                group_id=group_id,
                enable_auto_commit=False,
                auto_offset_reset=self._config.consumer.auto_offset_reset,
                max_poll_interval_ms=self._config.consumer.max_poll_interval_ms,
                session_timeout_ms=self._config.consumer.session_timeout_ms,
                isolation_level="read_committed",
            )
            # Published onto the instance the moment it exists, so a
            # partial setup leaves truthful state and stop_consuming()
            # can stop it.
            self._kafka_consumer = kafka_consumer
            await kafka_consumer.start()
        except Exception as e:
            msg = (
                f"Failed to start Kafka consumer for topic "
                f"'{self._config.topic.name}' (group_id='{group_id}')"
            )
            raise PubSubSetupError(msg) from e

    async def start_consuming(self) -> None:
        """
        Begin consuming messages from the configured topic.

        Launches the poll loop: messages are fetched one at a time and
        processed sequentially (≙ prefetch_count 1), with the offset
        committed only after processing resolves.
        """
        if self._kafka_consumer is None:
            msg = "Must call setup() before start_consuming()"
            raise RuntimeError(msg)

        self._poll_task = asyncio.create_task(self._poll_loop())
        self._poll_task.add_done_callback(self._on_poll_loop_done)

    async def stop_consuming(self) -> None:
        """
        Gracefully stop message consumption and close Kafka resources.

        1. Signals shutdown (stops accepting new work)
        2. Cancels the poll loop (no new messages fetched)
        3. Waits for the in-flight message to complete (with timeout);
           when it completes it commits per the normal decision matrix
        4. Stops the Kafka consumer and tears down publishers

        Any offset left uncommitted is redelivered to the group after the
        rebalance — at-least-once, matching nack-on-shutdown on RabbitMQ.
        """
        self._shutting_down = True

        if self._poll_task is not None and not self._poll_task.done():
            self._poll_task.cancel()
            try:
                await self._poll_task
            except asyncio.CancelledError:
                pass
            except Exception as e:
                self._log.warning(
                    f"Error stopping poll loop: {summarize_exception_chain(e)}"
                )

        if self._in_flight_task is not None and not self._in_flight_task.done():
            try:
                await asyncio.wait_for(
                    self._in_flight_task,
                    timeout=self._config.consumer.on_shutdown_timeout,
                )
            except TimeoutError:
                self._log.warning(
                    f"Timed out waiting for in-flight message "
                    f"during shutdown "
                    f"(timeout={self._config.consumer.on_shutdown_timeout}s)"
                )
            except Exception as e:
                self._log.warning(
                    f"Error in in-flight message during shutdown: "
                    f"{summarize_exception_chain(e)}"
                )

        # Stop the Kafka consumer (ours to manage). Uncommitted offsets
        # stay with the group and redeliver after the rebalance.
        if self._kafka_consumer is not None:
            try:
                await self._kafka_consumer.stop()
            except Exception as e:
                self._log.warning(
                    f"Error stopping Kafka consumer: {summarize_exception_chain(e)}"
                )

        # Tear down all publishers — best-effort so one failure cannot
        # block the rest of shutdown.
        for publisher in self._publishers:
            try:
                await publisher.teardown()
            except Exception as e:
                self._log.warning(
                    f"Error tearing down publisher during shutdown: "
                    f"{summarize_exception_chain(e)}"
                )

        # Consumer stopped.
        # Admin client is owned by KafkaConnectionManager — not our responsibility to close.

    async def _poll_loop(self) -> None:
        """Sequential poll loop: fetch one message, process it, repeat.

        Processing runs in its own, shielded task so that cancelling the
        poll loop (shutdown) does not cancel the in-flight message —
        stop_consuming() drains it separately.
        """
        assert self._kafka_consumer is not None

        while not self._shutting_down:
            message = await self._kafka_consumer.getone()

            task = asyncio.create_task(self._process_message(message))
            self._in_flight_task = task
            try:
                # shield: cancelling the poll task must not cancel the
                # in-flight message (awaiting a task propagates
                # cancellation into it).
                await asyncio.shield(task)
            except Exception as e:
                # _process_message resolves worker failures itself; this
                # catches only transport errors (commit/seek). Logged so
                # the loop can move on — the uncommitted offset
                # redelivers after a rebalance.
                self._log.error(
                    f"Error finalising message offsets: {summarize_exception_chain(e)}"
                )

    def _on_poll_loop_done(self, task: asyncio.Task) -> None:
        """Log unexpected poll-loop termination — never let it die silently."""
        if task.cancelled():
            return
        exc = task.exception()
        if exc is not None:
            self._log.error(
                f"Poll loop terminated unexpectedly: {summarize_exception_chain(exc)}"
            )

    async def _commit(self, message: ConsumerRecord) -> None:
        """Commit past the message — ≙ ack / reject(requeue=False).

        The message will not be redelivered to the group.
        """
        assert self._kafka_consumer is not None
        tp = TopicPartition(message.topic, message.partition)
        await self._kafka_consumer.commit({tp: message.offset + 1})

    def _seek_back(self, message: ConsumerRecord) -> None:
        """Rewind to the message without committing — ≙ nack(requeue=True).

        The next fetch on the partition redelivers the message.
        """
        assert self._kafka_consumer is not None
        tp = TopicPartition(message.topic, message.partition)
        self._kafka_consumer.seek(tp, message.offset)

    async def _process_message(self, message: ConsumerRecord) -> None:
        """Process a single message: deserialize, execute worker, handle commit/seek."""
        # Deserialize. TypeError covers tombstone records (value=None).
        try:
            body = json.loads(message.value)
        except (json.JSONDecodeError, TypeError) as e:
            self._log.error(
                f"Failed to deserialize message: {summarize_exception_chain(e)}"
            )
            await self._commit(message)  # ≙ reject(requeue=False)
            return

        # Process — dispatch sync consumers to thread pool, await async directly.
        # iscoroutinefunction checks both plain async functions and callable
        # objects with async __call__ (the latter requires checking __call__).
        try:
            is_async = inspect.iscoroutinefunction(
                self._consumer
            ) or inspect.iscoroutinefunction(getattr(self._consumer, "__call__", None))
            if is_async:
                result = await self._consumer(body)
            else:
                loop = asyncio.get_event_loop()
                result = await loop.run_in_executor(
                    None,
                    self._consumer,
                    body,
                )

            # Publish downstream if publishers configured and result returned
            if result is not None and self._publishers:
                await self._handle_publish(result)

            await self._commit(message)  # ≙ ack

        except SoftFailureException as e:
            await self._handle_soft_failure(message, body, e)

        except PublishFailureException as e:
            await self._handle_publish_failure(message, body, e)

        except HardFailureException as e:
            await self._handle_hard_failure(message, body, e)

        except Exception as e:
            await self._handle_hard_failure(message, body, e)

    async def _handle_publish(self, result: dict) -> None:
        """Publish a result to all configured publishers."""
        for publisher in self._publishers:
            await publisher(result)

    async def _handle_soft_failure(
        self,
        message: ConsumerRecord,
        body: dict,
        error: SoftFailureException,
    ) -> None:
        """Handle a soft (retryable) failure.

        Wraps the failed message in a ``data_type: "soft-failure"``
        envelope and publishes it through the regular downstream
        publishers. A RetryWorker (or JobManagerWorker) on the same
        topic picks it up.

        If no publishers are configured, falls back to seek-back
        (redelivery) with a delay.
        """
        identity = self._extract_identity(body)

        if not self._publishers:
            delay = self._retry_config.fallback_requeue_delay
            self._log.warning(
                f"[{identity}] No publishers for retry, "
                f"redelivered after {delay}s: "
                f"{summarize_exception_chain(error)}"
            )
            await asyncio.sleep(delay)
            self._seek_back(message)  # ≙ nack(requeue=True)
            return

        # Read existing retry state from message (may be a re-retry)
        existing_retry = body.get("retry", {})
        current_count = existing_retry.get("count", 0)

        # Workers can request delayed redelivery without consuming a
        # retry slot — used when SoftFailureException is the redelivery
        # primitive for non-failure conditions (e.g. polling a
        # long-running backup). When `retry_count_consumed=False`, the
        # counter and max-retry guard are both skipped.
        if error.retry_count_consumed:
            new_count = current_count + 1
        else:
            new_count = current_count

        # Resolve retry parameters: worker intent -> config defaults -> caps
        retry_after = self._resolve_retry_after(error, new_count)
        retry_max = self._resolve_retry_max(error)

        # Check if max retries exceeded (skip when redelivery does not
        # consume a slot — the worker controls termination itself).
        if error.retry_count_consumed and new_count > retry_max:
            self._log.error(
                f"[{identity}] Max retries ({retry_max}) exceeded, "
                f"treating as hard failure."
            )
            await self._handle_hard_failure(message, body, error)
            return

        if error.retry_count_consumed:
            self._log.warning(
                f"[{identity}] Soft failure (attempt {new_count}/{retry_max}, "
                f"delay {retry_after}s): {summarize_exception_chain(error)}"
            )
        else:
            self._log.info(
                f"[{identity}] Delayed redelivery in {retry_after}s "
                f"(no retry slot consumed): {error.reason}"
            )

        # Embed retry metadata in the failed message
        body["retry"] = {
            **existing_retry,
            "count": new_count,
            "after": retry_after,
            "reason": error.reason,
            "max": retry_max,
        }

        soft_failure_msg: dict = {
            "data_type": "soft-failure",
            "data": body,
            "job_id": body.get("job_id"),
            "origin": {
                "type": self._consumer.type,
                "name": self._consumer.name,
            },
        }

        try:
            await self._handle_publish(soft_failure_msg)
            await self._commit(message)  # ≙ ack
        except PublishFailureException as e:
            self._log.warning(
                f"[{identity}] Failed to publish soft-failure message, "
                f"seeking back for redelivery: {summarize_exception_chain(e)}"
            )
            self._seek_back(message)  # ≙ nack(requeue=True)

    async def _handle_publish_failure(
        self,
        message: ConsumerRecord,
        body: dict,
        error: PublishFailureException,
    ) -> None:
        """Handle a downstream publish failure.

        Cannot route through the same broken publishers, so falls
        back to seek-back (redelivery) with a delay.
        """
        identity = self._extract_identity(body)
        delay = self._retry_config.fallback_requeue_delay
        self._log.warning(
            f"[{identity}] Publish failed, redelivered after {delay}s: "
            f"{summarize_exception_chain(error)}"
        )
        await asyncio.sleep(delay)
        self._seek_back(message)  # ≙ nack(requeue=True)

    async def _handle_hard_failure(
        self,
        message: ConsumerRecord,
        body: dict,
        error: Exception,
    ) -> None:
        """Handle a hard (permanent) failure.

        Publishes a ``data_type: "hard-failure"`` envelope to make the
        failure visible on the topic (for the JobStatusWorker), then
        commits past the original message. The envelope is best-effort —
        if publishing fails, the offset is still committed.
        """
        identity = self._extract_identity(body)
        self._log.error(
            f"[{identity}] Hard failure (message dropped): "
            f"{summarize_exception_chain(error)}"
        )

        if self._publish_hard_failures and self._publishers:
            hard_failure_msg: dict = {
                "data_type": "hard-failure",
                "data": body,
                "job_id": body.get("job_id"),
                "origin": {
                    "type": self._consumer.type,
                    "name": self._consumer.name,
                },
                "error": summarize_exception_chain(error),
            }
            try:
                await self._handle_publish(hard_failure_msg)
            except Exception as pub_error:
                self._log.warning(
                    f"[{identity}] Failed to publish hard-failure envelope: "
                    f"{summarize_exception_chain(pub_error)}"
                )

        await self._commit(message)  # ≙ reject(requeue=False)

    def _resolve_retry_after(self, error: SoftFailureException, attempt: int) -> int:
        """Calculate delay for this retry attempt with exponential backoff and optional jitter."""
        base = error.retry_after or self._retry_config.initial_delay
        exp_base = error.backoff_base or self._retry_config.backoff_base
        use_jitter = (
            error.jitter if error.jitter is not None else self._retry_config.jitter
        )

        delay = base * (exp_base ** (attempt - 1))

        if use_jitter:
            delay *= random.uniform(0.5, 1.5)

        return min(int(delay), self._retry_config.max_delay_cap)

    def _resolve_retry_max(self, error: SoftFailureException) -> int:
        """Resolve max retries: worker intent capped by system limit."""
        requested = error.retry_max or self._retry_config.max_retries
        return min(requested, self._retry_config.max_retries_cap)
