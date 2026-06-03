import asyncio
import inspect
import json
import random

from aio_pika.abc import (
    AbstractChannel,
    AbstractExchange,
    AbstractIncomingMessage,
    AbstractQueue,
)
from basics.logging_utils import summarize_exception_chain

from document_processing.distributed.warren.common import (
    HardFailureException,
    MessageConsumerInterface,
    SoftFailureException,
)
from document_processing.distributed.warren.pubsub.base import ConsumerManagerBase
from document_processing.distributed.warren.pubsub.common import (
    PublisherInterface,
    PublishFailureException,
    PubSubSetupError,
)
from document_processing.distributed.warren.pubsub.rabbitmq.config import (
    RetryConfig,
    RMQConsumerManagerConfig,
)
from document_processing.distributed.warren.pubsub.rabbitmq.aio_pika.connection import (
    RMQConnectionManager,
)
from document_processing.distributed.warren.pubsub.rabbitmq.aio_pika.topology import (
    declare_exchange,
    declare_queue,
)
from document_processing.distributed.warren.workers.messages import (
    ExtractMessageIdentityFunc,
    extract_message_identity,
)


class RMQConsumerManager(ConsumerManagerBase):
    """
    RabbitMQ consumer manager that bridges message queue operations with application workers.

    This class handles all RabbitMQ-specific concerns (connection management, message
    acknowledgment, retry logic) while delegating business logic to a worker function
    that remains agnostic of the underlying message transport.

    The consumer receives plain dictionaries and returns optional
    response dictionaries—it never interacts with RabbitMQ directly.
    """

    def __init__(
        self,
        config: RMQConsumerManagerConfig,
        connection_manager: RMQConnectionManager,
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
        self._connection_manager: RMQConnectionManager = connection_manager
        self._retry_config = retry_config or RetryConfig()
        self._extract_identity = extract_identity_func or extract_message_identity
        self._publish_hard_failures = publish_hard_failures

        self._channel: AbstractChannel | None = None
        self._exchange: AbstractExchange | None = None
        self._queue: AbstractQueue | None = None
        self._consumer_tag: str | None = None

        # Track in-flight tasks for graceful shutdown
        self._in_flight_tasks: set[asyncio.Task] = set()
        self._shutting_down: bool = False

    async def setup(self) -> None:
        """
        Initialize RabbitMQ resources for consumption.

        - Sets up all publishers.
        - Creates a channel from the shared connection manager.
        - Declares the necessary exchange and queue, and binds the queue to the exchange.
        - Must be called before start_consuming().
        """
        # Publishers self-contextualise their own setup failures (exchange
        # identity), so the loop is left unwrapped.
        for publisher in self._publishers:
            await publisher.setup()

        # Each resource is published onto the instance the moment it exists,
        # so a partial setup leaves truthful state and teardown can close the
        # channel (the only closeable resource). The locals are passed
        # downstream to keep the type narrowed — instance attributes are not
        # narrowed across awaits.
        try:
            channel = await self._connection_manager.create_channel()
        except Exception as e:
            raise PubSubSetupError("Failed to create consumer channel") from e
        self._channel = channel

        try:
            # TODO: Couple this to the worker's concurrency level?
            await channel.set_qos(
                prefetch_count=self._config.consumer.prefetch_count
            )
        except Exception as e:
            raise PubSubSetupError(
                f"Failed to set consumer QoS "
                f"(prefetch_count={self._config.consumer.prefetch_count})"
            ) from e

        # declare_exchange / declare_queue self-contextualise (exchange/queue
        # identity), so they are left unwrapped.
        exchange = await declare_exchange(channel, self._config.exchange)
        self._exchange = exchange

        queue = await declare_queue(
            channel,
            exchange,
            self._config.queue,
            exchange_type=self._config.exchange.type,
        )
        self._queue = queue

    async def start_consuming(self) -> None:
        """
        Begin consuming messages from the configured queue.

        Messages are delivered to _on_message() as they arrive.
        Each message spawns a task, allowing concurrent processing
        up to prefetch_count limit.
        """
        if self._queue is None:
            raise RuntimeError("Must call setup() before start_consuming()")

        self._consumer_tag = await self._queue.consume(
            self._on_message,
            no_ack=False,
        )

    async def stop_consuming(self) -> None:
        """
        Gracefully stop message consumption and close RabbitMQ resources.

        1. Signals shutdown (stops accepting new work)
        2. Cancels the consumer (no new messages delivered)
        3. Waits for in-flight messages to complete (with timeout)
        4. Closes channel and tears down publishers
        """
        self._shutting_down = True

        if self._consumer_tag and self._queue:
            try:
                await self._queue.cancel(self._consumer_tag)
            except Exception as e:
                self._log.warning(
                    f"Error cancelling queue consumption: "
                    f"{summarize_exception_chain(e)}"
                )

        if self._in_flight_tasks:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*self._in_flight_tasks, return_exceptions=True),
                    timeout=self._config.consumer.on_shutdown_timeout,
                )
            except TimeoutError:
                self._log.warning(
                    f"Timed out waiting for {len(self._in_flight_tasks)} "
                    f"in-flight tasks during shutdown "
                    f"(timeout={self._config.consumer.on_shutdown_timeout}s)"
                )

        # Close consume channel (ours to manage)
        if self._channel and not self._channel.is_closed:
            try:
                await self._channel.close()
            except Exception as e:
                self._log.warning(
                    f"Error closing consumer channel: {summarize_exception_chain(e)}"
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
        # Connection is owned by RMQConnectionManager — not our responsibility to close.

    async def _on_message(self, message: AbstractIncomingMessage) -> None:
        """Handle incoming RabbitMQ message delivery."""
        if self._shutting_down:
            await message.nack(requeue=True)
            return

        task = asyncio.create_task(self._process_message(message))
        self._in_flight_tasks.add(task)
        task.add_done_callback(self._in_flight_tasks.discard)

    async def _process_message(self, message: AbstractIncomingMessage) -> None:
        """Process a single message: deserialize, execute worker, handle ack/nack."""
        # Deserialize
        try:
            body = json.loads(message.body)
        except json.JSONDecodeError as e:
            self._log.error(
                f"Failed to deserialize message: {summarize_exception_chain(e)}"
            )
            await message.reject(requeue=False)
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

            await message.ack()

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
        message: AbstractIncomingMessage,
        body: dict,
        error: SoftFailureException,
    ) -> None:
        """Handle a soft (retryable) failure.

        Wraps the failed message in a ``data_type: "soft-failure"``
        envelope and publishes it through the regular downstream
        publishers. A RetryWorker (or JobManagerWorker) on the same
        exchange picks it up.

        If no publishers are configured, falls back to nack+requeue
        with a delay.
        """
        identity = self._extract_identity(body)

        if not self._publishers:
            delay = self._retry_config.fallback_requeue_delay
            self._log.warning(
                f"[{identity}] No publishers for retry, "
                f"requeued after {delay}s: "
                f"{summarize_exception_chain(error)}"
            )
            await asyncio.sleep(delay)
            await message.nack(requeue=True)
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
            await message.ack()
        except PublishFailureException as e:
            self._log.warning(
                f"[{identity}] Failed to publish soft-failure message, "
                f"nacking with requeue: {summarize_exception_chain(e)}"
            )
            await message.nack(requeue=True)

    async def _handle_publish_failure(
        self,
        message: AbstractIncomingMessage,
        body: dict,
        error: PublishFailureException,
    ) -> None:
        """Handle a downstream publish failure.

        Cannot route through the same broken publishers, so falls
        back to nack+requeue with a delay.
        """
        identity = self._extract_identity(body)
        delay = self._retry_config.fallback_requeue_delay
        self._log.warning(
            f"[{identity}] Publish failed, requeued after {delay}s: "
            f"{summarize_exception_chain(error)}"
        )
        await asyncio.sleep(delay)
        await message.nack(requeue=True)

    async def _handle_hard_failure(
        self,
        message: AbstractIncomingMessage,
        body: dict,
        error: Exception,
    ) -> None:
        """Handle a hard (permanent) failure.

        Publishes a ``data_type: "hard-failure"`` envelope to make the
        failure visible on the exchange (for the JobStatusWorker), then
        rejects the original message. The envelope is best-effort — if
        publishing fails, the message is still rejected.
        """
        identity = self._extract_identity(body)
        self._log.error(
            f"[{identity}] Hard failure (message rejected): "
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

        await message.reject(requeue=False)

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
