import asyncio
import inspect
import json
import random

from basics.logging_utils import summarize_exception_chain

from warren.common import (
    HardFailureException,
    MessageConsumerInterface,
    SoftFailureException,
)
from warren.pubsub.base import ConsumerManagerBase
from warren.pubsub.common import (
    ConsumerHealth,
    PublisherInterface,
    PublishFailureException,
    RetryConfig,
)
from warren.pubsub.memory.broker import Delivery
from warren.pubsub.memory.connection import MemoryConnectionManager
from warren.pubsub.rabbitmq.config import RMQExchangeConfig
from warren.pubsub.routing import REPLAY_ROUTING_KEY_FIELD
from warren.workers.messages import (
    ExtractMessageIdentityFunc,
    extract_message_identity,
)


_DEFAULT_SHUTDOWN_TIMEOUT_SECONDS: float = 30.0


class MemoryConsumerManager(ConsumerManagerBase):
    """Consumer manager for the in-process backend.

    Bridges a ``MemoryBroker`` queue and an application worker. The worker
    receives plain dicts and returns optional dicts, exactly as on the
    RabbitMQ and Kafka backends.

    Settlement semantics, the in-process mapping of ack/nack/reject:

    - ack and reject(requeue=False): the delivery is simply not put back.
    - nack(requeue=True): the delivery goes back on the queue.

    Messages are processed one at a time per manager, the faithful mapping
    of ``prefetch_count: 1``.

    Single-process only. The broker lives in this process's memory, and
    ``RetryWorker`` schedules retries with in-process timers, so nothing
    survives a restart and nothing crosses a process boundary. This is the
    only run mode the memory backend supports.
    """

    def __init__(
        self,
        connection_manager: MemoryConnectionManager,
        consumer: MessageConsumerInterface,
        *,
        exchange: RMQExchangeConfig,
        queue_name: str,
        binding_key: str | None = None,
        data_publisher: PublisherInterface | None = None,
        control_publisher: PublisherInterface | None = None,
        observer_publisher: PublisherInterface | None = None,
        retry_config: RetryConfig | None = None,
        extract_identity_func: ExtractMessageIdentityFunc | None = None,
        publish_hard_failures: bool = True,
        on_shutdown_timeout: float = _DEFAULT_SHUTDOWN_TIMEOUT_SECONDS,
    ) -> None:
        # Same three publishing paths as the other backends (see
        # warren/docs/routing.md): data, control, observer. The base
        # tracks all of them for setup/teardown.
        all_publishers = [
            p
            for p in (data_publisher, control_publisher, observer_publisher)
            if p is not None
        ]
        super().__init__(consumer, publishers=all_publishers)

        self._data_publisher = data_publisher
        self._control_publisher = control_publisher
        self._observer_publisher = observer_publisher

        self._connection_manager = connection_manager
        self._exchange = exchange
        self._queue_name = queue_name
        self._binding_key = binding_key
        self._retry_config = retry_config or RetryConfig()
        self._extract_identity = extract_identity_func or extract_message_identity
        self._publish_hard_failures = publish_hard_failures
        self._on_shutdown_timeout = on_shutdown_timeout

        self._queue: asyncio.Queue[Delivery] | None = None
        self._consume_task: asyncio.Task | None = None
        # Processing is sequential, so there is at most one.
        self._in_flight_task: asyncio.Task | None = None
        self._shutting_down: bool = False

    async def setup(self) -> None:
        """Set up publishers and bind the queue.

        Binding happens here, not in ``start_consuming()``, so that every
        runner in the process is bound before any of them publishes.
        """
        for publisher in self._publishers:
            await publisher.setup()
        self._queue = self._connection_manager.broker.bind(
            self._exchange,
            self._queue_name,
            self._binding_key,
        )

    async def start_consuming(self) -> None:
        if self._queue is None:
            msg = "Must call setup() before start_consuming()"
            raise RuntimeError(msg)

        self._shutting_down = False
        self._consume_task = asyncio.create_task(self._consume_loop())
        self._consume_task.add_done_callback(self._on_consume_loop_done)

    async def stop_consuming(self) -> None:
        """Stop taking messages, drain the in-flight one, tear down publishers."""
        self._shutting_down = True

        if self._consume_task is not None and not self._consume_task.done():
            self._consume_task.cancel()
            try:
                await self._consume_task
            except asyncio.CancelledError:
                pass
            except Exception as e:
                self._log.warning(
                    f"Error stopping consume loop: {summarize_exception_chain(e)}"
                )

        if self._in_flight_task is not None and not self._in_flight_task.done():
            try:
                await asyncio.wait_for(
                    self._in_flight_task,
                    timeout=self._on_shutdown_timeout,
                )
            except TimeoutError:
                self._log.warning(
                    f"Timed out waiting for in-flight message during shutdown "
                    f"(timeout={self._on_shutdown_timeout}s)"
                )
            except Exception as e:
                self._log.warning(
                    f"Error in in-flight message during shutdown: "
                    f"{summarize_exception_chain(e)}"
                )

        # Best-effort so one failure cannot block the rest of shutdown.
        for publisher in self._publishers:
            try:
                await publisher.teardown()
            except Exception as e:
                self._log.warning(
                    f"Error tearing down publisher during shutdown: "
                    f"{summarize_exception_chain(e)}"
                )

    async def health(self, *, probe_timeout: float = 1.0) -> ConsumerHealth:
        """No connection to lose and no blocked state; liveness is the loop."""
        bound = self._queue is not None
        consuming = self._consume_task is not None and not self._consume_task.done()
        return ConsumerHealth(
            connected=bound,
            blocked=False,
            channel_open=bound,
            consumer_registered=consuming if bound else None,
            detail="" if bound and consuming else "consume loop not running",
        )

    async def _consume_loop(self) -> None:
        """Sequential loop: take one delivery, process it, repeat.

        Processing runs in its own shielded task so that cancelling the
        loop (shutdown) does not cancel the in-flight message;
        ``stop_consuming()`` drains it separately.
        """
        assert self._queue is not None

        while not self._shutting_down:
            delivery = await self._queue.get()

            task = asyncio.create_task(self._process_message(delivery))
            self._in_flight_task = task
            # A done-callback (not a finally) so the clear is tied to the
            # task completing, not to this loop being cancelled out of the
            # shield() below while the message is still in flight.
            task.add_done_callback(self._clear_in_flight_task)
            try:
                await asyncio.shield(task)
            except Exception as e:
                # _process_message resolves worker failures itself; anything
                # reaching here is a bug in settlement. Log and move on.
                self._log.error(
                    f"Error settling message: {summarize_exception_chain(e)}"
                )

    def _clear_in_flight_task(self, task: asyncio.Task) -> None:
        if self._in_flight_task is task:
            self._in_flight_task = None

    def _on_consume_loop_done(self, task: asyncio.Task) -> None:
        """Log unexpected loop termination; never let it die silently."""
        if task.cancelled():
            return
        exc = task.exception()
        if exc is not None:
            self._log.error(
                f"Consume loop terminated unexpectedly: {summarize_exception_chain(exc)}"
            )

    def _requeue(self, delivery: Delivery) -> None:
        assert self._queue is not None
        self._queue.put_nowait(delivery)

    async def _process_message(self, delivery: Delivery) -> None:
        """Process one delivery: decode, run the worker, settle."""
        try:
            body = json.loads(delivery.body)
        except (json.JSONDecodeError, TypeError) as e:
            self._log.error(
                f"Failed to deserialize message: {summarize_exception_chain(e)}"
            )
            return  # reject(requeue=False)

        # iscoroutinefunction checks plain async functions; callable objects
        # with an async __call__ need the second check.
        try:
            is_async = inspect.iscoroutinefunction(
                self._consumer
            ) or inspect.iscoroutinefunction(getattr(self._consumer, "__call__", None))
            if is_async:
                result = await self._consumer(body)
            else:
                loop = asyncio.get_running_loop()
                result = await loop.run_in_executor(None, self._consumer, body)

            if result is not None:
                if self._data_publisher is not None:
                    await self._data_publisher(result)
                if self._observer_publisher is not None:
                    await self._observer_publisher(result)
            # ack: nothing to do, the delivery is already off the queue.

        except SoftFailureException as e:
            await self._handle_soft_failure(delivery, body, e)

        except PublishFailureException as e:
            await self._handle_publish_failure(delivery, body, e)

        except HardFailureException as e:
            await self._handle_hard_failure(body, e)

        except Exception as e:
            await self._handle_hard_failure(body, e)

    async def _handle_soft_failure(
        self,
        delivery: Delivery,
        body: dict,
        error: SoftFailureException,
    ) -> None:
        """Wrap the failed message in a ``soft-failure`` envelope and publish it.

        Without a control publisher, fall back to requeue after a delay.
        """
        identity = self._extract_identity(body)

        if self._control_publisher is None:
            delay = self._retry_config.fallback_requeue_delay
            self._log.warning(
                f"[{identity}] No control publisher for retry, "
                f"requeued after {delay}s: {summarize_exception_chain(error)}"
            )
            await asyncio.sleep(delay)
            self._requeue(delivery)
            return

        existing_retry = body.get("retry", {})
        current_count = existing_retry.get("count", 0)

        # A worker can ask for delayed redelivery without spending a retry
        # slot (polling a long-running operation). Then the counter and the
        # max-retry guard are both skipped.
        new_count = current_count + 1 if error.retry_count_consumed else current_count

        retry_after = self._resolve_retry_after(error, new_count)
        retry_max = self._resolve_retry_max(error)

        if error.retry_count_consumed and new_count > retry_max:
            self._log.error(
                f"[{identity}] Max retries ({retry_max}) exceeded, "
                f"treating as hard failure."
            )
            await self._handle_hard_failure(body, error)
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

        body["retry"] = {
            **existing_retry,
            "count": new_count,
            "after": retry_after,
            "reason": error.reason,
            "max": retry_max,
        }
        # The key this delivery arrived with, so ReplayRouter sends the
        # retry back to the worker that failed on topic/direct pipelines.
        body[REPLAY_ROUTING_KEY_FIELD] = delivery.routing_key

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
            await self._control_publisher(soft_failure_msg)
        except PublishFailureException as e:
            self._log.warning(
                f"[{identity}] Failed to publish soft-failure message, "
                f"requeueing: {summarize_exception_chain(e)}"
            )
            self._requeue(delivery)

    async def _handle_publish_failure(
        self,
        delivery: Delivery,
        body: dict,
        error: PublishFailureException,
    ) -> None:
        """A downstream publish failed; requeue after a delay."""
        identity = self._extract_identity(body)
        delay = self._retry_config.fallback_requeue_delay
        self._log.warning(
            f"[{identity}] Publish failed, requeued after {delay}s: "
            f"{summarize_exception_chain(error)}"
        )
        await asyncio.sleep(delay)
        self._requeue(delivery)

    async def _handle_hard_failure(self, body: dict, error: Exception) -> None:
        """Publish a best-effort ``hard-failure`` envelope and drop the message."""
        identity = self._extract_identity(body)
        self._log.error(
            f"[{identity}] Hard failure (message dropped): "
            f"{summarize_exception_chain(error)}"
        )

        if self._publish_hard_failures and self._control_publisher is not None:
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
                await self._control_publisher(hard_failure_msg)
            except Exception as pub_error:
                self._log.warning(
                    f"[{identity}] Failed to publish hard-failure envelope: "
                    f"{summarize_exception_chain(pub_error)}"
                )

    def _resolve_retry_after(self, error: SoftFailureException, attempt: int) -> int:
        """Delay for this attempt: exponential backoff, optional jitter, capped."""
        base = error.retry_after or self._retry_config.initial_delay
        exp_base = error.backoff_base or self._retry_config.backoff_base
        use_jitter = (
            error.jitter if error.jitter is not None else self._retry_config.jitter
        )

        # attempt is 0 for a deferral that consumed no retry slot; clamp so
        # the exponent never goes negative and halves the requested delay.
        delay = base * (exp_base ** max(attempt - 1, 0))

        if use_jitter:
            delay *= random.uniform(0.5, 1.5)

        return min(int(delay), self._retry_config.max_delay_cap)

    def _resolve_retry_max(self, error: SoftFailureException) -> int:
        """Max retries: worker intent capped by the system limit."""
        requested = error.retry_max or self._retry_config.max_retries
        return min(requested, self._retry_config.max_retries_cap)
