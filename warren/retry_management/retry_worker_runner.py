"""
Runner for the retry worker.

Manages the RetryWorker lifecycle: creates the worker from injected
dependencies, sets up the consumer, schedules pending retries on
startup, and cancels timers on shutdown.

Transport-agnostic — the caller provides pre-built store, publisher,
and a factory for the consumer manager.
"""

from typing import Dict, Optional, Callable

from document_processing.distributed.warren.pubsub.common import PublisherInterface
from document_processing.distributed.warren.storage.document_store.interface import (
    DocumentStoreInterface,
)
from document_processing.distributed.warren.retry_management.retry_worker import RetryWorker
from document_processing.distributed.warren.workers.runners import (
    ConsumerManagerFactory,
    WorkerRunnerBase,
)


class RetryWorkerRunner(WorkerRunnerBase):
    """Runs a RetryWorker with its lifecycle hooks.

    Accepts pre-built dependencies. The caller is responsible for
    creating the retry store, publisher, and consumer manager factory
    with appropriate transport configuration.

    :param worker_name: Unique identifier for this retry worker.
    :param retry_store: Persistent store for retry envelopes. Must
        use ``"retry_key"`` as its ``doc_id_field``.
    :param republish_publisher: Publisher targeting the processing
        exchange. The runner calls ``setup()`` and ``teardown()``
        on it.
    :param consumer_manager_factory: Factory that creates a consumer
        manager for the retry worker. Called with the worker as
        argument. The runner calls ``setup()`` on the result.
    :param message_key_func: Optional function to extract a composite
        key from a message dict. Passed to ``RetryWorker``.
    """

    def __init__(
        self,
        worker_name: str,
        *,
        retry_store: DocumentStoreInterface,
        republish_publisher: PublisherInterface,
        consumer_manager_factory: ConsumerManagerFactory,
        message_key_func: Optional[Callable[[Dict], str]] = None,
    ) -> None:
        super().__init__(name=worker_name)
        self._worker_name = worker_name
        self._retry_store = retry_store
        self._republish_publisher = republish_publisher
        self._consumer_manager_factory = consumer_manager_factory
        self._message_key_func = message_key_func
        self._retry_worker: Optional[RetryWorker] = None

    async def setup(self) -> None:
        """Create retry worker, set up publisher and consumer,
        schedule pending retries.

        1. Set up the republish publisher (channel + exchange)
        2. Create the RetryWorker with injected dependencies
        3. Create and set up the consumer manager via factory
        4. Schedule pending retries from the store
        """
        await self._republish_publisher.setup()

        self._retry_worker = RetryWorker(
            worker_name=self._worker_name,
            retry_store=self._retry_store,
            republish_publisher=self._republish_publisher,
            message_key_func=self._message_key_func,
        )

        self._consumer_manager = self._consumer_manager_factory(self._retry_worker)
        await self._consumer_manager.setup()

        await self._retry_worker.schedule_pending()
        self._mark_setup_succeeded()

    async def _on_teardown(self) -> None:
        """Cancel retry timers and tear down the republish
        publisher."""
        if self._retry_worker is not None:
            await self._retry_worker.shutdown()

        await self._republish_publisher.teardown()
