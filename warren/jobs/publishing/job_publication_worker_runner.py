"""
Runner for the job publication worker.

Manages the JobPublicationWorker lifecycle: creates the worker from
injected dependencies and sets up the consumer. Transport-agnostic —
the caller provides a pre-built publisher and a factory for the
consumer manager.
"""

from typing import Optional, Dict, Callable, AsyncIterable

from document_processing.distributed.warren.jobs.publishing.job_documents_publisher import (
    JobDocumentsPublisher,
)
from document_processing.distributed.warren.jobs.publishing.job_publication_worker import (
    JobPublicationWorker,
)
from document_processing.distributed.warren.workers.runners import (
    ConsumerManagerFactory,
    WorkerRunnerBase,
)


class JobPublicationWorkerRunner(WorkerRunnerBase):
    """Runs a JobPublicationWorker with its lifecycle hooks.

    :param worker_name: Unique identifier for this worker instance.
    :param documents_publisher: Publisher harness for the
        load -> register -> publish flow.
    :param consumer_manager_factory: Factory that creates a consumer
        manager for the worker.
    :param create_source_generator: Optional callable that receives
        the message ``data`` dict and returns an ``AsyncIterable``
        of document sources. When ``None``, defaults to iterating
        ``data["items"]``.
    """

    def __init__(
        self,
        worker_name: str,
        *,
        documents_publisher: JobDocumentsPublisher,
        consumer_manager_factory: ConsumerManagerFactory,
        create_source_generator: Optional[
            Callable[[Dict], AsyncIterable]
        ] = None,
    ) -> None:
        super().__init__(name=worker_name)
        self._worker_name = worker_name
        self._documents_publisher = documents_publisher
        self._consumer_manager_factory = consumer_manager_factory
        self._create_source_generator = create_source_generator

    async def setup(self) -> None:
        """Create job publication worker and consumer manager.

        1. Create the JobPublicationWorker with injected publisher
        2. Create and set up the consumer manager via factory
        """
        worker = JobPublicationWorker(
            self._worker_name,
            documents_publisher=self._documents_publisher,
            create_source_generator=self._create_source_generator,
        )

        self._consumer_manager = self._consumer_manager_factory(worker)
        await self._consumer_manager.setup()

        self._mark_setup_succeeded()
