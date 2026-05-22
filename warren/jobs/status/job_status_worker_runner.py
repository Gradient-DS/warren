"""
Runner for the job status worker.

Manages the JobStatusWorker lifecycle: creates the worker from injected
dependencies and sets up the consumer. Transport-agnostic — the caller
provides pre-built stores and a factory for the consumer manager.
"""

from document_processing.distributed.warren.storage.jobs.interface import (
    JobStoreInterface,
)
from document_processing.distributed.warren.storage.job_results.interface import (
    JobResultsStoreInterface,
)
from document_processing.distributed.warren.jobs.status.job_status_worker import (
    JobStatusWorker,
)
from document_processing.distributed.warren.workers.runners import (
    ConsumerManagerFactory,
    WorkerRunnerBase,
)


class JobStatusWorkerRunner(WorkerRunnerBase):
    """Runs a JobStatusWorker with its lifecycle hooks.

    :param worker_name: Unique identifier for this worker instance.
    :param job_store: Store for job definitions and completion status.
    :param job_results_store: Store for per-document processing results.
    :param consumer_manager_factory: Factory that creates a consumer
        manager. The worker needs a publisher configured (for the
        ``"job-completed"`` signal).
    """

    def __init__(
        self,
        worker_name: str,
        *,
        job_store: JobStoreInterface,
        job_results_store: JobResultsStoreInterface,
        consumer_manager_factory: ConsumerManagerFactory,
    ) -> None:
        super().__init__(name=worker_name)
        self._worker_name = worker_name
        self._job_store = job_store
        self._job_results_store = job_results_store
        self._consumer_manager_factory = consumer_manager_factory

    async def setup(self) -> None:
        """Create job status worker and consumer manager.

        1. Create the JobStatusWorker with injected stores
        2. Create and set up the consumer manager via factory
        """
        worker = JobStatusWorker(
            worker_name=self._worker_name,
            job_store=self._job_store,
            job_results_store=self._job_results_store,
        )

        self._consumer_manager = self._consumer_manager_factory(worker)
        await self._consumer_manager.setup()

        self._mark_setup_succeeded()
