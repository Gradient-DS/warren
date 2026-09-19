"""Run a whole pipeline inside one process on ``backend: memory``.

Normally each warren worker is its own process and they meet at the broker
and the database. Here they share one event loop, one ``RuntimeInfra``
(which carries the in-process broker) and one ``MemoryStoreRegistry``.

Deliberately not re-exported from ``warren.runtime``: this module imports
the status and retry runners, which import ``warren.runtime.infrastructure``,
so re-exporting it from the package ``__init__`` would create an import
cycle. Import it as ``warren.runtime.in_process``.
"""

import asyncio
import signal
from collections.abc import Awaitable, Callable, Sequence

from warren.jobs.status.job_status_worker_runner import JobStatusWorkerRunner
from warren.pubsub.routing import observer_exchange
from warren.retry_management.retry_worker import RetryWorker
from warren.retry_management.retry_worker_runner import RetryWorkerRunner
from warren.runtime.config import RuntimeConfig
from warren.runtime.infrastructure import RuntimeInfra
from warren.runtime.runner import DefaultWorkerRunner, create_default_resolvers
from warren.runtime.spec import PipelineSpec
from warren.storage.documents.fetcher import CachedDocumentFetcher
from warren.storage.memory_registry import MemoryStoreRegistry
from warren.workers.runners import WorkerRunnerBase


async def create_in_process_runners(
    config: RuntimeConfig,
    pipeline: PipelineSpec,
    *,
    infra: RuntimeInfra,
    stores: MemoryStoreRegistry,
) -> list[WorkerRunnerBase]:
    """Build one runner per worker type, plus the status and retry runners.

    The one place that knows which registry store goes into which runner
    parameter. Every runner shares ``infra`` (and with it the broker) and
    draws its stores from ``stores``, so a stage reads what the previous
    stage wrote. The retry runner is included when ``config.retry.enabled``.

    :param config: Runtime configuration with ``backend: memory``.
    :param pipeline: The pipeline to run.
    :param infra: Shared infrastructure. The caller creates and closes it.
    :param stores: Shared store registry.
    :return: Runners, not yet set up.
    """
    runners: list[WorkerRunnerBase] = []

    for worker_type, spec in pipeline.workers.items():
        results_stores = {
            role: await stores.results_store(collection)
            for role, collection in spec.collections.items()
        }
        document_store = (
            stores.document_store(
                "documents",
                doc_id_field="doc_id",
                unique_indexes=[("doc_id",)],
            )
            if spec.needs_document_store
            else None
        )
        document_fetcher = (
            CachedDocumentFetcher(
                cache=stores.cache("documents"),
                resolvers=create_default_resolvers(),
            )
            if spec.needs_document_fetcher
            else None
        )
        runners.append(
            DefaultWorkerRunner(
                config,
                f"{worker_type}-0",
                worker_type=worker_type,
                worker_spec=spec,
                exchange=pipeline.exchange,
                document_fetcher=document_fetcher,
                document_store=document_store,
                results_stores=results_stores,
                infra=infra,
            )
        )

    # Status and retry workers observe the observer exchange: the data
    # exchange itself for fanout/topic, a derived fanout one for direct.
    observer = observer_exchange(pipeline.exchange)

    runners.append(
        JobStatusWorkerRunner(
            config,
            "job-status-0",
            exchange=observer,
            job_store=stores.job_store(),
            job_results_store=stores.job_results_store(),
            infra=infra,
        )
    )

    if config.retry.enabled:
        runners.append(
            RetryWorkerRunner(
                config,
                "retry-0",
                exchange=observer,
                republish_exchange=pipeline.exchange,
                retry_store=stores.document_store(
                    config.retry.collection_name,
                    doc_id_field=RetryWorker.REQUIRED_DOC_ID_FIELD,
                ),
                infra=infra,
            )
        )

    return runners


async def run_in_process(
    runners: Sequence[WorkerRunnerBase],
    *,
    until: Callable[[], Awaitable[None]] | None = None,
) -> None:
    """Set up and run several runners in this event loop until one stops.

    Every runner is set up before any of them runs, so every queue is bound
    before anything publishes (the in-process broker, like RabbitMQ, drops
    a message nobody is bound for). The run ends when the first runner
    returns or raises, or when ``until`` returns. Everything is then
    cancelled and torn down, and the first failure is re-raised.

    ``WorkerRunnerBase.run()`` installs SIGINT/SIGTERM handlers per runner,
    so with several runners only the last one's handler survives. That is
    enough: on Ctrl-C that one runner returns, which ends the run for all.
    The handlers in place before the run are restored afterwards.

    :param runners: Runners to co-host. Not yet set up.
    :param until: Called once every runner is consuming. Publish work and
        wait for it here. A callable, not a coroutine object, so nothing is
        left un-awaited if setup fails.
    """
    previous_handlers = {
        sig: signal.getsignal(sig) for sig in (signal.SIGINT, signal.SIGTERM)
    }
    tasks: list[asyncio.Task] = []
    try:
        for runner in runners:
            await runner.setup()

        tasks = [asyncio.create_task(runner.run()) for runner in runners]
        if until is not None:
            tasks.append(asyncio.create_task(until()))

        done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        for runner in reversed(runners):
            await runner.teardown()
        for sig, handler in previous_handlers.items():
            signal.signal(sig, handler)

    for task in done:
        if not task.cancelled() and task.exception() is not None:
            raise task.exception()
