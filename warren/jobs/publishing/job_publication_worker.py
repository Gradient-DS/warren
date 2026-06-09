"""
Framework worker that consumes job messages and publishes their documents.

Listens on the fanout exchange for ``data_type == "job"`` messages,
extracts document sources from the message data, and delegates to a
``JobDocumentsPublisher`` for the load -> register -> publish flow.
"""

from collections.abc import AsyncIterable, Callable, Iterable

from warren.common import HardFailureException
from warren.jobs.publishing.job_documents_publisher import (
    JobDocumentsPublisher,
)
from warren.workers.workers import (
    FilteringWorkerBase,
)


_MAX_DATA_REPR_LENGTH: int = 500


async def _items_as_async_iterable(items: Iterable) -> AsyncIterable:
    """Wrap an iterable as an async iterable."""
    for item in items:
        yield item


def _default_source_generator(data: dict) -> AsyncIterable:
    """Validate and return items from the standard job message format.

    Expects ``data["items"]`` to be an iterable of document source
    dicts. Raises ``ValueError`` if the field is missing or not
    iterable — caught by the caller and wrapped as a hard failure.

    Validation runs eagerly at call time (sync function), while
    iteration is lazy (async generator).
    """
    items = data.get("items")
    if items is None:
        msg = "Job message data has no 'items' field"
        raise ValueError(msg)
    if not hasattr(items, "__iter__"):
        msg = f"Job message data 'items' is not iterable: {type(items).__name__}"
        raise ValueError(msg)
    return _items_as_async_iterable(items)


class JobPublicationWorker(FilteringWorkerBase):
    """Consumes job messages and publishes their documents.

    Filters for ``data_type == "job"`` on the fanout exchange. For each
    matching message, calls ``create_source_generator`` to extract an
    async iterable of document sources from the message data, then
    delegates to ``JobDocumentsPublisher.publish_job()`` for the actual
    load -> register -> publish orchestration.

    :param worker_name: Unique worker instance name.
    :param documents_publisher: Publisher harness for the
        load -> register -> publish flow.
    :param create_source_generator: Callable that receives the message
        ``data`` dict and returns an ``AsyncIterable`` of document
        sources. When ``None``, defaults to iterating
        ``data["items"]`` — the standard job message format where
        data contains a list of document source items.
    :param worker_type: Optional override for the worker type
        (default: ``"job_publication_worker"``).
    """

    def __init__(
        self,
        worker_name: str,
        *,
        documents_publisher: JobDocumentsPublisher,
        create_source_generator: Callable[[dict], AsyncIterable] | None = None,
        worker_type: str | None = None,
    ) -> None:
        super().__init__(worker_name, worker_type=worker_type)
        self._documents_publisher = documents_publisher
        self._create_source_generator = (
            create_source_generator or _default_source_generator
        )

    def should_process(self, message: dict) -> bool:
        """Only process job messages whose preprocessing is complete.

        Like the per-document workers, this consumer accepts a message
        only when its ``preprocessing_required`` list is empty. Job-level
        preprocessing steps (e.g. ``vectordb_provisioning``) are advertised
        by Pipeline API on the publication-request and removed by the
        relevant lifecycle worker before it is re-emitted, gating
        document fan-out until the preprocessing step is done.
        """
        return message.get("data_type") == "job" and not message.get(
            "preprocessing_required"
        )

    async def process(self, message: dict) -> dict | None:
        """Extract sources from the job message and publish them.

        :param message: Job message with ``job_id``, ``data``, and an
            optional ``job_parameters`` dict (forwarded to the publisher
            so per-document messages can carry job-level settings).
        :return: None -- publishing produces individual document
            messages via the publisher's own ``PublisherInterface``.
        """
        job_id: str = message["job_id"]

        data: dict | None = message.get("data")
        if data is None:
            msg = f"Job {job_id}: message has no valid 'data' field"
            raise HardFailureException(msg)

        try:
            sources = self._create_source_generator(data)
        except Exception as e:
            data_repr = str(data)[:_MAX_DATA_REPR_LENGTH]
            msg = (
                f"Job {job_id}: failed to create source generator. "
                f"Data: {data_repr} ..."
            )
            raise HardFailureException(msg) from e

        job_parameters: dict = message.get("job_parameters") or {}

        result = await self._documents_publisher.publish_job(
            job_id=job_id,
            sources=sources,
            job_parameters=job_parameters,
        )

        self._log.info(
            "Job %s: published=%d, failed=%d, total=%d",
            job_id,
            result["published"],
            result["failed"],
            result["total"],
        )

        return None
