"""
Publish fake documents for the fake E2E scenario.

Creates a job entry, publishes synthetic documents via
``FakeE2EPublisher``, and reports the outcome. Prints the
store-generated job ID to stdout for capture by the caller.

Usage:
    python -m document_processing.distributed.e2e_test.fake.publish_jobs \
        --job-name e2e-test-001 \
        --config-file document_processing/distributed/e2e_test/fake/config.yaml
"""

from typing import AsyncIterable, Tuple

import argparse
import asyncio
import logging
from pathlib import Path

from basics.logging import get_logger
from pymongo import AsyncMongoClient

from document_processing.distributed.e2e_test.fake.data import FAKE_DOCUMENTS
from document_processing.distributed.e2e_test.fake.e2e_publisher import (
    FakeE2EPublisher,
)
from document_processing.distributed.e2e_test.fake.pipeline_spec import PIPELINE
from document_processing.distributed.runtime_scripts.lib.logging_setup import (
    configure_logging,
    resolve_log_level,
)
from document_processing.distributed.warren.runtime.config import RuntimeConfig
from document_processing.distributed.warren.pubsub.rabbitmq.aio_pika.connection import (
    RMQConnectionManager,
)
from document_processing.distributed.warren.pubsub.rabbitmq.aio_pika.publisher import (
    RMQPublisher,
)
from document_processing.distributed.warren.storage.jobs.mongodb import (
    MongoDBJobStore,
)
from document_processing.distributed.warren.storage.publishing_tracker.mongodb import (
    MongoDBPublishingTracker,
)

module_logger: logging.Logger = get_logger(__name__)

DEFAULT_CONFIG_PATH: Path = Path(__file__).parent / "config.yaml"


async def _as_async_iterable(
    items: dict,
) -> AsyncIterable[Tuple[str, str]]:
    """Wrap FAKE_DOCUMENTS dict as an async iterable of (doc_id, content)."""
    for doc_id, content in items.items():
        yield (doc_id, content)


async def _publish(config: RuntimeConfig, job_name: str) -> str:
    """Create job, publish fake documents, report results.

    :return: The store-generated job ID.
    """
    mongo_cfg = config.mongodb
    mongo_client = AsyncMongoClient(host=mongo_cfg.host, port=mongo_cfg.port)

    job_store = MongoDBJobStore(
        client=mongo_client,
        database_name=mongo_cfg.database,
    )
    await job_store.setup()

    tracker = MongoDBPublishingTracker(
        client=mongo_client,
        database_name=mongo_cfg.database,
    )
    await tracker.setup()

    # job_name allows tracking across scripts (publish, check)
    # without scraping the store-generated job_id from logs,
    # which is fragile.
    job_id = await job_store.create_job(
        final_data_type=PIPELINE.final_data_type,
        metadata={"job_name": job_name},
    )

    connection_manager = RMQConnectionManager(config.rabbitmq.connection)
    exchange_config = config.rabbitmq.exchange

    publisher = RMQPublisher(
        connection_manager=connection_manager,
        exchange_config=exchange_config,
    )

    try:
        await connection_manager.setup()
        await publisher.setup()

        e2e_publisher = FakeE2EPublisher(
            publisher=publisher,
            tracker=tracker,
            job_store=job_store,
            name="fake-e2e-publisher",
        )

        result = await e2e_publisher.publish_job(
            job_id=job_id,
            sources=_as_async_iterable(FAKE_DOCUMENTS),
        )

        module_logger.info(
            f"Job {job_id}: published={result['published']}, "
            f"failed={result['failed']}, total={result['total']}"
        )

    finally:
        await publisher.teardown()
        await connection_manager.teardown()
        await mongo_client.close()

    return job_id


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Publish fake E2E test messages to RabbitMQ",
    )
    # job_name is stored in metadata so callers (check_completion)
    # can look up the job without scraping the store-generated
    # job_id from logs.
    parser.add_argument(
        "--job-name",
        required=True,
        help="Human-readable job name (stored in metadata).",
    )
    parser.add_argument(
        "--config-file",
        type=Path,
        default=DEFAULT_CONFIG_PATH,
        help=f"Path to e2e config YAML (default: {DEFAULT_CONFIG_PATH}).",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        default=False,
        help="Enable DEBUG logging (default: INFO).",
    )
    return parser.parse_args()


def main() -> None:
    global module_logger
    args = _parse_args()
    log_level = resolve_log_level(debug=args.debug)
    configure_logging(debug=args.debug)
    module_logger = get_logger(__name__, log_level=log_level)

    config = RuntimeConfig.from_yaml(args.config_file)

    asyncio.run(_publish(config, args.job_name))


if __name__ == "__main__":
    main()
