"""
Publish synthetic documents into a **fanout or topic** exchange example.

Creates a job entry, publishes the stand-in documents via
``MockDocumentsPublisher``, and reports the outcome. The exchange is taken
from whatever ``--pipeline-spec`` points at, so this one publisher serves
both the fanout and the topic example (the direct example has its own
publisher — it needs a routing plan).

Usage:
    # fanout (default)
    python -m examples.exchanges.publish \
        --job-name demo-001 \
        --config-file examples/exchanges/fanout/config.yaml

    # topic
    python -m examples.exchanges.publish \
        --job-name demo-001 \
        --pipeline-spec ./examples/exchanges/topic \
        --config-file examples/exchanges/topic/config.yaml
"""

import argparse
import asyncio
import logging
from collections.abc import AsyncIterable
from pathlib import Path

from basics.logging import get_logger
from pymongo import AsyncMongoClient

from examples.exchanges.data import FAKE_DOCUMENTS
from examples.exchanges.documents_publisher import (
    MockDocumentsPublisher,
)
from runtime_scripts.lib.logging_setup import (
    configure_logging,
    resolve_log_level,
)
from runtime_scripts.lib.pipeline import load_pipeline
from warren.pubsub.routing import observer_route_func
from warren.runtime import backends
from warren.runtime.config import RuntimeConfig
from warren.runtime.spec import PipelineSpec
from warren.storage.jobs.mongodb import (
    MongoDBJobStore,
)
from warren.storage.publishing_tracker.mongodb import (
    MongoDBPublishingTracker,
)


module_logger: logging.Logger = get_logger(__name__)

# Configs live per-exchange (each writes to its own database); the fanout
# one is the default. Override with --config-file for the topic example.
DEFAULT_CONFIG_PATH: Path = Path(__file__).parent / "fanout" / "config.yaml"


async def _as_async_iterable(
    items: dict,
) -> AsyncIterable[tuple[str, str]]:
    """Wrap FAKE_DOCUMENTS dict as an async iterable of (doc_id, content)."""
    for doc_id, content in items.items():
        yield (doc_id, content)


async def _publish(
    config: RuntimeConfig,
    pipeline: PipelineSpec,
    job_name: str,
) -> str:
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
        final_data_type=pipeline.final_data_type,
        metadata={"job_name": job_name},
    )

    connection_manager = backends.create_connection_manager(config)
    exchange_config = pipeline.exchange

    publisher = backends.create_publisher(
        config,
        connection_manager,
        exchange=exchange_config,
        route_func=observer_route_func(exchange_config),
    )

    try:
        await connection_manager.setup()
        await publisher.setup()

        documents_publisher = MockDocumentsPublisher(
            publisher=publisher,
            tracker=tracker,
            job_store=job_store,
            name="mock-documents-publisher",
        )

        result = await documents_publisher.publish_job(
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
        description="Publish synthetic documents to a fanout/topic exchange example",
    )
    # job_name is stored in metadata so other tools (e.g. inspect_job)
    # can look up the job without scraping the store-generated
    # job_id from logs.
    parser.add_argument(
        "--job-name",
        required=True,
        help="Human-readable job name (stored in metadata).",
    )
    parser.add_argument(
        "--pipeline-spec",
        type=str,
        default="./examples/exchanges/fanout",
        help=(
            "Pipeline spec location (same format as start_worker). Determines "
            "the exchange the documents are published to. "
            "Default: ./examples/exchanges/fanout"
        ),
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
    pipeline, _ = load_pipeline(args.pipeline_spec, module_logger)

    asyncio.run(_publish(config, pipeline, args.job_name))


if __name__ == "__main__":
    main()
