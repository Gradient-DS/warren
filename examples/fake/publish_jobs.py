"""
Publish initial pdf_document messages for the fake E2E scenario.

Publishes one message per fake document to the fanout exchange.

Usage:
    python -m document_processing.distributed.e2e_test.fake.publish_jobs \
        --job-id e2e-test-001 \
        --config-file document_processing/distributed/e2e_test/fake/config.yaml
"""
from typing import Dict, List

import argparse
import asyncio
import logging
import uuid
from pathlib import Path

from basics.logging import get_logger
from pydantic import SecretStr

from document_processing.distributed.e2e_test.config import (
    E2EConfig,
    configure_logging,
    resolve_log_level,
)
from document_processing.distributed.e2e_test.fake.data import FAKE_DOCUMENTS
from document_processing.distributed.pubsub.rabbitmq.connection import (
    RMQConnectionConfig,
    RMQConnectionManager,
)
from document_processing.distributed.pubsub.rabbitmq.consumer import (
    RMQExchangeConfig,
)
from document_processing.distributed.pubsub.rabbitmq.publisher import RMQPublisher

module_logger: logging.Logger = get_logger(__name__)

DEFAULT_CONFIG_PATH: Path = Path(__file__).parent / "config.yaml"


def _build_messages(job_id: str) -> List[Dict]:
    """Build pdf_document messages for all fake documents.

    :param job_id: Job identifier to attach to each message.
    :return: List of message dicts ready for publishing.
    """
    messages: List[Dict] = []

    for doc_id in FAKE_DOCUMENTS:
        messages.append({
            "data_type": "pdf_document",
            "data": {
                "doc_id": doc_id,
                "path": f"/fake/path/{doc_id}.pdf",
            },
            "job_id": job_id,
            "origin": {
                "type": "test_publisher",
                "name": "e2e-publisher",
            },
        })

    return messages


async def _publish(config: E2EConfig, job_id: str) -> None:
    """Connect to RabbitMQ and publish all test messages.

    :param config: E2E configuration.
    :param job_id: Job identifier.
    """
    rmq_conn_cfg = config.rabbitmq.connection
    connection_manager = RMQConnectionManager(
        RMQConnectionConfig(
            host=rmq_conn_cfg.host,
            port=rmq_conn_cfg.port,
            login=rmq_conn_cfg.login,
            password=SecretStr(rmq_conn_cfg.password),
        )
    )

    exchange_cfg = config.rabbitmq.exchange
    exchange_config = RMQExchangeConfig(
        name=exchange_cfg.name,
        type=exchange_cfg.type,
        durable=exchange_cfg.durable,
    )

    publisher = RMQPublisher(
        connection_manager=connection_manager,
        exchange_config=exchange_config,
    )

    try:
        await connection_manager.setup()
        await publisher.setup()

        messages = _build_messages(job_id)
        for msg in messages:
            await publisher(msg)
            module_logger.info(f"Published {msg['data']['doc_id']} (job={job_id})")

        module_logger.info(f"Published {len(messages)} messages for job {job_id}")

    finally:
        await publisher.teardown()
        await connection_manager.teardown()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Publish fake E2E test messages to RabbitMQ",
    )
    parser.add_argument(
        "--job-id",
        default=None,
        help="Job ID (default: auto-generated e2e-<uuid>).",
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

    config = E2EConfig.from_yaml(args.config_file)
    job_id = args.job_id or f"e2e-{uuid.uuid4().hex[:8]}"

    module_logger.info(f"Job ID: {job_id}")
    asyncio.run(_publish(config, job_id))


if __name__ == "__main__":
    main()
