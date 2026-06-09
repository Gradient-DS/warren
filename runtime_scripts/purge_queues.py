r"""
Purge RabbitMQ queues and exchange.

Thin CLI wrapper around the framework ``purge_queues()`` utility.
Reads queue names from a pipeline spec or accepts them as CLI args.

Usage::

    # From pipeline spec
    python -m document_processing.distributed.runtime_scripts.purge_queues \
        --config-file ./pipeline/config.yaml \
        --pipeline-spec ./pipeline

    # Explicit queue names
    python -m document_processing.distributed.runtime_scripts.purge_queues \
        --config-file ./pipeline/config.yaml \
        --queues jobs.parser jobs.chunker jobs.embedder
"""

import argparse
import asyncio
import logging
import sys
from pathlib import Path

from basics.logging import get_logger
from basics.logging_utils import summarize_exception_chain

from document_processing.distributed.runtime_scripts.lib.logging_setup import (
    configure_logging,
    resolve_log_level,
)
from document_processing.distributed.runtime_scripts.lib.pipeline import (
    DEFAULT_PIPELINE_DIR,
    load_config,
    load_pipeline,
)
from document_processing.distributed.warren.exceptions import WarrenError
from document_processing.distributed.warren.pubsub.rabbitmq.aio_pika.connection import (
    RMQConnectionManager,
)
from document_processing.distributed.warren.pubsub.rabbitmq.aio_pika.purge import (
    purge_queues,
)
from document_processing.distributed.warren.pubsub.rabbitmq.config import (
    RMQConnectionConfig,
)


module_logger: logging.Logger = get_logger(__name__)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Purge RabbitMQ queues and exchange",
    )
    parser.add_argument(
        "--config-file",
        type=Path,
        default=None,
        help="Path to RuntimeConfig YAML. Default: ./pipeline/config.yaml",
    )
    parser.add_argument(
        "--pipeline-spec",
        type=str,
        default=None,
        help=(
            "Pipeline spec location (to read queue names from worker types). "
            f"Format: [<path>|<module>][:<var>]. Default: {DEFAULT_PIPELINE_DIR}"
        ),
    )
    parser.add_argument(
        "--queues",
        nargs="+",
        default=None,
        help="Explicit queue names to purge (overrides --pipeline-spec).",
    )
    parser.add_argument(
        "--exchange",
        type=str,
        default=None,
        help="Exchange name to delete. Read from config if not specified.",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        default=False,
        help="Enable DEBUG logging (default: INFO).",
    )
    return parser.parse_args()


def describe_config(
    config_file: Path | None,
    pipeline_spec: str | None,
    queues: list[str] | None,
    exchange: str | None,
    debug: bool,
    logger: logging.Logger,
) -> None:
    """Log input configuration."""
    logger.info("Configuration:")
    logger.info(f"  config_file: {config_file}")
    logger.info(f"  pipeline_spec: {pipeline_spec}")
    logger.info(f"  queues: {queues}")
    logger.info(f"  exchange: {exchange}")
    logger.info(f"  debug: {debug}")


async def run_purge(
    *,
    config_file: Path | None = None,
    pipeline_spec: str | None = None,
    queues: list[str] | None = None,
    exchange: str | None = None,
    debug: bool = False,
    logger: logging.Logger | None = None,
) -> None:
    """Purge RabbitMQ queues and optionally delete the exchange.

    :param config_file: path to RuntimeConfig YAML.
    :param pipeline_spec: pipeline spec location (for deriving queue
        names from worker types).
    :param queues: explicit queue names (overrides pipeline_spec).
    :param exchange: exchange name to delete. Read from config if
        not specified.
    :param debug: enable DEBUG logging.
    :param logger: optional logger override.
    """
    log = logger or module_logger

    describe_config(
        config_file=config_file,
        pipeline_spec=pipeline_spec,
        queues=queues,
        exchange=exchange,
        debug=debug,
        logger=log,
    )

    resolved_config_path = config_file or Path("./pipeline/config.yaml")

    try:
        config = load_config(resolved_config_path)
    except Exception as e:
        raise WarrenError(f"Unable to load config from: {resolved_config_path}") from e

    if queues is not None:
        queue_names = queues
    else:
        spec_str = pipeline_spec or DEFAULT_PIPELINE_DIR
        try:
            pipeline, _ = load_pipeline(spec_str, log)
        except Exception as e:
            raise WarrenError(f"Unable to load pipeline spec from: {spec_str}") from e

        exchange_name = config.rabbitmq.exchange.name
        queue_names = [f"{exchange_name}.{wt}" for wt in pipeline.workers]

    exchange_to_delete = exchange or config.rabbitmq.exchange.name

    log.info(f"Queues to purge: {queue_names}")
    log.info(f"Exchange to delete: {exchange_to_delete}")

    rmq_cfg = config.rabbitmq.connection
    try:
        connection_manager = RMQConnectionManager(
            RMQConnectionConfig(
                host=rmq_cfg.host,
                port=rmq_cfg.port,
                login=rmq_cfg.login,
                password=rmq_cfg.password,
            ),
        )
    except Exception as e:
        raise WarrenError(
            f"Unable to create RMQ manager for: {rmq_cfg.host}:{rmq_cfg.port}"
        ) from e

    try:
        await connection_manager.setup()
    except Exception as e:
        raise WarrenError(
            f"Unable to connect to RabbitMQ at: {rmq_cfg.host}:{rmq_cfg.port}"
        ) from e

    try:
        await purge_queues(
            connection_manager=connection_manager,
            queue_names=queue_names,
            exchange_name=exchange_to_delete,
        )
    except Exception as e:
        raise WarrenError(
            f"Failed to purge queues: {queue_names}, exchange: {exchange_to_delete}"
        ) from e
    finally:
        await connection_manager.teardown()


def main() -> None:
    global module_logger
    args = _parse_args()
    configure_logging(debug=args.debug)
    module_logger = get_logger(__name__, log_level=resolve_log_level(debug=args.debug))

    try:
        asyncio.run(run_purge(**vars(args), logger=module_logger))
    except Exception as e:
        module_logger.error(f"Purge failed: {summarize_exception_chain(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
