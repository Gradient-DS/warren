r"""
Retry worker launcher.

Starts a ``RetryWorkerRunner`` that consumes ``soft-failure`` messages
from the processing exchange, persists them, and republishes after a
delay.

Usage::

    python -m document_processing.distributed.runtime_scripts.start_retry_worker \
        --config-file ./pipeline/config.yaml
"""

import argparse
import asyncio
import logging
import sys
from pathlib import Path

from basics.logging import get_logger
from basics.logging_utils import summarize_exception_chain

from document_processing.distributed.runtime_scripts.lib.cli import (
    add_common_args,
)
from document_processing.distributed.runtime_scripts.lib.logging_setup import (
    configure_logging,
    resolve_log_level,
)
from document_processing.distributed.runtime_scripts.lib.runner import run
from document_processing.distributed.warren.retry_management.retry_worker_runner import (
    RetryWorkerRunner,
)

module_logger: logging.Logger = get_logger(__name__)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Start a retry worker",
    )
    add_common_args(parser)
    return parser.parse_args()


def describe_config(
    config_file: str | None,
    worker_name: str | None,
    debug: bool,
    logger: logging.Logger,
) -> None:
    """Log input configuration before any resolution or work."""
    logger.info("Configuration:")
    logger.info(f"  config_file: {config_file}")
    logger.info(f"  worker_name: {worker_name}")
    logger.info(f"  debug: {debug}")


async def start_retry_worker(
    *,
    config_file: str | None = None,
    worker_name: str | None = None,
    debug: bool = False,
    logger: logging.Logger | None = None,
) -> None:
    """Start a retry worker.

    :param config_file: path to RuntimeConfig YAML.
    :param worker_name: unique worker instance name.
    :param debug: enable DEBUG logging.
    :param logger: optional logger override.
    """
    log = logger or module_logger

    describe_config(
        config_file=config_file,
        worker_name=worker_name,
        debug=debug,
        logger=log,
    )

    resolved_config = Path(config_file) if config_file else Path("./pipeline/config.yaml")

    await run(
        runner_factory_func=RetryWorkerRunner,
        config_file=resolved_config,
        worker_name=worker_name,
        worker_name_prefix="retry-worker",
        debug=debug,
        logger=log,
    )


def main() -> None:
    global module_logger
    args = _parse_args()
    configure_logging(debug=args.debug)
    module_logger = get_logger(
        __name__, log_level=resolve_log_level(debug=args.debug)
    )

    try:
        asyncio.run(start_retry_worker(**vars(args), logger=module_logger))
    except Exception as e:
        module_logger.error(
            f"Start retry worker failed: {summarize_exception_chain(e)}"
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
