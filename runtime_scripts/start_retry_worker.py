r"""
Retry worker launcher.

Starts a ``RetryWorkerRunner`` that consumes ``soft-failure`` messages
from the processing exchange, persists them, and republishes after a
delay.

Usage::

    python -m runtime_scripts.start_retry_worker \
        --config-file ./pipeline/config.yaml
"""

import argparse
import asyncio
import logging
import sys
from functools import partial
from pathlib import Path

from basics.logging import get_logger
from basics.logging_utils import summarize_exception_chain

from runtime_scripts.lib.cli import (
    add_common_args,
)
from runtime_scripts.lib.logging_setup import (
    configure_logging,
    resolve_log_level,
)
from runtime_scripts.lib.pipeline import (
    DEFAULT_PIPELINE_DIR,
    load_pipeline,
    resolve_config_path,
    resolve_observation_exchange,
)
from runtime_scripts.lib.runner import run
from warren.exceptions import WarrenError
from warren.retry_management.retry_worker_runner import (
    RetryWorkerRunner,
)


module_logger: logging.Logger = get_logger(__name__)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Start a retry worker",
    )
    parser.add_argument(
        "--pipeline-spec",
        type=str,
        default=None,
        help=(
            "Pipeline spec location (same format as start_worker). Needed to "
            "observe (must be a fanout or topic exchange). "
            f"Default path: {DEFAULT_PIPELINE_DIR}"
        ),
    )
    add_common_args(parser)
    return parser.parse_args()


def describe_config(
    pipeline_spec: str | None,
    config_file: str | None,
    worker_name: str | None,
    debug: bool,
    logger: logging.Logger,
) -> None:
    """Log input configuration before any resolution or work."""
    logger.info("Configuration:")
    logger.info(f"  pipeline_spec: {pipeline_spec}")
    logger.info(f"  config_file: {config_file}")
    logger.info(f"  worker_name: {worker_name}")
    logger.info(f"  debug: {debug}")


async def start_retry_worker(
    *,
    pipeline_spec: str | None = None,
    config_file: str | None = None,
    worker_name: str | None = None,
    debug: bool = False,
    logger: logging.Logger | None = None,
) -> None:
    """Start a retry worker.

    :param pipeline_spec: pipeline spec location (see ``--pipeline-spec``).
        Used to resolve the exchange the worker observes.
    :param config_file: path to RuntimeConfig YAML.
    :param worker_name: unique worker instance name.
    :param debug: enable DEBUG logging.
    :param logger: optional logger override.
    """
    log = logger or module_logger

    describe_config(
        pipeline_spec=pipeline_spec,
        config_file=config_file,
        worker_name=worker_name,
        debug=debug,
        logger=log,
    )

    spec_str = pipeline_spec or DEFAULT_PIPELINE_DIR
    try:
        pipeline, pipeline_dir = load_pipeline(spec_str, log)
    except Exception as e:
        msg = f"Unable to load pipeline spec from: {spec_str}"
        raise WarrenError(msg) from e

    exchange = resolve_observation_exchange(pipeline)
    resolved_config = resolve_config_path(
        Path(config_file) if config_file else None, pipeline_dir
    )

    await run(
        runner_factory_func=partial(RetryWorkerRunner, exchange=exchange),
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
    module_logger = get_logger(__name__, log_level=resolve_log_level(debug=args.debug))

    try:
        asyncio.run(start_retry_worker(**vars(args), logger=module_logger))
    except Exception as e:
        module_logger.error(
            f"Start retry worker failed: {summarize_exception_chain(e)}"
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
