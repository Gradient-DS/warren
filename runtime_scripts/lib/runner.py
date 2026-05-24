"""
Shared worker lifecycle for all launcher scripts.

Provides the single ``run()`` function that all launchers call.
The only thing that differs between launchers is the
``runner_factory_func`` that creates the runner instance.
"""

import logging
import uuid
from pathlib import Path

from basics.logging import get_logger

from document_processing.distributed.warren.exceptions import WarrenError
from document_processing.distributed.warren.runtime.config import RuntimeConfig
from document_processing.distributed.warren.workers.runners import WorkerRunnerBase

from document_processing.distributed.runtime_scripts.lib.pipeline import (
    load_config,
)

module_logger: logging.Logger = get_logger(__name__)

RunnerFactoryFunc = "Callable[[RuntimeConfig, str], WorkerRunnerBase]"


async def run(
    *,
    runner_factory_func: "Callable[[RuntimeConfig, str], WorkerRunnerBase]",
    config_file: Path,
    worker_name: str | None = None,
    worker_name_prefix: str = "worker",
    debug: bool = False,
    logger: logging.Logger | None = None,
) -> None:
    """Load config, create a runner via factory, and run it.

    This is the shared entry point for all launcher scripts. Each
    launcher provides its own ``runner_factory_func`` that creates the
    appropriate runner from ``(config, worker_name)``.

    :param runner_factory_func: callable that takes
        ``(config: RuntimeConfig, worker_name: str)`` and returns a
        ``WorkerRunnerBase`` instance. Use ``partial()`` to bind
        additional arguments (e.g. ``worker_spec``).
    :param config_file: path to RuntimeConfig YAML.
    :param worker_name: unique worker instance name. Defaults to
        ``<worker_name_prefix>-<uuid8>``.
    :param worker_name_prefix: prefix for auto-generated worker names.
    :param debug: enable DEBUG logging.
    :param logger: optional logger override.
    """
    log = logger or module_logger

    try:
        config = load_config(config_file)
    except Exception as e:
        raise WarrenError(
            f"Unable to load config from: {config_file}"
        ) from e

    log.info(f"Loaded config from: {config_file}")

    resolved_worker_name = (
        worker_name or f"{worker_name_prefix}-{uuid.uuid4().hex[:8]}"
    )

    try:
        runner = runner_factory_func(config, resolved_worker_name)
    except Exception as e:
        raise WarrenError(
            f"Unable to create runner for: {resolved_worker_name}"
        ) from e

    try:
        try:
            await runner.setup()
        except Exception as e:
            raise WarrenError(
                f"Worker setup failed for: {resolved_worker_name}"
            ) from e

        try:
            await runner.run()
        except Exception as e:
            raise WarrenError(
                f"Worker run failed for: {resolved_worker_name}"
            ) from e
    finally:
        await runner.teardown()
