"""
Pipeline spec and config loading utilities.

Shared by all launcher scripts for resolving pipeline specs from
directories, files, or dotted module paths.
"""

import importlib
import importlib.util
import logging
from pathlib import Path

from basics.logging import get_logger

from warren.runtime.config import RuntimeConfig
from warren.runtime.spec import PipelineSpec


module_logger: logging.Logger = get_logger(__name__)

DEFAULT_PIPELINE_DIR: str = "./pipeline"
DEFAULT_SPEC_MODULE: str = "pipeline_spec"
DEFAULT_SPEC_VAR: str = "PIPELINE"
DEFAULT_CONFIG_FILE: str = "config.yaml"


def split_var_suffix(spec_str: str) -> tuple[str, str]:
    """Split ``path_or_module:VAR`` into ``(path_or_module, var_name)``.

    :return: tuple of (location, variable name).
    """
    if ":" in spec_str:
        location, var_name = spec_str.rsplit(":", 1)
        return location, var_name
    return spec_str, DEFAULT_SPEC_VAR


def import_file(file_path: Path) -> object:
    """Import a .py file as a module and return it.

    The spec module's own import/syntax/runtime errors propagate
    unchanged — the calling launcher wraps them with phase context
    (e.g. "Unable to load pipeline spec from: ..."). Only failure to
    create the module spec raises ``ImportError`` here.
    """
    spec = importlib.util.spec_from_file_location(
        file_path.stem,
        file_path,
    )
    if spec is None or spec.loader is None:
        msg = f"Cannot create module spec from {file_path}"
        raise ImportError(msg)

    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def resolve_pipeline_file(directory: Path) -> Path:
    """Find ``pipeline_spec.py`` inside a directory (non-recursive).

    :return: path to the spec file.
    :raises FileNotFoundError: if no pipeline_spec.py exists in the
        directory.
    """
    candidate = directory / f"{DEFAULT_SPEC_MODULE}.py"
    if candidate.is_file():
        return candidate
    msg = f"No {DEFAULT_SPEC_MODULE}.py in directory: {directory}"
    raise FileNotFoundError(msg)


def load_pipeline(
    spec_str: str,
    logger: logging.Logger | None = None,
) -> tuple[PipelineSpec, Path | None]:
    """Resolve a pipeline spec from the given location string.

    Resolution order:

    1. Split off ``:VAR`` suffix (default: ``PIPELINE``).
    2. If the location exists as a directory -> look for
       ``pipeline_spec.py`` inside, import it.
    3. If it exists as a file -> import it directly.
    4. Otherwise -> try ``importlib.import_module(location)``.
    5. Read ``VAR`` from the loaded module.

    :return: tuple of (PipelineSpec, resolved_dir) where
        ``resolved_dir`` is the directory the spec was found in
        (None for dotted-module imports).
    :raises FileNotFoundError: if directory/file does not exist or
        contains no pipeline_spec.py.
    :raises ImportError: if dotted module cannot be imported.
    :raises AttributeError: if the variable is not found in the module.
    :raises TypeError: if the variable is not a PipelineSpec.
    """
    log = logger or module_logger
    location, var_name = split_var_suffix(spec_str)

    resolved_dir: Path | None = None
    path = Path(location)

    if path.is_dir():
        spec_file = resolve_pipeline_file(path)
        log.info(
            f"Loading pipeline spec from directory: {path} "
            f"(file: {spec_file.name}, var: {var_name})"
        )
        mod = import_file(spec_file)
        resolved_dir = path

    elif path.is_file():
        log.info(f"Loading pipeline spec from file: {path} (var: {var_name})")
        mod = import_file(path)
        resolved_dir = path.parent

    else:
        log.info(f"Loading pipeline spec from module: {location} (var: {var_name})")
        mod = importlib.import_module(location)

    module_name = getattr(mod, "__name__", location)

    if not hasattr(mod, var_name):
        msg = f"Module {module_name} has no attribute '{var_name}'"
        raise AttributeError(msg)

    pipeline = getattr(mod, var_name)

    if not isinstance(pipeline, PipelineSpec):
        msg = (
            f"{var_name} in {module_name} is "
            f"{type(pipeline).__name__}, expected PipelineSpec"
        )
        raise TypeError(msg)

    return pipeline, resolved_dir


def resolve_config_path(
    config_file: Path | None,
    pipeline_dir: Path | None,
) -> Path:
    """Determine the config file path.

    :param config_file: explicit config path (takes precedence).
    :param pipeline_dir: pipeline directory to look for config.yaml in.
    :raises FileNotFoundError: if no config.yaml found in pipeline_dir.
    :raises ValueError: if neither config_file nor pipeline_dir provided.
    """
    if config_file is not None:
        return config_file

    if pipeline_dir is not None:
        candidate = pipeline_dir / DEFAULT_CONFIG_FILE
        if candidate.is_file():
            return candidate
        msg = (
            f"No {DEFAULT_CONFIG_FILE} in pipeline directory {pipeline_dir} "
            f"and no --config-file specified"
        )
        raise FileNotFoundError(msg)

    msg = (
        "--config-file is required when pipeline spec is loaded "
        "from a Python module (no directory to infer config.yaml from)"
    )
    raise ValueError(msg)


def load_config(config_file: Path) -> RuntimeConfig:
    """Load a RuntimeConfig from a YAML file.

    :raises FileNotFoundError: if the file does not exist.
    """
    if not config_file.is_file():
        msg = f"Config file not found: {config_file}"
        raise FileNotFoundError(msg)
    return RuntimeConfig.from_yaml(config_file)
