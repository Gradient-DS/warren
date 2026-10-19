"""
Logging setup for runtime script entry points.

Workaround for basics.logging auto-configuring the root logger at
DEBUG on import. Call ``configure_logging()`` early in ``main()``
before any substantive log output.

Two environment variables change the output, for every entry point:

- ``WARREN_LOG_CONFIG``: a JSON or YAML file in the standard
  ``logging.config.dictConfig`` schema. It replaces warren's own setup,
  so a host can plug in its own formatter. ``--debug`` does not apply, and
  ``disable_existing_loggers`` defaults to false.
- ``WARREN_LOG_FORMAT``: ``text`` (default) or ``json``, one JSON object
  per line on stderr for a log collector.
"""

import json
import logging
import logging.config
import os
import sys
import traceback
from datetime import UTC, datetime
from pathlib import Path

import yaml


LOG_CONFIG_ENV = "WARREN_LOG_CONFIG"
LOG_FORMAT_ENV = "WARREN_LOG_FORMAT"

#: Attributes every ``LogRecord`` has; anything else on a record came from ``extra=``.
_RECORD_ATTRIBUTES = frozenset(vars(logging.LogRecord("", 0, "", 0, "", (), None))) | {
    "message",
    "asctime",
}


class JsonFormatter(logging.Formatter):
    """One JSON object per record: time, level, logger, message, any ``extra=`` fields, and the error."""

    def format(self, record: logging.LogRecord) -> str:
        entry: dict[str, object] = {
            "ts": datetime.fromtimestamp(record.created, UTC).isoformat(
                timespec="milliseconds"
            ),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        entry.update(
            {
                key: value
                for key, value in vars(record).items()
                if key not in _RECORD_ATTRIBUTES
            }
        )
        if record.exc_info and record.exc_info[1] is not None:
            entry["error"] = {
                "type": type(record.exc_info[1]).__qualname__,
                "traceback": "".join(traceback.format_exception(*record.exc_info)),
            }
        return json.dumps(entry, ensure_ascii=False, default=str)


def resolve_log_level(*, debug: bool) -> int:
    """Return the logging level based on the --debug flag.

    :param debug: whether debug logging is enabled.
    :return: ``logging.DEBUG`` or ``logging.INFO``.
    """
    return logging.DEBUG if debug else logging.INFO


def configure_logging(*, debug: bool) -> None:
    """Configure logging for a CLI entry point.

    :param debug: whether debug logging is enabled.
    :raises ValueError: ``WARREN_LOG_FORMAT`` is neither ``text`` nor ``json``.
    :raises OSError: ``WARREN_LOG_CONFIG`` names a file that cannot be read.
    """
    if config := os.environ.get(LOG_CONFIG_ENV):
        schema = yaml.safe_load(Path(config).read_text(encoding="utf-8"))
        # dictConfig would otherwise silence every logger created before it, warren's included.
        schema.setdefault("disable_existing_loggers", False)
        logging.config.dictConfig(schema)
        return
    log_format = os.environ.get(LOG_FORMAT_ENV, "text").strip().lower()
    if log_format not in ("text", "json"):
        msg = f"{LOG_FORMAT_ENV} must be 'text' or 'json', got {log_format!r}"
        raise ValueError(msg)
    log_level = resolve_log_level(debug=debug)
    if log_format == "json":
        handler = logging.StreamHandler(sys.stderr)
        handler.setFormatter(JsonFormatter())
        # The level sits on the handler too: warren's loggers set themselves to DEBUG.
        handler.setLevel(log_level)
        logging.basicConfig(level=log_level, handlers=[handler], force=True)
        return
    from basics.logging import setup_logging

    setup_logging(log_level=log_level)
    logging.getLogger().setLevel(log_level)
