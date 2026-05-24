"""
Logging setup for runtime script entry points.

Workaround for basics.logging auto-configuring the root logger at
DEBUG on import. Call ``configure_logging()`` early in ``main()``
before any substantive log output.
"""

import logging


def resolve_log_level(*, debug: bool) -> int:
    """Return the logging level based on the --debug flag.

    :param debug: whether debug logging is enabled.
    :return: ``logging.DEBUG`` or ``logging.INFO``.
    """
    return logging.DEBUG if debug else logging.INFO


def configure_logging(*, debug: bool) -> None:
    """Configure logging for a CLI entry point.

    :param debug: whether debug logging is enabled.
    """
    from basics.logging import setup_logging

    log_level = resolve_log_level(debug=debug)
    setup_logging(log_level=log_level)
    logging.getLogger().setLevel(log_level)
