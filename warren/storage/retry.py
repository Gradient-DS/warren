"""Retry helper for transient storage failures.

Backend-agnostic: operates on the storage taxonomy's
``TransientStoreError`` (see ``exceptions.py``), so any caller that
cannot tolerate a dropped operation — e.g. an observer worker — can wrap
its store interaction without depending on a concrete backend. The
classification of *what* is transient lives in the implementation (see
``mongo_errors.py``); this is only the retry *policy* mechanism.
"""

import asyncio
from collections.abc import Awaitable, Callable

from document_processing.distributed.warren.storage.exceptions import (
    TransientStoreError,
)


async def run_with_transient_retry[R](
    operation: Callable[[], Awaitable[R]],
    *,
    attempts: int = 5,
    base_delay: float = 1.0,
    max_delay: float = 8.0,
    on_retry: Callable[[int, TransientStoreError], None] | None = None,
) -> R:
    """Run ``operation``, retrying ``TransientStoreError`` with exponential backoff.

    Permanent errors propagate immediately — only ``TransientStoreError``
    is retried. After ``attempts`` transient failures the last error is
    re-raised, leaving the caller to decide whether to propagate or drop.

    Defaults (5 attempts, 1s base, 8s cap → ~15s total) are sized to ride
    a MongoDB replica-set failover/election (``electionTimeoutMillis``
    defaults to 10s); a longer outage is treated as a real one.

    :param operation: Zero-argument async callable. Re-invoked from
        scratch on each attempt, so it must be idempotent.
    :param attempts: Maximum number of attempts (must be >= 1).
    :param base_delay: Base backoff in seconds; before the try after
        attempt ``n`` it sleeps ``base_delay * 2 ** (n - 1)``.
    :param max_delay: Upper bound on a single backoff sleep, so the
        exponential growth cannot run away.
    :param on_retry: Optional callback ``on_retry(attempt, error)``
        invoked before each backoff sleep — e.g. for contextual logging.

    :return: The result of ``operation``.

    :raises TransientStoreError: if every attempt fails transiently.
    """
    for attempt in range(1, attempts + 1):
        try:
            return await operation()
        except TransientStoreError as e:
            if attempt >= attempts:
                raise
            if on_retry is not None:
                on_retry(attempt, e)
            delay = min(base_delay * (2 ** (attempt - 1)), max_delay)
            await asyncio.sleep(delay)

    msg = f"run_with_transient_retry requires attempts >= 1, got {attempts}"
    raise TransientStoreError(msg)
