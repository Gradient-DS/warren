"""Unit tests for ``run_with_transient_retry`` (B9.5 M6).

Pins the retry mechanism: transient errors are retried up to ``attempts``,
permanent errors propagate immediately (never retried), and exhaustion
re-raises so the caller decides propagate-vs-drop.
"""

import asyncio

from document_processing.distributed.warren.storage.exceptions import (
    TransientStoreError,
)
from document_processing.distributed.warren.storage.retry import (
    run_with_transient_retry,
)


def test_retries_transient_until_success() -> None:
    calls = {"n": 0}
    retries: list[int] = []

    async def op() -> str:
        calls["n"] += 1
        if calls["n"] < 3:
            msg = "blip"
            raise TransientStoreError(msg)
        return "ok"

    result = asyncio.run(
        run_with_transient_retry(
            op,
            attempts=5,
            base_delay=0.0,
            on_retry=lambda attempt, _error: retries.append(attempt),
        )
    )

    assert result == "ok"
    assert calls["n"] == 3
    assert retries == [1, 2]  # fired before attempts 2 and 3, not after success


def test_raises_after_exhausting_attempts() -> None:
    calls = {"n": 0}

    async def op() -> str:
        calls["n"] += 1
        msg = "always"
        raise TransientStoreError(msg)

    raised = False
    try:
        asyncio.run(run_with_transient_retry(op, attempts=3, base_delay=0.0))
    except TransientStoreError:
        raised = True

    assert raised
    assert calls["n"] == 3


def test_permanent_error_propagates_immediately() -> None:
    calls = {"n": 0}

    async def op() -> str:
        calls["n"] += 1
        msg = "permanent"
        raise ValueError(msg)

    raised = False
    try:
        asyncio.run(run_with_transient_retry(op, attempts=3, base_delay=0.0))
    except ValueError:
        raised = True

    assert raised
    assert calls["n"] == 1  # not retried
