from typing import TYPE_CHECKING

import math
import random


if TYPE_CHECKING:
    from warren.common import SoftFailureException
    from warren.pubsub.common import RetryConfig


def nonnegative_number(value: object, name: str) -> int | float:
    """Reject booleans, nonnumbers and values unsuitable for timers."""
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or (isinstance(value, float) and not math.isfinite(value))
        or value < 0
    ):
        reason = f"{name} must be a finite nonnegative number"
        raise ValueError(reason)
    return value


def retry_metadata(value: object) -> dict:
    """Validate counters before arithmetic and delays before scheduling."""
    if not isinstance(value, dict):
        reason = "retry must be a dictionary"
        raise ValueError(reason)
    count = value.get("count", 0)
    if isinstance(count, bool) or not isinstance(count, int) or count < 0:
        reason = "retry.count must be a nonnegative integer"
        raise ValueError(reason)
    if "after" in value:
        nonnegative_number(value["after"], "retry.after")
    return value


def resolve_retry_after(error: "SoftFailureException", attempt: int, config: "RetryConfig") -> int:
    """Keep zero as the legacy default request and cap computed backoff."""
    base = nonnegative_number(error.retry_after if error.retry_after is not None else 0, "retry_after")
    exp_base = nonnegative_number(error.backoff_base if error.backoff_base is not None else 0, "backoff_base")
    base = base or config.initial_delay
    exp_base = exp_base or config.backoff_base
    use_jitter = error.jitter if error.jitter is not None else config.jitter
    # Bound the exponent even for a caller that bypassed message validation.
    exponent = max(min(attempt, config.max_retries_cap + 1) - 1, 0)
    try:
        delay = base * (exp_base**exponent)
    except OverflowError:
        return config.max_delay_cap
    if use_jitter:
        delay *= random.uniform(0.5, 1.5)
    return int(min(delay, config.max_delay_cap))


def resolve_retry_max(error: "SoftFailureException", config: "RetryConfig") -> int:
    """Keep zero as the legacy default request and cap worker intent."""
    requested = error.retry_max
    if requested is not None and (isinstance(requested, bool) or not isinstance(requested, int) or requested < 0):
        reason = "retry_max must be a nonnegative integer"
        raise ValueError(reason)
    return min(requested or config.max_retries, config.max_retries_cap)
