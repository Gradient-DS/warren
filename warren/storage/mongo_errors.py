"""Translate transient MongoDB errors into the storage exception taxonomy.

pymongo raises backend-specific exceptions; the storage interfaces speak
the generic ``TransientStoreError`` (see ``storage/exceptions.py``) so
callers — e.g. the ``JobStatusWorker`` observer, which must not drop an
observation — can retry transient failures without depending on pymongo.

Permanent errors (``OperationFailure``, ``DuplicateKeyError``, …) are not
translated; they propagate unchanged so they fail loud immediately.
"""

from typing import Awaitable, Callable, ParamSpec, TypeVar

import functools
import inspect

from pymongo.errors import ConnectionFailure

from document_processing.distributed.warren.storage.exceptions import (
    TransientStoreError,
)


# ``ConnectionFailure`` is the base of the transient/retryable failover
# family: ``AutoReconnect``, ``NetworkTimeout``, ``NotPrimaryError`` and
# ``ServerSelectionTimeoutError``. pymongo's own ``retryWrites``/
# ``retryReads`` (default on) already absorb a single such blip; this
# widens the window to multi-attempt failovers for callers that retry.
_TRANSIENT_MONGO_ERRORS = (ConnectionFailure,)

P = ParamSpec("P")
R = TypeVar("R")


def classify_transient(
    func: Callable[P, Awaitable[R]],
) -> Callable[P, Awaitable[R]]:
    """Re-raise transient pymongo errors from ``func`` as ``TransientStoreError``.

    Permanent errors propagate unchanged. Wrap async store methods so
    callers can retry on the standard ``TransientStoreError`` type.
    """

    @functools.wraps(func)
    async def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
        try:
            return await func(*args, **kwargs)
        except _TRANSIENT_MONGO_ERRORS as e:
            raise TransientStoreError(
                f"Transient MongoDB error in '{func.__name__}'"
            ) from e

    return wrapper


def classify_transient_methods(cls: type) -> type:
    """Class decorator: apply ``classify_transient`` to every public async method.

    Applies the transient-error contract store-wide, so the whole
    implementation honours "transient backend failures are raised as
    ``TransientStoreError``" without per-method boilerplate. Private
    (``_``-prefixed) methods are left untouched — public methods that
    call them still classify at the public boundary.
    """
    for name, attr in list(vars(cls).items()):
        if name.startswith("_"):
            continue
        if inspect.iscoroutinefunction(attr):
            setattr(cls, name, classify_transient(attr))
    return cls
