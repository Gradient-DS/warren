"""Scope context shared by handlers and storage adapters."""

from typing import TypeGuard

import asyncio
import re
from collections.abc import Awaitable, Callable
from contextvars import ContextVar

from pymongo import AsyncMongoClient
from pymongo.asynchronous.collection import AsyncCollection
from pymongo.asynchronous.database import AsyncDatabase

from warren.common import HardFailureException


current_scope: ContextVar[str | None] = ContextVar("warren_scope", default=None)


def get_current_scope() -> str | None:
    """Return the raw scope of the current handler."""
    return current_scope.get()


def valid_scope(scope: object) -> TypeGuard[str]:
    """Whether a raw scope is a valid namespace."""
    return (
        isinstance(scope, str)
        and re.fullmatch(r"[a-z0-9][a-z0-9-]{0,39}", scope) is not None
    )


def require_scope(*, required: bool = True) -> str | None:
    scope = current_scope.get()
    if scope is None and not required:
        return None
    if not valid_scope(scope):
        reason = "missing scope" if scope is None else "malformed scope"
        raise HardFailureException(reason)
    return scope


def scope_fields(enabled: bool, scope: object = None) -> dict[str, str]:
    """Fields for a control row, without rejecting invalid envelopes."""
    value = current_scope.get() if scope is None else scope
    return {"scope": value} if enabled and valid_scope(value) else {}


def scoped_cache_key(key: str, *, required: bool = True) -> str:
    scope = require_scope(required=required)
    return f"s:{scope}:{key}" if scope is not None else key


class ScopedDatabase:
    """Resolve content collections and prepare indexes once per namespace."""

    def __init__(
        self,
        client: AsyncMongoClient,
        prefix: str = "wr_",
        *,
        required: bool = True,
        default_database: str | None = None,
    ) -> None:
        self.client = client
        self.prefix = prefix
        self.required = required
        self.default_database = default_database
        self._prepared: set[tuple[str, str]] = set()
        self._locks: dict[str, asyncio.Lock] = {}

    def __call__(self) -> AsyncDatabase:
        scope = require_scope(required=self.required)
        if scope is None:
            if self.default_database is None:
                reason = "missing scope"
                raise HardFailureException(reason)
            return self.client[self.default_database]
        return self.client[self.prefix + scope]

    async def collection(
        self,
        name: str,
        prepare: Callable[[AsyncCollection], Awaitable[None]],
    ) -> AsyncCollection:
        database = self()
        collection = database[name]
        key = (database.name, name)
        if key not in self._prepared:
            lock = self._locks.setdefault(database.name, asyncio.Lock())
            async with lock:
                if key not in self._prepared:
                    await prepare(collection)
                    self._prepared.add(key)
        return collection
