"""Scope context shared by handlers and storage adapters."""

from typing import ClassVar, Protocol, TypeGuard

import asyncio
import re
from collections.abc import Awaitable, Callable
from contextvars import ContextVar
from weakref import WeakSet

from pymongo import AsyncMongoClient
from pymongo.asynchronous.collection import AsyncCollection
from pymongo.asynchronous.database import AsyncDatabase
from pymongo.errors import OperationFailure
from redis.asyncio import Redis

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

    _instances: ClassVar[WeakSet["ScopedDatabase"]] = WeakSet()

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
        self._instances.add(self)

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


class ScopeClients(Protocol):
    mongo_client: AsyncMongoClient | None
    redis_client: Redis | None


async def erase_scope(
    infra_or_clients: ScopeClients | tuple[AsyncMongoClient, Redis],
    scope: str,
    *,
    control_database: str,
    prefix: str,
) -> dict[str, int]:
    """Erase scoped content, cache keys and control rows after traffic is stopped."""
    if not valid_scope(scope):
        reason = "malformed scope"
        raise HardFailureException(reason)
    database_name = prefix + scope
    if database_name == control_database:
        reason = "scope database must differ from the control database"
        raise ValueError(reason)
    if isinstance(infra_or_clients, tuple) and not hasattr(
        infra_or_clients, "mongo_client"
    ):
        mongo, redis = infra_or_clients
    else:
        mongo = infra_or_clients.mongo_client
        redis = infra_or_clients.redis_client
    if mongo is None or redis is None:
        reason = "erase_scope requires MongoDB and Redis clients"
        raise ValueError(reason)

    database = mongo[database_name]
    collections = await database.list_collection_names()
    try:
        await mongo.drop_database(database_name)
    except OperationFailure as error:
        if error.code != 13:
            raise
        for name in collections:
            await database.drop_collection(name)
    for resolver in ScopedDatabase._instances:
        if resolver.client is mongo:
            resolver._prepared.difference_update(
                key for key in tuple(resolver._prepared) if key[0] == database_name
            )

    redis_count = 0
    cursor = 0
    while True:
        cursor, keys = await redis.scan(cursor=cursor, match=f"s:{scope}:*", count=1000)
        for offset in range(0, len(keys), 1000):
            redis_count += await redis.unlink(*keys[offset : offset + 1000])
        if cursor == 0:
            break

    control_count = 0
    control = mongo[control_database]
    for name in await control.list_collection_names():
        result = await control[name].delete_many({"scope": scope})
        control_count += result.deleted_count
    return {
        "collections": len(collections),
        "redis_keys": redis_count,
        "control_rows": control_count,
    }
