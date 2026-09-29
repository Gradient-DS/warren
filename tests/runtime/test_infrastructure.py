"""Runtime client configuration without external services."""

from typing import Any

import asyncio
import logging

import pytest
from pydantic import SecretStr
from redis.asyncio.connection import SSLConnection

from warren.runtime import infrastructure
from warren.runtime.config import MongoDBConfig, RedisConfig, RuntimeConfig


def test_mongo_client_options(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}
    client = object()

    def create(*args: object, **kwargs: object) -> object:
        captured.update(args=args, **kwargs)
        return client

    monkeypatch.setattr(infrastructure, "AsyncMongoClient", create)
    config = MongoDBConfig(
        host="mongo.example",
        port=27018,
        username="worker",
        password=SecretStr("test-secret"),
        auth_source="admin",
        tls=True,
        max_pool_size=20,
        server_selection_timeout_ms=1500,
    )
    assert infrastructure._create_mongo_client(config) is client
    assert captured == {
        "args": (),
        "host": "mongo.example",
        "port": 27018,
        "username": "worker",
        "password": config.password.get_secret_value(),
        "authSource": "admin",
        "tls": True,
        "maxPoolSize": 20,
        "serverSelectionTimeoutMS": 1500,
    }


def test_mongo_uri_overrides_host_and_preserves_uri_options(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}

    def create(*args: object, **kwargs: object) -> object:
        captured.update(args=args, **kwargs)
        return object()

    monkeypatch.setattr(infrastructure, "AsyncMongoClient", create)
    uri = "mongodb://mongo.example:27018/?tls=true&maxPoolSize=12"
    infrastructure._create_mongo_client(MongoDBConfig(uri=SecretStr(uri)))
    assert captured == {"args": (uri,)}


def test_redis_client_options() -> None:
    config = RedisConfig(
        host="redis.example",
        port=6380,
        username="worker",
        password=SecretStr("test-secret"),
        db=3,
        ssl=True,
        max_connections=20,
        socket_timeout=1.5,
    )
    client = infrastructure._create_redis_client(config)
    pool = client.connection_pool
    options = pool.connection_kwargs
    assert pool.connection_class is SSLConnection
    assert pool.max_connections == 20
    assert options["host"] == "redis.example"
    assert options["port"] == 6380
    assert options["username"] == "worker"
    assert options["password"] == config.password.get_secret_value()
    assert options["db"] == 3
    assert options["socket_timeout"] == 1.5
    asyncio.run(client.aclose())


def test_redis_url_overrides_host_and_preserves_url_options() -> None:
    config = RedisConfig(
        url=SecretStr("rediss://worker:test-secret@redis.example:6380/4"),
        host="ignored.example",
        port=1234,
        max_connections=20,
        socket_timeout=1.5,
    )
    client = infrastructure._create_redis_client(config)
    pool = client.connection_pool
    assert pool.connection_class is SSLConnection
    assert pool.max_connections == 20
    assert pool.connection_kwargs["host"] == "redis.example"
    assert pool.connection_kwargs["port"] == 6380
    assert pool.connection_kwargs["db"] == 4
    assert pool.connection_kwargs["socket_timeout"] == 1.5
    asyncio.run(client.aclose())


@pytest.mark.parametrize("use_url", [False, True])
def test_secrets_are_hidden_in_representations_and_logs(
    caplog: pytest.LogCaptureFixture, *, use_url: bool
) -> None:
    sensitive_value = "sensitive-test-value"
    if use_url:
        mongo = MongoDBConfig(
            uri=SecretStr(f"mongodb://worker:{sensitive_value}@localhost")
        )
        redis = RedisConfig(
            url=SecretStr(f"redis://worker:{sensitive_value}@localhost")
        )
    else:
        mongo = MongoDBConfig(username="worker", password=SecretStr(sensitive_value))
        redis = RedisConfig(username="worker", password=SecretStr(sensitive_value))
    config = RuntimeConfig(mongodb=mongo, redis=redis)
    mongo_client = infrastructure._create_mongo_client(mongo)
    redis_client = infrastructure._create_redis_client(redis)
    with caplog.at_level(logging.INFO):
        for value in (config, mongo, redis, mongo_client, redis_client):
            assert sensitive_value not in repr(value)
            assert sensitive_value not in str(value)
            infrastructure.module_logger.info("Configuration: %s; %r", value, value)
    assert sensitive_value not in caplog.text

    async def close() -> None:
        await mongo_client.close()
        await redis_client.aclose()

    asyncio.run(close())
