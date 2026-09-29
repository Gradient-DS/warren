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


class _ConnectionManager:
    def __init__(self, failures: int = 0) -> None:
        self.failures = failures
        self.calls = 0
        self.closed = 0

    async def setup(self) -> None:
        self.calls += 1
        if self.calls <= self.failures:
            msg = "broker unavailable"
            raise ConnectionError(msg)

    async def teardown(self) -> None:
        self.closed += 1


class _StoreClient:
    def __init__(self, failures: int = 0) -> None:
        self.admin = self
        self.failures = failures
        self.calls = 0
        self.closed = 0

    async def command(self, command: str) -> bool:
        assert command == "ping"
        return await self.ping()

    async def ping(self) -> bool:
        self.calls += 1
        if self.calls <= self.failures:
            msg = "store unavailable"
            raise ConnectionError(msg)
        return True

    async def close(self) -> None:
        self.closed += 1

    async def aclose(self) -> None:
        await self.close()


@pytest.mark.parametrize("target", ["broker", "mongodb", "redis"])
@pytest.mark.parametrize("attempts", [1, 3, 5])
def test_startup_retries_and_closes_failed_attempts(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    target: str,
    attempts: int,
) -> None:
    manager = _ConnectionManager(failures=3 if target == "broker" else 0)
    mongo = _StoreClient(failures=3 if target == "mongodb" else 0)
    redis = _StoreClient(failures=3 if target == "redis" else 0)
    monkeypatch.setattr(
        infrastructure.backends, "create_connection_manager", lambda config: manager
    )
    monkeypatch.setattr(infrastructure, "_create_mongo_client", lambda config: mongo)
    monkeypatch.setattr(infrastructure, "_create_redis_client", lambda config: redis)
    delays: list[float] = []

    async def sleep(delay: float) -> None:
        delays.append(delay)

    config = RuntimeConfig.model_validate(
        {
            "startup": {
                "attempts": attempts,
                "initial_delay_seconds": 2,
                "max_delay_seconds": 3,
            }
        }
    )
    with caplog.at_level(logging.WARNING):
        if attempts <= 3:
            with pytest.raises(ConnectionError, match="unavailable"):
                asyncio.run(
                    infrastructure.create_runtime_infrastructure(config, sleep=sleep)
                )
        else:
            infra = asyncio.run(
                infrastructure.create_runtime_infrastructure(config, sleep=sleep)
            )
            assert infra == infrastructure.RuntimeInfra(mongo, redis, manager)
    used = min(attempts, 4)
    failed = min(attempts, 3)
    assert manager.calls == used
    assert mongo.calls == (int(attempts > 3) if target == "broker" else used)
    assert redis.calls == (used if target == "redis" else int(attempts > 3))
    assert manager.closed == mongo.closed == redis.closed == failed
    assert delays == [2, 3, 3][: used - 1]
    assert len(caplog.records) == failed
    for attempt, record in enumerate(caplog.records, 1):
        assert f"attempt {attempt}/{attempts} failed:" in record.message
        assert "ConnectionError:" in record.message


def test_memory_startup_skips_stores_and_retry_delays(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def unexpected(config: object) -> None:
        pytest.fail("Memory startup must not create store clients")

    async def sleep(delay: float) -> None:
        pytest.fail("Memory startup must not retry")

    monkeypatch.setattr(infrastructure, "_create_mongo_client", unexpected)
    monkeypatch.setattr(infrastructure, "_create_redis_client", unexpected)
    infra = asyncio.run(
        infrastructure.create_runtime_infrastructure(
            RuntimeConfig(backend="memory"), sleep=sleep
        )
    )
    assert infra.mongo_client is infra.redis_client is None
    asyncio.run(infrastructure.close_runtime_infrastructure(infra))


def test_startup_warning_redacts_credentials_and_flattens_exception_chain(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    sensitive_value = "sensitive-test-value"
    uri = f"mongodb://worker:{sensitive_value}@localhost"

    class FailingManager(_ConnectionManager):
        async def setup(self) -> None:
            try:
                raise ValueError(uri)
            except ValueError as error:
                raise ConnectionError(sensitive_value) from error

    manager = FailingManager()
    monkeypatch.setattr(
        infrastructure.backends, "create_connection_manager", lambda config: manager
    )
    monkeypatch.setattr(
        infrastructure, "_create_mongo_client", lambda config: _StoreClient()
    )
    monkeypatch.setattr(
        infrastructure, "_create_redis_client", lambda config: _StoreClient()
    )
    config = RuntimeConfig(mongodb=MongoDBConfig(uri=SecretStr(uri)))
    with caplog.at_level(logging.WARNING), pytest.raises(ConnectionError):
        asyncio.run(infrastructure.create_runtime_infrastructure(config))
    assert len(caplog.records) == 1
    assert sensitive_value not in caplog.text
    assert "ValueError" in caplog.text
    assert "ConnectionError" in caplog.text
    assert "\n" not in caplog.records[0].message


@pytest.mark.parametrize("cancel", [False, True])
def test_startup_cleans_up_partial_connections(
    monkeypatch: pytest.MonkeyPatch, *, cancel: bool
) -> None:
    class Manager(_ConnectionManager):
        async def setup(self) -> None:
            raise asyncio.CancelledError

    manager = Manager()
    mongo = _StoreClient()
    redis = _StoreClient()

    def create_redis(config: RedisConfig) -> _StoreClient:
        if not cancel:
            msg = "invalid Redis configuration"
            raise ValueError(msg)
        return redis

    async def sleep(delay: float) -> None:
        pytest.fail("Cancelled startup must not retry")

    monkeypatch.setattr(
        infrastructure.backends, "create_connection_manager", lambda config: manager
    )
    monkeypatch.setattr(infrastructure, "_create_mongo_client", lambda config: mongo)
    monkeypatch.setattr(infrastructure, "_create_redis_client", create_redis)
    config = RuntimeConfig.model_validate({"startup": {"attempts": 3 if cancel else 1}})
    error = asyncio.CancelledError if cancel else ValueError
    with pytest.raises(error):
        asyncio.run(infrastructure.create_runtime_infrastructure(config, sleep=sleep))
    assert manager.closed == mongo.closed == 1
    assert redis.closed == int(cancel)
