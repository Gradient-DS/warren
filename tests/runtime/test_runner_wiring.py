"""Pins what ``DefaultWorkerRunner`` hands to the backend factory.

No infrastructure: the factory is replaced by a recorder and the runner's
``_infra`` by a namespace carrying the one attribute the wiring reads.
"""

from types import SimpleNamespace

import pytest

from warren.pubsub.common import RetryConfig
from warren.pubsub.rabbitmq.config import RMQExchangeConfig
from warren.runtime import backends
from warren.runtime.config import RuntimeConfig, RuntimeRetryConfig
from warren.runtime.runner import DefaultWorkerRunner
from warren.runtime.spec import WorkerSpec


class _FakeWorker:
    name = "worker-1"
    type = "test_worker"

    async def __call__(self, message: dict) -> dict | None:
        return None


async def _factory(ctx):
    return _FakeWorker()


def test_runner_passes_retry_policy_to_consumer_manager(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict = {}

    def fake_create(config, connection_manager, **kwargs):
        captured.update(kwargs)
        return object()

    monkeypatch.setattr(backends, "create_consumer_manager", fake_create)
    config = RuntimeConfig(
        retry=RuntimeRetryConfig(policy=RetryConfig(max_delay_cap=900))
    )
    runner = DefaultWorkerRunner(
        config,
        "worker-1",
        worker_type="test_worker",
        worker_spec=WorkerSpec(collections={}, factory=_factory),
        exchange=RMQExchangeConfig(name="jobs", type="fanout"),
    )
    runner._infra = SimpleNamespace(pubsub_connection_manager=object())

    runner._create_consumer_manager(_FakeWorker(), None, None, None)

    assert captured["retry_config"].max_delay_cap == 900
