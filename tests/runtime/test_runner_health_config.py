"""Every runner must honour the ``health:`` section of ``RuntimeConfig``.

Construction alone must preserve the settings, without starting infrastructure.
"""

from warren.jobs.publishing.job_publication_worker_runner import (
    JobPublicationWorkerRunner,
)
from warren.jobs.status.job_status_worker_runner import JobStatusWorkerRunner
from warren.pubsub.rabbitmq.config import RMQExchangeConfig
from warren.retry_management.retry_worker_runner import RetryWorkerRunner
from warren.runtime.config import RuntimeConfig
from warren.runtime.runner import DefaultWorkerRunner
from warren.runtime.spec import WorkerSpec
from warren.workers.health import HealthConfig


async def _worker_factory(ctx):
    msg = "Construction must not create a worker"
    raise AssertionError(msg)


async def _documents_publisher_factory(publisher, infra, config, worker_name):
    msg = "Construction must not create a documents publisher"
    raise AssertionError(msg)


def test_default_worker_runner_honours_health_config() -> None:
    config = RuntimeConfig(health=HealthConfig(enabled=False, port=9191))
    runner = DefaultWorkerRunner(
        config,
        "worker-1",
        worker_type="test_worker",
        worker_spec=WorkerSpec(collections={}, factory=_worker_factory),
        exchange=RMQExchangeConfig(name="jobs", type="fanout"),
    )

    assert runner._health == config.health
    assert runner._health.enabled is False
    assert runner._health.port == 9191


def test_job_status_worker_runner_honours_health_config() -> None:
    config = RuntimeConfig(health=HealthConfig(enabled=False, port=9191))
    runner = JobStatusWorkerRunner(
        config,
        "status-1",
        exchange=RMQExchangeConfig(name="jobs", type="fanout"),
    )

    assert runner._health == config.health
    assert runner._health.enabled is False
    assert runner._health.port == 9191


def test_retry_worker_runner_honours_health_config() -> None:
    config = RuntimeConfig(health=HealthConfig(enabled=False, port=9191))
    runner = RetryWorkerRunner(
        config,
        "retry-1",
        exchange=RMQExchangeConfig(name="jobs", type="fanout"),
    )

    assert runner._health == config.health
    assert runner._health.enabled is False
    assert runner._health.port == 9191


def test_job_publication_worker_runner_honours_health_config() -> None:
    config = RuntimeConfig(health=HealthConfig(enabled=False, port=9191))
    runner = JobPublicationWorkerRunner(
        config,
        "publication-1",
        exchange=RMQExchangeConfig(name="jobs", type="fanout"),
        documents_publisher_factory=_documents_publisher_factory,
    )

    assert runner._health == config.health
    assert runner._health.enabled is False
    assert runner._health.port == 9191
