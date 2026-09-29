import asyncio
import sys
from dataclasses import replace
from types import ModuleType
from unittest.mock import AsyncMock

import pytest

from runtime_scripts import start_job_publication_worker as launcher
from warren.pubsub.common import Route
from warren.pubsub.rabbitmq.config import RMQExchangeConfig
from warren.pubsub.routing import LANE_FIELD, MessageFieldRouter, observer_exchange
from warren.runtime.spec import PipelineSpec, PublishSpec
from warren.runtime.validation import PipelineValidationError


@pytest.fixture
def pipeline_module(monkeypatch):
    module = ModuleType("publication_test_pipeline")
    module.PIPELINE = PipelineSpec(
        workers={},
        exchange=RMQExchangeConfig(name="documents", type="topic"),
        result_collections=[],
        reference_collection="c",
        completion_collection="c",
        final_data_type="done",
    )
    module.create_publisher = AsyncMock()
    monkeypatch.setitem(sys.modules, module.__name__, module)
    return module


@pytest.mark.parametrize("exchange_type", ["fanout", "topic", "direct"])
@pytest.mark.parametrize("custom_routing", [False, True])
def test_cli_forwards_publication_router(
    monkeypatch, tmp_path, pipeline_module, exchange_type, custom_routing
):
    route_func = (
        MessageFieldRouter(prefix_field=LANE_FIELD, default_prefix="bulk")
        if custom_routing
        else None
    )
    pipeline = replace(
        pipeline_module.PIPELINE,
        exchange=RMQExchangeConfig(name="documents", type=exchange_type),
        publication=PublishSpec(route_func=route_func) if custom_routing else None,
    )
    pipeline_module.PIPELINE = pipeline
    run = AsyncMock()
    monkeypatch.setattr(launcher, "run", run)
    config_file = tmp_path / "config.yaml"
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "start_job_publication_worker",
            "--pipeline-spec",
            pipeline_module.__name__,
            "--publisher-factory",
            f"{pipeline_module.__name__}:create_publisher",
            "--config-file",
            str(config_file),
        ],
    )

    launcher.main()

    run.assert_awaited_once()
    assert run.call_args.kwargs["config_file"] == config_file
    factory = run.call_args.kwargs["runner_factory_func"]
    assert factory.func is launcher.JobPublicationWorkerRunner
    assert factory.keywords == {
        "exchange": observer_exchange(pipeline.exchange),
        "publish_exchange": pipeline.exchange,
        "route_func": route_func,
        "documents_publisher_factory": pipeline_module.create_publisher,
    }


def test_launcher_validates_publication_before_running(monkeypatch, pipeline_module):
    pipeline_module.PIPELINE = replace(
        pipeline_module.PIPELINE, publication=PublishSpec(route=Route("input"))
    )
    run = AsyncMock()
    monkeypatch.setattr(launcher, "run", run)

    with pytest.raises(PipelineValidationError, match="publication: static route"):
        asyncio.run(
            launcher.start_job_publication_worker(
                pipeline_spec=pipeline_module.__name__,
                publisher_factory=f"{pipeline_module.__name__}:create_publisher",
            )
        )

    run.assert_not_awaited()
