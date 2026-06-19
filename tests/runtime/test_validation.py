"""Unit tests for deploy-time pipeline validation (no broker needed)."""

import dataclasses

import pytest

from warren.pubsub.rabbitmq.config import RMQExchangeConfig
from warren.pubsub.routing import MessageFieldRouter
from warren.runtime.spec import PipelineSpec, PublishSpec, WorkerSpec
from warren.runtime.validation import PipelineValidationError, validate_pipeline


async def _factory(ctx):  # pragma: no cover - never called by validation
    raise NotImplementedError


def _pipeline(workers, exchanges, default_exchange="jobs"):
    return PipelineSpec(
        workers=workers,
        exchanges=exchanges,
        default_exchange=default_exchange,
        result_collections=["c"],
        reference_collection="c",
        completion_collection="c",
        final_data_type="done",
    )


def _fanout_pipeline():
    return _pipeline(
        workers={
            "w": WorkerSpec(
                collections={"write": "c"},
                factory=_factory,
                consume_exchange="jobs",
                publish=[PublishSpec(exchange="jobs")],
            )
        },
        exchanges={"jobs": RMQExchangeConfig(name="jobs", type="fanout")},
    )


def _topic_pipeline():
    return _pipeline(
        workers={
            "w": WorkerSpec(
                collections={"write": "c"},
                factory=_factory,
                consume_exchange="docs",
                binding_key="pdf_document",
                publish=[PublishSpec(exchange="docs", route_func=MessageFieldRouter())],
            )
        },
        exchanges={"docs": RMQExchangeConfig(name="docs", type="topic")},
        default_exchange="docs",
    )


def test_valid_fanout_pipeline_passes():
    validate_pipeline(_fanout_pipeline())


def test_valid_topic_pipeline_passes():
    validate_pipeline(_topic_pipeline())


def test_dangling_default_exchange():
    p = dataclasses.replace(_fanout_pipeline(), default_exchange="missing")
    with pytest.raises(PipelineValidationError, match="default_exchange"):
        validate_pipeline(p)


def test_dangling_consume_exchange():
    bad = WorkerSpec(
        collections={"write": "c"}, factory=_factory, consume_exchange="nope"
    )
    p = _pipeline({"w": bad}, {"jobs": RMQExchangeConfig(name="jobs", type="fanout")})
    with pytest.raises(PipelineValidationError, match="consume_exchange"):
        validate_pipeline(p)


def test_topic_consumer_requires_binding_key():
    bad = WorkerSpec(
        collections={"write": "c"},
        factory=_factory,
        consume_exchange="docs",  # topic, but no binding_key
    )
    p = _pipeline(
        {"w": bad},
        {"docs": RMQExchangeConfig(name="docs", type="topic")},
        default_exchange="docs",
    )
    with pytest.raises(PipelineValidationError, match="requires a binding_key"):
        validate_pipeline(p)


def test_fanout_consumer_forbids_binding_key():
    bad = WorkerSpec(
        collections={"write": "c"},
        factory=_factory,
        consume_exchange="jobs",
        binding_key="oops",
    )
    p = _pipeline({"w": bad}, {"jobs": RMQExchangeConfig(name="jobs", type="fanout")})
    with pytest.raises(PipelineValidationError, match="binding_key must be None"):
        validate_pipeline(p)


def test_topic_publish_requires_route():
    bad = WorkerSpec(
        collections={"write": "c"},
        factory=_factory,
        consume_exchange="docs",
        binding_key="x",
        publish=[PublishSpec(exchange="docs")],  # no route on a topic exchange
    )
    p = _pipeline(
        {"w": bad},
        {"docs": RMQExchangeConfig(name="docs", type="topic")},
        default_exchange="docs",
    )
    with pytest.raises(PipelineValidationError, match="requires a route"):
        validate_pipeline(p)


def test_dangling_publish_exchange():
    bad = WorkerSpec(
        collections={"write": "c"},
        factory=_factory,
        consume_exchange="jobs",
        publish=[PublishSpec(exchange="ghost")],
    )
    p = _pipeline({"w": bad}, {"jobs": RMQExchangeConfig(name="jobs", type="fanout")})
    with pytest.raises(PipelineValidationError, match="publish exchange"):
        validate_pipeline(p)
