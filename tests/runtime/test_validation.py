"""Unit tests for deploy-time pipeline validation (no broker needed)."""

from dataclasses import replace

import pytest

from warren.pubsub.common import Route
from warren.pubsub.rabbitmq.config import RMQExchangeConfig
from warren.pubsub.routing import MessageFieldRouter, RoutingPlan
from warren.runtime.spec import PipelineSpec, PublishSpec, WorkerSpec
from warren.runtime.validation import (
    PipelineValidationError,
    RoutingPlanValidationError,
    validate_pipeline,
    validate_routing_plan,
)


async def _factory(ctx):  # pragma: no cover - never called by validation
    raise NotImplementedError


def _pipeline(worker, exchange_type="fanout"):
    return PipelineSpec(
        workers={"w": worker},
        exchange=RMQExchangeConfig(name="x", type=exchange_type),
        result_collections=["c"],
        reference_collection="c",
        completion_collection="c",
        final_data_type="done",
    )


def test_valid_fanout_pipeline_passes():
    worker = WorkerSpec(
        collections={"write": "c"}, factory=_factory, publish=PublishSpec()
    )
    validate_pipeline(_pipeline(worker))


def test_valid_topic_pipeline_passes():
    worker = WorkerSpec(
        collections={"write": "c"},
        factory=_factory,
        binding_key="pdf_document",
        publish=PublishSpec(route_func=MessageFieldRouter()),
    )
    validate_pipeline(_pipeline(worker, exchange_type="topic"))


def test_topic_consumer_requires_binding_key():
    worker = WorkerSpec(collections={"write": "c"}, factory=_factory)  # no binding_key
    with pytest.raises(
        PipelineValidationError, match="requires non-empty binding_keys"
    ):
        validate_pipeline(_pipeline(worker, exchange_type="topic"))


def test_fanout_consumer_ignores_binding_key():
    worker = WorkerSpec(
        collections={"write": "c"}, factory=_factory, binding_key="oops"
    )
    validate_pipeline(_pipeline(worker))


def test_topic_publish_requires_route():
    worker = WorkerSpec(
        collections={"write": "c"},
        factory=_factory,
        binding_key="x",
        publish=PublishSpec(),  # no route on a topic exchange
    )
    with pytest.raises(PipelineValidationError, match="requires a publish route"):
        validate_pipeline(_pipeline(worker, exchange_type="topic"))


def test_fanout_publish_forbids_route():
    worker = WorkerSpec(
        collections={"write": "c"},
        factory=_factory,
        publish=PublishSpec(route=Route(key="nope")),
    )
    with pytest.raises(PipelineValidationError, match="publish route must be unset"):
        validate_pipeline(_pipeline(worker))


@pytest.mark.parametrize("exchange_type", ["fanout", "topic", "direct"])
@pytest.mark.parametrize(
    "publication", [None, PublishSpec(route_func=MessageFieldRouter())]
)
def test_publication_default_and_custom_routing_pass(exchange_type, publication):
    pipeline = replace(
        _pipeline(WorkerSpec({}, _factory), exchange_type),
        workers={},
        publication=publication,
    )
    validate_pipeline(pipeline)


@pytest.mark.parametrize("exchange_type", ["fanout", "topic", "direct"])
def test_publication_static_route_is_rejected(exchange_type):
    pipeline = replace(
        _pipeline(WorkerSpec({}, _factory), exchange_type),
        workers={},
        publication=PublishSpec(route=Route("input")),
    )
    with pytest.raises(PipelineValidationError, match="publication: static route"):
        validate_pipeline(pipeline)


@pytest.mark.parametrize("exchange_type", ["fanout", "topic", "direct"])
def test_empty_publication_spec_requires_router_unless_fanout(exchange_type):
    pipeline = replace(
        _pipeline(WorkerSpec({}, _factory), exchange_type),
        workers={},
        publication=PublishSpec(),
    )
    if exchange_type == "fanout":
        validate_pipeline(pipeline)
    else:
        with pytest.raises(PipelineValidationError, match=r"publication: .*route_func"):
            validate_pipeline(pipeline)


# --- validate_routing_plan (submission-time) ---

# parser→md, chunker accepts md→chunks, embedder accepts chunks→vec
_REGISTRY = {
    "parser": (frozenset({"pdf"}), "md"),
    "chunker": (frozenset({"md"}), "chunks"),
    "embedder": (frozenset({"chunks"}), "vec"),
}


def test_valid_routing_plan_passes():
    plan = RoutingPlan(
        entry=["parser"],
        edges={"parser": ["chunker"], "chunker": ["embedder"], "embedder": []},
    )
    validate_routing_plan(plan, _REGISTRY, entry_data_type="pdf")


def test_routing_plan_undeployed_node():
    plan = RoutingPlan(entry=["parser"], edges={"parser": ["ghost"]})
    with pytest.raises(RoutingPlanValidationError, match="not a deployed worker"):
        validate_routing_plan(plan, _REGISTRY)


def test_routing_plan_type_incompatible_edge():
    # parser produces "md" but embedder accepts only "chunks"
    plan = RoutingPlan(entry=["parser"], edges={"parser": ["embedder"]})
    with pytest.raises(RoutingPlanValidationError, match="not in consumer accepts"):
        validate_routing_plan(plan, _REGISTRY)


def test_routing_plan_entry_rejects_input_type():
    plan = RoutingPlan(entry=["parser"], edges={"parser": ["chunker"]})
    with pytest.raises(RoutingPlanValidationError, match="does not accept"):
        validate_routing_plan(plan, _REGISTRY, entry_data_type="png")


@pytest.mark.parametrize("keys", [("input.*", "retry.#"), ("input", "retry")])
def test_multiple_binding_keys_pass_validation(keys: tuple[str, ...]) -> None:
    worker = WorkerSpec(collections={}, factory=_factory, binding_keys=keys)
    validate_pipeline(_pipeline(worker, exchange_type="topic"))


def test_empty_binding_key_is_rejected() -> None:
    worker = WorkerSpec(collections={}, factory=_factory, binding_keys=("input.*", ""))
    with pytest.raises(PipelineValidationError, match="non-empty binding_keys"):
        validate_pipeline(_pipeline(worker, exchange_type="topic"))
