"""Unit tests for the routing helpers (no broker needed)."""

import asyncio

import pytest

from warren.pubsub.rabbitmq.config import RMQExchangeConfig
from warren.pubsub.routing import (
    REPLAY_ROUTING_KEY_FIELD,
    MessageFieldRouter,
    ReplayRouter,
    RoutingPlan,
    RoutingPlanRouter,
    observer_binding_key,
    observer_exchange,
    observer_route_func,
)


_PLAN = {
    "entry": ["parser"],
    "edges": {"parser": ["chunker"], "chunker": ["embedder"], "embedder": []},
}


def _msg(origin_type, plan=_PLAN):
    return {
        "data_type": "x",
        "origin": {"type": origin_type, "name": "n"},
        "job_parameters": {"routing": plan},
    }


def test_message_field_router_routes_by_data_type():
    routes = asyncio.run(MessageFieldRouter()({"data_type": "markdown_document"}))
    assert [r.key for r in routes] == ["markdown_document"]


def test_message_field_router_custom_field():
    routes = asyncio.run(
        MessageFieldRouter(field="kind")({"kind": "abc", "data_type": "ignored"})
    )
    assert [r.key for r in routes] == ["abc"]


def test_message_field_router_raises_on_missing_field():
    with pytest.raises(ValueError, match="data_type"):
        asyncio.run(MessageFieldRouter()({"no_data_type": "x"}))


def test_observer_exchange_fanout_and_topic_observe_in_place():
    fan = RMQExchangeConfig(name="jobs", type="fanout")
    top = RMQExchangeConfig(name="docs", type="topic")
    assert observer_exchange(fan) is fan  # same object — observed in place
    assert observer_exchange(top) is top


def test_observer_exchange_direct_derives_fanout():
    obs = observer_exchange(RMQExchangeConfig(name="route", type="direct"))
    assert obs.name == "route.observer"
    assert obs.type == "fanout"


def test_observer_binding_key_per_exchange_type():
    assert observer_binding_key(RMQExchangeConfig(name="x", type="fanout")) is None
    assert observer_binding_key(RMQExchangeConfig(name="x", type="topic")) == "#"
    # direct has no wildcard — wholesale observation unsupported (None).
    assert observer_binding_key(RMQExchangeConfig(name="x", type="direct")) is None


def test_observer_route_func_per_exchange_type():
    assert observer_route_func(RMQExchangeConfig(name="x", type="fanout")) is None
    assert isinstance(
        observer_route_func(RMQExchangeConfig(name="x", type="topic")),
        MessageFieldRouter,
    )
    assert isinstance(
        observer_route_func(RMQExchangeConfig(name="x", type="direct")),
        MessageFieldRouter,
    )


def test_routing_plan_router_unknown_origin_routes_to_entry():
    # The initial publish (publisher origin not in the plan) → entry nodes.
    routes = asyncio.run(RoutingPlanRouter()(_msg("some-publisher")))
    assert [r.key for r in routes] == ["parser"]


def test_routing_plan_router_routes_to_successors():
    routes = asyncio.run(RoutingPlanRouter()(_msg("parser")))
    assert [r.key for r in routes] == ["chunker"]


def test_routing_plan_router_terminal_node_routes_nowhere():
    routes = asyncio.run(RoutingPlanRouter()(_msg("embedder")))
    assert routes == []


def test_routing_plan_router_fan_out():
    plan = {"entry": ["a"], "edges": {"a": ["b", "c"]}}
    routes = asyncio.run(RoutingPlanRouter()(_msg("a", plan)))
    assert sorted(r.key for r in routes) == ["b", "c"]


def test_routing_plan_router_raises_without_plan():
    with pytest.raises(ValueError, match="no routing plan"):
        asyncio.run(RoutingPlanRouter()({"origin": {"type": "parser"}}))


def test_routing_plan_accepts_model_instance():
    plan = RoutingPlan(entry=["parser"], edges={"parser": ["chunker"]})
    msg = {"origin": {"type": "parser"}, "job_parameters": {"routing": plan}}
    routes = asyncio.run(RoutingPlanRouter()(msg))
    assert [r.key for r in routes] == ["chunker"]


def test_replay_router_replays_stamped_key():
    routes = asyncio.run(
        ReplayRouter()({REPLAY_ROUTING_KEY_FIELD: "markdown_document"})
    )
    assert [r.key for r in routes] == ["markdown_document"]


def test_replay_router_defaults_to_empty_key():
    # Missing stamp (e.g. fanout, which ignores keys) → "".
    routes = asyncio.run(ReplayRouter()({"data_type": "x"}))
    assert [r.key for r in routes] == [""]
