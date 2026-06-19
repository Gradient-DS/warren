"""Unit tests for the routing helpers (no broker needed)."""

import asyncio

import pytest

from warren.pubsub.rabbitmq.config import RMQExchangeConfig
from warren.pubsub.routing import (
    MessageFieldRouter,
    observer_binding_key,
    observer_route_func,
)


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
