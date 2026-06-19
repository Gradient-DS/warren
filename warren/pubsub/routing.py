"""
Content-based routing helpers for direct/topic exchanges.

A fanout exchange ignores routing keys — every bound queue receives every
message, and workers self-select via ``should_process()``. Direct and topic
exchanges instead route each message by its *routing key*, so the broker
delivers a message only to queues whose binding matches.

``MessageFieldRouter`` is a ready-made :class:`RouteFunc`: it derives the
routing key from a field of the message body (``data_type`` by convention).
Because every message a worker publishes already carries a ``data_type``, this
lets a deployment route purely on payload kind with no extra configuration — a
worker binds its queue to the ``data_type`` values (or wildcard patterns) it
wants to consume.

It is a *convenience*, not a policy: a pipeline injects whichever ``route_func``
it wants per worker (see ``PublishSpec``). This helper depends only on
``Route`` / ``RouteFunc`` from :mod:`warren.pubsub.common`, not on ``aio_pika``.
"""

from pydantic import BaseModel

from warren.pubsub.common import Route, RouteFunc
from warren.pubsub.rabbitmq.config import RMQExchangeConfig


DATA_TYPE_FIELD = "data_type"
"""The message field used as the routing key by convention."""

ROUTING_PLAN_KEY = "routing"
"""Key under ``job_parameters`` where a :class:`RoutingPlan` is carried."""


class MessageFieldRouter:
    """A :class:`RouteFunc` that routes by a single field of the message body.

    The value of ``message[field]`` becomes the AMQP routing key, so the broker
    delivers the message only to queues whose binding matches that value.

    :param field: Message-body key to read the routing key from.
        Defaults to ``"data_type"``.
    """

    def __init__(self, field: str = DATA_TYPE_FIELD) -> None:
        self._field = field

    async def __call__(self, message: dict) -> list[Route]:
        key = message.get(self._field)
        if not key:
            msg = (
                f"Cannot route message: missing or empty '{self._field}' field. "
                "Topic/direct routing derives the routing key from this field; "
                "ensure every published message sets it."
            )
            raise ValueError(msg)
        return [Route(key=str(key))]


class RoutingPlan(BaseModel):
    """A job-defined routing tree over worker-type (capability) ids.

    Carried in ``job_parameters[ROUTING_PLAN_KEY]`` and propagated through the
    message chain, so each job can take its own path through the same deployed
    workers. A **tree** (each node has one parent): fan-out is supported (a node
    with multiple successors), fan-in/join is not.

    :param entry: Worker-type id(s) the document enters at.
    :param edges: ``producer-id -> successor-id(s)``; ``[]`` marks a terminal
        node (routes nowhere).
    """

    entry: list[str]
    edges: dict[str, list[str]]


class RoutingPlanRouter:
    """A :class:`RouteFunc` for job-defined *addressed* routing (direct exchange).

    Reads the :class:`RoutingPlan` from ``message['job_parameters'][plan_key]``,
    looks up the producing node via ``message['origin']['type']`` (the
    worker-type id), and emits one :class:`Route` per successor. When the origin
    is not a node in the plan — e.g. the initial publish from a publisher — it
    routes to the plan's ``entry`` nodes.

    :param plan_key: Key under ``job_parameters`` holding the plan.
    """

    def __init__(self, plan_key: str = ROUTING_PLAN_KEY) -> None:
        self._plan_key = plan_key

    async def __call__(self, message: dict) -> list[Route]:
        params = message.get("job_parameters") or {}
        raw = params.get(self._plan_key)
        if not raw:
            msg = (
                f"Cannot route message: no routing plan at "
                f"job_parameters['{self._plan_key}']. Set it at job submission."
            )
            raise ValueError(msg)
        plan = raw if isinstance(raw, RoutingPlan) else RoutingPlan.model_validate(raw)

        current = (message.get("origin") or {}).get("type")
        keys = plan.edges.get(current, plan.entry)
        return [Route(key=key) for key in keys]


def observer_binding_key(exchange: RMQExchangeConfig) -> str | None:
    """Binding key for a support worker that observes *all* messages on ``exchange``.

    Support workers (job-status, retry, publication) watch the whole pipeline.
    On ``fanout`` they receive everything (no key). On ``topic`` they bind the
    catch-all ``#``. ``direct`` has no wildcard, so wholesale observation is not
    supported (returns ``None`` → matches nothing); observing a direct exchange
    is a later-phase concern.
    """
    return "#" if exchange.type == "topic" else None


def observer_route_func(exchange: RMQExchangeConfig) -> RouteFunc | None:
    """Route function for a support worker that *publishes* on ``exchange``.

    ``fanout`` ignores keys (``None``); ``topic``/``direct`` route by
    ``data_type`` via :class:`MessageFieldRouter`.
    """
    return MessageFieldRouter() if exchange.type != "fanout" else None
