"""In-process message broker with RabbitMQ exchange semantics.

Exchanges are not declared: an exchange exists once something binds to it.
A message published to an exchange with no matching binding is dropped,
which is what RabbitMQ does, so everything that consumes must be bound
before anything publishes.
"""

import asyncio
from dataclasses import dataclass

from warren.pubsub.rabbitmq.config import RMQExchangeConfig


@dataclass(frozen=True)
class Delivery:
    """One message as a consumer receives it.

    :param body: Serialised message. Bytes, not a dict, so every bound
        queue gets an independent copy once decoded.
    :param routing_key: The key the message was published with.
    """

    body: bytes
    routing_key: str


@dataclass(frozen=True)
class _Binding:
    queue_name: str
    binding_key: str


def topic_matches(pattern: str, key: str) -> bool:
    """AMQP topic match: ``*`` is exactly one word, ``#`` is zero or more."""
    return _words_match(pattern.split("."), key.split("."))


def _words_match(pattern: list[str], key: list[str]) -> bool:
    if not pattern:
        return not key
    head, rest = pattern[0], pattern[1:]
    if head == "#":
        return any(_words_match(rest, key[i:]) for i in range(len(key) + 1))
    if not key:
        return False
    if head in ("*", key[0]):
        return _words_match(rest, key[1:])
    return False


class MemoryBroker:
    """Routes published messages to the queues bound to an exchange."""

    def __init__(self) -> None:
        self._bindings: dict[str, list[_Binding]] = {}
        self._queues: dict[str, asyncio.Queue[Delivery]] = {}

    def bind(
        self,
        exchange: RMQExchangeConfig,
        queue_name: str,
        binding_key: str | None = None,
    ) -> asyncio.Queue[Delivery]:
        """Bind ``queue_name`` to ``exchange`` and return the queue.

        Idempotent. Binding an existing queue name returns the same queue,
        so several consumers of one worker type compete for its messages.
        """
        queue = self._queues.get(queue_name)
        if queue is None:
            queue = asyncio.Queue()
            self._queues[queue_name] = queue

        binding = _Binding(queue_name, binding_key or "")
        bindings = self._bindings.setdefault(exchange.name, [])
        if binding not in bindings:
            bindings.append(binding)
        return queue

    def publish(
        self,
        exchange: RMQExchangeConfig,
        routing_key: str,
        body: bytes,
    ) -> int:
        """Deliver ``body`` to every matching queue; return how many."""
        delivered: set[str] = set()
        for binding in self._bindings.get(exchange.name, []):
            if binding.queue_name in delivered:
                continue
            if _routes(exchange.type, binding.binding_key, routing_key):
                self._queues[binding.queue_name].put_nowait(
                    Delivery(body=body, routing_key=routing_key)
                )
                delivered.add(binding.queue_name)
        return len(delivered)


def _routes(exchange_type: str, binding_key: str, routing_key: str) -> bool:
    if exchange_type == "fanout":
        return True
    if exchange_type == "direct":
        return binding_key == routing_key
    return topic_matches(binding_key, routing_key)
