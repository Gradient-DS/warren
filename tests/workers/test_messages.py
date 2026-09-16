"""Pins that transport-level fields on a message body stay on that hop."""

from warren.pubsub.routing import DELIVERY_COUNT_FIELD
from warren.workers.messages import MessageOrigin, ProcessingMessage


def test_delivery_count_is_not_copied_downstream() -> None:
    incoming = ProcessingMessage.create_from(
        {
            "data_type": "raw_document",
            "data": {"doc_id": "d"},
            "job_id": "j",
            "origin": {"type": "api", "name": "api-1"},
            DELIVERY_COUNT_FIELD: 3,
        }
    )

    derived = incoming.derive(
        data_type="parsed", data={}, origin=MessageOrigin(type="parser", name="p-1")
    )

    assert DELIVERY_COUNT_FIELD not in derived.to_dict()
