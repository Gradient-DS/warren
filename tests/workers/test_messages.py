"""Message inheritance, serialization, and transport-only fields."""

from typing import Any

import pytest

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


@pytest.mark.parametrize("priority", [None, 0, 2, 255])
@pytest.mark.parametrize("lane", [None, "interactive", "bulk"])
def test_lane_and_priority_round_trip_and_derive(
    lane: str | None, priority: int | None
) -> None:
    parent = ProcessingMessage(
        data_type="raw_document",
        data={},
        job_id="j",
        origin=MessageOrigin("api", "api-1"),
        lane=lane,
        priority=priority,
    )
    body = parent.to_dict()
    assert ("lane" in body) == (lane is not None)
    assert ("priority" in body) == (priority is not None)
    assert ProcessingMessage.create_from(body) == parent
    child = ProcessingMessage.create_from(body).derive(
        data_type="parsed", data={}, origin=MessageOrigin("parser", "p-1")
    )
    assert child.lane == lane
    assert child.priority == priority


@pytest.mark.parametrize("priority", [-1, 256, True, False, 1.5, "2"])
@pytest.mark.parametrize("from_dict", [False, True])
def test_invalid_priority(priority: Any, from_dict: bool) -> None:
    body = {
        "data_type": "raw_document",
        "data": {},
        "job_id": "j",
        "origin": {"type": "api", "name": "api-1"},
        "priority": priority,
    }
    if from_dict:
        with pytest.raises(ValueError, match=r"priority must be an int in 0\.\.255"):
            ProcessingMessage.create_from(body)
    else:
        with pytest.raises(ValueError, match=r"priority must be an int in 0\.\.255"):
            ProcessingMessage(**{**body, "origin": MessageOrigin("api", "api-1")})
