"""Unit tests for ``RMQConsumerManager`` — the dead-channel rule.

A delivery whose channel died (reconnect, channel-level close) is no longer
ours: nothing is published for it, nothing is settled, one log line says so,
and the broker's own requeue redelivers it. These tests pin that rule at
every settle site and pin that a task exception is logged rather than lost.
"""

import asyncio

import pytest

from warren.common import HardFailureException, SoftFailureException
from warren.pubsub.common import PublishFailureException
from warren.pubsub.rabbitmq.config import RMQConsumerConfig
from warren.pubsub.routing import DELIVERY_COUNT_FIELD

from .fakes import (
    BODY,
    FakeIncomingMessage,
    FakePublisher,
    FakeWorker,
    make_manager,
    process,
)


# ---------------------------------------------------------------------------
# Dead channel at each settle site
# ---------------------------------------------------------------------------


def test_success_with_dead_channel_publishes_nothing(
    caplog: pytest.LogCaptureFixture,
) -> None:
    data, control = FakePublisher(), FakePublisher()
    worker = FakeWorker(result={"data_type": "parsed"})
    manager = make_manager(worker, data_publisher=data, control_publisher=control)
    message = FakeIncomingMessage(closed=True)

    process(manager, message)

    assert worker.calls == [BODY]
    assert data.published == []
    assert control.published == []  # no spurious hard-failure envelope
    assert not message.settled
    assert "result not published, ack skipped" in caplog.text


def test_ack_failing_after_publish_is_logged_not_hard_failed(
    caplog: pytest.LogCaptureFixture,
) -> None:
    message = FakeIncomingMessage()
    # The channel dies while the result is being published.
    data = FakePublisher(side_effect=lambda: setattr(message, "closed", True))
    control = FakePublisher()
    manager = make_manager(
        FakeWorker(result={"data_type": "parsed"}),
        data_publisher=data,
        control_publisher=control,
    )

    process(manager, message)

    assert len(data.published) == 1
    assert control.published == []
    assert not message.acked
    assert "Could not ack delivery" in caplog.text


def test_hard_failure_with_dead_channel_skips_envelope(
    caplog: pytest.LogCaptureFixture,
) -> None:
    control = FakePublisher()
    manager = make_manager(
        FakeWorker(error=HardFailureException("bad")), control_publisher=control
    )
    message = FakeIncomingMessage(closed=True)

    process(manager, message)

    assert control.published == []
    assert not message.settled
    assert "failure not recorded" in caplog.text


def test_soft_failure_with_dead_channel_skips_envelope(
    caplog: pytest.LogCaptureFixture,
) -> None:
    control = FakePublisher()
    manager = make_manager(
        FakeWorker(error=SoftFailureException("later")), control_publisher=control
    )
    message = FakeIncomingMessage(closed=True)

    process(manager, message)

    assert control.published == []
    assert not message.settled
    assert "failure not recorded" in caplog.text


def test_publish_failure_with_dead_channel_skips_requeue(
    caplog: pytest.LogCaptureFixture,
) -> None:
    data = FakePublisher(error=PublishFailureException("broker gone"))
    manager = make_manager(
        FakeWorker(result={"data_type": "parsed"}), data_publisher=data
    )
    message = FakeIncomingMessage()
    data.side_effect = lambda: setattr(message, "closed", True)

    process(manager, message)

    assert not message.settled
    assert "Delivery channel closed" in caplog.text


def test_undecodable_body_on_dead_channel_is_logged(
    caplog: pytest.LogCaptureFixture,
) -> None:
    manager = make_manager(FakeWorker())
    message = FakeIncomingMessage(b"not json {", closed=True)

    process(manager, message)

    assert not message.settled
    assert "Could not reject delivery" in caplog.text


def test_shutdown_nack_on_dead_channel_is_logged(
    caplog: pytest.LogCaptureFixture,
) -> None:
    manager = make_manager(FakeWorker())
    manager._shutting_down = True
    message = FakeIncomingMessage(closed=True)

    asyncio.run(manager._on_message(message))  # type: ignore[arg-type]

    assert not message.settled
    assert "Could not requeue delivery" in caplog.text


# ---------------------------------------------------------------------------
# Live channel: today's matrix still holds
# ---------------------------------------------------------------------------


def test_success_publishes_and_acks() -> None:
    data = FakePublisher()
    manager = make_manager(
        FakeWorker(result={"data_type": "parsed"}), data_publisher=data
    )
    message = FakeIncomingMessage()

    process(manager, message)

    assert data.published == [{"data_type": "parsed"}]
    assert message.acked


def test_hard_failure_publishes_envelope_and_rejects() -> None:
    control = FakePublisher()
    manager = make_manager(
        FakeWorker(error=HardFailureException("bad")), control_publisher=control
    )
    message = FakeIncomingMessage()

    process(manager, message)

    assert control.published[0]["data_type"] == "hard-failure"
    assert message.rejected is False


# ---------------------------------------------------------------------------
# Task exceptions and internal cancellation
# ---------------------------------------------------------------------------


def test_message_task_exception_is_logged(caplog: pytest.LogCaptureFixture) -> None:
    """A non-transport error escaping the task must reach the log, not GC."""

    class _Exploding(FakeIncomingMessage):
        @property
        def channel(self) -> object:
            msg = "not a transport error"
            raise KeyError(msg)

    manager = make_manager(FakeWorker(result=None))

    async def scenario() -> None:
        await manager._on_message(_Exploding())  # type: ignore[arg-type]
        await asyncio.gather(*manager._in_flight_tasks, return_exceptions=True)
        await asyncio.sleep(0)  # let the done-callback run

    asyncio.run(scenario())

    assert "Unhandled error in message task" in caplog.text
    assert manager._in_flight_tasks == set()


def test_settle_absorbs_internal_cancellation(caplog: pytest.LogCaptureFixture) -> None:
    """aiormq cancels the drain future when the channel closes mid-ack; that
    CancelledError is not a cancellation of *our* task and is logged."""
    manager = make_manager(FakeWorker())
    message = FakeIncomingMessage(settle_error=asyncio.CancelledError())

    ok = asyncio.run(manager._settle(message, "ack", identity="x"))  # type: ignore[arg-type]

    assert ok is False
    assert "interrupted by a channel close" in caplog.text


def test_settle_reraises_real_cancellation() -> None:
    manager = make_manager(FakeWorker())
    message = FakeIncomingMessage(settle_error=asyncio.CancelledError())

    async def scenario() -> None:
        asyncio.current_task().cancel()
        await manager._settle(message, "ack", identity="x")  # type: ignore[arg-type]

    with pytest.raises(asyncio.CancelledError):
        asyncio.run(scenario())


# ---------------------------------------------------------------------------
# Delivery counting (poison messages)
# ---------------------------------------------------------------------------


def _counting_manager(
    worker: FakeWorker, control: FakePublisher | None, max_deliveries: int = 3
):
    return make_manager(
        worker,
        control_publisher=control,
        consumer_config=RMQConsumerConfig(
            max_deliveries=max_deliveries, redelivery_delay=5
        ),
    )


def test_first_delivery_is_processed_normally() -> None:
    worker = FakeWorker(result=None)
    manager = _counting_manager(worker, FakePublisher())
    message = FakeIncomingMessage(redelivered=False)

    process(manager, message)

    assert worker.calls == [BODY]
    assert message.acked


def test_redelivery_is_deferred_with_count() -> None:
    worker, control = FakeWorker(result=None), FakePublisher()
    manager = _counting_manager(worker, control)
    message = FakeIncomingMessage(redelivered=True)

    process(manager, message)

    assert worker.calls == []
    envelope = control.published[0]
    assert envelope["data_type"] == "soft-failure"
    assert envelope["data"][DELIVERY_COUNT_FIELD] == 2  # first delivery counted as 1
    assert envelope["data"]["retry"]["after"] == 5
    assert envelope["data"]["retry"]["count"] == 0  # no retry slot consumed
    assert message.acked


def test_redelivery_over_limit_is_hard_failure() -> None:
    worker, control = FakeWorker(result=None), FakePublisher()
    manager = _counting_manager(worker, control, max_deliveries=3)
    message = FakeIncomingMessage({**BODY, DELIVERY_COUNT_FIELD: 3}, redelivered=True)

    process(manager, message)

    assert worker.calls == []
    envelope = control.published[0]
    assert envelope["data_type"] == "hard-failure"
    assert "delivered 4 times" in envelope["error"]
    assert message.rejected is False


def test_redelivery_without_control_publisher_is_processed(
    caplog: pytest.LogCaptureFixture,
) -> None:
    worker = FakeWorker(result=None)
    manager = _counting_manager(worker, None)
    message = FakeIncomingMessage(redelivered=True)

    process(manager, message)

    assert worker.calls == [BODY]
    assert "no control publisher to count it" in caplog.text


def test_max_deliveries_none_ignores_redelivered() -> None:
    worker = FakeWorker(result=None)
    manager = make_manager(worker, control_publisher=FakePublisher())
    message = FakeIncomingMessage(redelivered=True)

    process(manager, message)

    assert worker.calls == [BODY]
    assert message.acked


def test_redelivery_delay_below_one_is_rejected() -> None:
    with pytest.raises(ValueError, match="redelivery_delay"):
        RMQConsumerConfig(redelivery_delay=0)
