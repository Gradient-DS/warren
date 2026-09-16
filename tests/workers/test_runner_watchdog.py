"""Pins the runner watchdog: only a consumer lost with a live connection,
for longer than the grace period, ends the process; blocked and
reconnecting states are logged and waited out; a failing health() is
logged and ignored. Intervals are shrunk so each test runs in
milliseconds."""

import asyncio
import signal

import pytest

from warren.exceptions import WarrenError
from warren.pubsub.common import ConsumerHealth
from warren.workers.health import HealthConfig
from warren.workers.runners import WorkerRunnerBase


READY = ConsumerHealth(
    connected=True, blocked=False, channel_open=True, consumer_registered=True
)
LOST = ConsumerHealth(
    connected=True,
    blocked=False,
    channel_open=False,
    consumer_registered=None,
    detail="closed",
)
BLOCKED = ConsumerHealth(
    connected=True, blocked=True, channel_open=True, consumer_registered=True
)
DOWN = ConsumerHealth(
    connected=False, blocked=False, channel_open=False, consumer_registered=None
)


class _ScriptedManager:
    """Returns the scripted samples in order, then repeats the last one."""

    def __init__(self, samples: list, *, error: Exception | None = None) -> None:
        self.samples = list(samples)
        self.error = error
        self.stopped = False

    async def setup(self) -> None:
        pass

    async def start_consuming(self) -> None:
        pass

    async def stop_consuming(self) -> None:
        self.stopped = True

    async def health(self, *, probe_timeout: float = 1.0) -> ConsumerHealth:
        if self.error is not None:
            raise self.error
        if len(self.samples) > 1:
            return self.samples.pop(0)
        return self.samples[0]


class _Runner(WorkerRunnerBase):
    def __init__(self, manager: _ScriptedManager, health: HealthConfig) -> None:
        super().__init__(name="test", health=health)
        self._manager = manager

    async def setup(self) -> None:
        self._consumer_manager = self._manager
        self._mark_setup_succeeded()


def _config(grace: float = 0.05) -> HealthConfig:
    return HealthConfig(
        enabled=False,
        check_interval_s=0.01,
        probe_timeout_s=0.01,
        consumer_lost_grace_s=grace,
    )


def _watch(runner: _Runner, *, for_s: float) -> asyncio.Event:
    """Run the watchdog for a bounded time; return its shutdown event."""
    event = asyncio.Event()

    async def scenario() -> None:
        await runner.setup()
        task = asyncio.create_task(runner._watch_health(event))
        try:
            await asyncio.wait_for(asyncio.shield(task), timeout=for_s)
        except TimeoutError:
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task

    asyncio.run(scenario())
    return event


@pytest.fixture
def _restore_signals():
    saved = {s: signal.getsignal(s) for s in (signal.SIGINT, signal.SIGTERM)}
    yield
    for sig, handler in saved.items():
        signal.signal(sig, handler)


def test_consumer_lost_past_grace_sets_shutdown_and_fatal(
    caplog: pytest.LogCaptureFixture,
) -> None:
    runner = _Runner(_ScriptedManager([LOST]), _config(grace=0.03))

    event = _watch(runner, for_s=1.0)

    assert event.is_set()
    assert "Consumer lost for 0.03s" in runner._fatal_health
    assert "exiting so the orchestrator restarts this worker" in caplog.text


def test_consumer_lost_within_grace_does_not_exit() -> None:
    runner = _Runner(_ScriptedManager([LOST]), _config(grace=10.0))

    event = _watch(runner, for_s=0.1)

    assert not event.is_set()
    assert runner._fatal_health is None


def test_recovery_inside_grace_resets_the_clock() -> None:
    runner = _Runner(_ScriptedManager([LOST, LOST, READY, LOST]), _config(grace=0.035))

    event = _watch(runner, for_s=0.06)

    assert not event.is_set()


def test_blocked_is_logged_once_and_never_exits(
    caplog: pytest.LogCaptureFixture,
) -> None:
    runner = _Runner(_ScriptedManager([BLOCKED]), _config(grace=0.0))

    event = _watch(runner, for_s=0.1)

    assert not event.is_set()
    assert caplog.text.count("Consumer state: blocked") == 1


def test_reconnecting_never_exits() -> None:
    runner = _Runner(_ScriptedManager([DOWN]), _config(grace=0.0))

    assert not _watch(runner, for_s=0.1).is_set()


def test_health_error_is_logged_and_ignored(caplog: pytest.LogCaptureFixture) -> None:
    runner = _Runner(
        _ScriptedManager([READY], error=RuntimeError("boom")), _config(grace=0.0)
    )

    event = _watch(runner, for_s=0.05)

    assert not event.is_set()
    assert "Health check failed" in caplog.text


@pytest.mark.usefixtures("_restore_signals")
def test_run_raises_when_watchdog_declares_fatal() -> None:
    manager = _ScriptedManager([LOST])
    runner = _Runner(manager, _config(grace=0.02))

    async def scenario() -> None:
        await runner.setup()
        await runner.run()

    with pytest.raises(WarrenError, match="Consumer lost"):
        asyncio.run(asyncio.wait_for(scenario(), timeout=2.0))
