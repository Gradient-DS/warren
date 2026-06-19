"""Unit tests for CapabilityWorkerBase (no broker needed)."""

import asyncio

from warren.workers.workers import CapabilityWorkerBase


class _Echo(CapabilityWorkerBase):
    async def process(self, message: dict) -> dict | None:
        return {"data_type": self.produces, "echoed": message["data"]}


def _worker():
    return _Echo(
        worker_name="echo-1",
        accepts={"md", "html"},
        produces="chunks",
        worker_type="chunker",
    )


def test_capabilities_exposed():
    w = _worker()
    assert w.accepts == frozenset({"md", "html"})
    assert w.produces == "chunks"
    assert w.type == "chunker"  # worker_type override -> matches the spec key


def test_should_process_derives_from_accepts():
    w = _worker()
    assert w.should_process({"data_type": "md"}) is True
    assert w.should_process({"data_type": "pdf"}) is False
    assert w.should_process({}) is False


def test_call_skips_non_accepted_and_processes_accepted():
    w = _worker()
    assert asyncio.run(w({"data_type": "pdf", "data": 1})) is None
    out = asyncio.run(w({"data_type": "md", "data": 42}))
    assert out == {"data_type": "chunks", "echoed": 42}
