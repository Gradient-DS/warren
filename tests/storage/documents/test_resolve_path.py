"""resolve_path stays inside its base dir; the runtime registers it only if set."""

import asyncio
from pathlib import Path

import pytest

from warren.pubsub.rabbitmq.config import RMQExchangeConfig
from warren.runtime.config import RuntimeConfig
from warren.runtime.runner import DefaultWorkerRunner, create_default_resolvers
from warren.runtime.spec import WorkerSpec
from warren.storage.documents.interface import DocumentNotFoundError
from warren.storage.documents.location import DocumentPathLocation
from warren.storage.documents.resolvers import resolve_path


async def _factory(ctx):
    return None


@pytest.fixture
def layout(tmp_path: Path) -> Path:
    base = tmp_path / "base"
    base.mkdir()
    (base / "sub").mkdir()
    (base / "sub" / "doc.txt").write_bytes(b"inside")
    (tmp_path / "secret.txt").write_bytes(b"outside")
    return base


def _read(base: Path, relative_path: str) -> bytes:
    location = DocumentPathLocation(relative_path=relative_path)
    return asyncio.run(resolve_path(location, base_dir=base))


def test_reads_file_inside_base(layout: Path) -> None:
    assert _read(layout, "sub/doc.txt") == b"inside"
    assert _read(layout, "sub/../sub/doc.txt") == b"inside"


@pytest.mark.parametrize("relative_path", ["../secret.txt", "sub/../../secret.txt"])
def test_traversal_is_refused(layout: Path, relative_path: str) -> None:
    with pytest.raises(DocumentNotFoundError, match="escapes"):
        _read(layout, relative_path)


def test_absolute_path_is_refused(layout: Path) -> None:
    with pytest.raises(DocumentNotFoundError, match="escapes"):
        _read(layout, str(layout.parent / "secret.txt"))
    with pytest.raises(DocumentNotFoundError, match="escapes"):
        _read(layout, str(layout / "sub" / "doc.txt"))


def test_symlink_escape_is_refused(layout: Path) -> None:
    (layout / "link.txt").symlink_to(layout.parent / "secret.txt")
    with pytest.raises(DocumentNotFoundError, match="escapes"):
        _read(layout, "link.txt")


def test_missing_file_is_not_found(layout: Path) -> None:
    with pytest.raises(DocumentNotFoundError, match="not found"):
        _read(layout, "sub/absent.txt")


def test_path_resolver_unregistered_without_base_dir() -> None:
    assert "path" not in create_default_resolvers()


def test_path_resolver_registered_and_confined_with_base_dir(layout: Path) -> None:
    resolver = create_default_resolvers(path_base_dir=layout)["path"]
    location = DocumentPathLocation(relative_path="../secret.txt")
    with pytest.raises(DocumentNotFoundError):
        asyncio.run(resolver(location))


def test_runner_wires_path_base_dir_from_config(layout: Path) -> None:
    def runner(config: RuntimeConfig) -> DefaultWorkerRunner:
        return DefaultWorkerRunner(
            config,
            "worker-1",
            worker_type="test_worker",
            worker_spec=WorkerSpec(collections={}, factory=_factory),
            exchange=RMQExchangeConfig(name="jobs", type="fanout"),
        )

    assert "path" not in runner(RuntimeConfig())._create_resolvers()
    configured = RuntimeConfig.model_validate(
        {"documents": {"path_base_dir": str(layout)}}
    )
    resolver = runner(configured)._create_resolvers()["path"]
    location = DocumentPathLocation(relative_path="sub/doc.txt")
    assert asyncio.run(resolver(location)) == b"inside"
