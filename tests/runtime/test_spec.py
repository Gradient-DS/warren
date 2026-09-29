from dataclasses import asdict, dataclass, replace

import pytest

from warren.common import MessageConsumerInterface
from warren.pubsub.rabbitmq.config import RMQExchangeConfig
from warren.pubsub.routing import MessageFieldRouter
from warren.runtime.spec import (
    PipelineSpec,
    PublishSpec,
    WorkerFactoryContext,
    WorkerSpec,
)


async def factory(ctx: WorkerFactoryContext) -> MessageConsumerInterface:
    raise NotImplementedError


def test_pipeline_publication_defaults_to_none() -> None:
    pipeline = PipelineSpec({}, RMQExchangeConfig(name="x"), [], "c", "c", "done")
    assert pipeline.publication is None


def test_pipeline_accepts_publication_spec() -> None:
    publication = PublishSpec(route_func=MessageFieldRouter())
    pipeline = PipelineSpec(
        workers={},
        exchange=RMQExchangeConfig(name="x", type="topic"),
        result_collections=[],
        reference_collection="c",
        completion_collection="c",
        final_data_type="done",
        publication=publication,
    )
    assert pipeline.publication is publication
    assert replace(pipeline, final_data_type="output").publication is publication


def test_binding_key_constructor_alias() -> None:
    spec = WorkerSpec({}, factory, "input.*")
    assert spec.binding_keys == ("input.*",)
    assert "binding_key" not in asdict(spec)
    assert replace(spec, produces="output").binding_keys == ("input.*",)


def test_binding_keys_constructor() -> None:
    spec = WorkerSpec({}, factory, binding_keys=("input.*", "retry.#"))
    assert spec.binding_keys == ("input.*", "retry.#")
    assert WorkerSpec({}, factory).binding_keys == ()


def test_binding_key_alias_rejects_conflict() -> None:
    with pytest.raises(ValueError, match="either binding_key or binding_keys"):
        WorkerSpec({}, factory, binding_key="a", binding_keys=("b",))


def test_binding_key_alias_inherited_by_specs() -> None:
    @dataclass(frozen=True)
    class ExtendedSpec(WorkerSpec):
        enabled: bool = True

    assert ExtendedSpec({}, factory, binding_key="input").binding_keys == ("input",)
