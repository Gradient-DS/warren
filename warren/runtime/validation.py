"""
Deploy-time validation for a ``PipelineSpec``.

``validate_pipeline`` is a cheap, fail-fast check run at launcher startup (and
available as a standalone CLI) to catch topology mistakes before any worker
connects to infrastructure: binding/route settings that don't match the
exchange type (e.g. a topic worker with no binding_key).

Scope (see warren/docs/routing.md): this validates binding/route *presence*,
not *reachability*. It does NOT verify that a published routing key actually
reaches a consumer — dynamic ``route_func`` keys cannot be enumerated
statically. ``validate_routing_plan`` covers nominal (`produces ∈ accepts`)
compatibility for a job's routing plan at submission time.
"""

import logging

from warren.exceptions import WarrenError
from warren.pubsub.routing import RoutingPlan
from warren.runtime.spec import PipelineSpec


class PipelineValidationError(WarrenError):
    """A ``PipelineSpec`` is internally inconsistent. Carries all findings."""


class RoutingPlanValidationError(WarrenError):
    """A job's ``RoutingPlan`` is invalid for the deployed pipeline."""


# worker-type -> (accepts, produces)
CapabilityRegistry = dict[str, tuple[frozenset[str], str | None]]


def validate_pipeline(
    pipeline: PipelineSpec,
    *,
    logger: logging.Logger | None = None,
) -> None:
    """Validate a pipeline's exchange wiring; raise on any error.

    :param pipeline: the pipeline to validate.
    :param logger: optional logger for the success / not-yet-validated note.
    :raises PipelineValidationError: with every problem found, not just the
        first.
    """
    is_fanout = pipeline.exchange.type == "fanout"
    errors: list[str] = []

    for worker_type, spec in pipeline.workers.items():
        # Consume-side binding key.
        if is_fanout:
            if spec.binding_key is not None:
                errors.append(
                    f"worker '{worker_type}': fanout exchange ignores routing "
                    f"keys, so binding_key must be None (got '{spec.binding_key}')"
                )
        elif not spec.binding_key:
            errors.append(
                f"worker '{worker_type}': {pipeline.exchange.type} exchange "
                f"requires a binding_key"
            )

        # Publish-side route.
        publish = spec.publish
        if publish is None:
            continue
        has_route = publish.route is not None or publish.route_func is not None
        if is_fanout:
            if publish.route is not None:
                errors.append(
                    f"worker '{worker_type}': fanout exchange ignores routing "
                    f"keys, so publish route must be unset"
                )
        elif not has_route:
            errors.append(
                f"worker '{worker_type}': {pipeline.exchange.type} exchange "
                f"requires a publish route or route_func"
            )

    if errors:
        bullets = "\n  - ".join(errors)
        msg = f"Invalid pipeline ({len(errors)} problem(s)):\n  - {bullets}"
        raise PipelineValidationError(msg)

    if logger is not None:
        logger.info(
            "Pipeline validation passed (binding/route presence). "
            "Note: route reachability is not yet validated."
        )


def build_capability_registry(pipeline: PipelineSpec) -> CapabilityRegistry:
    """Map each worker type to its declared ``(accepts, produces)`` capability."""
    return {
        worker_type: (spec.accepts, spec.produces)
        for worker_type, spec in pipeline.workers.items()
    }


def validate_routing_plan(
    plan: RoutingPlan,
    registry: CapabilityRegistry,
    *,
    entry_data_type: str | None = None,
) -> None:
    """Validate a job's ``RoutingPlan`` against deployed worker capabilities.

    Run at job-submission time, before any message is published.

    Checks: every node maps to a deployed worker; every edge ``u -> v`` is
    nominally type-compatible (``produces[u] in accepts[v]``); and — if given —
    every entry node accepts ``entry_data_type``.

    :raises RoutingPlanValidationError: with every problem found.
    """
    errors: list[str] = []

    nodes = (
        set(plan.entry)
        | set(plan.edges)
        | {succ for succs in plan.edges.values() for succ in succs}
    )
    for node in sorted(nodes):
        if node not in registry:
            errors.append(
                f"routing node '{node}' is not a deployed worker type "
                f"{sorted(registry)}"
            )

    for producer, successors in plan.edges.items():
        if producer not in registry:
            continue
        produces = registry[producer][1]
        for consumer in successors:
            if consumer not in registry:
                continue
            accepts = registry[consumer][0]
            if produces not in accepts:
                errors.append(
                    f"edge {producer} -> {consumer}: producer produces "
                    f"'{produces}', not in consumer accepts {sorted(accepts)}"
                )

    if entry_data_type is not None:
        for entry in plan.entry:
            if entry in registry and entry_data_type not in registry[entry][0]:
                errors.append(
                    f"entry '{entry}' does not accept the submitted data_type "
                    f"'{entry_data_type}' (accepts {sorted(registry[entry][0])})"
                )

    if errors:
        bullets = "\n  - ".join(errors)
        msg = f"Invalid routing plan ({len(errors)} problem(s)):\n  - {bullets}"
        raise RoutingPlanValidationError(msg)
