"""
Deploy-time validation for a ``PipelineSpec``.

``validate_pipeline`` is a cheap, fail-fast check run at launcher startup (and
available as a standalone CLI) to catch topology mistakes before any worker
connects to infrastructure: dangling exchange references, and binding/route
settings that don't match the exchange type.

Scope (see tasks/routing-design.md D13): this validates *references and
presence*, not *reachability*. It does NOT yet verify that a published routing
key actually reaches a consumer — static-route reachability and nominal-type
(`produces ∈ accepts`) reachability arrive with capabilities in Phase 2.
Dynamic ``route_func`` keys cannot be enumerated statically at all.
"""

import logging

from warren.exceptions import WarrenError
from warren.runtime.spec import PipelineSpec


class PipelineValidationError(WarrenError):
    """A ``PipelineSpec`` is internally inconsistent. Carries all findings."""


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
    exchanges = pipeline.exchanges
    errors: list[str] = []

    if pipeline.default_exchange not in exchanges:
        errors.append(
            f"default_exchange '{pipeline.default_exchange}' is not defined in "
            f"exchanges {sorted(exchanges)}"
        )

    for worker_type, spec in pipeline.workers.items():
        consume = exchanges.get(spec.consume_exchange)
        if consume is None:
            errors.append(
                f"worker '{worker_type}': consume_exchange "
                f"'{spec.consume_exchange}' is not defined in exchanges "
                f"{sorted(exchanges)}"
            )
        elif consume.type == "fanout":
            if spec.binding_key is not None:
                errors.append(
                    f"worker '{worker_type}': fanout exchange "
                    f"'{spec.consume_exchange}' ignores routing keys, so "
                    f"binding_key must be None (got '{spec.binding_key}')"
                )
        elif not spec.binding_key:
            errors.append(
                f"worker '{worker_type}': {consume.type} exchange "
                f"'{spec.consume_exchange}' requires a binding_key"
            )

        for target in spec.publish:
            dest = exchanges.get(target.exchange)
            if dest is None:
                errors.append(
                    f"worker '{worker_type}': publish exchange "
                    f"'{target.exchange}' is not defined in exchanges "
                    f"{sorted(exchanges)}"
                )
                continue
            has_route = target.route is not None or target.route_func is not None
            if dest.type == "fanout":
                if target.route is not None:
                    errors.append(
                        f"worker '{worker_type}': fanout exchange "
                        f"'{target.exchange}' ignores routing keys, so route "
                        f"must be unset"
                    )
            elif not has_route:
                errors.append(
                    f"worker '{worker_type}': {dest.type} exchange "
                    f"'{target.exchange}' requires a route or route_func"
                )

    if errors:
        bullets = "\n  - ".join(errors)
        msg = f"Invalid pipeline ({len(errors)} problem(s)):\n  - {bullets}"
        raise PipelineValidationError(msg)

    if logger is not None:
        logger.info(
            "Pipeline validation passed (references + binding/route presence). "
            "Note: route reachability is not yet validated."
        )
