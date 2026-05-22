"""
Protocols for distributed processors.

Processors are pure, synchronous transformation units. They are generic over
input and output types, making them reusable outside the distributed system.

The worker (orchestration layer) handles: reading from stores, dispatching to
the correct processor, storing results, and publishing downstream messages.
"""

from typing import Any, Protocol, TypeVar


TInput = TypeVar("TInput")
TOutput = TypeVar("TOutput")


class Processor(Protocol[TInput, TOutput]):
    """Generic protocol for a processing strategy.

    Processors are synchronous callables that transform TInput to TOutput.
    They declare what input types they support and what output type they
    produce for downstream routing.

    Sync by design — processors are reusable outside the distributed
    system (e.g., in scripts, notebooks, tests). The worker handles
    async concerns (run_in_executor, store I/O).

    Result types are Pydantic BaseModels — workers call model_dump()
    for storage and model_validate() for reconstruction.

    @property supported_types: Format/data types this processor handles.
        Used by the worker to build its dispatch table.
    @property output_type: The data_type to publish downstream
        (e.g., "markdown_document", "xml_document").
    """

    @property
    def supported_types(self) -> frozenset[str]: ...

    @property
    def output_type(self) -> str: ...

    def __call__(self, input_data: TInput, job_parameters: dict[str, Any]) -> TOutput:
        """Transform input data into a typed result.

        :param input_data: Input data (bytes, API response, etc.).
        :param job_parameters: Per-job configuration from the message.
            Processor extracts what it needs via .get() with defaults.

        :return: Typed result (Pydantic BaseModel).

        :raises: Domain-specific exceptions. The worker maps these to
            SoftFailureException / HardFailureException.
        """
        ...
