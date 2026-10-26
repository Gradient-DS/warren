"""
Protocols for distributed processors.

Processors are pure, synchronous transformation units. They are generic over
input and output types, making them reusable outside the distributed system.

An application worker (orchestration layer) handles: reading from stores,
dispatching to the correct processor, storing results, and publishing
downstream messages. Warren's worker base classes do not use this protocol.
"""

from typing import Any, Protocol


class Processor[TInput, TOutput](Protocol):
    """Generic protocol for a processing strategy.

    Processors are synchronous callables that transform TInput to TOutput.
    They declare what input types they support and what output type they
    produce for downstream routing.

    A convention for application workers: a worker can keep a dispatch
    table from ``supported_types`` and call the processor in an executor.
    Warren's worker base classes do not use this protocol.

    Sync by design — processors are reusable outside the distributed
    system (e.g., in scripts, notebooks, tests). The worker handles
    async concerns (run_in_executor, store I/O).

    Result types are Pydantic BaseModels, so a worker can call
    model_dump() for storage and model_validate() for reconstruction.
    """

    @property
    def supported_types(self) -> frozenset[str]:
        """Format/data types this processor handles.

        A worker can use them to build its dispatch table.
        """
        ...

    @property
    def output_type(self) -> str:
        """The data_type to publish downstream."""
        ...

    def __call__(self, input_data: TInput, job_parameters: dict[str, Any]) -> TOutput:
        """Transform input data into a typed result.

        :param input_data: Input data (bytes, API response, etc.).
        :param job_parameters: Per-job configuration from the message.
            Processor extracts what it needs via .get() with defaults.

        :return: Typed result (Pydantic BaseModel).

        :raises Exception: Exceptions of the processor. A worker that calls
            the processor maps them to SoftFailureException /
            HardFailureException.
        """
        ...
