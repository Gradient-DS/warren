"""
Message data structures for the distributed processing framework.

Defines the canonical message format used by workers. The transport layer
handles only raw dicts — ``ProcessingMessage`` is instantiated from
incoming dicts at the worker level and converted back to dicts for
publishing.

The framework is application-agnostic: message fields like ``data``,
``job_parameters``, and ``job_metadata`` are opaque dictionaries whose
structure is defined by the application, not the framework.

Routing convention: preprocessing_required / preprocessing_completed
--------------------------------------------------------------------

Two ordered lists of step names that let multiple workers consume the same
``data_type`` along a linear preprocessing chain, without needing distinct
data types per step.

Typical use: several workers observe the same fanout exchange and receive
messages of type ``raw_document``, but only one of them should process a
given message depending on whether an optional preprocessing step (e.g.
metadata extraction) has run yet. Encoding this on ``preprocessing_required``
keeps ``data_type`` reserved for *kind of payload*, not *stage of pipeline*.

Protocol:

- The publisher (or upstream worker) sets
  ``preprocessing_required=["step_name", ...]`` on the initial message.
- Each preprocessing worker's ``should_process`` accepts only when its own
  step name is present in ``preprocessing_required``.
- The downstream main worker's ``should_process`` accepts only when
  ``preprocessing_required`` is empty.
- On completion, a preprocessing worker uses
  ``ProcessingMessage.derive(preprocessing_required=..., preprocessing_completed=...)``
  to emit the same ``data_type`` with its step moved from the required
  list to the completed list.

Step names are application-defined (snake_case by convention, e.g.
``"field_extraction"``). The framework assigns no semantics to the strings
themselves — workers choose which names they recognise.

When to use this vs. a distinct ``data_type``
---------------------------------------------

Use ``preprocessing_required`` for steps that sit *in series on the same
path* — enrichment, extraction, normalisation — producing side effects or
attachments alongside the eventual main payload.

Use a distinct ``data_type`` for *alternate paths* with different downstream
consumers. Example: ``ocr_required`` flows to an OCR worker that emits
``markdown_document``, the same type the regular parser produces. OCR is a
replacement for the parser, not a preprocessing step for it. The two
patterns coexist — a message can be routed by ``data_type`` and still carry
``preprocessing_required`` within that route.
"""

from typing import Any

from collections.abc import Callable
from dataclasses import dataclass, field


@dataclass(frozen=True)
class MessageOrigin:
    """Identifies the source that produced a message.

    :param type: Category of the origin (e.g., "document_parser", "API").
    :param name: Specific instance name (e.g., "document_parser-1").
    """

    type: str
    name: str

    def to_dict(self) -> dict[str, str]:
        """Serialize to dict for transport layer."""
        return {"type": self.type, "name": self.name}


@dataclass(frozen=True)
class RetryInfo:
    """Retry state attached to messages on soft failure.

    :param count: Current retry attempt number.
    :param after: Delay in seconds before the next retry.
    :param reason: Explanation for the retry.
    :param max: Maximum retry attempts before hard failure.
    """

    count: int
    after: int
    reason: str
    max: int = 5

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dict for transport layer."""
        return {
            "count": self.count,
            "after": self.after,
            "reason": self.reason,
            "max": self.max,
        }


# TODO: Consider making ProcessingMessage a Pydantic model if validation
#   at deserialization boundaries becomes important.


@dataclass(frozen=True)
class ProcessingMessage:
    """Canonical message format for the distributed processing framework.

    Workers use ``data_type`` (and optionally ``job_metadata``) to decide
    *whether* to process a message, and ``job_parameters`` to decide *how*.

    :param data_type: Semantic type of the payload
        (e.g., "pdf_document", "markdown_document", "text_chunks").
    :param data: Payload or reference to payload in storage. Structure
        is determined by ``data_type`` and is opaque to the framework.
    :param job_id: Identifier grouping related messages into a single job.
    :param origin: Source that produced this message.
    :param job_metadata: Optional metadata about the job. Structure is
        application-defined (e.g., user, reason, priority).
    :param job_parameters: Application-defined processing parameters.
        Workers interpret these to decide how to process (e.g., force
        flags for reprocessing).
    :param preprocessing_required: Preprocessing steps still needed
        before the main processing can proceed.
    :param preprocessing_completed: Preprocessing steps already done.
    :param retry: Retry state, populated on soft failure.
    """

    data_type: str
    data: dict[str, Any]
    job_id: str
    origin: MessageOrigin
    job_metadata: dict[str, Any] | None = None
    job_parameters: dict[str, Any] = field(default_factory=dict)
    preprocessing_required: list[str] = field(default_factory=list)
    preprocessing_completed: list[str] = field(default_factory=list)
    retry: RetryInfo | None = None

    @classmethod
    def create_from(cls, message: dict[str, Any]) -> "ProcessingMessage":
        """Create a ProcessingMessage from a raw message dict.

        :param message: Raw message dictionary from the transport layer.
        :return: Typed ProcessingMessage instance.
        :raises KeyError: If required fields are missing.
        """
        origin_raw = message["origin"]
        origin = MessageOrigin(type=origin_raw["type"], name=origin_raw["name"])

        retry_raw = message.get("retry")
        retry: RetryInfo | None = None
        if retry_raw is not None:
            retry = RetryInfo(
                count=retry_raw["count"],
                after=retry_raw["after"],
                reason=retry_raw["reason"],
                max=retry_raw.get("max", 5),
            )

        return cls(
            data_type=message["data_type"],
            data=message["data"],
            job_id=message["job_id"],
            origin=origin,
            job_metadata=message.get("job_metadata"),
            job_parameters=message.get("job_parameters", {}),
            preprocessing_required=message.get("preprocessing_required", []),
            preprocessing_completed=message.get("preprocessing_completed", []),
            retry=retry,
        )

    def derive(
        self,
        *,
        data_type: str,
        data: dict[str, Any],
        origin: MessageOrigin,
        preprocessing_required: list[str] | None = None,
        preprocessing_completed: list[str] | None = None,
    ) -> "ProcessingMessage":
        """Derive a new message carrying forward job metadata from this one.

        Copies ``job_id``, ``job_metadata``, and ``job_parameters`` from
        self. Preprocessing fields are copied unless explicitly overridden.
        The ``retry`` field is intentionally not copied — a derived
        message starts with a clean retry state.

        :param data_type: Semantic type of the new payload.
        :param data: New payload or reference data.
        :param origin: Origin identifying the worker producing this message.
        :param preprocessing_required: Override preprocessing steps still
            needed. If None, copies from self.
        :param preprocessing_completed: Override completed preprocessing
            steps. If None, copies from self.
        :return: New ProcessingMessage with inherited job metadata.
        """
        return ProcessingMessage(
            data_type=data_type,
            data=data,
            job_id=self.job_id,
            origin=origin,
            job_metadata=self.job_metadata,
            job_parameters=self.job_parameters,
            preprocessing_required=(
                preprocessing_required
                if preprocessing_required is not None
                else self.preprocessing_required
            ),
            preprocessing_completed=(
                preprocessing_completed
                if preprocessing_completed is not None
                else self.preprocessing_completed
            ),
        )

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dict for publishing via transport layer.

        Produces a dict compatible with ``create_from()`` — optional
        fields (``job_metadata``, ``retry``) are omitted when None.
        """
        result: dict[str, Any] = {
            "data_type": self.data_type,
            "data": self.data,
            "job_id": self.job_id,
            "origin": self.origin.to_dict(),
            "job_parameters": self.job_parameters,
            "preprocessing_required": self.preprocessing_required,
            "preprocessing_completed": self.preprocessing_completed,
        }
        if self.job_metadata is not None:
            result["job_metadata"] = self.job_metadata
        if self.retry is not None:
            result["retry"] = self.retry.to_dict()
        return result


def build_message_key(
    job_id: str | None,
    doc_id: str | None,
    part_idx: int = 0,
) -> str:
    """Build a composite message key from identity fields.

    Consistent key format used across ResultsStore, RetryWorker, and
    cache layers. Format: ``job:{job_id}:doc:{doc_id}:part:{part_idx}``.

    None values are normalized to ``"-"`` so keys remain valid and
    consistent regardless of whether identifiers are available.

    :param job_id: Job identifier, or None.
    :param doc_id: Document identifier, or None.
    :param part_idx: Part index within the document.

    :return: Composite key string.
    """
    return f"job:{job_id or '-'}:doc:{doc_id or '-'}:part:{part_idx}"


def build_message_key_prefix(
    doc_id: str | None,
    job_id: str | None,
) -> str:
    """Build a key prefix for matching all parts of a document.

    Use with ``CacheInterface.get_by_key_prefix()`` to retrieve all
    parts of a document within a job. Includes trailing separator
    to avoid matching unrelated keys.

    None values are normalized to ``"-"``.

    :param doc_id: Document identifier, or None.
    :param job_id: Job identifier, or None.

    :return: Key prefix string ending with ``:``.
    """
    return f"job:{job_id or '-'}:doc:{doc_id or '-'}:"


ExtractMessageIdentityFunc = Callable[[dict], str]
"""Callable that extracts a human-readable identity string from a message dict."""


def extract_message_identity(message: dict) -> str:
    """Extract identity fields from a message dict for logging.

    Returns a human-readable string with job_id, doc_id, and part_idx.
    Missing fields are shown as ``"?"``. Safe to call on any message dict.

    :param message: Raw message dictionary.

    :return: Identity string for log messages, e.g.
        ``"job=abc123 doc=doc456 part=0"``.
    """
    job_id = message.get("job_id", "?")
    data = message.get("data", {})
    doc_id = data.get("doc_id", "?")
    part_idx = data.get("part_idx", "?")
    return f"job={job_id} doc={doc_id} part={part_idx}"
