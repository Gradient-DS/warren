"""
Common storage exception base classes.

Domain-specific storage modules derive from these for consistent
exception handling across the framework. Callers can catch the base
classes when they don't need to distinguish between storage domains.
"""

from document_processing.distributed.warren.exceptions import WarrenError


class ResourceNotFoundError(WarrenError):
    """A requested resource does not exist in the store."""

    pass


class ResourceAlreadyExistsError(WarrenError):
    """A resource with the given identity already exists."""

    pass


class TransientStoreError(WarrenError):
    """A store operation failed for a transient, retryable reason.

    Signals a temporary backend condition (connection blip, failover,
    server-selection timeout) rather than a permanent fault. It is part
    of the store interface contract: implementations translate their
    backend-specific transient errors into this type, so callers that
    cannot tolerate a dropped operation (e.g. the JobStatusWorker
    observer, where a lost write strands a job) can retry on it without
    depending on the concrete backend. The absence of this type means
    the failure is permanent and must not be retried.
    """

    pass
