"""
Common storage exception base classes.

Domain-specific storage modules derive from these for consistent
exception handling across the framework. Callers can catch the base
classes when they don't need to distinguish between storage domains.
"""

from warren.exceptions import WarrenError


class ResourceNotFoundError(WarrenError):
    """A requested resource does not exist in the store."""

    pass


class ResourceAlreadyExistsError(WarrenError):
    """A resource with the given identity already exists."""

    pass


class TransientStoreError(WarrenError):
    """A store operation failed for a transient, retryable reason.

    Signals a temporary backend condition (connection blip, failover,
    server-selection timeout) rather than a permanent fault. Callers that
    cannot tolerate a dropped operation (e.g. the JobStatusWorker
    observer, where a lost write strands a job) can retry on it without
    depending on the concrete backend.

    The job store and the job-results store raise this type for transient
    failures, and other errors from these two stores are permanent. The
    document, results and publishing stores raise the backend error
    unchanged.
    """

    pass
