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
