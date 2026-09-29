"""Scope context shared by handlers and storage adapters."""

from contextvars import ContextVar


current_scope: ContextVar[str | None] = ContextVar("warren_scope", default=None)


def get_current_scope() -> str | None:
    """Return the raw scope of the current handler."""
    return current_scope.get()
