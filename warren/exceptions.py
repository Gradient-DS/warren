"""
Base exception hierarchy for the distributed processing framework.

All framework-specific exceptions should derive from ``WarrenError`` so
callers can catch the full tree with a single handler when needed.
"""


class WarrenError(Exception):
    """Base exception for the distributed processing framework."""


class OptionalDependencyError(WarrenError):
    """Raised when an optional dependency is not installed.

    :param package_name: pip package name that is missing.
    :param install_hint: install command or extra specifier
        (e.g. ``pip install warren[gcs]``).
    """

    def __init__(
        self,
        package_name: str,
        *,
        install_hint: str = "",
    ) -> None:
        hint = f" — install with: {install_hint}" if install_hint else ""
        super().__init__(
            f"Optional dependency '{package_name}' is not installed{hint}"
        )
        self.package_name = package_name
        self.install_hint = install_hint
