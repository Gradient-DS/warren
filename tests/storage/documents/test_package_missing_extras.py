"""The ``warren.storage.documents`` package imports without the ``http`` and
``gcs`` extras; only the names that need an extra fail, when they are used.

A fresh subprocess gives a clean import graph, for the same reason as in
``tests/runtime/test_backends_missing_extras.py``.
"""

import subprocess
import sys
import textwrap


def _run_without_extras(body: str) -> subprocess.CompletedProcess:
    """Run ``body`` in a fresh interpreter where ``httpx`` and ``google``
    cannot be imported.
    """
    script = "import sys\n"
    script += "for name in ('httpx', 'google', 'google.cloud'):\n"
    script += "    sys.modules[name] = None\n"
    script += textwrap.dedent(body)
    return subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        timeout=60,
    )


def test_package_imports_without_http_and_gcs_extras() -> None:
    result = _run_without_extras(
        """
        from warren.storage.documents import DocumentPathLocation, resolve_path
        print("OK")
        """
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "OK"


def test_name_that_needs_an_extra_raises_import_error_when_used() -> None:
    result = _run_without_extras(
        """
        import warren.storage.documents as documents

        for name in ("resolve_http", "build_client", "resolve_gcs"):
            try:
                getattr(documents, name)
            except ImportError:
                print(name + ":ImportError")
            else:
                print(name + ":NO_ERROR")
        """
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.split() == [
        "resolve_http:ImportError",
        "build_client:ImportError",
        "resolve_gcs:ImportError",
    ]
