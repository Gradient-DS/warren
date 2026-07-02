"""Negative-path tests: a backend selected without its transport extra
installed surfaces a friendly ``OptionalDependencyError``, not a raw
``ImportError`` traceback.

Why subprocess and not in-process import-mangling
-------------------------------------------------
``aiokafka`` and ``aio_pika`` are real dependencies in the test
environment (the ``kafka``/``rmq`` extras are installed for the suite),
and other tests import them at module load — so by the time these tests
run, both libraries *and* the ``warren.pubsub.*`` transport sub-packages
already sit in ``sys.modules``. Faking "not installed" in-process would
mean installing a meta-path finder that raises ``ImportError`` for the
transport library *and* purging every already-imported
``aiokafka`` / ``aio_pika`` submodule plus the ``warren.pubsub.*.aio*``
packages so the guard in each sub-package ``__init__`` re-executes — then
restoring all of it afterwards so sibling tests still see the real
modules. That is brittle and order-dependent.

A fresh subprocess sidesteps all of it: a clean interpreter with a
meta-path finder that blocks the one transport library before any
``warren`` import gives a truly pristine import graph, exercises the real
guard path end-to-end, and cannot leak state into the rest of the suite.
It stays fast (sub-second) and deterministic.
"""

import subprocess
import sys
import textwrap


def _run_blocked(blocked_module: str, backend: str) -> subprocess.CompletedProcess:
    """Run a fresh interpreter with ``blocked_module`` made unimportable,
    then build the connection manager for ``backend`` and report whether an
    ``OptionalDependencyError`` was raised.

    The child prints ``OK:<message>`` on the expected error, or
    ``RAW_IMPORT_ERROR:<message>`` / ``NO_ERROR`` / ``WRONG:<type>:<msg>``
    on any other outcome, so the parent can assert precisely.
    """
    script = textwrap.dedent(
        f"""
        import importlib.abc
        import sys


        _BLOCKED = {blocked_module!r}


        class _Blocker(importlib.abc.MetaPathFinder):
            def find_spec(self, name, path, target=None):
                if name == _BLOCKED or name.startswith(_BLOCKED + "."):
                    raise ImportError(f"blocked: {{name}}")
                return None


        # Install before any warren import so the transport library cannot
        # be imported by the guard.
        sys.meta_path.insert(0, _Blocker())

        from warren.exceptions import OptionalDependencyError
        from warren.runtime.backends import create_connection_manager
        from warren.runtime.config import RuntimeConfig

        config = RuntimeConfig(backend={backend!r})
        try:
            create_connection_manager(config)
        except OptionalDependencyError as e:
            print("OK:" + str(e))
        except ImportError as e:
            print("RAW_IMPORT_ERROR:" + str(e))
        except Exception as e:
            print("WRONG:" + type(e).__name__ + ":" + str(e))
        else:
            print("NO_ERROR")
        """
    )
    return subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        timeout=60,
    )


def test_kafka_backend_missing_aiokafka_raises_optional_dependency_error() -> None:
    result = _run_blocked("aiokafka", "kafka")
    assert result.returncode == 0, result.stderr
    out = result.stdout.strip()
    assert out.startswith("OK:"), out
    assert "warren[kafka]" in out


def test_rabbitmq_backend_missing_aio_pika_raises_optional_dependency_error() -> None:
    result = _run_blocked("aio_pika", "rabbitmq")
    assert result.returncode == 0, result.stderr
    out = result.stdout.strip()
    assert out.startswith("OK:"), out
    assert "warren[rmq]" in out
