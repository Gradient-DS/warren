"""Each public name of ``warren`` has exactly one home: the ``__all__`` of one
package.

A name is public when neither the name nor a part of its module path starts
with ``_``. A module that only imports a name does not define it. The test
reads the source with ``ast`` and imports nothing, because some modules need
an extra that the dev install does not have (``resolve_gcs`` needs ``gcs``).
"""

import ast
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent / "warren"

Name = tuple[str, str]  # (module path, name)


def _public(name: str) -> bool:
    return not name.startswith("_")


def _modules(root: Path) -> dict[str, ast.Module]:
    """Return the syntax tree of each public module below ``root``, by path."""
    found = {}
    for file in sorted(root.rglob("*.py")):
        parts = [*file.relative_to(root.parent).with_suffix("").parts]
        if parts[-1] == "__init__":
            parts.pop()
        if all(_public(part) for part in parts):
            found[".".join(parts)] = ast.parse(file.read_text(encoding="utf-8"))
    return found


def _top_level(tree: ast.Module) -> list[ast.stmt]:
    """Return the statements of a module, with the bodies of ``if`` and ``try``."""
    found = []
    todo = list(tree.body)
    while todo:
        node = todo.pop(0)
        found.append(node)
        if isinstance(node, ast.If | ast.Try):
            todo += [*node.body, *node.orelse]
            todo += [
                stmt
                for handler in getattr(node, "handlers", [])
                for stmt in handler.body
            ]
    return found


def _defined(tree: ast.Module) -> set[str]:
    """Return the names that a module defines (not imports)."""
    names = set()
    for node in _top_level(tree):
        if isinstance(node, ast.ClassDef | ast.FunctionDef | ast.AsyncFunctionDef):
            names.add(node.name)
        elif isinstance(node, ast.Assign):
            names |= {t.id for t in node.targets if isinstance(t, ast.Name)}
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            names.add(node.target.id)
        elif isinstance(node, ast.TypeAlias):
            names.add(node.name.id)
    return names - {"__all__"}


def _imported(tree: ast.Module) -> dict[str, Name]:
    """Return each name that a module imports, with its source module and name."""
    return {
        alias.asname or alias.name: (node.module, alias.name)
        for node in _top_level(tree)
        if isinstance(node, ast.ImportFrom) and node.module
        for alias in node.names
    }


def _exports(tree: ast.Module) -> list[str]:
    for node in _top_level(tree):
        if isinstance(node, ast.Assign) and any(
            isinstance(t, ast.Name) and t.id == "__all__" for t in node.targets
        ):
            return [element.value for element in node.value.elts]
    return []


def _resolve(modules: dict[str, ast.Module], module: str, name: str) -> Name:
    """Follow the imports from ``module`` to the module that defines ``name``."""
    while name not in _defined(modules[module]):
        module, name = _imported(modules[module])[name]
    return module, name


def _homes(root: Path) -> tuple[dict[Name, list[str]], set[Name]]:
    """Return the packages whose ``__all__`` has each name, and every public name."""
    modules = _modules(root)
    packages = [
        path for path in modules if (root.parent / path.replace(".", "/")).is_dir()
    ]
    homes: dict[Name, list[str]] = {}
    for package in packages:
        for name in _exports(modules[package]):
            homes.setdefault(_resolve(modules, package, name), []).append(package)
    public = {
        (module, name)
        for module, tree in modules.items()
        for name in _defined(tree)
        if _public(name)
    }
    return homes, public


def test_each_public_name_is_in_the_all_of_a_package() -> None:
    homes, public = _homes(ROOT)
    assert sorted(f"{m}.{n}" for m, n in public - homes.keys()) == []


def test_no_public_name_is_in_two_packages() -> None:
    homes, _ = _homes(ROOT)
    assert {
        f"{m}.{n}": found for (m, n), found in homes.items() if len(found) > 1
    } == {}
