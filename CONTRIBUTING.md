# Contributing to Warren

Thanks for your interest in contributing! This document covers the practical side of getting changes merged.

## Development setup

Requires Python 3.12+.

```bash
git clone https://github.com/Gradient-DS/warren.git
cd warren
pip install -e .[dev]
```

## Tests and lint

```bash
python -m pytest tests -q
ruff check .
ruff format --check .
```

All three must pass before a PR can merge — CI runs exactly these commands.

To exercise changes against a real pipeline, run the fake example pipeline from the README quickstart (needs local RabbitMQ, MongoDB, and Redis, e.g. via Docker).

## Making changes

1. For anything beyond a small fix, open an issue first so we can discuss the approach.
2. Fork the repo and branch from `main`.
3. Keep PRs small and focused — one logical change per PR.
4. Add or update tests for behavior changes.
5. Update docs when behavior changes: README, [`warren/runtime/USAGE.md`](warren/runtime/USAGE.md), or [`warren/docs/`](warren/docs/).

## Commit messages

Use [Conventional Commits](https://www.conventionalcommits.org/) prefixes, as in the existing history: `feat:`, `fix:`, `docs:`, `refactor:`, `test:`, `build:`, `ci:`, `chore:`.

## Code style

- Formatting and linting are enforced by ruff (`ruff.toml`); run `ruff format .` before committing.
- Type hints on public interfaces, `async`/`await` for I/O, `pathlib` for paths.
- Program to abstractions: workers depend on the framework-layer storage/pubsub interfaces, not on concrete MongoDB/Redis/RabbitMQ classes (those live in `warren/runtime/`).

## Security issues

Never report vulnerabilities in public issues — see [SECURITY.md](SECURITY.md).
