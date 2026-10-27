# Contributing to Warren

Thanks for your interest in contributing! This document covers the practical side of getting changes merged.

## Development setup

Requires Python 3.12+.

```bash
git clone https://github.com/Gradient-DS/warren.git
cd warren
pip install -e ".[dev]"
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
5. Update docs when behavior changes: README, [`docs/public/`](docs/public/), [`warren/runtime/USAGE.md`](warren/runtime/USAGE.md), or [`warren/docs/`](warren/docs/).

## Documentation

`docs/public/` is the documentation for users, and a docs site publishes it whole. The site also generates the API reference from the docstrings. Write the pages of `docs/public/` in plain Markdown:

- Frontmatter with `title` and `description`.
- Relative links to other pages, such as `./index.md`. No HTML tags outside code.
- Link a public name or module to the reference with its import path after `py:`, such as `` [`PipelineSpec`](py:warren.runtime.PipelineSpec) ``, not with a site URL. Link the first mention on a page.

The site gives each `py:` link its URL, and its build fails on a path that the reference does not have. A public name has its reference entry at the package whose `__all__` lists it, so link that path: `warren.runtime.PipelineSpec`, not `warren.runtime.spec.PipelineSpec`. `tests/test_public_api.py` checks that each public name is in the `__all__` of exactly one package.

## Releasing

The package version is derived from Git tags by hatch-vcs. Do not bump a
version in `pyproject.toml`.

1. Open a PR moving the `CHANGELOG.md` entries under `[Unreleased]` into a
   dated heading, for example `## [0.6.2] — 2026-09-29`. Keep an empty
   `[Unreleased]` section for subsequent changes.
2. Merge the PR into `main`.
3. Create and publish a GitHub Release with a new tag `vX.Y.Z` (for example
   `v0.6.2`) targeting that merge commit on `main`.

Publishing the GitHub Release triggers the PyPI workflow; pushing a tag alone
does not. The workflow fetches the full Git history and tags, builds the
distributions, checks their metadata, and verifies every distribution filename
matches the release tag before uploading via trusted publishing.

Development checkouts may produce development versions. A `0.0.0` fallback
allows builds when no version can be determined, such as source copies without
Git metadata. The release guard prevents these versions from being published
under a different release tag.

## Commit messages

Use [Conventional Commits](https://www.conventionalcommits.org/) prefixes, as in the existing history: `feat:`, `fix:`, `docs:`, `refactor:`, `test:`, `build:`, `ci:`, `chore:`.

## Code style

- Formatting and linting are enforced by ruff (`ruff.toml`); run `ruff format .` before committing.
- Type hints on public interfaces, `async`/`await` for I/O, `pathlib` for paths.
- Program to abstractions: workers depend on the framework-layer storage/pubsub interfaces, not on concrete MongoDB/Redis/RabbitMQ classes (those live in `warren/runtime/`).

## Security issues

Never report vulnerabilities in public issues — see [SECURITY.md](SECURITY.md).
