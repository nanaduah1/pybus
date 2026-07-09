# pybus

`pybus` is a small Python messaging framework for:

- events
- commands
- request/response flows
- transport abstraction
- outbox/inbox reliability patterns

The core package stays dependency-light. Framework integrations live behind
extras, starting with:

- `pybus[redis]`
- `pybus[django]`

## Status

This repository is the initial scaffold and design home for the framework.
The detailed repository spec lives in [`docs/pybus-repository-spec.md`](docs/pybus-repository-spec.md).

## Docs

- [`docs/pybus-repository-spec.md`](docs/pybus-repository-spec.md)
- [`docs/api-contract.md`](docs/api-contract.md)
- [`docs/compatibility-contract.md`](docs/compatibility-contract.md)
- [`docs/architecture-decisions.md`](docs/architecture-decisions.md)
- [`docs/migration-guide.md`](docs/migration-guide.md)
- [`docs/implementation-checklist.md`](docs/implementation-checklist.md)
- [`design-docs/pybus-v1-core-and-migration-track.md`](design-docs/pybus-v1-core-and-migration-track.md)

## Development

This repository is managed with Poetry.

Common commands:

- install the project: `poetry install`
- run the test suite: `poetry run pytest -q`
- run lint checks: `poetry run ruff check .`
- format code: `poetry run ruff format .`
- inspect the lockfile: `poetry lock`

Notes:

- keep the core package dependency-light
- prefer optional extras for Redis and Django integrations
- run tests from the Poetry environment rather than a global interpreter
