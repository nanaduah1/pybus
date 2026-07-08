# CONVENTIONS — pybus

> Per-repo standards loaded by the agnostic agents at step 0 of every slice.
> Keep this file short and current. It is the source of truth an agent trusts over
> its own habits.

## Stack
- Language(s) & version: Python 3.11+
- Framework(s): setuptools package, pytest test suite, ruff
- Package/dependency manager: Poetry, optional extras in `pyproject.toml`

## Architecture & patterns
- Core package lives in `src/pybus`
- Optional integrations live in `src/pybus/integrations`
- Keep the core dependency-light and import-safe
- Preserve the JSON wire contract and public import surface
- Reference docs: `README.md`, `docs/pybus-repository-spec.md`, `docs/api-contract.md`, `docs/compatibility-contract.md`

## API contract
- Case convention: JSON-compatible payloads, snake_case in Python objects as needed, documented wire shapes in docs
- Error shape: `{"errors": [...]}`
- Versioning / pagination / auth conventions: follow the docs and tests; compatibility changes are explicit

## Multi-tenancy
- Tenant key: not applicable
- How every query MUST be scoped: n/a
- Known footguns (unscoped managers, caches, exports): importing optional integrations at module import time; changing message/envelope fields without a compatibility plan

## Money & finance (if applicable)
- Money type / representation: not applicable
- Rounding & currency rules: n/a
- Idempotency expectations: preserve retry and request/response semantics where relevant

## Tests
- Test runner & how to run a single test: `poetry run pytest`; e.g. `poetry run pytest tests/core/test_imports.py`
- Coverage expectations: add regression tests for contract, serialization, transport, and optional-dependency boundaries
- Fixtures/factories to use: prefer small, explicit fixtures in `tests/`

## Lint / format (exact commands)
- Lint: `poetry run ruff check .`
- Format: `poetry run ruff format .`
- Type-check: not configured yet; rely on tests/import smoke until a type checker is adopted

## Feature toggles
- How toggles are defined and read: not currently centralized
- Default state for in-progress slices: keep new integrations isolated and optional

## Build / run for UAT
- How to start the stack locally for end-to-end UAT: run the targeted tests or import smoke for the slice
- Seed/demo data: not applicable

## Project management
- Install dependencies: `poetry install`
- Refresh the lockfile: `poetry lock`
- Run the full suite: `poetry run pytest -q`
- Use the Poetry virtualenv for all repo-local work

## Branch & PR
- Branch naming: `feat/<slug>` / `fix/<slug>`
- PR sizing & ordering norms: keep slices small and compatibility-safe
- CI gates that must pass: tests and any packaging/import checks added for the slice
