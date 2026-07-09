# pybus v1 Core and Migration Track

Status: PROPOSED
Date: 2026-07-08
Related issues:
- `#1` Pybus v1 core and migration track
- `#3` Pybus follow-up: durability, sync, and compatibility adapters

## 1. Problem

`pybus` needs a narrow v1 release boundary that makes the core framework usable
on its own without dragging durability, Redis transport details, or Django
transaction behavior into the first slice.

At the same time, downstream adopters need a clear migration path from
`core.events` so that the old framework can be bridged in place while callers
move to the new package.

## 2. Goals

- Freeze the v1 core boundary for message modeling, envelopes, registry,
  serializer, dispatcher, listener, and in-memory transport support.
- Make the compatibility bridge boundary explicit so migration work stays
  separate from core primitives.
- Name the follow-up durability/sync track as a separate piece of work rather
  than hiding it inside the core scope.
- Preserve the existing public contract during migration, including queue
  defaults and transaction deferral semantics.

## 3. Non-goals

- Implementing Redis transport as part of the core v1 slice.
- Implementing Django transaction hooks as part of the core v1 slice.
- Shipping outbox/inbox storage backends in v1 core.
- Reworking downstream application handlers or domain publishers.
- Collapsing migration shims into the same layer as the core contracts.

## 4. Release Boundaries

### 4.1 Core v1 boundary

The v1 core boundary includes:

- `MessageEnvelope`
- typed message classes
- JSON serialization
- transport, registry, dispatcher, and listener contracts
- the in-memory transport used by tests and local development
- request/response and retry semantics as core framework concepts

The v1 core boundary does not include:

- Redis as a required runtime dependency
- Django as a required runtime dependency
- outbox/inbox persistence backends
- legacy `core.events` shim implementation details

### 4.2 Compatibility bridge boundary

The compatibility bridge covers:

- legacy import shims under `core.events`
- queue name preservation during migration
- transaction deferral parity for Django-backed callers
- gradual payload migration from pickle to JSON
- compatibility adapters for batched/legacy worker behavior

The bridge is a migration tool, not the definition of the v1 core API.

### 4.3 Follow-up durability/sync boundary

The follow-up track, represented by issue `#3`, covers:

- outbox and inbox storage abstractions
- Redis transport adapter
- Django integration adapter
- transaction-on-commit behavior
- batch buffering where legacy consumers still need it

This work is intentionally downstream of the v1 core so the first release stays
small, dependency-light, and easy to review.

## 5. Slice Decomposition

1. Document the v1 core boundary and migration boundary.
2. Keep the public API contract aligned with the core primitives.
3. Keep compatibility semantics explicit in the migration docs.
4. Defer durability and synchronization implementation to the follow-up track.

## 6. Failure Modes

- The core slice grows to include Redis or Django code paths.
- Migration logic becomes indistinguishable from the public v1 API.
- Docs describe the follow-up track only implicitly, which makes the boundary
  hard for new contributors to find.
- Compatibility promises drift away from the documented queue and transaction
  semantics.

## 7. Rollout and Rollback

Because this slice is documentation-only, rollout is simply publishing the
updated docs. Rollback is likewise a doc revert if the boundary wording proves
confusing or inconsistent with the codebase.

## 8. UAT

A new contributor should be able to read the docs and answer:

1. What is in the v1 core boundary?
2. What is the compatibility bridge for?
3. What work belongs to the follow-up durability/sync track?

If the answer requires reading implementation code to infer the boundary, the
docs are not explicit enough.
