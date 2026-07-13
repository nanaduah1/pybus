# pybus Architecture Decisions

Status: DECISION LOG
Date: 2026-07-07

This document captures the implementation choices that should stay fixed while
`pybus` is being built.

---

## ADR-001: JSON is the canonical wire format

Decision:

- Use JSON for all transported envelopes.

Rationale:

- portable across runtimes
- inspectable in queues and logs
- avoids pickle compatibility and security issues

Consequences:

- payloads must be serializable
- datetime and UUID encoding must be documented
- legacy pickle payloads require migration handling

---

## ADR-002: Messages are typed classes

Decision:

- Event, command, request, and response messages are represented by classes.

Rationale:

- safer API
- clearer schema
- easier versioning

Consequences:

- message conversion utilities are part of the core contract
- docs must show canonical examples for defining message types

---

## ADR-003: Transport is abstract

Decision:

- Transport is a core interface, not a Redis-specific implementation.

Rationale:

- lets the core package be reusable
- makes testing easier
- allows future backends

Consequences:

- Redis becomes an extra
- the listener/dispatcher must be backend-agnostic

---

## ADR-004: Redis is the first reference transport

Decision:

- Ship Redis support as the initial backend extra.

Rationale:

- matches current operational reality
- easy migration path
- immediate usefulness

Consequences:

- Redis behavior must remain compatible with current queues
- Redis-specific behavior lives outside core

---

## ADR-005: Django is optional

Decision:

- Django integration is provided as an extra.

Rationale:

- core should remain installable in plain Python environments
- Django-specific transaction and app-loading behavior should not leak into core

Consequences:

- Django integration code must be isolated
- transaction-safe helpers must be provided by the Django extra

---

## ADR-006: Outbox and inbox are first-class abstractions

Decision:

- Add outbox and inbox interfaces in the core design, but keep durable storage
  implementations outside the v1 core.

Rationale:

- delivery reliability cannot rely only on Redis queue semantics
- idempotency and deduplication need explicit storage behavior

Consequences:

- a durable dispatch path is part of the framework design
- consumers must be able to opt into inbox deduplication when a backend is
  available
- the v1 release can expose hooks without shipping storage backends

---

## ADR-007: Use framework-owned queue defaults with compatibility mapping

Decision:

- Ship framework-owned default queue names and allow applications to remap
  queue roles to deployment-specific names.

Rationale:

- keeps the framework reusable outside the source application
- still allows existing deployments to preserve their operational assumptions

Defaults:

- `pybus.jobs`
- `pybus.jobs.slow`
- `pybus.jobs.failed`

---

## ADR-008: Support commands and request/response alongside events

Decision:

- The framework models commands and request/response as first-class concepts.

Rationale:

- the current bus is already being used for job-like intent messages
- request/response is necessary for richer inter-module workflows

Consequences:

- handlers and registry must distinguish message kinds
- request/response needs correlation and timeout semantics

---

## ADR-009: v1 core stops at portable primitives

Decision:

- The first public release should define and stabilize the portable core
  primitives without absorbing Redis, Django, or durability adapters into the
  base package.

Rationale:

- keeps the v1 slice reviewable
- avoids coupling the public API to operational backends
- lets migration scaffolding live separately from the core contracts

Consequences:

- the core package can ship dependency-light
- Redis and Django remain explicit extras
- outbox/inbox and sync behavior are tracked as follow-up work

---

## ADR-010: Durability and sync are follow-up track work

Decision:

- Outbox, inbox, Redis transport, Django integration, and transaction deferral
  parity are part of the migration track, not the minimum core slice.

Rationale:

- these concerns are operationally important but not required to define the
  core developer experience
- separating them keeps the compatibility bridge easier to reason about

Consequences:

- documentation must point clearly from core v1 to the follow-up track
- compatibility behavior stays explicit while the implementation layers evolve

---

## ADR-011: Typed payloads use configured codecs and explicit type registries

Decision:

- Configure one payload codec on the bus and use it for every message path.
- Encode dataclasses with fully qualified Python type identifiers inline in the
  value.
- Resolve typed values only through an application-configured registry.
- Keep Django model resolution in the optional Django codec and require an
  allowlisted application resolver for every model identifier.

Rationale:

- inline metadata supports nested typed values across Python projects
- a registry makes refactors explicit through aliases
- rejecting unknown types avoids arbitrary imports from untrusted payload data
- generic Python serialization must not depend on Django
- tenant-aware model lookup belongs in an explicit application resolver, not
  an unscoped framework default

Consequences:

- every producer and consumer must configure compatible registries
- moving a payload class requires a registered alias or a versioned migration
- Django model references use stable `django://app_label/model_name` identifiers
- legacy marker shapes are read through the same registries and allowlists;
  arbitrary imports and default-manager lookups are never restored
