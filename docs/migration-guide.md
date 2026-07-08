# pybus Migration Guide

Status: WORKING DRAFT
Date: 2026-07-07

This guide describes how to move from the current `core.events` framework to
`pybus` without breaking existing contracts.

---

## 1. Migration Goal

The migration should let existing code continue to work while the underlying
framework moves to the new repository.

The migration path should preserve:

- event handler registration
- transaction deferral
- worker queue names
- retry behavior
- batch buffering behavior

---

## 2. Phase 0: Add pybus as a dependency

Add `pybus` to the monolith and introduce compatibility shims.

At this phase:

- existing code still imports `core.events.*`
- `core.events.*` internally forwards to `pybus`
- Redis payloads may still need pickle compatibility during drain-down

---

## 3. Phase 1: Move publishers first

Publisher classes should be migrated before handler logic where practical.

Recommended order:

1. low-risk publishers
2. pure intent/job publishers
3. batched publishers
4. transaction-sensitive publishers

Publisher migration should preserve:

- event type strings
- queue names
- payload keys
- transaction deferral semantics

---

## 4. Phase 2: Move handlers next

Handler modules should move to `pybus`-compatible message objects while keeping
the same event type names.

During migration:

- handler functions may accept new typed message objects
- compatibility facades may adapt old `Event` wrappers
- dead-letter behavior must remain intact

---

## 5. Phase 3: Introduce JSON envelopes

After publishers and handlers are compatible, switch the transport payload
format from pickle to JSON.

Rules:

- do not drop in-flight legacy payloads
- support mixed read behavior during migration
- ensure serialization is versioned

---

## 6. Phase 4: Enable outbox/inbox storage

Move high-value workflows to durable outbox/inbox patterns.

Use this phase for:

- money-related flows
- job orchestration flows
- request/response flows that need stronger reliability

---

## 7. Phase 5: Remove compatibility shims

Once the application has been fully migrated:

- remove `core.events` shims
- remove pickle compatibility readers
- remove legacy worker assumptions if replaced

This should only happen after a versioned deprecation period.

---

## 8. Recommended Migration Order by Feature

1. simple event publishers
2. simple event handlers
3. batched event handlers
4. Django transaction-safe publishers
5. worker bootstrap code
6. outbox/inbox flows
7. request/response flows

---

## 9. Safety Checklist

Before switching a feature, confirm:

- the event type string is unchanged
- the queue routing is unchanged
- the payload schema is unchanged or versioned
- the handler is idempotent
- the failure path still routes correctly
- test coverage exists for the migration path

