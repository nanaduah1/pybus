# pybus Migration Guide

Status: WORKING DRAFT
Date: 2026-07-13

This guide describes how to move from the current `core.events` framework to
`pybus` without breaking existing contracts.

---

## 1. Migration Goal

The migration should let existing code continue to work while the underlying
framework moves to the new repository.

The first milestone is the core v1 boundary. Durability, sync, Redis adapter,
and Django adapter work belong to the follow-up track and should not be
mistaken for the initial core release.

The migration path should preserve:

- event handler registration
- transaction deferral
- worker queue names
- retry behavior
- batch buffering behavior

The core v1 release may already expose outbox and inbox hooks as interfaces.
Durable storage backends and draining behavior still belong to the follow-up
durability/sync track.

---

## 2. Phase 0: Add pybus as a dependency

Add `pybus` to the monolith and introduce compatibility shims.

At this phase:

- existing code still imports `core.events.*`
- `core.events.*` internally forwards to `pybus`
- Redis payloads may still need pickle compatibility during drain-down
- the bridge is the compatibility layer, not the definition of the v1 core

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
- handler adapters may live in the bridge while core message classes stay
  portable

---

## 5. Phase 3: Introduce JSON envelopes

After publishers and handlers are compatible, switch the transport payload
format from pickle to JSON.

Rules:

- do not drop in-flight legacy payloads
- support mixed read behavior during migration
- ensure serialization is versioned
- configure the same payload type registry in every producer and consumer
- register aliases before moving a dataclass to a new Python namespace
- use `DjangoPayloadCodec` only for Django model references; dataclasses and
  `Decimal` remain generic pybus concerns
- configure an allowlisted, tenant-aware resolver for every Django model
  reference; pybus does not query default managers by primary key
- legacy dataclass and Django model marker shapes remain readable only when
  their type identifiers are explicitly registered or allowlisted
- new dataclass, `Decimal`, and Django-model encodings use the versioned
  `__pybus_codec__` namespace; unknown `__pybus_type__` values remain unchanged
  as application-owned JSON

---

## 6. Phase 4: Enable outbox/inbox storage

Move high-value workflows to durable outbox/inbox patterns.

Use this phase for:

- money-related flows
- job orchestration flows
- request/response flows that need stronger reliability
- these are follow-up track concerns, not prerequisites for the core v1 slice

At this point, the app should be wiring real storage backends behind the v1
`OutboxStore` and `InboxStore` contracts rather than introducing the concepts
for the first time. The hooks are part of the core contract; this phase makes
them durable.

---

## 7. Phase 5: Remove compatibility shims

Once the application has been fully migrated:

- remove `core.events` shims
- remove pickle compatibility readers
- remove legacy worker assumptions if replaced

This should only happen after a versioned deprecation period.

### Scheduler state migration

Before replacing the application scheduler, assign explicit identities to tasks
whose Python module or qualified name may change during migration:

```python
@scheduled(hour=23, identity="reports.nightly")
def build_reports():
    ...
```

Inventory existing `{function_name}_last_run` or
`pybus.scheduler.last_run:{function_name}` values and transform each required
checkpoint into the versioned `pybus.scheduler.state:{identity}` record before
starting the new scheduler. Include the offset-aware last-run timestamp and the
next intended due time. Do not copy bare-name keys when more than one module
defines that name; choose distinct explicit identities first.

For a completed task, the version-1 value has this shape:

```json
{
  "due": "2026-07-14T23:00:00+00:00",
  "failures": 0,
  "last_failure": null,
  "last_run": "2026-07-13T23:00:00+00:00",
  "version": 1
}
```

Configure `RedisScheduleStateStore` in the process that owns scheduling. Keep
the default in-memory store for tests that intentionally do not cross a process
boundary. Run only one scheduler process for a given identity set: durable state
preserves restarts and backoff but does not add leader election or exactly-once
execution. Scheduled callables should remain idempotent because a completed
call followed by a failed checkpoint can be replayed.

---

## 8. Recommended Migration Order by Feature

1. simple event publishers
2. simple event handlers
3. batched event handlers
4. Django transaction-safe publishers
5. worker bootstrap code
6. outbox/inbox flows
7. request/response flows

Outbox/inbox flows should be treated as a durability upgrade path, not as a
new core capability in v1.

For worker bootstrap migration, replace custom listener subclasses and polling
loops with `bus.create_worker(...)`. Use lifecycle hooks for integration cleanup;
for Django workers, add `DjangoConnectionCleanupHook`. Keep signal registration
and process supervision in the host application. Do not configure the terminal
dead-letter queue as worker input.

```python
from pybus.integrations.django import DjangoConnectionCleanupHook

default_worker = bus.create_worker(
    hooks=[DjangoConnectionCleanupHook()],
)
slow_worker = bus.create_worker(
    bus.topology.slow_queue,
    hooks=[DjangoConnectionCleanupHook()],
)
```

The host application starts and supervises these blocking workers separately
and owns thread/process and signal handling. It must not start a native worker
for `bus.topology.dead_letter_queue`; failed work remains terminal until an
explicit redrive operation is introduced.

---

## 9. Safety Checklist

Before switching a feature, confirm:

- the event type string is unchanged
- the queue routing is unchanged
- the payload schema is unchanged or versioned
- the handler is idempotent
- the failure path still routes correctly
- test coverage exists for the migration path
