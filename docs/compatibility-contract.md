# pybus Compatibility Contract

Status: STABLE COMPATIBILITY DRAFT
Date: 2026-07-07

This document describes the compatibility baseline we are preserving from the
current `skuulbe-api` event framework.

The goal is to make implementation in the new repo safe to do in isolation
without accidentally changing the behavior relied on today.

---

## 1. Existing Behavior Baseline

The current framework behavior in `core.events` includes:

- a Redis-backed publish path
- `EventPublisher.schedule()` behavior with transaction commit deferral
- `@transaction_safe_events` as a function decorator
- `@transaction_safe_event_publisher` as a class decorator
- `event_handler(...)` and `batched_event_handler(...)` registration
- `EventListener` worker loops on default, slow, and failed queues
- `ContinueProcess` for requeueing or continuing a flow
- batched buffering using `batched:{event_type}` keys

The new repo must preserve these semantics until a deliberate migration step
changes them.

---

## 2. Compatibility Targets

### 2.1 Imports

During migration, these legacy imports should continue to work via shims:

- `core.events.bus`
- `core.events.decorators`
- `core.events.handlers`
- `core.events.listener`
- `core.events.common`

### 2.2 Queue names

The default queue names should remain:

- `skuulbe.jobs`
- `skuulbe.jobs.slow`
- `skuulbe.jobs.failed`

These should be configurable, but the defaults must not change during the
compatibility phase.

### 2.3 Transaction semantics

The following must remain true:

- when in a Django atomic block, publishes are deferred until commit
- when outside a transaction, publishes happen immediately
- explicit `publish_on_commit=False` behavior should bypass deferral

### 2.4 Retry semantics

The following compatibility rules must be preserved:

- handler retry limits default to `10`
- handler delay defaults to `0`
- failed events should route to the failed queue
- retry payloads should preserve `retries` and `last_attempt`

### 2.5 Batching semantics

The batching contract must preserve:

- a buffer key shaped as `batched:{event_type}`
- default batch size of `100`
- default max wait of `10`
- requeue-on-failure behavior with retry counts

---

## 3. Data Compatibility

The current framework serializes via `pickle`.

The new framework will move to JSON, so compatibility needs to be handled in two
steps:

1. accept legacy pickled payloads during migration where needed
2. write JSON envelopes as the new canonical format

Existing Redis messages in-flight during migration must not be silently lost.

---

## 4. Behavioral Compatibility Matrix

| Feature | Current behavior | New behavior target |
|---|---|---|
| Publish transport | Redis | Transport abstraction with Redis extra |
| Wire format | Pickle | JSON |
| Transaction deferral | `transaction.on_commit` | Same behavior via Django extra |
| Handler registry | Global module registry | Registry object with compatibility facade |
| Worker loop | Process-local listener | Listener abstraction with optional Redis transport |
| Batch buffering | Redis list per event type | Same behavior exposed through transport/store adapters |
| Request/response | Not present | New capability, no legacy breakage |

---

## 5. Non-Breaking Migration Rule

When a new feature is added, it must not change the default behavior of the old
feature unless:

- the change is documented in this repo
- the change is versioned as a breaking release
- the migration guide explicitly says what to do

This includes:

- queue naming
- retry defaults
- handler invocation order
- transaction deferral behavior
- dead-letter routing

---

## 6. Compatibility Shims

The new package should eventually expose shims that map:

- `EventPublisher` -> message publisher facade
- `BatchedEventPublisher` -> batching-aware publisher facade
- `event_handler` -> registry registration helper
- `batched_event_handler` -> batched registration helper
- `EventListener` -> listener facade
- `ContinueProcess` -> flow control response object

These shims are temporary and should be clearly marked deprecated when used.

