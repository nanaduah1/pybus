# pybus Compatibility Contract

Status: STABLE COMPATIBILITY DRAFT
Date: 2026-07-13

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

The new repo must preserve these semantics through the compatibility bridge
until a deliberate migration step changes them.

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

The framework should ship with framework-owned queue defaults and still allow
apps to declare extra queues as needed.

The framework defaults are:

- `pybus.jobs`
- `pybus.jobs.slow`
- `pybus.jobs.failed`

Applications migrating from the monolith should be able to map those framework
roles onto their existing queue names, including:

- `skuulbe.jobs`
- `skuulbe.jobs.slow`
- `skuulbe.jobs.failed`

That compatibility mapping must remain configurable during the migration
phase.

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
- a retry limit of `N` allows `N` retries after the initial attempt
- a valid envelope header is canonical when payload and header retry metadata
  differ; payload metadata is a legacy fallback only when the header is absent
  or malformed
- exhausted messages are terminal in the failed queue and are not dispatched by
  ordinary listener polling

### 2.5 Batching semantics

The batching contract must preserve:

- a buffer key shaped as `batched:{event_type}`
- default batch size of `100`
- default max wait of `10`
- requeue-on-failure behavior with retry counts
- deterministic partitioning of mixed batches: eligible envelopes are requeued
  while exhausted envelopes are dead-lettered exactly once
- one batched handler per message type until per-handler buffer identities are
  introduced; conflicting registrations fail explicitly
- malformed batch members become terminal decode-failure records without
  discarding valid siblings from the same claimed batch

Native pybus treats its dead-letter channel as terminal. During migration, only
an explicit compatibility/redrive adapter may consume legacy failed-queue
retries; the native listener must not be pointed at that queue as ordinary work.

### 2.6 Scheduler semantics

Scheduler registration no longer uses a bare function name as its operational
identity. Defaults are module-qualified, explicit `identity=` values are
supported, and duplicate identities fail instead of replacing an earlier task.
The observable keys returned by `Scheduler.tasks()` therefore change from
`func.__name__` to the qualified or explicit identity. `ScheduledTask.name`
continues to expose the short function name.

The durable key changes from the timestamp-only
`pybus.scheduler.last_run:{name}` shape to a versioned JSON record at
`pybus.scheduler.state:{identity}`. Deployments with existing state must migrate
that checkpoint before enabling the new scheduler contract; the old key is not
silently treated as durable proof for a potentially colliding identity.

`run_due_tasks()` still reports failure to its caller, but only after attempting
every task in the due snapshot. This deliberately changes the old first-error
abort behavior so one failing task cannot starve later work.

---

## 3. Data Compatibility

The current framework serializes via `pickle`.

The new framework will move to JSON, so compatibility needs to be handled in two
steps:

1. accept legacy pickled payloads during migration where needed
2. write JSON envelopes as the new canonical format

Existing Redis messages in-flight during migration must not be silently lost.

The JSON core should not be coupled to legacy pickle handling at import time.
Mixed-read support belongs to the compatibility bridge and the follow-up
transport track, not to the base envelope model.

---

## 4. Behavioral Compatibility Matrix

| Feature | Current behavior | New behavior target |
|---|---|---|
| Publish transport | Redis | Transport abstraction with Redis extra |
| Wire format | Pickle | JSON |
| Transaction deferral | `transaction.on_commit` | Same behavior via Django extra |
| Handler registry | Global module registry | Registry object with compatibility facade |
| Worker loop | Process-local listener | `Worker` around `Listener`, with optional Django cleanup hook and Redis transport |
| Batch buffering | Redis list per event type | Same behavior exposed through transport/store adapters |
| Scheduler identity | Bare function name | Module-qualified or explicit stable identity |
| Scheduler state | Process-local or bare-name timestamp | Versioned identity-keyed state with restart-safe backoff |
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
- `EventListener` -> `Worker` plus `Listener` composition; use the optional
  Django cleanup hook where database connections are involved
- `ContinueProcess` -> flow control response object

The shims are migration scaffolding. They should make the downstream codebase
feel familiar while the core repo stays focused on the new public contracts.

These shims are temporary and should be clearly marked deprecated when used.
Native `Worker` input must never include the terminal failed queue; legacy
failed-queue processing requires an explicit compatibility/redrive adapter.
