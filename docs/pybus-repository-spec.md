# pybus — Repository Definition & Detailed Spec

Status: PROPOSAL
Date: 2026-07-07
Owner: `nanaduah1`
Target repository: `nanaduah1/pybus`

---

## 1. Summary

`pybus` will be a standalone Python framework for asynchronous and
request-oriented message handling. It extracts the current monolith's event
bus into a dedicated repository with a clean core, an explicit compatibility
bridge, and optional framework extras.

The package is intentionally broader than an event bus:

- **Events** represent facts that already happened and are usually broadcast.
- **Commands** represent intent and usually have one handler.
- **Requests/Responses** represent a query or workflow that expects a reply.

The runtime must support the core reliability primitives from day one:

- a transport abstraction
- outbox/inbox storage abstractions as part of the public contract
- JSON message envelopes
- serializable message classes
- optional Redis transport
- optional Django integration

The goal is to keep the core dependency-light, portable, and usable in plain
Python services while still supporting Django as a first-class integration
through a separate follow-up track.

---

## 2. Repository Purpose

`pybus` is the public home for the framework that currently lives inside the
`skuulbe-api` codebase under `core.events`.

This repository will be the system of record for:

- message envelope definitions
- serializer contracts
- transport abstractions
- handler registration
- listener / dispatcher runtime
- outbox and inbox interfaces as part of the core contract, with the v1 method
  set (`add`/`claim`/`complete` and `seen`/`record`) and durable
  implementations tracked as follow-up work
- command and request/response semantics
- reference implementations for Redis and Django on the follow-up track

It will **not** contain application-specific domain publishers or handlers from
`skuulbe-api`. Those remain in application repositories and only depend on
`pybus`.

---

## 3. Repository Boundary

### 3.1 In scope

- Core message framework
- Transport abstraction
- Redis transport adapter planning and compatibility boundary definition
- Django integration adapter planning and compatibility boundary definition
- Message registry
- Outbox and inbox interfaces as a documented core contract, with the v1
  method set (`add`/`claim`/`complete` and `seen`/`record`) and storage
  backends deferred to the follow-up track
- Retry and batching primitives
- Request/reply correlation support
- Documentation and examples
- Test suite for core and extras

### 3.2 Out of scope

- Business-domain events from `skuulbe-api`
- ORM models for specific product domains
- application-specific worker processes
- provider integrations for billing, messaging, payments, or reports
- any hard dependency on Django in the core package
- any hard dependency on Redis in the core package
- implementation of durability/sync adapters in the v1 core slice

---

## 4. Packaging Model

The repository should publish one primary package with optional extras.

### 4.1 Distribution name

- `pybus`

### 4.2 Extras

- `pybus[redis]`
  - installs Redis transport support
  - installs Redis-backed listener/queue helpers
- `pybus[django]`
  - installs Django integration
  - adds transaction hooks
  - adds app autodiscovery helpers
  - adds management command helpers

### 4.3 Recommended dependency policy

- Core package must depend only on the Python standard library plus minimal
  message-shaping utilities if strictly necessary.
- Redis and Django dependencies must live behind extras.
- Optional backends should be importable only when their extra is installed.

---

## 5. Core Concepts

### 5.1 Message

A message is a typed, serialized unit of work carried by the framework.

All messages share a common envelope with:

- `message_id`
- `message_type`
- `message_kind`
- `version`
- `payload`
- `headers`
- `created_at`
- `correlation_id`
- `causation_id`
- `reply_to`
- `expires_at`

### 5.2 Event

An event is a fact that already happened.

Rules:

- broadcast semantics by default
- multiple handlers allowed
- should be idempotent
- should usually be append-only and immutable after creation

Examples:

- `billing.payment.made`
- `student.enrolled`
- `report.queued`

### 5.3 Command

A command is a request to do something.

Rules:

- intent semantics
- usually exactly one handler
- should fail fast on invalid or unsupported states
- should support explicit acknowledgement/resulting status

Examples:

- `billing.generate_student_bill`
- `collections.enroll_account`
- `messaging.send_campaign_step`

### 5.4 Request / Response

A request is a message that expects a response, either synchronously or
asynchronously.

Rules:

- request and response are correlated by ID
- response messages must reference the originating request
- request handlers may reply once or emit a terminal error
- timeouts must be explicit

Examples:

- `billing.get_invoice`
- `school.resolve_active_session`
- `messaging.preview_template`

### 5.5 Serializable Message Classes

`pybus` should prefer typed message objects over ad hoc dicts.

Each message family should define a serializable class interface:

- `to_dict()`
- `from_dict()`
- `validate()`
- `message_type`
- `version`

The wire format must remain JSON-compatible.

---

## 6. Reliability Model

### 6.1 Delivery guarantees

The baseline guarantee should be **at-least-once delivery**.

That means:

- messages may be delivered more than once
- handlers must be idempotent or guarded by an inbox
- duplicates must be expected, not treated as exceptional

The Redis reference transport realizes this guarantee once a claim is durably
indexed: `RedisTransport` claims into a per-channel processing list and settles
via `ack`/`nack`, with `RedisReaperRunner` recovering claims a crashed worker
left unsettled. See `docs/compatibility-contract.md` §4.1 for the wire-level
detail and the narrow gap this guarantee doesn't yet cover.

### 6.2 Outbox

The outbox is the mechanism for durable publication.

When a business transaction creates a message, the framework should support:

- writing the outgoing message to durable storage
- committing the business change and message in the same transaction where
  possible
- a dispatcher process that drains the outbox and publishes through a transport

This is especially important for Django-backed applications and any workflow
where a database change must not be visible without the message.

### 6.3 Inbox

The inbox is the mechanism for deduplicating consumption.

When a handler processes a message, the framework should support:

- checking whether the message was already processed
- recording a processed message ID
- skipping duplicate handling safely

This should be optional but recommended for all non-trivial consumers.

### 6.4 Idempotency contract

The framework should document that:

- producers may retry
- transports may redeliver
- listeners may restart mid-flight
- handlers must tolerate duplicates

---

## 7. Transport Abstraction

Transport must be an interface, not an implementation detail.

### 7.1 Transport responsibilities

A transport is responsible only for moving serialized envelopes.

It should support:

- publish
- consume
- ack/nack or equivalent
- dead-letter handling or redirection
- optional channel/queue naming

It should **not** own business state, retry state, or deduplication state.

### 7.2 Default transport

Redis should be the first reference transport, but only as an extra.

Reasons:

- it matches the current system
- it is simple to operate
- it supports list- or stream-based queueing patterns

### 7.3 Future transport compatibility

The abstraction should leave room for:

- in-memory transport for tests
- PostgreSQL transport
- SQS transport
- Kafka transport
- NATS transport

The core API should not assume Redis semantics.

---

## 8. Django Integration

Django support belongs in an optional extra.

### 8.1 Django extra should provide

- `transaction.on_commit` integration
- Django settings adapter
- app autodiscovery helpers
- model-friendly outbox/inbox adapters
- management commands or worker bootstrap helpers
- compatibility shims for migrating from `core.events`

### 8.2 Django extra should not do

- define the core message model
- require Django for core package import
- own transport behavior
- own handler semantics

### 8.3 Migration compatibility

The Django extra should make the current monolith migration smooth by
supporting:

- `@transaction_safe_events`-style behavior
- `EventPublisher`-style wrappers
- app `ready()` registration during the transition period

---

## 9. Proposed Public API

The API should remain simple enough for application teams to adopt quickly.

### 9.1 Publishing

```python
pybus.publish_event(UserEnrolled(...))
pybus.send_command(GenerateBill(...))
result = pybus.request(GetBillSummary(...), timeout=5)
```

### 9.2 Defining messages

```python
from pybus import event


@event("student.enrolled")
class StudentEnrolled:
    student_id: int
    school_id: int
    session_id: int
```

### 9.3 Registering handlers

```python
from pybus.handlers import event_handler, command_handler, request_handler


@event_handler(StudentEnrolled)
def on_student_enrolled(message):
    ...


@command_handler(GenerateStudentBill)
def handle_generate_student_bill(command):
    ...


@request_handler(GetInvoice)
def handle_get_invoice(request):
    return InvoiceResponse(...)
```

### 9.4 Request/response

```python
response = pybus.request(
    GetInvoice(invoice_id=123),
    timeout=3,
)
```

The request API should support:

- correlation IDs
- timeouts
- error responses
- optional reply transport/channel configuration

---

## 10. Proposed Repository Layout

```text
pybus/
  pyproject.toml
  README.md
  LICENSE
  CHANGELOG.md
  src/
    pybus/
      __init__.py
      envelope.py
      messages.py
      registry.py
      dispatcher.py
      listener.py
      serializer.py
      contracts.py
      batching.py
      retries.py
      outbox.py
      inbox.py
      request_response.py
      exceptions.py
      transports/
        __init__.py
        base.py
        memory.py
      integrations/
        __init__.py
        redis.py
        django.py
      cli/
        __init__.py
        worker.py
  tests/
    core/
    transports/
    django/
    examples/
```

This layout intentionally separates:

- core primitives
- transport implementations
- integration code
- tests by concern

---

## 11. Design Principles

### 11.1 JSON first

The wire format must be JSON by default.

This means:

- no pickled payloads on the wire
- payloads must be serializable primitives or explicit encodable objects
- datetime and UUID handling must be well-defined

### 11.2 Strongly typed envelopes

Messages should be represented by classes, not raw dictionaries.

Benefits:

- self-documenting API
- versioning support
- safer serialization
- easier tests

### 11.3 Small core, rich extras

The core should be boring and stable.

Anything framework-specific goes into extras.

### 11.4 Composition over hidden globals

Avoid global mutable registries as the primary API surface.

Prefer:

- explicit registry objects
- explicit transport objects
- explicit store objects

### 11.5 One message, one intent

Messages should not try to do everything.

- events should not double as commands
- commands should not be used as query results
- responses should not be used as events

---

## 12. Compatibility Strategy for skuulbe-api

The current codebase should migrate incrementally.

### 12.1 First phase

- introduce `pybus`
- add compatibility shims in `core.events`
- keep existing imports working
- switch internals to the new package behind the scenes
- keep the v1 core slice focused on primitives and contracts
- treat durability/sync as follow-up work, not as the core release boundary

### 12.2 Second phase

- migrate publishers and handlers package by package
- move Django bootstrap to `pybus[django]`
- replace pickle payloads with JSON envelopes

### 12.3 Third phase

- move outbox/inbox-backed reliability into production paths
- retire direct Redis-push publishing paths
- remove compatibility shims
- close the follow-up durability/sync track only after the bridge has served
  migration safely

---

## 13. Implementation Milestones

### Milestone 1: Core foundation

- envelope classes
- message base classes
- JSON serializer
- registry
- transport interface
- in-memory transport for tests
- v1 core boundary documentation

### Milestone 2: Redis extra

- Redis transport
- Redis listener
- dead-letter support
- retry policy support

### Milestone 3: Django extra

- transaction-aware publishing
- app autodiscovery
- management command helpers
- settings integration

### Milestone 4: Reliability layer

- outbox store interface
- inbox store interface
- dispatcher for outbox draining
- idempotent consumption helpers

### Milestone 5: Request/response

- correlation IDs
- response routing
- timeout handling
- request API

### Milestone 6: Compatibility migration

- shims for legacy imports
- monolith-by-monolith migration
- deprecation notices

---

## 14. Acceptance Criteria

`pybus` is ready to ship when:

- the core package imports without Django or Redis installed
- JSON envelopes are the default wire format
- event, command, and request/response flows are supported
- transport is pluggable
- Redis works as an extra, not a hard dependency
- Django works as an extra, not a hard dependency
- outbox and inbox abstractions exist and are tested
- compatibility shims let the monolith migrate incrementally
- the repo has clear documentation and examples

---

## 15. Open Decisions

These should be settled before implementation begins:

1. Should responses be synchronous-only, asynchronous-only, or support both?
2. Should command handlers allow multiple subscribers or strictly one?
3. Should the default Redis transport use lists, streams, or both?
4. Should outbox/inbox backends be defined as storage interfaces only, or should the repo ship a Postgres reference implementation in v1?
5. Should messages be dataclasses-first, Pydantic-first, or support both via adapters?

---

## 16. Recommended Naming

The repository name should be:

- `pybus`

The package name should also be:

- `pybus`

Suggested extras:

- `pybus[redis]`
- `pybus[django]`

Suggested internal namespaces:

- `pybus.messages`
- `pybus.transports`
- `pybus.integrations`
- `pybus.outbox`
- `pybus.inbox`
- `pybus.request_response`

---

## 17. Next Step

Create the `nanaduah1/pybus` repository scaffold, then implement Milestone 1
with JSON envelopes, serializable message classes, and the transport/registry
interfaces before adding Redis and Django extras.
