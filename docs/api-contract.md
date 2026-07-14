# pybus API Contract

Status: STABLE CONTRACT DRAFT
Date: 2026-07-13

This document defines the minimum public contract that implementation must
honor for `pybus` to be usable in isolation.

The intent is to freeze the shape of the framework before the internals are
implemented so that:

- core code can be built without consulting the monolith on every decision
- compatibility with current `core.events` behavior remains explicit
- downstream applications can migrate incrementally

---

## 1. Package Surface

The import surface should be stable and small:

```python
import pybus
from pybus import command, event, publish_event, send_command
from pybus.messages import EventMessage, CommandMessage, RequestMessage, ResponseMessage
from pybus.envelope import MessageEnvelope
from pybus.registry import Registry
from pybus.dispatcher import Dispatcher
from pybus.listener import Listener
from pybus.worker import Worker, WorkerHook
from pybus.serializer import JsonSerializer
from pybus.contracts import Transport, OutboxStore, InboxStore
```

Optional integrations:

```python
from pybus.integrations.redis import RedisScheduleStateStore, RedisTransport
from pybus.integrations.django import (
    DjangoBusAdapter,
    DjangoConnectionCleanupHook,
    publish_event,
    send_command,
)
```

These optional integrations must not be imported by the core package during
module import.

The v1 core package surface is intentionally smaller than the eventual
durability/sync track. `OutboxStore` and `InboxStore` are part of the public
core contract as optional hooks, but durable outbox/inbox implementations,
Redis transport, and Django integration remain follow-up layers even though
their contracts are described in this repository.

### 1.1 Scheduler contract

`Scheduler`, `scheduled`, `configure_scheduler`, `get_scheduler`,
`ScheduleStateStore`, and `InMemoryScheduleStateStore` are dependency-light core
imports. `configure_scheduler(state_store=...)` accepts any store implementing
string `get(key)` and `set(key, value)` operations. In-memory state remains the
default; `RedisScheduleStateStore` is available only from the optional Redis
integration.

Every scheduled task has a stable identity. The default is
`{func.__module__}:{func.__qualname__}`; callers may pass `identity=` when code
moves or when a deployment needs an application-owned identifier. Empty or
duplicate identities fail registration. `ScheduledTask.name` remains the short
callable name for display, while `ScheduledTask.identity`, `Scheduler.tasks()`
keys, logs, and durable keys use the stable identity.

State is stored at `pybus.scheduler.state:{identity}` as one versioned JSON
record containing `last_run`, `due`, `failures`, and `last_failure`. Timestamps
and scheduler clock inputs must be ISO 8601 values with a UTC offset. Unknown
versions, malformed values, and store read failures fail registration closed
rather than treating completed work as new. An active failure restores its
persisted backoff. After a successful run, the current decorator configuration
is authoritative and computes the next due time from the persisted `last_run`,
so changing an interval or cron rule does not execute once on the old schedule.

Each due task is isolated for a complete run cycle. A failed callback or state
checkpoint receives exponential backoff and does not prevent later due tasks
from running. After the cycle, `run_due_tasks()` re-raises the first error so
direct callers retain an observable failure contract. Transient callback errors
may be retried in the same cycle; persistence errors never cause the callback to
be invoked twice in that cycle.

Scheduler execution is at-least-once. Completion is checkpointed only after the
callable returns, so a failed checkpoint creates an indeterminate window in
which application work may have succeeded and may run again after restart.
Tasks should be idempotent. A durable state store does not provide leader
election or exclude multiple scheduler processes.

---

## 2. Message Contract

### 2.1 Envelope

Every transported payload must be wrapped in a `MessageEnvelope`.

Required fields:

- `message_id`
- `message_type`
- `message_kind`
- `version`
- `payload`
- `headers`
- `created_at`

Optional fields:

- `correlation_id`
- `causation_id`
- `reply_to`
- `expires_at`
- `content_type`
- `content_encoding`

### 2.2 Kind values

`message_kind` must be one of:

- `event`
- `command`
- `request`
- `response`

### 2.3 Message classes

All message classes must support:

- `message_type` as a string constant
- `version` as an integer constant
- `to_dict()` returning JSON-serializable primitives
- `from_dict()` constructing the message object

### 2.4 JSON constraint

The canonical wire format must be JSON.

Allowed payload value types:

- `str`
- `int`
- `float`
- `bool`
- `None`
- nested `dict`
- nested `list`
- explicitly encoded datetime/UUID values

The framework must provide a documented encoding strategy for non-primitive
types.

### 2.5 Payload codecs

Applications configure one payload codec on `Pybus` / `configure_transport`.
The same codec applies to event, command, request, and response payloads and
headers, including listener and retry paths.

The core `PythonPayloadCodec` supports the normal JSON-compatible values plus:

- datetime/date/time and UUID values
- `Decimal` without float conversion
- registered Python dataclasses

Dataclasses are encoded inline with a fully qualified type identifier, schema
version, and recursively encoded fields:

```json
{
  "__pybus_codec__": "dataclass",
  "type": "reports.descriptors:ReportDescriptor",
  "version": 1,
  "fields": {}
}
```

Inline metadata is required because nested values may have different types.
Envelope headers may additionally describe the top-level schema, but are not a
replacement for nested metadata.

`__pybus_codec__` is the versioned namespace for codec-owned values. Unknown
codec types and versions fail deserialization. Known legacy `__pybus_type__`
encodings remain readable during migration; unknown legacy marker values are
ordinary application JSON and round-trip unchanged.
Application mappings containing `__pybus_codec__` are encoded through a
versioned mapping wrapper, including when nested, so the reserved codec key
cannot collide with ordinary payload data. Framework retry and dead-letter
metadata added to the wrapper remains visible after the application mapping is
decoded.

Producers and consumers must register allowed dataclass types through
`PayloadTypeRegistry`.
Unknown types fail deserialization; the framework never imports arbitrary
classes named by message data. Registry aliases provide an explicit migration
path when a class moves modules.

Framework integrations extend this contract by composition. The Django codec
adds model references such as `django://schools/student` and delegates all
generic Python values to `PythonPayloadCodec`. Django model identifiers are an
explicit allowlist: each identifier requires an application-supplied resolver,
which receives the decoded envelope headers and is responsible for tenant-aware
lookup. The codec never uses an unscoped default model manager, and a resolver
returning `None` fails deserialization.

---

## 3. Event Contract

Events are facts that happened.

Declare the stable event type once:

```python
@pybus.event("student.enrolled", queue="student.lifecycle")
class StudentEnrolled:
    student_id: int
    school_id: int
```

Public contract:

```python
pybus.publish_event(event)
```

Rules:

- may fan out to multiple handlers
- should be idempotent
- should not return business data
- should not imply a single consumer
- annotations define the static field contract; runtime business invariants
  belong in the typed class's `__post_init__`
- an optional decorator `queue` declares the normal publication route once
- an explicit `publish_event(event, queue=...)` wins over the decorator queue;
  without either, the bus default queue is used
- every resolved queue must be declared in the bus topology; invalid or unknown
  routes fail before transport publication
- queue selection is transport routing metadata and is not serialized into the
  domain payload or message envelope

Event handler contract:

```python
@pybus.event_handler(StudentEnrolled)
def handle_student_enrolled(event: StudentEnrolled) -> None:
    print(event.student_id)
```

Default behavior:

- handler errors should be retried according to policy
- exhausted retries should go to dead-letter handling
- handler registrations should be process-local and deterministic

Retry limits count additional delivery attempts: `retry_limit=0` means one
initial handler invocation and no retry; `retry_limit=2` permits at most three
handler invocations. Envelope headers are the canonical framework retry state:
a valid header count wins. Legacy mapping payload fields are used only when the
header is absent or malformed and may mirror the canonical value when requeued.
Retry timestamps must include a UTC offset; a naive or malformed header timestamp
is ignored and the listener may fall back to a valid legacy payload timestamp.
Typed message fields are never mutated with retry metadata; their payload is
unchanged through retries and dead-lettering.

Typed handlers receive the domain message rather than its envelope. During
migration, handlers that require transport headers or correlation metadata may
remain string-bound and receive the legacy generic message wrapper.

Batched delivery currently supports one batched handler per message type. A
batched handler may not share its message type with another batched or ordinary
event handler because the buffer is claimed as one delivery unit. Registration
must fail explicitly instead of silently losing fan-out deliveries.

---

## 4. Command Contract

Commands are intents.

```python
@pybus.command("billing.generate_student_bill")
class GenerateStudentBill:
    student_id: int
```

Public contract:

```python
pybus.send_command(command)
```

Rules:

- should normally route to one handler
- should return an acknowledgement or a failure
- should fail fast when no handler exists
- may be used with the outbox for durability

Command handler contract:

```python
@pybus.command_handler(GenerateStudentBill)
def handle_generate_student_bill(command: GenerateStudentBill) -> None:
    ...
```

The command layer may allow multiple subscribers only if explicitly configured.
The default must be single-handler semantics.

---

## 5. Request / Response Contract

Request/response is a correlated interaction pattern.

Public contract:

```python
response = pybus.request(request, timeout=5)
```

Required behavior:

- the request envelope must include a correlation ID
- the response envelope must reference the originating request
- callers must be able to provide a timeout
- timeout expiration must raise a documented exception
- request handlers may return a response object or raise a terminal error

Response handlers:

```python
@pybus.request_handler(GetInvoice)
def handle_get_invoice(request: RequestMessage) -> ResponseMessage:
    ...
```

---

## 6. Transport Contract

Transport is an abstraction over message movement.

Required transport protocol:

```python
class Transport(Protocol):
    def publish(self, channel: str, message: bytes) -> None: ...
    def consume(self, channel: str, timeout: int = 5) -> bytes | None: ...
    def ack(self, receipt: str) -> None: ...
    def nack(self, receipt: str, *, requeue: bool = True) -> None: ...
```

If a backend cannot support `ack`/`nack`, it must document the equivalent
behavior and adapt to the same public semantics.

### 6.1 Outbox / inbox hooks

`OutboxStore` and `InboxStore` are part of the v1 contract as interfaces and
integration points.

Rules:

- v1 may expose the interfaces, helper methods, and compatibility hooks
- v1 does not need to ship durable storage backends
- the durable implementations belong to the follow-up durability/sync track

Minimum method set:

```python
class OutboxStore(Protocol):
    def add(self, message: bytes) -> str: ...
    def claim(self, limit: int = 100) -> list[bytes]: ...
    def complete(self, receipt: str) -> None: ...


class InboxStore(Protocol):
    def seen(self, message_id: str) -> bool: ...
    def record(self, message_id: str) -> None: ...
```

The v1 names above are the public contract. Storage backends may add extra
capabilities, but they must preserve these methods.

Transport must not:

- store application state
- perform message deduplication
- enforce handler retry logic

The first transport slice is the abstract contract plus the in-memory test
transport. Backend-specific durability and sync adapters are documented
separately so the v1 core remains dependency-light.

---

## 7. Registry Contract

The registry is responsible for mapping message types to handlers.

Required behavior:

- register handlers by message type
- return deterministic handler sets for a type
- allow inspection of registered handlers
- support separate registration buckets for events, commands, and requests

The registry must not rely on import-time side effects in the core package.

---

## 8. Dispatcher Contract

Dispatcher is responsible for:

- serializing messages to envelopes
- resolving the correct transport and channel
- sending to the transport
- optionally writing to an outbox first
- optionally awaiting a response for request/response flows

Dispatcher must not:

- know domain-specific business rules
- directly inspect application models
- own handler retry decisions

---

## 9. Listener Contract

Listener is responsible for consuming envelopes from a transport and invoking
registered handlers.

Required behavior:

- pull from one channel or a configured set of channels
- deserialize envelopes from JSON
- reconstruct typed message objects
- call handlers with typed message instances
- apply retry policy
- support dead-letter routing

The configured dead-letter channel is terminal storage, not an ordinary input
channel. Normal listener polling must leave it untouched so exhausted messages
cannot re-enter the handler and exceed their retry bound. A future explicit
redrive API may move selected messages back to an input queue.

This is an intentional native-pybus boundary. A migration compatibility adapter
may explicitly consume legacy failed-queue retries, but it must not configure the
native listener's terminal dead-letter channel as an ordinary worker input.

If a claimed ordinary message or batch member cannot be decoded, the listener
writes a terminal
`pybus.message.decode_failed` envelope with this payload and header contract:

```json
{
  "payload": {
    "raw_message": "<base64 text>",
    "raw_message_encoding": "base64",
    "error_type": "DeserializationError"
  },
  "headers": {"dead_lettered_from": "<source queue>"}
}
```

The envelope's `content_encoding` remains unset because only the nested raw
message field is base64 encoded. The next ordinary message, and valid siblings
from the same claimed batch, must remain processable.

### 9.1 Worker lifecycle

`Worker` provides the reusable blocking loop around `Listener.listen_once`.
`Pybus.create_worker()` creates one for the bus default queue unless the caller
supplies a queue or ordered queue sequence.

Lifecycle callbacks run in this order:

1. `on_start` once, in hook order
2. `before_poll`, then one listener poll, then `after_poll`, in hook order
3. `on_error` in hook order when a cycle raises an `Exception`
4. `on_stop` once for successfully started hooks, in reverse order

`stop()` is thread-safe and idempotent. It prevents a new poll, while an
already-running poll is allowed to finish. The worker catches `Exception`, not
`BaseException`, and waits the configured fixed `error_delay` through its stop
event so shutdown can interrupt the delay. A dead-letter queue cannot be used
as worker input.

Hook failures are isolated where recovery is safe: every error observer and
stop hook is attempted. An `after_poll` failure is reported without repeating
the delivery that already completed. A failed `before_poll` skips consumption
for that cycle.

Known pre-claim polling and lifecycle errors are recoverable cycle failures. A
Redis destructive-pop exception has an unknown server-side claim outcome and is
therefore indeterminate. If a retry, dead-letter, poison record, continuation,
batch buffer, or response cannot be encoded or published after a message was
claimed, the listener also raises `IndeterminateDeliveryError`. The worker
reports that error to hooks and then aborts instead of consuming later messages.

The optional `DjangoConnectionCleanupHook` lazily imports Django and runs
`close_old_connections()` before and after each poll and again at shutdown.
The core package remains importable without Django installed.

The worker does not own or disconnect its transport. Current list transports
consume with a destructive pop, so a process crash or indeterminate transport
failure after claim can still lose claimed work. Failing closed prevents the
worker from draining later queues but cannot restore the claimed item. A batch
claim may leave multiple already-popped members indeterminate. A future
claim/ack transport is required for a crash-safe at-least-once guarantee; this
lifecycle API does not imply one.

The topology should preserve these declared defaults:

- default queue
- slow queue
- terminal dead-letter storage, which is never normal worker input
- any additional queues explicitly configured by the app

unless explicitly configured otherwise.

---

## 10. Store Contracts

### 10.1 Outbox

The outbox must support:

- adding a pending outgoing message
- claiming a batch for dispatch
- marking rows as dispatched
- exposing a durable message ID

### 10.2 Inbox

The inbox must support:

- checking if a message was already processed
- marking a message as processed
- optional TTL or retention policy

The exact storage backend is an implementation choice, but the methods are part
of the contract.

---

## 11. Error Contract

The package must define explicit exceptions for:

- serialization errors
- deserialization errors
- handler not found
- message timeout
- duplicate message detection
- transport failure
- invalid message definition

Exception names may evolve, but their semantics must remain stable.

---

## 12. Default Behavior Contract

The following defaults should remain stable unless a versioned breaking change
is intentionally introduced:

- default queue name: `pybus.jobs`
- dead-letter queue name: `pybus.jobs.failed`
- slow queue name: `pybus.jobs.slow`
- default retry limit: `10`
- default batched handler size: `100`
- default batched max wait: `10`
- JSON as the primary wire format
- Redis as the first reference transport extra
- Django as the first reference framework extra

Applications may override those queue-role names for compatibility with an
existing deployment such as `skuulbe.jobs`.
