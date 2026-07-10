# pybus API Contract

Status: STABLE CONTRACT DRAFT
Date: 2026-07-07

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
from pybus.messages import EventMessage, CommandMessage, RequestMessage, ResponseMessage
from pybus.envelope import MessageEnvelope
from pybus.registry import Registry
from pybus.dispatcher import Dispatcher
from pybus.listener import Listener
from pybus.serializer import JsonSerializer
from pybus.contracts import Transport, OutboxStore, InboxStore
```

Optional integrations:

```python
from pybus.integrations.redis import RedisTransport
from pybus.integrations.django import DjangoBusAdapter
```

These optional integrations must not be imported by the core package during
module import.

The v1 core package surface is intentionally smaller than the eventual
durability/sync track. `OutboxStore` and `InboxStore` are part of the public
core contract as optional hooks, but durable outbox/inbox implementations,
Redis transport, and Django integration remain follow-up layers even though
their contracts are described in this repository.

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

---

## 3. Event Contract

Events are facts that happened.

Public contract:

```python
pybus.publish_event(event)
```

Rules:

- may fan out to multiple handlers
- should be idempotent
- should not return business data
- should not imply a single consumer

Event handler contract:

```python
@pybus.event_handler("student.enrolled")
def handle_student_enrolled(event: EventMessage) -> None:
    ...
```

Default behavior:

- handler errors should be retried according to policy
- exhausted retries should go to dead-letter handling
- handler registrations should be process-local and deterministic

---

## 4. Command Contract

Commands are intents.

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
@pybus.command_handler("billing.generate_student_bill")
def handle_generate_student_bill(command: CommandMessage) -> None:
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
@pybus.request_handler("billing.get_invoice")
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

The listener should preserve the current worker model defaults:

- default queue
- dead-letter queue
- slow queue
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

- default queue name: `skuulbe.jobs`
- dead-letter queue name: `skuulbe.jobs.failed`
- slow queue name: `skuulbe.jobs.slow`
- default retry limit: `10`
- default batched handler size: `100`
- default batched max wait: `10`
- JSON as the primary wire format
- Redis as the first reference transport extra
- Django as the first reference framework extra

These defaults are currently aligned with the existing monolith behavior.
