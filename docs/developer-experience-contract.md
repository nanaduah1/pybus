# pybus Developer Experience Contract

Status: TARGET CONTRACT
Date: 2026-07-10

This document describes the experience `pybus` should provide to a consuming
application when the framework is used the way it was originally intended.

The goal is simple:

- install `pybus`
- choose a transport
- rely on the built-in default queue and dead-letter queue
- declare additional queues as needed
- define messages
- define handlers
- let the framework wire the rest

If a team still needs to hand-wire routing, retry policy, dead-letter handling,
or transaction safety for every handler, then the framework is still too low
level.

---

## 1. Product Shape

`pybus` should feel like an application framework for async business work, not
just a bag of transport primitives.

The core should own the infrastructure concerns:

- envelope creation
- serialization
- transport plumbing
- queue routing
- retries
- dead-lettering
- listener loop
- request/response correlation
- delivery safety helpers
- durable future and recurring command lifecycle

The consuming app should own the business concerns:

- message definitions
- queue intent
- handler logic
- idempotency rules
- domain validation
- payload shaping
- exceptional business-calendar decisions

---

## 2. Target Consumption Flow

The intended setup flow is:

1. install `pybus`
2. configure a transport
3. use the built-in default and dead-letter queues, then add any extra queues
4. register handlers
5. start the worker
6. publish messages

```mermaid
flowchart TD
  A[Install pybus] --> B[Configure transport]
  B --> C[Declare queues]
  C --> D[Register handlers]
  D --> E[Start worker]
  E --> F[Publish messages]
  F --> G[pybus handles retries, delivery, dead letters]
```

That flow should work without each app re-implementing its own message bus
plumbing.

---

## 3. What The Framework Should Do

The framework should provide the following by default:

- typed message envelopes
- JSON wire format
- handler registry and dispatch
- transport abstraction
- listener / worker runtime
- retry policy
- dead-letter routing
- request/response correlation
- inbox / outbox compatibility hooks

The framework should make the reliability model visible and boring:

- bounded in-process retries and terminal dead-lettering
- retry on transient failure
- dead-letter on exhaustion
- explicit correlation for request/response
- optional deduplication for consumers that need it

---

## 4. What The Application Should Still Define

Even with a strong framework, each app still owns its own semantics.

The app should define:

- message classes and versioning
- any extra queue naming beyond the built-in defaults
- event versus command intent
- handler placement
- business validation
- idempotency strategy
- retry-sensitive behavior

That is the part that should remain local to the app because it encodes the
domain.

---

## 5. Example Shape

The target public shape should read like this:

```python
from pybus.integrations.django import BusConfiguration

topology = pybus.QueueTopology().declare_queues("student.lifecycle", "billing")
application_bus = BusConfiguration(
    transport_factory=lambda: ...,
    topology=topology,
    handler_modules=("application.handlers",),
)
bus = application_bus.configure()


@pybus.event("student.enrolled", queue="student.lifecycle")
class StudentEnrolled:
    student_id: int


@pybus.command("billing.generate_student_bill", queue="billing")
class GenerateStudentBill:
    student_id: int


@pybus.event_handler(StudentEnrolled)
def handle_student_enrolled(event: StudentEnrolled):
    print(event.student_id)


@pybus.command_handler(GenerateStudentBill)
def handle_generate_student_bill(command: GenerateStudentBill):
    ...


pybus.publish_event(StudentEnrolled(student_id=7))
pybus.send_command(GenerateStudentBill(student_id=7))
```

Core and Django publishing have identical function names and inputs. A Django
application imports `publish_event` or `send_command` from
`pybus.integrations.django`; transaction commit deferral is internal.
For both imports, an explicit publication queue overrides the message
decorator's queue, which overrides the configured bus default. The resolved
queue must be declared in the configured topology.

The exact helper names may evolve, but the division of responsibility should
not:

- the app declares intent
- `pybus` wires execution and reliability

---

## 6. Success Criteria

The framework is doing its job when a new app can answer these questions
without building a custom bus:

1. How do I publish an event?
2. How do I route it to the right queue?
3. How are failures retried?
4. What happens when retries are exhausted?
5. How do I define a handler?
6. How do I keep request/response correlated?

If those answers require copy-pasting the old application framework, then the
extraction is still incomplete.

## 7. Worker lifecycle

Applications start a reusable worker from their configured bus:

```python
worker = bus.create_worker(error_delay=1.0)
worker.run()
```

The framework owns lifecycle ordering, cooperative stop behavior, recovery
after ordinary cycle errors, and terminal handling for malformed messages.
Applications may add hooks for framework integration concerns, but should not
need a custom listener subclass or process loop. Signal registration and
process supervision remain application-owned.

Reusable application composition may declare `worker_hook_factories` once.
Each worker receives fresh hook instances. An explicit `hooks=` argument on
`create_worker` replaces those defaults, and `hooks=()` disables them.
The Django `BusConfiguration` import supplies connection cleanup by default;
the core import remains framework-neutral.

Known pre-claim polling errors are recoverable. Redis claim errors (a failed
`blmove`/`lmove`), and settlement encoding or publication failures after
consumption, raise `IndeterminateDeliveryError` and abort the worker so later
messages are not drained through the same outage. An indeterminate batch claim
can affect every member already removed by that claim.

The Redis transport provides crash-safe at-least-once delivery once a claim is
durably indexed: `consume()` claims into a per-channel processing list instead
of popping destructively, and `ack`/`nack` settle that claim. A worker crash or
an indeterminate publication outcome after claim leaves the message in the
processing list rather than losing it — `RedisReaperRunner`/`create_redis_reaper_worker`
periodically finds claims older than `RedisReaperPolicy.visibility_timeout` and
redelivers or dead-letters them. The claim move and the claim-index write are
two separate steps; a crash or indeterminate failure in that narrow window
between them leaves the payload in the processing list with no index entry,
which the reaper cannot yet discover — a known, tracked gap (see
[pybus#57](https://github.com/nanaduah1/pybus/issues/57)). This is the
raw-transport guarantee `Worker` runs on; the separate durable-job/outbox layer
adds its own storage-backed persistence and reconciliation on top for
applications that need a durable job record, not just durable transport
delivery.
