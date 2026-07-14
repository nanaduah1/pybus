# pybus

`pybus` is a Python messaging framework for applications that want to define
events, commands, and request/response flows without rebuilding the queue,
transport, retry, and delivery infrastructure in every service.

The intended consumer experience is:

1. install `pybus`
2. choose a transport
3. use the built-in default queue and dead-letter queue, then declare any
   additional queues you need
4. define messages and handlers
5. let the framework wire delivery, retries, and dead-letter handling

The core package stays dependency-light. Framework integrations live behind
extras, starting with:

- `pybus[redis]`
- `pybus[django]`

## Quick start

Declare each stable wire name once. Pybus turns the annotated class into an
immutable typed message, creates the transport envelope, and reconstructs the
same type for its handler:

```python
from pybus import event, event_handler, publish_event


@event("student.enrolled")
class StudentEnrolled:
    student_id: int
    school_id: int


@event_handler(StudentEnrolled)
def handle_student_enrolled(message: StudentEnrolled) -> None:
    print(message.student_id)


publish_event(StudentEnrolled(student_id=7, school_id=3))
```

Commands use the same shape with `@command(...)`, `@command_handler(...)`, and
`send_command(command)`. Applications never construct `MessageEnvelope` for the
normal path.

Messages that normally use a dedicated queue can declare it once:

```python
from pybus import QueueTopology, configure_transport


@event("student.enrolled", queue="student.lifecycle")
class StudentEnrolled:
    student_id: int


topology = QueueTopology().declare_queue("student.lifecycle")
configure_transport(transport, topology=topology)
```

`publish_event(event, queue="student.priority")` overrides that declaration for
one publication. Queue precedence is call-site override, decorator default,
then the bus default. Routing metadata is not added to the message payload or
wire envelope. Every resolved queue must be present in the configured topology,
so a missing worker route fails before the transport write instead of silently
stranding work.

Annotations provide the static contract. Put runtime domain invariants in the
message class's `__post_init__`; the same validation runs for local construction
and worker reconstruction.

Django code changes only the import:

```python
from pybus.integrations.django import publish_event
```

That function accepts the same event object. It publishes immediately outside
an atomic block and defers through `transaction.on_commit` inside one.

## Reusable application composition

Declare transport, topology, handlers, and integration hooks once. The same
configuration creates isolated buses for tests and workers or idempotently
installs the process-default bus used by `publish_event` and `send_command`:

```python
from django.conf import settings

from pybus import QueueTopology
from pybus.integrations.django import BusConfiguration
from pybus.integrations.redis import RedisTransport


APPLICATION_BUS = BusConfiguration(
    transport_factory=lambda: RedisTransport(url=settings.REDIS_URL),
    topology=QueueTopology().declare_queue("student.lifecycle"),
    handler_modules=("myapp.handlers",),
)

APPLICATION_BUS.configure()  # process default; safe to call repeatedly
```

Tests reuse those choices while replacing only the transport:

```python
from pybus.transports.memory import MemoryTransport

test_bus = APPLICATION_BUS.create(transport=MemoryTransport())
```

Workers reuse the configured process bus:

```python
worker = APPLICATION_BUS.configure().create_worker()
worker.run()
```

Constructing `BusConfiguration` does not create the transport or import handler
modules. Each `create()` call builds a fresh registry and dispatcher. A
successful `configure()` caches one bus for that configuration and reinstalls it
as the process default if another low-level configuration temporarily replaced
it. Failed transport creation, handler import, or registration is retryable and
does not replace the current default bus.

Handler module paths are explicit, imported in declaration order, and may be
combined with concrete `handler_targets`. Worker hook factories create fresh
hooks for each worker. Passing `hooks=` to `create_worker` replaces configured
defaults; `hooks=()` explicitly disables them. Importing `BusConfiguration`
from `pybus.integrations.django` enables fresh Django connection-cleanup hooks
by default; pass `worker_hook_factories=()` to disable them. The core import has
no framework hooks.

`configure()` invokes the transport factory. Factories may construct lazy
client objects but should not probe external infrastructure; the bundled Redis
transport connects only when an operation is performed. Configure once in each
worker process rather than sharing arbitrary client objects across a fork.

`Pybus(...)` and `configure_transport(...)` remain available as low-level,
backward-compatible APIs for callers that need to supply their own dispatcher.

## Rich nested payload values

Top-level typed events and commands need no payload registration. Configure a
payload codec when their fields contain nested dataclasses or other non-JSON
values:

```python
from dataclasses import dataclass
from decimal import Decimal

from pybus import PayloadTypeRegistry, PythonPayloadCodec, configure_transport


@dataclass
class ReportDescriptor:
    report_name: str
    total: Decimal


types = PayloadTypeRegistry([ReportDescriptor])
bus = configure_transport(
    transport,
    payload_codec=PythonPayloadCodec(type_registry=types),
)
```

Dataclasses carry a fully qualified identifier such as
`reports.descriptors:ReportDescriptor` in the encoded value. Consumers resolve
that identifier through their configured registry; pybus never imports a class
named by untrusted message data. Register aliases when a class moves modules.
New dataclass, `Decimal`, and Django-model encodings use the versioned
`__pybus_codec__` marker namespace. Known legacy `__pybus_type__` values remain
readable, while unknown legacy markers are preserved as application-owned JSON.
Application mappings that contain `__pybus_codec__` are escaped through a
versioned mapping wrapper so their keys and values still round-trip unchanged.

The optional `DjangoPayloadCodec` composes this generic codec and adds only
Django model references using identifiers such as `django://schools/student`.
Each allowed model identifier must have an application-supplied resolver. This
keeps model lookup explicit and lets applications enforce tenant scoping rather
than allowing pybus to query arbitrary models by primary key. Resolvers receive
the decoded envelope headers as context alongside the primary key.

## Workers

Run registered handlers through the reusable worker owned by the configured
bus:

```python
worker = bus.create_worker(error_delay=1.0)
worker.run()
```

`worker.stop()` cooperatively prevents another poll after any in-flight poll
finishes. Lifecycle hooks can wrap polling; Django applications can use
`DjangoConnectionCleanupHook` to close obsolete database connections around
each cycle. The worker deliberately does not install signal handlers or own the
shared transport lifecycle.

## Scheduled tasks

The in-memory scheduler remains the zero-dependency default. Production
processes can opt into durable restart state through the Redis extra:

```python
from pybus import configure_scheduler, scheduled
from pybus.integrations.redis import RedisScheduleStateStore


configure_scheduler(
    state_store=RedisScheduleStateStore(url="redis://localhost:6379/0")
)


@scheduled(hour=23, minute=0, identity="reports.nightly")
def build_nightly_reports() -> None:
    ...
```

Without an explicit identity, pybus uses the callable's module-qualified name.
Duplicate identities fail during registration. Scheduler state records the last
successful run, next due time, and failure backoff, so a new scheduler instance
does not repeat completed cron work or discard an active backoff. Task execution
is at-least-once: if a task finishes but its completion cannot be written, it may
run again after restart. The Redis store does not provide leader election; run
only one active scheduler for a task set.

## Status

This repository is the initial scaffold and design home for the framework.
The detailed repository spec lives in [`docs/pybus-repository-spec.md`](docs/pybus-repository-spec.md).

## Docs

- [`docs/pybus-repository-spec.md`](docs/pybus-repository-spec.md)
- [`docs/api-contract.md`](docs/api-contract.md)
- [`docs/compatibility-contract.md`](docs/compatibility-contract.md)
- [`docs/architecture-decisions.md`](docs/architecture-decisions.md)
- [`docs/migration-guide.md`](docs/migration-guide.md)
- [`docs/implementation-checklist.md`](docs/implementation-checklist.md)
- [`design-docs/pybus-v1-core-and-migration-track.md`](design-docs/pybus-v1-core-and-migration-track.md)
- [`docs/developer-experience-contract.md`](docs/developer-experience-contract.md)

## Development

This repository is managed with Poetry.

Common commands:

- install the project: `poetry install`
- run the test suite: `poetry run pytest -q`
- run lint checks: `poetry run ruff check .`
- format code: `poetry run ruff format .`
- inspect the lockfile: `poetry lock`

Notes:

- keep the core package dependency-light
- prefer optional extras for Redis and Django integrations
- run tests from the Poetry environment rather than a global interpreter
