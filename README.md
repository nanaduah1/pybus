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

Applications that persist work before publishing can prepare the exact JSON
envelope with an application-owned identity, then publish that same envelope:

```python
from pybus import prepare_command, publish_prepared

prepared = prepare_command(
    GenerateBill(student_id=7),
    message_id="job-42",
    headers={"tenant_id": 3},
)
job.envelope = prepared.to_dict()
job.save()
publish_prepared(prepared, queue="billing.commands")
```

`prepare_event` and `prepare_command` perform no transport I/O. The Django
versions have the same inputs; `publish_prepared` defers the already-created
envelope until commit, so rollback publishes nothing and retrying from stored
JSON preserves its identity.

## Durable jobs for typed commands

Use the opt-in Django durable-command app when a typed command must be committed
before any transport publication:

```python
# settings.py
INSTALLED_APPS += ["pybus.integrations.django_durable"]
```

Run the normal Django migrations, then configure the store on the same database
alias as the transaction that schedules work:

```python
from pybus import BusConfiguration
from pybus.integrations.django_durable.store import DjangoDurableJobStore

APPLICATION_BUS = BusConfiguration(
    transport_factory=create_transport,
    durable_job_store_factory=lambda: DjangoDurableJobStore(
        using="default"
    ),
)
bus = APPLICATION_BUS.configure()
```

Ordinary callers only schedule the domain command. Routing still comes from the
command declaration and configured topology; scheduling performs no transport
I/O and participates in the caller's database transaction:

```python
handle = pybus.schedule_command(GenerateBill(student_id=7))
```

Pass an `idempotency_key` only when intentional schedule deduplication is
required.

The same operation can defer a one-off command or attach a recurring lifecycle:

```python
from datetime import datetime
from zoneinfo import ZoneInfo

from pybus import Recurrence, RecurrenceCadence


first_run = datetime(2026, 7, 31, 9, tzinfo=ZoneInfo("Africa/Accra"))
handle = pybus.schedule_command(
    GenerateBill(student_id=7),
    run_at=first_run,
    recurrence=Recurrence(
        cadence=RecurrenceCadence.MONTHLY,
        timezone="Africa/Accra",
    ),
    idempotency_key="monthly-bill:7",
)
```

`run_at` is an optional aware eligibility timestamp: without recurrence it is a
one-off, and a value at or before the current time is immediately eligible.
With recurrence, daily, weekly, and monthly schedules retain the original local
wall-clock anchor, skip missed slots, and create only one successor after a
successful occurrence. A recurring handler returns `None` for the default
cadence, `ScheduleNextOccurrence(at=...)` to override only the next run, or
`EndRecurrence()` to finish. Call `cancel_recurring_command(handle.series_id)`
to stop an active series. Times must be timezone-aware; `ends_at` is an optional
exclusive boundary.

Run a durable publisher separately from the ordinary command consumer:

```python
bus.create_durable_job_worker().run()  # claims and publishes
bus.create_worker().run()                  # invokes typed handlers
```

The contract is at-least-once. Generation fencing prevents stale transport
copies from invoking handlers, but handlers must still be idempotent or protect
domain side effects with an inbox/domain guard. An idempotency key identifies
one canonical command; reusing it for different content raises
`DurableJobConflictError`. To disable the feature, stop its publisher and
remove its store configuration while retaining the table for recovery.

`DurableJobPolicy` configures publisher-claim leases and the delay before a
missing publication or handler outcome becomes eligible for reconciliation.
Size the reconciliation delay above normal end-to-end command duration until a
later release adds lease renewal. Configure it on `BusConfiguration`, or pass a
one-worker override to `create_durable_job_worker(policy=...)`.

The earlier command-oriented durability names remain direct compatibility
aliases throughout `0.1.x` (`DurableCommandStore`, `DjangoDurableCommandStore`,
`durable_command_store`, and `create_durable_command_worker`). New code should
use the job-oriented names; the aliases are eligible for removal in `0.2.0`.

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

Command owners may configure metadata-only delivery observers through
`command_delivery_observers=` on `BusConfiguration`, `Pybus`, or
`configure_transport`. A single-handler command reports `STARTED` before its
handler and then one of `SUCCEEDED`, `CONTINUED`, `RETRY_SCHEDULED`, or
`DEAD_LETTERED`. Final callbacks run only after the corresponding local
settlement succeeds. They are best-effort reconciliation signals, not durable
acknowledgements: a process crash can lose a callback. A failed `STARTED`
callback restores the unchanged command and aborts before business work. Later
callback failures are logged after settlement and abort the worker without
replaying a completed handler or changing its outcome.

Handlers that finish one bounded pass but need another may return a paced
continuation:

```python
from pybus import ContinueProcessing


def handle_chunk(message) -> ContinueProcessing:
    process_next_chunk(message)
    return ContinueProcessing(queue="pybus.jobs.slow", delay=0.05)
```

The optional delay is a short synchronous worker pause before Pybus republishes
the unchanged envelope. It is capped at 60 seconds and is intended to prevent
tight workflow loops on an isolated worker. It is not durable future scheduling:
the worker cannot stop during the pause, other queues assigned to that worker
also wait, and a process crash before republication can lose the destructively
claimed message. Use the scheduler or transport-specific delayed delivery for
long waits. The backward-compatible default delay is zero, so applications that
can make no progress must opt into a positive value and retain their own loop
termination or stall ceiling.

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
