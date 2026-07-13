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

## Typed payloads

Configure a payload codec once on the bus when messages contain Python
dataclasses or other non-JSON values:

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
