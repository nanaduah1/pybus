# pybus Implementation Checklist

Status: READY FOR IMPLEMENTATION
Date: 2026-07-07

This checklist is the minimal set of documents and implementation guardrails
needed before coding the framework in isolation.

---

## 1. Required docs

- repository spec
- public API contract
- compatibility contract
- architecture decisions
- migration guide

All of these now exist in `docs/`.

---

## 2. Required first implementation layer

Implement in this order:

1. `MessageEnvelope`
2. typed message base classes
3. JSON serializer
4. transport protocol
5. registry
6. dispatcher
7. listener
8. in-memory transport

---

## 3. Required compatibility behavior

The first implementation pass must not break:

- default queue names
- transaction deferral semantics
- handler retry defaults
- batched buffer semantics

---

## 4. Required test coverage

Add tests for:

- envelope serialization round-trip
- message class round-trip
- registry registration and lookup
- listener handler invocation
- retry routing
- batch buffering
- request timeout behavior
- outbox/inbox idempotency helpers

---

## 5. Required integration extras

After core stabilizes:

- Redis transport extra
- Django integration extra

These should remain optional and import-clean.

