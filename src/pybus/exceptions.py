from __future__ import annotations


class PybusError(Exception):
    """Base class for all pybus-specific errors."""


class SerializationError(PybusError):
    """Raised when a value cannot be encoded as JSON."""


class DeserializationError(PybusError):
    """Raised when a JSON payload cannot be decoded."""


class HandlerNotFoundError(LookupError, PybusError):
    """Raised when no handler is registered for a message."""


class MessageTimeoutError(TimeoutError, PybusError):
    """Raised when a correlated request expires before a response arrives."""


class DuplicateMessageError(PybusError):
    """Raised when an inbox detects a duplicate message ID."""


class TransportError(PybusError):
    """Raised when a transport operation fails."""


class IndeterminateDeliveryError(TransportError):
    """Raised when a destructive claim or its later settlement is indeterminate."""


class WorkerAbortError(PybusError):
    """Raised when a known delivery state requires the worker to stop."""


class DeliveryObservationError(WorkerAbortError):
    """Raised when delivery observers fail before or after a known settlement."""


class DurableJobsNotConfiguredError(PybusError):
    """Raised when durable job APIs are used without a durable job store."""


class DurableJobConflictError(PybusError):
    """Raised when an idempotency key identifies a different durable job."""


class JobSeriesNotSupportedError(PybusError):
    """Raised when recurrence is used with a one-off-only durable store."""


class JobSeriesNotFoundError(LookupError, PybusError):
    """Raised when a job series cannot be found."""


class InvalidMessageDefinitionError(ValueError, PybusError):
    """Raised when a message or envelope definition is invalid."""


DurableCommandsNotConfiguredError = DurableJobsNotConfiguredError
DurableCommandConflictError = DurableJobConflictError
DurableRecurrenceNotSupportedError = JobSeriesNotSupportedError
RecurringCommandSeriesNotFoundError = JobSeriesNotFoundError
