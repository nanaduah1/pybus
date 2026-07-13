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


class InvalidMessageDefinitionError(ValueError, PybusError):
    """Raised when a message or envelope definition is invalid."""
