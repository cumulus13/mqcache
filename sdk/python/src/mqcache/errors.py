# File: sdk/python/src/mqcache/errors.py
# Author: Hadi Cahyadi <cumulus13@gmail.com>
# Date: 2026-10-12
# Description: Exception hierarchy of the MQCache Python SDK.
# License: MIT
from __future__ import annotations


class MQCacheError(Exception):
    """Base class for all SDK errors."""


class WireFormatError(MQCacheError, ValueError):
    """A message could not be decoded (truncated, oversized lengths, unknown version...)."""


class TransportError(MQCacheError):
    """MSMQ could not send or receive."""


class MsmqUnavailableError(TransportError):
    """MSMQ (or its COM automation objects) is not installed or the service is not running."""


class TransactionUsageError(TransportError):
    """The destination queue is of the other kind (transactional vs non-transactional)."""


class CacheTimeoutError(MQCacheError):
    """No reply arrived in time after all retries."""


class ServerError(MQCacheError):
    """The server answered with an error reply, e.g. ``WRONGTYPE ...`` or ``OOM ...``."""

    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.message = message

    @property
    def code(self) -> str:
        """First word of the reply (ERR, WRONGTYPE, OOM, NOAUTH, WRONGPASS, ...)."""
        return self.message.split(" ", 1)[0]
