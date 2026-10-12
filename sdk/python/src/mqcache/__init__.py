# File: sdk/python/src/mqcache/__init__.py
# Author: Hadi Cahyadi <cumulus13@gmail.com>
# Date: 2026-10-12
# Description: MQCache Python SDK public API.
# License: MIT
"""Python client for MQCache (Redis-like cache over Microsoft Message Queuing). Windows only."""
from .client import CacheClient, Priority, Subscriber
from .errors import (CacheTimeoutError, MQCacheError, MsmqUnavailableError, ServerError, TransactionUsageError,
                     TransportError, WireFormatError)
from .protocol import Command, PubMessage, Resp, RespType

__all__ = [
    "CacheClient", "Subscriber", "Priority", "Command", "PubMessage", "Resp", "RespType",
    "MQCacheError", "ServerError", "CacheTimeoutError", "TransportError", "MsmqUnavailableError",
    "TransactionUsageError", "WireFormatError",
]
__version__ = "0.1.0"
