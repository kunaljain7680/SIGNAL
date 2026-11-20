from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Protocol, runtime_checkable
import uuid


@dataclass
class Cursor:
    """
    Mutable source-specific cursor state for one fetch operation.

    Connectors update this in-memory object only after a successful fetch.
    The ingestion service persists the updated state atomically with raw
    event inserts.
    """

    state: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not isinstance(self.state, dict):
            raise TypeError("Cursor state must be a dictionary.")


@dataclass(frozen=True)
class RawEvent:
    """One immutable source record ready for raw JSONB persistence."""

    source_native_id: str
    raw_payload: dict[str, Any]

    def __post_init__(self) -> None:
        if not self.source_native_id or not self.source_native_id.strip():
            raise ValueError("RawEvent source_native_id must not be empty.")
        if not isinstance(self.raw_payload, dict):
            raise TypeError("RawEvent raw_payload must be a dictionary.")


@dataclass(frozen=True)
class SourceHealth:
    status: str
    message: str | None = None
    checked_at: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )


@dataclass(frozen=True)
class RateLimitConfig:
    requests_per_window: int | None = None
    window_seconds: int | None = None
    minimum_interval_seconds: float = 0.0


@dataclass(frozen=True)
class AuthConfig:
    required: bool
    environment_variable: str | None = None


@runtime_checkable
class SourceConnector(Protocol):
    source_id: uuid.UUID

    async def fetch_since(self, cursor: Cursor) -> list[RawEvent]: ...

    def health_check(self) -> SourceHealth: ...

    def rate_limit_config(self) -> RateLimitConfig: ...

    def auth_config(self) -> AuthConfig: ...
