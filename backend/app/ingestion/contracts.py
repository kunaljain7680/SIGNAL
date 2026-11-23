from dataclasses import dataclass
from enum import Enum
from typing import Any, Protocol


Cursor = dict[str, Any]


@dataclass(frozen=True)
class RawEvent:
    source_native_id: str
    raw_payload: dict[str, Any]


@dataclass(frozen=True)
class FetchResult:
    events: list[RawEvent]
    next_cursor: Cursor


class SourceHealth(str, Enum):
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    DOWN = "down"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class RateLimitConfig:
    min_run_interval_s: int
    min_request_interval_s: float


@dataclass(frozen=True)
class AuthConfig:
    required_env: tuple[str, ...]


class SourceConnector(Protocol):
    name: str
    rate_limit: RateLimitConfig
    auth: AuthConfig

    async def scopes(self) -> list[str]: ...

    async def fetch_since(
        self, scope_key: str, cursor: Cursor
    ) -> FetchResult: ...
