from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta, timezone
import re
from typing import Any
import uuid

from app.clients.remotive_client import RemotiveClient
from app.ingestion.contracts import (
    AuthConfig,
    Cursor,
    RateLimitConfig,
    RawEvent,
    SourceHealth,
)


class RemotiveConnector:
    """
    Daily, bounded adapter for Remotive's active remote-jobs feed.

    Remotive does not document server-side since_id or page pagination.
    A local publication watermark filters records while the existing raw
    event uniqueness constraint makes the daily overlap safe.
    """

    INITIAL_LOOKBACK = timedelta(days=7)
    WATERMARK_OVERLAP = timedelta(days=1)
    MINIMUM_FETCH_INTERVAL = timedelta(hours=20)

    def __init__(
        self,
        source_id: uuid.UUID,
        tracked_technologies: list[str],
        client: Any | None = None,
    ) -> None:
        if not tracked_technologies:
            raise ValueError("At least one tracked technology is required.")

        self.source_id = source_id
        cleaned = []
        for name in tracked_technologies:
            value = name.strip()
            if value and value.casefold() not in {
                existing.casefold() for existing in cleaned
            }:
                cleaned.append(value)
        if not cleaned:
            raise ValueError("Tracked technology names must not be empty.")

        self.tracked_technologies = cleaned
        self._owns_client = client is None
        self.client = client or RemotiveClient()

    async def fetch_since(self, cursor: Cursor) -> list[RawEvent]:
        now = datetime.now(timezone.utc)
        next_state = deepcopy(cursor.state)
        jobs_state = next_state.setdefault("remotive", {})

        last_fetch = self._parse_datetime(jobs_state.get("last_fetch_at"))
        if last_fetch is not None and now - last_fetch < self.MINIMUM_FETCH_INTERVAL:
            return []

        payload = await self.client.fetch_active_jobs()
        jobs = payload.get("jobs")
        if not isinstance(jobs, list):
            raise ValueError("Remotive response is missing a jobs array.")

        watermark = self._parse_datetime(
            jobs_state.get("last_publication_date")
        )
        cutoff = (
            watermark - self.WATERMARK_OVERLAP
            if watermark is not None
            else now - self.INITIAL_LOOKBACK
        )

        max_publication_date = watermark
        events: dict[str, RawEvent] = {}

        for job in jobs:
            if not isinstance(job, dict) or job.get("id") is None:
                continue

            publication_date = self._parse_datetime(
                job.get("publication_date")
            )
            if publication_date is None:
                continue

            if (
                max_publication_date is None
                or publication_date > max_publication_date
            ):
                max_publication_date = publication_date

            if publication_date < cutoff:
                continue
            if not self._matches_tracked_technology(job):
                continue

            native_id = f"job:{job['id']}"
            events.setdefault(
                native_id,
                RawEvent(
                    source_native_id=native_id,
                    raw_payload=job,
                ),
            )

        if max_publication_date is not None:
            jobs_state["last_publication_date"] = (
                max_publication_date.isoformat()
            )
        jobs_state["last_fetch_at"] = now.isoformat()
        jobs_state["last_job_count"] = len(jobs)

        cursor.state = next_state
        return list(events.values())

    def _matches_tracked_technology(self, job: dict[str, Any]) -> bool:
        title = job.get("title")
        description = job.get("description")
        searchable = " ".join(
            value for value in (title, description)
            if isinstance(value, str)
        )
        searchable = searchable.casefold()

        for technology in self.tracked_technologies:
            candidate = technology.casefold()

            # "Go" as a standalone word is too ambiguous in job prose.
            if candidate == "go":
                if re.search(
                    r"\b(golang|go programming|go developer|go engineer)\b",
                    searchable,
                ):
                    return True
                continue

            pattern = rf"(?<!\w){re.escape(candidate)}(?!\w)"
            if re.search(pattern, searchable):
                return True

        return False

    @staticmethod
    def _parse_datetime(value: Any) -> datetime | None:
        if not isinstance(value, str) or not value.strip():
            return None
        parsed_value = value.strip().replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(parsed_value)
        except ValueError:
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)

    def health_check(self) -> SourceHealth:
        return SourceHealth(
            status="unknown",
            message="No live Remotive request performed by configuration check.",
        )

    def rate_limit_config(self) -> RateLimitConfig:
        return RateLimitConfig(
            requests_per_window=4,
            window_seconds=86400,
            minimum_interval_seconds=float(
                self.MINIMUM_FETCH_INTERVAL.total_seconds()
            ),
        )

    def auth_config(self) -> AuthConfig:
        return AuthConfig(required=False)

    async def aclose(self) -> None:
        if self._owns_client:
            await self.client.aclose()
