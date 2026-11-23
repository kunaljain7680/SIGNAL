import logging
from datetime import datetime, timezone
from typing import Any, Callable

from app.clients.remotive_client import RemotiveClient
from app.ingestion.contracts import (
    AuthConfig,
    Cursor,
    FetchResult,
    RateLimitConfig,
    RawEvent,
)


logger = logging.getLogger(__name__)

FEED_SCOPE = "feed"
SOFTWARE_DEVELOPMENT_CATEGORY = "software-dev"

CURSOR_LAST_FETCHED_AT = "last_fetched_at"
CURSOR_MAX_ID_SEEN = "max_id_seen"
CURSOR_MAX_PUBLICATION_DATE_SEEN = "max_publication_date_seen"


class RemotiveConnector:
    """Fetch Remotive's complete software-development feed as raw events."""

    name = "remotive"
    rate_limit = RateLimitConfig(
        min_run_interval_s=6 * 60 * 60,
        min_request_interval_s=0.0,
    )
    auth = AuthConfig(required_env=())

    def __init__(
        self,
        client_factory: Callable[[], RemotiveClient] = RemotiveClient,
        now_factory: Callable[[], datetime] | None = None,
    ) -> None:
        self._client_factory = client_factory
        self._now_factory = now_factory or (
            lambda: datetime.now(timezone.utc)
        )

    async def scopes(self) -> list[str]:
        return [FEED_SCOPE]

    async def fetch_since(self, scope_key: str, cursor: Cursor) -> FetchResult:
        """Fetch the whole feed; cursor values are observational, not filters."""
        if scope_key != FEED_SCOPE:
            raise ValueError(
                f"Unknown Remotive scope '{scope_key}'. Expected '{FEED_SCOPE}'."
            )
        if not isinstance(cursor, dict):
            raise ValueError("Remotive scope cursor must be a JSON object.")

        # Validate existing high-water observations before making a request.
        max_id_seen = self._read_max_id(cursor.get(CURSOR_MAX_ID_SEEN))
        max_publication_date_seen = cursor.get(
            CURSOR_MAX_PUBLICATION_DATE_SEEN
        )
        max_publication_datetime = self._parse_publication_date(
            max_publication_date_seen,
            field_name=CURSOR_MAX_PUBLICATION_DATE_SEEN,
            allow_none=True,
        )

        fetched_at = self._now_factory()
        if fetched_at.tzinfo is None or fetched_at.utcoffset() is None:
            raise ValueError(
                "Remotive connector clock must return a timezone-aware datetime."
            )
        fetched_at_utc = fetched_at.astimezone(timezone.utc)

        client = self._client_factory()
        try:
            payload = await client.fetch_jobs(
                category=SOFTWARE_DEVELOPMENT_CATEGORY
            )
            jobs = self._extract_jobs(payload)

            events: list[RawEvent] = []
            skipped_native_ids = 0

            for job in jobs:
                if not isinstance(job, dict):
                    raise ValueError(
                        "Remotive feed jobs must contain only JSON objects."
                    )

                native_id = self._native_id(job.get("id"))
                if native_id is None:
                    skipped_native_ids += 1
                    continue

                # Keep the complete provider job object unchanged.
                events.append(
                    RawEvent(
                        source_native_id=native_id,
                        raw_payload=job,
                    )
                )

                numeric_id = self._numeric_id(job.get("id"))
                if numeric_id is not None and (
                    max_id_seen is None or numeric_id > max_id_seen
                ):
                    max_id_seen = numeric_id

                publication_date = job.get("publication_date")
                if publication_date is None:
                    continue
                if not isinstance(publication_date, str) or not publication_date.strip():
                    logger.warning(
                        "Remotive job %s has an invalid publication_date; "
                        "the publication-date cursor will not use it.",
                        native_id,
                    )
                    continue

                try:
                    publication_datetime = self._parse_publication_date(
                        publication_date,
                        field_name=f"job {native_id} publication_date",
                        allow_none=False,
                    )
                except ValueError:
                    logger.warning(
                        "Remotive job %s has an invalid publication_date; "
                        "the publication-date cursor will not use it.",
                        native_id,
                    )
                    continue

                if (
                    max_publication_datetime is None
                    or publication_datetime > max_publication_datetime
                ):
                    max_publication_datetime = publication_datetime
                    max_publication_date_seen = publication_date

            next_cursor = dict(cursor)
            next_cursor[CURSOR_LAST_FETCHED_AT] = fetched_at_utc.isoformat()
            if max_id_seen is not None:
                next_cursor[CURSOR_MAX_ID_SEEN] = max_id_seen
            if max_publication_date_seen is not None:
                next_cursor[CURSOR_MAX_PUBLICATION_DATE_SEEN] = (
                    max_publication_date_seen
                )

            if skipped_native_ids:
                logger.warning(
                    "Skipped %d Remotive job(s) without a usable native ID.",
                    skipped_native_ids,
                )

            return FetchResult(events=events, next_cursor=next_cursor)
        finally:
            await client.aclose()

    @staticmethod
    def _extract_jobs(payload: Any) -> list[Any]:
        if not isinstance(payload, dict):
            raise ValueError("Remotive API response must be a JSON object.")

        jobs = payload.get("jobs")
        if not isinstance(jobs, list):
            raise ValueError(
                "Remotive API response must contain a jobs list."
            )

        job_count = payload.get("job-count")
        if (
            not isinstance(job_count, int)
            or isinstance(job_count, bool)
            or job_count < 0
        ):
            raise ValueError(
                "Remotive API response must contain a non-negative integer job-count."
            )
        if job_count != len(jobs):
            raise ValueError(
                "Remotive API job-count does not match the returned jobs list."
            )

        return jobs

    @staticmethod
    def _native_id(value: Any) -> str | None:
        if isinstance(value, bool) or not isinstance(value, (int, str)):
            return None
        native_id = str(value).strip()
        return native_id or None

    @staticmethod
    def _numeric_id(value: Any) -> int | None:
        if isinstance(value, bool):
            return None
        if isinstance(value, int):
            return value
        if isinstance(value, str) and value.strip().isdecimal():
            return int(value.strip())
        return None

    @classmethod
    def _read_max_id(cls, value: Any) -> int | None:
        if value is None:
            return None
        parsed = cls._numeric_id(value)
        if parsed is None:
            raise ValueError(
                "Remotive max_id_seen cursor value must be an integer."
            )
        return parsed

    @staticmethod
    def _parse_publication_date(
        value: Any,
        field_name: str,
        allow_none: bool,
    ) -> datetime | None:
        if value is None and allow_none:
            return None
        if not isinstance(value, str) or not value.strip():
            raise ValueError(
                f"Remotive {field_name} cursor value must be an ISO date string."
            )

        normalized = value.strip()
        if normalized.endswith("Z"):
            normalized = f"{normalized[:-1]}+00:00"

        try:
            parsed = datetime.fromisoformat(normalized)
        except ValueError as exc:
            raise ValueError(
                f"Remotive {field_name} must be a valid ISO date."
            ) from exc

        # Remotive's documented examples include timezone-naive ISO strings.
        # Treat those as UTC for comparison while retaining the original value.
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
