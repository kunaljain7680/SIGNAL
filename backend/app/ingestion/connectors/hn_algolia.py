import logging
from datetime import datetime, timezone
from typing import Any, Callable

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.clients.hn_client import HackerNewsClient
from app.core.database import AsyncSessionLocal
from app.ingestion.contracts import (
    AuthConfig,
    Cursor,
    FetchResult,
    RateLimitConfig,
    RawEvent,
)
from app.models.technology import Technology


logger = logging.getLogger(__name__)

SECONDS_PER_DAY = 24 * 60 * 60
INITIAL_BACKFILL_DAYS = 42
INITIAL_SLICE_DAYS = 7
INCREMENTAL_OVERLAP_SECONDS = 60 * 60
ALGOLIA_MAX_HITS_PER_QUERY = 1000

CURSOR_INITIAL_BACKFILL_COMPLETE = "initial_backfill_complete"
CURSOR_INITIAL_BACKFILL_COMPLETED_AT = "initial_backfill_completed_at_i"
CURSOR_LAST_CREATED_AT = "last_created_at_i"


class HNAlgoliaConnector:
    """Read Hacker News Algolia stories as unmodified source events."""

    name = "hn_algolia"
    rate_limit = RateLimitConfig(
        min_run_interval_s=6 * 60 * 60,
        # One request per second stays comfortably below Algolia's
        # documented 10,000-requests-per-hour IP limit.
        min_request_interval_s=1.0,
    )
    auth = AuthConfig(required_env=())

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession] = AsyncSessionLocal,
        client_factory: Callable[[], HackerNewsClient] = HackerNewsClient,
        now_factory: Callable[[], datetime] | None = None,
    ) -> None:
        self._session_factory = session_factory
        self._client_factory = client_factory
        self._now_factory = now_factory or (lambda: datetime.now(timezone.utc))
        self._query_by_scope: dict[str, str] = {}

    async def scopes(self) -> list[str]:
        """Return active technology slugs and cache their Phase 1 query terms."""
        async with self._session_factory() as session:
            result = await session.execute(
                select(Technology.slug, Technology.canonical_name)
                .where(Technology.status == "active")
                .order_by(Technology.slug)
            )
            technology_rows = result.all()

        self._query_by_scope = {
            slug: canonical_name
            for slug, canonical_name in technology_rows
        }
        return list(self._query_by_scope)

    async def fetch_since(self, scope_key: str, cursor: Cursor) -> FetchResult:
        """Fetch one technology scope using its checkpointed time window."""
        query = self._query_by_scope.get(scope_key)
        if query is None:
            raise ValueError(
                f"Unknown HN scope '{scope_key}'. Call scopes() before fetching."
            )
        if not isinstance(cursor, dict):
            raise ValueError("HN scope cursor must be a JSON object.")

        now = self._now_factory()
        if now.tzinfo is None or now.utcoffset() is None:
            raise ValueError("HN connector clock must return a timezone-aware datetime.")
        now_timestamp = int(now.timestamp())

        client = self._client_factory()
        try:
            if not cursor.get(CURSOR_INITIAL_BACKFILL_COMPLETE, False):
                raw_hits = await self._initial_backfill(
                    client=client,
                    query=query,
                    end_timestamp=now_timestamp,
                )
            else:
                raw_hits = await self._incremental_fetch(
                    client=client,
                    query=query,
                    cursor=cursor,
                    end_timestamp=now_timestamp,
                )

            events, observed_timestamps = self._to_raw_events(raw_hits)
            next_cursor = self._build_next_cursor(
                cursor=cursor,
                observed_timestamps=observed_timestamps,
                initial_backfill_completed_at=(
                    now_timestamp
                    if not cursor.get(CURSOR_INITIAL_BACKFILL_COMPLETE, False)
                    else None
                ),
            )
            return FetchResult(events=events, next_cursor=next_cursor)
        finally:
            await client.aclose()

    async def _initial_backfill(
        self,
        client: HackerNewsClient,
        query: str,
        end_timestamp: int,
    ) -> list[dict[str, Any]]:
        start_timestamp = end_timestamp - (
            INITIAL_BACKFILL_DAYS * SECONDS_PER_DAY
        )
        slice_seconds = INITIAL_SLICE_DAYS * SECONDS_PER_DAY
        all_hits: list[dict[str, Any]] = []

        for slice_start in range(start_timestamp, end_timestamp, slice_seconds):
            slice_end = min(slice_start + slice_seconds, end_timestamp)
            all_hits.extend(
                await self._fetch_complete_window(
                    client=client,
                    query=query,
                    start_timestamp=slice_start,
                    end_timestamp=slice_end,
                )
            )

        return all_hits

    async def _incremental_fetch(
        self,
        client: HackerNewsClient,
        query: str,
        cursor: Cursor,
        end_timestamp: int,
    ) -> list[dict[str, Any]]:
        watermark = cursor.get(CURSOR_LAST_CREATED_AT)
        if watermark is None:
            watermark = cursor.get(CURSOR_INITIAL_BACKFILL_COMPLETED_AT)

        if not isinstance(watermark, int) or isinstance(watermark, bool):
            raise ValueError(
                "Completed HN cursor must contain an integer creation-time watermark."
            )

        start_timestamp = max(
            0,
            watermark - INCREMENTAL_OVERLAP_SECONDS,
        )
        if start_timestamp >= end_timestamp:
            return []

        return await self._fetch_complete_window(
            client=client,
            query=query,
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp,
        )

    async def _fetch_complete_window(
        self,
        client: HackerNewsClient,
        query: str,
        start_timestamp: int,
        end_timestamp: int,
    ) -> list[dict[str, Any]]:
        """Fetch a whole interval, splitting it if Algolia's result is truncated."""
        response = await client.search_stories(
            query=query,
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp,
            hits_per_page=ALGOLIA_MAX_HITS_PER_QUERY,
        )

        if not isinstance(response, dict):
            raise ValueError("HN Algolia response must be a JSON object.")

        hits = response.get("hits")
        reported_hits = response.get("nbHits")
        if (
            not isinstance(hits, list)
            or not isinstance(reported_hits, int)
            or isinstance(reported_hits, bool)
            or reported_hits < 0
        ):
            raise ValueError("HN Algolia response has an invalid hits/nbHits shape.")

        if len(hits) == reported_hits:
            return hits
        if len(hits) > reported_hits:
            raise ValueError("HN Algolia returned more hits than its nbHits count.")

        midpoint = start_timestamp + (
            (end_timestamp - start_timestamp) // 2
        )
        if midpoint <= start_timestamp or midpoint >= end_timestamp:
            raise RuntimeError(
                "HN Algolia returned an incomplete result for a one-second "
                f"or smaller interval [{start_timestamp}, {end_timestamp})."
            )

        logger.info(
            "HN Algolia returned %s of %s hits for %s in [%s, %s); "
            "splitting the interval.",
            len(hits),
            reported_hits,
            query,
            start_timestamp,
            end_timestamp,
        )
        left_hits = await self._fetch_complete_window(
            client=client,
            query=query,
            start_timestamp=start_timestamp,
            end_timestamp=midpoint,
        )
        right_hits = await self._fetch_complete_window(
            client=client,
            query=query,
            start_timestamp=midpoint,
            end_timestamp=end_timestamp,
        )
        return left_hits + right_hits

    @staticmethod
    def _to_raw_events(
        hits: list[dict[str, Any]],
    ) -> tuple[list[RawEvent], list[int]]:
        events: list[RawEvent] = []
        observed_timestamps: list[int] = []

        for hit in hits:
            if not isinstance(hit, dict):
                logger.warning("Skipping an HN Algolia hit that is not an object.")
                continue

            native_id = hit.get("objectID")
            if native_id is None or not str(native_id).strip():
                logger.warning("Skipping an HN Algolia hit without objectID.")
                continue

            # Keep the provider hit unchanged: Phase 1 normalization expects
            # its original title, created_at, url and other Algolia fields.
            events.append(
                RawEvent(
                    source_native_id=str(native_id),
                    raw_payload=hit,
                )
            )

            created_at_i = hit.get("created_at_i")
            if isinstance(created_at_i, int) and not isinstance(created_at_i, bool):
                observed_timestamps.append(created_at_i)
            else:
                logger.warning(
                    "HN Algolia hit %s has no valid created_at_i; "
                    "it will be stored but will not advance the cursor.",
                    native_id,
                )

        return events, observed_timestamps

    @staticmethod
    def _build_next_cursor(
        cursor: Cursor,
        observed_timestamps: list[int],
        initial_backfill_completed_at: int | None,
    ) -> Cursor:
        next_cursor = dict(cursor)
        next_cursor[CURSOR_INITIAL_BACKFILL_COMPLETE] = True

        if initial_backfill_completed_at is not None:
            next_cursor[CURSOR_INITIAL_BACKFILL_COMPLETED_AT] = (
                initial_backfill_completed_at
            )

        previous_watermark = next_cursor.get(CURSOR_LAST_CREATED_AT)
        if previous_watermark is not None and (
            not isinstance(previous_watermark, int)
            or isinstance(previous_watermark, bool)
        ):
            raise ValueError("HN last_created_at_i cursor value must be an integer.")

        candidates = list(observed_timestamps)
        if previous_watermark is not None:
            candidates.append(previous_watermark)

        if candidates:
            next_cursor[CURSOR_LAST_CREATED_AT] = max(candidates)

        return next_cursor
