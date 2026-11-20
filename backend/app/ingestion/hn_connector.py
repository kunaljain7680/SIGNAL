from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta, timezone
from typing import Any
import uuid

from app.clients.hn_client import HackerNewsClient
from app.ingestion.contracts import (
    AuthConfig,
    Cursor,
    RateLimitConfig,
    RawEvent,
    SourceHealth,
)


class HackerNewsConnector:
    """
    Phase 2 adapter for the existing HN Algolia client.

    It fetches each tracked keyword from its saved timestamp, paginates the
    date-sorted result, and returns raw hits without changing Phase 1 scoring.
    """

    DEFAULT_LOOKBACK = timedelta(days=7)
    TIMESTAMP_OVERLAP_SECONDS = 1

    def __init__(
        self,
        source_id: uuid.UUID,
        keywords: list[str],
        client: Any | None = None,
        hits_per_page: int = 100,
    ) -> None:
        if not keywords:
            raise ValueError("At least one HN keyword is required.")
        if hits_per_page < 1:
            raise ValueError("hits_per_page must be positive.")

        cleaned_keywords = []
        seen = set()
        for keyword in keywords:
            value = keyword.strip()
            if value and value.casefold() not in seen:
                cleaned_keywords.append(value)
                seen.add(value.casefold())
        if not cleaned_keywords:
            raise ValueError("HN keywords must contain non-empty values.")

        self.source_id = source_id
        self.keywords = cleaned_keywords
        self.hits_per_page = hits_per_page
        self._owns_client = client is None
        self.client = client or HackerNewsClient()

    async def fetch_since(self, cursor: Cursor) -> list[RawEvent]:
        now_timestamp = int(datetime.now(timezone.utc).timestamp())
        next_state = deepcopy(cursor.state)
        hn_state = next_state.setdefault("hn", {})
        keyword_state = hn_state.setdefault("keywords", {})
        raw_events: dict[str, RawEvent] = {}

        for keyword in self.keywords:
            saved = keyword_state.get(keyword, {})
            saved_timestamp = saved.get("last_search_timestamp")

            if saved_timestamp is None:
                start_timestamp = int(
                    (datetime.now(timezone.utc) - self.DEFAULT_LOOKBACK).timestamp()
                )
            else:
                start_timestamp = max(
                    0,
                    int(saved_timestamp) - self.TIMESTAMP_OVERLAP_SECONDS,
                )

            page = 0
            page_count: int | None = None

            while page_count is None or page < page_count:
                result = await self.client.search_stories_by_date(
                    query=keyword,
                    start_timestamp=start_timestamp,
                    end_timestamp=now_timestamp,
                    hits_per_page=self.hits_per_page,
                    page=page,
                )
                hits = result.get("hits", [])
                if not isinstance(hits, list):
                    raise ValueError(
                        f"HN Algolia returned invalid hits for {keyword!r}."
                    )

                for hit in hits:
                    if not isinstance(hit, dict) or "objectID" not in hit:
                        continue
                    native_id = str(hit["objectID"])
                    raw_events.setdefault(
                        native_id,
                        RawEvent(
                            source_native_id=native_id,
                            raw_payload=hit,
                        ),
                    )

                if page_count is None:
                    reported_pages = result.get("nbPages")
                    if reported_pages is not None:
                        page_count = max(1, int(reported_pages))
                    elif len(hits) < self.hits_per_page:
                        page_count = page + 1

                if not hits:
                    break
                page += 1

            keyword_state[keyword] = {
                "last_search_timestamp": now_timestamp,
            }

        # Publish the updated state only after all keywords and pages succeed.
        cursor.state = next_state
        return list(raw_events.values())

    def health_check(self) -> SourceHealth:
        return SourceHealth(
            status="unknown",
            message="No live HN request performed by configuration check.",
        )

    def rate_limit_config(self) -> RateLimitConfig:
        return RateLimitConfig(minimum_interval_seconds=0.1)

    def auth_config(self) -> AuthConfig:
        return AuthConfig(required=False)

    async def aclose(self) -> None:
        if self._owns_client:
            await self.client.aclose()
