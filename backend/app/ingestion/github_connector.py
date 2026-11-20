from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from typing import Any
import uuid

from app.clients.github_client import GitHubClient
from app.ingestion.contracts import (
    AuthConfig,
    Cursor,
    RateLimitConfig,
    RawEvent,
    SourceHealth,
)


class GitHubConnector:
    """
    Collect repository snapshots and releases for tracked technology topics.
    Repository and release ETags are persisted in the source cursor.
    """

    def __init__(
        self,
        source_id: uuid.UUID,
        technology_topics: list[str],
        token: str | None = None,
        client: Any | None = None,
        max_repositories_per_topic: int = 10,
    ) -> None:
        if not technology_topics:
            raise ValueError("At least one GitHub technology topic is required.")
        if max_repositories_per_topic < 1:
            raise ValueError("max_repositories_per_topic must be positive.")

        self.source_id = source_id
        self.technology_topics = list(dict.fromkeys(
            topic.strip().lower()
            for topic in technology_topics
            if topic and topic.strip()
        ))
        if not self.technology_topics:
            raise ValueError("GitHub technology topics must not be empty.")

        self.max_repositories_per_topic = max_repositories_per_topic
        self._owns_client = client is None and bool(token)
        self.client = client or (GitHubClient(token=token) if token else None)

    async def fetch_since(self, cursor: Cursor) -> list[RawEvent]:
        if self.client is None:
            raise RuntimeError(
                "GitHub ingestion requires GITHUB_TOKEN. Configure it as an "
                "environment variable; secrets are not stored in sources.config."
            )

        next_state = deepcopy(cursor.state)
        github_state = next_state.setdefault("github", {})
        topic_state = github_state.setdefault("topics", {})
        repository_state = github_state.setdefault("repositories", {})

        repo_topics: dict[str, set[str]] = {}
        for topic in self.technology_topics:
            response = await self.client.search_repositories(
                topic=topic,
                per_page=self.max_repositories_per_topic,
            )
            if response.status_code == 304:
                topic_state[topic] = {
                    "last_checked_at": datetime.now(timezone.utc).isoformat()
                }
                continue

            payload = response.json()
            if not isinstance(payload, dict) or not isinstance(
                payload.get("items"), list
            ):
                raise ValueError(
                    f"GitHub repository search returned an invalid payload for {topic!r}."
                )

            for repository in payload["items"]:
                if not isinstance(repository, dict):
                    continue
                repository_id = repository.get("id")
                full_name = repository.get("full_name")
                if repository_id is None or not isinstance(full_name, str):
                    continue
                key = str(repository_id)
                repo_topics.setdefault(key, set()).add(topic)
                repo_topics[key].add(full_name)

            topic_state[topic] = {
                "last_checked_at": datetime.now(timezone.utc).isoformat()
            }

        raw_events: dict[str, RawEvent] = {}

        for repository_key, topic_and_name in repo_topics.items():
            full_name = next(value for value in topic_and_name if "/" in value)
            state = repository_state.setdefault(repository_key, {})
            repository_response = await self.client.get(
                f"/repos/{full_name}",
                etag=state.get("repository_etag"),
            )

            if repository_response.status_code != 304:
                repository = repository_response.json()
                if not isinstance(repository, dict):
                    raise ValueError(
                        f"GitHub repository endpoint returned invalid JSON for {full_name}."
                    )
                actual_id = repository.get("id")
                if actual_id is None:
                    raise ValueError(
                        f"GitHub repository payload is missing its ID for {full_name}."
                    )

                etag = repository_response.headers.get("ETag")
                state["repository_etag"] = etag
                native_version = etag or (
                    f"{repository.get('updated_at', '')}:"
                    f"{repository.get('stargazers_count', '')}:"
                    f"{repository.get('forks_count', '')}"
                )
                native_id = f"repository:{actual_id}:{native_version}"
                raw_events.setdefault(
                    native_id,
                    RawEvent(
                        source_native_id=native_id,
                        raw_payload=repository,
                    ),
                )

            releases_response = await self.client.get(
                f"/repos/{full_name}/releases",
                params={"per_page": 10},
                etag=state.get("releases_etag"),
            )
            if releases_response.status_code != 304:
                releases = releases_response.json()
                if not isinstance(releases, list):
                    raise ValueError(
                        f"GitHub releases endpoint returned invalid JSON for {full_name}."
                    )
                state["releases_etag"] = releases_response.headers.get("ETag")
                for release in releases:
                    if not isinstance(release, dict) or release.get("id") is None:
                        continue
                    native_id = f"release:{release['id']}"
                    raw_events.setdefault(
                        native_id,
                        RawEvent(
                            source_native_id=native_id,
                            raw_payload=release,
                        ),
                    )

        cursor.state = next_state
        return list(raw_events.values())

    def health_check(self) -> SourceHealth:
        if self.client is None:
            return SourceHealth(
                status="down",
                message="GITHUB_TOKEN is not configured.",
            )
        return SourceHealth(
            status="unknown",
            message="No live GitHub request performed by configuration check.",
        )

    def rate_limit_config(self) -> RateLimitConfig:
        return RateLimitConfig(
            requests_per_window=5000,
            window_seconds=3600,
            minimum_interval_seconds=2.1,
        )

    def auth_config(self) -> AuthConfig:
        return AuthConfig(
            required=True,
            environment_variable="GITHUB_TOKEN",
        )

    async def aclose(self) -> None:
        if self._owns_client and self.client is not None:
            await self.client.aclose()
