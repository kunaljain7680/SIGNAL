import unittest

from app.ingestion.contracts import (
    AuthConfig,
    Cursor,
    FetchResult,
    RawEvent,
    RateLimitConfig,
    SourceConnector,
    SourceHealth,
)


class FakeConnector:
    name = "fake"
    rate_limit = RateLimitConfig(
        min_run_interval_s=0,
        min_request_interval_s=0.0,
    )
    auth = AuthConfig(required_env=())

    async def scopes(self) -> list[str]:
        return ["scope-a"]

    async def fetch_since(
        self, scope_key: str, cursor: Cursor
    ) -> FetchResult:
        if scope_key != "scope-a":
            raise ValueError(f"Unknown scope: {scope_key}")

        event = RawEvent(
            source_native_id="fake-001",
            raw_payload={"value": "fixture"},
        )
        return FetchResult(
            events=[event],
            next_cursor={"last_seen": "fake-001"},
        )


class ConnectorContractTests(unittest.IsolatedAsyncioTestCase):
    async def test_fake_connector_exposes_contract_attributes(self):
        connector: SourceConnector = FakeConnector()

        self.assertEqual(connector.name, "fake")
        self.assertEqual(connector.rate_limit.min_run_interval_s, 0)
        self.assertEqual(connector.rate_limit.min_request_interval_s, 0.0)
        self.assertEqual(connector.auth.required_env, ())

    async def test_source_health_values_match_architecture(self):
        self.assertEqual(
            {health.value for health in SourceHealth},
            {"healthy", "degraded", "down", "unknown"},
        )

    async def test_scopes_returns_scope_keys(self):
        connector = FakeConnector()

        self.assertEqual(await connector.scopes(), ["scope-a"])

    async def test_fetch_returns_raw_events_and_next_cursor(self):
        connector = FakeConnector()

        result = await connector.fetch_since("scope-a", {})

        self.assertEqual(len(result.events), 1)
        self.assertEqual(result.events[0].source_native_id, "fake-001")
        self.assertEqual(result.events[0].raw_payload, {"value": "fixture"})
        self.assertEqual(result.next_cursor, {"last_seen": "fake-001"})

    async def test_unknown_scope_raises(self):
        connector = FakeConnector()

        with self.assertRaisesRegex(ValueError, "Unknown scope"):
            await connector.fetch_since("missing-scope", {})


if __name__ == "__main__":
    unittest.main()
