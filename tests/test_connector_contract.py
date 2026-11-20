import unittest
import uuid

from app.ingestion.contracts import (
    AuthConfig,
    Cursor,
    RateLimitConfig,
    RawEvent,
    SourceConnector,
    SourceHealth,
)


class _MinimalConnector:
    def __init__(self, source_id: uuid.UUID) -> None:
        self.source_id = source_id

    async def fetch_since(self, cursor: Cursor) -> list[RawEvent]:
        return []

    def health_check(self) -> SourceHealth:
        return SourceHealth(status="healthy")

    def rate_limit_config(self) -> RateLimitConfig:
        return RateLimitConfig()

    def auth_config(self) -> AuthConfig:
        return AuthConfig(required=False)


class TestConnectorContract(unittest.TestCase):
    def test_raw_event_requires_identity_and_json_object(self) -> None:
        event = RawEvent("external-1", {"title": "A story"})
        self.assertEqual(event.source_native_id, "external-1")
        with self.assertRaises(ValueError):
            RawEvent(" ", {})
        with self.assertRaises(TypeError):
            RawEvent("external-2", ["not", "an", "object"])

    def test_cursor_rejects_non_object_state(self) -> None:
        self.assertEqual(Cursor().state, {})
        with self.assertRaises(TypeError):
            Cursor(state=[])

    def test_protocol_is_structural(self) -> None:
        connector = _MinimalConnector(uuid.uuid4())
        self.assertIsInstance(connector, SourceConnector)


if __name__ == "__main__":
    unittest.main()
