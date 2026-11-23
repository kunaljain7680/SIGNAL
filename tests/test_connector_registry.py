import unittest

from app.ingestion.connectors.hn_algolia import HNAlgoliaConnector
from app.ingestion.connectors.remotive import RemotiveConnector
from app.ingestion.registry import (
    CONNECTORS,
    CONNECTOR_SOURCE_IDENTITY,
    get_connector,
)


class ConnectorRegistryTests(unittest.TestCase):
    def test_registry_contains_approved_hn_and_remotive_connectors(self):
        self.assertEqual(set(CONNECTORS), {"hn_algolia", "remotive"})
        self.assertIsInstance(get_connector("hn_algolia"), HNAlgoliaConnector)
        self.assertIsInstance(get_connector("remotive"), RemotiveConnector)

    def test_hn_connector_maps_to_existing_database_source(self):
        self.assertEqual(
            CONNECTOR_SOURCE_IDENTITY["hn_algolia"],
            ("Hacker News", "hn"),
        )

    def test_remotive_connector_maps_to_existing_database_source(self):
        self.assertEqual(
            CONNECTOR_SOURCE_IDENTITY["remotive"],
            ("Remotive", "jobs"),
        )

    def test_github_draft_and_firebase_prototype_are_not_registered(self):
        self.assertNotIn("github", CONNECTORS)
        self.assertNotIn("firebase", CONNECTORS)
        self.assertNotIn("hn_firebase", CONNECTORS)

    def test_unknown_connector_raises_clear_error(self):
        with self.assertRaisesRegex(KeyError, "Unknown source connector"):
            get_connector("missing")


if __name__ == "__main__":
    unittest.main()
