import unittest

from app.ingestion.connectors.github import GitHubConnector
from app.ingestion.connectors.hn_algolia import HNAlgoliaConnector
from app.ingestion.connectors.remotive import RemotiveConnector
from app.ingestion.registry import (
    CONNECTORS,
    CONNECTOR_SOURCE_IDENTITY,
    get_connector,
)


class ConnectorRegistryTests(unittest.TestCase):
    def test_registry_contains_hn_remotive_and_approved_github(self):
        self.assertEqual(set(CONNECTORS), {"hn_algolia", "remotive", "github"})
        self.assertIsInstance(get_connector("hn_algolia"), HNAlgoliaConnector)
        self.assertIsInstance(get_connector("remotive"), RemotiveConnector)
        self.assertIsInstance(get_connector("github"), GitHubConnector)

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

    def test_github_connector_maps_to_seeded_database_source(self):
        self.assertEqual(
            CONNECTOR_SOURCE_IDENTITY["github"],
            ("GitHub", "github"),
        )

    def test_firebase_prototype_is_not_registered(self):
        self.assertNotIn("firebase", CONNECTORS)
        self.assertNotIn("hn_firebase", CONNECTORS)

    def test_unknown_connector_raises_clear_error(self):
        with self.assertRaisesRegex(KeyError, "Unknown source connector"):
            get_connector("missing")


if __name__ == "__main__":
    unittest.main()
