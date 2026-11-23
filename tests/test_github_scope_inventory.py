import json
import unittest
from pathlib import Path


INVENTORY_PATH = (
    Path(__file__).resolve().parents[1]
    / "backend"
    / "app"
    / "ingestion"
    / "scopes"
    / "github_scopes.json"
)


class GitHubScopeInventoryTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.inventory = json.loads(INVENTORY_PATH.read_text(encoding="utf-8"))

    def test_inventory_contains_all_28_seeded_technologies(self):
        technologies = self.inventory["technologies"]

        self.assertEqual(len(technologies), 28)
        self.assertEqual(
            len({item["technology_slug"] for item in technologies}),
            28,
        )

    def test_inventory_stays_within_architecture_caps(self):
        technologies = self.inventory["technologies"]
        repository_count = sum(
            len(item["repositories"]) for item in technologies
        )
        topic_count = sum(
            item["topic"] is not None for item in technologies
        )

        self.assertLessEqual(
            repository_count,
            self.inventory["limits"]["max_repositories"],
        )
        self.assertLessEqual(
            topic_count,
            self.inventory["limits"]["max_topics"],
        )

    def test_only_five_verified_repository_starters_are_present(self):
        repositories = [
            repository
            for item in self.inventory["technologies"]
            for repository in item["repositories"]
        ]

        self.assertEqual(len(repositories), 5)
        self.assertTrue(all(repository["verified"] for repository in repositories))
        self.assertEqual(
            {
                f'{repository["owner"]}/{repository["name"]}'
                for repository in repositories
            },
            {
                "python/cpython",
                "fastapi/fastapi",
                "django/django",
                "pallets/flask",
                "pytorch/pytorch",
            },
        )

    def test_inventory_is_not_approved_for_live_fetching(self):
        self.assertEqual(self.inventory["approval_status"], "draft")
        self.assertTrue(self.inventory["owner_review_required"])

    def test_unverified_topic_scopes_are_not_guessed(self):
        self.assertTrue(
            all(item["topic"] is None for item in self.inventory["technologies"])
        )


if __name__ == "__main__":
    unittest.main()
