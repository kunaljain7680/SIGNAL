import unittest
from pathlib import Path


MIGRATION_PATH = (
    Path(__file__).resolve().parents[1]
    / "backend"
    / "migrations"
    / "versions"
    / "d6f0a21b9c43_add_phase2_source_cursor_state.py"
)


class TestPhase2SourceMigration(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.migration = MIGRATION_PATH.read_text(encoding="utf-8")

    def test_adds_non_nullable_jsonb_cursor(self) -> None:
        self.assertIn('"cursor_state"', self.migration)
        self.assertIn("postgresql.JSONB", self.migration)
        self.assertIn("nullable=False", self.migration)
        self.assertIn("'{}'::jsonb", self.migration)

    def test_adds_failed_status(self) -> None:
        self.assertIn("'COMPLETED', 'FAILED', 'RETRYING', 'DEAD_LETTER'", self.migration)

    def test_seeds_exact_source_rows_and_intervals(self) -> None:
        self.assertIn("('GitHub', 'github'", self.migration)
        self.assertIn("('Remotive', 'jobs'", self.migration)
        self.assertIn("86400", self.migration)
        self.assertIn("21600", self.migration)
        self.assertIn("config = config ||", self.migration)
        self.assertIn("WHERE name = 'Hacker News' AND type = 'hn'", self.migration)

    def test_downgrade_reverses_changes(self) -> None:
        self.assertRegex(
            self.migration,
            r'op\.drop_column\(\s*"sources",\s*"cursor_state",?\s*\)',
        )
        self.assertIn("DELETE FROM sources", self.migration)
        self.assertIn("config = config - 'schedule_interval_s'", self.migration)


if __name__ == "__main__":
    unittest.main()
