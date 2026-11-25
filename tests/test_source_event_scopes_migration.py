import unittest
from pathlib import Path

MIGRATION = (
    Path(__file__).resolve().parents[1]
    / "backend"
    / "migrations"
    / "versions"
    / "f3c9b8a7d2e1_add_source_event_scopes.py"
)


class TestSourceEventScopesMigration(unittest.TestCase):
    def test_upgrade_and_downgrade_are_present(self):
        text = MIGRATION.read_text(encoding="utf-8")
        self.assertIn("source_event_scopes", text)
        self.assertIn("uq_source_event_scope", text)
        self.assertIn('ondelete="CASCADE"', text)
        self.assertIn('op.drop_table("source_event_scopes")', text)


if __name__ == "__main__":
    unittest.main()
