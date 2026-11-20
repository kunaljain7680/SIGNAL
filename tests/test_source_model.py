import unittest

from sqlalchemy.dialects.postgresql import JSONB

from app.models.source import Source


class TestSourceModel(unittest.TestCase):
    def test_source_has_separate_non_nullable_cursor_state(self) -> None:
        table = Source.__table__

        self.assertIn("cursor_state", table.columns)
        cursor_state = table.c.cursor_state

        self.assertIsInstance(cursor_state.type, JSONB)
        self.assertFalse(cursor_state.nullable)
        self.assertEqual(cursor_state.server_default.arg.text, "'{}'::jsonb")

    def test_source_config_remains_separate_from_cursor_state(self) -> None:
        table = Source.__table__

        self.assertIsInstance(table.c.config.type, JSONB)
        self.assertIsInstance(table.c.cursor_state.type, JSONB)
        self.assertIsNot(table.c.config, table.c.cursor_state)


if __name__ == "__main__":
    unittest.main()
