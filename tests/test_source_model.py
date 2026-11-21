import unittest

from sqlalchemy import CheckConstraint
from app.models.source import Source


class TestSourceModel(unittest.TestCase):
    def test_source_schema_includes_cursor_state(self) -> None:
        table = Source.__table__
        self.assertIn("cursor_state", table.columns)
        column = table.c.cursor_state
        self.assertFalse(column.nullable)
        self.assertEqual(column.server_default.arg.text, "'{}'::jsonb")

    def test_source_type_constraint_includes_all_source_types(self) -> None:
        constraint = next(
            item
            for item in Source.__table__.constraints
            if isinstance(item, CheckConstraint)
            and item.name == "check_source_type"
        )
        sql = str(constraint.sqltext)
        self.assertIn("'github'", sql)
        self.assertIn("'jobs'", sql)
        self.assertIn("'hn'", sql)


if __name__ == "__main__":
    unittest.main()
