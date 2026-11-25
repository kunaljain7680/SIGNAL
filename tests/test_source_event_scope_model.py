import unittest
from sqlalchemy import UniqueConstraint
from app.models.source_event_scope import SourceEventScope


class TestSourceEventScopeModel(unittest.TestCase):
    def test_schema_and_scope_uniqueness(self):
        table = SourceEventScope.__table__
        self.assertEqual(set(table.columns.keys()), {"id", "source_event_id", "scope_key", "recorded_at"})
        fk = next(iter(table.c.source_event_id.foreign_keys))
        self.assertEqual(fk.target_fullname, "source_events.id")
        self.assertEqual(fk.ondelete, "CASCADE")
        uq = next(c for c in table.constraints if isinstance(c, UniqueConstraint))
        self.assertEqual(list(uq.columns.keys()), ["source_event_id", "scope_key"])


if __name__ == "__main__":
    unittest.main()
