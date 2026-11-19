import unittest

from sqlalchemy import CheckConstraint
from app.models.ingestion_run import IngestionRun


class TestIngestionRunModel(unittest.TestCase):
    def test_ingestion_run_schema(self) -> None:
        table = IngestionRun.__table__

        self.assertEqual(
            set(table.columns.keys()),
            {
                "id",
                "source_id",
                "status",
                "current_step",
                "started_at",
                "completed_at",
                "error_message",
                "retry_count",
            },
        )

        source_fk = next(iter(table.c.source_id.foreign_keys))
        self.assertEqual(source_fk.target_fullname, "sources.id")

        status_constraint = next(
            constraint
            for constraint in table.constraints
            if isinstance(constraint, CheckConstraint)
            and constraint.name == "check_ingestion_run_status"
        )
        self.assertIn("DEAD_LETTER", str(status_constraint.sqltext))
        self.assertIn("COMPLETED", str(status_constraint.sqltext))

        self.assertEqual(table.c.current_step.server_default.arg.text, "'INIT'")
        self.assertEqual(table.c.retry_count.server_default.arg.text, "0")


if __name__ == "__main__":
    unittest.main()
