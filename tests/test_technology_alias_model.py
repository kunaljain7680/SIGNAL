import unittest

from sqlalchemy import Index, UniqueConstraint

from app.models.technology_alias import TechnologyAlias


class TestTechnologyAliasModel(unittest.TestCase):
    def test_alias_schema_matches_architecture(self) -> None:
        table = TechnologyAlias.__table__

        self.assertEqual(
            set(table.columns.keys()),
            {"id", "technology_id", "alias_text"},
        )

        technology_fk = next(iter(table.c.technology_id.foreign_keys))
        self.assertEqual(technology_fk.target_fullname, "technologies.id")
        self.assertEqual(technology_fk.ondelete, "CASCADE")

        unique_constraint = next(
            constraint
            for constraint in table.constraints
            if isinstance(constraint, UniqueConstraint)
            and constraint.name == "uq_technology_alias_text"
        )
        self.assertEqual(
            [column.name for column in unique_constraint.columns],
            ["alias_text"],
        )

        trigram_index = next(
            index
            for index in table.indexes
            if isinstance(index, Index) and index.name == "idx_alias_trgm"
        )
        self.assertEqual(
            trigram_index.dialect_options["postgresql"]["using"],
            "gin",
        )
        self.assertEqual(
            trigram_index.dialect_options["postgresql"]["ops"],
            {"alias_text": "gin_trgm_ops"},
        )


if __name__ == "__main__":
    unittest.main()
