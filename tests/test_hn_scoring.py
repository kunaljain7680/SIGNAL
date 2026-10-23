import unittest

from app.services.hn_scoring import calculate_hn_v0_score


class TestHNScoring(unittest.TestCase):
    def test_calculate_hn_v0_score_boundaries(self) -> None:
        # event_count <= 0 -> 0
        self.assertEqual(calculate_hn_v0_score(-1), 0)
        self.assertEqual(calculate_hn_v0_score(0), 0)

        # 1–2 -> 25
        self.assertEqual(calculate_hn_v0_score(1), 25)
        self.assertEqual(calculate_hn_v0_score(2), 25)

        # 3–5 -> 50
        self.assertEqual(calculate_hn_v0_score(3), 50)
        self.assertEqual(calculate_hn_v0_score(5), 50)

        # 6–9 -> 75
        self.assertEqual(calculate_hn_v0_score(6), 75)
        self.assertEqual(calculate_hn_v0_score(9), 75)

        # 10 or more -> 100
        self.assertEqual(calculate_hn_v0_score(10), 100)
        self.assertEqual(calculate_hn_v0_score(15), 100)


if __name__ == "__main__":
    unittest.main()