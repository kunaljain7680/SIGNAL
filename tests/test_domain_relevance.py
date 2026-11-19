import unittest

from app.services.domain_relevance import calculate_domain_relevance


class TestDomainRelevance(unittest.TestCase):
    def test_all_technology_domains_match(self) -> None:
        score = calculate_domain_relevance(
            technology_domain_tags=["backend", "cloud"],
            target_domains=["backend", "cloud"],
            interests=[],
        )
        self.assertEqual(score, 1.0)

    def test_partial_overlap_is_a_fraction_of_technology_tags(self) -> None:
        score = calculate_domain_relevance(
            technology_domain_tags=["backend", "cloud", "ai"],
            target_domains=["backend"],
            interests=["ai"],
        )
        self.assertAlmostEqual(score, 2 / 3)

    def test_interests_contribute_to_domain_match(self) -> None:
        score = calculate_domain_relevance(
            technology_domain_tags=["backend", "ai"],
            target_domains=["backend"],
            interests=["ai"],
        )
        self.assertEqual(score, 1.0)

    def test_no_overlap_returns_zero(self) -> None:
        score = calculate_domain_relevance(
            technology_domain_tags=["backend"],
            target_domains=["mobile"],
            interests=["data"],
        )
        self.assertEqual(score, 0.0)

    def test_empty_technology_tags_returns_zero(self) -> None:
        score = calculate_domain_relevance(
            technology_domain_tags=[],
            target_domains=["backend"],
            interests=["ai"],
        )
        self.assertEqual(score, 0.0)

    def test_comparison_ignores_case_and_outer_whitespace(self) -> None:
        score = calculate_domain_relevance(
            technology_domain_tags=[" Backend ", "AI"],
            target_domains=["backend"],
            interests=[" ai "],
        )
        self.assertEqual(score, 1.0)


if __name__ == "__main__":
    unittest.main()
