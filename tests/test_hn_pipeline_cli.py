import unittest

from scripts.run_hn_pipeline import parse_args


class TestHNPipelineCLI(unittest.TestCase):
    def test_defaults_preserve_fastapi_manual_run(self) -> None:
        args = parse_args([])

        self.assertEqual(args.technology, "FastAPI")
        self.assertEqual(args.hits_per_page, 100)

    def test_accepts_another_seeded_technology(self) -> None:
        args = parse_args(
            ["--technology", "PyTorch", "--hits-per-page", "50"]
        )

        self.assertEqual(args.technology, "PyTorch")
        self.assertEqual(args.hits_per_page, 50)


if __name__ == "__main__":
    unittest.main()
