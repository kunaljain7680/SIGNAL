import argparse
import asyncio
from pathlib import Path
import sys

# Make the backend package importable when launched from backend/.
BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from app.services.ingestion_runner import run_enabled_ingestion


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run Phase 2 source ingestion once."
    )
    parser.add_argument(
        "--source",
        choices=["all", "hn", "github", "jobs"],
        default="all",
        help="Select one source type or all active sources.",
    )
    return parser.parse_args(argv)


async def main(source_type: str | None = None) -> int:
    outcomes = await run_enabled_ingestion(source_type=source_type)
    if not outcomes:
        print("No active sources matched the requested selection.")
        return 0

    failed = False
    for outcome in outcomes:
        print(
            f"{outcome.source_name}: {outcome.status} "
            f"(fetched={outcome.fetched_count}, inserted={outcome.inserted_count}, "
            f"run_id={outcome.ingestion_run_id})"
        )
        if outcome.status != "COMPLETED":
            failed = True
            print(f"  error: {outcome.error_message}")

    return 1 if failed else 0


if __name__ == "__main__":
    args = parse_args()
    raise SystemExit(asyncio.run(main(
        source_type=None if args.source == "all" else args.source
    )))
