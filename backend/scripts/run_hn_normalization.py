"""Normalize pending Hacker News events using persisted discovery scopes."""

import asyncio
from pathlib import Path
import sys

# Ensure app is importable when this script is run from backend/.
BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from app.core.database import AsyncSessionLocal
from app.services.hn_normalization import normalize_hn_scoped_events


async def main() -> None:
    async with AsyncSessionLocal() as db:
        inserted = await normalize_hn_scoped_events(db)

    print(
        "Completed HN scope-based normalization: "
        f"normalized_inserted={inserted}"
    )


if __name__ == "__main__":
    asyncio.run(main())
