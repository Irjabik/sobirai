from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

sys.path.append(str(Path(__file__).resolve().parents[1]))

from app.db import Database  # noqa: E402


async def main() -> int:
    load_dotenv()
    db_path = Path(os.getenv("DATABASE_PATH", "./data/bot.db"))
    db = Database(db_path)
    await db.connect()
    try:
        stats = await db.health_stats()
        failed = stats.get("delivery_status", {}).get("failed", 0)
        sent = stats.get("delivery_status", {}).get("sent", 0)
        print("health_stats:", stats)
        if failed > sent and failed > 50:
            print("ERROR: failed deliveries exceed sent volume")
            return 2
        return 0
    finally:
        await db.close()


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))

