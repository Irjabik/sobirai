"""Перед запуском бота после простоя: не раздавать накопленные в БД посты.

Все запросы доставки отсекают посты с датой раньше users.started_at.
Сдвиг started_at на «сейчас» для всех пользователей = «подписка началась с этого момента».

Плюс обновляется user_settings.last_digest_sent_at, чтобы цикл дайджестов не
отправил сразу всем просроченный дайджест по старому таймеру.

Запуск (из каталога 01_ai_news_bot_mvp, с .env и DATABASE_PATH):

  python scripts/skip_delivery_backlog.py
  python scripts/skip_delivery_backlog.py --dry-run
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="только показать время и число пользователей, без UPDATE",
    )
    args = parser.parse_args()

    from app.config import Settings
    from app.db import Database

    settings = Settings.from_env()
    db = Database(settings.database_path)
    await db.connect()
    now = Database._now()
    try:
        async with db.conn.execute("SELECT COUNT(*) AS c FROM users") as cur:
            row = await cur.fetchone()
        n = int(row["c"]) if row else 0
        if args.dry_run:
            print(f"dry-run: users={n}, timestamp={now}")
            print("would run: UPDATE users SET started_at = ?")
            print(
                "would run: UPDATE user_settings SET last_digest_sent_at=?, updated_at=?"
            )
            return
        await db.conn.execute("UPDATE users SET started_at = ?", (now,))
        await db.conn.execute(
            "UPDATE user_settings SET last_digest_sent_at = ?, updated_at = ?",
            (now, now),
        )
        await db.conn.commit()
        print(f"ok: updated started_at + last_digest_sent_at for all ({n} users), t={now}")
    finally:
        await db.close()


if __name__ == "__main__":
    asyncio.run(main())
