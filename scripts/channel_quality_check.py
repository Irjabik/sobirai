from __future__ import annotations

import argparse
import asyncio
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

sys.path.append(str(Path(__file__).resolve().parents[1]))

from app.db import Database  # noqa: E402


def _status_from_ratio(value: float, warn: float, crit: float) -> str:
    if value >= crit:
        return "crit"
    if value >= warn:
        return "warn"
    return "ok"


async def main() -> int:
    parser = argparse.ArgumentParser(description="Lightweight channel dedup quality check")
    parser.add_argument("--window-hours", type=int, default=24)
    parser.add_argument("--sample-size", type=int, default=200)
    parser.add_argument("--warn-dup-ratio", type=float, default=0.55)
    parser.add_argument("--crit-dup-ratio", type=float, default=0.75)
    parser.add_argument("--warn-fail-ratio", type=float, default=0.10)
    parser.add_argument("--crit-fail-ratio", type=float, default=0.20)
    args = parser.parse_args()

    load_dotenv()
    db_path = Path(os.getenv("DATABASE_PATH", "./data/bot.db"))
    db = Database(db_path)
    await db.connect()
    try:
        q = """
        SELECT status, error, updated_at, published_at
        FROM generated_channel_posts
        WHERE datetime(updated_at) >= datetime('now', ?)
        ORDER BY datetime(updated_at) DESC
        LIMIT ?
        """
        interval = f"-{max(1, int(args.window_hours))} hour"
        async with db.conn.execute(q, (interval, max(20, int(args.sample_size)))) as cur:
            rows = await cur.fetchall()

        total = len(rows)
        duplicates = 0
        failed = 0
        published = 0
        reasons = {"exact": 0, "near": 0, "post_llm": 0, "link_overlap": 0, "topic_memory": 0, "other": 0}
        for r in rows:
            st = str(r["status"] or "")
            err = str(r["error"] or "")
            if st == "duplicate":
                duplicates += 1
                if err == "exact_fingerprint_match":
                    reasons["exact"] += 1
                elif err == "link_overlap_duplicate":
                    reasons["link_overlap"] += 1
                elif err.startswith("near_duplicate_jaccard>="):
                    reasons["near"] += 1
                elif err.startswith("topic_memory_") or err.startswith("post_llm_topic_memory_"):
                    reasons["topic_memory"] += 1
                elif err.startswith("post_llm_"):
                    reasons["post_llm"] += 1
                else:
                    reasons["other"] += 1
            elif st == "failed":
                failed += 1
            elif st == "published":
                published += 1

        dup_ratio = (duplicates / total) if total else 0.0
        fail_ratio = (failed / total) if total else 0.0
        dup_state = _status_from_ratio(dup_ratio, args.warn_dup_ratio, args.crit_dup_ratio)
        fail_state = _status_from_ratio(fail_ratio, args.warn_fail_ratio, args.crit_fail_ratio)
        states = [dup_state, fail_state]
        final_state = "crit" if "crit" in states else ("warn" if "warn" in states else "ok")

        print("channel_quality_report")
        print(f"window_hours={args.window_hours} sample_size={args.sample_size} rows={total}")
        print(f"published={published} duplicate={duplicates} failed={failed}")
        print(f"duplicate_ratio={dup_ratio:.4f} status={dup_state}")
        print(f"failed_ratio={fail_ratio:.4f} status={fail_state}")
        print(f"duplicate_reasons={reasons}")
        print(f"quality_status={final_state}")
        if final_state == "crit":
            return 2
        if final_state == "warn":
            return 1
        return 0
    finally:
        await db.close()


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
