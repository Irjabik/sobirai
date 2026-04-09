# Sobirai AI News Bot (MVP)

Перед первым запуском прочитай [SECURITY.md](SECURITY.md) (токены и `.env` не коммитить).

Telegram bot that forwards AI-related posts from a curated list of Russian Telegram channels to bot subscribers.

## MVP scope implemented

- Telegram-only sources (29 channels).
- Russian content stream passthrough (no ranking).
- Commands: `/start`, `/help`, `/sources`, `/pause`, `/resume`.
- User settings:
  - `mute` on/off
  - delivery mode: `instant` or configurable `digest` interval in hours
  - exclude categories (block_only mode)
  - exclude specific channels
- News freshness window: bot delivers only posts from the last 12 hours.
- Digest format: one numbered list message (up to 10 items) with clickable links to originals.
- Digest anti-duplicates: similar/reposted entries are collapsed before sending.
- Long text truncation after `1200` chars with "read more" style cut.
- Media forwarding as available in source.
- Required source link at the end of every delivered item.
- Deduplication by `(channel_username, source_message_id)`.
- Retries with exponential backoff for delivery failures.
- Basic observability via structured logs and health stats endpoint.

## High-level architecture

- `collector` uses Telethon (user API) to poll source channels.
- `dispatcher` persists normalized posts and distributes to active users.
- `bot` (Aiogram) handles commands and user preferences.
- `scheduler` prepares digests for non-instant users.
- SQLite is used for low-cost persistence.

## Quickstart

1. Create and activate virtualenv.
2. Install dependencies:
   - `pip install -r requirements.txt`
3. Copy `.env.example` to `.env` and fill values.
4. Run:
   - `python -m app.main`

## Локальная проверка без секретов

Из корня проекта (с активированным `.venv`):

```bash
python scripts/smoke_local.py
```

## Полный запуск (нужны секреты в `.env`)

1. Отзови старый токен в BotFather, если он светился где-то кроме `.env`, и вставь **новый** `BOT_TOKEN` в `.env` вместе с `TELEGRAM_API_ID` и `TELEGRAM_API_HASH`.
2. `python -m app.main` — один раз введи код Telethon в терминале.
3. В Telegram у бота: `/start`, `/sources`, `/health`.

## Команды фильтров и режимов

- `/mode_instant`
- `/digest` — ручной дайджест прямо сейчас
- `/digest 12` — включить авто-дайджест каждые 12 часов (1-168)
- `/digest_filter_off` — отключить фильтр окна часов для дайджеста
- `/digest_filter_on` — включить фильтр окна часов обратно
- `/categories` — статусы категорий
- `/my_filters` — текущие исключения
- `/block_category новости`
- `/unblock_category новости`
- `/block_channel @username`
- `/unblock_channel @username`

## Health check script

- `python scripts/health_check.py`
- Returns non-zero exit code if failed deliveries dominate sent volume.

## Required environment

- `BOT_TOKEN` from BotFather
- `TELEGRAM_API_ID` and `TELEGRAM_API_HASH` for Telethon user session
- Optional:
  - `DATABASE_PATH` (default `./data/bot.db`)
  - `LOG_LEVEL` (default `INFO`)

## Notes

- First run of Telethon will ask for phone login in terminal and save a local session file.
- This MVP does not backfill archives; it only processes new content window while running.
- Ops docs: `docs/OPS.md`, release checklist: `docs/RELEASE_CHECKLIST.md`.
