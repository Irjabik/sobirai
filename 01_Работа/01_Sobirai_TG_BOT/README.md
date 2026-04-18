# Sobirai AI News Bot (MVP)

Перед первым запуском прочитай [SECURITY.md](SECURITY.md) (токены и `.env` не коммитить).

**Контекст проекта, два репозитория GitHub и выкат на Bothost:** [docs/CONTEXT_AND_WORKFLOW.md](docs/CONTEXT_AND_WORKFLOW.md).

Telegram bot that forwards AI-related posts from a curated list of Russian Telegram channels to bot subscribers.

## MVP scope implemented

- Автопостинг в **Telegram-канал** (опционально, отдельный контур): парсер как раньше пишет в `source_posts`, фоновый цикл переписывает текст через **Groq**, режет дубли, публикует в канал с лимитом **N постов/сутки UTC**. Включается `ENABLE_CHANNEL_AUTOPUBLISH=1`, см. `.env.example` и раздел ниже.
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

Из корня проекта (нужен `aiosqlite` из `requirements.txt`, удобно через venv):

```bash
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
.venv/bin/python scripts/smoke_local.py
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

Полный шаблон переменных: [.env.example](.env.example).

- `BOT_TOKEN` from BotFather
- `TELEGRAM_API_ID` and `TELEGRAM_API_HASH` for Telethon user session
- `X_API_BEARER_TOKEN` — **обязателен**, если `ENABLE_X_SOURCES=1` (по умолчанию включено). Если X не нужен, задай `ENABLE_X_SOURCES=0` и оставь токен пустым.
- Optional:
  - `DATABASE_PATH` (default `./data/bot.db`)
  - `LOG_LEVEL` (default `INFO`)
  - `X_API_FETCH_INTERVAL_SECONDS` (default `60`)
  - `X_API_SOURCES_PER_TICK` (default `1`)
  - `X_API_MAX_PAGES_PER_SOURCE` (default `1`)
  - `X_API_MAX_RESULTS` (default `20`)
  - `X_API_MAX_REQUESTS_PER_HOUR` (default `120`)

### Автопостинг в канал (Groq + Bot API)

1. Создай канал, добавь бота **администратором** с правом **Post messages**.
2. Узнай `CHANNEL_CHAT_ID` (число вида `-100...`: через `@userinfobot`, логи или Bot API).
3. В `.env`: `ENABLE_CHANNEL_AUTOPUBLISH=1`, `CHANNEL_CHAT_ID=...`, `GROQ_API_KEY=...`, остальное по `.env.example`.
4. Запуск `python -m app.main` — отдельный цикл с периодом `CHANNEL_POLL_SECONDS` обрабатывает новые строки из `source_posts`.
5. Статусы и лимит (UTC): таблицы `generated_channel_posts`, `publish_daily_counters`. В `/health` добавлены агрегаты по каналу.

**Smoke (ручной, с сетью):** после шагов выше дождись нового поста в источниках или временно уменьши `CHANNEL_POLL_SECONDS`, проверь появление сообщения в канале и строку `published` в БД. Локально без сети: `scripts/smoke_local.py` проверяет миграции таблиц и дедуп-хелпер.

**Риски MVP:** один процесс, лимит суток без жесткой транзакции на гонку; near-dup эвристический; при `response_format=json_object` старые модели Groq могут вернуть ошибку — смотри логи `groq_http_*`.

## Как снизить расход X API

- Начни с консервативных значений:
  - `X_API_FETCH_INTERVAL_SECONDS=300`
  - `X_API_SOURCES_PER_TICK=1`
  - `X_API_MAX_PAGES_PER_SOURCE=1`
  - `X_API_MAX_RESULTS=20`
  - `X_API_MAX_REQUESTS_PER_HOUR=60`
- Если `x_collected_posts` растет слишком медленно — постепенно уменьшай интервал (`300 -> 180 -> 120`).
- Следи за `/health`: ключевые индикаторы `x_requests_per_post`, `x_api_requests_last_hour`, `x_api_cache_hits/misses`.

## Notes

- First run of Telethon will ask for phone login in terminal and save a local session file.
- This MVP does not backfill archives; it only processes new content window while running.
- Ops docs: `docs/OPS.md`, release checklist: `docs/RELEASE_CHECKLIST.md`.
