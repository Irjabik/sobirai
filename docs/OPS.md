# Ops and Reliability Runbook (MVP)

## Core runtime loops

- Collector loop: every 10s pulls new posts and dispatches `instant` mode.
- Configurable digest loop: every minute checks users with `digest` mode and sends when personal interval is due.

## Failure handling

- Delivery retries: 3 attempts per message with exponential backoff `1s -> 2s -> 4s`.
- Any final failure is stored in `delivery_events` with `status=failed` and `last_error`.
- Collector per-channel errors are logged and do not stop other channels.

## Deduplication strategy

- Unique database key: `(channel_username, source_message_id)` in `source_posts`.
- Repeated collection attempts are ignored by `INSERT OR IGNORE`.
- Delivery uniqueness key: `(user_id, source_post_id)`.

## Monitoring checklist

- Run `/health` in bot chat to verify current counters.
- Watch logs for:
  - repeated `Collect failed` on same channel
  - rising `failed_messages`
  - abnormal retry growth

## Incident quick actions

1. If instant mode creates overload:
   - ask users to switch to auto-digest via `/digest <hours>` (e.g. `/digest 12`).
2. If one source consistently fails:
   - keep service running, isolate source issue later.
3. If Telegram API sends frequent errors:
   - temporary pause rollouts, restart process, verify tokens/session.

