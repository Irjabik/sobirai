# Release Checklist (MVP)

## 1) Local smoke

- [ ] `.env` configured (`BOT_TOKEN`, `TELEGRAM_API_ID`, `TELEGRAM_API_HASH`).
- [ ] Bot starts with `python -m app.main`.
- [ ] Telethon session login completed.
- [ ] Commands respond: `/start`, `/help`, `/sources`, `/pause`, `/resume`.
- [ ] Mode switching works: `/mode_instant`, `/digest 12` (or other hours).
- [ ] Filters work: `/block_category`, `/unblock_category`, `/block_channel`, `/unblock_channel`, `/my_filters`.
- [ ] `/health` returns counters.

## 2) Staging pilot

- [ ] At least 3-5 test users subscribed.
- [ ] New posts from multiple channels arrive.
- [ ] Duplicates are not re-sent.
- [ ] Media (photo/video) is delivered or cleanly falls back to text.
- [ ] Pause/mute settings block notifications as expected.

## 3) Production launch

- [ ] Publish bot username.
- [ ] Keep process alive (pm2/systemd/docker).
- [ ] Save initial 24h metrics snapshot.
- [ ] Track error log and retry trend for first day.

## 4) Rollback

- [ ] Keep previous working commit/tag.
- [ ] If severe incident: stop process, restore previous release, restart.
- [ ] If noise overload: recommend digest mode for all users.

