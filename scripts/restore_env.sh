#!/usr/bin/env bash
set -euo pipefail

# Restore .env from a persistent location outside the git repo.
# Usage:
#   ENV_BACKUP_PATH=/path/to/persistent/.env ./scripts/restore_env.sh
# or:
#   ./scripts/restore_env.sh /path/to/persistent/.env

APP_DIR="${APP_DIR:-/app}"
TARGET_ENV="${TARGET_ENV:-$APP_DIR/.env}"
BACKUP_ENV="${1:-${ENV_BACKUP_PATH:-}}"

if [[ -z "$BACKUP_ENV" ]]; then
  echo "ERROR: backup .env path is required." >&2
  echo "Use: ENV_BACKUP_PATH=/path/to/.env ./scripts/restore_env.sh" >&2
  echo "  or: ./scripts/restore_env.sh /path/to/.env" >&2
  exit 1
fi

if [[ ! -f "$BACKUP_ENV" ]]; then
  echo "ERROR: backup file not found: $BACKUP_ENV" >&2
  exit 1
fi

mkdir -p "$(dirname "$TARGET_ENV")"
cp "$BACKUP_ENV" "$TARGET_ENV"
chmod 600 "$TARGET_ENV" || true

echo "OK: restored .env to $TARGET_ENV from $BACKUP_ENV"
