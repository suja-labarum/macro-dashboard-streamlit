#!/usr/bin/env bash
set -euo pipefail

# One-time setup only. After the first successful login, twscrape saves
# cookies into accounts.db automatically so later runs can reuse them.
#
# Preferred path when X blocks automated login:
# 1. Sign in to x.com in a normal browser session
# 2. Export cookies as X_COOKIES or provide X_AUTH_TOKEN and X_CT0
# 3. Re-run this script; cookie import skips twscrape login entirely
#
# Optional:
# - export TWS_PROXY='http://user:pass@host:port' for a cleaner residential exit
# - export TWS_MANUAL=1 to force manual email-code entry during login_accounts

BASE_DIR="${HOME}/.macro_dashboard"
ACCOUNTS_FILE="${BASE_DIR}/accounts.txt"
DB_PATH="${BASE_DIR}/accounts.db"

mkdir -p "${BASE_DIR}"

: "${X_USERNAME:?Set X_USERNAME before running this script.}"
: "${X_PASSWORD:?Set X_PASSWORD before running this script.}"
: "${X_EMAIL:?Set X_EMAIL before running this script.}"
: "${X_EMAIL_PASSWORD:?Set X_EMAIL_PASSWORD before running this script.}"

pip install twscrape httpx typer

printf '%s:%s:%s:%s\n' \
  "${X_USERNAME}" \
  "${X_PASSWORD}" \
  "${X_EMAIL}" \
  "${X_EMAIL_PASSWORD}" \
  > "${ACCOUNTS_FILE}"

twscrape --db "${DB_PATH}" add_accounts "${ACCOUNTS_FILE}" username:password:email:email_password

if [ -n "${X_COOKIES:-}" ] || { [ -n "${X_AUTH_TOKEN:-}" ] && [ -n "${X_CT0:-}" ]; }; then
  echo "Cookie-based bootstrap detected. Importing existing X session cookies."
  python3 cli/import_x_cookies.py
elif [ "${TWS_MANUAL:-0}" = "1" ]; then
  echo "Manual verification enabled. twscrape will prompt for the email code if X requests it."
  twscrape --db "${DB_PATH}" login_accounts --manual
else
  twscrape --db "${DB_PATH}" login_accounts
fi

twscrape --db "${DB_PATH}" accounts
