#!/usr/bin/env python3
import asyncio
import os
from pathlib import Path

from twscrape.accounts_pool import AccountsPool
from twscrape.account import TOKEN
from twscrape.utils import parse_cookies

BASE_DIR = Path.home() / ".macro_dashboard"
DB_PATH = BASE_DIR / "accounts.db"


def build_cookie_string() -> str:
    cookie_string = os.getenv("X_COOKIES", "").strip()
    if cookie_string:
        return cookie_string

    auth_token = os.getenv("X_AUTH_TOKEN", "").strip()
    ct0 = os.getenv("X_CT0", "").strip()
    if auth_token and ct0:
        return f"auth_token={auth_token}; ct0={ct0}"

    raise SystemExit("Set X_COOKIES or both X_AUTH_TOKEN and X_CT0 before importing cookies.")


async def main() -> None:
    username = os.getenv("X_USERNAME", "").strip()
    password = os.getenv("X_PASSWORD", "").strip()
    email = os.getenv("X_EMAIL", "").strip()
    email_password = os.getenv("X_EMAIL_PASSWORD", "").strip()
    proxy = os.getenv("TWS_PROXY", "").strip() or None

    if not all([username, password, email, email_password]):
        raise SystemExit("Set X_USERNAME, X_PASSWORD, X_EMAIL, and X_EMAIL_PASSWORD before importing cookies.")

    BASE_DIR.mkdir(parents=True, exist_ok=True)
    pool = AccountsPool(str(DB_PATH))
    cookies = parse_cookies(build_cookie_string())

    account = await pool.get_account(username)
    if account is None:
        await pool.add_account(
            username=username,
            password=password,
            email=email,
            email_password=email_password,
            cookies="; ".join(f"{k}={v}" for k, v in cookies.items()),
            proxy=proxy,
        )
        account = await pool.get_account(username)
    else:
        account.password = password
        account.email = email
        account.email_password = email_password
        account.proxy = proxy
        account.cookies = cookies

    account.active = "ct0" in account.cookies
    account.error_msg = None
    account.headers["authorization"] = TOKEN
    if "ct0" in account.cookies:
        account.headers["x-csrf-token"] = account.cookies["ct0"]

    await pool.save(account)
    print(f"Imported cookies for @{username} into {DB_PATH}")


if __name__ == "__main__":
    asyncio.run(main())
