"""
Postgres persistence for the privacy cover bot.
Uses asyncpg since python-telegram-bot v20+ is fully async.

Env vars needed:
  DATABASE_URL - Postgres connection string (Render provides this
                 automatically if you attach a Render Postgres instance)
"""

import os
import asyncpg

DATABASE_URL = os.environ["DATABASE_URL"]

_pool: asyncpg.Pool | None = None

DEFAULT_SETTINGS = {
    "accept_photos": True,
    "accept_text": True,
    "accept_gifs": True,
    "accept_audio": True,
    "dedup_enabled": True,
}


async def init_db():
    global _pool
    _pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)

    async with _pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS user_settings (
                user_id BIGINT PRIMARY KEY,
                accept_photos BOOLEAN NOT NULL DEFAULT TRUE,
                accept_text BOOLEAN NOT NULL DEFAULT TRUE,
                accept_gifs BOOLEAN NOT NULL DEFAULT TRUE,
                accept_audio BOOLEAN NOT NULL DEFAULT TRUE,
                dedup_enabled BOOLEAN NOT NULL DEFAULT TRUE
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS seen_hashes (
                user_id BIGINT NOT NULL,
                file_hash TEXT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                PRIMARY KEY (user_id, file_hash)
            )
        """)
        # Keeps the dedup table from growing forever
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_seen_hashes_created_at
            ON seen_hashes (created_at)
        """)


async def get_settings(user_id: int) -> dict:
    async with _pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT accept_photos, accept_text, accept_gifs, accept_audio, "
            "dedup_enabled FROM user_settings WHERE user_id = $1",
            user_id,
        )
        if row is None:
            await conn.execute(
                "INSERT INTO user_settings (user_id) VALUES ($1) "
                "ON CONFLICT (user_id) DO NOTHING",
                user_id,
            )
            return dict(DEFAULT_SETTINGS)
        return dict(row)


async def set_setting(user_id: int, key: str, value: bool):
    if key not in DEFAULT_SETTINGS:
        raise ValueError(f"Unknown setting: {key}")
    async with _pool.acquire() as conn:
        await conn.execute(
            f"""
            INSERT INTO user_settings (user_id, {key}) VALUES ($1, $2)
            ON CONFLICT (user_id) DO UPDATE SET {key} = EXCLUDED.{key}
            """,
            user_id, value,
        )


async def is_duplicate(user_id: int, file_hash: str) -> bool:
    async with _pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT 1 FROM seen_hashes WHERE user_id = $1 AND file_hash = $2",
            user_id, file_hash,
        )
        return row is not None


async def mark_seen(user_id: int, file_hash: str):
    async with _pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO seen_hashes (user_id, file_hash) VALUES ($1, $2) "
            "ON CONFLICT DO NOTHING",
            user_id, file_hash,
        )


async def prune_old_hashes(days: int = 30):
    """Optional housekeeping: call periodically (e.g. from a cron) to keep
    the dedup table from growing unbounded."""
    async with _pool.acquire() as conn:
        await conn.execute(
            "DELETE FROM seen_hashes WHERE created_at < now() - $1::interval",
            f"{days} days",
        )
