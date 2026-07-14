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
    "near_dup_enabled": True,
    "min_file_size_mb": 0,  # 0 = no minimum
}

BOOL_KEYS = {k for k, v in DEFAULT_SETTINGS.items() if isinstance(v, bool)}
INT_KEYS = {k for k, v in DEFAULT_SETTINGS.items() if isinstance(v, int) and not isinstance(v, bool)}


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
                dedup_enabled BOOLEAN NOT NULL DEFAULT TRUE,
                near_dup_enabled BOOLEAN NOT NULL DEFAULT TRUE,
                min_file_size_mb INTEGER NOT NULL DEFAULT 0
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
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_seen_hashes_created_at
            ON seen_hashes (created_at)
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS photo_phashes (
                user_id BIGINT NOT NULL,
                phash TEXT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )
        """)
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_phash_user ON photo_phashes (user_id)
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS user_stats (
                user_id BIGINT PRIMARY KEY,
                total_processed INTEGER NOT NULL DEFAULT 0,
                total_duplicates INTEGER NOT NULL DEFAULT 0,
                total_near_duplicates INTEGER NOT NULL DEFAULT 0,
                total_size_filtered INTEGER NOT NULL DEFAULT 0
            )
        """)


async def get_settings(user_id: int) -> dict:
    async with _pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT accept_photos, accept_text, accept_gifs, accept_audio, "
            "dedup_enabled, near_dup_enabled, min_file_size_mb "
            "FROM user_settings WHERE user_id = $1",
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


async def set_setting(user_id: int, key: str, value):
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


# ---------- exact-hash dedup ----------

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
    async with _pool.acquire() as conn:
        await conn.execute(
            "DELETE FROM seen_hashes WHERE created_at < now() - $1::interval",
            f"{days} days",
        )


# ---------- perceptual-hash near-dup (photos only) ----------

async def get_recent_phashes(user_id: int, limit: int = 2000) -> list[str]:
    """Fetch this user's stored perceptual hashes for comparison.
    Capped at `limit` most recent to bound comparison cost."""
    async with _pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT phash FROM photo_phashes WHERE user_id = $1 "
            "ORDER BY created_at DESC LIMIT $2",
            user_id, limit,
        )
        return [r["phash"] for r in rows]


async def add_phash(user_id: int, phash: str):
    async with _pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO photo_phashes (user_id, phash) VALUES ($1, $2)",
            user_id, phash,
        )


# ---------- usage stats ----------

async def increment_stat(user_id: int, field: str, amount: int = 1):
    valid_fields = {"total_processed", "total_duplicates",
                     "total_near_duplicates", "total_size_filtered"}
    if field not in valid_fields:
        raise ValueError(f"Unknown stat field: {field}")
    async with _pool.acquire() as conn:
        await conn.execute(
            f"""
            INSERT INTO user_stats (user_id, {field}) VALUES ($1, $2)
            ON CONFLICT (user_id) DO UPDATE
            SET {field} = user_stats.{field} + EXCLUDED.{field}
            """,
            user_id, amount,
        )


async def get_stats(user_id: int) -> dict:
    async with _pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT total_processed, total_duplicates, total_near_duplicates, "
            "total_size_filtered FROM user_stats WHERE user_id = $1",
            user_id,
        )
        if row is None:
            return {"total_processed": 0, "total_duplicates": 0,
                     "total_near_duplicates": 0, "total_size_filtered": 0}
        return dict(row)
