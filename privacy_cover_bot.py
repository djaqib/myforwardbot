"""
Privacy Cover Bot — strips metadata (EXIF, GPS, device info) from photos,
videos, GIFs, and audio before returning them to the user.

Features:
  - Persistent per-user settings & dedup (Postgres via db.py)
  - Per-type accept toggles: photos, text, gifs, audio (videos/docs always on)
  - Dedup on/off toggle
  - File size limit handling (Telegram Bot API caps downloads at 20MB)
  - Media group (album) batching — waits for all items in an album before
    processing so you get one reply per album, not one per photo
  - Rate-limited send queue — smooths out bursts (e.g. forwarding a huge
    backlog) so you don't trip Telegram's flood limits and risk a restriction

Stack: python-telegram-bot v20+, Pillow, ffmpeg, asyncpg
Deploy target: Render free web service (webhook mode)

Env vars needed:
  BOT_TOKEN     - your Telegram bot token from @BotFather
  DATABASE_URL  - Postgres connection string
  SEND_DELAY    - optional, seconds between processed replies (default 1.5)
"""

import asyncio
import hashlib
import io
import os
import logging
import subprocess
import tempfile
import time

from PIL import Image
from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

import db

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

BOT_TOKEN = os.environ["BOT_TOKEN"]
SEND_DELAY = float(os.environ.get("SEND_DELAY", "1.5"))

# Telegram Bot API caps file downloads at 20MB regardless of your plan
MAX_DOWNLOAD_BYTES = 20 * 1024 * 1024

# How long to wait after the last item in an album arrives before
# processing the batch as a whole (Telegram sends album items as
# separate updates with a shared media_group_id, with slight delay)
ALBUM_DEBOUNCE_SECONDS = 1.2

TOGGLEABLE = {"photos": "accept_photos", "text": "accept_text",
              "gifs": "accept_gifs", "audio": "accept_audio"}


# ---------- rate-limited send queue ----------
# A single worker processes jobs one at a time with a fixed delay between
# them. This is what protects your account when forwarding large batches
# (e.g. 3000+ videos) — bursts get smoothed into a steady, safe rate
# instead of hammering Telegram's API all at once.

send_queue: "asyncio.Queue" = asyncio.Queue()


async def queue_worker():
    while True:
        job = await send_queue.get()
        try:
            await job()
        except Exception:
            logger.exception("Queued job failed")
        finally:
            send_queue.task_done()
        await asyncio.sleep(SEND_DELAY)


# ---------- hashing helpers ----------

def hash_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def hash_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


# ---------- album (media group) buffering ----------
# messages sharing a media_group_id arrive as separate updates in quick
# succession. We buffer them per group and flush once no new item has
# arrived for ALBUM_DEBOUNCE_SECONDS.

album_buffers: dict[str, list] = {}
album_last_seen: dict[str, float] = {}


async def flush_album_later(group_id: str, context: ContextTypes.DEFAULT_TYPE):
    await asyncio.sleep(ALBUM_DEBOUNCE_SECONDS)
    if time.monotonic() - album_last_seen.get(group_id, 0) < ALBUM_DEBOUNCE_SECONDS:
        return  # another item arrived; a newer flush task will handle it

    messages = album_buffers.pop(group_id, [])
    album_last_seen.pop(group_id, None)
    if not messages:
        return

    user_id = messages[0].from_user.id
    settings = await db.get_settings(user_id)
    processed = 0
    for msg in messages:
        ok = await process_photo_message(msg, settings)
        if ok:
            processed += 1

    await messages[-1].reply_text(
        f"Album done: {processed}/{len(messages)} processed "
        f"(duplicates/skips excluded)."
    )


# ---------- command handlers ----------

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Hey! I'm a privacy cover bot.\n\n"
        "Send me a photo, video, GIF, audio file, or document and I'll "
        "strip the metadata (EXIF, GPS, device info, timestamps) and send "
        "back a clean copy.\n\n"
        "Commands:\n"
        "/photos on|off — accept photos\n"
        "/text on|off — accept text messages\n"
        "/gifs on|off — accept GIFs\n"
        "/audio on|off — accept audio/voice notes\n"
        "/dedup on|off — skip files you've already sent me\n"
        "/settings — see current settings\n"
        "/queue — see how many items are still pending"
    )


async def toggle_setting(update: Update, context: ContextTypes.DEFAULT_TYPE, key: str, label: str):
    user_id = update.effective_user.id
    args = context.args

    if not args or args[0].lower() not in ("on", "off"):
        settings = await db.get_settings(user_id)
        current = "on" if settings[key] else "off"
        await update.message.reply_text(
            f"{label} is currently: {current}\nUsage: /{label.lower()} on | off"
        )
        return

    value = args[0].lower() == "on"
    await db.set_setting(user_id, key, value)
    await update.message.reply_text(f"{label} {'enabled' if value else 'disabled'}.")


async def toggle_photos(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await toggle_setting(update, context, "accept_photos", "Photos")


async def toggle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await toggle_setting(update, context, "accept_text", "Text")


async def toggle_gifs(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await toggle_setting(update, context, "accept_gifs", "GIFs")


async def toggle_audio(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await toggle_setting(update, context, "accept_audio", "Audio")


async def toggle_dedup(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await toggle_setting(update, context, "dedup_enabled", "Dedup")


async def show_settings(update: Update, context: ContextTypes.DEFAULT_TYPE):
    settings = await db.get_settings(update.effective_user.id)
    lines = [f"{k}: {'on' if v else 'off'}" for k, v in settings.items()]
    await update.message.reply_text("Your settings:\n" + "\n".join(lines))


async def show_queue(update: Update, context: ContextTypes.DEFAULT_TYPE):
    pending = send_queue.qsize()
    if pending == 0:
        await update.message.reply_text("Queue is empty — nothing pending.")
        return

    eta_seconds = pending * SEND_DELAY
    eta_minutes = eta_seconds / 60
    if eta_minutes < 1:
        eta_str = f"~{eta_seconds:.0f}s"
    elif eta_minutes < 60:
        eta_str = f"~{eta_minutes:.1f} min"
    else:
        eta_str = f"~{eta_minutes / 60:.1f} hr"

    await update.message.reply_text(
        f"{pending} item(s) pending in the queue.\n"
        f"Estimated time to clear: {eta_str} (at {SEND_DELAY}s/item)."
    )


# ---------- media handlers ----------

async def process_photo_message(msg, settings) -> bool:
    """Returns True if a cleaned photo was sent, False if skipped."""
    user_id = msg.from_user.id
    photo_file = await (msg.photo[-1] if msg.photo else msg.document).get_file()

    if photo_file.file_size and photo_file.file_size > MAX_DOWNLOAD_BYTES:
        await msg.reply_text(
            f"That file is too large ({photo_file.file_size // (1024*1024)}MB) — "
            f"Telegram bots can only download files up to 20MB."
        )
        return False

    raw = await photo_file.download_as_bytearray()

    if settings["dedup_enabled"]:
        file_hash = hash_bytes(bytes(raw))
        if await db.is_duplicate(user_id, file_hash):
            return False
        await db.mark_seen(user_id, file_hash)

    img = Image.open(io.BytesIO(raw))
    clean = Image.new(img.mode, img.size)
    clean.putdata(list(img.getdata()))

    out = io.BytesIO()
    fmt = img.format or "JPEG"
    clean.save(out, format=fmt)
    out.seek(0)
    out.name = f"clean.{fmt.lower()}"

    await msg.reply_document(document=out, filename=out.name,
                              caption="Metadata stripped ✅")
    return True


async def handle_photo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    user_id = update.effective_user.id
    settings = await db.get_settings(user_id)

    if not settings["accept_photos"]:
        await msg.reply_text("Photo acceptance is off. Use /photos on to enable.")
        return

    group_id = msg.media_group_id
    if group_id:
        # buffer this item, flush the whole album after debounce
        album_buffers.setdefault(group_id, []).append(msg)
        album_last_seen[group_id] = time.monotonic()
        asyncio.create_task(flush_album_later(group_id, context))
        return

    async def job():
        await process_photo_message(msg, settings)

    await send_queue.put(job)


async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    settings = await db.get_settings(user_id)
    if not settings["accept_text"]:
        return  # silently ignore, acceptance is off
    # Text has no metadata to strip; this toggle just controls whether
    # the bot responds to plain text at all.
    await update.message.reply_text(
        "Got your message. Send photos, videos, GIFs, audio, or documents "
        "to have metadata stripped."
    )


async def handle_gif(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    user_id = update.effective_user.id
    settings = await db.get_settings(user_id)

    if not settings["accept_gifs"]:
        await msg.reply_text("GIF acceptance is off. Use /gifs on to enable.")
        return

    async def job():
        await process_ffmpeg_message(msg, settings, out_name="clean.mp4")

    await send_queue.put(job)


async def handle_audio(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    user_id = update.effective_user.id
    settings = await db.get_settings(user_id)

    if not settings["accept_audio"]:
        await msg.reply_text("Audio acceptance is off. Use /audio on to enable.")
        return

    async def job():
        await process_ffmpeg_message(msg, settings, out_name="clean.mp3")

    await send_queue.put(job)


async def handle_video_or_doc(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message

    async def job():
        settings = await db.get_settings(update.effective_user.id)
        await process_ffmpeg_message(msg, settings, out_name="clean.mp4")

    await send_queue.put(job)


async def process_ffmpeg_message(msg, settings, out_name: str):
    """Strip metadata from video/audio/gif/document using ffmpeg."""
    user_id = msg.from_user.id
    tg_obj = msg.video or msg.animation or msg.audio or msg.voice or msg.document
    tg_file = await tg_obj.get_file()

    if tg_file.file_size and tg_file.file_size > MAX_DOWNLOAD_BYTES:
        await msg.reply_text(
            f"That file is too large ({tg_file.file_size // (1024*1024)}MB) — "
            f"Telegram bots can only download files up to 20MB."
        )
        return

    with tempfile.TemporaryDirectory() as tmp:
        in_path = os.path.join(tmp, "input")
        out_path = os.path.join(tmp, out_name)

        await tg_file.download_to_drive(in_path)

        if settings["dedup_enabled"]:
            file_hash = hash_file(in_path)
            if await db.is_duplicate(user_id, file_hash):
                return
            await db.mark_seen(user_id, file_hash)

        cmd = [
            "ffmpeg", "-y", "-i", in_path,
            "-map_metadata", "-1",
            "-c", "copy",
            out_path,
        ]
        result = subprocess.run(cmd, capture_output=True, text=True)

        if result.returncode != 0:
            logger.error("ffmpeg failed: %s", result.stderr)
            await msg.reply_text("Couldn't process that file — try a different format.")
            return

        with open(out_path, "rb") as f:
            await msg.reply_document(document=f, filename=out_name,
                                      caption="Metadata stripped ✅")


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    logger.error("Update %s caused error: %s", update, context.error)


# ---------- app setup ----------

async def post_init(app: Application):
    await db.init_db()
    asyncio.create_task(queue_worker())


def main():
    app = Application.builder().token(BOT_TOKEN).post_init(post_init).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("photos", toggle_photos))
    app.add_handler(CommandHandler("text", toggle_text))
    app.add_handler(CommandHandler("gifs", toggle_gifs))
    app.add_handler(CommandHandler("audio", toggle_audio))
    app.add_handler(CommandHandler("dedup", toggle_dedup))
    app.add_handler(CommandHandler("settings", show_settings))
    app.add_handler(CommandHandler("queue", show_queue))

    app.add_handler(MessageHandler(filters.PHOTO, handle_photo))
    app.add_handler(MessageHandler(filters.ANIMATION, handle_gif))
    app.add_handler(MessageHandler(filters.AUDIO | filters.VOICE, handle_audio))
    app.add_handler(MessageHandler(filters.VIDEO | filters.Document.ALL, handle_video_or_doc))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))
    app.add_error_handler(error_handler)

    port = int(os.environ.get("PORT", 8443))
    external_url = os.environ.get("RENDER_EXTERNAL_URL")

    if external_url:
        webhook_path = BOT_TOKEN
        app.run_webhook(
            listen="0.0.0.0",
            port=port,
            url_path=webhook_path,
            webhook_url=f"{external_url}/{webhook_path}",
        )
    else:
        app.run_polling()


if __name__ == "__main__":
    main()
