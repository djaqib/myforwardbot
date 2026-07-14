"""
Privacy Cover Bot — resends media stripped of caption/attribution, batched
into albums of up to 10 (Telegram's max), using file_id + sendMediaGroup
so nothing is downloaded and there's no 20MB limit for anything except
photos when near-duplicate detection is on (see below).

Album batching: see buffer_add() / try_flush_full_chunks() / debounced_flush().
Send 8 videos, wait, send 5 more -> the first 2 of the new batch complete
a 10-item album with the original 8; the remaining 3 wait for more items
or the ALBUM_FLUSH_TIMEOUT.

Near-duplicate detection (photos only): computes a perceptual hash
(imagehash.phash) so a re-compressed or slightly-edited copy of a photo
you've already sent still gets caught, not just byte-identical copies.
This requires downloading the photo (unlike everything else in this bot,
which works purely off file_id), so it's still subject to Telegram's
20MB download cap — if the download fails for any reason, the photo is
processed normally rather than blocking on the near-dup check.

Buffer safety valve: MAX_QUEUE_SIZE caps how much can be pending (queued
+ buffered) per user at once, so dumping thousands of files can't grow
memory unboundedly if the queue is draining slower than it's filling.

Stack: python-telegram-bot v20+, asyncpg, Pillow, imagehash
Deploy target: Render free web service (webhook mode)

Env vars needed:
  BOT_TOKEN            - your Telegram bot token from @BotFather
  DATABASE_URL          - Postgres connection string
  SEND_DELAY            - optional, seconds between queued sends (default 1.5)
  ALBUM_FLUSH_TIMEOUT   - optional, seconds before flushing a partial album (default 180)
  MAX_QUEUE_SIZE        - optional, cap on pending+buffered items per user (default 2000)
  NEAR_DUP_THRESHOLD    - optional, max Hamming distance to count as a near-dup (default 6)
"""

import asyncio
import io
import os
import time
import logging

import imagehash
from PIL import Image
from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    InputMediaPhoto,
    InputMediaVideo,
    InputMediaAudio,
    InputMediaDocument,
)
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
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
ALBUM_FLUSH_TIMEOUT = float(os.environ.get("ALBUM_FLUSH_TIMEOUT", "180"))
MAX_QUEUE_SIZE = int(os.environ.get("MAX_QUEUE_SIZE", "2000"))
NEAR_DUP_THRESHOLD = int(os.environ.get("NEAR_DUP_THRESHOLD", "6"))
ALBUM_MAX = 10
SIZE_OPTIONS_MB = [0, 5, 10, 20]  # 0 = off, cycled via settings button

BOOL_SETTING_LABELS = {
    "accept_photos": "Photos",
    "accept_text": "Text",
    "accept_gifs": "GIFs",
    "accept_audio": "Audio",
    "dedup_enabled": "Dedup",
    "near_dup_enabled": "Near-dup detect",
}

BOT = None  # set in post_init; used by timer-driven buffer flushes


# ---------- rate-limited send queue ----------

send_queue: "asyncio.Queue" = asyncio.Queue()


async def queue_worker():
    while True:
        job, chat_id = await send_queue.get()
        try:
            await job()
        except Exception:
            logger.exception("Queued job failed")
            try:
                await BOT.send_message(chat_id, "Something went wrong sending that batch.")
            except Exception:
                logger.exception("Failed to notify user of job failure")
        finally:
            send_queue.task_done()
        await asyncio.sleep(SEND_DELAY)


def total_pending(user_id: int) -> int:
    buffered = sum(len(cat.get(user_id, [])) for cat in buffers.values())
    return send_queue.qsize() + buffered


async def capacity_ok(user_id: int, chat_id: int) -> bool:
    if total_pending(user_id) >= MAX_QUEUE_SIZE:
        await BOT.send_message(
            chat_id,
            f"Queue is at capacity ({MAX_QUEUE_SIZE} pending) — hold off "
            f"sending more until it drains a bit. Check /queue for status."
        )
        return False
    return True


# ---------- album buffering ----------

buffers: dict[str, dict[int, list]] = {"media": {}, "audio": {}, "document": {}}
last_activity: dict[str, dict[int, float]] = {"media": {}, "audio": {}, "document": {}}


def build_input_media(category: str, item: dict):
    if category == "media":
        return InputMediaPhoto(item["file_id"]) if item["type"] == "photo" else InputMediaVideo(item["file_id"])
    if category == "audio":
        return InputMediaAudio(item["file_id"])
    return InputMediaDocument(item["file_id"])


async def send_single(chat_id: int, category: str, item: dict):
    if category == "media":
        if item["type"] == "photo":
            await BOT.send_photo(chat_id, item["file_id"])
        else:
            await BOT.send_video(chat_id, item["file_id"])
    elif category == "audio":
        await BOT.send_audio(chat_id, item["file_id"])
    else:
        await BOT.send_document(chat_id, item["file_id"])


async def flush_chunk(chat_id: int, category: str, items: list):
    async def job():
        if len(items) == 1:
            await send_single(chat_id, category, items[0])
            await BOT.send_message(chat_id, "✅ Sent.")
        else:
            media = [build_input_media(category, item) for item in items]
            await BOT.send_media_group(chat_id, media=media)
            await BOT.send_message(chat_id, f"✅ Sent album of {len(items)}.")

    await send_queue.put((job, chat_id))


async def try_flush_full_chunks(user_id: int, chat_id: int, category: str):
    buf = buffers[category].setdefault(user_id, [])
    while len(buf) >= ALBUM_MAX:
        chunk, buf = buf[:ALBUM_MAX], buf[ALBUM_MAX:]
        buffers[category][user_id] = buf
        await flush_chunk(chat_id, category, chunk)


async def debounced_flush(user_id: int, chat_id: int, category: str):
    await asyncio.sleep(ALBUM_FLUSH_TIMEOUT)
    if time.monotonic() - last_activity[category].get(user_id, 0) < ALBUM_FLUSH_TIMEOUT:
        return

    buf = buffers[category].pop(user_id, [])
    if buf:
        await flush_chunk(chat_id, category, buf)


async def buffer_add(user_id: int, chat_id: int, category: str, item: dict):
    buffers[category].setdefault(user_id, []).append(item)
    last_activity[category][user_id] = time.monotonic()
    await try_flush_full_chunks(user_id, chat_id, category)
    asyncio.create_task(debounced_flush(user_id, chat_id, category))


# ---------- settings menu (inline buttons) ----------

def build_settings_keyboard(settings: dict) -> InlineKeyboardMarkup:
    rows = []
    for key, label in BOOL_SETTING_LABELS.items():
        state = "✅" if settings[key] else "❌"
        rows.append([InlineKeyboardButton(f"{label}: {state}", callback_data=f"toggle:{key}")])

    size_label = "Off" if settings["min_file_size_mb"] == 0 else f"{settings['min_file_size_mb']}MB+"
    rows.append([InlineKeyboardButton(f"Min file size: {size_label}", callback_data="cycle_size")])
    rows.append([InlineKeyboardButton("Close", callback_data="close")])
    return InlineKeyboardMarkup(rows)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Hey! Send me photos, videos, GIFs, audio, or documents and I'll "
        "send them back stripped of captions/attribution — photos and "
        "videos get batched into albums of up to 10 automatically.\n\n"
        "/settings — toggle what I accept, dedup, near-dup detection, min size\n"
        "/queue — see what's pending\n"
        "/stats — see your usage totals"
    )


async def show_settings(update: Update, context: ContextTypes.DEFAULT_TYPE):
    settings = await db.get_settings(update.effective_user.id)
    await update.message.reply_text(
        "Settings — tap to toggle:",
        reply_markup=build_settings_keyboard(settings),
    )


async def settings_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = query.from_user.id
    await query.answer()

    if query.data == "close":
        await query.edit_message_text("Settings closed. Use /settings to reopen.")
        return

    settings = await db.get_settings(user_id)

    if query.data == "cycle_size":
        current = settings["min_file_size_mb"]
        next_idx = (SIZE_OPTIONS_MB.index(current) + 1) % len(SIZE_OPTIONS_MB)
        new_value = SIZE_OPTIONS_MB[next_idx]
        await db.set_setting(user_id, "min_file_size_mb", new_value)
        settings["min_file_size_mb"] = new_value
    else:
        _, key = query.data.split(":", 1)
        new_value = not settings[key]
        await db.set_setting(user_id, key, new_value)
        settings[key] = new_value

    await query.edit_message_text(
        "Settings — tap to toggle:",
        reply_markup=build_settings_keyboard(settings),
    )


async def show_queue(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    pending = send_queue.qsize()
    buffered = sum(len(cat.get(user_id, [])) for cat in buffers.values())
    if pending == 0 and buffered == 0:
        await update.message.reply_text("Nothing pending — all clear.")
        return

    eta_seconds = pending * SEND_DELAY
    eta_str = f"~{eta_seconds:.0f}s" if eta_seconds < 60 else f"~{eta_seconds / 60:.1f} min"
    await update.message.reply_text(
        f"{pending} batch(es) queued to send ({eta_str}).\n"
        f"{buffered} item(s) still buffering into an album (flushes at "
        f"{ALBUM_MAX} items or after {int(ALBUM_FLUSH_TIMEOUT)}s of no new items)."
    )


async def show_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    stats = await db.get_stats(update.effective_user.id)
    await update.message.reply_text(
        "Your usage stats:\n"
        f"Processed: {stats['total_processed']}\n"
        f"Exact duplicates skipped: {stats['total_duplicates']}\n"
        f"Near-duplicates skipped: {stats['total_near_duplicates']}\n"
        f"Skipped by size filter: {stats['total_size_filtered']}"
    )


# ---------- size filter helper ----------

async def passes_size_filter(user_id: int, chat_id: int, settings: dict, file_size) -> bool:
    min_mb = settings["min_file_size_mb"]
    if min_mb == 0 or not file_size:
        return True
    if file_size < min_mb * 1024 * 1024:
        await db.increment_stat(user_id, "total_size_filtered")
        return False
    return True


# ---------- near-dup helper (photos only) ----------

async def is_near_duplicate(user_id: int, file_obj) -> bool:
    """Downloads the photo to compute a perceptual hash and compares
    against stored hashes. Returns False (not a dup) if anything about
    the download/hash step fails, so this never blocks normal processing."""
    try:
        tg_file = await file_obj.get_file()
        raw = await tg_file.download_as_bytearray()
        phash = imagehash.phash(Image.open(io.BytesIO(raw)))
    except Exception:
        logger.warning("Near-dup check failed, skipping check for this photo", exc_info=True)
        return False

    existing = await db.get_recent_phashes(user_id)
    for stored in existing:
        try:
            if phash - imagehash.hex_to_hash(stored) <= NEAR_DUP_THRESHOLD:
                return True
        except Exception:
            continue

    await db.add_phash(user_id, str(phash))
    return False


# ---------- media handling ----------

async def handle_photo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id
    settings = await db.get_settings(user_id)

    if not settings["accept_photos"]:
        await msg.reply_text("Photo acceptance is off. Toggle it in /settings.")
        return

    file_obj = msg.photo[-1]

    if not await passes_size_filter(user_id, chat_id, settings, file_obj.file_size):
        return

    if settings["dedup_enabled"]:
        if await db.is_duplicate(user_id, file_obj.file_unique_id):
            await db.increment_stat(user_id, "total_duplicates")
            return
        await db.mark_seen(user_id, file_obj.file_unique_id)

    if settings["near_dup_enabled"]:
        if await is_near_duplicate(user_id, file_obj):
            await db.increment_stat(user_id, "total_near_duplicates")
            return

    if not await capacity_ok(user_id, chat_id):
        return

    await db.increment_stat(user_id, "total_processed")
    await buffer_add(user_id, chat_id, "media", {"type": "photo", "file_id": file_obj.file_id})


async def handle_video_or_doc(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id
    settings = await db.get_settings(user_id)

    if msg.video:
        file_obj = msg.video
        category, item_type = "media", "video"
    else:
        file_obj = msg.document
        category, item_type = "document", "document"

    if not await passes_size_filter(user_id, chat_id, settings, file_obj.file_size):
        return

    if settings["dedup_enabled"]:
        if await db.is_duplicate(user_id, file_obj.file_unique_id):
            await db.increment_stat(user_id, "total_duplicates")
            return
        await db.mark_seen(user_id, file_obj.file_unique_id)

    if not await capacity_ok(user_id, chat_id):
        return

    await db.increment_stat(user_id, "total_processed")
    await buffer_add(user_id, chat_id, category, {"type": item_type, "file_id": file_obj.file_id})


async def handle_audio(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id
    settings = await db.get_settings(user_id)

    if not settings["accept_audio"]:
        await msg.reply_text("Audio acceptance is off. Toggle it in /settings.")
        return

    file_obj = msg.audio or msg.voice

    if not await passes_size_filter(user_id, chat_id, settings, file_obj.file_size):
        return

    if settings["dedup_enabled"]:
        if await db.is_duplicate(user_id, file_obj.file_unique_id):
            await db.increment_stat(user_id, "total_duplicates")
            return
        await db.mark_seen(user_id, file_obj.file_unique_id)

    if not await capacity_ok(user_id, chat_id):
        return

    await db.increment_stat(user_id, "total_processed")

    if msg.voice:
        async def job():
            await BOT.send_voice(chat_id, file_obj.file_id)
        await send_queue.put((job, chat_id))
    else:
        await buffer_add(user_id, chat_id, "audio", {"type": "audio", "file_id": file_obj.file_id})


async def handle_gif(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id
    settings = await db.get_settings(user_id)

    if not settings["accept_gifs"]:
        await msg.reply_text("GIF acceptance is off. Toggle it in /settings.")
        return

    file_obj = msg.animation

    if not await passes_size_filter(user_id, chat_id, settings, file_obj.file_size):
        return

    if settings["dedup_enabled"]:
        if await db.is_duplicate(user_id, file_obj.file_unique_id):
            await db.increment_stat(user_id, "total_duplicates")
            return
        await db.mark_seen(user_id, file_obj.file_unique_id)

    if not await capacity_ok(user_id, chat_id):
        return

    await db.increment_stat(user_id, "total_processed")

    async def job():
        await BOT.send_animation(chat_id, file_obj.file_id)
    await send_queue.put((job, chat_id))


async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id
    settings = await db.get_settings(user_id)
    if not settings["accept_text"]:
        return

    if not await capacity_ok(user_id, chat_id):
        return

    message_id = update.message.message_id
    await db.increment_stat(user_id, "total_processed")

    async def job():
        await BOT.copy_message(chat_id=chat_id, from_chat_id=chat_id,
                                message_id=message_id, caption="")
    await send_queue.put((job, chat_id))


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    logger.error("Update %s caused error: %s", update, context.error)


# ---------- app setup ----------

async def post_init(app: Application):
    global BOT
    BOT = app.bot
    await db.init_db()
    asyncio.create_task(queue_worker())


def main():
    app = Application.builder().token(BOT_TOKEN).post_init(post_init).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("settings", show_settings))
    app.add_handler(CommandHandler("queue", show_queue))
    app.add_handler(CommandHandler("stats", show_stats))
    app.add_handler(CallbackQueryHandler(settings_callback))

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
