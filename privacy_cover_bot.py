"""
Privacy Cover Bot — resends media stripped of caption/attribution, batched
into albums of up to 10 (Telegram's max), using file_id + sendMediaGroup
so nothing is downloaded and there's no 20MB limit.

Album batching behavior:
  - Incoming photos/videos are buffered per user into a "media" bucket
    (photos+videos can share one album). Audio and documents get their
    own buckets, since Telegram only allows same-type grouping for those.
  - The moment a bucket hits 10 items, those 10 are flushed as one album.
  - Leftovers (<10) are flushed after ALBUM_FLUSH_TIMEOUT seconds of no
    new activity in that bucket — so if you send 8 videos, wait, then
    send 5 more, the first 2 of the new batch complete a 10-item album
    with the original 8, and the remaining 3 wait for the next chunk or
    the timeout.
  - GIFs (animations) can't be grouped into albums via the Bot API, so
    they're always sent individually.

Features:
  - Inline button settings menu (no need to remember commands)
  - Persistent settings & dedup (Postgres via db.py), dedup by file_unique_id
  - Per-type accept toggles: photos, text, gifs, audio (video/docs always on)
  - Rate-limited send queue — smooths bursts so large batches (thousands
    of files) don't trip Telegram's flood limits

Stack: python-telegram-bot v20+, asyncpg
Deploy target: Render free web service (webhook mode)

Env vars needed:
  BOT_TOKEN            - your Telegram bot token from @BotFather
  DATABASE_URL         - Postgres connection string
  SEND_DELAY           - optional, seconds between queued sends (default 1.5)
  ALBUM_FLUSH_TIMEOUT  - optional, seconds of inactivity before flushing a
                         partial album (default 180 = 3 minutes)
"""

import asyncio
import os
import time
import logging

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
ALBUM_MAX = 10

SETTING_LABELS = {
    "accept_photos": "Photos",
    "accept_text": "Text",
    "accept_gifs": "GIFs",
    "accept_audio": "Audio",
    "dedup_enabled": "Dedup",
}

# global bot handle set in main(), used by buffer flush jobs which don't
# have a per-update Context to hand them (they fire on a timer, not an update)
BOT = None


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


# ---------- album buffering ----------
# Three buckets per user, matching Telegram's grouping rules:
#   "media" -> photos + videos mixed
#   "audio" -> audio files only
#   "document" -> documents only

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
        else:
            media = [build_input_media(category, item) for item in items]
            await BOT.send_media_group(chat_id, media=media)

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
        return  # newer item arrived; a fresher debounce task will handle it

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
    for key, label in SETTING_LABELS.items():
        state = "✅" if settings[key] else "❌"
        rows.append([InlineKeyboardButton(f"{label}: {state}", callback_data=f"toggle:{key}")])
    rows.append([InlineKeyboardButton("Close", callback_data="close")])
    return InlineKeyboardMarkup(rows)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Hey! Send me photos, videos, GIFs, audio, or documents and I'll "
        "send them back stripped of captions/attribution — photos and "
        "videos get batched into albums of up to 10 automatically.\n\n"
        "Tap /settings any time to toggle what I accept, dedup, etc.\n"
        "/queue shows how much is still pending."
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

    _, key = query.data.split(":", 1)
    settings = await db.get_settings(user_id)
    new_value = not settings[key]
    await db.set_setting(user_id, key, new_value)
    settings[key] = new_value

    await query.edit_message_text(
        "Settings — tap to toggle:",
        reply_markup=build_settings_keyboard(settings),
    )


async def show_queue(update: Update, context: ContextTypes.DEFAULT_TYPE):
    pending = send_queue.qsize()
    buffered = sum(len(v) for cat in buffers.values() for v in [cat.get(update.effective_user.id, [])])
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


# ---------- media handling ----------

async def handle_photo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    user_id = update.effective_user.id
    settings = await db.get_settings(user_id)
    if not settings["accept_photos"]:
        await msg.reply_text("Photo acceptance is off. Toggle it in /settings.")
        return

    file_obj = msg.photo[-1]
    if settings["dedup_enabled"]:
        if await db.is_duplicate(user_id, file_obj.file_unique_id):
            return
        await db.mark_seen(user_id, file_obj.file_unique_id)

    await buffer_add(user_id, update.effective_chat.id, "media",
                      {"type": "photo", "file_id": file_obj.file_id})


async def handle_video_or_doc(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    user_id = update.effective_user.id
    settings = await db.get_settings(user_id)

    if msg.video:
        file_obj = msg.video
        category, item_type = "media", "video"
    else:
        file_obj = msg.document
        category, item_type = "document", "document"

    if settings["dedup_enabled"]:
        if await db.is_duplicate(user_id, file_obj.file_unique_id):
            return
        await db.mark_seen(user_id, file_obj.file_unique_id)

    await buffer_add(user_id, update.effective_chat.id, category,
                      {"type": item_type, "file_id": file_obj.file_id})


async def handle_audio(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    user_id = update.effective_user.id
    settings = await db.get_settings(user_id)
    if not settings["accept_audio"]:
        await msg.reply_text("Audio acceptance is off. Toggle it in /settings.")
        return

    file_obj = msg.audio or msg.voice
    if settings["dedup_enabled"]:
        if await db.is_duplicate(user_id, file_obj.file_unique_id):
            return
        await db.mark_seen(user_id, file_obj.file_unique_id)

    # Voice notes aren't groupable as InputMediaAudio reliably across
    # clients, so send those individually; regular audio files can batch.
    if msg.voice:
        async def job():
            await BOT.send_voice(update.effective_chat.id, file_obj.file_id)
        await send_queue.put((job, update.effective_chat.id))
    else:
        await buffer_add(user_id, update.effective_chat.id, "audio",
                          {"type": "audio", "file_id": file_obj.file_id})


async def handle_gif(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    user_id = update.effective_user.id
    settings = await db.get_settings(user_id)
    if not settings["accept_gifs"]:
        await msg.reply_text("GIF acceptance is off. Toggle it in /settings.")
        return

    file_obj = msg.animation
    if settings["dedup_enabled"]:
        if await db.is_duplicate(user_id, file_obj.file_unique_id):
            return
        await db.mark_seen(user_id, file_obj.file_unique_id)

    # Animations can't be grouped into an album via the Bot API
    async def job():
        await BOT.send_animation(update.effective_chat.id, file_obj.file_id)
    await send_queue.put((job, update.effective_chat.id))


async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    settings = await db.get_settings(user_id)
    if not settings["accept_text"]:
        return  # silently ignore

    chat_id = update.effective_chat.id
    message_id = update.message.message_id

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
