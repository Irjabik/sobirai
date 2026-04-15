"""
Бот-контекст-сборщик — сохраняет всё из Telegram в папку Распределение.
Курс АССИСТ+, Модуль 05.
"""

import os
import logging
from datetime import datetime
from telegram import Update
from telegram.ext import (
    ApplicationBuilder, CommandHandler, MessageHandler,
    filters, ContextTypes
)
from config import TELEGRAM_BOT_TOKEN, DISTRIBUTION_FOLDER, ADMIN_ID

# --- Логирование ---
logging.basicConfig(
    filename="bot.log",
    format="[%(asctime)s] [%(levelname)s] %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# --- Создание папок ---
FOLDERS = ["сообщения", "транскрипты", "документы", "изображения", "ссылки"]
for folder in FOLDERS:
    os.makedirs(f"{DISTRIBUTION_FOLDER}/{folder}", exist_ok=True)


def timestamp():
    return datetime.now().strftime("%Y-%m-%d_%H-%M-%S")


# --- Команды ---
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Привет! Я бот-контекст-сборщик.\n\n"
        "Отправь мне что угодно:\n"
        "— текст → сохраню в сообщения/\n"
        "— фото → сохраню в изображения/\n"
        "— документ → сохраню в документы/\n"
        "— голосовое → транскрибирую и сохраню\n\n"
        "Команды: /help /status /sync /log /list"
    )


async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "/start — приветствие\n"
        "/help — эта справка\n"
        "/status — путь к папке и кол-во файлов\n"
        "/sync — статистика обработки\n"
        "/log — последние 10 записей лога\n"
        "/list — последние сохранённые файлы"
    )


async def status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    total = sum(
        len(os.listdir(f"{DISTRIBUTION_FOLDER}/{f}"))
        for f in FOLDERS if os.path.isdir(f"{DISTRIBUTION_FOLDER}/{f}")
    )
    await update.message.reply_text(
        f"📁 Папка: {os.path.abspath(DISTRIBUTION_FOLDER)}\n"
        f"📊 Всего файлов: {total}"
    )


async def sync(update: Update, context: ContextTypes.DEFAULT_TYPE):
    stats = {}
    for folder in FOLDERS:
        path = f"{DISTRIBUTION_FOLDER}/{folder}"
        stats[folder] = len(os.listdir(path)) if os.path.isdir(path) else 0
    text = "📊 Статистика:\n\n"
    for folder, count in stats.items():
        text += f"  {folder}: {count} файлов\n"
    await update.message.reply_text(text)


async def log_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if os.path.exists("bot.log"):
        with open("bot.log") as f:
            lines = f.readlines()[-10:]
        await update.message.reply_text("".join(lines) or "Лог пуст")
    else:
        await update.message.reply_text("Лог пуст")


async def list_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    files = []
    for folder in FOLDERS:
        path = f"{DISTRIBUTION_FOLDER}/{folder}"
        if os.path.isdir(path):
            for f in sorted(os.listdir(path), reverse=True)[:3]:
                files.append(f"{folder}/{f}")
    text = "📄 Последние файлы:\n\n" + "\n".join(files[:10]) if files else "Пока пусто"
    await update.message.reply_text(text)


# --- Обработчики контента ---
async def save_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
    user = update.message.from_user
    filename = f"{DISTRIBUTION_FOLDER}/сообщения/{timestamp()}.md"

    content = (
        f"# Сообщение из Telegram\n\n"
        f"**Дата:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        f"**Пользователь:** {user.full_name} (@{user.username or 'нет'}) [ID: {user.id}]\n\n"
        f"---\n\n{text}\n"
    )

    with open(filename, "w") as f:
        f.write(content)

    logger.info(f"Текст сохранён: {filename}")
    await update.message.reply_text("✅ Сохранено в сообщения/")


async def save_photo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    photo = update.message.photo[-1]  # Самое большое разрешение
    file = await photo.get_file()
    filename = f"{DISTRIBUTION_FOLDER}/изображения/{timestamp()}.jpg"
    await file.download_to_drive(filename)

    logger.info(f"Фото сохранено: {filename}")
    await update.message.reply_text("✅ Фото сохранено в изображения/")


async def save_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    doc = update.message.document
    original_name = doc.file_name or "file"
    file = await doc.get_file()
    filename = f"{DISTRIBUTION_FOLDER}/документы/{timestamp()}_{original_name}"
    await file.download_to_drive(filename)

    # Сохраняем метаданные
    meta = f"{DISTRIBUTION_FOLDER}/документы/{timestamp()}_{original_name}.meta.md"
    with open(meta, "w") as f:
        f.write(
            f"# Документ: {original_name}\n\n"
            f"**Размер:** {doc.file_size} байт\n"
            f"**MIME:** {doc.mime_type}\n"
            f"**Дата:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        )

    logger.info(f"Документ сохранён: {filename}")
    await update.message.reply_text(f"✅ Документ «{original_name}» сохранён")


async def save_voice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    voice = update.message.voice
    file = await voice.get_file()
    ogg_path = f"/tmp/{timestamp()}_voice.ogg"
    await file.download_to_drive(ogg_path)

    # Транскрибация через Whisper (бесплатный, работает локально)
    try:
        import whisper
        model = whisper.load_model("base")
        result = model.transcribe(ogg_path, language="ru")
        transcript = result["text"]
        filename = f"{DISTRIBUTION_FOLDER}/транскрипты/{timestamp()}_voice.md"
        with open(filename, "w") as f:
            f.write(
                f"# Транскрипт голосового\n\n"
                f"**Дата:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                f"**Длительность:** {voice.duration} сек\n\n"
                f"---\n\n{transcript}\n"
            )
        logger.info(f"Голосовое транскрибировано: {filename}")
        await update.message.reply_text("✅ Голосовое расшифровано и сохранено")
    except Exception as e:
        # Если Whisper не установлен — просто сохраняем аудио
        filename = f"{DISTRIBUTION_FOLDER}/транскрипты/{timestamp()}_voice.ogg"
        os.rename(ogg_path, filename)
        logger.warning(f"Транскрибация не удалась ({e}), сохранён аудиофайл")
        await update.message.reply_text("⚠️ Сохранил аудио (транскрибация недоступна)")

    # Удаляем временный файл
    if os.path.exists(ogg_path):
        os.remove(ogg_path)


# --- Запуск ---
def main():
    app = ApplicationBuilder().token(TELEGRAM_BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(CommandHandler("status", status))
    app.add_handler(CommandHandler("sync", sync))
    app.add_handler(CommandHandler("log", log_cmd))
    app.add_handler(CommandHandler("list", list_cmd))

    app.add_handler(MessageHandler(filters.PHOTO, save_photo))
    app.add_handler(MessageHandler(filters.Document.ALL, save_document))
    app.add_handler(MessageHandler(filters.VOICE, save_voice))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, save_text))

    logger.info("Бот запущен")
    print("🤖 Бот запущен! Ctrl+C для остановки")
    app.run_polling()


if __name__ == "__main__":
    main()
