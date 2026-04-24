# Echo-бот — минимальный Telegram-бот
# Курс АССИСТ+, Модуль 06

import os
from dotenv import load_dotenv
from telegram import Update
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    filters,
    ContextTypes,
)

load_dotenv()
TOKEN = os.getenv("TELEGRAM_TOKEN")


async def start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /start."""
    await update.message.reply_text(
        "Привет! Я echo-бот. Напиши мне что-нибудь, и я повторю."
    )


async def echo(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Повторяет текст пользователя."""
    await update.message.reply_text(update.message.text)


def main():
    app = ApplicationBuilder().token(TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, echo))
    print("Бот запущен...")
    app.run_polling()


if __name__ == "__main__":
    main()
