"""
AI-консультант — Telegram-бот, который знает о вашем деле и отвечает на вопросы.
Использует бесплатный Google Gemini API.
Курс АССИСТ+, Модуль 05.
"""

import google.generativeai as genai
from telegram import Update
from telegram.ext import (
    ApplicationBuilder, CommandHandler, MessageHandler,
    filters, ContextTypes
)
from config import TELEGRAM_BOT_TOKEN, GEMINI_API_KEY

# --- Настройки AI ---
genai.configure(api_key=GEMINI_API_KEY)
model = genai.GenerativeModel("gemini-2.0-flash")

SYSTEM_PROMPT = """
Ты — AI-ассистент компании [НАЗВАНИЕ КОМПАНИИ].

Наши услуги:
- [Услуга 1] — [цена]
- [Услуга 2] — [цена]
- [Услуга 3] — [цена]

Контакты:
- Telegram: @username
- Телефон: +7 (999) 123-45-67
- Сайт: example.com

Правила:
- Отвечай вежливо и конкретно
- Если не знаешь точный ответ — предложи связаться с менеджером
- Не придумывай цены и услуги, которых нет в списке
- Отвечай на русском языке
"""


# --- Команды ---
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Привет! Я AI-помощник компании [НАЗВАНИЕ].\n\n"
        "Задайте мне любой вопрос — я отвечу на основе информации о нашем деле.\n\n"
        "Команды: /start /help"
    )


async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "/start — приветствие\n"
        "/help — эта справка\n\n"
        "Просто напишите вопрос — я отвечу."
    )


# --- AI-ответы ---
async def ai_reply(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_message = update.message.text

    try:
        response = model.generate_content(
            f"{SYSTEM_PROMPT}\n\nВопрос пользователя: {user_message}"
        )
        answer = response.text
    except Exception:
        answer = (
            "Извините, произошла ошибка. "
            "Свяжитесь с нами напрямую: @username"
        )

    await update.message.reply_text(answer)


# --- Запуск ---
def main():
    app = ApplicationBuilder().token(TELEGRAM_BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, ai_reply))

    print("AI-консультант запущен!")
    app.run_polling()


if __name__ == "__main__":
    main()
