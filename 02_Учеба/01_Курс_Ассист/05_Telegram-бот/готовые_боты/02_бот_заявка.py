"""
Бот-заявка — пошагово собирает данные клиента.
Курс АССИСТ+

Что делает:
- Клиент пишет /start → бот спрашивает имя → телефон → задачу
- Сохраняет заявку в файл (Распределение/заявки/)
- Отправляет уведомление менеджеру

Настройка: замени MANAGER_CHAT_ID на свой Telegram ID.
"""

import os
from datetime import datetime
from dotenv import load_dotenv
from telegram import Update, ReplyKeyboardMarkup, ReplyKeyboardRemove
from telegram.ext import (
    ApplicationBuilder, CommandHandler, MessageHandler,
    ConversationHandler, filters, ContextTypes
)
import requests

load_dotenv()
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
MANAGER_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")  # Telegram ID менеджера

# Этапы разговора
NAME, PHONE, TASK = range(3)

# Папка для заявок
os.makedirs("Распределение/заявки", exist_ok=True)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "👋 Здравствуйте! Я помогу оставить заявку.\n\n"
        "Как вас зовут?",
        reply_markup=ReplyKeyboardRemove()
    )
    return NAME


async def get_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["name"] = update.message.text
    await update.message.reply_text(
        f"Приятно познакомиться, {update.message.text}! 👋\n\n"
        "Укажите номер телефона для связи:"
    )
    return PHONE


async def get_phone(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["phone"] = update.message.text

    keyboard = [
        ["Подбор ассистента", "Обучение"],
        ["Аудит процессов", "Другое"]
    ]
    markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, resize_keyboard=True)

    await update.message.reply_text(
        "Отлично! Что вас интересует?",
        reply_markup=markup
    )
    return TASK


async def get_task(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["task"] = update.message.text
    user = update.message.from_user

    # Формируем заявку
    data = {
        "name": context.user_data["name"],
        "phone": context.user_data["phone"],
        "task": context.user_data["task"],
        "username": f"@{user.username}" if user.username else "нет",
        "user_id": user.id,
        "date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }

    # Сохраняем в файл
    ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    filename = f"Распределение/заявки/{ts}_{data['name']}.md"
    with open(filename, "w") as f:
        f.write(f"# Заявка от {data['date']}\n\n")
        f.write(f"**Имя:** {data['name']}\n")
        f.write(f"**Телефон:** {data['phone']}\n")
        f.write(f"**Задача:** {data['task']}\n")
        f.write(f"**Telegram:** {data['username']} (ID: {data['user_id']})\n")

    # Уведомляем менеджера
    if MANAGER_CHAT_ID:
        notify_text = (
            f"🔔 <b>Новая заявка!</b>\n\n"
            f"<b>Имя:</b> {data['name']}\n"
            f"<b>Телефон:</b> {data['phone']}\n"
            f"<b>Задача:</b> {data['task']}\n"
            f"<b>Telegram:</b> {data['username']}\n"
            f"<b>Время:</b> {data['date']}"
        )
        url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
        requests.post(url, json={
            "chat_id": MANAGER_CHAT_ID,
            "text": notify_text,
            "parse_mode": "HTML"
        })

    await update.message.reply_text(
        f"✅ Заявка принята!\n\n"
        f"📋 {data['name']}\n"
        f"📞 {data['phone']}\n"
        f"📝 {data['task']}\n\n"
        f"Менеджер свяжется с вами в ближайшее время!",
        reply_markup=ReplyKeyboardRemove()
    )
    return ConversationHandler.END


async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Заявка отменена. Напишите /start чтобы начать заново.",
        reply_markup=ReplyKeyboardRemove()
    )
    return ConversationHandler.END


def main():
    app = ApplicationBuilder().token(TOKEN).build()

    conv = ConversationHandler(
        entry_points=[CommandHandler("start", start)],
        states={
            NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, get_name)],
            PHONE: [MessageHandler(filters.TEXT & ~filters.COMMAND, get_phone)],
            TASK: [MessageHandler(filters.TEXT & ~filters.COMMAND, get_task)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
    )
    app.add_handler(conv)

    print("🤖 Бот-заявка запущен!")
    app.run_polling()


if __name__ == "__main__":
    main()
