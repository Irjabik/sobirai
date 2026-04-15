"""
Бот-визитка — показывает информацию о компании с кнопками.
Курс АССИСТ+

Что делает:
- /start — карточка компании с кнопками
- Кнопки: Услуги, Цены, Отзывы, Связаться
- Кнопка «Позвонить» открывает номер телефона
- Кнопка «Написать» открывает Telegram

Настройка: замени COMPANY_INFO на свои данные.
"""

import os
from dotenv import load_dotenv
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    ApplicationBuilder, CommandHandler, CallbackQueryHandler, ContextTypes
)

load_dotenv()
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")

# ========== НАСТРОЙ ПОД СЕБЯ ==========
COMPANY_INFO = {
    "name": "АССИСТ+",
    "tagline": "HR-агентство по подбору бизнес-ассистентов",
    "services": (
        "📋 <b>Наши услуги:</b>\n\n"
        "1. <b>Подбор ассистента</b> — найдём идеального кандидата за 7 дней\n"
        "2. <b>Обучение ассистентов</b> — корпоративный формат, 2 недели\n"
        "3. <b>Аудит процессов</b> — найдём, что можно делегировать\n"
        "4. <b>Курс Cursor AI</b> — научим работать с AI-инструментами"
    ),
    "prices": (
        "💰 <b>Цены:</b>\n\n"
        "• Подбор ассистента — от 50 000 ₽\n"
        "• Обучение (группа) — от 30 000 ₽/чел\n"
        "• Аудит процессов — от 25 000 ₽\n"
        "• Курс Cursor AI — 15 000 ₽\n\n"
        "Точная стоимость зависит от задач. Напишите — рассчитаем!"
    ),
    "reviews": (
        "⭐ <b>Отзывы клиентов:</b>\n\n"
        "💬 <i>«Нашли ассистента за 5 дней. Работает уже 8 месяцев, "
        "всё отлично.»</i>\n— Анна, основатель студии дизайна\n\n"
        "💬 <i>«Аудит показал, что я трачу 15 часов в неделю на рутину. "
        "Теперь ассистент делает это за 3 часа.»</i>\n— Михаил, IT-предприниматель\n\n"
        "💬 <i>«Курс по Cursor — огонь. Теперь создаю ботов и лендинги сама.»</i>\n"
        "— Олеся, маркетолог"
    ),
    "phone": "+79991234567",
    "telegram": "assist_plus",
    "website": "https://example.com",
}
# =======================================


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = [
        [InlineKeyboardButton("📋 Услуги", callback_data="services"),
         InlineKeyboardButton("💰 Цены", callback_data="prices")],
        [InlineKeyboardButton("⭐ Отзывы", callback_data="reviews")],
        [InlineKeyboardButton("📞 Позвонить", url=f"tel:{COMPANY_INFO['phone']}"),
         InlineKeyboardButton("✉️ Написать", url=f"https://t.me/{COMPANY_INFO['telegram']}")],
    ]
    markup = InlineKeyboardMarkup(keyboard)

    text = (
        f"👋 Добро пожаловать в <b>{COMPANY_INFO['name']}</b>!\n\n"
        f"{COMPANY_INFO['tagline']}\n\n"
        f"Выберите раздел:"
    )
    await update.message.reply_text(text, reply_markup=markup, parse_mode="HTML")


async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    back_button = [[InlineKeyboardButton("◀️ Назад в меню", callback_data="menu")]]
    markup = InlineKeyboardMarkup(back_button)

    responses = {
        "services": COMPANY_INFO["services"],
        "prices": COMPANY_INFO["prices"],
        "reviews": COMPANY_INFO["reviews"],
    }

    if query.data == "menu":
        keyboard = [
            [InlineKeyboardButton("📋 Услуги", callback_data="services"),
             InlineKeyboardButton("💰 Цены", callback_data="prices")],
            [InlineKeyboardButton("⭐ Отзывы", callback_data="reviews")],
            [InlineKeyboardButton("📞 Позвонить", url=f"tel:{COMPANY_INFO['phone']}"),
             InlineKeyboardButton("✉️ Написать", url=f"https://t.me/{COMPANY_INFO['telegram']}")],
        ]
        await query.edit_message_text(
            f"👋 <b>{COMPANY_INFO['name']}</b>\n\n{COMPANY_INFO['tagline']}\n\nВыберите раздел:",
            reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML"
        )
    elif query.data in responses:
        await query.edit_message_text(
            responses[query.data], reply_markup=markup, parse_mode="HTML"
        )


def main():
    app = ApplicationBuilder().token(TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(button_handler))
    print("🤖 Бот-визитка запущен!")
    app.run_polling()


if __name__ == "__main__":
    main()
