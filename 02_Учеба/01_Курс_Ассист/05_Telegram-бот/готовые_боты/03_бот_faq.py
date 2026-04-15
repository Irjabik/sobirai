"""
Бот-FAQ — отвечает на частые вопросы по кнопкам, без AI.
Курс АССИСТ+

Что делает:
- /start — главное меню с категориями
- Каждая категория → список вопросов
- Каждый вопрос → готовый ответ
- Навигация кнопками, без ввода текста

Настройка: замени FAQ_DATA на свои вопросы и ответы.
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
FAQ_DATA = {
    "general": {
        "title": "📌 Общие вопросы",
        "questions": {
            "what": {
                "q": "Чем вы занимаетесь?",
                "a": "Мы — HR-агентство АССИСТ+. Подбираем бизнес-ассистентов для предпринимателей. "
                     "Находим, обучаем и сопровождаем первый месяц работы."
            },
            "how_long": {
                "q": "Сколько времени занимает подбор?",
                "a": "В среднем 5-7 рабочих дней. Срочный подбор — 3 дня (доплата 30%)."
            },
            "guarantee": {
                "q": "Есть ли гарантия?",
                "a": "Да! Если ассистент не подошёл в первые 14 дней — подберём замену бесплатно."
            },
        }
    },
    "prices": {
        "title": "💰 Цены",
        "questions": {
            "cost": {
                "q": "Сколько стоит подбор?",
                "a": "Подбор ассистента — от 50 000 ₽. Точная стоимость зависит от требований: "
                     "опыт, навыки, график работы. Напишите — рассчитаем бесплатно!"
            },
            "payment": {
                "q": "Как оплатить?",
                "a": "Принимаем: банковский перевод (ИП/ООО), карта (через ЮKassa), "
                     "криптовалюта. Возможна рассрочка на 2 месяца."
            },
            "trial": {
                "q": "Есть пробный период?",
                "a": "Первая неделя — тестовая. Если за 7 дней понимаете, что не подходит — "
                     "возврат 100%. Без вопросов."
            },
        }
    },
    "process": {
        "title": "🔄 Процесс работы",
        "questions": {
            "steps": {
                "q": "Как проходит подбор?",
                "a": "1. Вы заполняете бриф (15 мин)\n"
                     "2. Мы ищем кандидатов (3-5 дней)\n"
                     "3. Присылаем 3-5 анкет\n"
                     "4. Вы проводите собеседование\n"
                     "5. Выбираете → ассистент выходит на работу\n"
                     "6. Мы сопровождаем первые 30 дней"
            },
            "remote": {
                "q": "Ассистенты работают удалённо?",
                "a": "Да, 90% наших ассистентов работают удалённо. "
                     "Но можем найти и в вашем городе, если нужно офисное присутствие."
            },
            "tasks": {
                "q": "Какие задачи может делать ассистент?",
                "a": "• Управление календарём и почтой\n"
                     "• Бухгалтерия и документы\n"
                     "• Социальные сети и контент\n"
                     "• Работа с клиентами и CRM\n"
                     "• Закупки и логистика\n"
                     "• Исследования и аналитика\n"
                     "• Всё, что можно делегировать!"
            },
        }
    },
}
# =======================================


def build_categories_keyboard():
    buttons = []
    for key, cat in FAQ_DATA.items():
        buttons.append([InlineKeyboardButton(cat["title"], callback_data=f"cat_{key}")])
    return InlineKeyboardMarkup(buttons)


def build_questions_keyboard(category_key):
    cat = FAQ_DATA[category_key]
    buttons = []
    for qkey, qdata in cat["questions"].items():
        buttons.append([InlineKeyboardButton(qdata["q"], callback_data=f"q_{category_key}_{qkey}")])
    buttons.append([InlineKeyboardButton("◀️ Назад", callback_data="menu")])
    return InlineKeyboardMarkup(buttons)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "❓ <b>Частые вопросы</b>\n\nВыберите категорию:",
        reply_markup=build_categories_keyboard(),
        parse_mode="HTML"
    )


async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    if query.data == "menu":
        await query.edit_message_text(
            "❓ <b>Частые вопросы</b>\n\nВыберите категорию:",
            reply_markup=build_categories_keyboard(),
            parse_mode="HTML"
        )

    elif query.data.startswith("cat_"):
        cat_key = query.data[4:]
        cat = FAQ_DATA[cat_key]
        await query.edit_message_text(
            f"{cat['title']}\n\nВыберите вопрос:",
            reply_markup=build_questions_keyboard(cat_key),
            parse_mode="HTML"
        )

    elif query.data.startswith("q_"):
        parts = query.data.split("_", 2)
        cat_key, q_key = parts[1], parts[2]
        answer = FAQ_DATA[cat_key]["questions"][q_key]["a"]
        question = FAQ_DATA[cat_key]["questions"][q_key]["q"]

        back_buttons = [
            [InlineKeyboardButton("◀️ К вопросам", callback_data=f"cat_{cat_key}")],
            [InlineKeyboardButton("🏠 В начало", callback_data="menu")],
        ]
        await query.edit_message_text(
            f"<b>{question}</b>\n\n{answer}",
            reply_markup=InlineKeyboardMarkup(back_buttons),
            parse_mode="HTML"
        )


def main():
    app = ApplicationBuilder().token(TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(button_handler))
    print("🤖 Бот-FAQ запущен!")
    app.run_polling()


if __name__ == "__main__":
    main()
