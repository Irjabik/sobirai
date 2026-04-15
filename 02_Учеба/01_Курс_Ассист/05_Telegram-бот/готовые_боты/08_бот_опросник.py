"""
Бот-опросник — создаёт опросы, собирает ответы, показывает статистику.
Курс АССИСТ+

Что делает:
- /start — запускает опрос (пошагово, вопрос за вопросом)
- Поддерживает текстовые ответы и выбор из вариантов
- Сохраняет результаты в JSON
- /results — статистика ответов (для админа)

Настройка: замени SURVEY на свои вопросы.
"""

import os
import json
from datetime import datetime
from dotenv import load_dotenv
from telegram import Update, ReplyKeyboardMarkup, ReplyKeyboardRemove
from telegram.ext import (
    ApplicationBuilder, CommandHandler, MessageHandler,
    ConversationHandler, filters, ContextTypes
)

load_dotenv()
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
RESULTS_FILE = "survey_results.json"

# ========== НАСТРОЙ ПОД СЕБЯ ==========
SURVEY = [
    {
        "question": "Как вы узнали о нас?",
        "type": "choice",
        "options": ["Instagram", "Telegram", "Рекомендация", "Google", "Другое"]
    },
    {
        "question": "Какая услуга вас интересует?",
        "type": "choice",
        "options": ["Подбор ассистента", "Обучение", "Аудит", "Курс Cursor AI"]
    },
    {
        "question": "Какой у вас бюджет?",
        "type": "choice",
        "options": ["До 30 000 ₽", "30 000 - 50 000 ₽", "50 000 - 100 000 ₽", "Больше 100 000 ₽"]
    },
    {
        "question": "Опишите вашу задачу в 2-3 предложениях:",
        "type": "text"
    },
    {
        "question": "Оцените ваш опыт работы с ассистентами (1-5):",
        "type": "choice",
        "options": ["1 — Нет опыта", "2 — Пробовал", "3 — Средний", "4 — Хороший", "5 — Эксперт"]
    },
]
# =======================================


def load_results():
    if os.path.exists(RESULTS_FILE):
        with open(RESULTS_FILE) as f:
            return json.load(f)
    return []


def save_result(result):
    results = load_results()
    results.append(result)
    with open(RESULTS_FILE, "w") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["answers"] = []
    context.user_data["step"] = 0

    await update.message.reply_text(
        "📋 <b>Опрос</b>\n\n"
        f"Всего вопросов: {len(SURVEY)}\n"
        "Займёт 2 минуты.\n\n"
        "Поехали!",
        parse_mode="HTML",
        reply_markup=ReplyKeyboardRemove()
    )
    return await ask_question(update, context)


async def ask_question(update, context):
    step = context.user_data["step"]

    if step >= len(SURVEY):
        return await finish(update, context)

    q = SURVEY[step]
    text = f"<b>Вопрос {step + 1}/{len(SURVEY)}</b>\n\n{q['question']}"

    if q["type"] == "choice":
        keyboard = [[opt] for opt in q["options"]]
        markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, resize_keyboard=True)
        if hasattr(update, 'message') and update.message:
            await update.message.reply_text(text, reply_markup=markup, parse_mode="HTML")
        elif hasattr(update, 'callback_query'):
            await update.callback_query.message.reply_text(text, reply_markup=markup, parse_mode="HTML")
    else:
        if hasattr(update, 'message') and update.message:
            await update.message.reply_text(text, reply_markup=ReplyKeyboardRemove(), parse_mode="HTML")

    return step


async def handle_answer(update: Update, context: ContextTypes.DEFAULT_TYPE):
    step = context.user_data.get("step", 0)
    answer = update.message.text

    context.user_data["answers"].append({
        "question": SURVEY[step]["question"],
        "answer": answer,
    })
    context.user_data["step"] = step + 1

    return await ask_question(update, context)


async def finish(update, context):
    user = update.message.from_user
    result = {
        "user_id": user.id,
        "name": user.full_name,
        "username": f"@{user.username}" if user.username else "нет",
        "date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "answers": context.user_data["answers"],
    }
    save_result(result)

    await update.message.reply_text(
        "✅ <b>Спасибо за ответы!</b>\n\n"
        "Мы обработаем вашу информацию и свяжемся.\n\n"
        "Хотите пройти опрос заново? Нажмите /start",
        reply_markup=ReplyKeyboardRemove(),
        parse_mode="HTML"
    )
    return ConversationHandler.END


async def results(update: Update, context: ContextTypes.DEFAULT_TYPE):
    data = load_results()
    if not data:
        await update.message.reply_text("Пока нет ответов.")
        return

    text = f"📊 <b>Статистика опроса</b>\n\nВсего ответов: {len(data)}\n\n"

    # Считаем статистику по каждому вопросу с вариантами
    for i, q in enumerate(SURVEY):
        if q["type"] != "choice":
            continue

        text += f"<b>{q['question']}</b>\n"
        counts = {}
        for r in data:
            if i < len(r["answers"]):
                ans = r["answers"][i]["answer"]
                counts[ans] = counts.get(ans, 0) + 1

        for opt, cnt in sorted(counts.items(), key=lambda x: -x[1]):
            pct = cnt / len(data) * 100
            bar = "▓" * int(pct / 10) + "░" * (10 - int(pct / 10))
            text += f"  {bar} {opt}: {cnt} ({pct:.0f}%)\n"
        text += "\n"

    await update.message.reply_text(text, parse_mode="HTML")


async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Опрос отменён.", reply_markup=ReplyKeyboardRemove())
    return ConversationHandler.END


def main():
    app = ApplicationBuilder().token(TOKEN).build()

    states = {i: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_answer)]
              for i in range(len(SURVEY))}

    conv = ConversationHandler(
        entry_points=[CommandHandler("start", start)],
        states=states,
        fallbacks=[CommandHandler("cancel", cancel)],
    )
    app.add_handler(conv)
    app.add_handler(CommandHandler("results", results))

    print("🤖 Бот-опросник запущен!")
    app.run_polling()


if __name__ == "__main__":
    main()
