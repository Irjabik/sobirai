"""
Бот-трекер привычек — отмечай выполненное, бот считает серии.
Курс АССИСТ+

Что делает:
- /start — список привычек с кнопками ✅/❌
- Нажал ✅ — отмечено на сегодня
- /stats — статистика: серия дней, процент выполнения
- /add привычка — добавить новую привычку
- Напоминание утром (запусти через cron)

Настройка: замени DEFAULT_HABITS на свои привычки.
"""

import os
import json
from datetime import datetime, timedelta
from dotenv import load_dotenv
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    ApplicationBuilder, CommandHandler, CallbackQueryHandler, ContextTypes
)

load_dotenv()
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")

DATA_FILE = "habits_data.json"

# ========== НАСТРОЙ ПОД СЕБЯ ==========
DEFAULT_HABITS = [
    "🏃 Спорт / зарядка",
    "📚 Чтение 30 мин",
    "🧘 Медитация",
    "💧 Выпить 2л воды",
    "📝 Вести дневник",
]
# =======================================


def load_data():
    if os.path.exists(DATA_FILE):
        with open(DATA_FILE) as f:
            return json.load(f)
    return {}


def save_data(data):
    with open(DATA_FILE, "w") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def get_user_data(user_id):
    data = load_data()
    uid = str(user_id)
    if uid not in data:
        data[uid] = {
            "habits": DEFAULT_HABITS.copy(),
            "log": {}  # {"2026-03-10": {"habit_index": true/false}}
        }
        save_data(data)
    return data, uid


def get_streak(log, habit_idx):
    """Считает текущую серию дней для привычки."""
    streak = 0
    day = datetime.now()
    while True:
        date_str = day.strftime("%Y-%m-%d")
        day_log = log.get(date_str, {})
        if day_log.get(str(habit_idx)):
            streak += 1
            day -= timedelta(days=1)
        else:
            break
    return streak


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    data, uid = get_user_data(user_id)
    habits = data[uid]["habits"]
    today = datetime.now().strftime("%Y-%m-%d")
    today_log = data[uid]["log"].get(today, {})

    buttons = []
    for i, habit in enumerate(habits):
        done = today_log.get(str(i), False)
        status = "✅" if done else "⬜"
        buttons.append([InlineKeyboardButton(
            f"{status} {habit}",
            callback_data=f"toggle_{i}"
        )])

    buttons.append([InlineKeyboardButton("📊 Статистика", callback_data="stats")])

    await update.message.reply_text(
        f"📅 <b>Привычки на {today}</b>\n\nНажми, чтобы отметить:",
        reply_markup=InlineKeyboardMarkup(buttons),
        parse_mode="HTML"
    )


async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    user_id = query.from_user.id
    data, uid = get_user_data(user_id)

    if query.data.startswith("toggle_"):
        idx = query.data.split("_")[1]
        today = datetime.now().strftime("%Y-%m-%d")

        if today not in data[uid]["log"]:
            data[uid]["log"][today] = {}

        current = data[uid]["log"][today].get(idx, False)
        data[uid]["log"][today][idx] = not current
        save_data(data)

        # Обновляем кнопки
        habits = data[uid]["habits"]
        today_log = data[uid]["log"].get(today, {})
        buttons = []
        for i, habit in enumerate(habits):
            done = today_log.get(str(i), False)
            status = "✅" if done else "⬜"
            buttons.append([InlineKeyboardButton(
                f"{status} {habit}", callback_data=f"toggle_{i}"
            )])
        buttons.append([InlineKeyboardButton("📊 Статистика", callback_data="stats")])

        done_count = sum(1 for v in today_log.values() if v)
        total = len(habits)

        await query.edit_message_text(
            f"📅 <b>Привычки на {today}</b>\n"
            f"Выполнено: {done_count}/{total}\n\nНажми, чтобы отметить:",
            reply_markup=InlineKeyboardMarkup(buttons),
            parse_mode="HTML"
        )

    elif query.data == "stats":
        habits = data[uid]["habits"]
        log = data[uid]["log"]

        text = "📊 <b>Статистика привычек</b>\n\n"

        for i, habit in enumerate(habits):
            streak = get_streak(log, i)

            # Считаем % за последние 30 дней
            done_days = 0
            for d in range(30):
                day = (datetime.now() - timedelta(days=d)).strftime("%Y-%m-%d")
                if log.get(day, {}).get(str(i)):
                    done_days += 1
            pct = done_days / 30 * 100

            fire = "🔥" if streak >= 3 else ""
            text += f"<b>{habit}</b>\n"
            text += f"  Серия: {streak} дн. {fire} | За 30 дн.: {done_days}/30 ({pct:.0f}%)\n\n"

        back = [[InlineKeyboardButton("◀️ Назад", callback_data="back_to_habits")]]
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(back), parse_mode="HTML")

    elif query.data == "back_to_habits":
        habits = data[uid]["habits"]
        today = datetime.now().strftime("%Y-%m-%d")
        today_log = data[uid]["log"].get(today, {})
        buttons = []
        for i, habit in enumerate(habits):
            done = today_log.get(str(i), False)
            status = "✅" if done else "⬜"
            buttons.append([InlineKeyboardButton(
                f"{status} {habit}", callback_data=f"toggle_{i}"
            )])
        buttons.append([InlineKeyboardButton("📊 Статистика", callback_data="stats")])
        await query.edit_message_text(
            f"📅 <b>Привычки на {today}</b>\n\nНажми, чтобы отметить:",
            reply_markup=InlineKeyboardMarkup(buttons),
            parse_mode="HTML"
        )


async def add_habit(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Использование: /add Название привычки")
        return
    habit = " ".join(context.args)
    data, uid = get_user_data(update.message.from_user.id)
    data[uid]["habits"].append(habit)
    save_data(data)
    await update.message.reply_text(f"✅ Привычка «{habit}» добавлена!")


async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показать статистику в виде команды."""
    data, uid = get_user_data(update.message.from_user.id)
    habits = data[uid]["habits"]
    log = data[uid]["log"]

    text = "📊 <b>Статистика привычек</b>\n\n"
    for i, habit in enumerate(habits):
        streak = get_streak(log, i)
        fire = "🔥" if streak >= 3 else ""
        text += f"{habit}: серия {streak} дн. {fire}\n"

    await update.message.reply_text(text, parse_mode="HTML")


def main():
    app = ApplicationBuilder().token(TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("stats", stats))
    app.add_handler(CommandHandler("add", add_habit))
    app.add_handler(CallbackQueryHandler(button_handler))
    print("🤖 Бот-трекер привычек запущен!")
    app.run_polling()


if __name__ == "__main__":
    main()
