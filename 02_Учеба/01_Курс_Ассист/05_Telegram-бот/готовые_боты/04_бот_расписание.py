"""
Бот-расписание — показывает свободные слоты, записывает на время.
Курс АССИСТ+

Что делает:
- /start — показывает доступные дни
- Выбор дня → показывает свободные слоты
- Выбор слота → запись + подтверждение
- /my — показывает мои записи
- Данные хранятся в JSON-файле

Настройка: замени SCHEDULE на свои слоты.
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

BOOKINGS_FILE = "bookings.json"

# ========== НАСТРОЙ ПОД СЕБЯ ==========
# Генерируем слоты на ближайшие 5 дней
def generate_schedule():
    schedule = {}
    for i in range(1, 6):  # Следующие 5 дней
        day = datetime.now() + timedelta(days=i)
        if day.weekday() < 5:  # Пн-Пт
            date_str = day.strftime("%Y-%m-%d")
            schedule[date_str] = [
                "10:00", "11:00", "12:00",
                "14:00", "15:00", "16:00", "17:00"
            ]
    return schedule
# =======================================


def load_bookings():
    if os.path.exists(BOOKINGS_FILE):
        with open(BOOKINGS_FILE) as f:
            return json.load(f)
    return {}


def save_bookings(bookings):
    with open(BOOKINGS_FILE, "w") as f:
        json.dump(bookings, f, ensure_ascii=False, indent=2)


def get_free_slots(date_str):
    schedule = generate_schedule()
    bookings = load_bookings()
    all_slots = schedule.get(date_str, [])
    booked = [b["time"] for b in bookings.get(date_str, [])]
    return [s for s in all_slots if s not in booked]


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    schedule = generate_schedule()
    buttons = []
    for date_str in sorted(schedule.keys()):
        day = datetime.strptime(date_str, "%Y-%m-%d")
        days_ru = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]
        label = f"{days_ru[day.weekday()]} {day.strftime('%d.%m')}"
        free = len(get_free_slots(date_str))
        buttons.append([InlineKeyboardButton(
            f"📅 {label} ({free} свободно)", callback_data=f"day_{date_str}"
        )])

    await update.message.reply_text(
        "📅 <b>Запись на консультацию</b>\n\nВыберите день:",
        reply_markup=InlineKeyboardMarkup(buttons),
        parse_mode="HTML"
    )


async def my_bookings(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    bookings = load_bookings()
    my = []
    for date_str, slots in bookings.items():
        for slot in slots:
            if slot["user_id"] == user_id:
                my.append(f"📅 {date_str} в {slot['time']}")

    if my:
        text = "📋 <b>Ваши записи:</b>\n\n" + "\n".join(my)
    else:
        text = "У вас пока нет записей. Нажмите /start чтобы записаться."

    await update.message.reply_text(text, parse_mode="HTML")


async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    if query.data.startswith("day_"):
        date_str = query.data[4:]
        free = get_free_slots(date_str)

        if not free:
            await query.edit_message_text(
                f"😔 На {date_str} все слоты заняты.\nВыберите другой день: /start"
            )
            return

        buttons = []
        for slot in free:
            buttons.append([InlineKeyboardButton(
                f"🕐 {slot}", callback_data=f"book_{date_str}_{slot}"
            )])
        buttons.append([InlineKeyboardButton("◀️ Назад", callback_data="back")])

        day = datetime.strptime(date_str, "%Y-%m-%d")
        await query.edit_message_text(
            f"📅 <b>{day.strftime('%d.%m.%Y')}</b>\n\nВыберите время:",
            reply_markup=InlineKeyboardMarkup(buttons),
            parse_mode="HTML"
        )

    elif query.data.startswith("book_"):
        parts = query.data.split("_", 2)
        date_str, time_str = parts[1], parts[2]
        user = query.from_user

        bookings = load_bookings()
        if date_str not in bookings:
            bookings[date_str] = []

        # Проверяем, не занято ли
        if any(b["time"] == time_str for b in bookings[date_str]):
            await query.edit_message_text("😔 Этот слот уже занят. Попробуйте другой: /start")
            return

        bookings[date_str].append({
            "time": time_str,
            "user_id": user.id,
            "name": user.full_name,
            "username": f"@{user.username}" if user.username else "нет",
            "booked_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        })
        save_bookings(bookings)

        await query.edit_message_text(
            f"✅ <b>Вы записаны!</b>\n\n"
            f"📅 Дата: {date_str}\n"
            f"🕐 Время: {time_str}\n\n"
            f"Ждём вас! Для отмены свяжитесь: @assist_plus\n\n"
            f"Посмотреть записи: /my",
            parse_mode="HTML"
        )

    elif query.data == "back":
        # Возвращаемся к выбору дня
        schedule = generate_schedule()
        buttons = []
        for ds in sorted(schedule.keys()):
            day = datetime.strptime(ds, "%Y-%m-%d")
            days_ru = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]
            label = f"{days_ru[day.weekday()]} {day.strftime('%d.%m')}"
            free = len(get_free_slots(ds))
            buttons.append([InlineKeyboardButton(
                f"📅 {label} ({free} свободно)", callback_data=f"day_{ds}"
            )])
        await query.edit_message_text(
            "📅 <b>Запись на консультацию</b>\n\nВыберите день:",
            reply_markup=InlineKeyboardMarkup(buttons),
            parse_mode="HTML"
        )


def main():
    app = ApplicationBuilder().token(TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("my", my_bookings))
    app.add_handler(CallbackQueryHandler(button_handler))
    print("🤖 Бот-расписание запущен!")
    app.run_polling()


if __name__ == "__main__":
    main()
