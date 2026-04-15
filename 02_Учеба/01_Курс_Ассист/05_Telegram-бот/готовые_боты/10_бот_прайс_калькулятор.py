"""
Бот-прайс — калькулятор стоимости услуг с кнопками.
Курс АССИСТ+

Что делает:
- /start — выбор услуги
- Пошаговый выбор параметров (тип, срочность, доп. опции)
- Автоматический расчёт стоимости
- Итог + кнопка «Оставить заявку»

Настройка: замени SERVICES на свои услуги и цены.
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
SERVICES = {
    "search": {
        "name": "Подбор ассистента",
        "base_price": 50000,
        "types": {
            "basic": {"name": "Базовый поиск", "multiplier": 1.0},
            "pro": {"name": "Расширенный поиск", "multiplier": 1.5},
            "vip": {"name": "VIP (с обучением)", "multiplier": 2.0},
        },
        "urgency": {
            "normal": {"name": "Обычный (7 дней)", "multiplier": 1.0},
            "fast": {"name": "Быстрый (3 дня)", "multiplier": 1.3},
            "urgent": {"name": "Срочный (24 часа)", "multiplier": 1.7},
        },
    },
    "training": {
        "name": "Обучение ассистентов",
        "base_price": 30000,
        "types": {
            "individual": {"name": "Индивидуальное", "multiplier": 1.5},
            "group": {"name": "Групповое (до 5 чел)", "multiplier": 1.0},
            "corporate": {"name": "Корпоративное (до 20)", "multiplier": 2.5},
        },
        "urgency": {
            "standard": {"name": "Стандарт (2 недели)", "multiplier": 1.0},
            "intensive": {"name": "Интенсив (1 неделя)", "multiplier": 1.4},
        },
    },
    "audit": {
        "name": "Аудит процессов",
        "base_price": 25000,
        "types": {
            "mini": {"name": "Мини-аудит (3 процесса)", "multiplier": 0.8},
            "standard": {"name": "Стандарт (до 10)", "multiplier": 1.0},
            "full": {"name": "Полный (вся компания)", "multiplier": 2.0},
        },
        "urgency": {
            "normal": {"name": "Обычный (5 дней)", "multiplier": 1.0},
            "express": {"name": "Экспресс (2 дня)", "multiplier": 1.5},
        },
    },
}

CONTACT_USERNAME = "assist_plus"  # Telegram для связи
# =======================================


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    buttons = []
    for key, service in SERVICES.items():
        buttons.append([InlineKeyboardButton(
            f"💼 {service['name']} (от {service['base_price']:,} ₽)",
            callback_data=f"svc_{key}"
        )])

    await update.message.reply_text(
        "💰 <b>Калькулятор стоимости</b>\n\nВыберите услугу:",
        reply_markup=InlineKeyboardMarkup(buttons),
        parse_mode="HTML"
    )


async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    data = query.data

    # Шаг 1: выбор услуги
    if data.startswith("svc_"):
        svc_key = data[4:]
        service = SERVICES[svc_key]
        buttons = []
        for type_key, type_info in service["types"].items():
            buttons.append([InlineKeyboardButton(
                type_info["name"],
                callback_data=f"type_{svc_key}_{type_key}"
            )])
        buttons.append([InlineKeyboardButton("◀️ Назад", callback_data="back_start")])

        await query.edit_message_text(
            f"💼 <b>{service['name']}</b>\n\nВыберите тип:",
            reply_markup=InlineKeyboardMarkup(buttons),
            parse_mode="HTML"
        )

    # Шаг 2: выбор типа
    elif data.startswith("type_"):
        parts = data.split("_", 2)
        svc_key, type_key = parts[1], parts[2]
        service = SERVICES[svc_key]
        buttons = []
        for urg_key, urg_info in service["urgency"].items():
            buttons.append([InlineKeyboardButton(
                urg_info["name"],
                callback_data=f"urg_{svc_key}_{type_key}_{urg_key}"
            )])
        buttons.append([InlineKeyboardButton("◀️ Назад", callback_data=f"svc_{svc_key}")])

        await query.edit_message_text(
            f"💼 <b>{service['name']}</b>\n"
            f"Тип: {service['types'][type_key]['name']}\n\n"
            f"Выберите срочность:",
            reply_markup=InlineKeyboardMarkup(buttons),
            parse_mode="HTML"
        )

    # Шаг 3: расчёт
    elif data.startswith("urg_"):
        parts = data.split("_", 3)
        svc_key, type_key, urg_key = parts[1], parts[2], parts[3]

        service = SERVICES[svc_key]
        type_info = service["types"][type_key]
        urg_info = service["urgency"][urg_key]

        base = service["base_price"]
        total = int(base * type_info["multiplier"] * urg_info["multiplier"])

        buttons = [
            [InlineKeyboardButton(
                f"📩 Оставить заявку",
                url=f"https://t.me/{CONTACT_USERNAME}?text=Заявка: {service['name']}, {type_info['name']}, {urg_info['name']}. Расчёт: {total:,} ₽"
            )],
            [InlineKeyboardButton("🔄 Рассчитать заново", callback_data="back_start")],
        ]

        await query.edit_message_text(
            f"💰 <b>Расчёт стоимости</b>\n\n"
            f"📋 Услуга: {service['name']}\n"
            f"📦 Тип: {type_info['name']}\n"
            f"⏰ Срочность: {urg_info['name']}\n\n"
            f"───────────────\n"
            f"💵 <b>Итого: {total:,} ₽</b>\n"
            f"───────────────\n\n"
            f"<i>Это ориентировочная стоимость. "
            f"Точную сумму рассчитаем после обсуждения деталей.</i>",
            reply_markup=InlineKeyboardMarkup(buttons),
            parse_mode="HTML"
        )

    elif data == "back_start":
        buttons = []
        for key, service in SERVICES.items():
            buttons.append([InlineKeyboardButton(
                f"💼 {service['name']} (от {service['base_price']:,} ₽)",
                callback_data=f"svc_{key}"
            )])
        await query.edit_message_text(
            "💰 <b>Калькулятор стоимости</b>\n\nВыберите услугу:",
            reply_markup=InlineKeyboardMarkup(buttons),
            parse_mode="HTML"
        )


def main():
    app = ApplicationBuilder().token(TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(button_handler))
    print("🤖 Бот-калькулятор запущен!")
    app.run_polling()


if __name__ == "__main__":
    main()
