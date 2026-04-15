"""
Бот-уведомления — отправляет напоминания по расписанию.
Курс АССИСТ+

Что делает:
- Отправляет сообщения в Telegram по расписанию
- Утренний брифинг, напоминания о встречах, дедлайнах
- Запускается через cron или как фоновый скрипт

Использование:
    python 05_бот_уведомления.py                    # Отправит утренний брифинг
    python 05_бот_уведомления.py --type deadline     # Напоминание о дедлайне
    python 05_бот_уведомления.py --type custom --text "Текст"  # Произвольное

Для автозапуска добавь в cron:
    crontab -e
    0 9 * * 1-5 python /path/to/05_бот_уведомления.py          # Каждый будний в 9:00
    30 17 * * 5 python /path/to/05_бот_уведомления.py --type weekly  # Пт в 17:30
"""

import os
import sys
import argparse
from datetime import datetime
from dotenv import load_dotenv
import requests

load_dotenv()
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# ========== НАСТРОЙ ПОД СЕБЯ ==========
MESSAGES = {
    "morning": (
        "☀️ <b>Доброе утро!</b>\n\n"
        "📋 <b>Чеклист на сегодня:</b>\n"
        "□ Проверить почту и мессенджеры\n"
        "□ Посмотреть календарь — есть ли встречи\n"
        "□ Проверить задачи в Todoist\n"
        "□ Ответить на срочные сообщения\n\n"
        "🎯 Фокус дня: [впиши свою главную задачу]"
    ),
    "deadline": (
        "⏰ <b>Напоминание о дедлайне!</b>\n\n"
        "Не забудь: [описание задачи]\n"
        "Срок: [дата]\n\n"
        "Осталось: [сколько времени]"
    ),
    "weekly": (
        "📊 <b>Еженедельный отчёт</b>\n\n"
        f"Неделя: {datetime.now().strftime('%d.%m.%Y')}\n\n"
        "📌 <b>Что сделано:</b>\n"
        "• [задача 1]\n"
        "• [задача 2]\n\n"
        "📌 <b>На следующую неделю:</b>\n"
        "• [задача 1]\n"
        "• [задача 2]\n\n"
        "💡 <b>Заметки:</b>\n"
        "• [заметка]"
    ),
    "meeting": (
        "🗓 <b>Напоминание о встрече!</b>\n\n"
        "Через 30 минут:\n"
        "[название встречи]\n"
        "[ссылка на Zoom/Google Meet]"
    ),
}
# =======================================


def send_message(text):
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    response = requests.post(url, json={
        "chat_id": CHAT_ID,
        "text": text,
        "parse_mode": "HTML"
    })
    if response.ok:
        print(f"✅ Отправлено в {datetime.now().strftime('%H:%M:%S')}")
    else:
        print(f"❌ Ошибка: {response.text}")


def main():
    parser = argparse.ArgumentParser(description="Бот-уведомления")
    parser.add_argument("--type", default="morning",
                        choices=["morning", "deadline", "weekly", "meeting", "custom"],
                        help="Тип уведомления")
    parser.add_argument("--text", default="", help="Текст для custom")
    args = parser.parse_args()

    if args.type == "custom":
        if not args.text:
            print("Укажи текст: --text 'Твоё сообщение'")
            sys.exit(1)
        send_message(args.text)
    elif args.type in MESSAGES:
        send_message(MESSAGES[args.type])
    else:
        print(f"Неизвестный тип: {args.type}")


if __name__ == "__main__":
    main()
