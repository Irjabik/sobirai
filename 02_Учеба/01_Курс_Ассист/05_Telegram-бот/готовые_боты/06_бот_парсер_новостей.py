"""
Бот-парсер новостей — собирает RSS и присылает дайджест.
Курс АССИСТ+

Что делает:
- Парсит RSS-ленты (новости, блоги, каналы)
- Формирует дайджест с заголовками и ссылками
- Отправляет в Telegram

Запуск по расписанию:
    crontab -e
    0 9 * * 1-5 python 06_бот_парсер_новостей.py   # Каждый будний в 9:00

Зависимости: pip install feedparser requests python-dotenv
"""

import os
from datetime import datetime, timedelta
from dotenv import load_dotenv
import requests

try:
    import feedparser
except ImportError:
    print("Установи: pip install feedparser")
    exit(1)

load_dotenv()
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# ========== НАСТРОЙ ПОД СЕБЯ ==========
RSS_FEEDS = [
    {
        "name": "Хабр — Карьера",
        "url": "https://habr.com/ru/rss/hub/career/",
        "emoji": "💼"
    },
    {
        "name": "vc.ru — Бизнес",
        "url": "https://vc.ru/rss",
        "emoji": "📊"
    },
    {
        "name": "TechCrunch",
        "url": "https://techcrunch.com/feed/",
        "emoji": "🌍"
    },
]

MAX_PER_FEED = 3  # Максимум новостей с каждого источника
# =======================================


def fetch_news():
    all_news = []

    for feed_info in RSS_FEEDS:
        try:
            feed = feedparser.parse(feed_info["url"])
            count = 0

            for entry in feed.entries:
                if count >= MAX_PER_FEED:
                    break

                # Проверяем свежесть (последние 24 часа)
                published = entry.get("published_parsed") or entry.get("updated_parsed")
                if published:
                    pub_date = datetime(*published[:6])
                    if pub_date < datetime.now() - timedelta(hours=24):
                        continue

                title = entry.get("title", "Без заголовка")
                link = entry.get("link", "")

                all_news.append({
                    "source": feed_info["name"],
                    "emoji": feed_info["emoji"],
                    "title": title,
                    "link": link,
                })
                count += 1

        except Exception as e:
            print(f"Ошибка парсинга {feed_info['name']}: {e}")

    return all_news


def format_digest(news):
    if not news:
        return "📭 Новых новостей нет."

    date = datetime.now().strftime("%d.%m.%Y")
    text = f"📰 <b>Дайджест новостей — {date}</b>\n\n"

    current_source = ""
    for item in news:
        if item["source"] != current_source:
            current_source = item["source"]
            text += f"\n{item['emoji']} <b>{current_source}</b>\n"
        text += f"• <a href='{item['link']}'>{item['title']}</a>\n"

    text += f"\n\n—\nСобрано автоматически в {datetime.now().strftime('%H:%M')}"
    return text


def send_telegram(text):
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    response = requests.post(url, json={
        "chat_id": CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    })
    if response.ok:
        print(f"✅ Дайджест отправлен ({len(text)} символов)")
    else:
        print(f"❌ Ошибка: {response.text}")


def main():
    print(f"📰 Собираю новости... ({datetime.now().strftime('%H:%M')})")
    news = fetch_news()
    print(f"   Найдено: {len(news)} новостей")

    digest = format_digest(news)
    send_telegram(digest)


if __name__ == "__main__":
    main()
