"""
Бот-модератор группы — приветствие, антиспам, бан.
Курс АССИСТ+

Что делает:
- Приветствует новых участников
- Удаляет сообщения со ссылками от новичков
- Бан за спам-слова
- /ban — бан пользователя (только для админов)
- /mute — мут на N минут
- /rules — правила группы

Настройка: добавь бота в группу как администратора.
"""

import os
import re
from datetime import datetime, timedelta
from dotenv import load_dotenv
from telegram import Update, ChatPermissions
from telegram.ext import (
    ApplicationBuilder, CommandHandler, MessageHandler,
    ChatMemberHandler, filters, ContextTypes
)

load_dotenv()
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")

# ========== НАСТРОЙ ПОД СЕБЯ ==========
WELCOME_MESSAGE = (
    "👋 Добро пожаловать, {name}!\n\n"
    "Правила группы:\n"
    "1. Уважайте друг друга\n"
    "2. Никакого спама и рекламы\n"
    "3. Вопросы по теме группы\n\n"
    "Напишите /rules для полных правил."
)

RULES_TEXT = (
    "📜 <b>Правила группы:</b>\n\n"
    "1. Без спама и рекламы\n"
    "2. Без оскорблений и токсичности\n"
    "3. Вопросы по теме группы\n"
    "4. Запрещены ссылки без одобрения админов\n"
    "5. Один вопрос — одно сообщение\n\n"
    "Нарушение → предупреждение → мут → бан"
)

SPAM_WORDS = [
    "заработок", "казино", "ставки", "крипто схема",
    "бесплатные деньги", "пассивный доход", "инвестиции гарантия",
]

# Время "новичка" — первые N минут в группе не могут слать ссылки
NEWBIE_MINUTES = 60

ADMIN_IDS = [
    int(os.getenv("TELEGRAM_CHAT_ID", "0")),  # Твой ID
]
# =======================================

# Хранилище новичков (в реальном проекте — база данных)
newbies = {}  # user_id: datetime_joined


async def welcome(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Приветствие нового участника."""
    for member in update.message.new_chat_members:
        if member.is_bot:
            continue
        newbies[member.id] = datetime.now()
        name = member.full_name or member.first_name
        await update.message.reply_text(WELCOME_MESSAGE.format(name=name))


async def check_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Проверка сообщений на спам."""
    if not update.message or not update.message.text:
        return

    user = update.message.from_user
    text = update.message.text.lower()

    # Проверка спам-слов
    for word in SPAM_WORDS:
        if word in text:
            await update.message.delete()
            await context.bot.send_message(
                update.message.chat_id,
                f"🚫 Сообщение от {user.full_name} удалено (спам)."
            )
            return

    # Проверка ссылок от новичков
    if user.id in newbies:
        joined = newbies[user.id]
        if datetime.now() - joined < timedelta(minutes=NEWBIE_MINUTES):
            # Ищем ссылки
            url_pattern = r'https?://\S+|t\.me/\S+|@\w+'
            if re.search(url_pattern, text):
                await update.message.delete()
                await context.bot.send_message(
                    update.message.chat_id,
                    f"🔗 {user.full_name}, ссылки запрещены в первый час. "
                    f"Подождите немного!"
                )
                return


async def rules(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(RULES_TEXT, parse_mode="HTML")


async def ban_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Бан пользователя (только для админов)."""
    if update.message.from_user.id not in ADMIN_IDS:
        return

    if not update.message.reply_to_message:
        await update.message.reply_text("Ответь на сообщение пользователя, которого нужно забанить.")
        return

    user = update.message.reply_to_message.from_user
    await context.bot.ban_chat_member(update.message.chat_id, user.id)
    await update.message.reply_text(f"🔨 {user.full_name} забанен.")


async def mute_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Мут пользователя на N минут."""
    if update.message.from_user.id not in ADMIN_IDS:
        return

    if not update.message.reply_to_message:
        await update.message.reply_text("Ответь на сообщение. Использование: /mute 30 (минут)")
        return

    # Парсим время
    args = context.args
    minutes = int(args[0]) if args else 30

    user = update.message.reply_to_message.from_user
    until = datetime.now() + timedelta(minutes=minutes)

    await context.bot.restrict_chat_member(
        update.message.chat_id,
        user.id,
        permissions=ChatPermissions(can_send_messages=False),
        until_date=until
    )
    await update.message.reply_text(
        f"🔇 {user.full_name} замьючен на {minutes} мин."
    )


def main():
    app = ApplicationBuilder().token(TOKEN).build()

    app.add_handler(MessageHandler(filters.StatusUpdate.NEW_CHAT_MEMBERS, welcome))
    app.add_handler(CommandHandler("rules", rules))
    app.add_handler(CommandHandler("ban", ban_user))
    app.add_handler(CommandHandler("mute", mute_user))
    app.add_handler(MessageHandler(
        filters.TEXT & ~filters.COMMAND & filters.ChatType.GROUPS,
        check_message
    ))

    print("🤖 Бот-модератор запущен!")
    app.run_polling()


if __name__ == "__main__":
    main()
