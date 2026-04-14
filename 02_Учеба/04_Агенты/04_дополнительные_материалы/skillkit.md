# SkillKit — менеджер скиллов для AI-агентов

## Что это

Пакетный менеджер скиллов. Как npm для Node.js, только для AI-агентов. Написал скилл один раз — работает в Claude Code, Cursor, Copilot, Windsurf и ещё 40 агентах.

400 000+ скиллов в реестре.

## Зачем

Проблема: каждый агент использует свой формат скиллов:
- Claude Code → `SKILL.md` в `.claude/skills/`
- Cursor → `.mdc` в `.cursor/skills/`
- Copilot → Markdown в `.github/skills/`

SkillKit переводит между форматами автоматически.

## Установка

```bash
npx skillkit@latest
```

## Основные команды

```bash
# Инициализация — определит твоих агентов, создаст папки
skillkit init

# Умные рекомендации — анализирует проект и предлагает скиллы
skillkit recommend
# Результат:
# 92% vercel-react-best-practices
# 87% tailwind-v4-patterns
# 85% nextjs-app-router

# Установить скиллы
skillkit install anthropics/skills       # из GitHub
skillkit install ./my-local-skills       # локальные

# Перевести скилл из Claude в Cursor
skillkit translate my-skill --to cursor

# Перевести все скиллы
skillkit translate --all --to windsurf

# Деплой скиллов в агентов
skillkit sync
```

## Когда использовать

- Работаешь с несколькими AI-агентами (Cursor + Claude Code)
- Хочешь быстро найти готовые скиллы под свой проект
- Хочешь поделиться скиллами с командой

## Ссылки

- GitHub: https://github.com/rohitg00/skillkit
- Сайт: https://skillkit.sh
- Docs: https://skillkit.sh/docs
