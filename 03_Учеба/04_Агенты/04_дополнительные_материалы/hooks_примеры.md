# Cursor Hooks — автоматические проверки

> Hooks запускаются автоматически при сохранении файла.
> Settings → Cursor → Hooks (или `.cursor/hooks.json`)

## Пример hooks.json

Создай файл `.cursor/hooks.json` в корне проекта:

```json
{
  "hooks": {
    "on-save": [
      {
        "glob": "*.py",
        "command": "python -m py_compile ${file}",
        "description": "Проверка синтаксиса Python"
      },
      {
        "glob": "*.md",
        "command": "echo 'Markdown saved: ${file}'",
        "description": "Лог сохранения markdown"
      }
    ],
    "on-create": [
      {
        "glob": "*.py",
        "command": "echo '# -*- coding: utf-8 -*-' > ${file}",
        "description": "Добавить кодировку в новый Python-файл"
      }
    ]
  }
}
```

## Полезные проверки

```json
{
  "glob": "*.json",
  "command": "python -m json.tool ${file} > /dev/null",
  "description": "Валидация JSON"
}
```

```json
{
  "glob": "requirements.txt",
  "command": "pip install -r ${file} --dry-run",
  "description": "Проверка зависимостей"
}
```
