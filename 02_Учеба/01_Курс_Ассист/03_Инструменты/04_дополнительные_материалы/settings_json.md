# settings.json — рекомендуемые настройки Cursor

> Cmd+Shift+P → "Open User Settings (JSON)" → вставь:

```json
{
  "editor.fontSize": 14,
  "editor.tabSize": 2,
  "editor.wordWrap": "on",
  "editor.minimap.enabled": false,
  "editor.bracketPairColorization.enabled": true,
  "editor.guides.bracketPairs": true,
  "editor.formatOnSave": true,
  "editor.defaultFormatter": "esbenp.prettier-vscode",
  "editor.cursorBlinking": "smooth",
  "editor.smoothScrolling": true,

  "files.autoSave": "afterDelay",
  "files.autoSaveDelay": 1000,
  "files.trimTrailingWhitespace": true,

  "terminal.integrated.fontSize": 13,
  "terminal.integrated.defaultProfile.osx": "zsh",

  "workbench.colorTheme": "Default Dark+",
  "workbench.iconTheme": "material-icon-theme",
  "workbench.startupEditor": "none",
  "workbench.sideBar.location": "right",

  "explorer.confirmDelete": false,
  "explorer.confirmDragAndDrop": false,

  "git.autofetch": true,
  "git.confirmSync": false
}
```

## Что это даёт
- Автосохранение каждую секунду
- Форматирование при сохранении
- Боковая панель справа (файлы не прыгают при открытии терминала)
- Отключена миникарта (экономит место)
- Подсветка парных скобок
