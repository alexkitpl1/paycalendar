# PayCalendar — Deploy to Railway

## Способ 1: Railway CLI (проще всего, без GitHub)

### Шаг 1 — Распакуй этот архив
Извлеки все файлы в папку, например `C:\paycalendar-cloud\`

### Шаг 2 — Запусти deploy.bat
Двойной клик на `deploy.bat` — он:
1. Установит Railway CLI
2. Попросит войти в Railway (откроется браузер)
3. Задеплоит приложение
4. Покажет URL

### Шаг 3 — Добавь переменные
После деплоя зайди в Railway Dashboard → твой проект → Variables:

| Переменная | Значение |
|-----------|----------|
| PC_EMAIL_ADDRESS | vadim.orlov@welltechnology.eu |
| PC_EMAIL_PASSWORD | твой_пароль |
| PC_EMAIL_IMAP_HOST | mail.zone.ee |
| PC_CLAUDE_API_KEY | sk-ant-... |
| ACCESS_KEY | придумай_пароль (для входа с телефона) |

Railway автоматически перезапустит после добавления переменных.

### Шаг 4 — Открой на Android
Railway даёт URL вида: `https://paycalendar-xxx.up.railway.app`
Открывай в браузере на телефоне → вводи ACCESS_KEY → готово!

---

## Способ 2: Через GitHub (если CLI не работает)

1. github.com → New repository → `paycalendar`
2. Settings → Default branch: убедись что `main`
3. Загрузи все файлы из этой папки в репозиторий
4. Railway → New Project → Deploy from GitHub → выбери `paycalendar` → ветка `main`
5. Добавь Variables (см. выше)

---

## Persistent Storage (сохранение данных)
Railway Dashboard → твой проект → Add Volume → Mount path: `/data`
Затем добавь переменную: `DATA_DIR = /data`
