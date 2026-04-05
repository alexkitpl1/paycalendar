#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────
#  deploy.sh  —  backup client data, then push to Railway
#  Usage:  bash deploy.sh [commit message]
#          bash deploy.sh              ← uses auto message
# ─────────────────────────────────────────────────────────────

set -e

PROD_URL="https://paycalendar-production.up.railway.app"
BACKUP_DIR="./backups"
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
BACKUP_FILE="${BACKUP_DIR}/backup_${TIMESTAMP}.json"

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  PayCalendar Deploy Script"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# 1. Create backups directory
mkdir -p "$BACKUP_DIR"

# 2. Download backup from production
echo ""
echo "📥 Сохраняю данные с production..."
if curl -sf --max-time 30 "${PROD_URL}/api/backup/export" -o "$BACKUP_FILE"; then
    SIZE=$(wc -c < "$BACKUP_FILE" | tr -d ' ')
    INV_COUNT=$(python3 -c "import json,sys; d=json.load(open('$BACKUP_FILE')); print(len(d.get('invoices',[])))" 2>/dev/null || echo "?")
    echo "✅ Бэкап сохранён: ${BACKUP_FILE}"
    echo "   Счетов: ${INV_COUNT} · Размер: ${SIZE} байт"
else
    echo "⚠  Не удалось подключиться к production (${PROD_URL})"
    echo "   Продолжаю без бэкапа..."
fi

# Keep only last 20 backups
ls -t "${BACKUP_DIR}"/backup_*.json 2>/dev/null | tail -n +21 | xargs -r rm --
echo "   Старые бэкапы очищены (оставлено последних 20)"

# 3. Git status
echo ""
echo "📋 Изменённые файлы:"
git diff --name-only HEAD 2>/dev/null || true
git status --short 2>/dev/null | head -20

# 4. Commit message
MSG="${1:-"deploy $(date +'%d.%m.%Y %H:%M')"}"

# 5. Commit and push
echo ""
echo "🚀 Деплой: ${MSG}"
git add -A
git commit -m "$MSG" --allow-empty
git push origin main

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  ✅ Деплой завершён!"
echo "  📦 Бэкап: ${BACKUP_FILE}"
echo "  🔗 ${PROD_URL}"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
