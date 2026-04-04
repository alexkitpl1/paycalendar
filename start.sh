#!/bin/bash
set -e

echo "=== PayCalendar Startup ==="

# Force correct Zone.ee IMAP host in config (fixes mail.zone.ee → imap.zone.eu)
python3 -c "
import os, sys
sys.path.insert(0, '.')
os.environ.setdefault('PC_EMAIL_IMAP_HOST', 'imap.zone.eu')
os.environ.setdefault('ACCESS_KEY', os.environ.get('ACCESS_KEY', 'pay123'))
# Write directly to config file
import configparser, pathlib
data_dir = pathlib.Path(os.environ.get('DATA_DIR', '.'))
cfg_file = data_dir / 'config.ini'
data_dir.mkdir(parents=True, exist_ok=True)
cfg = configparser.RawConfigParser()
if cfg_file.exists():
    cfg.read(cfg_file, encoding='utf-8')
if not cfg.has_section('email'):
    cfg.add_section('email')
cfg.set('email', 'imap_host', 'imap.zone.eu')
cfg.set('email', 'imap_port', '993')
with open(cfg_file, 'w', encoding='utf-8') as f:
    cfg.write(f)
print('IMAP host fixed: imap.zone.eu')
"

echo "Starting gunicorn..."
exec gunicorn --bind 0.0.0.0:$PORT --workers 1 --threads 4 --timeout 120 app:app
