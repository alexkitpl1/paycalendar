"""
PayCalendar — платёжный календарь через почту
"""
import email, email.header, imaplib, json, logging, os, re, socket
import subprocess, sys, threading, time, traceback
import urllib.parse as urlparse, webbrowser, configparser
from datetime import datetime, date
from email.utils import parsedate_to_datetime
from pathlib import Path

import requests
from flask import Flask, jsonify, request, send_from_directory, Response, stream_with_context, redirect, make_response
from flask_cors import CORS

# ── Logging ───────────────────────────────────────────────────────────────────
# ── Debug log file ───────────────────────────────────────────────────────────
# Log file - uses persistent volume if available

class DebugFormatter(logging.Formatter):
    def format(self, record):
        return super().format(record)

# File handler - define DEBUG_FILE early using env var (redefined properly later)
import os as _os
_early_data_dir = _os.environ.get("DATA_DIR", str(Path(__file__).parent))
DEBUG_FILE = Path(_early_data_dir) / "debug_full.log"
try:
    Path(_early_data_dir).mkdir(parents=True, exist_ok=True)
except Exception:
    DEBUG_FILE = Path(__file__).parent / "debug_full.log"

_file_handler = logging.FileHandler(DEBUG_FILE, encoding="utf-8", mode="a")
_file_handler.setLevel(logging.DEBUG)
_file_handler.setFormatter(logging.Formatter(
    "%(asctime)s.%(msecs)03d [%(levelname)-8s] %(funcName)-25s | %(message)s",
    datefmt="%H:%M:%S"
))

# Console handler - INFO level
_console_handler = logging.StreamHandler(sys.stdout)
_console_handler.setLevel(logging.INFO)
_console_handler.setFormatter(logging.Formatter(
    "%(asctime)s [%(levelname)s] %(message)s"
))

logging.basicConfig(level=logging.DEBUG, handlers=[_file_handler, _console_handler])
log = logging.getLogger("paycalendar")

# Write separator at startup (deferred - CONFIG_FILE defined later)
def _write_startup_log():
    with open(DEBUG_FILE, "a", encoding="utf-8") as _f:
        from datetime import datetime as _dt
        _f.write("\n" + "="*80 + "\n")
        _f.write(f"  PayCalendar started: {_dt.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        _f.write(f"  Python: {sys.version}\n")
        try:
            _f.write(f"  Config: {CONFIG_FILE}\n")
        except Exception:
            pass
        _f.write("="*80 + "\n")

# Log all unhandled exceptions to file
def _exc_handler(exc_type, exc_value, exc_tb):
    import traceback as _tb
    log.error(f"UNHANDLED EXCEPTION: {exc_type.__name__}: {exc_value}")
    log.error("".join(_tb.format_tb(exc_tb)))
    sys.__excepthook__(exc_type, exc_value, exc_tb)
sys.excepthook = _exc_handler

# Also patch requests to log all HTTP calls
import requests as _req
_orig_request = _req.Session.request
def _debug_request(self, method, url, **kwargs):
    log.debug(f"HTTP {method.upper()} {url}")
    if kwargs.get("json"):
        import json as _j
        body = _j.dumps(kwargs["json"])
        # Hide password
        if "password" in body:
            body = body[:body.find("password")] + 'password":"***"}'
        log.debug(f"  Body: {body[:200]}")
    try:
        resp = _orig_request(self, method, url, **kwargs)
        log.debug(f"  → {resp.status_code} ({len(resp.content)} bytes)")
        if resp.status_code not in (200, 201, 204, 301, 302) or log.isEnabledFor(logging.DEBUG):
            try:
                ct = resp.headers.get("Content-Type", "")
                if "json" in ct:
                    log.debug(f"  Response JSON: {resp.text[:300]}")
                elif len(resp.text) < 500:
                    log.debug(f"  Response: {resp.text[:200]}")
            except Exception:
                pass
        return resp
    except Exception as ex:
        log.error(f"  Request FAILED: {ex}")
        raise
_req.Session.request = _debug_request

# ── Paths ─────────────────────────────────────────────────────────────────────
BASE_DIR    = Path(__file__).parent
# config.ini saved to persistent volume on Railway, local dir on Windows
CONFIG_FILE = BASE_DIR / "config.ini"  # redefined below after _DATA_DIR
# Railway: set DATA_DIR=/data after adding a Volume
# Without Volume, data resets on each deploy
_DATA_DIR = Path(os.environ.get("DATA_DIR", str(BASE_DIR)))
try:
    _DATA_DIR.mkdir(parents=True, exist_ok=True)
except Exception:
    _DATA_DIR = BASE_DIR
DATA_FILE   = _DATA_DIR / "invoices.json"
# Use persistent dir for config on cloud, app dir on Windows
CONFIG_FILE  = _DATA_DIR / "config.ini"
# Copy base config.ini to DATA_DIR if not there yet
_base_cfg = BASE_DIR / "config.ini"
if not CONFIG_FILE.exists() and _base_cfg.exists():
    import shutil as _sh
    _sh.copy2(_base_cfg, CONFIG_FILE)
elif not CONFIG_FILE.exists():
    CONFIG_FILE.write_text(
        "[email]\naddress =\npassword =\nimap_host = mail.zone.ee\n"
        "imap_port = 993\nimap_folder = INBOX\n"
        "scan_last_emails = 500\nquick_scan_emails = 30\n"
        "auto_scan_minutes = 60\n[claude]\napi_key =\n"
        "[app]\nport = 5050\nauto_open_browser = true\n"
        "company_name = PayCalendar\n[notifications]\n"
        "warn_days_before = 3\nwindows_notifications = true\n",
        encoding="utf-8")
STATE_FILE  = _DATA_DIR / "scan_state.json"
DEBUG_FILE = _DATA_DIR / "debug_full.log"
# Update file handler to use correct path
try:
    _file_handler.stream.close()
    _file_handler.baseFilename = str(DEBUG_FILE)
    _file_handler.stream = open(str(DEBUG_FILE), 'a', encoding='utf-8')
except Exception:
    pass
TMPL_DIR    = BASE_DIR / "templates"

# ── Config ────────────────────────────────────────────────────────────────────
cfg = configparser.RawConfigParser()
if CONFIG_FILE.exists():
    cfg.read(CONFIG_FILE, encoding="utf-8")
_write_startup_log()

def c(sec, key, fb=""):
    # Check environment variables first (for Railway/cloud deployment)
    env_key = f"PC_{sec.upper()}_{key.upper()}"
    env_val = os.environ.get(env_key, "")
    if env_val:
        return env_val.strip()
    # Also check simple key name
    env_val2 = os.environ.get(key.upper(), "")
    if env_val2:
        return env_val2.strip()
    if not cfg.has_option(sec, key):
        return fb
    val = cfg.get(sec, key, fallback=fb).strip()
    if ' ;' in val:
        val = val[:val.index(' ;')].strip()
    if val.startswith(';'):
        val = fb
    return val

EMAIL_ADDR     = c("email", "address")
EMAIL_PASS     = c("email", "password")
IMAP_HOST      = c("email", "imap_host", "mail.zone.ee")
IMAP_PORT      = int(c("email", "imap_port", "993"))
IMAP_FOLDER    = c("email", "imap_folder", "INBOX")
WEBMAIL_URL    = c("email", "webmail_session_url", "")
WEBMAIL_COOKIE = c("email", "webmail_cookie", "")
WEBMAIL_XSRF   = c("email", "webmail_xsrf_token", "")
SCAN_LIMIT       = int(c("email", "scan_last_emails", "500"))
QUICK_SCAN_LIMIT = int(c("email", "quick_scan_emails", "30"))
AUTO_SCAN      = int(c("email", "auto_scan_minutes", "60"))
API_KEY        = c("claude", "api_key")
APP_PORT       = int(os.environ.get("PORT", c("app", "port", "5050")))
AUTO_BROWSER   = c("app", "auto_open_browser", "true").lower() == "true"
COMPANY        = c("app", "company_name", "My Company")
WARN_DAYS      = int(c("notifications", "warn_days_before", "3"))
WIN_NOTIFY     = c("notifications", "windows_notifications", "true").lower() == "true"
MODEL          = "claude-sonnet-4-5"

# ── Keywords (8 languages) ────────────────────────────────────────────────────
INVOICE_KW = [
    "arve","arved","arve nr","arve number","tasuda","maksetähtaeg","makse",
    "invoice","bill","payment due","amount due","receipt","purchase order",
    "pro forma","statement","overdue","remittance","payable",
    "счёт","счёт-фактура","оплата","к оплате","квитанция","задолженность",
    "rechnung","zahlung","fällig","betrag","mahnung",
    "lasku","maksu","laskutus","eräpäivä",
    "rēķins","sąskaita","apmokėjimas",
    "faktura","betalning","förfallodatum",
    "facture","factura","fattura","paiement","pago","pagamento",
]

IMAP_SUBJECTS = [
    b'SUBJECT "invoice"', b'SUBJECT "bill"', b'SUBJECT "payment"',
    b'SUBJECT "receipt"', b'SUBJECT "due"', b'SUBJECT "payable"',
    b'SUBJECT "arve"', b'SUBJECT "arved"', b'SUBJECT "arve nr"',
    b'SUBJECT "tasuda"', b'SUBJECT "lasku"', b'SUBJECT "rechnung"',
    b'SUBJECT "faktura"', b'SUBJECT "facture"', b'SUBJECT "factura"',
    b'SUBJECT "fattura"', b'SUBJECT "zahlung"',
    b'SUBJECT "\xd1\x81\xd1\x87\xd1\xbc\xd1\x82"',
    b'SUBJECT "\xd0\xbe\xd0\xbf\xd0\xbb\xd0\xb0\xd1\x82\xd0\xb0"',
]

CLAUDE_SYSTEM = "You are an invoice extractor. Return ONLY valid JSON."
CLAUDE_TMPL = """\
Subject: {subject}
From: {sender}
Date: {date}
Has attachments: {has_att}
Body:
{body}

Detect invoices in ANY language (this company has global partners):
Estonian: arve/arved/tasuda | English: invoice/bill/payment due/receipt |
Russian: счёт/оплата | German: Rechnung/Zahlung | Nordic: faktura/lasku |
French/Spanish/Italian: facture/factura/fattura

If email has attachments it is LIKELY an invoice — set is_invoice=true.

If invoice: {{"is_invoice":true,"vendor":"","invoice_number":"","amount":0.0,
"currency":"EUR","due_date":"YYYY-MM-DD","issue_date":"YYYY-MM-DD",
"description":"","category":"utilities|software|services|rent|taxes|supplies|logistics|marketing|other"}}
If not invoice: {{"is_invoice":false}}"""

# ── Flask app ─────────────────────────────────────────────────────────────────
app = Flask(__name__, template_folder=str(TMPL_DIR))
CORS(app)

@app.after_request
def set_headers(response):
    # Allow embedding in claude.ai iframe for testing
    response.headers.pop('X-Frame-Options', None)
    response.headers['Content-Security-Policy'] = "frame-ancestors *"
    return response
scan_lock = threading.Lock()

# ── Data helpers ──────────────────────────────────────────────────────────────
def load_invoices():
    try:
        if DATA_FILE.exists():
            return json.loads(DATA_FILE.read_text(encoding="utf-8"))
    except Exception:
        pass
    return []

def save_invoices(data):
    DATA_FILE.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


# ── Scan state (remembers where we left off) ──────────────────────────────────
def load_state():
    """Load scan state: last UID, date, scan count."""
    try:
        if STATE_FILE.exists():
            return json.loads(STATE_FILE.read_text(encoding="utf-8"))
    except Exception:
        pass
    return {"last_uid": None, "last_date": None, "scan_count": 0,
            "scanned_uids": []}

def save_state(state):
    STATE_FILE.write_text(json.dumps(state, ensure_ascii=False, indent=2),
                          encoding="utf-8")

def get_scan_limit():
    """First scan=500, subsequent=200."""
    st = load_state()
    return 200 if st.get("scan_count", 0) > 0 else 500

def update_scan_state(new_uids, last_uid=None, last_date=None):
    """Update scan state after a scan."""
    st = load_state()
    st["scan_count"] = st.get("scan_count", 0) + 1
    if last_uid:
        st["last_uid"]  = last_uid
    if last_date:
        st["last_date"] = last_date
    # Keep scanned UIDs (last 2000 to cap memory)
    existing = set(st.get("scanned_uids", []))
    existing.update(str(u) for u in new_uids)
    st["scanned_uids"] = list(existing)[-2000:]
    save_state(st)

def is_already_scanned(uid):
    st = load_state()
    return str(uid) in set(st.get("scanned_uids", []))

def reload_config():
    global WEBMAIL_COOKIE, WEBMAIL_XSRF, WEBMAIL_URL
    global EMAIL_ADDR, EMAIL_PASS, IMAP_HOST, IMAP_PORT, CLAUDE_KEY, COMPANY
    cfg.read(CONFIG_FILE, encoding="utf-8")
    WEBMAIL_COOKIE = c("email", "webmail_cookie", "")
    WEBMAIL_XSRF   = c("email", "webmail_xsrf_token", "")
    WEBMAIL_URL    = c("email", "webmail_session_url", "")
    # Reload credentials from saved config (overrides env if file has values)
    _addr = c("email", "address", "")
    _pass = c("email", "password", "")
    _host = c("email", "imap_host", "")
    _key  = c("claude", "api_key", "")
    _co   = c("app", "company_name", "")
    if _addr: EMAIL_ADDR = _addr
    if _pass: EMAIL_PASS = _pass
    if _host: IMAP_HOST  = _host
    if _key:  CLAUDE_KEY = _key
    if _co:   COMPANY    = _co

def save_config_value(section, key, value):
    raw = configparser.RawConfigParser()
    raw.read(CONFIG_FILE, encoding="utf-8")
    if not raw.has_section(section):
        raw.add_section(section)
    raw.set(section, key, value)
    with open(CONFIG_FILE, "w", encoding="utf-8") as f:
        raw.write(f)
    reload_config()

# ── Claude ────────────────────────────────────────────────────────────────────
# ── AI provider config ───────────────────────────────────────────────────────
GEMINI_KEY  = c("ai", "gemini_key") or os.environ.get("GEMINI_API_KEY","AIzaSyDyrZ8ZkPmoNRGL7Wv9P1qs_laKClKIIAw")
GROQ_KEY    = c("ai", "groq_key")   or os.environ.get("GROQ_API_KEY","")
OPENAI_KEY  = c("ai", "openai_key") or os.environ.get("OPENAI_API_KEY","")

_provider_blocked = set()  # tracks quota-exceeded providers

def _active_provider():
    """Return first available AI provider (skips quota-exceeded ones)."""
    if GEMINI_KEY  and len(GEMINI_KEY)  > 10 and "gemini"  not in _provider_blocked: return "gemini"
    if API_KEY     and len(API_KEY)     > 20 and "claude"  not in _provider_blocked: return "claude"
    if GROQ_KEY    and len(GROQ_KEY)    > 10 and "groq"    not in _provider_blocked: return "groq"
    if OPENAI_KEY  and len(OPENAI_KEY)  > 10 and "openai"  not in _provider_blocked: return "openai"
    return None  # will use keyword extractor

def ask_ai(prompt):
    """Call active AI provider, auto-fallback on quota errors."""
    for attempt in range(4):
        provider = _active_provider()
        if not provider:
            raise ValueError("Все AI провайдеры недоступны — используй ключевые слова")
        try:
            log.debug(f"ask_ai: trying {provider}")
            if provider == "gemini":  return _ask_gemini(prompt)
            if provider == "claude":  return _ask_claude(prompt)
            if provider == "groq":    return _ask_groq(prompt)
            if provider == "openai":  return _ask_openai(prompt)
        except Exception as ex:
            err = str(ex).lower()
            if any(k in err for k in ["429","quota","rate","exceeded","limit","billing","credit"]):
                log.warning(f"{provider} quota/rate limit — switching to next provider")
                _provider_blocked.add(provider)
            else:
                raise
    raise ValueError("Все провайдеры исчерпали квоту")

_gemini_last_call = [0.0]

def _ask_gemini(prompt):
    """Google Gemini Flash — free tier: 15 RPM. Rate limited to 1 req/4s."""
    import time as _t
    # Rate limit: wait between calls (free tier = 15 req/min = 1 per 4s)
    elapsed = _t.time() - _gemini_last_call[0]
    if elapsed < 4.0:
        _t.sleep(4.0 - elapsed)
    _gemini_last_call[0] = _t.time()

    model = "gemini-2.0-flash"
    url   = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={GEMINI_KEY}"
    body  = {
        "contents": [{"parts": [{"text": CLAUDE_SYSTEM + "\n\n" + prompt}]}],
        "generationConfig": {"maxOutputTokens": 512, "temperature": 0.1},
    }
    # Retry up to 3 times on 429
    for attempt in range(3):
        r = requests.post(url, json=body, timeout=30)
        if r.status_code == 200:
            parts = r.json().get("candidates",[{}])[0].get("content",{}).get("parts",[])
            return "".join(p.get("text","") for p in parts)
        elif r.status_code == 429:
            wait = (attempt + 1) * 10
            log.warning(f"Gemini 429 rate limit — waiting {wait}s (attempt {attempt+1}/3)")
            _t.sleep(wait)
        else:
            log.error(f"Gemini error: {r.status_code} {r.text[:200]}")
            r.raise_for_status()
    raise Exception("Gemini: exceeded retry limit on 429")

def _ask_claude(prompt):
    """Anthropic Claude — paid."""
    r = requests.post(
        "https://api.anthropic.com/v1/messages",
        headers={"Content-Type":"application/json",
                 "x-api-key": API_KEY,
                 "anthropic-version":"2023-06-01"},
        json={"model": MODEL, "max_tokens": 1000,
              "system": CLAUDE_SYSTEM,
              "messages": [{"role":"user","content": prompt}]},
        timeout=30,
    )
    if r.status_code != 200:
        log.error(f"Claude API error: {r.status_code} {r.text[:200]}")
        r.raise_for_status()
    return "".join(b.get("text","") for b in r.json().get("content",[]))

def _ask_groq(prompt):
    """Groq — free tier with Llama 3."""
    r = requests.post(
        "https://api.groq.com/openai/v1/chat/completions",
        headers={"Authorization": f"Bearer {GROQ_KEY}",
                 "Content-Type": "application/json"},
        json={"model": "llama-3.3-70b-versatile",
              "max_tokens": 1000, "temperature": 0.1,
              "messages": [
                  {"role": "system", "content": CLAUDE_SYSTEM},
                  {"role": "user",   "content": prompt},
              ]},
        timeout=30,
    )
    r.raise_for_status()
    return r.json()["choices"][0]["message"]["content"]

def _ask_openai(prompt):
    """OpenAI GPT — paid."""
    r = requests.post(
        "https://api.openai.com/v1/chat/completions",
        headers={"Authorization": f"Bearer {OPENAI_KEY}",
                 "Content-Type": "application/json"},
        json={"model": "gpt-4o-mini", "max_tokens": 1000, "temperature": 0.1,
              "messages": [
                  {"role": "system", "content": CLAUDE_SYSTEM},
                  {"role": "user",   "content": prompt},
              ]},
        timeout=30,
    )
    r.raise_for_status()
    return r.json()["choices"][0]["message"]["content"]

# backward compat alias
def ask_claude(prompt):
    return ask_ai(prompt)

def extract_json(text):
    for o, cl in [("{","}"),("[","]")]:
        s, e = text.find(o), text.rfind(cl)
        if s != -1 and e != -1:
            try:
                return json.loads(text[s:e+1])
            except Exception:
                pass
    return None

# ── Email helpers ─────────────────────────────────────────────────────────────
def decode_header(val):
    if not val: return ""
    parts = email.header.decode_header(val)
    return " ".join(
        p.decode(enc or "utf-8", errors="replace") if isinstance(p, bytes) else str(p)
        for p, enc in parts
    )

def get_plain_body(msg):
    if msg.is_multipart():
        for part in msg.walk():
            if (part.get_content_type() == "text/plain"
                    and "attachment" not in str(part.get("Content-Disposition",""))):
                try:
                    return part.get_payload(decode=True).decode(
                        part.get_content_charset() or "utf-8", errors="replace")
                except Exception:
                    pass
        return ""
    try:
        return msg.get_payload(decode=True).decode(
            msg.get_content_charset() or "utf-8", errors="replace")
    except Exception:
        return ""

def has_attachment(msg):
    return any(p.get_content_disposition() == "attachment" for p in msg.walk())

def is_invoice_candidate(subject, body, has_att):
    if has_att:
        return True
    text = (subject + " " + body).lower()
    return any(kw in text for kw in INVOICE_KW)

def build_invoice(obj, uid, subject, sender, date_str, attach, source):
    amount       = float(obj.get("amount",     0) or 0)
    amount_ex    = obj.get("amount_ex_vat")
    vat_amount   = obj.get("vat_amount")
    vat_rate     = obj.get("vat_rate")

    # Auto-calculate missing VAT fields for Estonian invoices (24%)
    if amount > 0 and vat_rate and not vat_amount:
        vat_amount = round(amount - amount / (1 + vat_rate / 100), 2)
    if amount > 0 and vat_rate and not amount_ex:
        amount_ex  = round(amount / (1 + vat_rate / 100), 2)
    if amount_ex and vat_amount is None and amount > 0:
        vat_amount = round(amount - float(amount_ex), 2)
    if amount_ex and vat_rate is None and amount > 0:
        ex = float(amount_ex)
        if ex > 0:
            vat_rate = round((amount / ex - 1) * 100, 1)

    return {
        "id":             f"{int(time.time()*1000)}-{uid}",
        "email_uid":      str(uid),
        "vendor":         obj.get("vendor",""),
        "invoice_number": obj.get("invoice_number",""),
        "amount":         amount,
        "amount_ex_vat":  float(amount_ex)  if amount_ex  is not None else None,
        "vat_amount":     float(vat_amount) if vat_amount is not None else None,
        "vat_rate":       float(vat_rate)   if vat_rate   is not None else None,
        "currency":       obj.get("currency","EUR"),
        "due_date":       obj.get("due_date","") or "",
        "issue_date":     obj.get("issue_date", date_str) or date_str,
        "description":    obj.get("description",""),
        "category":       obj.get("category","other"),
        "status":         "pending",
        "has_attachment": bool(attach),
        "email_subject":  subject,
        "email_from":     sender,
        "added_at":       datetime.now().isoformat(),
        "source":         source,
    }

# ── Core email processor ──────────────────────────────────────────────────────

# ── Keyword-based invoice extractor (no Claude API needed) ────────────────────
INVOICE_KW_STRONG = [
    # Estonian
    "arve","arve nr","arved","tasuda","makse","maksetähtaeg","maksenõue",
    # English  
    "invoice","bill","payment due","amount due","please pay","remittance",
    "overdue","pro forma","statement of account",
    # Russian
    "счёт","счет","счёт-фактура","к оплате","оплата","квитанция","задолженность",
    # German
    "rechnung","zahlung","fällig","betrag fällig","mahnung","zahlungserinnerung",
    # Finnish/Nordic
    "lasku","maksu","eräpäivä","faktura","betalning","förfallodatum",
    # French/Spanish/Italian
    "facture","factura","fattura","paiement","pago","pagamento",
    # Latvian/Lithuanian
    "rēķins","sąskaita","apmokėjimas",
]

AMOUNT_PATTERNS = [
    # €1,234.56 or EUR 1234.56 or 1 234,56 €
    r"(?:EUR|€|USD|\$|GBP|£|SEK|NOK|DKK|PLN|CHF)?\s*([\d\s]{1,8}[,.]\d{2})\s*(?:EUR|€|USD|\$)?",
    r"(?:summa|amount|total|kokku|итого|gesamt|montant|importe|totale)[:\s]+(?:EUR|€)?\s*([\d\s,.]+)",
    r"([\d]+[,.]\d{2})\s*(?:EUR|€|eur)",
    r"(?:EUR|€)\s*([\d]+[,.]\d{2})",
]

VENDOR_PATTERNS = [
    r"^(?:from|от|von|de|da):\s*(.+)",
    r"(?:company|firma|ettevõte|компания)[:\s]+([A-ZÀ-Ža-zÀ-ž\s&.,]{3,50})",
    r"([A-Z][A-Za-zÀ-ž\s]{2,30}\s+(?:OÜ|AS|SIA|UAB|GmbH|Ltd|LLC|SA|NV|BV|AB))",
]

DUE_DATE_PATTERNS = [
    r"(?:due|tähtaeg|fällig|срок|eräpäivä|vencimiento|échéance)[:\s]+([\d]{1,2}[./-][\d]{1,2}[./-][\d]{2,4})",
    r"(?:pay by|maksta|bezahlen bis|оплатить до)[:\s]+([\d]{1,2}[./-][\d]{1,2}[./-][\d]{2,4})",
    r"([\d]{2}[.][\d]{2}[.][\d]{4})",  # DD.MM.YYYY
    r"([\d]{4}-[\d]{2}-[\d]{2})",      # YYYY-MM-DD
]


def extract_amount(text):
    """Extract monetary amount from text using regex."""
    for pat in AMOUNT_PATTERNS:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            raw = m.group(1).strip().replace(" ","").replace(",",".")
            try:
                val = float(raw)
                if 0.01 <= val <= 9_999_999:
                    return round(val, 2)
            except Exception:
                pass
    return 0.0


def extract_vendor(subject, sender, body):
    """Extract vendor name from email."""
    # Try sender name first
    if sender:
        name_part = sender.split("<")[0].strip().strip('"').strip("'")
        if 2 < len(name_part) < 60 and "@" not in name_part:
            return name_part
        # Extract from email domain
        email_match = re.search(r"@([\w.-]+)", sender)
        if email_match:
            domain = email_match.group(1)
            parts = domain.split(".")
            if len(parts) >= 2:
                return parts[-2].title()
    # Try body/subject for company patterns
    for pat in VENDOR_PATTERNS:
        m = re.search(pat, subject + " " + body[:500], re.IGNORECASE | re.MULTILINE)
        if m:
            return m.group(1).strip()[:60]
    return subject[:40] if subject else "Unknown"


def extract_due_date(text, email_date):
    """Extract due date from email text."""
    for pat in DUE_DATE_PATTERNS:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            raw = m.group(1)
            # Try to parse
            for fmt in ["%d.%m.%Y","%d/%m/%Y","%Y-%m-%d","%d-%m-%Y",
                        "%d.%m.%y","%m/%d/%Y"]:
                try:
                    from datetime import datetime as _dt
                    d = _dt.strptime(raw, fmt).date()
                    # Sanity check - within 2 years
                    from datetime import date as _d2
                    today = _d2.today()
                    if _d2(today.year-1, 1, 1) <= d <= _d2(today.year+2, 12, 31):
                        return str(d)
                except Exception:
                    pass
    # Default: email date + 30 days
    try:
        from datetime import date as _d3, timedelta
        base = _d3.fromisoformat(email_date[:10])
        return str(base + timedelta(days=30))
    except Exception:
        return email_date[:10] if email_date else ""


def extract_currency(text):
    """Detect currency from text."""
    for cur, pats in [
        ("EUR", [r"EUR|€|euro"]),
        ("USD", [r"USD|\$|dollar"]),
        ("GBP", [r"GBP|£|pound"]),
        ("SEK", [r"SEK|kr\b"]),
    ]:
        for pat in pats:
            if re.search(pat, text, re.IGNORECASE):
                return cur
    return "EUR"  # default


def guess_category(subject, body, sender):
    """Guess invoice category from keywords."""
    text = (subject + " " + body[:300] + " " + sender).lower()
    if any(k in text for k in ["elekter","electricity","energy","strom","énergie",
                                 "vesi","water","wasser","heat","küte","gas"]):
        return "utilities"
    if any(k in text for k in ["rent","üür","miete","loyer","alquiler","affitto"]):
        return "rent"
    if any(k in text for k in ["hosting","domain","server","cloud","software",
                                 "saas","subscription","tarkvara"]):
        return "software"
    if any(k in text for k in ["tax","vat","maks","steuer","taxe","impuesto"]):
        return "tax"
    if any(k in text for k in ["transport","logistics","shipping","freight","cargo"]):
        return "logistics"
    return "services"


def is_invoice_by_keywords(subject, body, sender, has_att):
    """Determine if email is an invoice using keywords. Returns confidence 0-100."""
    text = (subject + " " + body[:1000] + " " + sender).lower()
    score = 0
    
    # Strong invoice keywords in subject = high confidence
    for kw in INVOICE_KW_STRONG:
        if kw in subject.lower():
            score += 40
            break
    
    # Keywords in body
    for kw in INVOICE_KW_STRONG[:15]:  # most important ones
        if kw in text:
            score += 10
            break
    
    # Has attachment = likely invoice
    if has_att:
        score += 20
    
    # Amount pattern found
    if extract_amount(text) > 0:
        score += 30
    
    # Negative signals
    if any(k in text for k in ["newsletter","unsubscribe","отписаться",
                                 "promotion","sale","%off","discount code"]):
        score -= 40
    if any(k in text for k in ["meeting","call","calendar","invite","demo"]):
        score -= 30
        
    return min(100, max(0, score))



def process_emails(emails_raw, emit, fetch_body=None, source="webmail"):
    """
    Run Claude on list of email dicts.
    Emits progress events and skips already-scanned UIDs.
    Returns list of new invoices.
    """
    existing     = load_invoices()
    existing_ids = {i.get("email_uid") for i in existing}
    new_invs     = []

    # Filter to unprocessed candidates
    # Only skip emails already saved as invoices (existing_ids)
    # Don't skip based on scanned_uids — date/uid offsets handle dedup
    candidates = []
    for em in emails_raw:
        uid = str(em.get("id") or em.get("_id") or em.get("uid") or "")
        if uid in existing_ids:
            continue
        subj = str(em.get("subject") or "")
        body = str(em.get("text") or em.get("intro") or em.get("body") or "")
        att  = bool(em.get("hasAttachments") or em.get("has_attachment")
                    or em.get("attachments"))
        if is_invoice_candidate(subj, body, att):
            candidates.append(em)

    total   = len(candidates)
    already = len(emails_raw) - total
    emit(f"Получено {len(emails_raw)} писем | {already} уже есть в базе | {total} на анализ")

    if total == 0:
        emit("Новых писем не найдено — все уже в базе", "warn")
        return []

    # Emit initial progress
    emit(f"__progress__ 0 {total} 0 0", "progress")

    start_time   = time.time()
    last_uid     = None
    last_date    = None
    processed_uids = []

    for idx, em in enumerate(candidates):
        uid  = str(em.get("id") or em.get("_id") or em.get("uid") or "")
        subj = str(em.get("subject") or "")
        frm  = em.get("from") or em.get("sender") or ""
        if isinstance(frm, dict):
            frm = frm.get("address") or frm.get("name") or ""
        frm  = str(frm)
        body = str(em.get("text") or em.get("intro") or em.get("body") or "")
        att  = bool(em.get("hasAttachments") or em.get("has_attachment")
                    or em.get("attachments"))
        ds   = str(em.get("date") or em.get("idate") or em.get("received_at")
                   or date.today().isoformat())[:10]

        # Track progress
        current   = idx + 1
        elapsed   = time.time() - start_time
        speed     = current / elapsed if elapsed > 0 else 0
        remaining = total - current
        eta       = int(remaining / speed) if speed > 0 else 0

        # Emit progress event
        emit(f"__progress__ {current} {total} {speed:.1f} {eta}", "progress")
        emit(f"[{current}/{total}] {subj[:50] or uid}")

        # Fetch full body if needed
        if len(body) < 100 and fetch_body:
            try:
                full = fetch_body(uid)
                if full and len(full) > len(body):
                    body = re.sub(r"<[^>]+>", " ", full) if "<" in full else full
            except Exception:
                pass

        try:
            # Try Claude API first
            prompt = CLAUDE_TMPL.format(
                subject=subj, sender=frm, date=ds,
                has_att=att, body=body[:2000])
            obj = extract_json(ask_claude(prompt))
            if obj and obj.get("is_invoice"):
                inv = build_invoice(obj, uid, subj, frm, ds, att, source)
                new_invs.append(inv)
                emit(f"✓ [{_active_provider().upper()}] {inv['vendor']} {inv['amount']} {inv['currency']}", "ok")
        except Exception as ex:
            # Claude failed (no credits/API error) → use keyword extractor
            err_str = str(ex)
            if "credit" in err_str.lower() or "balance" in err_str.lower() or "api" in err_str.lower():
                emit(f"⚠ Claude недоступен — использую анализ по ключевым словам", "warn")
            score = is_invoice_by_keywords(subj, body, frm, att)
            if score >= 50:
                amount   = extract_amount(subj + " " + body[:1000])
                vendor   = extract_vendor(subj, frm, body)
                due_date = extract_due_date(body + " " + subj, ds)
                currency = extract_currency(subj + " " + body[:500])
                category = guess_category(subj, body, frm)
                inv = build_invoice({
                    "is_invoice":      True,
                    "vendor":          vendor,
                    "amount":          amount or 0,
                    "currency":        currency,
                    "due_date":        due_date,
                    "issue_date":      ds[:10],
                    "description":     subj[:100],
                    "category":        category,
                    "invoice_number":  "",
                }, uid, subj, frm, ds, att, source)
                new_invs.append(inv)
                emit(f"✓ [KW] {vendor} {amount} {currency} (score={score})", "ok")
            elif score >= 25:
                emit(f"  ~ Возможно счёт (score={score}): {subj[:50]}", "warn")
            if score < 50:
                emit(f"Пропускаю {uid}: {subj[:40]}", "info")

        processed_uids.append(uid)
        last_uid  = uid
        last_date = ds

    # Save invoices and update state
    if new_invs:
        save_invoices(existing + new_invs)
        emit(f"Done! Added {len(new_invs)} new invoices", "ok")
    else:
        emit("Новых счетов не найдено — состояние обновлено", "ok")

    update_scan_state(processed_uids, last_uid, last_date)
    total_time = time.time() - start_time
    avg_speed  = len(processed_uids) / total_time if total_time > 0 else 0
    emit(f"Scanned {len(processed_uids)} emails in {total_time:.0f}s ({avg_speed:.1f}/s)", "ok")
    emit(f"__progress__ {total} {total} {avg_speed:.1f} 0", "progress")
    return new_invs


# ═══════════════════════════════════════════════════════════════════════════════
#  AUTO-AUTH: keeps session alive without user interaction
# ═══════════════════════════════════════════════════════════════════════════════
import urllib.parse as urlparse


# ── Proxy detection ───────────────────────────────────────────────────────────
def _get_system_proxies():
    """Read proxy settings from Windows registry / env."""
    proxies = {}
    try:
        import winreg  # Windows only - skip on Linux/cloud
        key = winreg.OpenKey(winreg.HKEY_CURRENT_USER,
            r"Software\Microsoft\Windows\CurrentVersion\Internet Settings")
        enabled, _ = winreg.QueryValueEx(key, "ProxyEnable")
        if enabled:
            proxy_server, _ = winreg.QueryValueEx(key, "ProxyServer")
            if proxy_server:
                if "=" in proxy_server:
                    for part in proxy_server.split(";"):
                        if "=" in part:
                            k, v = part.split("=", 1)
                            proxies[k.strip()] = "http://" + v.strip()
                else:
                    proxies["http"]  = "http://" + proxy_server
                    proxies["https"] = "http://" + proxy_server
                log.info(f"System proxy detected: {proxy_server}")
        winreg.CloseKey(key)
    except Exception:
        pass
    # On Windows, env proxy vars may conflict - only use registry
    # Don't add env proxies automatically
    return proxies

SYSTEM_PROXIES = _get_system_proxies()

def _make_session_with_proxy():
    """Create requests session that works through corporate proxy."""
    sess = requests.Session()
    if SYSTEM_PROXIES:
        sess.proxies.update(SYSTEM_PROXIES)
        log.info(f"Using proxy: {SYSTEM_PROXIES}")
    else:
        # Try without proxy (direct connection)
        sess.proxies = {"http": None, "https": None}
    return sess

BASE_WM = "https://webmail.ee"
API_WM  = "https://api-mail-v1.webmail.ee"

def _build_sess_from_config():
    """Build requests.Session from saved cookies in config."""
    sess = _make_session_with_proxy()
    sess.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120",
        "Origin":     BASE_WM,
        "Referer":    BASE_WM + "/",
        "Accept":     "application/json, */*",
    })
    if WEBMAIL_COOKIE:
        for part in WEBMAIL_COOKIE.split(";"):
            part = part.strip()
            if "=" in part:
                name, _, val = part.partition("=")
                for dom in [BASE_WM.replace("https://",""),
                            API_WM.replace("https://","")]:
                    sess.cookies.set(name.strip(), val.strip(), domain=dom)
    xsrf = WEBMAIL_XSRF or urlparse.unquote(sess.cookies.get("XSRF-TOKEN",""))
    if xsrf:
        sess.headers["X-XSRF-TOKEN"] = xsrf
    tok = urlparse.unquote(sess.cookies.get("token",""))
    if len(tok) > 20:
        sess.headers["Authorization"] = "Bearer " + tok
    return sess

def _get_token_expiry():
    """Return expiry datetime from token_expiry_u_0 cookie or config."""
    if not WEBMAIL_COOKIE:
        return None
    for part in WEBMAIL_COOKIE.split(";"):
        part = part.strip()
        if part.lower().startswith("token_expiry"):
            try:
                val = part.split("=",1)[1]
                # Format: 2026-04-14T08:16:23+00:00
                from datetime import timezone
                dt = datetime.fromisoformat(val.replace("+00:00",""))
                return dt.replace(tzinfo=timezone.utc)
            except Exception:
                pass
    return None

def _try_refresh_token(sess):
    """Use refresh_token cookie to get new session. Returns True on success."""
    log.info("AUTH: trying token refresh...")
    rt = sess.cookies.get("refresh_token","")
    if not rt:
        log.warning("AUTH: no refresh_token in cookies")
        return False
    try:
        r = sess.post(BASE_WM + "/auth/refresh",
                      timeout=15,
                      headers={"Content-Type":"application/json",
                               "X-Requested-With":"XMLHttpRequest"},
                      json={"refresh_token": rt})
        log.info(f"AUTH: /auth/refresh → {r.status_code}")
        if r.status_code == 200:
            try:
                d = r.json()
                log.info(f"AUTH: refresh response keys: {list(d.keys())}")
                tok = d.get("token") or d.get("access_token") or d.get("ccd")
                if tok:
                    _save_auth_cookies(sess, tok)
                    return True
            except Exception:
                pass
        # Also try GET refresh (some implementations)
        r2 = sess.get(BASE_WM + "/auth/refresh", timeout=10,
                     headers={"Accept":"application/json"})
        log.info(f"AUTH: GET /auth/refresh → {r2.status_code}")
        if r2.status_code == 200:
            try:
                d2 = r2.json()
                tok2 = d2.get("token") or d2.get("access_token")
                if tok2:
                    _save_auth_cookies(sess, tok2)
                    return True
            except Exception:
                pass
    except Exception as ex:
        log.error(f"AUTH: refresh error: {ex}")
    return False

def _try_password_login(sess):
    """Login with stored email/password. Returns True on success."""
    if not EMAIL_ADDR or not EMAIL_PASS:
        log.warning("AUTH: no credentials in config")
        return False
    log.info(f"AUTH: password login as {EMAIL_ADDR}...")
    attempts = [
        (BASE_WM + "/auth/accounts", {"login":    EMAIL_ADDR,              "password": EMAIL_PASS}),
        (BASE_WM + "/auth/accounts", {"login":    EMAIL_ADDR.split("@")[0],"password": EMAIL_PASS}),
        (BASE_WM + "/auth/accounts", {"email":    EMAIL_ADDR,              "password": EMAIL_PASS}),
        (BASE_WM + "/login",         {"login":    EMAIL_ADDR,              "password": EMAIL_PASS}),
        (BASE_WM + "/login",         {"email":    EMAIL_ADDR,              "password": EMAIL_PASS}),
        (BASE_WM + "/login",         {"username": EMAIL_ADDR,              "password": EMAIL_PASS}),
    ]
    # First get XSRF
    try:
        r0 = sess.get(BASE_WM + "/", timeout=10)
        xsrf0 = urlparse.unquote(sess.cookies.get("XSRF-TOKEN",""))
        if xsrf0:
            sess.headers["X-XSRF-TOKEN"] = xsrf0
    except Exception:
        pass

    for url, payload in attempts:
        for ct in ["json", "form"]:
            try:
                hdrs = {"Content-Type": "application/json" if ct=="json"
                        else "application/x-www-form-urlencoded",
                        "X-Requested-With": "XMLHttpRequest"}
                kw = {"json": payload} if ct == "json" else {"data": payload}
                ra = sess.post(url, timeout=12, headers=hdrs,
                               allow_redirects=True, **kw)
                log.info(f"AUTH: POST {url.replace(BASE_WM,'')} [{ct}] → {ra.status_code} | ck={list(sess.cookies.keys())}")
                has_ck = any(n in sess.cookies for n in ["ccd","token","laravel_session"])
                try:
                    d = ra.json()
                    has_tok = bool(d.get("token") or d.get("id") or d.get("success"))
                    if d.get("error") or d.get("message"):
                        log.warning(f"AUTH: server: {d.get('error') or d.get('message')}")
                except Exception:
                    has_tok = False
                if has_ck or has_tok:
                    new_xsrf = urlparse.unquote(sess.cookies.get("XSRF-TOKEN",""))
                    if new_xsrf:
                        sess.headers["X-XSRF-TOKEN"] = new_xsrf
                    new_tok = urlparse.unquote(sess.cookies.get("token",""))
                    if len(new_tok) > 20:
                        sess.headers["Authorization"] = "Bearer " + new_tok
                    _save_auth_cookies(sess)
                    return True
            except Exception as ex:
                log.debug(f"AUTH: {url} [{ct}] error: {ex}")
    return False

def _save_auth_cookies(sess, extra_token=None):
    """Save current session cookies to config.ini."""
    parts = [f"{c.name}={c.value}" for c in sess.cookies]
    if extra_token and not any("token=" in p for p in parts):
        parts.append(f"token={extra_token}")
    cookie_str = "; ".join(parts)
    if not cookie_str:
        return
    xsrf = urlparse.unquote(sess.cookies.get("XSRF-TOKEN",""))
    save_config_value("email", "webmail_cookie", cookie_str)
    if xsrf:
        save_config_value("email", "webmail_xsrf_token", xsrf)
    log.info(f"AUTH: saved {len(cookie_str)} bytes of cookies")
    _capture_state.update({"status":"done",
                           "message":f"Auto-refreshed ({len(cookie_str)} bytes)",
                           "success":True})

def ensure_auth():
    """
    Main entry point: ensure we have valid session.
    Call before each scan. Returns True if ready, False if failed.
    """
    global WEBMAIL_COOKIE, WEBMAIL_XSRF
    log.debug("ensure_auth() called")
    log.debug(f"  WEBMAIL_COOKIE len: {len(WEBMAIL_COOKIE)}")
    log.debug(f"  EMAIL_ADDR: {EMAIL_ADDR}")
    log.debug(f"  EMAIL_PASS set: {bool(EMAIL_PASS)}")

    sess = _build_sess_from_config()
    log.debug(f"  Session cookies: {list(sess.cookies.keys())}")
    log.debug(f"  Session headers auth: {'Authorization' in sess.headers}")
    expiry = _get_token_expiry()
    log.debug(f"  Token expiry: {expiry}")

    if expiry:
        from datetime import timezone
        now = datetime.now(timezone.utc)
        days_left = (expiry - now).total_seconds() / 86400
        log.info(f"AUTH: token expires in {days_left:.1f} days ({expiry.date()})")

        if days_left > 1:
            log.info("AUTH: token still valid, no refresh needed")
            return True
        else:
            log.info(f"AUTH: token expires in {days_left:.1f}d — refreshing...")

    # Try refresh_token first (no password needed)
    if WEBMAIL_COOKIE and "refresh_token" in WEBMAIL_COOKIE:
        if _try_refresh_token(sess):
            log.info("AUTH: refreshed via refresh_token ✓")
            return True

    # Fall back to password login
    if EMAIL_PASS:
        if _try_password_login(sess):
            log.info("AUTH: logged in via password ✓")
            return True

    if WEBMAIL_COOKIE:
        log.info("AUTH: using existing cookies as-is")
        return True

    log.error("AUTH: no valid auth method available")
    return False

def auth_watchdog():
    """
    Background thread: auto-refresh session every 8 days.
    Runs silently, no user interaction needed.
    """
    while True:
        time.sleep(8 * 3600)  # check every 8 hours
        try:
            expiry = _get_token_expiry()
            if expiry:
                from datetime import timezone
                days_left = (expiry - datetime.now(timezone.utc)).total_seconds() / 86400
                if days_left < 2:
                    log.info(f"AUTH watchdog: {days_left:.1f}d left — refreshing...")
                    ensure_auth()
            elif WEBMAIL_COOKIE:
                # No expiry info but have cookies - try a test request
                sess = _build_sess_from_config()
                try:
                    r = sess.get(API_WM + "/auth/accounts",
                                timeout=8,
                                headers={"Accept":"application/json"})
                    if r.status_code == 401:
                        log.info("AUTH watchdog: 401 — refreshing session...")
                        ensure_auth()
                except Exception:
                    pass
        except Exception as ex:
            log.error(f"AUTH watchdog error: {ex}")


# ═══════════════════════════════════════════════════════════════════════════════
#  PLAYWRIGHT AUTO-LOGIN
# ═══════════════════════════════════════════════════════════════════════════════
_PW_BROWSERS = BASE_DIR / "pw-browsers"  # local Chromium

def _pw_cookies_to_str(cookies):
    """Convert Playwright cookie list to cookie string."""
    parts = [f"{c['name']}={c['value']}" for c in cookies
             if c.get('value') and c.get('name')]
    return "; ".join(parts)

def cdp_login(emit=None):
    """
    Auto-login via Chrome DevTools Protocol (CDP).
    Requires Chrome running via run_chrome.bat (port 9222).
    No external downloads needed - uses websocket-client.
    """
    def E(msg, t="info"):
        log.info(msg)
        if emit: emit(msg, t)

    import urllib.request as _ur
    import json as _j
    import time as _t

    try:
        import websocket as _ws
    except ImportError:
        E("websocket-client not installed!", "err")
        return None

    # Step 1: Get list of Chrome tabs
    E("Подключение к Chrome (порт 9222)...")
    try:
        tabs_raw = _ur.urlopen("http://localhost:9222/json", timeout=3).read()
        tabs = _j.loads(tabs_raw)
    except Exception as ex:
        E(f"Chrome не запущен на порту 9222. Используй run_chrome.bat! ({ex})", "err")
        return None

    # Find or create webmail.ee tab
    wm_tab = None
    for t in tabs:
        if "webmail.ee" in t.get("url","") and t.get("type") == "page":
            wm_tab = t
            break

    if not wm_tab:
        # Open new tab with webmail.ee
        E("Открываю webmail.ee...")
        try:
            new_tab = _j.loads(
                _ur.urlopen(f"http://localhost:9222/json/new?https://webmail.ee", timeout=3).read()
            )
            _t.sleep(3)
            # Re-fetch tabs
            tabs = _j.loads(_ur.urlopen("http://localhost:9222/json", timeout=3).read())
            for t in tabs:
                if "webmail.ee" in t.get("url","") and t.get("type") == "page":
                    wm_tab = t
                    break
        except Exception as ex:
            E(f"Не могу открыть вкладку: {ex}", "err")
            return None

    if not wm_tab:
        E("Вкладка webmail.ee не найдена!", "err")
        return None

    ws_url = wm_tab.get("webSocketDebuggerUrl","")
    if not ws_url:
        E("WebSocket URL недоступен", "err")
        return None

    E(f"Вкладка найдена: {wm_tab.get('url','')[:60]}", "ok")

    # Step 2: Connect via WebSocket CDP
    try:
        ws = _ws.create_connection(
            ws_url, timeout=10,
            origin=f"http://localhost:9222",
            host=f"localhost:9222",
        )
    except Exception as ex:
        E(f"WebSocket ошибка: {ex}", "err")
        return None

    _msg_id = [0]
    def cdp(method, params=None):
        _msg_id[0] += 1
        msg = {"id": _msg_id[0], "method": method, "params": params or {}}
        ws.send(_j.dumps(msg))
        # Read until we get our response
        for _ in range(20):
            raw = ws.recv()
            data = _j.loads(raw)
            if data.get("id") == _msg_id[0]:
                return data.get("result", {})
        return {}

    try:
        # Enable necessary domains
        cdp("Page.enable")
        cdp("Network.enable")
        cdp("Runtime.enable")

        # Get current URL
        result = cdp("Runtime.evaluate", {"expression": "window.location.href"})
        current_url = result.get("result",{}).get("value","")
        E(f"Текущий URL: {current_url[:60]}")

        if "/u/0/" in current_url:
            E("Уже вошли в систему!", "ok")
        else:
            # Navigate to webmail if needed
            if "webmail.ee" not in current_url:
                E("Перехожу на webmail.ee...")
                cdp("Page.navigate", {"url": "https://webmail.ee"})
                _t.sleep(3)

            E("Ввожу логин...")
            # Fill login form
            fill_js = f"""
(function() {{
    var selectors = ['[name="login"]','[name="email"]','[name="username"]','input[type="email"]'];
    for (var s of selectors) {{
        var el = document.querySelector(s);
        if (el) {{ el.value = {_j.dumps(EMAIL_ADDR)}; el.dispatchEvent(new Event('input', {{bubbles:true}})); return s; }}
    }}
    return null;
}})()
"""
            r1 = cdp("Runtime.evaluate", {"expression": fill_js.strip()})
            E(f"Логин введён в: {r1.get('result',{}).get('value','?')}")

            E("Ввожу пароль...")
            pwd_js = f"""
(function() {{
    var el = document.querySelector('[name="password"], input[type="password"]');
    if (el) {{ el.value = {_j.dumps(EMAIL_PASS)}; el.dispatchEvent(new Event('input', {{bubbles:true}})); return true; }}
    return false;
}})()
"""
            cdp("Runtime.evaluate", {"expression": pwd_js.strip()})

            E("Отправляю форму...")
            submit_js = """
(function() {
    var btn = document.querySelector('[type="submit"], button[class*="login"], button[class*="sign"]');
    if (btn) { btn.click(); return true; }
    var form = document.querySelector('form');
    if (form) { form.submit(); return true; }
    return false;
})()
"""
            cdp("Runtime.evaluate", {"expression": submit_js.strip()})

            # Wait for login
            E("Ожидаю входа...")
            for i in range(20):
                _t.sleep(1)
                r = cdp("Runtime.evaluate", {"expression": "window.location.href"})
                url = r.get("result",{}).get("value","")
                if "/u/0/" in url:
                    E("Вход выполнен!", "ok")
                    break
                if i == 19:
                    E(f"Таймаут входа. URL: {url}", "warn")

        # Step 3: Get cookies
        E("Получаю куки...")
        result = cdp("Network.getAllCookies")
        all_cookies = result.get("cookies",[])
        wm_cookies = [c for c in all_cookies
                      if "webmail.ee" in c.get("domain","")
                      or c.get("domain","").endswith("webmail.ee")]

        if not wm_cookies:
            E(f"Куки webmail.ee не найдены (всего {len(all_cookies)} куков)", "warn")
            # Try all cookies
            wm_cookies = all_cookies

        cookie_str = "; ".join(f"{c['name']}={c['value']}" for c in wm_cookies
                                if c.get('name') and c.get('value'))

        if not cookie_str:
            E("Куки пусты!", "err")
            ws.close()
            return None

        E(f"Получено {len(wm_cookies)} куков ({len(cookie_str)} байт)", "ok")

        # Save to config
        save_config_value("email", "webmail_cookie", cookie_str)
        xsrf = next((c["value"] for c in wm_cookies
                     if c["name"].upper() == "XSRF-TOKEN"), "")
        if xsrf:
            save_config_value("email", "webmail_xsrf_token",
                              urlparse.unquote(xsrf))

        _capture_state.update({
            "status": "done",
            "message": f"CDP: {len(wm_cookies)} куков сохранено",
            "success": True,
        })
        E("Куки сохранены в config.ini!", "ok")
        ws.close()
        return cookie_str

    except Exception as ex:
        E(f"Ошибка CDP: {ex}", "err")
        log.error("CDP login error:", exc_info=True)
        try: ws.close()
        except Exception: pass
        return None


def playwright_login(emit=None):
    """
    Auto-login: uses CDP websocket directly (no browser download needed).
    Chrome must be running via run_chrome.bat.
    """
    return cdp_login(emit)


def _playwright_login_old(emit=None):
    """
    Full auto-login using Playwright (fallback, requires install_browsers.bat).
    """


def _resolve_host_doh(hostname):
    """Resolve hostname via multiple DoH providers."""
    providers = [
        f"https://dns.google/resolve?name={hostname}&type=A",
        f"https://cloudflare-dns.com/dns-query?name={hostname}&type=A",
        f"https://dns.quad9.net:5053/dns-query?name={hostname}&type=A",
    ]
    for url in providers:
        try:
            r = requests.get(url, headers={"Accept": "application/dns-json"}, timeout=4)
            if r.status_code == 200:
                answers = r.json().get("Answer", [])
                ips = [a["data"] for a in answers if a.get("type") == 1]
                if ips:
                    log.info(f"DoH resolved {hostname} → {ips[0]}")
                    return ips[0]
        except Exception as ex:
            log.debug(f"DoH {url[:30]} failed: {ex}")
    return hostname

def _try_imap_connect(hosts, port, email, password):
    """Try connecting to multiple IMAP hostnames in order."""
    import ssl as _ssl
    for host in hosts:
        ip = _resolve_host_doh(host)
        for target in ([ip, host] if ip != host else [host]):
            try:
                ctx = _ssl.create_default_context()
                if target != host:
                    ctx.check_hostname = False
                    ctx.verify_mode = _ssl.CERT_NONE
                mail = imaplib.IMAP4_SSL(target, port, ssl_context=ctx)
                mail.login(email, password)
                log.info(f"IMAP connected: {host} via {target}")
                return mail, host
            except Exception as ex:
                log.debug(f"IMAP {target}: {ex}")
    return None, None

def scan_imap(emit, from_date=None, to_date=None):
    emit(f"Подключение к почте {IMAP_HOST}...")
    # Try multiple hostnames - DNS might resolve differently
    alt_hosts = list(dict.fromkeys([
        IMAP_HOST,
        "imap.zone.ee",
        "mail.zone.eu",
        f"mail.{EMAIL_ADDR.split('@')[1]}" if EMAIL_ADDR else "",
    ]))
    alt_hosts = [h for h in alt_hosts if h]
    emit(f"Пробую: {', '.join(alt_hosts)}", "info")
    mail, connected_host = _try_imap_connect(alt_hosts, IMAP_PORT, EMAIL_ADDR, EMAIL_PASS)
    if not mail:
        emit(f"IMAP недоступен — переключаюсь на webmail", "warn")
        return None  # caller will use webmail
    emit(f"✓ Подключено через {connected_host}", "ok")

    try:
        mail.select(IMAP_FOLDER)
        state    = load_state()
        last_uid = state.get("last_uid")

        # Add date range filter to IMAP search terms
        date_terms = []
        if from_date:
            import email.utils as _eu
            from datetime import datetime as _dt
            since_dt = _dt.strptime(from_date, "%Y-%m-%d")
            since_str = since_dt.strftime("%d-%b-%Y")
            date_terms.append(f"SINCE {since_str}".encode())
            emit(f"IMAP date filter: since {since_str}", "ok")
        if to_date:
            before_dt = _dt.strptime(to_date, "%Y-%m-%d") + __import__('datetime').timedelta(days=1)
            before_str = before_dt.strftime("%d-%b-%Y")
            date_terms.append(f"BEFORE {before_str}".encode())

        ids = set()

        # When date range given - fetch ALL emails in range (most complete)
        if date_terms:
            try:
                _, data = mail.search(None, *date_terms)
                for uid in data[0].split():
                    ids.add(uid)
                emit(f"Date-range search: {len(ids)} emails", "ok")
            except Exception as ex:
                emit(f"Date search failed: {ex}, falling back to keywords", "warn")

        # Also search by subject keywords (catches emails outside date range)
        if not date_terms or len(ids) == 0:
            for term in IMAP_SUBJECTS:
                try:
                    search_args = [term] + date_terms if date_terms else [term]
                    _, data = mail.search(None, *search_args)
                    for uid in data[0].split():
                        ids.add(uid)
                except Exception:
                    pass

        # If we have a last UID, grab newer messages too
        if last_uid and not from_date:
            try:
                _, data = mail.search(None, f"UID {int(last_uid)+1}:*")
                for uid in data[0].split():
                    ids.add(uid)
                emit(f"Searching from UID {last_uid} onwards...", "ok")
            except Exception:
                pass

        # Last resort: ALL emails up to limit
        if not ids:
            emit("No keyword matches - scanning all recent emails...", "warn")
            _, data = mail.search(None, "ALL")
            all_ids = data[0].split()
            ids = set(all_ids[-min(SCAN_LIMIT, len(all_ids)):])

        # Sort by UID desc (newest first), take limit
        try:
            ids_sorted = sorted(ids, key=lambda x: int(x.decode() if isinstance(x, bytes) else x), reverse=True)
        except Exception:
            ids_sorted = list(ids)
        ids = set(ids_sorted[:SCAN_LIMIT])

        emit(f"Found {len(ids)} messages to check (limit {SCAN_LIMIT})...")
        emails_raw = []
        for uid in list(ids)[-SCAN_LIMIT:]:
            try:
                _, data = mail.fetch(uid, "(RFC822)")
                msg = email.message_from_bytes(data[0][1])
                subj = decode_header(msg.get("Subject",""))
                sndr = decode_header(msg.get("From",""))
                body = get_plain_body(msg)
                att  = has_attachment(msg)
                try:
                    ds = parsedate_to_datetime(msg.get("Date","")).strftime("%Y-%m-%d")
                except Exception:
                    ds = date.today().isoformat()
                uid_s = uid.decode() if isinstance(uid, bytes) else str(uid)
                emails_raw.append({"id":uid_s,"subject":subj,"from":sndr,
                                   "body":body,"date":ds,"has_attachment":att})
            except Exception as ex:
                emit(f"Fetch error {uid}: {ex}", "err")
        mail.logout()
        return process_emails(emails_raw, emit, source="imap")
    except Exception as ex:
        emit(f"IMAP error: {ex}", "err")
        try: mail.logout()
        except Exception: pass
        return []

# ── Webmail HTTPS ─────────────────────────────────────────────────────────────
def make_session():
    """Build requests.Session with cookies from config."""
    BASE = "https://webmail.ee"
    sess = _make_session_with_proxy()
    sess.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120",
        "Origin": BASE, "Referer": BASE+"/", "Accept": "application/json, */*",
    })
    # Load cookies - sanitize to prevent latin-1 encoding errors
    if WEBMAIL_COOKIE:
        # Detect bad cookie (JS code saved instead of real cookies)
        if "fetch(" in WEBMAIL_COOKIE or "document.cookie" in WEBMAIL_COOKIE:
            log.error("BAD COOKIE DETECTED: contains JS code - clearing automatically")
            try:
                save_config_value("email", "webmail_cookie", "")
                reload_config()
            except Exception:
                pass
        else:
            # Strip ALL non-ASCII chars to avoid latin-1 HTTP header errors
            safe_ck = ''.join(c for c in WEBMAIL_COOKIE if ord(c) < 128)
            for part in safe_ck.split(";"):
                part = part.strip()
                if "=" in part:
                    k, _, v = part.partition("=")
                    k = k.strip(); v = v.strip()
                    if k and len(k) < 100 and len(v) < 2000:
                        try:
                            sess.cookies.set(k, v, domain="webmail.ee")
                            sess.cookies.set(k, v, domain="api-mail-v1.webmail.ee")
                        except Exception:
                            pass
    if WEBMAIL_COOKIE:
        for part in WEBMAIL_COOKIE.split(";"):
            part = part.strip()
            if "=" in part:
                name, _, val = part.partition("=")
                for d in ["webmail.ee","api-mail-v1.webmail.ee"]:
                    sess.cookies.set(name.strip(), val.strip(), domain=d)
    xsrf = WEBMAIL_XSRF or urlparse.unquote(sess.cookies.get("XSRF-TOKEN",""))
    if xsrf:
        sess.headers["X-XSRF-TOKEN"] = xsrf
    tok = urlparse.unquote(sess.cookies.get("token",""))
    if len(tok) > 20:
        sess.headers["Authorization"] = f"Bearer {tok}"
    return sess

def webmail_login(sess, email_addr, password, emit):
    """Login to webmail.ee, return True on success."""
    BASE = "https://webmail.ee"
    emit(f"Authenticating as {email_addr}...")
    try:
        r0 = sess.get(BASE+"/", timeout=12)
        xsrf = urlparse.unquote(sess.cookies.get("XSRF-TOKEN",""))
        if xsrf:
            sess.headers["X-XSRF-TOKEN"] = xsrf
        emit(f"  GET / → {r0.status_code}")
    except Exception as ex:
        emit(f"Cannot reach webmail.ee: {ex}", "err")
        return False

    attempts = [
        (BASE+"/auth/accounts", "login",    email_addr),
        (BASE+"/auth/accounts", "login",    email_addr.split("@")[0]),
        (BASE+"/auth/accounts", "email",    email_addr),
        (BASE+"/login",         "login",    email_addr),
        (BASE+"/login",         "email",    email_addr),
    ]
    for url, field, val in attempts:
        payload = {field: val, "password": password}
        for ct in ["json","form"]:
            try:
                kw   = {"json":payload} if ct=="json" else {"data":payload}
                hdrs = {"Content-Type":"application/json" if ct=="json"
                        else "application/x-www-form-urlencoded",
                        "X-Requested-With":"XMLHttpRequest"}
                ra = sess.post(url, timeout=12, headers=hdrs, allow_redirects=True, **kw)
                emit(f"  POST {url.replace(BASE,'')} [{ct},{field}] → {ra.status_code}")
                has_ck = any(n in sess.cookies for n in ["ccd","token","laravel_session"])
                try:
                    d = ra.json()
                    has_tok = bool(d.get("token") or d.get("id") or d.get("success"))
                    if d.get("error") or d.get("message"):
                        emit(f"  Server: {d.get('error') or d.get('message')}", "warn")
                except Exception:
                    has_tok = False
                if has_ck or has_tok:
                    new_xsrf = urlparse.unquote(sess.cookies.get("XSRF-TOKEN",""))
                    if new_xsrf:
                        sess.headers["X-XSRF-TOKEN"] = new_xsrf
                    emit("Авторизация успешна!", "ok")
                    return True
            except Exception as ex:
                emit(f"  {ex}", "err")
    return False

def fetch_messages(sess, user_id, mailbox_id, emit, from_date=None, to_date=None):
    """Fetch messages from api-mail-v1.webmail.ee. Returns list or None if 401."""
    API       = "https://api-mail-v1.webmail.ee"
    h         = {"Accept": "application/json"}
    state     = load_state()
    last_date = state.get("last_date")
    limit     = SCAN_LIMIT

    # Build query params - filter by date if we have last scan date
    date_param = ""
    if from_date:
        emit(f"Фильтр: с {from_date} по {to_date or '...'}", "ok")
        date_param = f"&dateAfter={from_date}"
        if to_date:
            date_param += f"&dateBefore={to_date}"
    elif last_date:
        emit(f"Продолжаю с {last_date} — новые письма после прошлого скана", "ok")
        date_param = f"&dateAfter={last_date}"

    cands = []
    if user_id and mailbox_id:
        cands.append(f"/u/0/{user_id}/mailboxes/{mailbox_id}/messages?limit={limit}&order=desc{date_param}")
    if user_id:
        cands += [f"/u/0/{user_id}/mailboxes/INBOX/messages?limit={limit}{date_param}",
                  f"/u/0/{user_id}/messages?limit={limit}{date_param}"]
    cands.append("/auth/accounts")

    for ep in cands:
        try:
            rr = sess.get(API+ep, timeout=12, headers=h)
            emit(f"  GET {ep[:60]} → {rr.status_code}")
            if rr.status_code == 401:
                return None
            if rr.status_code == 200:
                j   = rr.json()
                if ep == "/auth/accounts":
                    accs = j.get("accounts") or j.get("results") or []
                    if accs:
                        uid2 = accs[0].get("id") or accs[0].get("_id")
                        if uid2:
                            rm = sess.get(f"{API}/u/0/{uid2}/mailboxes/INBOX/messages?limit={SCAN_LIMIT}",
                                         timeout=10, headers=h)
                            if rm.status_code == 200:
                                lst = rm.json().get("results",[])
                                if lst: return lst
                    continue
                lst = (j.get("results") or j.get("messages") or
                       j.get("data") or (j if isinstance(j,list) else []))
                if lst:
                    emit(f"Got {len(lst)} messages", "ok")
                    return lst
        except Exception as ex:
            emit(f"  {ex}", "err")
    return []


# ═══════════════════════════════════════════════════════════════════════════════
#  PLAYWRIGHT AUTO-LOGIN  (headless Chromium on Railway)
# ═══════════════════════════════════════════════════════════════════════════════

def playwright_webmail_login(emit=None):
    """Login to webmail.ee using headless Chromium. Returns cookie string or None."""
    def say(msg, t="info"):
        log.info(f"PW: {msg}")
        if emit: emit(msg, t)

    if not EMAIL_ADDR or not EMAIL_PASS:
        say("❌ Нет email или пароля — зайди в '🌐 Войти в почту'", "err")
        return None

    try:
        from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
    except ImportError:
        say("❌ Playwright не установлен", "err")
        return None

    say("🌐 Запускаю браузер для входа в webmail.ee...")
    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-setuid-sandbox",
                      "--disable-dev-shm-usage", "--disable-gpu",
                      "--no-zygote", "--single-process"]
            )
            ctx = browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120",
                locale="et-EE",
            )
            page = ctx.new_page()

            say("Открываю webmail.ee...")
            page.goto("https://webmail.ee", timeout=30000)
            page.wait_for_load_state("networkidle", timeout=15000)

            # Fill login form
            say(f"Вхожу как {EMAIL_ADDR}...")
            
            # Try different selectors for email field
            for sel in ['input[name="username"]', 'input[name="email"]',
                        'input[type="email"]', 'input[name="login"]',
                        '#username', '#email', '#login']:
                try:
                    page.fill(sel, EMAIL_ADDR, timeout=2000)
                    log.debug(f"PW: filled email with selector {sel}")
                    break
                except Exception:
                    continue

            # Password field
            for sel in ['input[name="password"]', 'input[type="password"]', '#password']:
                try:
                    page.fill(sel, EMAIL_PASS, timeout=2000)
                    log.debug(f"PW: filled password with selector {sel}")
                    break
                except Exception:
                    continue

            # Submit
            for sel in ['button[type="submit"]', 'input[type="submit"]',
                        'button:has-text("Logi sisse")', 'button:has-text("Login")',
                        'button:has-text("Войти")', '.login-btn', '#login-btn']:
                try:
                    page.click(sel, timeout=3000)
                    log.debug(f"PW: clicked submit {sel}")
                    break
                except Exception:
                    continue

            # Wait for login
            say("Ожидаю подтверждения входа...")
            try:
                page.wait_for_url("**/u/**", timeout=12000)
                say("✅ Вход выполнен!", "ok")
            except PWTimeout:
                # Check if we're past login page anyway
                if "webmail.ee/u/" in page.url or "webmail.ee/mail" in page.url:
                    say("✅ Вход выполнен (redirect)!", "ok")
                else:
                    # Try to extract cookies anyway
                    log.warning(f"PW: login timeout, current URL: {page.url}")
                    say("⚠ Вход не подтверждён — пробую получить куки...", "warn")

            # Extract cookies
            cookies = ctx.cookies()
            if cookies:
                ck_str = "; ".join(
                    f"{c['name']}={c['value']}"
                    for c in cookies
                    if c.get('domain','') in ('webmail.ee', '.webmail.ee',
                                              'api-mail-v1.webmail.ee')
                    and c['value']
                    and ord(c['value'][0]) < 128  # latin-1 safe
                )
                if ck_str:
                    say(f"Получено {len(cookies)} куков ({len(ck_str)} байт)", "ok")
                    browser.close()
                    return ck_str

            browser.close()
            say("❌ Куки не получены после входа", "err")
            return None

    except Exception as ex:
        say(f"❌ Ошибка браузера: {ex}", "err")
        log.error(f"Playwright error: {ex}", exc_info=True)
        return None

def scan_webmail(emit, from_date=None, to_date=None):
    """Scan Zone.ee webmail via HTTPS."""
    API  = "https://api-mail-v1.webmail.ee"
    emit("Подключение к webmail.ee (HTTPS)...")
    log.debug(f"scan_webmail: WEBMAIL_COOKIE len={len(WEBMAIL_COOKIE)}")
    # Auto-ensure valid session before scanning
    ensure_auth()
    reload_config()
    if not WEBMAIL_COOKIE:
        # Try Playwright auto-login first (headless Chromium)
        if EMAIL_PASS and EMAIL_ADDR:
            emit("🌐 Нет сессии — запускаю автовход через браузер...", "warn")
            ck = playwright_webmail_login(emit)
            if ck:
                save_config_value("email", "webmail_cookie", ck)
                reload_config()
                emit("✅ Автовход выполнен! Куки сохранены.", "ok")
            else:
                # Fallback: try requests-based login
                sess_tmp = make_session()
                if webmail_login(sess_tmp, EMAIL_ADDR, EMAIL_PASS, emit):
                    ck2 = "; ".join(f"{c.name}={c.value}" for c in sess_tmp.cookies
                                    if all(ord(ch) < 128 for ch in c.value))
                    if ck2:
                        save_config_value("email", "webmail_cookie", ck2)
                        reload_config()
                        emit("✅ Вход выполнен (fallback)!", "ok")
                    else:
                        emit("❌ Не удалось получить куки. Войди через '🌐 Войти в почту'.", "err")
                        return []
                else:
                    emit("❌ Автовход не удался. Войди вручную через '🌐 Войти в почту'.", "err")
                    return []
        else:
            emit("❌ Нет пароля. Зайди в '🌐 Войти в почту' и введи email + пароль.", "err")
            return []
    sess = make_session()

    user_id = mailbox_id = None
    if WEBMAIL_URL and "/u/0/" in WEBMAIL_URL:
        parts = WEBMAIL_URL.rstrip("/").split("/")
        if len(parts) >= 7:
            user_id, mailbox_id = parts[5], parts[6]
            emit(f"IDs: user={user_id[:12]}...", "ok")

    def fetch_body(uid):
        mbox = mailbox_id or "INBOX"
        rb = sess.get(f"{API}/u/0/{user_id}/mailboxes/{mbox}/messages/{uid}",
                      timeout=10, headers={"Accept":"application/json"})
        if rb.status_code == 200:
            txt = rb.json().get("text") or rb.json().get("body") or ""
            return re.sub(r"<[^>]+>"," ",txt) if "<" in txt else txt
        return ""

    # Try saved cookies first
    if WEBMAIL_COOKIE:
        emit("Using saved cookies...")
        msgs = fetch_messages(sess, user_id, mailbox_id, emit, from_date=from_date, to_date=to_date)
        if msgs:
            return process_emails(msgs, emit, fetch_body, source="webmail")
        if msgs is None:
            emit("Session expired — trying password login...", "warn")

    # Password login
    if not webmail_login(sess, EMAIL_ADDR, EMAIL_PASS, emit):
        emit("❌ Ошибка авторизации — войди в почту через кнопку '🌐 Войти в почту'", "err")
        return []

    msgs = fetch_messages(sess, user_id, mailbox_id, emit, from_date=from_date, to_date=to_date)
    if not msgs:
        emit("No messages found", "warn")
        return []
    return process_emails(msgs, emit, fetch_body, source="webmail")

# ── Main scan dispatcher ──────────────────────────────────────────────────────
def scan_email(emit=None, quick=False, from_date=None, to_date=None):
    """
    Scan inbox for invoices.
    quick=True  → last 30 emails (fast, for auto-scan & refresh button)
    quick=False → last 500 emails (full scan)
    """
    def emitter(msg, t="info"):
        log.info(msg)
        if emit: emit(msg, t)

    if not scan_lock.acquire(blocking=False):
        emitter("Scan already running", "warn")
        return []
    # Temporarily override scan limit for quick scan
    global SCAN_LIMIT
    orig_limit = SCAN_LIMIT
    if quick:
        SCAN_LIMIT = QUICK_SCAN_LIMIT
        emitter(f"Quick scan: last {QUICK_SCAN_LIMIT} emails")
    try:
        emitter(f"Checking IMAP {IMAP_HOST}:{IMAP_PORT}...")
        try:
            with socket.create_connection((IMAP_HOST, IMAP_PORT), timeout=5):
                emitter("IMAP reachable", "ok")
                result = scan_imap(emitter, from_date=from_date, to_date=to_date)
                if result is not None:
                    return result
                # scan_imap returned None → fall through to webmail
        except Exception as ex:
            emitter(f"IMAP blocked ({type(ex).__name__}) — using HTTPS", "warn")
            return scan_webmail(emitter, from_date=from_date, to_date=to_date)
    finally:
        SCAN_LIMIT = orig_limit  # always restore, even on exception
        scan_lock.release()

# ── Notifications ─────────────────────────────────────────────────────────────
def win_notify(title, msg):
    if not WIN_NOTIFY: return
    try:
        from plyer import notification
        notification.notify(title=title, message=msg, app_name="PayCalendar", timeout=8)
    except Exception: pass  # silently skip on Linux/cloud

def check_notifications():
    today = date.today()
    for inv in load_invoices():
        if inv.get("status") == "paid" or not inv.get("due_date"):
            continue
        try:
            days = (date.fromisoformat(inv["due_date"]) - today).days
            amt  = f"{inv.get('amount',0)} {inv.get('currency','EUR')}"
            v    = inv.get("vendor","?")
            if days < 0:
                win_notify("⚠ Overdue", f"{v} — {amt} ({abs(days)}d overdue)")
            elif days <= WARN_DAYS:
                win_notify(f"💳 Due in {days}d", f"{v} — {amt}")
        except Exception:
            pass

def background_scanner():
    if AUTO_SCAN <= 0: return
    log.info(f"Auto-scan every {AUTO_SCAN}min")
    while True:
        time.sleep(AUTO_SCAN * 60)
        scan_email()
        check_notifications()

# ═══════════════════════════════════════════════════════════════════════════════
#  ROUTES
# ═══════════════════════════════════════════════════════════════════════════════


@app.route("/health")
def health():
    return "ok", 200

@app.route("/manifest.json")
def serve_manifest():
    return send_from_directory(BASE_DIR, "manifest.json",
                               mimetype="application/manifest+json")

@app.route("/icon-<size>.png")
def serve_icon(size):
    filename = f"icon-{size}.png"
    return send_from_directory(BASE_DIR, filename, mimetype="image/png")

@app.route("/")
def index():
    access_key = os.environ.get("ACCESS_KEY", "")
    if access_key:
        token = (request.cookies.get("pc_token","") or
                 request.headers.get("X-PC-Token","") or
                 request.args.get("_token",""))
        if token != access_key:
            return redirect("/login-page")
    return send_from_directory(TMPL_DIR, "index.html")

@app.route("/login-page")
def login_page():
    return """<!DOCTYPE html>
<html><head><meta charset="UTF-8"><title>PayCalendar</title>
<style>*{box-sizing:border-box}body{font-family:sans-serif;background:#0f1117;color:#e2e8f0;
display:flex;align-items:center;justify-content:center;min-height:100vh;margin:0}
.card{background:#1a1f2e;border:1px solid #2d3748;border-radius:16px;padding:40px;
max-width:360px;width:100%;text-align:center}h1{margin:0 0 8px;font-size:22px}
p{color:#718096;font-size:13px;margin:0 0 24px}
input{width:100%;padding:12px;border-radius:8px;border:1px solid #374151;
background:#111827;color:#e2e8f0;font-size:14px;outline:none;margin-bottom:12px}
button{width:100%;padding:12px;border-radius:9px;border:none;
background:linear-gradient(135deg,#2563eb,#7c3aed);color:#fff;
font-size:14px;font-weight:700;cursor:pointer}
</style></head><body>
<div class="card">
  <h1>📧 PayCalendar</h1>
  <p>Введи ключ доступа</p>
  <input type="password" id="k" placeholder="Ключ доступа" onkeydown="if(event.key==='Enter')login()">
  <button onclick="login()">Войти</button>
</div>
<script>
function login(){
  var k=document.getElementById('k').value;
  fetch('/api/auth-check',{method:'POST',headers:{'Content-Type':'application/json'},
    body:JSON.stringify({key:k})}).then(r=>r.json()).then(d=>{
    if(d.ok){document.cookie='pc_token='+k+';path=/;max-age=86400';location='/';}
    else alert('Неверный ключ');
  });
}
</script></body></html>"""

def _check_token():
    """Return True if request is authenticated."""
    access_key = os.environ.get("ACCESS_KEY", "")
    if not access_key:
        return True
    return (request.cookies.get("pc_token","") == access_key or
            request.headers.get("X-PC-Token","") == access_key or
            request.args.get("_token","") == access_key)

@app.before_request
def require_auth():
    """Block unauthenticated API calls."""
    skip = ("/api/auth-check", "/api/stats", "/health", "/api/test-scan", "/api/diagnose")
    if request.path.startswith("/api/") and request.path not in skip:
        if not _check_token():
            return jsonify({"error": "unauthorized"}), 401

@app.route("/api/auth-check", methods=["POST"])
def api_auth_check():
    access_key = os.environ.get("ACCESS_KEY", "")
    if not access_key:
        return jsonify({"ok": True})
    key = (request.json or {}).get("key", "")
    return jsonify({"ok": key == access_key})

@app.route("/setup")
def setup():
    return send_from_directory(TMPL_DIR, "setup.html")

@app.route("/api/config")
def api_config():
    reload_config()
    pw  = EMAIL_PASS or ""
    key = CLAUDE_KEY or ""
    provider = _active_provider()
    return jsonify({
        "company":          COMPANY,
        "email":            EMAIL_ADDR,
        "password":         pw[:2]+"***" if pw else "",
        "warn_days":        WARN_DAYS,
        "auto_scan_minutes":AUTO_SCAN,
        "has_password":     bool(pw and pw not in ("your_password_here","")),
        "has_api_key":      bool(key and "INSERT" not in key and len(key)>20),
        "imap_host":        IMAP_HOST or "mail.zone.ee",
        "ai_provider":      provider or "keyword",
        "has_gemini":       bool(GEMINI_KEY),
        "has_groq":         bool(GROQ_KEY),
    })

@app.route("/api/invoices", methods=["GET"])
def api_get_invoices():
    return jsonify(load_invoices())

@app.route("/api/invoices", methods=["POST"])
def api_save_invoices_route():
    data = request.json
    if not isinstance(data, list):
        return jsonify({"error":"Expected array"}), 400
    save_invoices(data)
    return jsonify({"ok":True,"count":len(data)})

@app.route("/api/invoices/<inv_id>", methods=["DELETE"])
def api_delete(inv_id):
    save_invoices([i for i in load_invoices() if i.get("id") != inv_id])
    return jsonify({"ok":True})

@app.route("/api/invoices/<inv_id>/paid", methods=["POST"])
def api_mark_paid(inv_id):
    save_invoices([
        {**i,"status":"paid","paid_at":datetime.now().isoformat()}
        if i.get("id") == inv_id else i
        for i in load_invoices()
    ])
    return jsonify({"ok":True})



@app.route("/debug-log")
def debug_log_view():
    level   = request.args.get("level", "all")   # all / info / error / warn
    lines_n = int(request.args.get("n", "500"))
    search  = request.args.get("q", "").lower()
    try:
        content = DEBUG_FILE.read_text(encoding="utf-8", errors="replace") if DEBUG_FILE.exists() else ""
        all_lines = content.split("\n")
        # Filter
        shown = []
        for line in all_lines:
            if level == "error" and "[ERROR" not in line: continue
            if level == "warn"  and "[WARN"  not in line and "[ERROR" not in line: continue
            if level == "info"  and "[DEBUG" in line: continue
            if search and search not in line.lower(): continue
            shown.append(line)
        shown = shown[-lines_n:]
        # Colorize
        parts = []
        for line in shown:
            col = "#9ca3af"
            if   "[ERROR"  in line: col = "#f87171"
            elif "[WARN"   in line: col = "#fbbf24"
            elif "[INFO"   in line: col = "#e2e8f0"
            elif "[DEBUG"  in line: col = "#6b7280"
            if "→ 200"     in line or "OK"   in line or "успешно" in line.lower(): col = "#34d399"
            if "→ 4" in line or "→ 5" in line: col = "#f87171"
            esc = line.replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")
            parts.append(f'<div class="l" style="color:{col}">{esc}</div>')
        body = "".join(parts) or '<div style="color:#6b7280">Лог пустой</div>'
        total = len(all_lines)
        sz    = f"{DEBUG_FILE.stat().st_size//1024} KB" if DEBUG_FILE.exists() else "0"
        return f"""<!DOCTYPE html>
<html><head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>PayCalendar — Лог</title>
<style>
*{{box-sizing:border-box;margin:0;padding:0}}
body{{background:#0f1117;font-family:'Courier New',monospace;font-size:12px;color:#9ca3af}}
.hdr{{background:#1a1f2e;border-bottom:1px solid #2d3748;padding:10px 14px;
      display:flex;flex-wrap:wrap;gap:8px;align-items:center;position:sticky;top:0;z-index:9}}
.hdr a,.hdr button{{color:#4b6bff;text-decoration:none;background:none;border:none;
               cursor:pointer;font-size:12px;padding:3px 8px;border-radius:5px;
               border:1px solid #374151}}
.hdr a:hover,.hdr button:hover{{background:#1e293b}}
.hdr a.active{{background:#2563eb;color:#fff;border-color:#2563eb}}
.meta{{color:#4b5563;font-size:11px;margin-left:auto}}
.search{{background:#111827;border:1px solid #374151;color:#e2e8f0;
         border-radius:5px;padding:4px 8px;font-size:12px;width:160px}}
.log{{padding:10px 14px;line-height:1.6}}
.l{{white-space:pre-wrap;word-break:break-all;padding:1px 0}}
.l:hover{{background:#1a1f2e}}
</style>
</head><body>
<div class="hdr">
  <a href="/">← Календарь</a>
  <a href="/debug-log?level=all&n={lines_n}" class="{'active' if level=='all' else ''}">Все</a>
  <a href="/debug-log?level=info&n={lines_n}" class="{'active' if level=='info' else ''}">INFO</a>
  <a href="/debug-log?level=warn&n={lines_n}" class="{'active' if level=='warn' else ''}">WARN</a>
  <a href="/debug-log?level=error&n={lines_n}" class="{'active' if level=='error' else ''}">ERROR</a>
  <input class="search" type="text" placeholder="Поиск..." value="{search}"
         onkeydown="if(event.key==='Enter')location='/debug-log?level={level}&q='+encodeURIComponent(this.value)">
  <a href="/debug-log?level={level}&n={lines_n}&q={search}" id="ref">🔄</a>
  <a href="/api/debug-clear" onclick="return confirm('Очистить лог?')">🗑</a>
  <span class="meta">{len(shown)} / {total} строк · {sz} · {str(DEBUG_FILE)}</span>
</div>
<div class="log">{body}</div>
<script>
window.scrollTo(0,document.body.scrollHeight);
// Auto-refresh every 5 sec if ?auto=1
if(location.search.includes('auto=1'))
  setTimeout(()=>location.reload(), 5000);
</script>
</body></html>"""
    except Exception as ex:
        return f"<pre style='color:#f87171'>Error: {ex}</pre>", 500

@app.route("/api/logs")
def api_logs_json():
    """JSON log endpoint for programmatic access."""
    n      = int(request.args.get("n", "100"))
    level  = request.args.get("level", "all")
    try:
        content = DEBUG_FILE.read_text(encoding="utf-8", errors="replace") if DEBUG_FILE.exists() else ""
        lines = content.split("\n")
        if level != "all":
            lines = [l for l in lines if f"[{level.upper()}" in l]
        return jsonify({"lines": lines[-n:], "total": len(lines),
                        "file": str(DEBUG_FILE), "exists": DEBUG_FILE.exists()})
    except Exception as ex:
        return jsonify({"error": str(ex)}), 500


@app.route("/api/debug-clear")
def api_debug_clear():
    try:
        (Path(__file__).parent / "debug_full.log").write_text("", encoding="utf-8")
        return "<script>window.location='/debug-log'</script>"
    except Exception as ex:
        return str(ex), 500


@app.route("/api/reset-scan", methods=["POST"])
def api_reset_scan():
    """Reset scan state - next scan will start from scratch."""
    try:
        empty = {"last_uid": None, "last_date": None, "scan_count": 0, "scanned_uids": []}
        save_state(empty)
        log.info("Scan state reset - will rescan from beginning")
        return jsonify({"ok": True, "message": "Состояние сброшено — следующий скан начнётся с начала"})
    except Exception as ex:
        return jsonify({"ok": False, "error": str(ex)})



@app.route("/api/test-scan")
def api_test_scan():
    """Fast diagnostic: config + DoH DNS + IMAP + Gemini."""
    import threading as _thr, imaplib as _il, ssl as _ssl2
    r = {"config": {}, "imap": {}, "gemini": {}, "emails": [], "candidates": []}

    # Config
    reload_config()
    r["config"] = {
        "email":    EMAIL_ADDR,
        "password": bool(EMAIL_PASS and len(EMAIL_PASS) > 3),
        "imap":     f"{IMAP_HOST}:{IMAP_PORT}",
        "provider": _active_provider() or "keyword-only",
        "gemini":   bool(GEMINI_KEY),
    }

    # IMAP in thread with 12s timeout
    imap_r = {}
    emails = []

    def _imap():
        try:
            ip = _resolve_host_doh(IMAP_HOST)
            imap_r["doh_ip"] = ip
            imap_r["doh_ok"] = ip != IMAP_HOST

            alt = [IMAP_HOST, "imap.zone.ee", "mail.zone.eu"]
            mail_conn, used_host = _try_imap_connect(alt, IMAP_PORT, EMAIL_ADDR, EMAIL_PASS)
            if not mail_conn:
                raise Exception("Все IMAP хосты недоступны")
            imap_r["connected_via"] = used_host
            mail = mail_conn
            mail.select("INBOX")
            _, data = mail.search(None, "ALL")
            ids = data[0].split()
            imap_r["total"] = len(ids)
            imap_r["ok"] = True

            for uid in reversed(ids[-15:]):
                try:
                    _, d = mail.fetch(uid, "(BODY[HEADER.FIELDS (SUBJECT FROM DATE)])")
                    if not d or not d[0]: continue
                    raw = d[0][1].decode("utf-8", errors="replace")
                    subj = frm = dt = ""
                    for line in raw.split("\n"):
                        ll = line.lower()
                        if ll.startswith("subject:"): subj = line[8:].strip()[:80]
                        elif ll.startswith("from:"):   frm  = line[5:].strip()[:60]
                        elif ll.startswith("date:"):   dt   = line[5:].strip()[:25]
                    score = is_invoice_by_keywords(subj, "", frm, False)
                    emails.append({"uid": uid.decode(), "subject": subj,
                                   "from": frm, "date": dt,
                                   "score": score, "invoice": score >= 50})
                except Exception:
                    pass
            mail.logout()
        except Exception as ex:
            imap_r["ok"] = False
            imap_r["error"] = str(ex)[:150]

    t = _thr.Thread(target=_imap, daemon=True)
    t.start(); t.join(timeout=12)

    r["imap"]       = imap_r if imap_r else {"ok": False, "error": "timeout"}
    r["emails"]     = emails
    r["candidates"] = [e for e in emails if e["invoice"]]

    # Gemini quick test
    try:
        gr = requests.post(
            f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key={GEMINI_KEY}",
            json={"contents": [{"parts": [{"text": "Reply OK"}]}],
                  "generationConfig": {"maxOutputTokens": 20, "temperature": 0}},
            timeout=10
        )
        r["gemini"] = {"status": gr.status_code, "ok": gr.status_code == 200}
        if gr.status_code == 200:
            pts = gr.json().get("candidates",[{}])[0].get("content",{}).get("parts",[])
            r["gemini"]["reply"] = "".join(p.get("text","") for p in pts)[:50]
        else:
            r["gemini"]["error"] = gr.text[:100]
    except Exception as ex:
        r["gemini"] = {"ok": False, "error": str(ex)[:100]}

    r["summary"] = {
        "emails_checked": len(emails),
        "invoices_found": len(r["candidates"]),
        "imap_ok": r["imap"].get("ok", False),
        "gemini_ok": r["gemini"].get("ok", False),
    }
    return jsonify(r)



@app.route("/api/diagnose")
def api_diagnose():
    """Diagnostic endpoint - shows system status."""
    import imaplib as _il, socket as _so
    results = {}

    # Config
    reload_config()
    results["config"] = {
        "email":        EMAIL_ADDR,
        "has_password": bool(EMAIL_PASS and len(EMAIL_PASS) > 3),
        "imap_host":    IMAP_HOST,
        "imap_port":    IMAP_PORT,
        "has_api_key":  bool(API_KEY and len(API_KEY) > 20),
        "api_key_prefix": API_KEY[:15] + "..." if API_KEY else "",
        "data_dir":     str(_DATA_DIR),
        "invoices":     len(load_invoices()),
        "scan_count":   load_state().get("scan_count", 0),
        "last_date":    load_state().get("last_date"),
    }

    # IMAP test
    try:
        m = _il.IMAP4_SSL(IMAP_HOST, IMAP_PORT)
        m.login(EMAIL_ADDR, EMAIL_PASS)
        m.select("INBOX")
        _, data = m.search(None, "ALL")
        total = len(data[0].split()) if data[0] else 0
        m.logout()
        results["imap"] = {"ok": True, "total_emails": total}
    except Exception as ex:
        results["imap"] = {"ok": False, "error": str(ex)}

    # Claude API test
    try:
        r = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={"Content-Type": "application/json",
                     "x-api-key": API_KEY,
                     "anthropic-version": "2023-06-01"},
            json={"model": MODEL, "max_tokens": 10,
                  "messages": [{"role": "user", "content": "Hi"}]},
            timeout=10,
        )
        results["claude"] = {"ok": r.status_code == 200, "status": r.status_code}
        if r.status_code != 200:
            results["claude"]["error"] = r.text[:200]
    except Exception as ex:
        results["claude"] = {"ok": False, "error": str(ex)}

    return jsonify(results)

@app.route("/api/scan-state")
def api_scan_state():
    """Return current scan state for UI."""
    st   = load_state()
    invs = load_invoices()
    ld   = st.get("last_date")
    sc   = st.get("scan_count", 0)
    return jsonify({
        "scan_count":         sc,
        "last_date":          ld,
        "last_uid":           st.get("last_uid"),
        "scanned_uids_count": len(st.get("scanned_uids", [])),
        "next_limit":         get_scan_limit(),
        "total_invoices":     len(invs),
        "next_scan_from":     ld if ld else ("начало (первый скан)" if sc == 0 else "все письма"),
        "data_persistent":    str(_DATA_DIR) != str(BASE_DIR),
        "data_dir":           str(_DATA_DIR),
    })

@app.route("/api/stats")
def api_stats():
    invs  = load_invoices()
    today = date.today()
    pend  = [i for i in invs if i.get("status") != "paid"]
    over  = [i for i in pend if i.get("due_date") and date.fromisoformat(i["due_date"]) < today]
    urg   = [i for i in pend if i.get("due_date") and
             0 <= (date.fromisoformat(i["due_date"]) - today).days <= WARN_DAYS]
    return jsonify({
        "total":len(invs),"pending":len(pend),"overdue":len(over),"urgent":len(urg),
        "sum_pending":round(sum(float(i.get("amount",0)) for i in pend),2),
        "sum_paid":round(sum(float(i.get("amount",0)) for i in invs if i.get("status")=="paid"),2),
    })



@app.route("/api/scan-month/stream")
def api_scan_month_stream():
    """Scan emails for a specific date range. ?from=YYYY-MM-DD&to=YYYY-MM-DD"""
    from_date = request.args.get("from", "") or request.args.get("from_date", "")
    to_date   = request.args.get("to",   "") or request.args.get("to_date",   "")
    # Only recalculate dates if not explicitly provided
    if not from_date or not to_date:
        month_str = request.args.get("month", "")
        try:
            if month_str:
                year, mon = int(month_str[:4]), int(month_str[5:7])
            else:
                year, mon = date.today().year, date.today().month
            from_date = f"{year:04d}-{mon:02d}-01"
            to_date   = f"{year:04d}-{mon:02d}-{__import__('calendar').monthrange(year, mon)[1]:02d}"
        except Exception:
            from_date = to_date = None
    log.info(f"scan-month: from={from_date} to={to_date}")

    import queue as _q, threading as _th
    q = _q.Queue()
    sentinel = object()
    _cnt = [0]

    def emit(msg, t="info"):
        q.put((msg, t))

    def run():
        result = scan_email(emit, from_date=from_date, to_date=to_date)
        _cnt[0] = len(result) if isinstance(result, list) else 0
        q.put(sentinel)

    _th.Thread(target=run, daemon=True).start()

    def generate():
        while True:
            try:
                item = q.get(timeout=60)
            except Exception:
                yield f"data: {json.dumps({'done': True, 'count': _cnt[0]})}\n\n"
                return
            if item is sentinel:
                yield f"data: {json.dumps({'done': True, 'count': _cnt[0]})}\n\n"
                return
            msg, t = item
            if t == "progress" and msg.startswith("__progress__"):
                parts = msg.split()
                try:
                    cur,tot,spd,eta = int(parts[1]),int(parts[2]),float(parts[3]),int(parts[4])
                    pct = int(cur/tot*100) if tot>0 else 0
                    ts  = datetime.now().strftime("%H:%M:%S")
                    yield f"data: {json.dumps({'type':'progress','current':cur,'total':tot,'pct':pct,'speed':spd,'eta':eta,'ts':ts})}\n\n"
                except Exception:
                    pass
                continue
            ts = datetime.now().strftime("%H:%M:%S")
            yield f"data: {json.dumps({'msg': msg, 'type': t, 'ts': ts})}\n\n"

    return Response(stream_with_context(generate()),
                    content_type="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@app.route("/api/scan/stream")
def api_scan_stream():
    return _make_scan_stream(quick=False)

@app.route("/api/refresh/stream")
def api_refresh_stream():
    return _make_scan_stream(quick=True)

def _make_scan_stream(quick=False):
    import queue as _q
    q = _q.Queue()
    sentinel = object()

    _cnt = [0]

    def emit(msg, t="info"):
        q.put((msg, t))

    def run():
        new_invs = scan_email(emit, quick=quick)
        _cnt[0] = len(new_invs) if isinstance(new_invs, list) else 0
        q.put(sentinel)

    import threading as _th
    _th.Thread(target=run, daemon=True).start()

    def generate():
        result_count = 0
        while True:
            try:
                item = q.get(timeout=30)
            except Exception:
                yield f"data: {json.dumps({'done': True, 'count': 0})}\n\n"
                return

            if item is sentinel:
                yield f"data: {json.dumps({'done': True, 'count': _cnt[0]})}\n\n"
                return

            msg, t = item

            # Progress event
            if t == "progress" and msg.startswith("__progress__"):
                parts = msg.split()
                try:
                    cur = int(parts[1]); tot = int(parts[2])
                    spd = float(parts[3]); eta = int(parts[4])
                    pct = int(cur / tot * 100) if tot > 0 else 0
                    ts  = datetime.now().strftime("%H:%M:%S")
                    yield f"data: {json.dumps({'type':'progress','current':cur,'total':tot,'pct':pct,'speed':spd,'eta':eta,'ts':ts})}\n\n"
                except Exception:
                    pass
                continue

            # Count found invoices
            if t == "ok" and "новых инвойс" in msg.lower() or (t == "ok" and "Added" in msg):
                try:
                    result_count = int(''.join(filter(str.isdigit, msg.split()[1])))
                except Exception:
                    pass

            ts = datetime.now().strftime("%H:%M:%S")
            yield f"data: {json.dumps({'msg': msg, 'type': t, 'ts': ts})}\n\n"

    return Response(
        stream_with_context(generate()),
        content_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"}
    )


@app.route("/api/open-webmail")
def api_open_webmail():
    webbrowser.open("https://webmail.ee")
    return jsonify({"ok":True})



# ── Auto-capture state ────────────────────────────────────────────────────────
_capture_state = {"status": "idle", "message": "", "success": False}
_capture_thread = None



# ── Chrome CDP cookie capture ─────────────────────────────────────────────────
CHROME_DEBUG_PORT = 9222

def _get_chrome_cookies():
    """Read webmail.ee cookies from Chrome via DevTools Protocol."""
    import urllib.request as _ur
    import json as _j

    # 1. Get list of targets
    try:
        resp = _ur.urlopen(f"http://localhost:{CHROME_DEBUG_PORT}/json", timeout=3)
        targets = _j.loads(resp.read())
    except Exception as ex:
        log.debug(f"CDP /json failed: {ex}")
        return None, str(ex)

    # Find webmail.ee tab
    wm_target = None
    for t in targets:
        url = t.get("url", "")
        if "webmail.ee" in url and t.get("type") == "page":
            wm_target = t
            break

    if not wm_target:
        urls = [t.get("url","")[:60] for t in targets[:5]]
        log.debug(f"No webmail.ee tab found. Open tabs: {urls}")
        return None, f"Вкладка webmail.ee не найдена. Открытые вкладки: {urls}"

    log.debug(f"Found webmail tab: {wm_target.get('url','')[:80]}")

    # 2. Connect to tab via WebSocket and get cookies
    ws_url = wm_target.get("webSocketDebuggerUrl")
    if not ws_url:
        return None, "WebSocket URL not available"

    try:
        import websocket as _ws
    except ImportError:
        # Try without websocket lib using CDP HTTP endpoint
        try:
            devtools_url = f"http://localhost:{CHROME_DEBUG_PORT}/json"
            # Use Network.getCookies via CDP HTTP if available
            pass
        except Exception:
            pass
        return None, "websocket-client not installed"

    try:
        ws = _ws.create_connection(
            ws_url, timeout=5,
            origin=f"http://localhost:{CHROME_DEBUG_PORT}",
            host=f"localhost:{CHROME_DEBUG_PORT}",
        )
        ws.send(_j.dumps({"id":1,"method":"Network.getAllCookies","params":{}}))
        result = _j.loads(ws.recv())
        ws.close()

        cookies = result.get("result",{}).get("cookies",[])
        wm_cookies = [c for c in cookies
                      if "webmail.ee" in c.get("domain","")
                      or c.get("domain","").endswith("webmail.ee")]

        if not wm_cookies:
            return None, f"Нет куков webmail.ee (всего {len(cookies)} куков)"

        cookie_str = "; ".join(f"{c['name']}={c['value']}" for c in wm_cookies)
        log.info(f"CDP: got {len(wm_cookies)} webmail.ee cookies")
        return cookie_str, None

    except Exception as ex:
        log.error(f"CDP WebSocket error: {ex}")
        return None, str(ex)

@app.route("/api/chrome-grab", methods=["GET","POST"])
def api_chrome_grab():
    """Grab cookies from Chrome via CDP. Chrome must be running with --remote-debugging-port=9222"""
    cookie_str, err = _get_chrome_cookies()
    if err:
        return jsonify({"ok": False, "error": err})
    try:
        save_config_value("email", "webmail_cookie", cookie_str)
        _capture_state.update({"status":"done",
                               "message":f"Grabbed from Chrome: {len(cookie_str)} bytes",
                               "success":True})
        log.info(f"Chrome cookies saved: {len(cookie_str)} chars")
        return jsonify({"ok": True, "length": len(cookie_str)})
    except Exception as ex:
        return jsonify({"ok": False, "error": str(ex)})

@app.route("/api/chrome-status")
def api_chrome_status():
    """Check if Chrome is running with debug port."""
    import socket as _s
    try:
        with _s.create_connection(("localhost", CHROME_DEBUG_PORT), timeout=2):
            # Chrome is running - try to get tabs
            import urllib.request as _ur, json as _j
            tabs = _j.loads(_ur.urlopen(
                f"http://localhost:{CHROME_DEBUG_PORT}/json", timeout=2).read())
            wm  = any("webmail.ee" in t.get("url","") for t in tabs)
            return jsonify({"running": True, "tabs": len(tabs),
                           "webmail_open": wm})
    except Exception:
        return jsonify({"running": False})


# ── Public access (Cloudflare Tunnel) ────────────────────────────────────────
_public_url = os.environ.get("PAYCALENDAR_PUBLIC_URL", "")

@app.route("/public-info")
def public_info():
    """Page shown after tunnel starts - displays URL and QR code."""
    url = request.args.get("url", _public_url or f"http://localhost:{APP_PORT}")
    qr_api = f"https://api.qrserver.com/v1/create-qr-code/?size=200x200&data={url}"
    return f"""<!DOCTYPE html>
<html><head><meta charset="UTF-8">
<title>PayCalendar - Public URL</title>
<meta name="viewport" content="width=device-width,initial-scale=1">
<style>
body{{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;
     background:#0f1117;color:#e2e8f0;min-height:100vh;
     display:flex;align-items:center;justify-content:center;padding:20px}}
.card{{background:#1a1f2e;border:1px solid #2d3748;border-radius:16px;
      padding:32px;max-width:480px;width:100%;text-align:center}}
h1{{font-size:20px;margin-bottom:8px;color:#f7fafc}}
.sub{{color:#718096;font-size:13px;margin-bottom:24px}}
.url-box{{background:#111827;border:1px solid #374151;border-radius:10px;
          padding:14px;margin:16px 0;word-break:break-all;
          font-family:monospace;font-size:13px;color:#34d399;cursor:pointer}}
.url-box:hover{{border-color:#2563eb}}
.qr{{margin:16px auto;border-radius:10px;overflow:hidden;width:200px;height:200px;
     background:#fff;display:flex;align-items:center;justify-content:center}}
.qr img{{width:200px;height:200px}}
.steps{{text-align:left;background:#111827;border-radius:10px;padding:14px;margin-top:16px}}
.step{{display:flex;gap:10px;align-items:flex-start;margin-bottom:10px;font-size:13px;color:#9ca3af}}
.sn{{background:#2563eb;color:#fff;width:22px;height:22px;border-radius:50%;
     display:flex;align-items:center;justify-content:center;font-size:11px;
     font-weight:700;flex-shrink:0}}
.btn{{display:block;width:100%;padding:13px;border-radius:9px;border:none;
      background:linear-gradient(135deg,#2563eb,#7c3aed);color:#fff;
      font-size:14px;font-weight:700;cursor:pointer;margin-top:16px;
      text-decoration:none;text-align:center}}
.back{{color:#4b6bff;font-size:12px;display:block;margin-top:14px;text-decoration:none}}
</style></head><body>
<div class="card">
  <h1>📱 PayCalendar</h1>
  <div class="sub">Доступен в интернете — открой на телефоне</div>
  
  <div class="qr">
    <img src="{qr_api}" alt="QR код" onerror="this.parentElement.innerHTML='<div style=&quot;color:#374151&quot;>QR недоступен</div>'">
  </div>
  
  <div class="sub" style="margin:8px 0 4px">Отсканируй QR или скопируй ссылку:</div>
  <div class="url-box" onclick="navigator.clipboard.writeText('{url}').then(()=>this.style.borderColor='#059669')">{url}</div>
  
  <div class="steps">
    <div class="step"><div class="sn">1</div><span>Отсканируй QR-код камерой телефона</span></div>
    <div class="step"><div class="sn">2</div><span>Или скопируй ссылку и открой в браузере на Android</span></div>
    <div class="step"><div class="sn">3</div><span>Добавь в закладки — работает пока открыт run_public.bat</span></div>
  </div>
  
  <a href="/" class="btn">Открыть PayCalendar →</a>
  <a href="/" class="back">← Вернуться в календарь</a>
</div>
<script>
if(/Android|iPhone/i.test(navigator.userAgent)){{
  var el=document.querySelector('.url-box');
  if(el) el.style.fontSize='16px';
}}
</script>
</body></html>"""

@app.route("/api/public-url")
def api_public_url():
    """Return current public tunnel URL if available."""
    # Try to read from tunnel log
    tunnel_log = Path(__file__).parent / "tunnel.log"
    url = _public_url
    if not url and tunnel_log.exists():
        try:
            import re as _re
            content = tunnel_log.read_text(encoding="utf-8", errors="ignore")
            m = _re.search(r'https://[a-z0-9-]+\.trycloudflare\.com', content)
            if m:
                url = m.group(0)
        except Exception:
            pass
    return jsonify({"url": url, "active": bool(url)})

@app.route("/login-helper")
def login_helper():
    """Relay page: opens webmail.ee, captures cookies after login."""
    return send_from_directory(TMPL_DIR, "login_helper.html")


@app.route("/api/playwright-login", methods=["POST"])
def api_playwright_login():
    """Run Playwright auto-login in background thread."""
    logs = []
    def emit(msg, t="info"):
        log.info(msg)
        logs.append({"msg": msg, "type": t})

    # Run in background so we can stream logs
    import threading as _th
    result = [None]
    done   = [False]

    def run():
        result[0] = playwright_login(emit)
        done[0]   = True

    t = _th.Thread(target=run, daemon=True)
    t.start()
    t.join(timeout=60)

    ok = bool(result[0])
    return jsonify({"ok": ok, "logs": logs,
                    "error": "" if ok else "Login failed — check logs"})

@app.route("/api/playwright-login/stream")
def api_playwright_login_stream():
    """SSE stream for Playwright login with live log."""
    import queue as _q
    q = _q.Queue()
    sentinel = object()

    def emit(msg, t="info"):
        q.put((msg, t))

    def run():
        playwright_login(emit)
        q.put(sentinel)

    import threading as _th
    _th.Thread(target=run, daemon=True).start()

    def generate():
        while True:
            try:
                item = q.get(timeout=90)
            except Exception:
                yield f"data: {json.dumps({'done': True, 'ok': False})}\n\n"
                return
            if item is sentinel:
                ok = _capture_state.get("success", False)
                yield f"data: {json.dumps({'done': True, 'ok': ok})}\n\n"
                return
            msg, t = item
            ts = datetime.now().strftime("%H:%M:%S")
            yield f"data: {json.dumps({'msg': msg, 'type': t, 'ts': ts})}\n\n"

    return Response(stream_with_context(generate()),
                    content_type="text/event-stream",
                    headers={"Cache-Control": "no-cache",
                             "X-Accel-Buffering": "no"})


@app.route("/api/test-connection", methods=["POST"])
def api_test_connection():
    """Save credentials and test if we can reach mail server."""
    data    = request.json or {}
    email   = data.get("email", EMAIL_ADDR)
    password= data.get("password", EMAIL_PASS)

    # Save to config immediately
    save_config_value("email", "address",  email)
    save_config_value("email", "password", password)

    # Reload globals
    reload_config()

    # Quick IMAP test
    import imaplib as _il, socket as _so
    try:
        m = _il.IMAP4_SSL(IMAP_HOST, IMAP_PORT)
        m.login(email, password)
        m.logout()
        log.info(f"IMAP test OK for {email}")
        return jsonify({"ok": True, "message": "IMAP подключение успешно!", "stream": False})
    except _il.IMAP4.error as e:
        log.warning(f"IMAP login failed: {e}")
        return jsonify({"ok": False, "error": f"Ошибка входа: {e}", "stream": False})
    except (_so.gaierror, OSError) as e:
        log.info(f"IMAP unreachable ({e}) — will use HTTPS webmail")
        # IMAP blocked but credentials saved — webmail will work
        return jsonify({"ok": True,
                        "message": "Данные сохранены. IMAP недоступен — будет использован webmail HTTPS.",
                        "stream": False})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e), "stream": False})

@app.route("/api/capture-start", methods=["POST"])
def api_capture_start():
    """Start background login attempt loop - runs while user logs in via browser."""
    global _capture_thread, _capture_state
    data = request.json or {}
    email_v = data.get("email", EMAIL_ADDR).strip()
    pwd_v   = data.get("password", EMAIL_PASS).strip()

    _capture_state = {"status": "running", "message": "Waiting for login...", "success": False}

    def _try_capture():
        import urllib.parse as _up
        BASE = "https://webmail.ee"
        attempts = 0
        max_attempts = 30  # 30 × 3s = 90 seconds

        while attempts < max_attempts and _capture_state["status"] == "running":
            attempts += 1
            try:
                sess = _make_session_with_proxy()
                sess.headers.update({
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                    "Origin": BASE, "Referer": BASE + "/",
                })
                # Get XSRF
                r0 = sess.get(BASE + "/", timeout=8)
                xsrf = _up.unquote(sess.cookies.get("XSRF-TOKEN", ""))
                if xsrf:
                    sess.headers["X-XSRF-TOKEN"] = xsrf

                login_attempts = [
                    (BASE + "/auth/accounts", {"login": email_v,               "password": pwd_v}),
                    (BASE + "/auth/accounts", {"login": email_v.split("@")[0], "password": pwd_v}),
                    (BASE + "/auth/accounts", {"email": email_v,               "password": pwd_v}),
                    (BASE + "/login",         {"login": email_v,               "password": pwd_v}),
                ]
                for url, payload in login_attempts:
                    for ct in ["json", "form"]:
                        try:
                            kw = {"json": payload} if ct == "json" else {"data": payload}
                            hdrs = {
                                "Content-Type": "application/json" if ct == "json"
                                                else "application/x-www-form-urlencoded",
                                "X-Requested-With": "XMLHttpRequest",
                            }
                            ra = sess.post(url, timeout=8, headers=hdrs, **kw)
                            has_ck = any(n in sess.cookies
                                        for n in ["ccd", "token", "laravel_session"])
                            has_tok = False
                            try:
                                d = ra.json()
                                has_tok = bool(d.get("token") or d.get("id") or d.get("success"))
                            except Exception:
                                pass

                            if has_ck or has_tok:
                                # Save cookies
                                cookie_str = "; ".join(f"{c.name}={c.value}"
                                                       for c in sess.cookies)
                                new_xsrf = _up.unquote(sess.cookies.get("XSRF-TOKEN",""))
                                save_config_value("email", "webmail_cookie", cookie_str)
                                if new_xsrf:
                                    save_config_value("email", "webmail_xsrf_token", new_xsrf)
                                _capture_state.update({
                                    "status": "done",
                                    "message": f"Login successful! {len(cookie_str)} bytes saved.",
                                    "success": True,
                                })
                                log.info(f"Auto-capture: cookies saved ({len(cookie_str)} chars)")
                                return
                        except Exception:
                            pass

                _capture_state["message"] = f"Attempt {attempts}/{max_attempts}..."
                time.sleep(3)
            except Exception as ex:
                _capture_state["message"] = f"Error: {ex}"
                time.sleep(3)

        if _capture_state["status"] == "running":
            _capture_state.update({
                "status": "timeout",
                "message": "Timeout. Use bookmarklet or console method.",
                "success": False,
            })

    _capture_thread = threading.Thread(target=_try_capture, daemon=True)
    _capture_thread.start()
    return jsonify({"ok": True})

@app.route("/api/capture-status")
def api_capture_status():
    return jsonify(_capture_state)

@app.route("/api/capture-stop", methods=["POST"])
def api_capture_stop():
    _capture_state["status"] = "idle"
    return jsonify({"ok": True})


@app.route("/api/import-cookies", methods=["POST"])
def api_import_cookies():
    """
    Import cookies from Cookie Editor extension (JSON format).
    Accepts: [{"name":"..","value":"..","domain":"..","path":"..","secure":..}, ...]
    """
    import urllib.parse as _up
    data = request.json or {}
    cookies_json = data.get("cookies")

    if not cookies_json:
        return jsonify({"ok": False, "error": "No cookies data"})

    try:
        if isinstance(cookies_json, str):
            cookies_json = json.loads(cookies_json)

        if not isinstance(cookies_json, list):
            return jsonify({"ok": False, "error": "Expected JSON array"})

        # Build cookie string from JSON array
        parts = []
        xsrf_val = ""
        for ck in cookies_json:
            name  = ck.get("name", "").strip()
            value = ck.get("value", "").strip()
            if name and value:
                parts.append(f"{name}={value}")
                if name.upper() in ("XSRF-TOKEN", "XSRF_TOKEN"):
                    xsrf_val = _up.unquote(value)

        if not parts:
            return jsonify({"ok": False, "error": "No valid cookies in JSON"})

        cookie_str = "; ".join(parts)
        save_config_value("email", "webmail_cookie", cookie_str)
        if xsrf_val:
            save_config_value("email", "webmail_xsrf_token", xsrf_val)

        log.info(f"Imported {len(cookies_json)} cookies from Cookie Editor ({len(cookie_str)} chars)")
        _capture_state.update({
            "status":  "done",
            "message": f"Imported {len(cookies_json)} cookies from Cookie Editor",
            "success": True,
        })
        return jsonify({
            "ok": True,
            "count": len(cookies_json),
            "length": len(cookie_str),
            "has_xsrf": bool(xsrf_val),
        })
    except Exception as ex:
        log.error(f"import-cookies error: {ex}")
        return jsonify({"ok": False, "error": str(ex)})


@app.route("/api/auth-status")
def api_auth_status():
    """Return current auth state for the setup page."""
    if WEBMAIL_COOKIE:
        expiry = _get_token_expiry()
        if expiry:
            from datetime import timezone
            days = (expiry - datetime.now(timezone.utc)).total_seconds() / 86400
            if days > 1:
                return jsonify({
                    "status": "ok",
                    "message": f"✅ Подключено: {EMAIL_ADDR} (куки до {expiry.strftime('%d.%m.%Y')})",
                })
            else:
                return jsonify({
                    "status": "warn",
                    "message": f"⚠ Куки истекают через {days:.0f}ч — обновляю...",
                })
        return jsonify({
            "status": "ok",
            "message": f"✅ Куки сохранены: {EMAIL_ADDR}",
        })
    if EMAIL_ADDR and EMAIL_PASS:
        return jsonify({
            "status": "warn",
            "message": f"⚠ Есть логин/пароль, но нет куков — нажми «Подключить»",
        })
    return jsonify({
        "status": "err",
        "message": "Не настроено — введи email и пароль",
    })

@app.route("/api/save-config", methods=["POST"])
def api_save_config():
    """Save any config value to config.ini and reload globals."""
    data    = request.json or {}
    section = data.get("section", "email")
    key     = data.get("key", "")
    value   = data.get("value", "")
    if not key:
        return jsonify({"ok": False, "error": "No key"})
    try:
        save_config_value(section, key, value)
        # Also update runtime globals for known keys
        global EMAIL_ADDR, EMAIL_PASS, IMAP_HOST, IMAP_PORT
        if section == "email":
            if key == "address":  EMAIL_ADDR = value
            if key == "password": EMAIL_PASS = value
            if key == "imap_host": IMAP_HOST = value
            if key == "imap_port": IMAP_PORT = int(value)
        log.info(f"Config saved: [{section}] {key}")
        return jsonify({"ok": True})
    except Exception as ex:
        return jsonify({"ok": False, "error": str(ex)})


@app.route("/api/clear-cookies", methods=["POST"])
def api_clear_cookies():
    """Clear saved webmail cookies."""
    save_config_value("email", "webmail_cookie", "")
    save_config_value("email", "webmail_xsrf_token", "")
    reload_config()
    log.info("Webmail cookies cleared")
    return jsonify({"ok": True, "message": "Куки очищены"})

@app.route("/api/save-cookies", methods=["POST"])
def api_save_cookies():
    cookie = (request.json or {}).get("cookie","").strip()
    if not cookie:
        return jsonify({"ok":False,"error":"No cookie"})
    # Reject if it looks like JS code instead of real cookies
    if "fetch(" in cookie or "function(" in cookie or "document.cookie" in cookie:
        return jsonify({"ok":False,"error":"Ошибка: в поле куки попал JS код. Нужно сначала открыть webmail.ee, потом выполнить команду в консоли WEBMAIL (F12), не в PayCalendar."})
    # Strip non-latin-1 chars (emojis etc) that break HTTP headers
    safe = cookie.encode("latin-1", errors="ignore").decode("latin-1")
    save_config_value("email", "webmail_cookie", safe)
    _capture_state.update({"status":"done","message":f"Cookies saved: {len(safe)} bytes","success":True})
    log.info(f"Cookies saved: {len(safe)} chars")
    return jsonify({"ok":True,"length":len(safe)})
    try:
        save_config_value("email","webmail_cookie",cookie)
        _capture_state.update({"status":"done","message":f"{len(cookie)} bytes captured","success":True})
        return jsonify({"ok":True,"length":len(cookie)})
    except Exception as ex:
        return jsonify({"ok":False,"error":str(ex)})

@app.route("/api/webmail-login", methods=["POST"])
def api_webmail_login():
    data    = request.json or {}
    email_v = data.get("email", EMAIL_ADDR).strip()
    pwd_v   = data.get("password", EMAIL_PASS).strip()
    logs    = []

    def L(msg, t="info"):
        log.info(msg)
        logs.append({"msg":msg,"type":t})

    BASE = "https://webmail.ee"
    sess = _make_session_with_proxy()
    sess.headers.update({
        "User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120",
        "Origin":BASE,"Referer":BASE+"/",
    })
    L(f"Connecting as {email_v}...")
    try:
        r0 = sess.get(BASE+"/", timeout=12)
        L(f"  GET / → {r0.status_code}")
        xsrf = urlparse.unquote(sess.cookies.get("XSRF-TOKEN",""))
        if xsrf:
            sess.headers["X-XSRF-TOKEN"] = xsrf
    except Exception as ex:
        L(f"Cannot reach webmail: {ex}", "err")
        return jsonify({"ok":False,"logs":logs,"error":str(ex)})

    def Lemit(msg, t="info"):
        L(msg, t)

    if not webmail_login(sess, email_v, pwd_v, Lemit):
        L("Login failed. Use browser method.", "warn")
        return jsonify({"ok":False,"logs":logs,"error":"Login failed"})

    cookie_str = "; ".join(f"{c.name}={c.value}" for c in sess.cookies)
    new_xsrf   = urlparse.unquote(sess.cookies.get("XSRF-TOKEN",""))
    L(f"Saving {len(cookie_str)} bytes...", "ok")
    try:
        raw = configparser.RawConfigParser()
        raw.read(CONFIG_FILE, encoding="utf-8")
        if not raw.has_section("email"): raw.add_section("email")
        raw.set("email","webmail_cookie",cookie_str)
        if new_xsrf: raw.set("email","webmail_xsrf_token",new_xsrf)
        with open(CONFIG_FILE,"w",encoding="utf-8") as f: raw.write(f)
        reload_config()
        L("Saved!", "ok")
    except Exception as ex:
        return jsonify({"ok":False,"logs":logs,"error":str(ex)})

    return jsonify({"ok":True,"logs":logs})

# ═══════════════════════════════════════════════════════════════════════════════
#  STARTUP
# ═══════════════════════════════════════════════════════════════════════════════
# Start background services when imported by gunicorn too
import threading as _bg_th
_bg_started = False
def _start_background():
    global _bg_started
    if _bg_started:
        return
    _bg_started = True
    _bg_th.Thread(target=auth_watchdog, daemon=True).start()
    _bg_th.Thread(target=background_scanner, daemon=True).start()
    check_notifications()
    log.info("Background services started")

# Auto-start when imported (gunicorn)
if os.environ.get("RAILWAY_ENVIRONMENT") or os.environ.get("RENDER"):
    # Auto-clear bad cookies on startup
    _ck = c("email", "webmail_cookie", "")
    if _ck and ("fetch(" in _ck or "document.cookie" in _ck or '✅' in _ck):
        log.error("STARTUP: bad cookie detected, clearing automatically")
        save_config_value("email", "webmail_cookie", "")
        reload_config()
    _start_background()

if __name__ == "__main__":
    print("="*60, flush=True)
    print(f"  PayCalendar — {COMPANY}", flush=True)
    print(f"  Email:  {EMAIL_ADDR}", flush=True)
    print(f"  Server: http://localhost:{APP_PORT}", flush=True)
    print("="*60, flush=True)

    if not API_KEY or "INSERT" in API_KEY:
        print("  [WARN] Claude API key not set in config.ini!", flush=True)

    # Kill old process if port busy
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        if s.connect_ex(("127.0.0.1", APP_PORT)) == 0:
            print(f"  Port {APP_PORT} busy — killing old process...", flush=True)
            try:
                result = subprocess.run(["netstat","-ano"], capture_output=True, text=True)
                for line in result.stdout.splitlines():
                    if f":{APP_PORT}" in line and "LISTENING" in line:
                        pid = line.split()[-1]
                        if pid.isdigit():
                            subprocess.run(["taskkill","/F","/PID",pid], capture_output=True)
                            print(f"  Killed PID {pid}", flush=True)
                            time.sleep(0.8)
            except Exception as ex:
                print(f"  Could not kill: {ex}", flush=True)

    if not DATA_FILE.exists() and WEBMAIL_COOKIE:
        print("  First run — scan in 5s...", flush=True)
        threading.Timer(5.0, scan_email).start()
    elif not DATA_FILE.exists():
        print("  First run — enter email credentials to start scanning.", flush=True)

    threading.Thread(target=background_scanner, daemon=True).start()
    threading.Thread(target=auth_watchdog, daemon=True).start()
    # Initial auth check
    if WEBMAIL_COOKIE:
        log.info("Startup: checking session validity...")
        threading.Timer(3.0, ensure_auth).start()
    check_notifications()

    url = f"http://localhost:{APP_PORT}"
    # Always open browser after server starts
    is_cloud = bool(os.environ.get("RAILWAY_ENVIRONMENT") or
                    os.environ.get("RENDER") or
                    os.environ.get("CLOUD_DEPLOY"))
    if AUTO_BROWSER and not is_cloud:
        def _open_browser():
            import time as _t
            _t.sleep(1.5)
            # Try to open in existing Chrome via CDP
            try:
                import urllib.request as _ur, json as _j
                tabs = _j.loads(_ur.urlopen("http://localhost:9222/json", timeout=2).read())
                # Navigate existing blank/about tab to our app
                for tab in tabs:
                    if tab.get("type") == "page" and (
                        "about:" in tab.get("url","") or
                        "localhost:5050" in tab.get("url","")
                    ):
                        ws_url = tab.get("webSocketDebuggerUrl","")
                        if ws_url:
                            import websocket as _ws
                            w = _ws.create_connection(ws_url, timeout=3,
                                origin="http://localhost:9222",
                                host="localhost:9222")
                            w.send(_j.dumps({"id":1,"method":"Page.navigate",
                                           "params":{"url":url}}))
                            w.close()
                            log.info("Opened calendar in existing Chrome tab")
                            return
                # No suitable tab - open new tab
                _ur.urlopen(f"http://localhost:9222/json/new?{url}", timeout=2)
                log.info("Opened new Chrome tab for calendar")
                return
            except Exception as ex:
                log.debug(f"CDP open failed ({ex}), using webbrowser")
            webbrowser.open(url)
        threading.Thread(target=_open_browser, daemon=True).start()

    print(f"\n  Browser: {url}", flush=True)
    print("  Ctrl+C to stop.\n", flush=True)

    try:
        host = "0.0.0.0"
        log.info(f"Starting on {host}:{APP_PORT}")
        app.run(host=host, port=APP_PORT,
                debug=False, threaded=True, use_reloader=False)
    except Exception as e:
        print(f"\n  [FATAL] {e}", flush=True)
        traceback.print_exc()
        input("\n  Press Enter to exit...")
