"""
PayCalendar — платёжный календарь через почту
"""
import os
# Zone.ee IMAP fix - MUST be before any config reads
if not os.environ.get("PC_EMAIL_IMAP_HOST"):
    os.environ["PC_EMAIL_IMAP_HOST"] = "imap.zone.eu"

import email, email.header, imaplib, json, logging, re, socket
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
DATA_FILE    = _DATA_DIR / "invoices.json"
PENDING_FILE = _DATA_DIR / "scan_pending.json"
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
# Zone.ee official IMAP server (mail.zone.ee doesn't work from cloud)
_cfg_imap = c("email", "imap_host", "imap.zone.eu")
IMAP_HOST  = "imap.zone.eu" if _cfg_imap in ("", "mail.zone.ee") else _cfg_imap
IMAP_PORT      = int(c("email", "imap_port", "993"))
IMAP_FOLDER    = c("email", "imap_folder", "INBOX")
SCAN_LIMIT       = int(c("email", "scan_last_emails", "5000"))
QUICK_SCAN_LIMIT = int(c("email", "quick_scan_emails", "100"))
AUTO_SCAN      = int(c("email", "auto_scan_minutes", "60"))
API_KEY        = c("claude", "api_key")
APP_PORT       = int(os.environ.get("PORT", c("app", "port", "5050")))
COMPANY        = c("app", "company_name", "My Company")
WARN_DAYS      = int(c("notifications", "warn_days_before", "3"))
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
IMPORTANT: Extract the ACTUAL amount from the text. Do NOT leave amount as 0 if the total/sum is mentioned anywhere in the text.
IMPORTANT: Extract issue_date and due_date ONLY from the document body text.
- issue_date: look for "invoice date", "arve kuupäev", "дата счёта", "Rechnungsdatum", "date of invoice" etc. in the body.
- due_date: look for "due date", "pay by", "maksetähtaeg", "tasuda hiljemalt", "срок оплаты", "оплатить до" etc. in the body.
- Do NOT use the email header "Date: {date}" as issue_date. Leave issue_date empty ("") if the document body does not contain an invoice date.
- Leave due_date empty ("") if the document body does not contain a payment due date.

If invoice: {{"is_invoice":true,"vendor":"NAME","invoice_number":"INV-XXX","amount":TOTAL_AMOUNT,
"currency":"EUR","due_date":"YYYY-MM-DD","issue_date":"YYYY-MM-DD",
"description":"brief description","category":"utilities|software|services|rent|taxes|supplies|logistics|marketing|other"}}
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
    """Reload credentials from config file."""
    global EMAIL_ADDR, EMAIL_PASS, IMAP_HOST, IMAP_PORT, CLAUDE_KEY, COMPANY
    cfg.read(CONFIG_FILE, encoding="utf-8")
    _addr = c("email", "address", "")
    _pass = c("email", "password", "")
    _host = c("email", "imap_host", "")
    _key  = c("claude", "api_key", "")
    _co   = c("app", "company_name", "")
    if _addr: EMAIL_ADDR = _addr
    if _pass: EMAIL_PASS = _pass
    # Always use correct Zone.ee IMAP (mail.zone.ee is wrong)
    IMAP_HOST = "imap.zone.eu" if not _host or _host == "mail.zone.ee" else _host
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

# ═══════════════════════════════════════════════════════════════════════════════
#  MULTI-KEY AI ROTATION SYSTEM
#  Automatically rotates through multiple API keys when quota is hit
# ═══════════════════════════════════════════════════════════════════════════════

class KeyPool:
    """Pool of API keys with auto-rotation on quota/rate errors."""
    def __init__(self, keys: list, name: str):
        self.name   = name
        self.keys   = [k for k in keys if k and len(k) > 10]
        self.idx    = 0
        self.failed = set()

    def current(self):
        """Get current working key."""
        good = [k for k in self.keys if k not in self.failed]
        return good[self.idx % len(good)] if good else None

    def rotate(self, bad_key=None):
        """Mark key as failed and move to next."""
        if bad_key:
            self.failed.add(bad_key)
            log.warning(f"{self.name}: key exhausted ({bad_key[:12]}...), rotating")
        good = [k for k in self.keys if k not in self.failed]
        if not good:
            log.error(f"{self.name}: ALL keys exhausted!")
            self.failed.clear()  # reset to try again
        self.idx = (self.idx + 1) % max(1, len(self.keys))

    def add(self, key):
        """Add a new key to the pool."""
        if key and key not in self.keys:
            self.keys.append(key)
            self.failed.discard(key)
            log.info(f"{self.name}: added new key ({key[:12]}...)")

    def status(self):
        return {
            "total": len(self.keys),
            "working": len(self.keys) - len(self.failed),
            "exhausted": len(self.failed),
        }


def _load_key_pool(env_prefix, config_key, base_key):
    """Load keys from env vars: KEY, KEY_2, KEY_3... + config."""
    keys = []
    if base_key and len(base_key) > 10:
        keys.append(base_key)
    # Check env vars: PREFIX, PREFIX_2, PREFIX_3, PREFIX_4, PREFIX_5
    for i in range(2, 11):
        k = os.environ.get(f"{env_prefix}_{i}", "").strip()
        if k and k not in keys:
            keys.append(k)
    # Check config: key, key_2, key_3
    for i in range(2, 6):
        k = c("ai", f"{config_key}_{i}", "")
        if k and k not in keys:
            keys.append(k)
    return keys

# ── AI provider config ───────────────────────────────────────────────────────
GEMINI_KEY  = c("ai", "gemini_key") or os.environ.get("GEMINI_API_KEY","AIzaSyDyrZ8ZkPmoNRGL7Wv9P1qs_laKClKIIAw")
GROQ_KEY    = c("ai", "groq_key")   or os.environ.get("GROQ_API_KEY","")
OPENAI_KEY  = c("ai", "openai_key") or os.environ.get("OPENAI_API_KEY","")

# Initialize key pools after keys are loaded
_gemini_pool = KeyPool(_load_key_pool("GEMINI_API_KEY", "gemini_key", GEMINI_KEY), "Gemini")
_groq_pool   = KeyPool(_load_key_pool("GROQ_API_KEY",   "groq_key",   GROQ_KEY),   "Groq")
_openai_pool = KeyPool(_load_key_pool("OPENAI_API_KEY", "openai_key", OPENAI_KEY), "OpenAI")

_provider_blocked = set()  # tracks quota-exceeded providers

def _active_provider():
    """Return first available AI provider — Claude is primary."""
    if API_KEY and len(API_KEY) > 20 and "claude" not in _provider_blocked: return "claude"
    if _gemini_pool.current() and "gemini" not in _provider_blocked: return "gemini"
    if _groq_pool.current()   and "groq"   not in _provider_blocked: return "groq"
    if _openai_pool.current() and "openai" not in _provider_blocked: return "openai"
    if API_KEY and len(API_KEY) > 20       and "claude" not in _provider_blocked: return "claude"
    return None


def _ask_huggingface(prompt):
    """HuggingFace Inference API - free public models, no key needed."""
    # Using Mistral-7B via public HuggingFace endpoint
    models_to_try = [
        "mistralai/Mistral-7B-Instruct-v0.3",
        "HuggingFaceH4/zephyr-7b-beta",
        "microsoft/Phi-3-mini-4k-instruct",
    ]
    for model in models_to_try:
        try:
            url = f"https://api-inference.huggingface.co/models/{model}"
            sys_msg = CLAUDE_SYSTEM + "\n\nAnalyze this email and respond with JSON only."
            r = requests.post(url, json={
                "inputs": f"[INST] {sys_msg}\n\n{prompt[:1500]} [/INST]",
                "parameters": {"max_new_tokens": 300, "temperature": 0.1,
                                "return_full_text": False},
            }, timeout=20)
            if r.status_code == 200:
                result = r.json()
                if isinstance(result, list) and result:
                    text = result[0].get("generated_text", "")
                    if text and "{" in text:
                        return text
            elif r.status_code == 503:  # model loading
                import time as _t; _t.sleep(3)
        except Exception as ex:
            log.debug(f"HF {model}: {ex}")
    raise Exception("HuggingFace: все модели недоступны")

def _ask_cohere(prompt):
    """Cohere API - free 1000 calls/month. Key from cohere.com"""
    cohere_key = os.environ.get("COHERE_API_KEY", c("ai", "cohere_key", ""))
    if not cohere_key:
        raise ValueError("No Cohere key")
    r = requests.post(
        "https://api.cohere.ai/v1/generate",
        headers={"Authorization": f"Bearer {cohere_key}",
                 "Content-Type": "application/json"},
        json={"model": "command", "prompt": CLAUDE_SYSTEM + "\n\n" + prompt[:2000],
              "max_tokens": 300, "temperature": 0.1},
        timeout=20
    )
    r.raise_for_status()
    return r.json()["generations"][0]["text"]

def ask_ai(prompt):
    """
    Priority chain: Claude (best quality) → Gemini → Groq → OpenAI → HuggingFace
    Claude is primary when API key is available — paid, fast, best extraction.
    Free providers (Gemini/Groq) used as fallback when Claude unavailable.
    """
    errors = []

    # ── 1. Claude (Anthropic) — PRIMARY: best quality, paid ──────────────────
    if API_KEY and len(API_KEY) > 20 and "claude" not in _provider_blocked:
        try:
            return _ask_claude(prompt)
        except Exception as ex:
            err = str(ex)
            errors.append(f"Claude: {err[:60]}")
            log.warning(f"Claude API failed: {err[:100]}")
            # Block Claude if auth/billing error (don't retry on every call)
            if "401" in err or "403" in err or "billing" in err.lower():
                _provider_blocked.add("claude")

    # ── 2. Gemini (free fallback, max 2 attempts) ────────────────────────────
    if _gemini_pool.keys and "gemini" not in _provider_blocked:
        for _attempt in range(2):
            try:
                return _ask_gemini(prompt)
            except Exception as ex:
                err = str(ex)
                errors.append(f"Gemini: {err[:60]}")
                if "нет ключей" in err or "ALL keys" in err.lower():
                    break
                if "invalid" in err.lower() or "billing" in err.lower():
                    _provider_blocked.add("gemini"); break

    # ── 3. Groq (free fallback, max 2 attempts) ─────────────────────────────
    if _groq_pool.keys and "groq" not in _provider_blocked:
        for _attempt in range(min(2, len(_groq_pool.keys))):
            try:
                return _ask_groq(prompt)
            except Exception as ex:
                err = str(ex)
                errors.append(f"Groq: {err[:60]}")
                if "нет доступных" in err or "ALL keys" in err.lower():
                    break
                if "invalid" in err.lower() or "billing" in err.lower():
                    _provider_blocked.add("groq"); break

    # ── 4. OpenAI (fallback, max 2 attempts) ─────────────────────────────────
    if _openai_pool.keys and "openai" not in _provider_blocked:
        for _attempt in range(min(2, len(_openai_pool.keys))):
            try:
                return _ask_openai(prompt)
            except Exception as ex:
                err = str(ex)
                errors.append(f"OpenAI: {err[:60]}")
                if "invalid" in err.lower() or "billing" in err.lower():
                    _provider_blocked.add("openai"); break

    # ── 5. HuggingFace (free, no key) ─────────────────────────────────────────
    try:
        return _ask_huggingface(prompt)
    except Exception as ex:
        errors.append(f"HuggingFace: {str(ex)[:40]}")

    raise ValueError("Все AI провайдеры недоступны — используются ключевые слова")

_gemini_last_call = [0.0]
# Per-key rate limiter: {key_prefix: last_call_time}
_gemini_key_timers = {}
_gemini_timer_lock = threading.Lock()

# Gemini models - each has SEPARATE daily quota (free tier)
_GEMINI_MODELS = [
    "gemini-2.0-flash",       # 1500 req/day free
    "gemini-1.5-flash",       # 1500 req/day free (separate quota!)
    "gemini-1.5-flash-8b",    # 1000 req/day free (separate quota!)
    "gemini-2.0-flash-lite",  # experimental free
]
_gemini_model_idx = [0]
_gemini_model_failed = set()

def _ask_gemini(prompt):
    """Google Gemini with model rotation (each model = separate free quota)."""
    import time as _t
    # Per-key rate limiting: 0.5s between requests PER KEY (not global)
    # This allows N keys to run at N× throughput
    key = _gemini_pool.current()
    if key:
        kp = key[:12]
        with _gemini_timer_lock:
            last = _gemini_key_timers.get(kp, 0.0)
            now = _t.time()
            wait = 0.5 - (now - last)
            if wait > 0:
                _t.sleep(wait)
            _gemini_key_timers[kp] = _t.time()

    body = {
        "contents": [{"parts": [{"text": CLAUDE_SYSTEM + "\n\n" + prompt}]}],
        "generationConfig": {"maxOutputTokens": 512, "temperature": 0.1},
    }
    key = _gemini_pool.current()
    if not key:
        raise Exception("Gemini: нет ключей")

    # Try each model (separate quotas)
    good_models = [m for m in _GEMINI_MODELS if m not in _gemini_model_failed]
    if not good_models:
        _gemini_model_failed.clear()  # reset and retry
        good_models = list(_GEMINI_MODELS)

    for model in good_models:
        url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={key}"
        try:
            r = requests.post(url, json=body, timeout=25)
            if r.status_code == 200:
                log.debug(f"Gemini OK via {model}")
                parts = r.json().get("candidates",[{}])[0].get("content",{}).get("parts",[])
                return "".join(p.get("text","") for p in parts)
            elif r.status_code == 429:
                log.warning(f"Gemini {model} quota hit → trying next model")
                _gemini_model_failed.add(model)
                _t.sleep(1)
                continue
            elif r.status_code == 404:
                _gemini_model_failed.add(model)  # model not available
                continue
            else:
                r.raise_for_status()
        except requests.HTTPError:
            raise
        except Exception:
            continue

    # All models exhausted → rotate key
    _gemini_pool.rotate(key)
    _gemini_model_failed.clear()
    raise Exception("Gemini: все модели исчерпали квоту на этом ключе")

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
    """Groq with auto key rotation."""
    import time as _t
    for attempt in range(max(1, _groq_pool.status()["total"])):
        key = _groq_pool.current()
        if not key:
            raise Exception("Groq: нет доступных ключей")
        try:
            r = requests.post(
                "https://api.groq.com/openai/v1/chat/completions",
                headers={"Authorization": f"Bearer {key}",
                         "Content-Type": "application/json"},
                json={"model": "llama-3.1-8b-instant",
                      "max_tokens": 400, "temperature": 0.1,
                      "messages": [
                          {"role": "system", "content": CLAUDE_SYSTEM},
                          {"role": "user",   "content": prompt},
                      ]},
                timeout=30,
            )
            if r.status_code in (429, 413):
                _groq_pool.rotate(key); _t.sleep(2); continue
            r.raise_for_status()
            return r.json()["choices"][0]["message"]["content"]
        except requests.HTTPError:
            raise
    raise Exception("Groq: все ключи исчерпаны")

def _ask_openai(prompt):
    """OpenAI GPT — tries all keys in pool."""
    for _attempt in range(max(1, len(_openai_pool.keys))):
        key = _openai_pool.current()
        if not key:
            raise Exception("OpenAI: нет ключей")
        try:
            r = requests.post(
                "https://api.openai.com/v1/chat/completions",
                headers={"Authorization": f"Bearer {key}",
                         "Content-Type": "application/json"},
                json={"model": "gpt-4o-mini", "max_tokens": 512, "temperature": 0.1,
                      "messages": [
                          {"role": "system", "content": CLAUDE_SYSTEM},
                          {"role": "user",   "content": prompt},
                      ]},
                timeout=30,
            )
            if r.status_code == 429:
                _openai_pool.rotate(key)
                continue
            r.raise_for_status()
            return r.json()["choices"][0]["message"]["content"]
        except requests.HTTPError as ex:
            if "401" in str(ex) or "403" in str(ex):
                _openai_pool.rotate(key)
            raise
    raise Exception("OpenAI: все ключи исчерпаны")

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



# ═══════════════════════════════════════════════════════════════════════════════
#  PDF STORAGE
#  PDFs saved to /data/pdfs/{inv_id}.pdf during IMAP scan
#  Served via /api/invoice/<id>/pdf
# ═══════════════════════════════════════════════════════════════════════════════

_PDF_DIR = _DATA_DIR / "pdfs"
_PDF_DIR.mkdir(parents=True, exist_ok=True)


def save_pdf(inv_id: str, pdf_bytes: bytes, filename: str = "", invoice_date: str = None, is_offer: bool = False) -> str | None:
    """
    PRIMARY: Upload PDF to Google Drive.
    FALLBACK: Save locally to Volume if Drive not connected.
    Returns: gdrive webViewLink (primary) or local path str (fallback).
    """
    safe_id = re.sub(r"[^a-zA-Z0-9_-]", "_", inv_id)[:80]
    clean_name = re.sub(r"[^a-zA-Z0-9._-]", "_", filename or inv_id)[:80]
    if not clean_name.endswith(".pdf"):
        clean_name += ".pdf"

    # ── Try Google Drive first (primary storage) ──────────────────────────
    try:
        gdrive_result = gdrive_upload_pdf(pdf_bytes, clean_name, invoice_date, is_offer=is_offer)
        if gdrive_result:
            link = gdrive_result.get("webViewLink", "")
            file_id = gdrive_result.get("id", "")
            # Cache the link locally (tiny text file, not the PDF)
            _PDF_DIR.mkdir(parents=True, exist_ok=True)
            (_PDF_DIR / f"{safe_id}.gdrive").write_text(link, encoding="utf-8")
            (_PDF_DIR / f"{safe_id}.gdrive_id").write_text(file_id, encoding="utf-8")
            log.info(f"PDF → GDrive: {clean_name} ({len(pdf_bytes)//1024}KB)")
            return link
    except Exception as ex:
        log.warning(f"GDrive upload failed, using local fallback: {ex}")

    # ── Fallback: local Volume storage ────────────────────────────────────
    try:
        _PDF_DIR.mkdir(parents=True, exist_ok=True)
        path = _PDF_DIR / f"{safe_id}.pdf"
        path.write_bytes(pdf_bytes)
        log.info(f"PDF → local fallback: {path.name} ({len(pdf_bytes)//1024}KB)")
        return str(path)
    except Exception as ex:
        log.error(f"PDF save failed entirely: {ex}")
        return None


def get_pdf_path(inv_id: str) -> Path | None:
    """Get local path to PDF (fallback storage). None if only in Drive."""
    safe_id = re.sub(r"[^a-zA-Z0-9_-]", "_", inv_id)[:80]
    path = _PDF_DIR / f"{safe_id}.pdf"
    return path if path.exists() else None

def has_pdf(inv_id: str) -> bool:
    """True if PDF exists in Drive OR locally."""
    safe_id = re.sub(r"[^a-zA-Z0-9_-]", "_", inv_id)[:80]
    return (
        (_PDF_DIR / f"{safe_id}.gdrive").exists() or
        (_PDF_DIR / f"{safe_id}.pdf").exists()
    )

def get_gdrive_id(inv_id: str) -> str:
    """Get Google Drive file ID for downloading."""
    safe_id = re.sub(r"[^a-zA-Z0-9_-]", "_", inv_id)[:80]
    id_file = _PDF_DIR / f"{safe_id}.gdrive_id"
    return id_file.read_text(encoding="utf-8").strip() if id_file.exists() else ""

def get_gdrive_link(inv_id: str) -> str:
    """Get Google Drive view link for invoice PDF. Empty string if not uploaded."""
    safe_id = re.sub(r"[^a-zA-Z0-9_-]", "_", inv_id)[:80]
    link_file = _PDF_DIR / f"{safe_id}.gdrive"
    return link_file.read_text(encoding="utf-8").strip() if link_file.exists() else ""


def pdf_count() -> int:
    """Count stored PDFs."""
    return len(list(_PDF_DIR.glob("*.pdf"))) if _PDF_DIR.exists() else 0



# ═══════════════════════════════════════════════════════════════════════════════
#  GOOGLE DRIVE INTEGRATION
# ═══════════════════════════════════════════════════════════════════════════════

_gdrive_service = None

def _get_gdrive_service(force_refresh=False):
    """Build Google Drive service from stored credentials."""
    global _gdrive_service
    if _gdrive_service and not force_refresh:
        return _gdrive_service
    _gdrive_service = None  # reset on each call to pick up new tokens
    try:
        from google.oauth2.credentials import Credentials
        from googleapiclient.discovery import build
        import json

        token_data = c("gdrive", "token", "") or os.environ.get("GDRIVE_TOKEN", "")
        if not token_data:
            return None
        token = json.loads(token_data)
        creds = Credentials(
            token=token.get("access_token"),
            refresh_token=token.get("refresh_token"),
            token_uri="https://oauth2.googleapis.com/token",
            client_id=token.get("client_id"),
            client_secret=token.get("client_secret"),
        )
        # Auto-refresh if expired
        if creds.expired and creds.refresh_token:
            from google.auth.transport.requests import Request
            creds.refresh(Request())
            # Save refreshed token
            token["access_token"] = creds.token
            save_config_value("gdrive", "token", json.dumps(token))

        _gdrive_service = build("drive", "v3", credentials=creds, cache_discovery=False)
        return _gdrive_service
    except Exception as ex:
        log.warning(f"Google Drive init: {ex}")
        return None


def _gdrive_get_or_create_folder(service, name: str, parent_id: str = None) -> str:
    """Get or create a folder in Drive. Returns folder ID."""
    q = f"name='{name}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
    if parent_id:
        q += f" and '{parent_id}' in parents"
    try:
        res = service.files().list(q=q, fields="files(id,name)").execute()
        if res.get("files"):
            return res["files"][0]["id"]
    except Exception:
        pass
    # Create folder
    meta = {"name": name, "mimeType": "application/vnd.google-apps.folder"}
    if parent_id:
        meta["parents"] = [parent_id]
    folder = service.files().create(body=meta, fields="id").execute()
    return folder["id"]


def gdrive_upload_pdf(pdf_bytes: bytes, filename: str, invoice_date: str = None,
                      is_offer: bool = False) -> dict | None:
    """
    Upload PDF to Google Drive.
    Invoices: PayCalendar/Счета/2026/03-March/
    Offers:   PayCalendar/Предложения/2026/03-March/
    Returns: {id, webViewLink} or None
    """
    service = _get_gdrive_service()
    if not service:
        return None
    try:
        from googleapiclient.http import MediaIoBaseUpload
        import io
        from datetime import datetime as _dt

        # Parse date for folder structure
        try:
            dt = _dt.fromisoformat(invoice_date) if invoice_date else _dt.now()
        except Exception:
            dt = _dt.now()
        year_str  = dt.strftime("%Y")
        month_str = dt.strftime("%m.%B")  # e.g. "03.Март" → use English for compatibility

        # Build folder: PayCalendar / Счета or Предложения / 2026 / 03-March
        root_id  = _gdrive_get_or_create_folder(service, "PayCalendar")
        cat_name = "Предложения" if is_offer else "Счета"
        cat_id   = _gdrive_get_or_create_folder(service, cat_name, root_id)
        year_id  = _gdrive_get_or_create_folder(service, year_str,  cat_id)
        month_id = _gdrive_get_or_create_folder(service, month_str, year_id)

        # Check if file already exists
        q = f"name='{filename}' and '{month_id}' in parents and trashed=false"
        existing = service.files().list(q=q, fields="files(id,webViewLink)").execute()
        if existing.get("files"):
            f = existing["files"][0]
            return {"id": f["id"], "webViewLink": f.get("webViewLink", "")}

        # Upload
        media = MediaIoBaseUpload(io.BytesIO(pdf_bytes), mimetype="application/pdf")
        file_meta = {"name": filename, "parents": [month_id]}
        uploaded = service.files().create(
            body=file_meta, media_body=media,
            fields="id,webViewLink"
        ).execute()

        # Make it publicly readable (anyone with link)
        service.permissions().create(
            fileId=uploaded["id"],
            body={"type": "anyone", "role": "reader"}
        ).execute()

        log.info(f"GDrive: uploaded {filename} → {uploaded.get('webViewLink','')[:60]}")
        return {"id": uploaded["id"], "webViewLink": uploaded.get("webViewLink", "")}

    except Exception as ex:
        log.error(f"GDrive upload {filename}: {ex}")
        return None


def gdrive_get_link(file_id: str) -> str:
    """Get view link for a Drive file ID."""
    return f"https://drive.google.com/file/d/{file_id}/view" if file_id else ""


# ── PDF Attachment Parser ─────────────────────────────────────────────────────
def extract_pdf_text(pdf_bytes: bytes, max_chars: int = 3000) -> str:
    """Extract text from PDF bytes. Returns empty string on failure."""
    try:
        import pypdf, io
        reader = pypdf.PdfReader(io.BytesIO(pdf_bytes), strict=False)
        texts = []
        for i, page in enumerate(reader.pages[:5]):  # max 5 pages
            try:
                t = page.extract_text() or ""
                if t.strip():
                    texts.append(t.strip())
            except Exception:
                pass
        result = "\n".join(texts)[:max_chars]
        log.debug(f"PDF: extracted {len(result)} chars from {len(reader.pages)} pages")
        return result
    except ImportError:
        log.warning("pypdf not installed - PDF parsing unavailable")
        return ""
    except Exception as ex:
        log.debug(f"PDF parse error: {ex}")
        return ""


def extract_attachments_from_msg(msg) -> list[dict]:
    """Extract attachments from email.message.Message object.
    Returns list of {filename, content_type, data: bytes}
    """
    attachments = []
    for part in msg.walk():
        cd = part.get_content_disposition() or ""
        ct = part.get_content_type() or ""
        fn = part.get_filename() or ""
        
        is_attachment = (
            "attachment" in cd or
            "inline" in cd and fn or
            ct in ("application/pdf", "application/octet-stream") and fn
        )
        
        if is_attachment and fn:
            try:
                data = part.get_payload(decode=True)
                if data:
                    attachments.append({
                        "filename":     fn,
                        "content_type": ct,
                        "data":         data,
                        "size":         len(data),
                    })
            except Exception as ex:
                log.debug(f"Attachment extract error ({fn}): {ex}")
    return attachments


def get_pdf_text_from_imap(mail_conn, uid: str) -> str:
    """Fetch full RFC822 message from IMAP and extract PDF text from attachments."""
    try:
        uid_b = uid.encode() if isinstance(uid, str) else uid
        _, data = mail_conn.fetch(uid_b, "(RFC822)")
        if not data or not data[0]:
            return ""
        raw = data[0][1]
        import email as _em
        msg = _em.message_from_bytes(raw)
        
        all_pdf_text = []
        for att in extract_attachments_from_msg(msg):
            fn  = att["filename"].lower()
            ct  = att["content_type"].lower()
            if "pdf" in fn or "pdf" in ct:
                txt = extract_pdf_text(att["data"])
                if txt:
                    all_pdf_text.append(f"[PDF: {att['filename']}]\n{txt}")
                    log.info(f"PDF parsed: {att['filename']} ({len(txt)} chars)")
        
        return "\n\n".join(all_pdf_text)
    except Exception as ex:
        log.debug(f"get_pdf_text error uid={uid}: {ex}")
        return ""


# ── PDF Invoice Header Parser ──────────────────────────────────────────────────
_PDF_INVOICE_PATTERNS = [
    # Amount patterns (Estonian/Russian/English)
    r"(?:kokku|total|summa|итого|gesamtbetrag|total amount)[\s:]*([\d\s.,]+)\s*(?:€|eur|usd|gbp|chf)?",
    r"(?:tasuda|to pay|zu zahlen|к оплате)[\s:]*([\d\s.,]+)\s*(?:€|eur)?",
    r"([\d\s.,]+)\s*(?:€|eur)\b",
    # Invoice number
    r"(?:arve nr|invoice no|rechnung nr|счёт №)[\s:.#]*([A-Z0-9\-/]+)",
    r"(?:arve|invoice|faktura|arvenumber)[\s:]*#?([A-Z0-9\-/]+)",
    # Date patterns
    r"(?:kuupäev|date|datum|дата)[\s:]*([\d]{1,2}[./\-][\d]{1,2}[./\-][\d]{2,4})",
    r"([\d]{1,2}[./][\d]{1,2}[./][\d]{4})",
    # Due date
    r"(?:maksetähtaeg|due date|fälligkeitsdatum|срок оплаты)[\s:]*([\d./\-]+)",
    r"(?:tasuda|pay by|bis)[\s:]*([\d]{1,2}[./][\d]{1,2}[./][\d]{4})",
]

def parse_pdf_invoice(pdf_text: str, filename: str = "") -> dict:
    """Extract invoice fields from PDF. English, Estonian, Russian, German."""
    if not pdf_text or len(pdf_text) < 10:
        return {}

    text = pdf_text.lower()
    result = {}

    # ── Invoice detection signals ─────────────────────────────────────────
    SIGNALS = [
        # English
        "invoice","invoice no","invoice number","amount due","total due",
        "due date","payment due","subtotal","vat no","tax invoice","bill to",
        "please pay","remit to","net 30","net 15","purchase order",
        # Estonian
        "arve","arve nr","tasuda","kokku","maksetähtaeg","käibemaks","kmkr",
        "viitenumber","ettemaks","lugupidamisega","arve kuupäev",
        # Russian
        "счёт","счет","к оплате","итого","инн","р/с","платёж",
        # German/Finnish
        "rechnung","rechnungsnr","fällig","zahlung","mwst","lasku","faktura",
    ]
    signals = sum(1 for s in SIGNALS if s in text)
    if signals < 1:
        return {}
    result["is_invoice"]   = True
    result["signal_count"] = signals

    import re as _re

    # ── Amount extraction ─────────────────────────────────────────────────
    # Normalize: "1,800.00" → 1800.00 | "1.800,00" → 1800.00 | "1 800,00" → 1800.00
    def clean_amount(s):
        s = s.strip()
        # European format: 1.800,00 or 1 800,00
        if _re.search(r"\d[.,]\d{3}[,\.]\d{2}$", s):
            s = _re.sub(r"[., ](?=\d{3}[,.])", "", s)
        # Remove thousands sep: 1,800.00 or 1.800,00
        if s.count(",") == 1 and s.count(".") == 0:
            # Could be "1800,00" (decimal comma)
            s = s.replace(",", ".")
        elif s.count(",") >= 1 and s.count(".") == 1:
            # "1,800.00" → remove commas
            s = s.replace(",", "")
        elif s.count(",") == 1 and s.count(".") >= 1:
            # "1.800,00" → remove dot, replace comma
            s = s.replace(".", "").replace(",", ".")
        s = _re.sub(r"\s", "", s)
        return float(s)

    amounts = []
    amount_patterns = [
        # English: "Total Due: 1,800.00" / "Amount Due: EUR 1,800.00"
        r"(?:total\s*due|amount\s*due|balance\s*due|grand\s*total|total\s*payable|invoice\s*total|net\s*total)[^\d€$£\\n]{0,15}([\d,\s.]+\d{2})",
        r"(?:subtotal|sub.total)[^\d€$£\\n]{0,10}([\d,\s.]+\d{2})",
        r"(?:total)[^\d€$£\\n]{0,5}([\d,\s.]+\d{2})",
        # Estonian: "Kokku tasuda: 1 800,00"
        r"(?:kokku\s*tasuda|tasuda|kokku)[^\d\\n]{0,10}([\d\s]+[.,]\d{2})",
        # Russian
        r"(?:итого|к\s*оплате)[^\d\\n]{0,10}([\d\s]+[.,]\d{2})",
        # Generic: number + currency symbol
        r"([\d]{1,3}(?:[,.\s]\d{3})*[.,]\d{2})\s*(?:€|EUR|USD|\$|GBP|£)",
        r"(?:€|EUR|\$|£)\s*([\d]{1,3}(?:[,.\s]\d{3})*[.,]\d{2})",
    ]
    for pat in amount_patterns:
        for m in _re.finditer(pat, pdf_text, _re.IGNORECASE):
            try:
                v = clean_amount(m.group(1))
                if 0.01 < v < 9_999_999:
                    amounts.append(v)
            except:
                pass

    if amounts:
        result["amount"] = round(max(amounts), 2)

    # ── Currency ──────────────────────────────────────────────────────────
    if "€" in pdf_text or _re.search(r"eur", text):
        result["currency"] = "EUR"
    elif "$" in pdf_text or _re.search(r"usd", text):
        result["currency"] = "USD"
    elif "£" in pdf_text or _re.search(r"gbp", text):
        result["currency"] = "GBP"
    else:
        result["currency"] = "EUR"

    # ── Invoice number ────────────────────────────────────────────────────
    for pat in [
        r"invoice\s*(?:no\.?|number|#|num\.?)\s*[:#]?\s*([A-Z0-9][\w\-/]{1,20})",
        r"inv\.?\s*(?:no\.?|#)\s*:?\s*([A-Z0-9][\w\-/]{1,15})",
        r"arve\s*(?:nr\.?|number|numbrid?)\s*:?\s*([A-Z0-9][\w\-/]{1,20})",
        r"arvenumber\s*:?\s*([A-Z0-9][\w\-/]{1,20})",
        r"rechnung\s*(?:nr\.?|nummer)\s*:?\s*([A-Z0-9][\w\-/]{1,15})",
        r"(?:^|\s)(?:no\.|nr\.)\s*([A-Z0-9]{2,}[\w\-/]{0,15})",
    ]:
        m = _re.search(pat, pdf_text, _re.IGNORECASE | _re.MULTILINE)
        if m:
            result["invoice_number"] = m.group(1).strip()[:30]
            break

    # ── Dates ─────────────────────────────────────────────────────────────
    def pd(s):
        from datetime import datetime as _dt
        for fmt in ["%d.%m.%Y","%d/%m/%Y","%m/%d/%Y","%Y-%m-%d","%d-%m-%Y"]:
            try:
                d = _dt.strptime(s.strip(), fmt)
                if 2020 <= d.year <= 2030:
                    return d.strftime("%Y-%m-%d")
            except:
                pass
        return None

    # Issue date — look for labeled date first (uses global ISSUE_DATE_PATTERNS at runtime)
    _issue_pats = [
        r"(?:invoice\s*date|date\s*of\s*invoice|issue\s*date|billed\s*date)[:\s]+([\d]{1,2}[./-][\d]{1,2}[./-][\d]{2,4})",
        r"(?:invoice\s*date|issue\s*date)[:\s]+([\d]{4}-[\d]{2}-[\d]{2})",
        r"(?:arve\s*kuupäev|arve\s*kp|väljastamise\s*kuupäev|koostatud)[:\s]+([\d]{1,2}[./-][\d]{1,2}[./-][\d]{2,4})",
        r"(?:дата\s*(?:счёта|счета|выставления|документа))[:\s]+([\d]{1,2}[./-][\d]{1,2}[./-][\d]{2,4})",
        r"(?:rechnungsdatum|ausstellungsdatum)[:\s]+([\d]{1,2}[./-][\d]{1,2}[./-][\d]{2,4})",
    ]
    for ip in _issue_pats:
        m = _re.search(ip, pdf_text, _re.IGNORECASE)
        if m:
            d = pd(m.group(1))
            if d:
                result["issue_date"] = d
                break

    # Due date — look for labeled date
    for due_pat in [
        r"(?:due\s*date|payment\s*due|pay\s*by|due\s*by)\s*[:\-]?\s*(\d{1,2}[./\-]\d{1,2}[./\-]\d{2,4})",
        r"(?:due\s*date|payment\s*due)\s*[:\-]?\s*(\d{4}-\d{2}-\d{2})",
        r"(?:tasuda\s*(?:hiljemalt)?|maksetähtaeg)\s*[:\-]?\s*(\d{1,2}[./]\d{1,2}[./]\d{4})",
        r"(?:срок\s*оплаты|оплатить\s*до)\s*[:\-]?\s*(\d{1,2}[./]\d{1,2}[./]\d{4})",
        r"(?:fällig\s*am?)\s*[:\-]?\s*(\d{1,2}[./]\d{1,2}[./]\d{4})",
    ]:
        m = _re.search(due_pat, pdf_text, _re.IGNORECASE)
        if m:
            d = pd(m.group(1))
            if d:
                result["due_date"] = d
                break

    # Collect all dates for fallback (if labeled dates not found)
    all_dates = []
    for dp in [r"\d{2}\.\d{2}\.\d{4}", r"\d{2}/\d{2}/\d{4}", r"\d{4}-\d{2}-\d{2}", r"\d{2}-\d{2}-\d{4}"]:
        for m in _re.finditer(dp, pdf_text):
            d = pd(m.group(0))
            if d and d not in all_dates:
                all_dates.append(d)
    if all_dates:
        all_dates.sort()
        if not result.get("issue_date"):
            result["issue_date"] = all_dates[0]
        if not result.get("due_date") and len(all_dates) > 1:
            result["due_date"] = all_dates[-1]

    # ── Vendor (first meaningful non-header line) ─────────────────────────
    skip_words = {"invoice","arve","rechnung","faktura","bill","receipt","statement",
                  "http","www","tel:","fax:","email:","phone:","page ","date:","from:","to:"}
    lines = [l.strip() for l in pdf_text.split("\n") if l.strip() and len(l.strip()) > 4]
    for line in lines[:10]:
        ll = line.lower()
        if not any(sw in ll for sw in skip_words) and not _re.match(r"^[\d\s.,€$£]+$", line):
            result["vendor"] = line[:60]
            break

    return result



# ── Keyword-based invoice extractor (no Claude API needed) ────────────────────
INVOICE_KW_STRONG = [
    # Estonian
    "arve","arve nr","arved","tasuda","makse","maksetähtaeg","maksenõue","arvetele",
    # English
    "invoice","bill","payment due","amount due","please pay","remittance",
    "overdue","pro forma","statement of account","purchase order","p.o.",
    "receipt","your order","order confirmation","subscription","renewal",
    # Commercial offers that may include payment
    "offer","quote","quotation","proposal","price list","pricelist",
    # Russian
    "счёт","счет","счёт-фактура","к оплате","оплата","квитанция","задолженность",
    "платёж","платеж","оплатить","услуги","акт","УПД","счёт на оплату",
    "стоимость","цена","прайс","предложение",
    # German
    "rechnung","zahlung","fällig","betrag fällig","mahnung","zahlungserinnerung",
    "angebot","kostenpflichtig",
    # Finnish/Nordic
    "lasku","maksu","eräpäivä","faktura","betalning","förfallodatum","tilaus",
    # French/Spanish/Italian
    "facture","factura","fattura","paiement","pago","pagamento","devis","offre",
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
    r"(?:due\s*date|payment\s*due|pay\s*by|due\s*by|tasuda\s*(?:hiljemalt)?|maksetähtaeg|срок\s*оплаты|оплатить\s*до|fällig\s*am?|eräpäivä|vencimiento|échéance)[:\s]+([\d]{1,2}[./-][\d]{1,2}[./-][\d]{2,4})",
    r"(?:due|tähtaeg|fällig|срок|eräpäivä)[:\s]+([\d]{1,2}[./-][\d]{1,2}[./-][\d]{2,4})",
    r"(?:pay by|maksta|bezahlen bis|оплатить до)[:\s]+([\d]{1,2}[./-][\d]{1,2}[./-][\d]{2,4})",
    r"(?:due\s*date|payment\s*due)[:\s]+([\d]{4}-[\d]{2}-[\d]{2})",
]

ISSUE_DATE_PATTERNS = [
    # English
    r"(?:invoice\s*date|date\s*of\s*invoice|issue\s*date|billed\s*date|bill\s*date)[:\s]+([\d]{1,2}[./-][\d]{1,2}[./-][\d]{2,4})",
    r"(?:invoice\s*date|issue\s*date)[:\s]+([\d]{4}-[\d]{2}-[\d]{2})",
    # Estonian
    r"(?:arve\s*kuupäev|arve\s*kp|väljastamise\s*kuupäev|koostatud)[:\s]+([\d]{1,2}[./-][\d]{1,2}[./-][\d]{2,4})",
    # Russian
    r"(?:дата\s*(?:счёта|счета|выставления|документа|арве)|выставлен)[:\s]+([\d]{1,2}[./-][\d]{1,2}[./-][\d]{2,4})",
    # German
    r"(?:rechnungsdatum|ausstellungsdatum)[:\s]+([\d]{1,2}[./-][\d]{1,2}[./-][\d]{2,4})",
    # Finnish/Nordic
    r"(?:laskupäivä|fakturadatum)[:\s]+([\d]{1,2}[./-][\d]{1,2}[./-][\d]{2,4})",
]


def extract_issue_date(text):
    """Extract invoice issue date from document body text. Returns '' if not found."""
    for pat in ISSUE_DATE_PATTERNS:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            raw = m.group(1)
            for fmt in ["%d.%m.%Y", "%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y",
                        "%d.%m.%y", "%m/%d/%Y"]:
                try:
                    d = datetime.strptime(raw, fmt).date()
                    today = date.today()
                    if date(today.year - 5, 1, 1) <= d <= date(today.year + 1, 12, 31):
                        return str(d)
                except Exception:
                    pass
    return ""


# extract_amount moved to new implementation above


def extract_amount(text):
    """Extract monetary amount. Handles EN (1,800.00) EU (1.800,00) and space (1 800,00) formats."""
    import re as _re2
    if not text: return 0.0

    def parse_amt(raw):
        raw = raw.strip().replace(" ","").replace("\xa0","")
        if not raw: return None
        dots = raw.count("."); commas = raw.count(",")
        try:
            if dots == 0 and commas == 0:
                return float(raw)
            elif dots == 1 and commas == 0:
                after = raw.split(".")[-1]
                if len(after) == 3: return float(raw.replace(".",""))  # 1.800 EU thousands
                return float(raw)  # 1800.00 decimal
            elif commas == 1 and dots == 0:
                after = raw.split(",")[-1]
                if len(after) == 3: return float(raw.replace(",",""))  # 1,800 EN thousands
                return float(raw.replace(",","."))  # 1800,00 EU decimal
            elif dots >= 1 and commas >= 1:
                ld = raw.rfind("."); lc = raw.rfind(",")
                if ld > lc: return float(raw.replace(",",""))           # 1,800.00 EN
                return float(raw.replace(".","").replace(",","."))      # 1.800,00 EU
            else:
                return float(_re2.sub(r"[.,](?=\d{3})", "", raw).replace(",","."))
        except: return None

    # Priority: invoice totals
    PRIORITY = [
        r"(?:total\s*due|amount\s*due|balance\s*due|grand\s*total|total\s*payable|invoice\s*total)[^\d€$£\n]{0,15}([\d,\.\s]+\d{2})",
        r"(?:total|subtotal)[^\d€$£\n]{0,8}([\d,\.\s]+\d{2})\s*(?:€|EUR|USD|\$|GBP)?",
        r"(?:kokku\s*tasuda|tasuda|kokku)[^\d\n]{0,10}([\d\s]+[.,]\d{2})",
        r"(?:итого|к\s*оплате)[^\d\n]{0,10}([\d\s]+[.,]\d{2})",
        r"(?:€|EUR|\$|£)\s*([\d]{1,3}(?:[\s,.]\d{3})*[.,]\d{2})",
        r"([\d]{1,3}(?:[\s,.]\d{3})*[.,]\d{2})\s*(?:€|EUR|\$|£|USD|GBP)",
    ]
    for pat in PRIORITY:
        for m in _re2.finditer(pat, text, _re2.IGNORECASE):
            v = parse_amt(m.group(1))
            if v and 0.01 <= v <= 9_999_999: return round(v, 2)

    # Fallback: AMOUNT_PATTERNS
    for pat in AMOUNT_PATTERNS:
        m = _re2.search(pat, text, _re2.IGNORECASE)
        if m:
            v = parse_amt(m.group(1))
            if v and 0.01 <= v <= 9_999_999: return round(v, 2)
    return 0.0


def extract_vendor(subject, sender, body):
    """Extract vendor name from email."""
    # 1. Try sender display name (before <email>)
    if sender:
        name_part = sender.split("<")[0].strip().strip('"').strip("'").strip()
        # Skip if it's just an email address or too short
        if 3 < len(name_part) < 60 and "@" not in name_part and name_part.replace(" ","").isalpha() == False:
            if not re.match(r'^[a-z0-9._%+-]+$', name_part, re.I):  # not plain email
                return name_part
        if 3 < len(name_part) < 60 and "@" not in name_part:
            return name_part

    # 2. Look for company name patterns in body
    for pat in VENDOR_PATTERNS:
        m = re.search(pat, body[:800], re.IGNORECASE | re.MULTILINE)
        if m:
            name = m.group(1).strip()[:60]
            if len(name) > 2:
                return name

    # 3. Extract from email domain (last resort - use full domain not just first part)
    if sender:
        em = re.search(r"@([\w.-]+)", sender)
        if em:
            domain = em.group(1)
            # Skip common email providers
            skip = {'gmail','yahoo','hotmail','outlook','mail','inbox','zone','yandex','icloud'}
            parts = [p for p in domain.split(".")[:-1] if p not in skip]
            if parts:
                return parts[-1].title()

    # 4. Use subject (clean up Re:/RE: prefixes)
    clean_subj = re.sub(r'^(Re:|RE:|Fwd:|FW:|\s)+', '', subject, flags=re.I).strip()
    return clean_subj[:40] if clean_subj else "Неизвестно"


def extract_due_date(text, email_date, fallback=True):
    """Extract due date from document text.
    If fallback=False, returns '' when not found (instead of email_date+30d)."""
    for pat in DUE_DATE_PATTERNS:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            raw = m.group(1)
            for fmt in ["%d.%m.%Y","%d/%m/%Y","%Y-%m-%d","%d-%m-%Y",
                        "%d.%m.%y","%m/%d/%Y"]:
                try:
                    from datetime import datetime as _dt
                    d = _dt.strptime(raw, fmt).date()
                    from datetime import date as _d2
                    today = _d2.today()
                    if _d2(today.year-2, 1, 1) <= d <= _d2(today.year+2, 12, 31):
                        return str(d)
                except Exception:
                    pass
    if not fallback:
        return ""
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
    
    # Has attachment = likely invoice/document
    if has_att:
        score += 25
    # Business email patterns
    if re.search(r'\b(nr|no|#|номер|nummer)\s*\.?\s*\d+', text, re.IGNORECASE):
        score += 15  # has document number
    
    # Amount pattern found
    if extract_amount(text) > 0:
        score += 30
    
    # Business correspondence boost
    if re.search(r'\d+[.,]\d{2}\s*(eur|usd|gbp|€|\$)', text, re.IGNORECASE):
        score += 20  # has price/amount
    # Only boost if it's a standalone offer/invoice number (not reply chains)
    if re.search(r'offer\s+\d{4,}|quote\s+\d+|order\s+\d+', text, re.IGNORECASE):
        score += 15  # numbered offer/quote/order
    # Penalize reply chains - Re: Re: is unlikely to be the invoice itself
    reply_depth = len(re.findall(r'\bRe:\s*|\bRE:\s*', subject, re.I))
    if reply_depth >= 2:
        score -= 20  # deep reply chain → less likely primary invoice
    # Negative signals
    if any(k in text for k in ["newsletter","unsubscribe","отписаться",
                                 "promotion","%off","discount code","click here"]):
        score -= 40
    if any(k in text for k in ["meeting","call","calendar","invite","demo"]):
        score -= 30
        
    return min(100, max(0, score))



def update_scan_state(uids, last_uid, last_date):
    if not uids: return
    st = load_state()
    seen = set(st.get("scanned_uids", []))
    seen.update(uids)
    st["scanned_uids"] = list(seen)[-5000:]  # keep last 5000
    if last_uid:  st["last_uid"]  = last_uid
    if last_date: st["last_date"] = last_date
    st["scan_count"] = st.get("scan_count", 0) + 1
    save_state(st)


def process_emails(emails_raw, emit, fetch_body=None, source="webmail"):
    """
    PARALLEL email analysis:
    Phase 1 — keyword pre-filter (instant, no API)
    Phase 2 — ThreadPoolExecutor, 1 worker per available API key
    """
    import concurrent.futures as _cf, time as _t

    existing     = load_invoices()
    existing_ids = {i.get("email_uid") for i in existing}
    # Also deduplicate by normalized subject+date
    def _norm_subj(s):
        return re.sub(r'^(Re:|RE:|Fwd:|FW:|Aw:|Vs:|\s)+', '', s, flags=re.IGNORECASE).strip().lower()
    existing_subj_dates = {
        (_norm_subj(i.get("description","") or i.get("vendor","")), i.get("issue_date",""))
        for i in existing
    }
    new_invs     = []
    last_uid = last_date = None
    all_uids = []
    t0 = _t.time()

    # ── Phase 1: decode + keyword pre-filter ─────────────────────────────────
    candidates, skipped_dup, skipped_kw = [], 0, 0

    for em in emails_raw:
        uid = str(em.get("id") or em.get("_id") or em.get("uid") or "")

        # Skip already-processed emails
        if uid in existing_ids:
            for _inv in existing:
                if _inv.get("email_uid") == uid:
                    _inv["reminder_count"] = _inv.get("reminder_count", 1) + 1
                    _inv["last_reminded"]  = datetime.now().strftime("%Y-%m-%d")
                    break
            skipped_dup += 1; all_uids.append(uid); continue

        # ── Decode all fields first ────────────────────────────────────────
        raw_subj = str(em.get("subject") or "")
        try:
            import email.header as _eh
            parts = _eh.decode_header(raw_subj)
            subj = "".join(
                (p.decode(enc or "utf-8", errors="replace") if isinstance(p, bytes) else p)
                for p, enc in parts)
        except Exception:
            subj = raw_subj

        frm  = str(em.get("from") or em.get("sender") or "")
        if isinstance(em.get("from"), dict):
            frm = em["from"].get("address") or em["from"].get("name") or ""
        body = str(em.get("text") or em.get("intro") or em.get("body") or "")[:2000]
        att  = bool(em.get("hasAttachments") or em.get("has_attachment") or em.get("attachments"))
        try:
            raw_date = str(em.get("date") or "")
            # Already ISO-formatted (YYYY-MM-DD) — use directly
            if re.match(r'^\d{4}-\d{2}-\d{2}', raw_date):
                ds = raw_date[:10]
            else:
                ds = parsedate_to_datetime(raw_date).strftime("%Y-%m-%d")
        except Exception:
            ds = date.today().isoformat()

        # ── Deduplication by (normalised subject, date) ───────────────────
        norm_key = (_norm_subj(subj), ds[:10])
        if norm_key in existing_subj_dates:
            for _inv in existing:
                inv_key = (_norm_subj(_inv.get("description","") or ""), _inv.get("issue_date","")[:10])
                if inv_key == norm_key:
                    _inv["reminder_count"] = _inv.get("reminder_count", 1) + 1
                    _inv["last_reminded"]  = ds
                    break
            skipped_dup += 1; all_uids.append(uid); continue
        existing_subj_dates.add(norm_key)

        # ── Skip Fwd: emails — find original and increment its reminder_count ─
        is_fwd = bool(re.match(r'^(fwd?:|fw:)', subj.strip(), re.IGNORECASE))
        if is_fwd:
            norm_fwd = _norm_subj(subj)
            matched = False
            for _inv in existing:
                if norm_fwd and norm_fwd in _norm_subj(_inv.get("description","") or _inv.get("vendor","")):
                    _inv["reminder_count"] = _inv.get("reminder_count", 1) + 1
                    _inv["last_reminded"]  = ds
                    matched = True
                    break
            skipped_dup += 1; all_uids.append(uid); continue

        # ── Keyword pre-filter ────────────────────────────────────────────
        score = is_invoice_by_keywords(subj, body, frm, att)
        if score < 15:
            skipped_kw += 1; all_uids.append(uid)
            last_uid = uid; last_date = ds
            continue

        candidates.append({"uid": uid, "subj": subj, "frm": frm,
                            "body": body, "ds": ds, "att": att, "score": score})

    total = len(candidates)
    emit(f"📬 Предфильтр: {len(emails_raw)} писем → {total} кандидатов "
         f"(пропущено: {skipped_dup} дубл, {skipped_kw} не счёт)", "ok")

    if total == 0:
        update_scan_state(all_uids, last_uid, last_date)
        return []

    # ── Phase 2: determine workers based on available keys ────────────────────
    gem_ok  = len([k for k in _gemini_pool.keys if k not in _gemini_pool.failed])
    grq_ok  = len([k for k in _groq_pool.keys   if k not in _groq_pool.failed])
    oai_ok  = len([k for k in _openai_pool.keys  if k not in _openai_pool.failed])
    has_cl  = 1 if (API_KEY and len(API_KEY) > 20) else 0
    n_keys  = gem_ok + grq_ok + oai_ok + has_cl
    # Gemini: each key × 4 models = high throughput with per-key rate limiting
    # Scale: 3 workers per Gemini key, 2 per Groq/OpenAI, 1 for Claude
    workers = max(1, min(gem_ok * 3 + grq_ok * 2 + oai_ok * 2 + has_cl, 40, total))

    emit(f"⚡ Параллельный анализ: {workers} потоков | "
         f"Gemini×{gem_ok} Groq×{grq_ok} OpenAI×{oai_ok} Claude×{has_cl}", "info")

    # ETA estimate
    ai_rps  = workers * 0.5  # ~0.5 req/sec per worker (IMAP lock + AI latency)
    eta_sec = int(total / ai_rps) if ai_rps > 0 else total * 4
    emit(f"⏱ Ожидаемое время: ~{eta_sec}с ({total} кандидатов × {workers} потоков)", "info")

    # ── Phase 3: parallel analysis ────────────────────────────────────────────
    done   = [0]
    lock   = __import__("threading").Lock()

    def analyze(em):
        uid   = em["uid"]; subj = em["subj"]; frm = em["frm"]
        body  = em["body"]; ds   = em["ds"];  att = em["att"]
        score = em["score"]
        pdf_text = ""  # will be filled after fetch_body

        # Fetch full body + PDFs (result includes PDF text appended)
        if fetch_body and uid:
            try:
                fb = fetch_body(uid)
                if fb:
                    body     = fb[:3000]
                    pdf_text = fb  # PDF text already appended by _fetch_pdf_and_body
                    new_score = is_invoice_by_keywords(subj, body, frm, att)
                    if new_score > score: score = new_score
            except Exception: pass

        inv = None
        try:
            combined = body
            if pdf_text:
                combined = body + "\n\n--- ВЛОЖЕНИЕ PDF ---\n" + pdf_text[:2000]

            # Fast-path: very high keyword score + PDF → skip AI, use keyword extractor
            if score >= 75 and att and not fetch_body:
                search_text = subj + " " + combined
                amount  = extract_amount(search_text)
                vendor  = extract_vendor(subj, frm, combined)
                if amount > 0 or score >= 90:
                    _fp_issue = extract_issue_date(search_text) or ds[:10]
                    _fp_due   = extract_due_date(search_text, ds, fallback=False) or ""
                    inv = build_invoice({
                        "is_invoice": True, "vendor": vendor, "amount": amount,
                        "currency": extract_currency(search_text),
                        "due_date": _fp_due,
                        "issue_date": _fp_issue,
                        "invoice_number": None, "description": subj,
                    }, uid, subj, frm, ds, att, source)
                    emit(f"✓ [KW-FAST] {inv['vendor']} {inv['amount']} {inv['currency']}", "ok")
                    with lock:
                        done[0] += 1
                        cur = done[0]
                        elapsed = max(0.1, _t.time() - t0)
                        spd = cur / elapsed
                        eta = int((total - cur) / spd) if spd > 0 else 0
                        emit(f"__progress__ {cur} {total} {spd:.1f} {eta}", "progress")
                    return uid, ds, inv

            prompt = CLAUDE_TMPL.format(subject=subj, sender=frm,
                                        date=ds, has_att=att, body=combined[:3000])
            obj = extract_json(ask_ai(prompt))
            if obj and obj.get("is_invoice"):
                # Fallback: if AI didn't extract amount, use regex extractor
                if not obj.get("amount") or float(obj.get("amount", 0)) == 0:
                    search_text = subj + " " + combined
                    fb_amount = extract_amount(search_text)
                    if fb_amount > 0:
                        obj["amount"] = fb_amount
                        obj["currency"] = obj.get("currency") or extract_currency(search_text)
                # Fallback: extract dates from document text if AI left them empty
                # User requirement: "дату бери из документа" — dates must come from document body
                if not obj.get("issue_date"):
                    doc_issue = extract_issue_date(combined)
                    if doc_issue:
                        obj["issue_date"] = doc_issue
                if not obj.get("due_date"):
                    doc_due = extract_due_date(combined + " " + subj, ds, fallback=False)
                    if doc_due:
                        obj["due_date"] = doc_due
                inv = build_invoice(obj, uid, subj, frm, ds, att, source)
                prov = _active_provider() or "AI"
                emit(f"✓ [{prov.upper()}] {inv['vendor']} "
                     f"{inv['amount']} {inv['currency']}", "ok")
        except Exception:
            # AI unavailable → keyword-only fallback
            if score >= 20:
                # Include PDF text in amount extraction
                search_text = subj + " " + body + " " + pdf_text
                amount   = extract_amount(search_text)
                vendor   = extract_vendor(subj, frm, body)
                # Skip Re: offer threads with no amount - these are email discussions, not invoices
                _offer_kws = ["offer","quote","proposal","предложение"]
                _is_offer_reply = (
                    re.match(r"^(re:|fw:|fwd:)", subj.strip(), re.IGNORECASE) and
                    any(k in subj.lower() for k in _offer_kws)
                )
                if _is_offer_reply and amount == 0:
                    log.debug(f"KW skip (offer reply, no amount): {subj[:50]}")
                elif amount == 0 and score < 40:
                    log.debug(f"KW skip (no amount, score={score}): {subj[:50]}")
                else:
                    _kw_text = subj + " " + body + " " + pdf_text
                    _kw_issue = extract_issue_date(_kw_text) or ds[:10]
                    _kw_due   = extract_due_date(_kw_text, ds, fallback=False) or ""
                    inv = build_invoice({
                        "is_invoice": True,
                        "vendor":   vendor,
                        "amount":   amount,
                        "currency": extract_currency(subj + " " + body[:300]),
                        "due_date": _kw_due,
                        "issue_date": _kw_issue,
                        "description": subj[:100],
                        "category": guess_category(subj, body, frm),
                        "invoice_number": "",
                    }, uid, subj, frm, ds, att, source)
                    emit(f"✓ [KW:{score}] {inv['vendor']} {inv['amount']} {inv['currency']}", "ok")

        with lock:
            done[0] += 1
            cur = done[0]
            elapsed = max(0.1, _t.time() - t0)
            spd = cur / elapsed
            eta = int((total - cur) / spd) if spd > 0 else 0
            emit(f"__progress__ {cur} {total} {spd:.1f} {eta}", "progress")

        return uid, ds, inv

    # Run threads
    results = []
    with _cf.ThreadPoolExecutor(max_workers=workers) as pool:
        futs = {pool.submit(analyze, em): em for em in candidates}
        for fut in _cf.as_completed(futs):
            try:
                results.append(fut.result())
            except Exception as ex:
                emit(f"Ошибка потока: {ex}", "err")

    # ── Phase 4: collect & save ───────────────────────────────────────────────
    for uid, ds, inv in results:
        all_uids.append(uid)
        if ds: last_date = max(last_date or ds, ds)
        last_uid = uid
        if inv: new_invs.append(inv)

    # Reload current state to preserve user changes made during scan
    # (user may have marked invoices paid while scan was running)
    current_saved  = load_invoices()
    current_by_id  = {i["id"]: i for i in current_saved}
    # Propagate reminder_count updates (non-destructive to user fields)
    for inv in existing:
        cur = current_by_id.get(inv["id"])
        if cur and inv.get("reminder_count", 1) > cur.get("reminder_count", 1):
            cur["reminder_count"] = inv["reminder_count"]
            cur["last_reminded"]  = inv.get("last_reminded", cur.get("last_reminded"))
    # Only add invoices not already in current state
    truly_new = [i for i in new_invs if i["id"] not in current_by_id]
    if truly_new:
        save_invoices(current_saved + truly_new)
        emit(f"✅ Добавлено {len(truly_new)} счетов!", "ok")
    else:
        emit("Новых счетов не найдено", "ok")

    elapsed = _t.time() - t0
    rps = len(candidates) / elapsed if elapsed > 0 else 0
    emit(f"⏱ Готово за {elapsed:.0f}с | {rps:.1f} писем/с | "
         f"{workers} потоков", "ok")
    emit(f"__progress__ {total} {total} {rps:.1f} 0", "progress")

    update_scan_state(all_uids, last_uid, last_date)
    return new_invs



# ── Main scan dispatcher ──────────────────────────────────────────────────────

def _resolve_host_doh(hostname):
    """Resolve hostname via socket (Railway has direct DNS access)."""
    try:
        import socket as _sock
        ip = _sock.gethostbyname(hostname)
        if ip and ip != hostname:
            log.debug(f"DNS {hostname} → {ip}")
            return ip
    except Exception as ex:
        log.debug(f"DNS resolve {hostname}: {ex}")
    return hostname


def _try_imap_connect(hosts, port, email, password):
    """Try connecting to multiple IMAP hostnames. Returns (mail, host) or (None, None)."""
    import ssl as _ssl
    for host in hosts:
        if not host: continue
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
    """Scan inbox via IMAP. Returns list of invoices or None on failure."""
    emit(f"IMAP подключение к {IMAP_HOST}:{IMAP_PORT}...")
    
    alt_hosts = list(dict.fromkeys(["imap.zone.eu", IMAP_HOST, "mail.zone.ee"]))
    alt_hosts = [h for h in alt_hosts if h]
    
    mail, connected_host = _try_imap_connect(alt_hosts, IMAP_PORT, EMAIL_ADDR, EMAIL_PASS)
    if not mail:
        emit("IMAP недоступен — переключаюсь на webmail", "warn")
        return None  # caller falls back to webmail

    emit(f"✓ IMAP подключён через {connected_host}", "ok")

    try:
        mail.select(IMAP_FOLDER)
        state    = load_state()
        last_uid = state.get("last_uid")

        # Build date filter
        date_terms = []
        if from_date:
            from datetime import datetime as _dt
            since_str = _dt.strptime(from_date, "%Y-%m-%d").strftime("%d-%b-%Y")
            date_terms.append(f"SINCE {since_str}".encode())
            emit(f"Фильтр: с {since_str}", "ok")
        if to_date:
            from datetime import datetime as _dt2, timedelta
            before_dt = _dt2.strptime(to_date, "%Y-%m-%d") + timedelta(days=1)
            date_terms.append(f"BEFORE {before_dt.strftime('%d-%b-%Y')}".encode())

        ids = set()

        # When date range given, fetch ALL emails in range
        if date_terms:
            try:
                _, data = mail.search(None, *date_terms)
                for uid in data[0].split():
                    ids.add(uid)
                emit(f"Дата-фильтр: {len(ids)} писем", "ok")
            except Exception as ex:
                emit(f"Дата-поиск ошибка: {ex}", "warn")

        # Use ALL emails (bypass IMAP case-sensitivity on zone.eu)
        # "SUBJECT arve" only matches lowercase; ALL+local filter catches all variants
        try:
            _, _all = mail.search(None, b"ALL")
            _all_uids = _all[0].split() if (_all and _all[0]) else []
            emit(f"📬 В ящике {len(_all_uids)} писем — берём {min(SCAN_LIMIT,len(_all_uids))} свежих", "ok")
            # Sort newest first, take SCAN_LIMIT
            try:
                _all_sorted = sorted(_all_uids,
                    key=lambda x: int(x.decode() if isinstance(x,bytes) else x),
                    reverse=True)
            except Exception:
                _all_sorted = list(reversed(_all_uids))
            for uid in _all_sorted[:SCAN_LIMIT]:
                ids.add(uid)
        except Exception as ex:
            emit(f"⚠ ALL: {ex} — пробуем ключевые слова", "warn")
            for term in IMAP_SUBJECTS:
                try:
                    _, data = mail.search(None, term)
                    if data and data[0]:
                        for uid in data[0].split(): ids.add(uid)
                except Exception: pass
            emit(f"🔍 Keyword fallback: {len(ids)}", "ok")

        # New since last scan
        if last_uid and not from_date:
            try:
                _, data = mail.search(None, f"UID {int(last_uid)+1}:*")
                for uid in data[0].split():
                    ids.add(uid)
                emit(f"Новые с UID {last_uid}...", "ok")
            except Exception:
                pass

        # ALL emails with PDF attachments (catch invoices with unusual subjects)
        try:
            for att_term in [b"HAS ATTACHMENT", b"HASATTACHMENT"]:
                try:
                    search_args = [att_term] + date_terms if date_terms else [att_term]
                    _, att_data = mail.search(None, *search_args)
                    if att_data and att_data[0]:
                        att_uids = set(att_data[0].split())
                        new_att = att_uids - ids
                        if new_att:
                            ids.update(new_att)
                            emit(f"📎 +{len(new_att)} писем с вложениями", "ok")
                    break
                except Exception:
                    continue
        except Exception as ex:
            log.debug(f"Attachment search: {ex}")

        # Fallback: last N emails
        if not ids:
            emit("Нет совпадений — беру последние письма", "warn")
            _, data = mail.search(None, "ALL")
            all_ids = data[0].split()
            ids = set(all_ids[-min(SCAN_LIMIT, len(all_ids)):])

        # Sort newest first; no limit when using date range (process everything)
        try:
            ids_sorted = sorted(ids, key=lambda x: int(x.decode() if isinstance(x,bytes) else x), reverse=True)
        except Exception:
            ids_sorted = list(ids)
        if not date_terms:
            ids_sorted = ids_sorted[:SCAN_LIMIT]
        emit(f"📬 {len(ids_sorted)} писем для анализа" + (f" (лимит {SCAN_LIMIT})" if not date_terms else " (все в диапазоне)"), "ok")
        ids = set(ids_sorted)

        # Fetch headers in batches (much faster than one-by-one)
        emails_raw = []
        ids_list   = list(ids)
        total_ids  = len(ids_list)
        BATCH_SZ   = 50  # UIDs per IMAP fetch command
        _hi = 0
        for batch_start in range(0, total_ids, BATCH_SZ):
            batch = ids_list[batch_start:batch_start + BATCH_SZ]
            uid_range = b",".join(
                (u if isinstance(u, bytes) else u.encode()) for u in batch
            )
            try:
                _, batch_data = mail.fetch(uid_range, "(RFC822.HEADER)")
                if not batch_data:
                    _hi += len(batch)
                    continue
                for item in batch_data:
                    if not isinstance(item, tuple) or len(item) < 2:
                        continue
                    try:
                        msg  = email.message_from_bytes(item[1])
                        subj = decode_header(msg.get("Subject",""))
                        sndr = decode_header(msg.get("From",""))
                        ct   = msg.get("Content-Type","").lower()
                        att  = "multipart/mixed" in ct or "multipart/related" in ct or "application/" in ct
                        try:
                            ds = parsedate_to_datetime(msg.get("Date","")).strftime("%Y-%m-%d")
                        except Exception:
                            ds = date.today().isoformat()
                        # Extract UID from response
                        uid_match = re.search(rb"UID (\d+)", item[0]) if isinstance(item[0], bytes) else None
                        uid_s = uid_match.group(1).decode() if uid_match else str(_hi)
                        emails_raw.append({
                            "id": uid_s, "subject": subj, "from": sndr,
                            "body": "", "date": ds, "has_attachment": att,
                        })
                    except Exception:
                        pass
                    _hi += 1
            except Exception as ex:
                # Fallback: fetch one-by-one for this batch
                for uid in batch:
                    try:
                        _, data = mail.fetch(
                            uid if isinstance(uid, bytes) else uid.encode(),
                            "(RFC822.HEADER)"
                        )
                        if not data or not data[0]: continue
                        msg  = email.message_from_bytes(data[0][1])
                        subj = decode_header(msg.get("Subject",""))
                        sndr = decode_header(msg.get("From",""))
                        ct   = msg.get("Content-Type","").lower()
                        att  = "multipart/mixed" in ct or "multipart/related" in ct or "application/" in ct
                        try:
                            ds = parsedate_to_datetime(msg.get("Date","")).strftime("%Y-%m-%d")
                        except Exception:
                            ds = date.today().isoformat()
                        uid_s = uid.decode() if isinstance(uid, bytes) else str(uid)
                        emails_raw.append({
                            "id": uid_s, "subject": subj, "from": sndr,
                            "body": "", "date": ds, "has_attachment": att,
                        })
                    except Exception:
                        pass
                    _hi += 1
            if _hi % 500 <= BATCH_SZ or _hi >= total_ids:
                emit(f"  📥 Заголовки: {min(_hi, total_ids)}/{total_ids}...", "info")

        # Hook for PDF fetching - pass mail connection
        _pending_pdfs = {}  # uid_str -> {filename, bytes, parsed}
        _imap_ref = [mail]  # mutable ref for reconnect inside closure
        _imap_lock = threading.Lock()  # Thread safety for shared IMAP connection

        def _fetch_pdf_and_body(uid_str):
            """Fetch full message: body text + extract ALL PDFs, parse invoice data."""
            try:
                uid_b = uid_str.encode() if isinstance(uid_str, str) else uid_str
                # Thread-safe IMAP fetch with timeout to prevent deadlock
                if not _imap_lock.acquire(timeout=30):
                    log.warning(f"IMAP lock timeout for {uid_str}, skipping")
                    return ""
                try:
                    # Set socket timeout to prevent hang
                    try:
                        _imap_ref[0].socket().settimeout(20)
                    except Exception:
                        pass
                    try:
                        _, data = _imap_ref[0].fetch(uid_b, "(RFC822)")
                    except Exception:
                        log.debug(f"scan_imap: IMAP reconnect for {uid_str}")
                        new_mail, _ = _try_imap_connect(alt_hosts, IMAP_PORT, EMAIL_ADDR, EMAIL_PASS)
                        if new_mail:
                            new_mail.select(IMAP_FOLDER)
                            _imap_ref[0] = new_mail
                            _, data = _imap_ref[0].fetch(uid_b, "(RFC822)")
                        else:
                            return ""
                finally:
                    _imap_lock.release()
                if not data or not data[0]: return ""
                raw_msg = email.message_from_bytes(data[0][1])
                body_text = get_plain_body(raw_msg)
                pdf_texts = []
                best_parsed = {}

                for part in raw_msg.walk():
                    ct = part.get_content_type() or ""
                    fn = part.get_filename() or ""
                    is_pdf = "pdf" in ct.lower() or (fn and fn.lower().endswith(".pdf"))
                    if not is_pdf: continue
                    try:
                        pdf_bytes = part.get_payload(decode=True)
                        if not pdf_bytes: continue
                        
                        # Extract text
                        pt = extract_pdf_text(pdf_bytes, max_chars=4000)
                        if pt:
                            pdf_texts.append(f"[PDF: {fn}]\n{pt}")
                            
                            # Parse invoice fields from PDF
                            parsed = parse_pdf_invoice(pt, fn)
                            if parsed.get("is_invoice"):
                                # Keep best parsed (highest signal count)
                                if parsed.get("signal_count", 0) > best_parsed.get("signal_count", 0):
                                    best_parsed = parsed
                        
                        # Store PDF bytes for saving
                        _pending_pdfs[uid_str] = {
                            "filename": fn, "bytes": pdf_bytes,
                            "parsed": best_parsed, "text": pt
                        }
                        emit(f"  📄 {fn} ({len(pdf_bytes)//1024}КБ)" +
                             (f" → invoice detected!" if best_parsed.get("is_invoice") else ""), "ok")
                    except Exception as pe:
                        log.debug(f"PDF {fn}: {pe}")

                result = body_text
                if pdf_texts:
                    result += "\n\n--- PDF СОДЕРЖИМОЕ ---\n" + "\n".join(pdf_texts)
                return result
            except Exception as ex:
                log.debug(f"fetch full {uid_str}: {ex}")
                return ""

        result = process_emails(emails_raw, emit,
                                fetch_body=_fetch_pdf_and_body, source="imap")

        # Save PDFs and enrich invoices with PDF-parsed data
        if _pending_pdfs and isinstance(result, list):
            for inv in result:
                uid = inv.get("email_uid","")
                if uid in _pending_pdfs:
                    pdf_info = _pending_pdfs[uid]
                    try:
                        save_pdf(inv["id"], pdf_info["bytes"], pdf_info["filename"],
                                     invoice_date=inv.get("issue_date",""),
                                     is_offer=bool(inv.get("is_offer")))
                        inv["has_pdf"]      = True
                        inv["pdf_filename"] = pdf_info["filename"]
                        
                        # Enrich invoice with PDF-extracted data if missing
                        # User requirement: "дату бери из документа" — prefer PDF dates
                        parsed = pdf_info.get("parsed", {})
                        pdf_text_raw = pdf_info.get("text", "")
                        if parsed.get("is_invoice"):
                            if not inv.get("amount") or float(inv.get("amount",0)) == 0:
                                if parsed.get("amount"):
                                    inv["amount"] = parsed["amount"]
                                    log.info(f"PDF amount: {parsed['amount']} for {inv['id']}")
                            if not inv.get("due_date") and parsed.get("due_date"):
                                inv["due_date"] = parsed["due_date"]
                            # Enrich issue_date from PDF if missing or equals email envelope date
                            if parsed.get("issue_date") and (
                                not inv.get("issue_date") or
                                inv.get("issue_date") == inv.get("added_at","")[:10]
                            ):
                                inv["issue_date"] = parsed["issue_date"]
                                log.info(f"PDF issue_date: {parsed['issue_date']} for {inv['id']}")
                            # If still no due_date, try extracting from raw PDF text
                            if not inv.get("due_date") and pdf_text_raw:
                                doc_due = extract_due_date(pdf_text_raw, inv.get("issue_date",""), fallback=False)
                                if doc_due:
                                    inv["due_date"] = doc_due
                            if not inv.get("invoice_number") and parsed.get("invoice_number"):
                                inv["invoice_number"] = parsed["invoice_number"]
                            if parsed.get("vendor") and not inv.get("vendor"):
                                inv["vendor"] = parsed["vendor"]
                    except Exception as pe:
                        log.error(f"PDF save error: {pe}")
            
            # Also add invoices detected ONLY in PDF (not caught by email keywords)
            result_uids = {i.get("email_uid","") for i in result}
            for uid_str, pdf_info in _pending_pdfs.items():
                if uid_str in result_uids: continue  # already processed
                parsed = pdf_info.get("parsed", {})
                if not parsed.get("is_invoice"): continue
                if not parsed.get("amount") and parsed.get("signal_count",0) < 3: continue
                
                # Find email metadata for this UID
                em_meta = next((e for e in emails_raw if str(e.get("id","")) == uid_str), {})
                subj = em_meta.get("subject","PDF Invoice")
                frm  = em_meta.get("from","")
                ds   = em_meta.get("date", date.today().isoformat())[:10]
                
                inv = build_invoice({
                    "is_invoice":     True,
                    "vendor":         parsed.get("vendor") or extract_vendor(subj, frm, ""),
                    "amount":         parsed.get("amount", 0),
                    "currency":       parsed.get("currency", "EUR"),
                    "due_date":       parsed.get("due_date"),
                    "issue_date":     parsed.get("issue_date") or ds,
                    "invoice_number": parsed.get("invoice_number",""),
                    "description":    subj[:100],
                    "category":       "services",
                    "status":         "pending",
                }, uid_str, subj, frm, ds, True, "imap:pdf")
                inv["has_pdf"]      = True
                inv["pdf_filename"] = pdf_info["filename"]
                try:
                    save_pdf(inv["id"], pdf_info["bytes"], pdf_info["filename"])
                except Exception: pass
                result.append(inv)
                emit(f"  📄 PDF invoice: {inv['vendor']} {inv['amount']}€", "ok")
            
            # Save all with has_pdf flags
            if any(i.get("has_pdf") for i in result):
                all_invs = load_invoices()
                inv_map  = {i["id"]: i for i in result}
                for i, inv_item in enumerate(all_invs):
                    if inv_item["id"] in inv_map:
                        all_invs[i] = inv_map[inv_item["id"]]
                save_invoices(all_invs)

        try: mail.logout()
        except Exception: pass
        return result

    except Exception as ex:
        emit(f"IMAP ошибка: {ex}", "err")
        try: mail.logout()
        except Exception: pass
        return []


def scan_email(emit=None, quick=False, from_date=None, to_date=None):
    """
    Scan inbox for invoices.
    quick=True  → last 30 emails (fast, for auto-scan & refresh button)
    quick=False → last 500 emails (full scan)
    """
    def emitter(msg, t="info"):
        log.info(msg)
        if emit: emit(msg, t)

    # Note: scan_lock is managed by _run_bg_scan (caller) - do NOT acquire here
    # Temporarily override scan limit for quick scan
    global SCAN_LIMIT
    orig_limit = SCAN_LIMIT
    if quick:
        SCAN_LIMIT = QUICK_SCAN_LIMIT
        emitter(f"Quick scan: last {QUICK_SCAN_LIMIT} emails")
    try:
        accounts = load_email_accounts()
        all_results = []

        if len(accounts) > 1:
            emitter(f"Сканирую {len(accounts)} почтовых аккаунта...", "ok")
            for acc in accounts:
                try:
                    res = scan_account(acc, emitter, from_date=from_date, to_date=to_date)
                    all_results.extend(res or [])
                except Exception as ex:
                    emitter(f"Аккаунт {acc['email']}: {ex}", "warn")
            return all_results

        # Smart routing: Zone IMAP (history) + Gmail API (new)
        all_results = []
        cutoff = GMAIL_CUTOFF  # 2026-04-05

        # ── Zone IMAP: historical emails (before cutoff) ──────────────
        if not from_date or from_date < cutoff:
            imap_to_date = min(to_date, cutoff) if to_date else cutoff
            emitter(f"📧 Zone IMAP (до {imap_to_date})...", "info")
            try:
                with socket.create_connection((IMAP_HOST, IMAP_PORT), timeout=8):
                    imap_r = scan_imap(emitter,
                        from_date=from_date, to_date=imap_to_date)
                    if imap_r:
                        all_results.extend(imap_r)
                        emitter(f"✅ Zone: {len(imap_r)} счетов", "ok")
            except Exception as ex:
                emitter(f"Zone IMAP: {type(ex).__name__}", "warn")

        # ── Gmail API: new emails (from cutoff onwards) ───────────────
        gmail_from = max(from_date or cutoff, cutoff)
        if not to_date or to_date >= cutoff:
            emitter(f"📨 Gmail (с {gmail_from})...", "info")
            gmail_r = scan_gmail(emitter,
                from_date=gmail_from, to_date=to_date)
            if gmail_r:
                all_results.extend(gmail_r)
                emitter(f"✅ Gmail: {len(gmail_r)} счетов", "ok")

        return all_results
    finally:
        SCAN_LIMIT = orig_limit  # always restore

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

def _bg_emit(msg, t="info"):
    """Thread-safe emit that stores to live state + log file."""
    log.info(f"[scan] {msg}")
    with _scan_state_lock:
        # Progress event from parallel worker
        if t == "progress" and msg.startswith("__progress__"):
            try:
                parts = msg.split()
                cur = int(parts[1]); tot = int(parts[2])
                spd = float(parts[3]); eta = int(parts[4])
                _scan_state_live["done"]  = cur
                _scan_state_live["total"] = tot
                _scan_state_live["speed"] = spd
                _scan_state_live["eta"]   = eta
                _scan_state_live["pct"]   = int(cur/tot*100) if tot > 0 else 0
            except Exception:
                pass
            return
        # Count found invoices
        if msg.startswith("✓") or msg.startswith("✅"):
            if "счет" in msg.lower() or any(c in msg for c in ["€","EUR","$"]):
                _scan_state_live["found"] = _scan_state_live.get("found", 0) + 1
        # Add to log
        entry = {"ts": datetime.now().strftime("%H:%M:%S"), "msg": msg, "t": t}
        _scan_state_live["log"].append(entry)
        if len(_scan_state_live["log"]) > _MAX_LOG:
            _scan_state_live["log"] = _scan_state_live["log"][-_MAX_LOG:]
    # Also persist to file so it survives gunicorn worker restarts
    try:
        progress_file = _DATA_DIR / "scan_progress.json"
        with open(progress_file, "w", encoding="utf-8") as f:
            json.dump(_scan_state_live, f, ensure_ascii=False, default=str)
    except Exception:
        pass

def _save_pending_scan(from_date, to_date, quick):
    """Write pending scan job to disk so it survives a deploy restart."""
    try:
        PENDING_FILE.write_text(
            json.dumps({"from_date": from_date, "to_date": to_date, "quick": quick}),
            encoding="utf-8",
        )
    except Exception:
        pass

def _clear_pending_scan():
    """Remove pending scan file (called on completion or manual stop)."""
    try:
        if PENDING_FILE.exists():
            PENDING_FILE.unlink()
    except Exception:
        pass

def _run_bg_scan(task_id, from_date=None, to_date=None, quick=False):
    """Background scan - runs independently of HTTP connections."""
    import time as _t
    with _scan_state_lock:
        _scan_state_live.update({
            "running": True, "queued": False, "task_id": task_id,
            "total": 0, "done": 0, "found": 0, "speed": 0, "eta": 0, "pct": 0,
            "log": [], "from_date": from_date, "to_date": to_date,
            "started": datetime.now().isoformat(), "finished": None, "error": None,
        })

    try:
        if not scan_lock.acquire(blocking=False):
            _bg_emit("Скан уже выполняется", "warn")
            return
        global SCAN_LIMIT
        orig_limit = SCAN_LIMIT
        if quick:
            SCAN_LIMIT = QUICK_SCAN_LIMIT
        try:
            _bg_emit(f"Фоновый скан запущен (task={task_id[:8]}...)", "ok")
            result = scan_email(_bg_emit, from_date=from_date, to_date=to_date, quick=quick)
            found = len(result) if isinstance(result, list) else 0
            with _scan_state_lock:
                _scan_state_live["found"] = found
            _bg_emit(f"✅ Скан завершён: {found} новых счетов", "ok")
            _clear_pending_scan()
        finally:
            SCAN_LIMIT = orig_limit
            if scan_lock.locked(): scan_lock.release()
    except Exception as ex:
        _bg_emit(f"❌ Ошибка: {ex}", "err")
        with _scan_state_lock:
            _scan_state_live["error"] = str(ex)
    finally:
        with _scan_state_lock:
            _scan_state_live["running"]  = False
            _scan_state_live["finished"] = datetime.now().isoformat()
            _scan_state_live["pct"]      = 100
        _bg_emit("Фоновый скан завершён", "ok")

def start_bg_scan(from_date=None, to_date=None, quick=False):
    """Start scan in background thread. Returns task_id."""
    import uuid
    task_id = str(uuid.uuid4())
    with _scan_state_lock:
        if _scan_state_live["running"]:
            return None  # already running
        _scan_state_live["running"] = True
        _scan_state_live["task_id"] = task_id
        _scan_state_live["queued"]  = False
    _save_pending_scan(from_date, to_date, quick)
    t = threading.Thread(
        target=_run_bg_scan,
        args=(task_id, from_date, to_date, quick),
        daemon=True, name=f"scan-{task_id[:8]}"
    )
    t.start()
    log.info(f"Background scan started: task={task_id[:8]}")
    return task_id



# ═══════════════════════════════════════════════════════════════════════════════
#  BITRIX24 CRM INTEGRATION
# ═══════════════════════════════════════════════════════════════════════════════

BITRIX_WEBHOOK = c("bitrix", "webhook_url") or os.environ.get("BITRIX_WEBHOOK", "")

def bitrix_call(method: str, params: dict = None) -> dict:
    """Call Bitrix24 REST API via webhook."""
    if not BITRIX_WEBHOOK:
        raise ValueError("Bitrix24 webhook не настроен")
    url = BITRIX_WEBHOOK.rstrip("/") + f"/{method}.json"
    try:
        r = requests.post(url, json=params or {}, timeout=15)
        r.raise_for_status()
        data = r.json()
        if "error" in data:
            raise Exception(f"Bitrix error: {data.get('error_description', data['error'])}")
        return data.get("result", data)
    except Exception as ex:
        log.error(f"Bitrix24 {method}: {ex}")
        raise


def sync_bitrix_invoices(emit=None) -> list:
    """
    Fetch invoices and deals from Bitrix24, convert to PayCalendar format.
    Returns list of new invoices added.
    """
    def say(msg, t="info"):
        log.info(f"Bitrix: {msg}")
        if emit: emit(f"[Bitrix] {msg}", t)

    if not BITRIX_WEBHOOK:
        say("Webhook не настроен — открой /keys", "warn")
        return []

    say("Подключение к Bitrix24...")
    existing     = load_invoices()
    existing_ids = {i.get("bitrix_id") for i in existing if i.get("bitrix_id")}
    new_invs     = []

    # ── Fetch smart-process invoices (modern Bitrix) ──────────────────────────
    try:
        say("Загружаю счета (crm.invoice)...")
        start = 0
        while True:
            res = bitrix_call("crm.invoice.list", {
                "select": ["ID","ACCOUNT_NUMBER","UF_DEAL_ID","UF_COMPANY_ID",
                           "PRICE","CURRENCY","DATE_INSERT","DATE_PAY_BEFORE",
                           "STATUS_ID","ORDER_TOPIC","UF_CONTACT_ID"],
                "order":  {"DATE_INSERT": "DESC"},
                "start":  start,
            })
            items = res if isinstance(res, list) else res.get("invoices", [])
            if not items:
                break
            for inv in items:
                bid = f"bx_inv_{inv.get('ID','')}"
                if bid in existing_ids:
                    continue
                # Map Bitrix status to PayCalendar status
                bstatus = str(inv.get("STATUS_ID", ""))
                status = "paid" if bstatus in ("P", "PAID", "5") else "pending"
                amount = float(inv.get("PRICE") or 0)
                vendor = inv.get("ORDER_TOPIC") or f"Клиент #{inv.get('UF_COMPANY_ID','?')}"
                due    = (inv.get("DATE_PAY_BEFORE") or "")[:10] or None
                issued = (inv.get("DATE_INSERT") or "")[:10] or date.today().isoformat()
                new_invs.append(build_invoice({
                    "is_invoice":   True,
                    "vendor":       vendor,
                    "amount":       amount,
                    "currency":     inv.get("CURRENCY", "EUR"),
                    "due_date":     due,
                    "issue_date":   issued,
                    "invoice_number": str(inv.get("ACCOUNT_NUMBER", "")),
                    "description":  inv.get("ORDER_TOPIC", ""),
                    "category":     "services",
                    "status":       status,
                }, bid, inv.get("ORDER_TOPIC","Bitrix счёт"),
                   "", issued, False, "bitrix"))
                new_invs[-1]["bitrix_id"] = bid
                new_invs[-1]["bitrix_raw_id"] = inv.get("ID")
            say(f"  Загружено: {len(items)} счетов (start={start})")
            if len(items) < 50:
                break
            start += 50
        say(f"✓ Счета: {len(new_invs)} новых", "ok")
    except Exception as ex:
        say(f"crm.invoice.list: {ex} — пробую crm.deal...", "warn")

    # ── Fetch deals as fallback (older Bitrix or deals-as-invoices) ───────────
    try:
        say("Загружаю сделки (crm.deal)...")
        deals_added = 0
        start = 0
        while True:
            res = bitrix_call("crm.deal.list", {
                "select": ["ID","TITLE","OPPORTUNITY","CURRENCY_ID",
                           "CLOSEDATE","DATE_CREATE","STAGE_ID",
                           "COMPANY_ID","CONTACT_ID","COMMENTS"],
                "filter": {"!STAGE_ID": ["LOSE", "FINAL_INVOICE"]},
                "order":  {"DATE_CREATE": "DESC"},
                "start":  start,
            })
            items = res if isinstance(res, list) else []
            if not items:
                break
            for deal in items:
                bid = f"bx_deal_{deal.get('ID','')}"
                if bid in existing_ids:
                    continue
                stage = str(deal.get("STAGE_ID", ""))
                status = "paid" if stage in ("WON","C7:WON") else "pending"
                amount = float(deal.get("OPPORTUNITY") or 0)
                if amount == 0:
                    continue  # skip zero deals
                vendor = deal.get("TITLE", "Сделка Bitrix")
                due    = (deal.get("CLOSEDATE") or "")[:10] or None
                issued = (deal.get("DATE_CREATE") or "")[:10] or date.today().isoformat()
                inv_obj = build_invoice({
                    "is_invoice":   True,
                    "vendor":       vendor,
                    "amount":       amount,
                    "currency":     deal.get("CURRENCY_ID", "EUR"),
                    "due_date":     due,
                    "issue_date":   issued,
                    "invoice_number": f"D-{deal.get('ID','')}",
                    "description":  deal.get("COMMENTS", "")[:100],
                    "category":     "services",
                    "status":       status,
                }, bid, vendor, "", issued, False, "bitrix_deal")
                inv_obj["bitrix_id"]     = bid
                inv_obj["bitrix_raw_id"] = deal.get("ID")
                inv_obj["source_type"]   = "deal"
                new_invs.append(inv_obj)
                deals_added += 1
            if len(items) < 50: break
            start += 50
        say(f"✓ Сделки: {deals_added} новых", "ok")
    except Exception as ex:
        say(f"crm.deal.list: {ex}", "warn")

    if new_invs:
        save_invoices(existing + new_invs)
        say(f"✅ Добавлено из Bitrix24: {len(new_invs)}", "ok")
    else:
        say("Новых записей не найдено", "info")
    return new_invs


def create_bitrix_invoice(inv: dict) -> dict:
    """Push a PayCalendar invoice back to Bitrix24 as invoice."""
    if not BITRIX_WEBHOOK:
        raise ValueError("Webhook не настроен")
    result = bitrix_call("crm.invoice.add", {"fields": {
        "ORDER_TOPIC":      inv.get("vendor", "Invoice"),
        "PRICE":            inv.get("amount", 0),
        "CURRENCY":         inv.get("currency", "EUR"),
        "DATE_PAY_BEFORE":  inv.get("due_date", ""),
        "DATE_INSERT":      inv.get("issue_date", date.today().isoformat()),
        "STATUS_ID":        "P" if inv.get("status") == "paid" else "N",
        "COMMENTS":         inv.get("description", ""),
    }})
    log.info(f"Bitrix invoice created: {result}")
    return result



# ═══════════════════════════════════════════════════════════════════════════════
#  MULTI-ACCOUNT EMAIL SUPPORT
#  Scan multiple email accounts for invoices
# ═══════════════════════════════════════════════════════════════════════════════

def load_email_accounts() -> list:
    """Load all configured email accounts from config + env vars."""
    accounts = []
    
    # Primary account (always first)
    if EMAIL_ADDR and EMAIL_PASS:
        accounts.append({
            "id":    "primary",
            "email": EMAIL_ADDR,
            "password": EMAIL_PASS,
            "imap":  IMAP_HOST,
            "port":  IMAP_PORT,
            "label": EMAIL_ADDR.split("@")[0],
        })
    
    # Additional accounts from config: [email_2], [email_3], ...
    for i in range(2, 11):
        addr = c("email", f"address_{i}", "") or os.environ.get(f"PC_EMAIL_ADDRESS_{i}", "")
        pwd  = c("email", f"password_{i}", "") or os.environ.get(f"PC_EMAIL_PASSWORD_{i}", "")
        if addr and pwd:
            host = c("email", f"imap_host_{i}", "imap.zone.eu") or os.environ.get(f"PC_EMAIL_IMAP_HOST_{i}", "imap.zone.eu")
            accounts.append({
                "id":    f"account_{i}",
                "email": addr,
                "password": pwd,
                "imap":  host,
                "port":  993,
                "label": addr.split("@")[0],
            })
    
    return accounts


def save_email_account(idx: int, email: str, password: str, imap_host: str = "imap.zone.eu"):
    """Save an additional email account to config."""
    if idx < 2 or idx > 10:
        raise ValueError("Account index must be 2-10")
    save_config_value("email", f"address_{idx}",  email)
    save_config_value("email", f"password_{idx}", password)
    save_config_value("email", f"imap_host_{idx}", imap_host or "imap.zone.eu")
    log.info(f"Saved email account #{idx}: {email}")


def delete_email_account(idx: int):
    """Remove an additional email account."""
    for key in [f"address_{idx}", f"password_{idx}", f"imap_host_{idx}"]:
        save_config_value("email", key, "")
    log.info(f"Deleted email account #{idx}")


def scan_account(account: dict, emit, from_date=None, to_date=None) -> list:
    """Scan a single email account. Returns new invoices."""
    label = account["email"]
    emit(f"📧 Аккаунт: {label}", "info")

    # Temporarily override globals for this account
    orig_addr = os.environ.get("_scan_addr", EMAIL_ADDR)
    orig_pass = os.environ.get("_scan_pass", EMAIL_PASS)
    orig_host = os.environ.get("_scan_host", IMAP_HOST)

    # Use a custom scan_imap that uses account credentials
    alt_hosts = list(dict.fromkeys([
        account["imap"],
        "imap.zone.eu",
        f"imap.{account['email'].split('@')[1]}" if '@' in account['email'] else "",
    ]))
    alt_hosts = [h for h in alt_hosts if h]

    mail, connected_host = _try_imap_connect(
        alt_hosts, account["port"],
        account["email"], account["password"]
    )
    if not mail:
        emit(f"  ✗ Не удалось подключиться к {label}", "err")
        return []

    emit(f"  ✓ Подключён к {connected_host}", "ok")

    try:
        mail.select(IMAP_FOLDER)
        date_terms = []
        if from_date:
            from datetime import datetime as _dt
            since_str = _dt.strptime(from_date, "%Y-%m-%d").strftime("%d-%b-%Y")
            date_terms.append(f"SINCE {since_str}".encode())
        if to_date:
            from datetime import datetime as _dt2, timedelta
            before_dt = _dt2.strptime(to_date, "%Y-%m-%d") + timedelta(days=1)
            date_terms.append(f"BEFORE {before_dt.strftime('%d-%b-%Y')}".encode())

        ids = set()
        # If date range given — use IMAP SINCE/BEFORE (no limit, fetch all in range)
        if date_terms:
            try:
                _, data = mail.search(None, *date_terms)
                for uid in (data[0].split() if data and data[0] else []):
                    ids.add(uid)
                emit(f"  📬 {len(ids)} писем в диапазоне дат", "ok")
            except Exception as ex:
                emit(f"  ⚠ Дата-фильтр: {ex}", "warn")

        # ALL emails strategy — get last SCAN_LIMIT regardless of date
        try:
            _, _ad = mail.search(None, b"ALL")
            _all_uids = _ad[0].split() if (_ad and _ad[0]) else []
            try:
                _sorted = sorted(_all_uids,
                    key=lambda x: int(x.decode() if isinstance(x,bytes) else x),
                    reverse=True)
            except Exception:
                _sorted = list(reversed(_all_uids))
            # If no date filter — apply limit; otherwise take all date-matched + last SCAN_LIMIT
            limit = SCAN_LIMIT if not date_terms else min(SCAN_LIMIT, len(_all_uids))
            for uid in _sorted[:limit]:
                ids.add(uid)
            emit(f"  📬 {len(ids)}/{len(_all_uids)} писем всего", "ok")
        except Exception as ex:
            emit(f"  ⚠ ALL: {ex} — fallback keyword", "warn")
            for term in IMAP_SUBJECTS:
                try:
                    _, data = mail.search(None, term)
                    if data and data[0]:
                        for uid in data[0].split(): ids.add(uid)
                except Exception: pass

        ids_sorted = sorted(ids, key=lambda x: int(x.decode() if isinstance(x,bytes) else x), reverse=True)
        # Don't apply limit if we have a date filter (already bounded by date range)
        if not date_terms:
            ids_sorted = ids_sorted[:SCAN_LIMIT]
        emit(f"  Писем для анализа: {len(ids)}", "info")
        # Reset progress bar to 0 for this account's header-loading phase
        emit("__progress__ 0 0 0 0", "progress")

        emails_raw  = []
        ids_list_ac = list(ids)
        total_ac    = len(ids_list_ac)
        for _hi_ac, uid in enumerate(ids_list_ac):
            try:
                _, data = mail.fetch(uid, "(RFC822.HEADER)")
                if not data or not data[0]: continue
                msg  = email.message_from_bytes(data[0][1])
                subj = decode_header(msg.get("Subject",""))
                sndr = decode_header(msg.get("From",""))
                ct   = msg.get("Content-Type","").lower()
                att  = "multipart/mixed" in ct or "multipart/related" in ct or "application/" in ct
                try:
                    ds = parsedate_to_datetime(msg.get("Date","")).strftime("%Y-%m-%d")
                except Exception:
                    ds = date.today().isoformat()
                uid_s = uid.decode() if isinstance(uid, bytes) else str(uid)
                emails_raw.append({
                    "id": f"{account['id']}_{uid_s}",
                    "subject": subj, "from": sndr,
                    "body": "", "date": ds, "has_attachment": att,
                    "account": account["email"],
                })
            except Exception as ex:
                emit(f"  Header error {uid}: {ex}", "err")
            if (_hi_ac + 1) % 200 == 0 or _hi_ac + 1 == total_ac:
                emit(f"  📥 Заголовки: {_hi_ac+1}/{total_ac}...", "info")

        _mail_ref = [mail]  # mutable ref for reconnect inside closure
        _mail_lock = threading.Lock()  # Thread safety for shared IMAP connection

        def _fetch_pdf(uid_str):
            nonlocal mail
            try:
                real_uid = uid_str.split("_")[-1].encode()
                # Thread-safe IMAP fetch with timeout to prevent deadlock
                if not _mail_lock.acquire(timeout=30):
                    log.warning(f"IMAP lock timeout for {uid_str}, skipping")
                    return ""
                try:
                    try:
                        _mail_ref[0].socket().settimeout(20)
                    except Exception:
                        pass
                    try:
                        _, data = _mail_ref[0].fetch(real_uid, "(RFC822)")
                    except Exception:
                        log.debug(f"scan_account: IMAP reconnect for {uid_str}")
                        new_mail, _ = _try_imap_connect(alt_hosts, account["port"],
                                                        account["email"], account["password"])
                        if new_mail:
                            new_mail.select(IMAP_FOLDER)
                            _mail_ref[0] = new_mail
                            mail = new_mail
                            _, data = _mail_ref[0].fetch(real_uid, "(RFC822)")
                        else:
                            return ""
                finally:
                    _mail_lock.release()
                if not data or not data[0]: return ""
                raw_msg = email.message_from_bytes(data[0][1])
                body_text = get_plain_body(raw_msg)
                pdf_texts = []
                for part in raw_msg.walk():
                    ct = part.get_content_type() or ""
                    fn = part.get_filename() or ""
                    if "pdf" in ct.lower() or fn.lower().endswith(".pdf"):
                        try:
                            pdf_bytes = part.get_payload(decode=True)
                            if pdf_bytes:
                                pt = extract_pdf_text(pdf_bytes)
                                if pt: pdf_texts.append(pt)
                        except Exception: pass
                result = body_text
                if pdf_texts: result += "\n--- PDF ---\n" + "\n".join(pdf_texts)
                return result
            except Exception: return ""

        result = process_emails(emails_raw, emit, fetch_body=_fetch_pdf, source=f"imap:{account['email']}")
        try: mail.logout()
        except Exception: pass
        return result if isinstance(result, list) else []

    except Exception as ex:
        emit(f"  ✗ {label}: {ex}", "err")
        try: mail.logout()
        except Exception: pass
        return []



# ── Global state for background scan worker ──────────────────────────────────
import threading as _thr_global
_scan_state_lock = _thr_global.Lock()
_scan_state_live = {
    "running": False, "queued": False, "task_id": None,
    "total": 0, "done": 0, "found": 0, "speed": 0.0, "eta": 0,
    "pct": 0, "log": [], "from_date": None, "to_date": None,
    "started": None, "finished": None, "error": None,
}
_MAX_LOG = 80

# ═══════════════════════════════════════════════════════════════════════════════
#  ROUTES
# ═══════════════════════════════════════════════════════════════════════════════



@app.route("/api/setup-zone", methods=["GET","POST"])
def api_setup_zone():
    """Force correct Zone.ee IMAP settings - no auth needed."""
    global IMAP_HOST
    save_config_value("email", "imap_host", "imap.zone.eu")
    save_config_value("email", "imap_port", "993")
    IMAP_HOST = "imap.zone.eu"
    reload_config()
    log.info("Zone.ee IMAP host fixed: imap.zone.eu")
    return jsonify({
        "ok": True,
        "message": "IMAP хост установлен: imap.zone.eu",
        "imap_host": IMAP_HOST
    })



@app.route("/api/version")  
def api_version():
    return jsonify({
        "version": "2026-04-04-v12",
        "imap_host": IMAP_HOST,
        "imap_env": os.environ.get("PC_EMAIL_IMAP_HOST","not_set"),
        "gemini_key": GEMINI_KEY[:15]+"..." if GEMINI_KEY else "none",
        "provider": _active_provider() or "keyword",
        "data_dir": str(_DATA_DIR),
    })

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
    skip = ("/api/auth-check", "/api/stats", "/health", "/api/test-scan", "/api/diagnose", "/api/setup-zone", "/api/keys", "/api/keys/add", "/api/version")
    if request.path.startswith("/api/") and request.path not in skip:
        if not _check_token():
            return jsonify({"error": "Не авторизован"}), 401

@app.route("/api/auth-check", methods=["POST"])
def api_auth_check():
    access_key = os.environ.get("ACCESS_KEY", "")
    if not access_key:
        return jsonify({"ok": True})
    key = (request.json or {}).get("key", "")
    return jsonify({"ok": key == access_key})


@app.route("/keys")
def keys_page():
    if not _check_token():
        return redirect("/login-page?next=/keys")
    return send_from_directory(TMPL_DIR, "keys.html")


@app.route("/rescan")
def rescan_page():
    if not _check_token():
        return redirect("/login-page?next=/rescan")
    return send_from_directory(TMPL_DIR, "rescan.html")


@app.route("/api/invoices/fix-offers", methods=["POST"])
def api_fix_offers():
    """Move items with invoice keywords from offers to regular invoices, remove offer-word duplicates."""
    invs = load_invoices()
    before = len(invs)

    OFFER_KW   = ["offer","quote","quotation","proposal","предложение","hinnapakkumine"]
    INVOICE_KW = ["arve","invoice","rechnung","счёт","счет","lasku","faktura","bill","payment"]

    moved = 0
    deduped = 0
    seen_desc = {}

    cleaned = []
    for inv in invs:
        desc = (inv.get("description") or "").lower()
        has_offer   = any(k in desc for k in OFFER_KW)
        has_invoice = any(k in desc for k in INVOICE_KW)
        amount      = float(inv.get("amount") or 0)

        # DELETE: zero-amount items with offer keyword and Re: thread prefix
        if has_offer and not has_invoice and amount == 0:
            import re as _re2
            is_reply = bool(_re2.match(r"^(re:|fw:|fwd:)", desc.strip(), _re2.IGNORECASE))
            if is_reply:
                deduped += 1
                continue  # drop silently

        # Move invoice-keyword items out of "offer" status
        if has_invoice and inv.get("is_offer"):
            inv.pop("is_offer", None)
            moved += 1

        # Move offer-only items (non-invoice) to offer tab
        if has_offer and not has_invoice and not inv.get("is_offer"):
            inv["is_offer"] = True
            moved += 1

        # Deduplicate remaining offer threads by normalised subject
        norm = re.sub(r"^(re:|fwd:|fw:)\s*", "", desc, flags=re.IGNORECASE).strip()[:60]
        if inv.get("is_offer"):
            if norm in seen_desc:
                deduped += 1
                continue
            seen_desc[norm] = True

        cleaned.append(inv)

    save_invoices(cleaned)
    log.info(f"fix-offers: moved={moved}, deduped={deduped}")
    return jsonify({"ok":True, "moved":moved, "deduped":deduped,
                    "before":before, "after":len(cleaned)})

@app.route("/api/invoices/cleanup", methods=["POST"])
def api_invoices_cleanup():
    """Remove invoices with amount=0, no vendor, and no useful data."""
    invs = load_invoices()
    before = len(invs)
    cleaned = [i for i in invs if (
        float(i.get("amount") or 0) > 0 or
        i.get("status") == "paid" or  # keep paid even if 0
        bool(i.get("vendor","").strip())  # keep if has vendor
    )]
    save_invoices(cleaned)
    removed = before - len(cleaned)
    log.info(f"Cleanup: removed {removed} empty invoices")
    return jsonify({"ok": True, "removed": removed, "remaining": len(cleaned)})

@app.route("/api/invoices/delete-scan-date", methods=["POST"])
def api_delete_scan_date_invoices():
    """Delete invoices where issue_date equals the scan date (wrong dates from date-parsing bug)."""
    data      = request.json or {}
    scan_date = data.get("scan_date", "2026-04-05")  # date of the bad scan
    invs      = load_invoices()
    before    = len(invs)
    # Keep: paid invoices, invoices with issue_date != scan_date, older invoices with correct dates
    cleaned = [i for i in invs if (
        i.get("status") == "paid" or
        i.get("issue_date","")[:10] != scan_date or
        i.get("added_at","")[:10] not in ("2026-04-04", "2026-04-05")
    )]
    removed = before - len(cleaned)
    save_invoices(cleaned)
    log.info(f"Deleted {removed} invoices with scan-date issue_date={scan_date}")
    return jsonify({"ok": True, "removed": removed, "remaining": len(cleaned)})

@app.route("/api/invoices/fix-amounts", methods=["POST"])
def api_invoices_fix_amounts():
    """Re-extract amounts for zero-amount invoices using regex on description/subject."""
    invs   = load_invoices()
    fixed  = 0
    for inv in invs:
        if float(inv.get("amount") or 0) > 0:
            continue
        # Try to extract amount from stored text fields
        text = " ".join(filter(None, [
            inv.get("description", ""),
            inv.get("email_subject", ""),
            inv.get("vendor", ""),
            inv.get("invoice_number", ""),
        ]))
        amount = extract_amount(text)
        if amount > 0:
            inv["amount"]   = amount
            inv["currency"] = inv.get("currency") or extract_currency(text) or "EUR"
            fixed += 1
    save_invoices(invs)
    log.info(f"Fix amounts: recovered {fixed} amounts")
    return jsonify({"ok": True, "fixed": fixed, "total": len(invs)})

@app.route("/api/invoices", methods=["GET"])
def api_get_invoices():
    invs = load_invoices()
    # Dynamically check PDF existence on disk + Drive link
    # so invoices scanned before PDF feature also show correct status
    for inv in invs:
        inv_id = inv.get("id","")
        if inv_id:
            # Check local PDF
            inv["has_pdf"] = has_pdf(inv_id)
            if not inv.get("gdrive_link"):
                gdrive = get_gdrive_link(inv_id)
                if gdrive:
                    inv["gdrive_link"] = gdrive
            # Check Drive link
            gdrive = get_gdrive_link(inv_id)
            if gdrive:
                inv["gdrive_link"] = gdrive
    return jsonify(invs)

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
    if not _check_token(): return redirect("/login-page?next=/debug-log")
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



@app.route("/api/scan-status")
def api_scan_status():
    """Poll-based scan progress - works even after browser reconnect."""
    try:
        with _scan_state_lock:
            state = dict(_scan_state_live)
            state["log"] = list(_scan_state_live.get("log") or [])

        if not state.get("running") and not state.get("started"):
            try:
                pf = _DATA_DIR / "scan_progress.json"
                if pf.exists():
                    with open(pf, encoding="utf-8") as f:
                        state = json.load(f)
                        state["log"] = state.get("log") or []
            except Exception:
                pass

        state["invoices_total"] = len(load_invoices())
        return jsonify(state)
    except Exception as ex:
        return jsonify({"running": False, "error": str(ex), "log": [], "pct": 0})

@app.route("/api/scan-start", methods=["POST"])
def api_scan_start():
    """Start background scan. Returns immediately with task_id."""
    data      = request.json or {}
    from_date = data.get("from_date") or data.get("from")
    to_date   = data.get("to_date")   or data.get("to")
    quick     = data.get("quick", False)

    with _scan_state_lock:
        if _scan_state_live["running"]:
            return jsonify({
                "ok": False,
                "error": "Скан уже выполняется",
                "task_id": _scan_state_live["task_id"],
                "running": True,
            })

    task_id = start_bg_scan(from_date=from_date, to_date=to_date, quick=quick)
    if not task_id:
        return jsonify({"ok": False, "error": "Не удалось запустить"})
    return jsonify({
        "ok": True, "task_id": task_id,
        "message": f"Скан запущен в фоне. Закрой страницу — скан продолжится.",
        "poll": "/api/scan-status",
    })

@app.route("/api/scan-stop", methods=["POST"])
def api_scan_stop():
    """Stop running scan."""
    try:
        if scan_lock.locked(): scan_lock.release()
    except Exception:
        pass
    with _scan_state_lock:
        _scan_state_live["running"] = False
    _clear_pending_scan()
    return jsonify({"ok": True})




@app.route("/api/email-accounts", methods=["GET"])
def api_email_accounts():
    """List all configured email accounts."""
    accounts = load_email_accounts()
    # Mask passwords
    safe = []
    for acc in accounts:
        safe.append({
            "id":    acc["id"],
            "email": acc["email"],
            "imap":  acc["imap"],
            "label": acc["label"],
            "has_password": bool(acc.get("password")),
        })
    return jsonify(safe)


@app.route("/api/email-accounts/add", methods=["POST"])
def api_email_accounts_add():
    """Add a new email account."""
    data  = request.json or {}
    email_addr = data.get("email","").strip()
    pwd        = data.get("password","").strip()
    imap_host  = data.get("imap","").strip() or "imap.zone.eu"
    if not email_addr or not pwd:
        return jsonify({"ok":False,"error":"Email и пароль обязательны"})
    # Find next free slot
    accounts = load_email_accounts()
    next_idx = len(accounts) + 1
    if next_idx > 10:
        return jsonify({"ok":False,"error":"Максимум 10 аккаунтов"})
    # Test connection first
    try:
        import imaplib as _il2, ssl as _ssl3
        alt = [imap_host, "imap.zone.eu"]
        mail, host = _try_imap_connect(alt, 993, email_addr, pwd)
        if not mail:
            return jsonify({"ok":False,"error":"Не удалось подключиться к IMAP — проверь данные"})
        mail.logout()
        log.info(f"IMAP test OK for {email_addr}")
    except Exception as ex:
        return jsonify({"ok":False,"error":f"IMAP ошибка: {ex}"})
    save_email_account(next_idx, email_addr, pwd, imap_host)
    return jsonify({"ok":True,"idx":next_idx,"email":email_addr,"imap":imap_host})


@app.route("/api/email-accounts/delete", methods=["POST"])
def api_email_accounts_delete():
    """Remove an email account by index."""
    idx = int((request.json or {}).get("idx", 0))
    if idx < 2:
        return jsonify({"ok":False,"error":"Нельзя удалить основной аккаунт"})
    delete_email_account(idx)
    return jsonify({"ok":True})


@app.route("/api/email-accounts/test", methods=["POST"])
def api_email_accounts_test():
    """Test IMAP connection for given credentials."""
    data  = request.json or {}
    email_addr = data.get("email","").strip()
    pwd        = data.get("password","").strip()
    imap_host  = data.get("imap","").strip() or "imap.zone.eu"
    try:
        mail, host = _try_imap_connect([imap_host, "imap.zone.eu"], 993, email_addr, pwd)
        if not mail:
            return jsonify({"ok":False,"error":"Подключение не удалось"})
        mail.select("INBOX")
        _, data2 = mail.search(None, "ALL")
        total = len(data2[0].split()) if data2[0] else 0
        mail.logout()
        return jsonify({"ok":True,"total":total,"host":host,"email":email_addr})
    except Exception as ex:
        return jsonify({"ok":False,"error":str(ex)[:100]})


@app.route("/api/bitrix/sync", methods=["POST"])
def api_bitrix_sync():
    """Sync invoices from Bitrix24 CRM."""
    import threading as _thr
    import queue as _q

    q = _q.Queue()
    sentinel = object()
    cnt = [0]

    def emit(msg, t="info"):
        q.put((msg, t))

    def run():
        try:
            result = sync_bitrix_invoices(emit)
            cnt[0] = len(result)
        except Exception as ex:
            emit(f"❌ {ex}", "err")
        q.put(sentinel)

    _thr.Thread(target=run, daemon=True).start()

    def generate():
        while True:
            try:
                item = q.get(timeout=30)
            except Exception:
                yield f"data: {json.dumps({'done':True,'count':cnt[0]})}\n\n"
                return
            if item is sentinel:
                yield f"data: {json.dumps({'done':True,'count':cnt[0]})}\n\n"
                return
            msg, t = item
            ts = datetime.now().strftime("%H:%M:%S")
            yield f"data: {json.dumps({'msg':msg,'type':t,'ts':ts})}\n\n"

    return Response(stream_with_context(generate()),
                    content_type="text/event-stream",
                    headers={"Cache-Control":"no-cache","X-Accel-Buffering":"no"})


@app.route("/api/bitrix/status")
def api_bitrix_status():
    """Check Bitrix24 connection."""
    reload_config()
    webhook = c("bitrix","webhook_url") or os.environ.get("BITRIX_WEBHOOK","")
    if not webhook:
        return jsonify({"ok":False,"error":"Webhook не настроен"})
    try:
        res = bitrix_call("app.info")
        return jsonify({"ok":True,"info":str(res)[:100],"webhook":webhook[:40]+"..."})
    except Exception as ex:
        return jsonify({"ok":False,"error":str(ex)[:100]})


@app.route("/api/bitrix/save-webhook", methods=["POST"])
def api_bitrix_save_webhook():
    """Save Bitrix24 webhook URL."""
    global BITRIX_WEBHOOK
    url = (request.json or {}).get("url","").strip()
    if not url:
        return jsonify({"ok":False,"error":"URL пустой"})
    if "bitrix24" not in url and "bitrix" not in url.lower():
        return jsonify({"ok":False,"error":"Не похоже на Bitrix24 webhook URL"})
    save_config_value("bitrix","webhook_url",url)
    BITRIX_WEBHOOK = url
    return jsonify({"ok":True,"saved":url[:50]+"..."})


@app.route("/api/bitrix/push", methods=["POST"])
def api_bitrix_push():
    """Push a PayCalendar invoice to Bitrix24."""
    inv_id = (request.json or {}).get("id","")
    invs = load_invoices()
    inv  = next((i for i in invs if i.get("id") == inv_id), None)
    if not inv:
        return jsonify({"ok":False,"error":"Счёт не найден"})
    try:
        result = create_bitrix_invoice(inv)
        return jsonify({"ok":True,"bitrix_result":str(result)[:100]})
    except Exception as ex:
        return jsonify({"ok":False,"error":str(ex)})



@app.route("/api/stats")
def api_stats():
    invs  = load_invoices()
    today = date.today()
    pend  = [i for i in invs if i.get("status") != "paid"]
    over  = [i for i in pend if i.get("due_date") and date.fromisoformat(i["due_date"]) < today]
    urg   = [i for i in pend if i.get("due_date") and
             0 <= (date.fromisoformat(i["due_date"]) - today).days <= WARN_DAYS]
    return jsonify({
        "total":len(invs), "pending":len(pend), "overdue":len(over), "urgent":len(urg),
        "sum_pending": round(sum(float(i.get("amount",0)) for i in pend), 2),
        "sum_paid":    round(sum(float(i.get("amount",0)) for i in invs if i.get("status")=="paid"), 2),
    })


@app.route("/api/config")
def api_config():
    reload_config()
    pw  = EMAIL_PASS or ""
    key = API_KEY or ""
    provider = _active_provider()
    return jsonify({
        "company":           COMPANY,
        "email":             EMAIL_ADDR,
        "password":          pw[:2]+"***" if pw else "",
        "warn_days":         WARN_DAYS,
        "auto_scan_minutes": AUTO_SCAN,
        "has_password":      bool(pw and pw not in ("your_password_here","")),
        "has_api_key":       bool(key and "INSERT" not in key and len(key)>20),
        "imap_host":         IMAP_HOST or "imap.zone.eu",
        "ai_provider":       provider or "keyword",
        "has_gemini":        bool(GEMINI_KEY),
        "has_groq":          bool(GROQ_KEY),
        "ui_theme":          c("ui", "theme", ""),  # user's saved theme from Volume
    })


@app.route("/api/scan-state")
def api_scan_state():
    st   = load_state()
    invs = load_invoices()
    ld   = st.get("last_date")
    sc   = st.get("scan_count", 0)
    return jsonify({
        "scan_count":         sc,
        "last_date":          ld,
        "last_uid":           st.get("last_uid"),
        "scanned_uids_count": len(st.get("scanned_uids", [])),
        "total_invoices":     len(invs),
        "next_scan_from":     ld if ld else ("начало" if sc == 0 else "все письма"),
        "data_dir":           str(_DATA_DIR),
    })

@app.route("/api/reset-scan", methods=["POST"])
def api_reset_scan():
    """Reset scan state. If clear_invoices=true, also clears all invoices."""
    try:
        data = request.json or {}
        empty = {"last_uid": None, "last_date": None, "scan_count": 0, "scanned_uids": []}
        save_state(empty)
        cleared_invoices = 0
        if data.get("clear_invoices"):
            existing = load_invoices()
            cleared_invoices = len(existing)
            save_invoices([])
            log.info(f"Cleared {cleared_invoices} invoices for full rescan")
        log.info("Scan state reset - will rescan from beginning")
        return jsonify({"ok": True, "message": "Состояние сброшено", "cleared_invoices": cleared_invoices})
    except Exception as ex:
        return jsonify({"ok": False, "error": str(ex)})




@app.route("/api/backup/export", methods=["GET"])
def api_backup_export():
    """Download full backup: invoices + config as a single JSON file."""
    import datetime as _dt
    backup = {
        "version": 2,
        "exported_at": _dt.datetime.utcnow().isoformat() + "Z",
        "invoices": load_invoices(),
        "config": {},
    }
    # Include config sections
    cfg = _load_cfg()
    for section in cfg.sections():
        backup["config"][section] = dict(cfg[section])
    # Include scan state if present
    if STATE_FILE.exists():
        try:
            backup["scan_state"] = json.loads(STATE_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
    ts = _dt.datetime.utcnow().strftime("%Y%m%d_%H%M")
    filename = f"paycalendar_backup_{ts}.json"
    resp = Response(
        json.dumps(backup, ensure_ascii=False, indent=2),
        mimetype="application/json",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )
    return resp


@app.route("/api/backup/import", methods=["POST"])
def api_backup_import():
    """Restore from backup JSON. Merges: new invoices added, existing kept."""
    try:
        data = request.json or {}
        if "invoices" not in data:
            return jsonify({"ok": False, "error": "Неверный формат — нет поля invoices"}), 400
        incoming = data["invoices"]
        if not isinstance(incoming, list):
            return jsonify({"ok": False, "error": "invoices должен быть массивом"}), 400

        mode = data.get("mode", "merge")  # merge | replace
        if mode == "replace":
            save_invoices(incoming)
            added = len(incoming)
            skipped = 0
        else:
            # merge: add only invoices missing from current
            current = load_invoices()
            current_ids = {i["id"] for i in current}
            new_only = [i for i in incoming if i.get("id") and i["id"] not in current_ids]
            if new_only:
                save_invoices(current + new_only)
            added = len(new_only)
            skipped = len(incoming) - added

        # Restore config if present
        if "config" in data and isinstance(data["config"], dict):
            cfg = _load_cfg()
            for section, kv in data["config"].items():
                if not cfg.has_section(section):
                    cfg.add_section(section)
                for k, v in kv.items():
                    cfg.set(section, k, str(v))
            with open(CONFIG_FILE, "w", encoding="utf-8") as f:
                cfg.write(f)

        return jsonify({"ok": True, "added": added, "skipped": skipped,
                        "total": len(load_invoices())})
    except Exception as ex:
        log.exception("backup import error")
        return jsonify({"ok": False, "error": str(ex)}), 500


@app.route("/api/invoice/<inv_id>/pdf")
def api_serve_pdf(inv_id):
    """Serve PDF: local file → Drive download → Drive redirect."""
    # 1. Try local file (fallback storage)
    local_path = get_pdf_path(inv_id)
    if local_path:
        from flask import send_file as _sf
        return _sf(str(local_path), mimetype="application/pdf",
                   as_attachment=False,
                   download_name=local_path.name)

    # 2. Try downloading from Google Drive by file ID
    gdrive_id = get_gdrive_id(inv_id)
    if gdrive_id:
        try:
            from googleapiclient.http import MediaIoBaseDownload
            import io as _io
            svc = _get_gdrive_service()
            if svc:
                request = svc.files().get_media(fileId=gdrive_id)
                buf = _io.BytesIO()
                downloader = MediaIoBaseDownload(buf, request)
                done = False
                while not done:
                    _, done = downloader.next_chunk()
                buf.seek(0)
                invs = load_invoices()
                inv  = next((i for i in invs if i.get("id") == inv_id), {})
                fname = inv.get("pdf_filename") or f"{inv_id}.pdf"
                from flask import send_file as _sf
                return _sf(buf, mimetype="application/pdf",
                           as_attachment=False, download_name=fname)
        except Exception as ex:
            log.warning(f"Drive download {inv_id}: {ex}")

    # 3. Redirect to Drive view link
    drive_link = get_gdrive_link(inv_id)
    if drive_link:
        from flask import redirect as _red
        return _red(drive_link)

    return jsonify({"error": "PDF не найден — не загружен в Drive"}), 404


@app.route("/api/keys", methods=["GET"])
def api_keys_status():
    """Show key pool status with truncated key previews."""
    def pool_info(pool):
        s = pool.status()
        s["keys"] = [k[:16]+"..." for k in pool.keys]
        s["has_keys"] = len(pool.keys) > 0
        return s
    return jsonify({
        "gemini":          pool_info(_gemini_pool),
        "groq":            pool_info(_groq_pool),
        "openai":          pool_info(_openai_pool),
        "claude_key":      bool(API_KEY and len(API_KEY) > 20),
        "active_provider": _active_provider() or "keyword",
    })

@app.route("/api/keys/add", methods=["POST"])
def api_keys_add():
    """Add API key to pool AND persist to Volume config.ini."""
    global GEMINI_KEY, GROQ_KEY, OPENAI_KEY, API_KEY
    data     = request.json or {}
    provider = data.get("provider", "").lower()
    key      = data.get("key", "").strip()
    if not key or len(key) < 10:
        return jsonify({"ok": False, "error": "Ключ слишком короткий"})

    if provider == "gemini":
        _gemini_pool.add(key)
        GEMINI_KEY = key  # update runtime global
        n = len(_gemini_pool.keys)
        cfg_key = f"gemini_key_{n}" if n > 1 else "gemini_key"
        save_config_value("ai", cfg_key, key)
        log.info(f"Gemini key #{n} saved to Volume config")
        return jsonify({"ok": True, "provider": "gemini", "pool": _gemini_pool.status()})

    elif provider == "groq":
        _groq_pool.add(key)
        GROQ_KEY = key
        n = len(_groq_pool.keys)
        cfg_key = f"groq_key_{n}" if n > 1 else "groq_key"
        save_config_value("ai", cfg_key, key)
        log.info(f"Groq key #{n} saved to Volume config")
        return jsonify({"ok": True, "provider": "groq", "pool": _groq_pool.status()})

    elif provider == "openai":
        _openai_pool.add(key)
        OPENAI_KEY = key
        n = len(_openai_pool.keys)
        cfg_key = f"openai_key_{n}" if n > 1 else "openai_key"
        save_config_value("ai", cfg_key, key)
        log.info(f"OpenAI key #{n} saved to Volume config")
        return jsonify({"ok": True, "provider": "openai", "pool": _openai_pool.status()})

    elif provider == "claude":
        API_KEY = key
        save_config_value("claude", "api_key", key)
        log.info("Claude API key saved to Volume config")
        return jsonify({"ok": True, "provider": "claude"})

    return jsonify({"ok": False, "error": f"Неизвестный провайдер: {provider}"})





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



# ── Google Drive OAuth Routes ─────────────────────────────────────────────────

@app.route("/api/gdrive/auth-url")
def api_gdrive_auth_url():
    """Generate Google OAuth URL for Drive access."""
    client_id     = c("gdrive","client_id","")     or os.environ.get("GDRIVE_CLIENT_ID","")
    client_secret = c("gdrive","client_secret","") or os.environ.get("GDRIVE_CLIENT_SECRET","")
    if not client_id or not client_secret:
        return jsonify({"ok":False,"error":"Нужны Client ID и Client Secret от Google Cloud Console"})
    redirect_uri = "https://paycalendar-production.up.railway.app/api/gdrive/callback"
    from urllib.parse import urlencode
    params = {
        "client_id":     client_id,
        "redirect_uri":  redirect_uri,
        "response_type": "code",
        "scope":         " ".join(["https://www.googleapis.com/auth/drive.file","https://www.googleapis.com/auth/gmail.readonly","https://www.googleapis.com/auth/gmail.insert"]),
        "access_type":   "offline",
        "prompt":        "consent",
    }
    url = "https://accounts.google.com/o/oauth2/v2/auth?" + urlencode(params)
    return jsonify({"ok":True,"url":url})


@app.route("/api/gdrive/callback")
def api_gdrive_callback():
    """Handle OAuth callback, exchange code for token."""
    code  = request.args.get("code","")
    error = request.args.get("error","")
    if error:
        return f"<h2>Ошибка: {error}</h2><a href='/keys'>← Назад</a>"
    client_id     = c("gdrive","client_id","")     or os.environ.get("GDRIVE_CLIENT_ID","")
    client_secret = c("gdrive","client_secret","") or os.environ.get("GDRIVE_CLIENT_SECRET","")
    redirect_uri  = "https://paycalendar-production.up.railway.app/api/gdrive/callback"
    try:
        r = requests.post("https://oauth2.googleapis.com/token", data={
            "code":          code,
            "client_id":     client_id,
            "client_secret": client_secret,
            "redirect_uri":  redirect_uri,
            "grant_type":    "authorization_code",
        }, timeout=15)
        token = r.json()
        token["client_id"]     = client_id
        token["client_secret"] = client_secret
        save_config_value("gdrive", "token", json.dumps(token))
        # Reset cached service
        global _gdrive_service
        _gdrive_service = None
        # Verify it actually works
        svc = _get_gdrive_service(force_refresh=True)
        if svc:
            try:
                about = svc.about().get(fields="user").execute()
                email = about.get("user",{}).get("emailAddress","")
            except Exception:
                email = "подключено"
        else:
            email = ""
        return f"""<html>
<head><meta charset="UTF-8">
<script>
  // Notify opener and close this window
  if(window.opener) {{
    window.opener.postMessage({{gdrive:'connected',email:'{email}'}}, '*');
    setTimeout(function(){{ window.close(); }}, 1500);
  }} else {{
    setTimeout(function(){{ window.location='/keys'; }}, 2000);
  }}
</script>
<body style="font-family:sans-serif;background:#0f1117;color:#e2e8f0;display:flex;align-items:center;justify-content:center;height:100vh;margin:0">
<div style="text-align:center">
  <div style="font-size:60px">✅</div>
  <h2 style="margin:12px 0">Google Drive подключён!</h2>
  <p style="color:#6b7280">{email}</p>
  <p style="color:#4ade80;font-size:13px">Окно закроется автоматически...</p>
  <a href="/keys" style="color:#60a5fa">← Вернуться в настройки</a>
</div></body></html>"""
    except Exception as ex:
        return f"<h2>Ошибка: {ex}</h2><a href='/keys'>← Назад</a>"


@app.route("/api/gdrive/save-credentials", methods=["POST"])
def api_gdrive_save_credentials():
    """Save Google API credentials."""
    data = request.json or {}
    cid  = data.get("client_id","").strip()
    csec = data.get("client_secret","").strip()
    if not cid or not csec:
        return jsonify({"ok":False,"error":"Client ID и Client Secret обязательны"})
    save_config_value("gdrive","client_id",cid)
    save_config_value("gdrive","client_secret",csec)
    return jsonify({"ok":True})


@app.route("/api/gdrive/status")
def api_gdrive_status():
    """Check Google Drive connection status."""
    service = _get_gdrive_service(force_refresh=True)
    if not service:
        return jsonify({"ok":False,"connected":False})
    try:
        about = service.about().get(fields="user,storageQuota").execute()
        user  = about.get("user",{})
        return jsonify({
            "ok":True,"connected":True,
            "email":   user.get("emailAddress",""),
            "name":    user.get("displayName",""),
        })
    except Exception as ex:
        return jsonify({"ok":False,"connected":False,"error":str(ex)})


@app.route("/api/gdrive/upload-existing", methods=["POST"])
def api_gdrive_upload_existing():
    """Upload all existing local PDFs to Google Drive."""
    service = _get_gdrive_service()
    if not service:
        return jsonify({"ok":False,"error":"Google Drive не подключён"})
    invs = load_invoices()
    uploaded = 0; failed = 0
    for inv in invs:
        inv_id = inv.get("id","")
        path   = get_pdf_path(inv_id)
        if not path: continue
        if get_gdrive_link(inv_id): continue  # already uploaded
        try:
            pdf_bytes = path.read_bytes()
            fn        = inv.get("pdf_filename") or f"{inv_id}.pdf"
            dt        = inv.get("issue_date") or inv.get("date","")
            result    = gdrive_upload_pdf(pdf_bytes, fn, dt, is_offer=bool(inv.get("is_offer")))
            if result:
                link_file = _PDF_DIR / f"{re.sub(r'[^a-zA-Z0-9_-]','_',inv_id)[:80]}.gdrive"
                link_file.write_text(result.get("webViewLink",""))
                inv["gdrive_link"] = result.get("webViewLink","")
                uploaded += 1
            else:
                failed += 1
        except Exception as ex:
            log.error(f"Upload existing {inv_id}: {ex}")
            failed += 1
    if uploaded:
        save_invoices(invs)
    return jsonify({"ok":True,"uploaded":uploaded,"failed":failed,"total":uploaded+failed})



@app.route("/api/search")
def api_search():
    """
    Universal search across invoices, PDFs, vendors, descriptions.
    Query params: q=<text> type=all|invoice|offer|paid n=<limit>
    """
    q     = request.args.get("q", "").strip().lower()
    typ   = request.args.get("type", "all")
    limit = min(int(request.args.get("n", "50")), 200)

    if not q:
        return jsonify({"results": [], "total": 0, "query": ""})

    invs    = load_invoices()
    results = []
    tokens  = q.split()

    for inv in invs:
        # Type filter
        if typ == "invoice" and inv.get("is_offer"):    continue
        if typ == "offer"   and not inv.get("is_offer"): continue
        if typ == "paid"    and inv.get("status") != "paid": continue
        if typ == "unpaid"  and inv.get("status") == "paid": continue

        # Build searchable text
        text = " ".join(str(v) for v in [
            inv.get("vendor",""),
            inv.get("description",""),
            inv.get("invoice_number",""),
            inv.get("amount",""),
            inv.get("currency",""),
            inv.get("due_date",""),
            inv.get("issue_date",""),
            inv.get("category",""),
            inv.get("pdf_filename",""),
        ]).lower()

        # Also search PDF text if available
        pdf_path = get_pdf_path(inv.get("id",""))
        if pdf_path and q in text or all(t in text for t in tokens):
            score = sum(10 for t in tokens if t in text)
            # Boost exact vendor match
            if q in (inv.get("vendor","") or "").lower(): score += 30
            if q in (inv.get("invoice_number","") or "").lower(): score += 20
            results.append({**inv, "_score": score})

    results.sort(key=lambda x: x.get("_score", 0), reverse=True)
    results = results[:limit]

    return jsonify({
        "results": results,
        "total":   len(results),
        "query":   q,
    })



# ── Gmail API ─────────────────────────────────────────────────────────────────
_gmail_service = None

def _get_gmail_service():
    """Build Gmail service from stored credentials (same token as Drive)."""
    global _gmail_service
    if _gmail_service and not force_refresh:
        return _gmail_service
    _gmail_service = None
    try:
        from google.oauth2.credentials import Credentials
        from googleapiclient.discovery import build
        import json as _j
        token_data = c("gdrive", "token", "") or os.environ.get("GDRIVE_TOKEN", "")
        if not token_data:
            return None
        token = _j.loads(token_data)
        creds = Credentials(
            token=token.get("access_token"),
            refresh_token=token.get("refresh_token"),
            token_uri="https://oauth2.googleapis.com/token",
            client_id=token.get("client_id"),
            client_secret=token.get("client_secret"),
            scopes=["https://www.googleapis.com/auth/gmail.readonly",
                    "https://www.googleapis.com/auth/gmail.insert",
                    "https://www.googleapis.com/auth/drive.file"],
        )
        if creds.expired and creds.refresh_token:
            from google.auth.transport.requests import Request
            creds.refresh(Request())
            token["access_token"] = creds.token
            save_config_value("gdrive", "token", _j.dumps(token))
        _gmail_service = build("gmail", "v1", credentials=creds, cache_discovery=False)
        return _gmail_service
    except Exception as ex:
        log.debug(f"Gmail service: {ex}")
        return None


def gmail_list_messages(max_results=50, query="", page_token=None):
    """List Gmail messages. Returns {messages, nextPageToken, resultSizeEstimate}."""
    svc = _get_gmail_service()
    if not svc:
        return None
    try:
        params = {"userId": "me", "maxResults": max_results}
        if query:
            params["q"] = query
        if page_token:
            params["pageToken"] = page_token
        return svc.users().messages().list(**params).execute()
    except Exception as ex:
        log.error(f"Gmail list: {ex}")
        return None


def gmail_get_message(msg_id, fmt="metadata"):
    """Get single Gmail message. fmt: metadata|full|minimal."""
    svc = _get_gmail_service()
    if not svc:
        return None
    try:
        return svc.users().messages().get(
            userId="me", id=msg_id, format=fmt,
            metadataHeaders=["Subject","From","Date","To"] if fmt=="metadata" else []
        ).execute()
    except Exception as ex:
        log.error(f"Gmail get {msg_id}: {ex}")
        return None


def gmail_get_attachment(msg_id, attachment_id):
    """Download a Gmail attachment. Returns base64 bytes."""
    svc = _get_gmail_service()
    if not svc:
        return None
    try:
        att = svc.users().messages().attachments().get(
            userId="me", messageId=msg_id, id=attachment_id
        ).execute()
        import base64
        return base64.urlsafe_b64decode(att["data"] + "==")
    except Exception as ex:
        log.error(f"Gmail attachment {attachment_id}: {ex}")
        return None


def _gmail_decode_header(raw):
    """Decode RFC2047 header value."""
    try:
        import email.header as _eh
        parts = _eh.decode_header(raw or "")
        return "".join(
            (p.decode(enc or "utf-8", errors="replace") if isinstance(p, bytes) else p)
            for p, enc in parts
        )
    except Exception:
        return raw or ""


def _gmail_msg_to_dict(msg):
    """Convert Gmail API message to simple dict."""
    headers = {h["name"]: h["value"] for h in msg.get("payload",{}).get("headers",[])}
    subj = _gmail_decode_header(headers.get("Subject",""))
    frm  = headers.get("From","")
    dt   = headers.get("Date","")
    try:
        ds = parsedate_to_datetime(dt).strftime("%Y-%m-%d %H:%M")
    except Exception:
        ds = dt[:16]

    # Check for PDF attachments
    has_pdf = False
    parts   = msg.get("payload",{}).get("parts",[])
    for part in parts:
        fn = part.get("filename","")
        ct = part.get("mimeType","")
        if fn.lower().endswith(".pdf") or "pdf" in ct.lower():
            has_pdf = True
            break

    snippet = msg.get("snippet","")[:120]
    labels  = msg.get("labelIds",[])

    return {
        "id":       msg["id"],
        "threadId": msg.get("threadId",""),
        "subject":  subj,
        "from":     frm,
        "date":     ds,
        "snippet":  snippet,
        "has_pdf":  has_pdf,
        "unread":   "UNREAD" in labels,
        "labels":   labels,
    }



# ── Gmail Routes ─────────────────────────────────────────────────────────────

@app.route("/api/gmail/status")
def api_gmail_status():
    svc = _get_gmail_service()
    if not svc:
        return jsonify({"ok": False, "connected": False})
    try:
        profile = svc.users().getProfile(userId="me").execute()
        return jsonify({
            "ok": True, "connected": True,
            "email":       profile.get("emailAddress",""),
            "total":       profile.get("messagesTotal", 0),
            "threads":     profile.get("threadsTotal", 0),
        })
    except Exception as ex:
        return jsonify({"ok": False, "connected": False, "error": str(ex)[:100]})


@app.route("/api/gmail/messages")
def api_gmail_messages():
    """List Gmail messages with optional filter."""
    q          = request.args.get("q","")
    max_r      = min(int(request.args.get("n","50")), 100)
    page_token = request.args.get("page","")
    result = gmail_list_messages(max_results=max_r, query=q, page_token=page_token or None)
    if not result:
        return jsonify({"ok": False, "error": "Gmail не подключён"})

    msg_ids = [m["id"] for m in result.get("messages",[])]
    messages = []
    for mid in msg_ids:
        msg = gmail_get_message(mid, fmt="metadata")
        if msg:
            messages.append(_gmail_msg_to_dict(msg))

    return jsonify({
        "ok":           True,
        "messages":     messages,
        "total":        result.get("resultSizeEstimate", len(messages)),
        "nextPage":     result.get("nextPageToken",""),
    })


@app.route("/api/gmail/message/<msg_id>")
def api_gmail_message(msg_id):
    """Get full message with body and attachments list."""
    msg = gmail_get_message(msg_id, fmt="full")
    if not msg:
        return jsonify({"ok": False, "error": "Сообщение не найдено"}), 404

    info = _gmail_msg_to_dict(msg)

    # Extract body text
    def get_body(payload):
        mime = payload.get("mimeType","")
        if mime == "text/plain":
            import base64
            data = payload.get("body",{}).get("data","")
            return base64.urlsafe_b64decode(data + "==").decode("utf-8", errors="replace") if data else ""
        if mime == "text/html":
            import base64
            data = payload.get("body",{}).get("data","")
            raw = base64.urlsafe_b64decode(data + "==").decode("utf-8", errors="replace") if data else ""
            # Strip HTML tags
            import re as _re
            return _re.sub(r"<[^>]+>","", raw)
        for part in payload.get("parts",[]):
            result = get_body(part)
            if result: return result
        return ""

    body_text = get_body(msg.get("payload",{}))[:3000]

    # List attachments
    attachments = []
    for part in msg.get("payload",{}).get("parts",[]):
        fn = part.get("filename","")
        ct = part.get("mimeType","")
        if fn:
            att_id = part.get("body",{}).get("attachmentId","")
            attachments.append({
                "filename":     fn,
                "mimeType":     ct,
                "attachmentId": att_id,
                "is_pdf":       fn.lower().endswith(".pdf") or "pdf" in ct.lower(),
                "size":         part.get("body",{}).get("size",0),
            })

    info["body"]        = body_text
    info["attachments"] = attachments
    return jsonify({"ok": True, "message": info})


@app.route("/api/gmail/attachment/<msg_id>/<att_id>")
def api_gmail_attachment_download(msg_id, att_id):
    """Download Gmail attachment — returns PDF inline."""
    msg_full = gmail_get_message(msg_id, fmt="full")
    filename = "attachment.pdf"
    if msg_full:
        for part in msg_full.get("payload",{}).get("parts",[]):
            if part.get("body",{}).get("attachmentId","") == att_id:
                filename = part.get("filename","attachment.pdf")
                break

    data = gmail_get_attachment(msg_id, att_id)
    if not data:
        return jsonify({"error": "Вложение не найдено"}), 404

    from flask import Response
    ct = "application/pdf" if filename.lower().endswith(".pdf") else "application/octet-stream"
    resp = Response(data, content_type=ct)
    resp.headers["Content-Disposition"] = f'inline; filename="{filename}"'
    return resp


@app.route("/api/gmail/add-as-invoice", methods=["POST"])
def api_gmail_add_as_invoice():
    """Extract PDF from Gmail message and add as invoice."""
    data   = request.json or {}
    msg_id = data.get("msg_id","")
    att_id = data.get("att_id","")
    if not msg_id or not att_id:
        return jsonify({"ok": False, "error": "msg_id и att_id обязательны"})

    # Download attachment
    pdf_bytes = gmail_get_attachment(msg_id, att_id)
    if not pdf_bytes:
        return jsonify({"ok": False, "error": "Не удалось скачать вложение"})

    # Get message metadata
    msg      = gmail_get_message(msg_id, fmt="metadata")
    msg_dict = _gmail_msg_to_dict(msg) if msg else {}
    subj     = msg_dict.get("subject","Gmail Invoice")
    frm      = msg_dict.get("from","")
    ds       = msg_dict.get("date","")[:10] or date.today().isoformat()

    # Parse PDF
    pdf_text = extract_pdf_text(pdf_bytes, max_chars=4000)
    parsed   = parse_pdf_invoice(pdf_text, att_id)

    # Get attachment filename
    att_filename = "invoice.pdf"
    if msg:
        for part in msg.get("payload",{}).get("parts",[]):
            if part.get("body",{}).get("attachmentId","") == att_id:
                att_filename = part.get("filename","invoice.pdf")
                break

    # Build invoice
    inv = build_invoice({
        "is_invoice":     True,
        "vendor":         parsed.get("vendor") or extract_vendor(subj, frm, pdf_text),
        "amount":         parsed.get("amount", 0),
        "currency":       parsed.get("currency","EUR"),
        "due_date":       parsed.get("due_date"),
        "issue_date":     parsed.get("issue_date") or ds,
        "invoice_number": parsed.get("invoice_number",""),
        "description":    subj[:100],
        "category":       "services",
        "status":         "pending",
    }, msg_id, subj, frm, ds, True, "gmail")

    inv["pdf_filename"] = att_filename
    inv["has_pdf"]      = True
    save_pdf(inv["id"], pdf_bytes, att_filename, invoice_date=ds)

    # Save
    existing = load_invoices()
    existing.append(inv)
    save_invoices(existing)

    return jsonify({"ok": True, "invoice": inv})


@app.route("/api/gmail/scan", methods=["POST"])
def api_gmail_scan():
    """Quick scan Gmail for invoice emails (last 100)."""
    svc = _get_gmail_service()
    if not svc:
        return jsonify({"ok": False, "error": "Gmail не подключён"})

    # Search for invoice-related emails
    queries = [
        "has:attachment filename:pdf",
        "subject:arve OR subject:invoice OR subject:счёт",
    ]
    found_ids = set()
    for q in queries:
        result = gmail_list_messages(max_results=100, query=q)
        if result:
            for m in result.get("messages",[]):
                found_ids.add(m["id"])

    emails_raw = []
    for mid in list(found_ids)[:200]:
        msg = gmail_get_message(mid, fmt="metadata")
        if not msg: continue
        d = _gmail_msg_to_dict(msg)
        emails_raw.append({
            "id":             mid,
            "subject":        d["subject"],
            "from":           d["from"],
            "date":           d["date"][:10] if d["date"] else "",
            "body":           d["snippet"],
            "has_attachment": d["has_pdf"],
        })

    def emit(msg, t="info"):
        log.info(f"[gmail-scan] {msg}")

    def fetch_gmail_body(uid):
        msg_full = gmail_get_message(uid, fmt="full")
        if not msg_full: return ""
        texts = []
        for part in msg_full.get("payload",{}).get("parts",[]):
            fn = part.get("filename","")
            ct = part.get("mimeType","")
            if fn.lower().endswith(".pdf") or "pdf" in ct.lower():
                att_id = part.get("body",{}).get("attachmentId","")
                if att_id:
                    pdf_bytes = gmail_get_attachment(uid, att_id)
                    if pdf_bytes:
                        pt = extract_pdf_text(pdf_bytes)
                        if pt: texts.append(pt)
        return "\n".join(texts)

    result = process_emails(emails_raw, emit, fetch_body=fetch_gmail_body, source="gmail")
    return jsonify({"ok": True, "found": len(result), "scanned": len(emails_raw)})



# ═══════════════════════════════════════════════════════════════════════════════
#  EMAIL MIGRATION: IMAP → Gmail
#  Copies emails from Zone.eu (or any IMAP) to Gmail via gmail.insert API
#  Priority: April, March, February, January 2026 → then older months
# ═══════════════════════════════════════════════════════════════════════════════

_migration_state = {
    "running": False, "total": 0, "done": 0, "errors": 0,
    "month": "", "account": "", "log": [], "started": None, "finished": None,
}
_migration_lock = threading.Lock()


def _migrate_month(imap_host, imap_port, imap_user, imap_pass, gmail_svc,
                   year, month, emit_fn, label_name="Zone-Import"):
    """Copy one month of emails from IMAP to Gmail."""
    import base64
    from datetime import date as _d

    # IMAP date range for this month
    start = _d(year, month, 1)
    if month == 12:
        end = _d(year + 1, 1, 1)
    else:
        end = _d(year, month + 1, 1)

    MONTHS_RU = ["","Январь","Февраль","Март","Апрель","Май","Июнь",
                 "Июль","Август","Сентябрь","Октябрь","Ноябрь","Декабрь"]
    month_label = f"{MONTHS_RU[month]} {year}"
    emit_fn(f"📧 Миграция: {month_label} ({imap_user})", "info")

    # Connect to IMAP
    alt_hosts = [imap_host, "imap.zone.eu", "mail.zone.ee"]
    mail_conn, used_host = _try_imap_connect(alt_hosts, imap_port, imap_user, imap_pass)
    if not mail_conn:
        emit_fn(f"❌ Не удалось подключиться к IMAP {imap_host}", "err")
        return 0

    try:
        mail_conn.select(IMAP_FOLDER)
        # Search by date range
        since_str = start.strftime("%d-%b-%Y")
        before_str = end.strftime("%d-%b-%Y")
        _, data = mail_conn.search(None, f'(SINCE "{since_str}" BEFORE "{before_str}")')
        uids = data[0].split() if data and data[0] else []
        total = len(uids)
        emit_fn(f"  📬 {total} писем за {month_label}", "ok")

        if total == 0:
            return 0

        # Get or create Gmail label for imported emails
        label_id = None
        try:
            labels = gmail_svc.users().labels().list(userId="me").execute()
            for lb in labels.get("labels", []):
                if lb["name"] == label_name:
                    label_id = lb["id"]
                    break
            if not label_id:
                new_label = gmail_svc.users().labels().create(
                    userId="me",
                    body={"name": label_name, "labelListVisibility": "labelShow",
                          "messageListVisibility": "show"}
                ).execute()
                label_id = new_label["id"]
        except Exception as ex:
            log.warning(f"Gmail label creation: {ex}")

        migrated = 0
        errors = 0
        for i, uid in enumerate(uids):
            try:
                # Fetch full email from IMAP
                mail_conn.socket().settimeout(20)
                _, msg_data = mail_conn.fetch(uid, "(RFC822)")
                if not msg_data or not msg_data[0]:
                    continue
                raw_bytes = msg_data[0][1]

                # Insert into Gmail via API
                raw_b64 = base64.urlsafe_b64encode(raw_bytes).decode("ascii")
                body = {"raw": raw_b64}
                if label_id:
                    body["labelIds"] = [label_id, "INBOX"]
                else:
                    body["labelIds"] = ["INBOX"]

                gmail_svc.users().messages().insert(
                    userId="me", body=body,
                    internalDateSource="dateHeader"
                ).execute()
                migrated += 1

            except Exception as ex:
                errors += 1
                if errors <= 3:
                    emit_fn(f"  ⚠ Ошибка UID {uid}: {str(ex)[:60]}", "warn")
                # Reconnect IMAP if connection dropped
                if "socket" in str(ex).lower() or "broken" in str(ex).lower():
                    try:
                        mail_conn, _ = _try_imap_connect(alt_hosts, imap_port, imap_user, imap_pass)
                        if mail_conn:
                            mail_conn.select(IMAP_FOLDER)
                    except Exception:
                        pass

            if (i + 1) % 50 == 0 or i + 1 == total:
                emit_fn(f"  📤 {month_label}: {i+1}/{total} ({migrated} ок, {errors} ош)", "info")
                _migration_state["done"] = _migration_state.get("_base_done", 0) + i + 1

        emit_fn(f"  ✅ {month_label}: {migrated}/{total} перенесено ({errors} ошибок)", "ok")
        return migrated

    except Exception as ex:
        emit_fn(f"❌ Ошибка миграции {month_label}: {ex}", "err")
        return 0
    finally:
        try:
            mail_conn.close()
            mail_conn.logout()
        except Exception:
            pass


def _run_migration(imap_host, imap_port, imap_user, imap_pass, months_list):
    """Background migration thread: copies emails from IMAP to Gmail month by month."""
    import time as _t

    def emit(msg, t="info"):
        entry = {"ts": datetime.now().strftime("%H:%M:%S"), "msg": msg, "t": t}
        _migration_state["log"].append(entry)
        if len(_migration_state["log"]) > 200:
            _migration_state["log"] = _migration_state["log"][-200:]
        log.info(f"[MIGRATE] {msg}")

    try:
        gmail_svc = _get_gmail_service()
        if not gmail_svc:
            emit("❌ Gmail не подключён. Подключите Google аккаунт в /keys", "err")
            return

        total_emails = 0
        total_migrated = 0
        _migration_state["_base_done"] = 0

        for year, month in months_list:
            _migration_state["month"] = f"{month:02d}.{year}"
            count = _migrate_month(
                imap_host, imap_port, imap_user, imap_pass,
                gmail_svc, year, month, emit
            )
            total_migrated += count
            _migration_state["_base_done"] = _migration_state["done"]
            _t.sleep(1)  # small pause between months

        emit(f"✅ Миграция завершена: {total_migrated} писем перенесено", "ok")

    except Exception as ex:
        emit(f"❌ Критическая ошибка: {ex}", "err")
    finally:
        _migration_state["running"] = False
        _migration_state["finished"] = datetime.now().isoformat()


@app.route("/api/migrate/start", methods=["POST"])
def api_migrate_start():
    """Start email migration from IMAP to Gmail.
    Body: {account_email, year, months: [4,3,2,1], imap_host, imap_port}
    Default: primary account, 2026, April→January
    """
    if _migration_state["running"]:
        return jsonify({"ok": False, "error": "Миграция уже выполняется"})

    data = request.json or {}
    acc_email = data.get("account_email", EMAIL_ADDR)
    acc_pass  = data.get("account_password", EMAIL_PASS)
    host      = data.get("imap_host", IMAP_HOST)
    port      = int(data.get("imap_port", IMAP_PORT))
    year      = int(data.get("year", 2026))
    months    = data.get("months", [4, 3, 2, 1])  # April first, then backwards

    # Check for additional accounts
    if acc_email != EMAIL_ADDR:
        accounts = _load_accounts()
        acc = next((a for a in accounts if a.get("email") == acc_email), None)
        if acc:
            acc_pass = acc.get("password", acc_pass)
            host = acc.get("imap", host)
            port = int(acc.get("port", port))

    months_list = [(year, m) for m in months]

    _migration_state.update({
        "running": True, "total": 0, "done": 0, "errors": 0,
        "month": "", "account": acc_email, "log": [],
        "started": datetime.now().isoformat(), "finished": None,
    })

    t = threading.Thread(
        target=_run_migration,
        args=(host, port, acc_email, acc_pass, months_list),
        daemon=True, name="email-migration"
    )
    t.start()
    return jsonify({"ok": True, "message": f"Миграция запущена: {acc_email} → Gmail"})


@app.route("/api/migrate/status")
def api_migrate_status():
    return jsonify(_migration_state)


@app.route("/api/migrate/stop", methods=["POST"])
def api_migrate_stop():
    _migration_state["running"] = False
    return jsonify({"ok": True})


# ═══════════════════════════════════════════════════════════════════════════════
#  GMAIL API SCANNER
#  Reads Gmail for emails from 2026-04-05 onwards (after forwarding setup)
#  Zone.eu IMAP handles everything before 2026-04-05
# ═══════════════════════════════════════════════════════════════════════════════

GMAIL_CUTOFF = "2026-04-05"  # Zone → before this date, Gmail → from this date


def _get_gmail_service():
    """Build Gmail service using same OAuth token as Drive."""
    try:
        from google.oauth2.credentials import Credentials
        from googleapiclient.discovery import build
        import json

        token_data = c("gdrive", "token", "") or os.environ.get("GDRIVE_TOKEN", "")
        if not token_data:
            return None
        token = json.loads(token_data)
        creds = Credentials(
            token=token.get("access_token"),
            refresh_token=token.get("refresh_token"),
            token_uri="https://oauth2.googleapis.com/token",
            client_id=token.get("client_id"),
            client_secret=token.get("client_secret"),
        )
        if creds.expired and creds.refresh_token:
            from google.auth.transport.requests import Request
            creds.refresh(Request())
            token["access_token"] = creds.token
            save_config_value("gdrive", "token", json.dumps(token))

        return build("gmail", "v1", credentials=creds, cache_discovery=False)
    except Exception as ex:
        log.debug(f"Gmail service init: {ex}")
        return None


def _gmail_get_body(service, msg_id: str) -> tuple[str, list]:
    """
    Fetch full message. Returns (body_text, attachments_list).
    attachments = [{filename, mime_type, data: bytes}]
    """
    import base64 as _b64
    msg = service.users().messages().get(
        userId="me", id=msg_id, format="full"
    ).execute()

    body_text   = ""
    attachments = []

    def _walk(parts):
        nonlocal body_text
        for part in parts:
            mime = part.get("mimeType","")
            fn   = part.get("filename","")
            body = part.get("body",{})

            if part.get("parts"):
                _walk(part["parts"])
            elif mime == "text/plain" and not fn:
                data = body.get("data","")
                if data:
                    body_text += _b64.urlsafe_b64decode(data + "==").decode("utf-8", errors="replace")
            elif fn and body.get("attachmentId"):
                # Download attachment
                try:
                    att = service.users().messages().attachments().get(
                        userId="me", messageId=msg_id,
                        id=body["attachmentId"]
                    ).execute()
                    att_data = _b64.urlsafe_b64decode(att["data"] + "==")
                    attachments.append({"filename": fn, "mime_type": mime, "data": att_data})
                except Exception:
                    pass
            elif fn and body.get("data"):
                att_data = _b64.urlsafe_b64decode(body["data"] + "==")
                attachments.append({"filename": fn, "mime_type": mime, "data": att_data})

    _walk(msg.get("payload",{}).get("parts", [msg.get("payload",{})]))
    return body_text, attachments


def scan_gmail(emit, from_date: str = None, to_date: str = None) -> list:
    """
    Scan Gmail for invoice emails.
    from_date defaults to GMAIL_CUTOFF (2026-04-05) — only new emails.
    """
    service = _get_gmail_service()
    if not service:
        emit("Gmail не подключён — пропускаем", "warn")
        return []

    start = from_date or GMAIL_CUTOFF
    emit(f"📨 Gmail: письма с {start}...", "info")

    # Build Gmail query
    query_parts = [f"after:{start.replace('-','/')}"]
    if to_date:
        query_parts.append(f"before:{to_date.replace('-','/')}")

    # Search for invoice-related emails
    inv_query = " OR ".join([
        "subject:arve", "subject:invoice", "subject:счёт", "subject:rechnung",
        "subject:faktura", "has:attachment filename:pdf"
    ])
    query = "(" + " ".join(query_parts) + ") (" + inv_query + ")"

    try:
        resp = service.users().messages().list(
            userId="me", q=query, maxResults=500
        ).execute()
        msgs = resp.get("messages", [])
        emit(f"📨 Gmail: найдено {len(msgs)} писем", "ok")
    except Exception as ex:
        emit(f"Gmail поиск: {ex}", "err")
        return []

    if not msgs:
        return []

    # Fetch headers for all messages
    emails_raw = []
    import email.header as _eh2
    from email.utils import parsedate_to_datetime as _pdt2

    for msg_stub in msgs:
        try:
            hdr = service.users().messages().get(
                userId="me", id=msg_stub["id"],
                format="metadata",
                metadataHeaders=["Subject","From","Date"]
            ).execute()
            headers = {h["name"]: h["value"] for h in hdr.get("payload",{}).get("headers",[])}
            raw_subj = headers.get("Subject","")
            try:
                parts = _eh2.decode_header(raw_subj)
                subj = "".join(
                    (p.decode(enc or "utf-8", errors="replace") if isinstance(p,bytes) else p)
                    for p,enc in parts)
            except Exception:
                subj = raw_subj

            frm = headers.get("From","")
            try:
                ds = _pdt2(headers.get("Date","")).strftime("%Y-%m-%d")
            except Exception:
                ds = date.today().isoformat()

            # Check for PDF attachments
            has_att = any(
                p.get("filename","").lower().endswith(".pdf")
                for p in hdr.get("payload",{}).get("parts",[])
            )

            emails_raw.append({
                "id":            f"gmail_{msg_stub['id']}",
                "_gmail_id":     msg_stub["id"],
                "subject":       subj,
                "from":          frm,
                "body":          "",
                "date":          ds,
                "has_attachment": has_att,
            })
        except Exception as ex:
            log.debug(f"Gmail header {msg_stub['id']}: {ex}")

    emit(f"📨 Gmail: {len(emails_raw)} заголовков загружено", "ok")

    # Fetch body+PDFs for candidates (after keyword pre-filter)
    _gmail_svc_ref = service  # capture for closure

    def _fetch_gmail_body(uid_str):
        gmail_id = uid_str.replace("gmail_", "")
        try:
            body_text, attachments = _gmail_get_body(_gmail_svc_ref, gmail_id)
            pdf_texts = []
            for att in attachments:
                fn = att["filename"].lower()
                if fn.endswith(".pdf") or "pdf" in att["mime_type"]:
                    pt = extract_pdf_text(att["data"], max_chars=4000)
                    if pt:
                        pdf_texts.append(f"[PDF: {att['filename']}]\n{pt}")
                    # Save to Drive
                    try:
                        clean = re.sub(r"[^a-zA-Z0-9._\-]","_",att["filename"])[:80]
                        gdrive_upload_pdf(att["data"], clean, None)
                    except Exception:
                        pass
            result = body_text
            if pdf_texts:
                result += "\n\n--- PDF ---\n" + "\n".join(pdf_texts)
            return result
        except Exception as ex:
            log.debug(f"Gmail body {uid_str}: {ex}")
            return ""

    # Process through standard pipeline
    result = process_emails(emails_raw, emit,
                            fetch_body=_fetch_gmail_body,
                            source="gmail")
    emit(f"✅ Gmail: найдено {len(result)} счетов", "ok")
    return result






# ═══════════════════════════════════════════════════════════════════════════════
#  SYNOLOGY DRIVE INTEGRATION
#  Reads documents from Synology NAS via WebDAV
#  Config: config.ini → [synology] host/user/password/base_path
# ═══════════════════════════════════════════════════════════════════════════════

def _syn_cfg():
    """Get Synology connection config."""
    return {
        "host":      c("synology","host","")      or os.environ.get("SYNOLOGY_HOST",""),
        "user":      c("synology","user","")      or os.environ.get("SYNOLOGY_USER",""),
        "password":  c("synology","password","")  or os.environ.get("SYNOLOGY_PASSWORD",""),
        "base_path": c("synology","base_path","/QualityDesk") or "/QualityDesk",
        "port":      int(c("synology","port","5006") or 5006),
        "https":     c("synology","https","true").lower() == "true",
    }

def _syn_url(cfg: dict, path: str = "") -> str:
    scheme = "https" if cfg["https"] else "http"
    base = path.lstrip("/")
    return f"{scheme}://{cfg['host']}:{cfg['port']}/webdav/{base}"

def syn_list(path: str = "/") -> list:
    """List files/folders on Synology via WebDAV PROPFIND."""
    cfg = _syn_cfg()
    if not cfg["host"]:
        return []
    try:
        import xml.etree.ElementTree as ET
        url = _syn_url(cfg, cfg["base_path"].rstrip("/") + "/" + path.lstrip("/"))
        r = requests.request("PROPFIND", url,
            auth=(cfg["user"], cfg["password"]),
            headers={"Depth": "1", "Content-Type": "application/xml"},
            data=b'''<?xml version="1.0"?><D:propfind xmlns:D="DAV:"><D:prop><D:displayname/><D:getcontentlength/><D:getlastmodified/><D:resourcetype/></D:prop></D:propfind>''',
            verify=False, timeout=15)
        if r.status_code not in (200, 207):
            return []
        ns = {"D": "DAV:"}
        root = ET.fromstring(r.content)
        items = []
        for resp in root.findall("D:response", ns):
            href = resp.findtext("D:href", "", ns)
            name = resp.findtext(".//D:displayname", "", ns) or href.split("/")[-1]
            is_dir = resp.find(".//D:collection", ns) is not None
            size   = resp.findtext(".//D:getcontentlength", "0", ns)
            mtime  = resp.findtext(".//D:getlastmodified", "", ns)
            if name and name != path.split("/")[-1]:
                items.append({
                    "name":    name,
                    "path":    href,
                    "is_dir":  is_dir,
                    "size":    int(size or 0),
                    "mtime":   mtime,
                    "ext":     name.rsplit(".",1)[-1].lower() if "." in name else "",
                })
        return sorted(items, key=lambda x: (not x["is_dir"], x["name"].lower()))
    except Exception as ex:
        log.warning(f"Synology list {path}: {ex}")
        return []

def syn_search(query: str, path: str = "/") -> list:
    """Simple recursive search by filename on Synology."""
    cfg = _syn_cfg()
    if not cfg["host"] or not query:
        return []
    try:
        results = []
        q = query.lower()
        def _recurse(p, depth=0):
            if depth > 4: return
            items = syn_list(p)
            for item in items:
                if q in item["name"].lower():
                    results.append(item)
                if item["is_dir"] and depth < 3:
                    _recurse(item["path"], depth+1)
                if len(results) >= 50: return
        _recurse(path)
        return results
    except Exception as ex:
        log.warning(f"Synology search: {ex}")
        return []

def syn_get_file(webdav_path: str) -> bytes | None:
    """Download file from Synology."""
    cfg = _syn_cfg()
    if not cfg["host"]:
        return None
    try:
        url = f"{'https' if cfg['https'] else 'http'}://{cfg['host']}:{cfg['port']}{webdav_path}"
        r = requests.get(url, auth=(cfg["user"], cfg["password"]),
                         verify=False, timeout=30)
        if r.status_code == 200:
            return r.content
        return None
    except Exception as ex:
        log.warning(f"Synology get {webdav_path}: {ex}")
        return None

def syn_upload_file(webdav_path: str, data: bytes, filename: str) -> bool:
    """Upload file to Synology."""
    cfg = _syn_cfg()
    if not cfg["host"]:
        return False
    try:
        url = f"{'https' if cfg['https'] else 'http'}://{cfg['host']}:{cfg['port']}{webdav_path}/{filename}"
        r = requests.put(url, data=data, auth=(cfg["user"], cfg["password"]),
                         verify=False, timeout=30)
        return r.status_code in (200, 201, 204)
    except Exception as ex:
        log.warning(f"Synology upload {filename}: {ex}")
        return False

def syn_status() -> dict:
    """Check Synology connection."""
    cfg = _syn_cfg()
    if not cfg["host"]:
        return {"ok": False, "connected": False, "error": "Не настроен хост Synology"}
    try:
        url = _syn_url(cfg, cfg["base_path"])
        r = requests.request("PROPFIND", url,
            auth=(cfg["user"], cfg["password"]),
            headers={"Depth": "0"},
            verify=False, timeout=10)
        if r.status_code in (200, 207):
            return {"ok": True, "connected": True, "host": cfg["host"],
                    "base_path": cfg["base_path"]}
        return {"ok": False, "connected": False, "error": f"HTTP {r.status_code}"}
    except Exception as ex:
        return {"ok": False, "connected": False, "error": str(ex)[:100]}



# ── Synology Routes ───────────────────────────────────────────────────────────

@app.route("/api/synology/status")
def api_syn_status():
    return jsonify(syn_status())

@app.route("/api/synology/browse")
def api_syn_browse():
    """List folder contents on Synology."""
    path = request.args.get("path", "/")
    return jsonify({"ok": True, "items": syn_list(path), "path": path})

@app.route("/api/synology/search")
def api_syn_search():
    """Search files on Synology by name."""
    q    = request.args.get("q", "").strip()
    path = request.args.get("path", "/")
    if not q:
        return jsonify({"ok": False, "error": "Введи поисковый запрос"})
    return jsonify({"ok": True, "results": syn_search(q, path), "query": q})

@app.route("/api/synology/file")
def api_syn_file():
    """Download/view file from Synology (proxy through server)."""
    path = request.args.get("path", "")
    if not path:
        return jsonify({"error": "path required"}), 400
    data = syn_get_file(path)
    if not data:
        return jsonify({"error": "Файл не найден на Synology"}), 404
    ext  = path.rsplit(".", 1)[-1].lower() if "." in path else ""
    mime = {"pdf": "application/pdf", "png": "image/png",
            "jpg": "image/jpeg", "jpeg": "image/jpeg",
            "docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            "xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    }.get(ext, "application/octet-stream")
    from flask import Response
    return Response(data, mimetype=mime,
                    headers={"Content-Disposition": f"inline; filename={path.split('/')[-1]}"})

@app.route("/api/synology/save-config", methods=["POST"])
def api_syn_save_config():
    """Save Synology connection settings."""
    d = request.json or {}
    for key in ["host", "user", "password", "base_path", "port", "https"]:
        if key in d:
            save_config_value("synology", key, str(d[key]))
    return jsonify({"ok": True})

@app.route("/api/synology/link-to-invoice", methods=["POST"])
def api_syn_link_invoice():
    """Link a Synology file to an invoice (save path reference)."""
    d       = request.json or {}
    inv_id  = d.get("inv_id", "")
    syn_path = d.get("synology_path", "")
    if not inv_id or not syn_path:
        return jsonify({"ok": False, "error": "inv_id и synology_path обязательны"})
    invs = load_invoices()
    for inv in invs:
        if inv.get("id") == inv_id:
            inv["synology_path"] = syn_path
            save_invoices(invs)
            return jsonify({"ok": True})
    return jsonify({"ok": False, "error": "Счёт не найден"}), 404


_bg_started = False

# Marker file — written after the one-time cleanup runs so it never repeats
_CLEANUP_MARKER = _DATA_DIR / "cleanup_dates_v1.done"


def _auto_cleanup_bad_dates():
    """
    One-time startup task: remove invoices whose issue_date equals their
    added_at date (scan date was used instead of the actual document date —
    bug present in scans run on 2026-04-04 and 2026-04-05).
    After cleanup, triggers a full re-scan from 2025-01-01 so all those
    emails are re-processed with the corrected date-extraction logic.
    Returns number of invoices removed.
    """
    if _CLEANUP_MARKER.exists():
        return 0  # already ran

    invs   = load_invoices()
    before = len(invs)

    # Bad invoices: added during the buggy scan AND issue_date == scan date
    # (meaning the date was never extracted from the document)
    BAD_SCAN_DATES = {"2026-04-04", "2026-04-05"}

    def _is_bad(i):
        added   = (i.get("added_at") or "")[:10]
        issued  = (i.get("issue_date") or "")[:10]
        if i.get("status") == "paid":
            return False                     # never delete paid invoices
        if added not in BAD_SCAN_DATES:
            return False                     # not from the buggy scan
        return issued == added               # issue_date == scan date → bad

    cleaned = [i for i in invs if not _is_bad(i)]
    removed = before - len(cleaned)

    if removed > 0:
        save_invoices(cleaned)
        log.info(f"Auto-cleanup: removed {removed} invoices with scan-date as issue_date "
                 f"({before} → {len(cleaned)})")
    else:
        log.info("Auto-cleanup: no bad-dated invoices found — nothing removed")

    # Write marker (prevents re-run on next deploy)
    _CLEANUP_MARKER.write_text(
        json.dumps({"done": datetime.now().isoformat(),
                    "removed": removed, "before": before},
                   ensure_ascii=False),
        encoding="utf-8",
    )
    return removed


def _start_background():
    global _bg_started
    if _bg_started:
        return
    _bg_started = True
    check_notifications()
    log.info("PayCalendar started")

    # ── One-time cleanup of invoices with wrong dates ─────────────────────────
    removed = 0
    try:
        removed = _auto_cleanup_bad_dates()
    except Exception as ex:
        log.error(f"Auto-cleanup error: {ex}")

    # ── Clean up pending file to prevent restart loops ──────────────────────
    if PENDING_FILE.exists():
        _clear_pending_scan()
        log.info("Cleared PENDING_FILE (no auto-scan on restart)")

# Auto-start when imported (gunicorn)
# Auto-fix wrong IMAP host (mail.zone.ee → imap.zone.eu)
_saved_host = c("email", "imap_host", "")
if _saved_host in ("mail.zone.ee", ""):
    save_config_value("email", "imap_host", "imap.zone.eu")
    IMAP_HOST = "imap.zone.eu"
    log.info("IMAP host set to imap.zone.eu (Zone.ee official server)")

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

    if not DATA_FILE.exists():
        print("  First run — scan will start automatically.", flush=True)
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
