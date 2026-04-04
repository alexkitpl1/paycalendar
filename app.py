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
# Zone.ee official IMAP server (mail.zone.ee doesn't work from cloud)
_cfg_imap = c("email", "imap_host", "imap.zone.eu")
IMAP_HOST  = "imap.zone.eu" if _cfg_imap in ("", "mail.zone.ee") else _cfg_imap
IMAP_PORT      = int(c("email", "imap_port", "993"))
IMAP_FOLDER    = c("email", "imap_folder", "INBOX")
SCAN_LIMIT       = int(c("email", "scan_last_emails", "500"))
QUICK_SCAN_LIMIT = int(c("email", "quick_scan_emails", "30"))
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
    """Return first available AI provider (skips quota-exceeded ones)."""
    if GEMINI_KEY  and len(GEMINI_KEY)  > 10 and "gemini"  not in _provider_blocked: return "gemini"
    if API_KEY     and len(API_KEY)     > 20 and "claude"  not in _provider_blocked: return "claude"
    if GROQ_KEY    and len(GROQ_KEY)    > 10 and "groq"    not in _provider_blocked: return "groq"
    if OPENAI_KEY  and len(OPENAI_KEY)  > 10 and "openai"  not in _provider_blocked: return "openai"
    return None  # will use keyword extractor


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
    """Call AI provider with auto-fallback chain."""
    # Try configured providers first
    for attempt in range(4):
        provider = _active_provider()
        if not provider:
            break
        try:
            log.debug(f"ask_ai: trying {provider}")
            if provider == "gemini":  return _ask_gemini(prompt)
            if provider == "claude":  return _ask_claude(prompt)
            if provider == "groq":    return _ask_groq(prompt)
            if provider == "openai":  return _ask_openai(prompt)
        except Exception as ex:
            err = str(ex).lower()
            if any(k in err for k in ["billing","credit","unauthorized","invalid_api_key"]):
                _provider_blocked.add(provider)
            # Don't block on 429 - model rotation handles it
    # Final fallback: HuggingFace free public models
    try:
        log.info("ask_ai: trying HuggingFace free inference")
        return _ask_huggingface(prompt)
    except Exception as ex:
        log.warning(f"HuggingFace failed: {ex}")
    raise ValueError("Все AI провайдеры недоступны — используется анализ ключевых слов")

_gemini_last_call = [0.0]

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
    elapsed = _t.time() - _gemini_last_call[0]
    if elapsed < 2.0:
        _t.sleep(2.0 - elapsed)
    _gemini_last_call[0] = _t.time()

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
                json={"model": "llama-3.3-70b-versatile",
                      "max_tokens": 512, "temperature": 0.1,
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



# ═══════════════════════════════════════════════════════════════════════════════
#  PDF STORAGE
#  PDFs saved to /data/pdfs/{inv_id}.pdf during IMAP scan
#  Served via /api/invoice/<id>/pdf
# ═══════════════════════════════════════════════════════════════════════════════

_PDF_DIR = _DATA_DIR / "pdfs"
_PDF_DIR.mkdir(parents=True, exist_ok=True)


def save_pdf(inv_id: str, pdf_bytes: bytes, filename: str = "") -> Path:
    """Save PDF bytes to disk. Returns path."""
    _PDF_DIR.mkdir(parents=True, exist_ok=True)
    # Sanitize inv_id for filename
    safe_id = re.sub(r"[^a-zA-Z0-9_-]", "_", inv_id)[:80]
    path = _PDF_DIR / f"{safe_id}.pdf"
    path.write_bytes(pdf_bytes)
    log.info(f"PDF saved: {path.name} ({len(pdf_bytes)//1024}KB) fn={filename}")
    return path


def get_pdf_path(inv_id: str) -> Path | None:
    """Get path to stored PDF for invoice. None if not found."""
    safe_id = re.sub(r"[^a-zA-Z0-9_-]", "_", inv_id)[:80]
    path = _PDF_DIR / f"{safe_id}.pdf"
    return path if path.exists() else None


def pdf_count() -> int:
    """Count stored PDFs."""
    return len(list(_PDF_DIR.glob("*.pdf"))) if _PDF_DIR.exists() else 0


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
    return clean_subj[:40] if clean_subj else "Unknown"


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
        if uid in existing_ids:
            # Same email seen again → increment reminder count
            for _inv in existing:
                if _inv.get("email_uid") == uid:
                    _inv["reminder_count"] = _inv.get("reminder_count", 1) + 1
                    _inv["last_reminded"] = datetime.now().strftime("%Y-%m-%d")
                    break
            skipped_dup += 1; all_uids.append(uid); continue
        # Normalise subject to catch Re:/RE: thread duplicates
        norm_key = (_norm_subj(subj), ds[:10])
        if norm_key in existing_subj_dates:
            for _inv in existing:
                inv_key = (_norm_subj(_inv.get("description","") or ""), _inv.get("issue_date","")[:10])
                if inv_key == norm_key:
                    _inv["reminder_count"] = _inv.get("reminder_count", 1) + 1
                    _inv["last_reminded"] = ds
                    break
            skipped_dup += 1; all_uids.append(uid); continue
        existing_subj_dates.add(norm_key)

        # Decode RFC2047 subject
        raw_subj = str(em.get("subject") or "")
        try:
            import email.header as _eh
            parts = _eh.decode_header(raw_subj)
            subj = "".join(
                (p.decode(enc or "utf-8", errors="replace") if isinstance(p, bytes) else p)
                for p, enc in parts)
        except Exception:
            subj = raw_subj

        frm = str(em.get("from") or em.get("sender") or "")
        if isinstance(em.get("from"), dict):
            frm = em["from"].get("address") or em["from"].get("name") or ""
        body = str(em.get("text") or em.get("intro") or em.get("body") or "")[:2000]
        att  = bool(em.get("hasAttachments") or em.get("has_attachment") or em.get("attachments"))
        try:
            ds = parsedate_to_datetime(str(em.get("date") or "")).strftime("%Y-%m-%d")
        except Exception:
            ds = date.today().isoformat()

        score = is_invoice_by_keywords(subj, body, frm, att)
        if score < 20:
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
    has_cl  = 1 if (API_KEY and len(API_KEY) > 20) else 0
    n_keys  = gem_ok + grq_ok + has_cl
    workers = max(1, min(n_keys, 6, total))

    emit(f"⚡ Параллельный анализ: {workers} потоков | "
         f"Gemini×{gem_ok} Groq×{grq_ok} Claude×{has_cl}", "info")

    # ETA estimate
    ai_rps  = workers * 0.8  # ~0.8 req/sec per worker conservatively
    eta_sec = int(total / ai_rps) if ai_rps > 0 else total * 4
    emit(f"⏱ Ожидаемое время: ~{eta_sec}с ({total} кандидатов × {workers} потоков)", "info")

    # ── Phase 3: parallel analysis ────────────────────────────────────────────
    done   = [0]
    lock   = __import__("threading").Lock()

    def analyze(em):
        uid   = em["uid"]; subj = em["subj"]; frm = em["frm"]
        body  = em["body"]; ds   = em["ds"];  att = em["att"]
        score = em["score"]

        # Fetch full body if IMAP
        if fetch_body and uid:
            try:
                fb = fetch_body(uid)
                if fb: body = fb[:3000]
            except Exception: pass

        inv = None
        try:
            combined = body
            if pdf_text:
                combined = body + "\n\n--- ВЛОЖЕНИЕ PDF ---\n" + pdf_text[:2000]
            prompt = CLAUDE_TMPL.format(subject=subj, sender=frm,
                                        date=ds, has_att=att, body=combined[:3000])
            obj = extract_json(ask_ai(prompt))
            if obj and obj.get("is_invoice"):
                inv = build_invoice(obj, uid, subj, frm, ds, att, source)
                prov = _active_provider() or "AI"
                emit(f"✓ [{prov.upper()}] {inv['vendor']} "
                     f"{inv['amount']} {inv['currency']}", "ok")
        except Exception:
            # AI unavailable → keyword-only fallback
            if score >= 35:
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
                elif amount == 0 and score < 65:
                    log.debug(f"KW skip (no amount, score={score}): {subj[:50]}")
                else:
                    inv = build_invoice({
                        "is_invoice": True,
                        "vendor":   vendor,
                        "amount":   amount,
                        "currency": extract_currency(subj + " " + body[:300]),
                        "due_date": extract_due_date(body + " " + subj, ds),
                        "issue_date": ds[:10],
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

    # Always save: new invoices + updated reminder counts
        save_invoices(existing + new_invs)
        emit(f"✅ Добавлено {len(new_invs)} счетов!", "ok")
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
    """Resolve hostname via multiple DoH providers (bypasses Railway DNS for .ee domains)."""
    providers = [
        f"https://dns.google/resolve?name={hostname}&type=A",
        f"https://cloudflare-dns.com/dns-query?name={hostname}&type=A",
    ]
    for url in providers:
        try:
            r = requests.get(url, headers={"Accept":"application/dns-json"}, timeout=4)
            if r.status_code == 200:
                answers = r.json().get("Answer",[])
                ips = [a["data"] for a in answers if a.get("type")==1]
                if ips:
                    log.info(f"DoH {hostname} → {ips[0]}")
                    return ips[0]
        except Exception as ex:
            log.debug(f"DoH failed {hostname}: {ex}")
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

        # Subject keyword search
        if not date_terms or not ids:
            for term in IMAP_SUBJECTS:
                try:
                    args = [term] + date_terms if date_terms else [term]
                    _, data = mail.search(None, *args)
                    for uid in data[0].split():
                        ids.add(uid)
                except Exception:
                    pass

        # New since last scan
        if last_uid and not from_date:
            try:
                _, data = mail.search(None, f"UID {int(last_uid)+1}:*")
                for uid in data[0].split():
                    ids.add(uid)
                emit(f"Новые с UID {last_uid}...", "ok")
            except Exception:
                pass

        # Fallback: last N emails
        if not ids:
            emit("Нет совпадений — беру последние письма", "warn")
            _, data = mail.search(None, "ALL")
            all_ids = data[0].split()
            ids = set(all_ids[-min(SCAN_LIMIT, len(all_ids)):])

        # Sort newest first, limit
        try:
            ids_sorted = sorted(ids, key=lambda x: int(x.decode() if isinstance(x,bytes) else x), reverse=True)
        except Exception:
            ids_sorted = list(ids)
        ids = set(ids_sorted[:SCAN_LIMIT])
        emit(f"Анализирую {len(ids)} писем (лимит {SCAN_LIMIT})...", "ok")

        # Fetch headers
        emails_raw = []
        for uid in list(ids):
            try:
                _, data = mail.fetch(uid, "(RFC822.HEADER)")
                if not data or not data[0]: continue
                msg = email.message_from_bytes(data[0][1])
                subj = decode_header(msg.get("Subject",""))
                sndr = decode_header(msg.get("From",""))
                att  = False  # will check separately
                try:
                    ds = parsedate_to_datetime(msg.get("Date","")).strftime("%Y-%m-%d")
                except Exception:
                    ds = date.today().isoformat()
                uid_s = uid.decode() if isinstance(uid, bytes) else str(uid)
                emails_raw.append({
                    "id": uid_s, "subject": subj, "from": sndr,
                    "body": "", "date": ds, "has_attachment": False,
                })
            except Exception as ex:
                emit(f"Ошибка заголовка {uid}: {ex}", "err")

        # Hook for PDF fetching - pass mail connection
        _pending_pdfs = {}  # uid_str -> {filename, bytes}

        def _fetch_pdf_and_body(uid_str):
            """Fetch body + PDF text, also save PDF to disk."""
            try:
                uid_b = uid_str.encode() if isinstance(uid_str, str) else uid_str
                _, data = mail.fetch(uid_b, "(RFC822)")
                if not data or not data[0]: return ""
                raw_msg = email.message_from_bytes(data[0][1])
                body_text = get_plain_body(raw_msg)
                pdf_texts = []
                for part in raw_msg.walk():
                    ct = part.get_content_type() or ""
                    fn = part.get_filename() or ""
                    if "pdf" in ct.lower() or (fn and fn.lower().endswith(".pdf")):
                        try:
                            pdf_bytes = part.get_payload(decode=True)
                            if pdf_bytes:
                                pt = extract_pdf_text(pdf_bytes)
                                if pt:
                                    pdf_texts.append(f"[PDF: {fn}]\n{pt}")
                                # Save PDF bytes for later storage (after invoice ID is known)
                                _pending_pdfs[uid_str] = {"filename": fn, "bytes": pdf_bytes}
                                emit(f"  📄 PDF: {fn} ({len(pdf_bytes)//1024}КБ)", "ok")
                        except Exception as pe:
                            log.debug(f"PDF {fn}: {pe}")
                result = body_text
                if pdf_texts:
                    result += "\n\n--- ВЛОЖЕНИЯ PDF ---\n" + "\n".join(pdf_texts)
                return result
            except Exception as ex:
                log.debug(f"fetch full {uid_str}: {ex}")
                return ""

        result = process_emails(emails_raw, emit,
                                fetch_body=_fetch_pdf_and_body, source="imap")

        # Save PDFs to disk, link to invoice IDs
        if _pending_pdfs and isinstance(result, list):
            existing_all = load_invoices()
            for inv in result:
                uid = inv.get("email_uid","")
                if uid in _pending_pdfs:
                    pdf_info = _pending_pdfs[uid]
                    try:
                        save_pdf(inv["id"], pdf_info["bytes"], pdf_info["filename"])
                        inv["has_pdf"]      = True
                        inv["pdf_filename"] = pdf_info["filename"]
                        log.info(f"PDF linked to invoice {inv['id']}: {pdf_info['filename']}")
                    except Exception as pe:
                        log.error(f"PDF save error: {pe}")
            # Re-save invoices with has_pdf flag
            if any(i.get("has_pdf") for i in result):
                all_invs = load_invoices()
                inv_map  = {i["id"]: i for i in result}
                for i, inv in enumerate(all_invs):
                    if inv["id"] in inv_map:
                        all_invs[i] = inv_map[inv["id"]]
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

        # Single account - original logic
        emitter(f"Checking IMAP {IMAP_HOST}:{IMAP_PORT}...")
        try:
            with socket.create_connection((IMAP_HOST, IMAP_PORT), timeout=5):
                emitter("IMAP reachable", "ok")
                result = scan_imap(emitter, from_date=from_date, to_date=to_date)
                if result is not None:
                    return result
                # scan_imap returned None → fall through to webmail
        except Exception as ex:
            emitter(f"IMAP blocked ({type(ex).__name__}) — пробуем другие серверы", "warn")
            return []
    finally:
        SCAN_LIMIT = orig_limit  # always restore, even on exception
        scan_lock.release()

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
        finally:
            SCAN_LIMIT = orig_limit
            scan_lock.release()
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
        _scan_state_live["queued"] = True
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
        if date_terms:
            _, data = mail.search(None, *date_terms)
            for uid in data[0].split(): ids.add(uid)
        else:
            for term in IMAP_SUBJECTS:
                try:
                    _, data = mail.search(None, term)
                    for uid in data[0].split(): ids.add(uid)
                except Exception: pass
            if not ids:
                _, data = mail.search(None, "ALL")
                all_ids = data[0].split()
                ids = set(all_ids[-min(SCAN_LIMIT, len(all_ids)):])

        ids_sorted = sorted(ids, key=lambda x: int(x.decode() if isinstance(x,bytes) else x), reverse=True)
        ids = set(ids_sorted[:SCAN_LIMIT])
        emit(f"  Писем для анализа: {len(ids)}", "info")

        emails_raw = []
        for uid in list(ids):
            try:
                _, data = mail.fetch(uid, "(RFC822.HEADER)")
                if not data or not data[0]: continue
                msg = email.message_from_bytes(data[0][1])
                subj = decode_header(msg.get("Subject",""))
                sndr = decode_header(msg.get("From",""))
                try:
                    ds = parsedate_to_datetime(msg.get("Date","")).strftime("%Y-%m-%d")
                except Exception:
                    ds = date.today().isoformat()
                uid_s = uid.decode() if isinstance(uid, bytes) else str(uid)
                emails_raw.append({
                    "id": f"{account['id']}_{uid_s}",
                    "subject": subj, "from": sndr,
                    "body": "", "date": ds, "has_attachment": False,
                    "account": account["email"],
                })
            except Exception as ex:
                emit(f"  Header error {uid}: {ex}", "err")

        def _fetch_pdf(uid_str):
            try:
                real_uid = uid_str.split("_")[-1].encode()
                _, data = mail.fetch(real_uid, "(RFC822)")
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



@app.route("/api/setup-zone")
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
        "message": "IMAP host set to imap.zone.eu",
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
            return jsonify({"error": "unauthorized"}), 401

@app.route("/api/auth-check", methods=["POST"])
def api_auth_check():
    access_key = os.environ.get("ACCESS_KEY", "")
    if not access_key:
        return jsonify({"ok": True})
    key = (request.json or {}).get("key", "")
    return jsonify({"ok": key == access_key})


@app.route("/keys")
def keys_page():
    return send_from_directory(TMPL_DIR, "keys.html")


@app.route("/rescan")
def rescan_page():
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
    """Remove invoices with amount=0 and no useful data."""
    invs = load_invoices()
    before = len(invs)
    cleaned = [i for i in invs if (
        float(i.get("amount") or 0) > 0 or
        i.get("status") == "paid"  # keep paid even if 0
    )]
    save_invoices(cleaned)
    removed = before - len(cleaned)
    log.info(f"Cleanup: removed {removed} zero-amount invoices")
    return jsonify({"ok": True, "removed": removed, "remaining": len(cleaned)})

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



@app.route("/api/scan-status")
def api_scan_status():
    """Poll-based scan progress - works even after browser reconnect."""
    # Try live state first
    with _scan_state_lock:
        state = dict(_scan_state_live)
        state["log"] = list(_scan_state_live["log"])

    # If not running, check persisted state
    if not state["running"] and not state["started"]:
        try:
            pf = _DATA_DIR / "scan_progress.json"
            if pf.exists():
                with open(pf, encoding="utf-8") as f:
                    state = json.load(f)
        except Exception:
            pass

    state["invoices_total"] = len(load_invoices())
    return jsonify(state)

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
        scan_lock.release()
    except Exception:
        pass
    with _scan_state_lock:
        _scan_state_live["running"] = False
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
    """Reset scan state - next scan will start from scratch."""
    try:
        empty = {"last_uid": None, "last_date": None, "scan_count": 0, "scanned_uids": []}
        save_state(empty)
        log.info("Scan state reset - will rescan from beginning")
        return jsonify({"ok": True, "message": "Состояние сброшено — следующий скан начнётся с начала"})
    except Exception as ex:
        return jsonify({"ok": False, "error": str(ex)})




@app.route("/api/invoice/<inv_id>/pdf")
def api_serve_pdf(inv_id):
    """Serve stored PDF for an invoice."""
    path = get_pdf_path(inv_id)
    if not path:
        return jsonify({"error":"PDF не найден"}), 404
    invs = load_invoices()
    inv  = next((i for i in invs if i.get("id") == inv_id), {})
    filename = inv.get("pdf_filename","invoice.pdf")
    return send_from_directory(str(_PDF_DIR), path.name,
                               mimetype="application/pdf",
                               as_attachment=False,
                               download_name=filename)

@app.route("/api/invoice/<inv_id>/pdf-info")
def api_pdf_info(inv_id):
    """Check if PDF exists for invoice."""
    path = get_pdf_path(inv_id)
    return jsonify({
        "has_pdf": path is not None,
        "size":    path.stat().st_size if path else 0,
        "name":    path.name if path else None,
    })

@app.route("/api/pdfs/stats")
def api_pdfs_stats():
    return jsonify({
        "count": pdf_count(),
        "dir": str(_PDF_DIR),
        "total_mb": round(sum(f.stat().st_size for f in _PDF_DIR.glob("*.pdf")) / 1024/1024, 2)
             if _PDF_DIR.exists() else 0
    })

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

            alt = ["imap.zone.eu", IMAP_HOST, "mail.zone.ee"]
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
                        if ll.startswith("subject:"):
                            _s = line[8:].strip()
                            try:
                                import email.header as _eh2
                                _parts = _eh2.decode_header(_s)
                                subj = "".join(
                                    (p.decode(enc or "utf-8", errors="replace") if isinstance(p,bytes) else p)
                                    for p,enc in _parts
                                )[:80]
                            except Exception:
                                subj = _s[:80]
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




@app.route("/api/keys", methods=["GET"])
def api_keys_status():
    """Show key pool status."""
    return jsonify({
        "gemini": _gemini_pool.status(),
        "groq":   _groq_pool.status(),
        "openai": _openai_pool.status(),
        "active_provider": _active_provider() or "keyword",
    })

@app.route("/api/keys/add", methods=["POST"])
def api_keys_add():
    """Add a new API key to the pool."""
    data     = request.json or {}
    provider = data.get("provider", "").lower()
    key      = data.get("key", "").strip()
    if not key or len(key) < 10:
        return jsonify({"ok": False, "error": "Key too short"})
    if provider == "gemini":
        _gemini_pool.add(key)
        # Save to config
        n = len(_gemini_pool.keys)
        save_config_value("ai", f"gemini_key_{n}" if n > 1 else "gemini_key", key)
        return jsonify({"ok": True, "pool": _gemini_pool.status()})
    elif provider == "groq":
        _groq_pool.add(key)
        n = len(_groq_pool.keys)
        save_config_value("ai", f"groq_key_{n}" if n > 1 else "groq_key", key)
        return jsonify({"ok": True, "pool": _groq_pool.status()})
    return jsonify({"ok": False, "error": f"Unknown provider: {provider}"})





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


_bg_started = False

def _start_background():
    global _bg_started
    if _bg_started:
        return
    _bg_started = True
    check_notifications()
    log.info("PayCalendar started")

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
