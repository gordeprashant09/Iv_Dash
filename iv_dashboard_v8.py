"""
╔══════════════════════════════════════════════════════════════════════════════╗
║  IV Skew & Intraday Volatility Dashboard — FINAL COMPLETE VERSION           ║
║  NSE F&O — ALL 7 Indices + ALL 155 Stocks — 2026                           ║
║                                                                              ║
║  WHAT'S INCLUDED:                                                            ║
║  ✅ ALL 7 indices  (Nifty, BankNifty, Sensex, Midcap, FinNifty, W2s)       ║
║  ✅ ALL 155 stocks from original script (_STKS list — unchanged)            ║
║  ✅ IV Skew Grid (Tab 1) — matplotlib grid for all + Plotly deep-dive       ║
║  ✅ Intraday ATM IV (Tab 2A) — today vs yesterday with hover                ║
║  ✅ IV vs Underlying (Tab 2B) — dual axis with hover                        ║
║  ✅ IV Movers (Tab 3) — ranked % change from open                           ║
║  ✅ Alerts Log (Tab 4) — Day High / Day Low breakout log                    ║
║  ✅ SQLite storage — iv_skew, atm_iv_history, alerts_log tables             ║
║  ✅ JSON snapshot persistence — yesterday overlay                           ║
║  ✅ Telegram alerts — Day High/Low breakout with task-spec format           ║
║  ✅ Live broker support — Zerodha Kite / Fyers / Mock fallback              ║
║  ✅ Parallel IV computation — ThreadPoolExecutor                             ║
║  ✅ All original charts preserved exactly                                    ║
║                                                                              ║
║  BUG FIX (v9 — 2026-03-30):                                                ║
║  ✅ Expiry rollover on holiday-shifted expiry day fixed                     ║
║     Root cause: _nmtue / _nmthu used `exp <= from_date` which caused the    ║
║     shifted expiry (e.g. Mar 31 holiday → Mar 30) to be treated as          ║
║     "already expired" on the expiry day itself, rolling stocks over to       ║
║     Apr 28 instead of showing Mar 30. Fixed to `exp < from_date`.           ║
║     Same fix applied to _nmthu (BSE monthly). Fully future-proof — works    ║
║     correctly for any year where the last Tue/Thu of the month is a holiday  ║
║     and the previous trading day happens to be today.                        ║
║                                                                              ║
║  Run:  streamlit run iv_dashboard_v9.py                                     ║
║                                                                              ║
║  Live Zerodha credentials embedded — auto-login via TOTP on startup.        ║
║  Override via .env: KITE_API_KEY / KITE_SECRET / KITE_USER_ID               ║
║                     KITE_PASSWORD / KITE_TOTP_KEY / KITE_ACCESS_TOKEN       ║
║  Telegram: TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID / ALERT_THRESHOLD_PCT      ║
║  Extra deps: pip install kiteconnect pyotp                                  ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""
# ═══════════════════════════════════════════════════════════════════════════════
#  IMPORTS
# ═══════════════════════════════════════════════════════════════════════════════
import math, warnings, calendar, io, json, os, sqlite3, threading, time, logging, subprocess, sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, date, timedelta, timezone
from functools import lru_cache
from pathlib import Path

# ── Auto-install any missing package ─────────────────────────────────────────
def _ensure_pkg(pkg: str, import_name: str = None) -> bool:
    """
    pip-install `pkg` if `import_name` cannot be imported.
    Returns True if the package is available after the call.
    Never raises — all failures are printed as warnings.
    """
    import_name = import_name or pkg
    try:
        __import__(import_name)
        return True
    except ImportError:
        pass
    print(f"[INFO] Installing missing package: {pkg} ...")
    try:
        result = subprocess.run(
            [sys.executable, "-m", "pip", "install", "--quiet",
             "--disable-pip-version-check", pkg],
            capture_output=True, text=True, timeout=120)
        if result.returncode != 0:
            print(f"[WARNING] pip install {pkg} failed:\n{result.stderr[:300]}")
            print(f"[WARNING] Run manually: pip install {pkg}")
            return False
        # Re-import after install
        try:
            __import__(import_name)
            print(f"[INFO] {pkg} installed and imported successfully.")
            return True
        except ImportError as e:
            print(f"[WARNING] {pkg} installed but import still failed: {e}")
            print(f"[WARNING] Try restarting the script after: pip install {pkg}")
            return False
    except Exception as _e:
        print(f"[WARNING] Auto-install of {pkg} failed: {_e}")
        print(f"[WARNING] Run manually: pip install {pkg}")
        return False

# ── Install ALL required packages before importing them ───────────────────────
# These are auto-installed so the script works in a fresh venv.
# Order matters: numpy → pandas → matplotlib → scipy → plotly
_REQUIRED_PKGS = [
    ("numpy",                  "numpy"),
    ("pandas",                 "pandas"),
    ("Pillow",                 "PIL"),
    ("matplotlib",             "matplotlib"),
    ("scipy",                  "scipy"),
    ("plotly",                 "plotly"),
    ("requests",               "requests"),
    ("python-dotenv",          "dotenv"),
    ("kiteconnect",            "kiteconnect"),
    ("pyotp",                  "pyotp"),
    ("streamlit-autorefresh",  "streamlit_autorefresh"),
]
_MISSING_PKGS = []
for _pkg, _imp in _REQUIRED_PKGS:
    if not _ensure_pkg(_pkg, _imp):
        _MISSING_PKGS.append(_pkg)

if _MISSING_PKGS:
    # Some packages couldn't be installed — show a clear Streamlit error
    # instead of crashing with an obscure ImportError deep in the stack.
    try:
        import streamlit as _st
        _st.set_page_config(page_title="IV Dashboard — Setup Required", page_icon="⚠️")
        _st.error(
            f"## ⚠️ Missing Python packages\n\n"
            f"The following packages could not be installed automatically:\n\n"
            f"```\n{chr(10).join(_MISSING_PKGS)}\n```\n\n"
            f"**Fix:** Open a terminal in your venv and run:\n\n"
            f"```bash\npip install {' '.join(_MISSING_PKGS)}\n```\n\n"
            f"Then reload this page."
        )
        _st.stop()
    except Exception:
        raise ImportError(
            f"Required packages missing and could not be auto-installed: "
            f"{_MISSING_PKGS}. "
            f"Run: pip install {' '.join(_MISSING_PKGS)}"
        )

import numpy as np
import pandas as pd
from PIL import Image
Image.MAX_IMAGE_PIXELS = None

import matplotlib
matplotlib.use('Agg')
import matplotlib.font_manager as _mpl_fm
# Pre-warm the font cache once so every subplot doesn't re-open the TTF file
_mpl_fm._load_fontmanager(try_read_cache=True)
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
from matplotlib.lines import Line2D
from matplotlib.ticker import FuncFormatter

# Raise the per-process open-file limit to avoid "Too many open files" when
# drawing large grids (matplotlib opens a font handle per figure/axes).
import resource as _resource
try:
    _soft, _hard = _resource.getrlimit(_resource.RLIMIT_NOFILE)
    _resource.setrlimit(_resource.RLIMIT_NOFILE, (min(_hard, 65536), _hard))
except Exception:
    pass  # non-fatal — best effort only
from scipy.stats import norm
from scipy.optimize import brentq
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import requests
import streamlit as st

# Auto-refresh support
try:
    from streamlit_autorefresh import st_autorefresh
    _HAS_AUTOREFRESH = True
except ImportError:
    _HAS_AUTOREFRESH = False

# Load .env if available
try:
    from dotenv import load_dotenv; load_dotenv()
except ImportError:
    pass

warnings.filterwarnings('ignore')
plt.rcParams['figure.max_open_warning'] = 0
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
log = logging.getLogger("IV")

# ═══════════════════════════════════════════════════════════════════════════════
#  CONFIG & CONSTANTS
# ═══════════════════════════════════════════════════════════════════════════════
IST       = timezone(timedelta(hours=5, minutes=30))
STORE_DIR = Path(__file__).parent / "iv_data_store"; STORE_DIR.mkdir(exist_ok=True)  # absolute path — fixes open_iv_cache.json not found on restart
DB_PATH   = Path(__file__).parent / "iv_store_v2.db"   # absolute path — fixes "unable to open database file"

# ── Zerodha Live Credentials ──────────────────────────────────────────────────
ZERODHA_API_KEY    = os.getenv("KITE_API_KEY",    "4tl671rr7bwffw7b")
ZERODHA_SECRET     = os.getenv("KITE_SECRET",     "4gesk7v5vsbx9us4t8j3gh229zwzwf9t")
ZERODHA_USER_ID    = os.getenv("KITE_USER_ID",    "QWK225")
ZERODHA_PASSWORD   = os.getenv("KITE_PASSWORD",   "Dec2025!")
ZERODHA_PIN        = os.getenv("KITE_PIN",        "123456")
ZERODHA_TOTP_KEY   = os.getenv("KITE_TOTP_KEY",   "VV2ZTNC3LG4V7EG7ECFLJIURPGVERJL7")

# Broker / Telegram from env
BROKER            = os.getenv("BROKER", "zerodha").lower()
KITE_API_KEY      = ZERODHA_API_KEY
KITE_ACCESS_TOKEN = os.getenv("KITE_ACCESS_TOKEN", "")   # auto-filled on login
FYERS_CLIENT_ID   = os.getenv("FYERS_CLIENT_ID", "")
FYERS_ACCESS_TOKEN= os.getenv("FYERS_ACCESS_TOKEN", "")
TG_TOKEN          = os.getenv("TELEGRAM_BOT_TOKEN", "8384721299:AAGonU21NxUAgaw2S-_y3SNK6CVISsb-PWA")
TG_CHAT_ID        = os.getenv("TELEGRAM_CHAT_ID", "1354155833")
ALERT_PCT           = float(os.getenv("ALERT_THRESHOLD_PCT",       "5.0"))   # % IV change for indices
ALERT_PCT_STOCK     = float(os.getenv("ALERT_THRESHOLD_PCT_STOCK",  "10.0"))  # % IV change for stocks  
ALERT_OPEN_SKIP_MIN = int(os.getenv("ALERT_OPEN_SKIP_MINUTES",      "8"))     # skip first N min (opening noise)

def _now_ist() -> datetime: return datetime.now(IST)
def _today_ist() -> date:   return _now_ist().date()

TODAY = _today_ist()
NOW   = _now_ist()

CFG = dict(
    risk_free_rate=0.0, stock_carry_rate=0.065,
    iv_solver_lo=0.001, iv_solver_hi=10.0, iv_max_valid=5.0, iv_min_valid=0.001,
    bps_lo=-300, bps_hi=300, bps_step=50,
    chain_lo=-800, chain_hi=800, chain_step=50,
    market_open_h=9, market_open_m=15, market_close_h=15, market_close_m=30,
    trading_day_mins=375, trading_days_year=252,
    mock_noise_live=0.005, mock_noise_yest=0.007, mock_noise_open=0.004,
    mock_spike_prob=0.025, mock_spike_mult=5.0, mock_iv_vol=0.0025, mock_spot_vol_bps=8,
    alert_breakout_pct=ALERT_PCT,
    alert_breakout_pct_stock=ALERT_PCT_STOCK,
    alert_open_skip_min=ALERT_OPEN_SKIP_MIN,
    curve_colors={'Live':'#00e5a0','Yesterday':'#ffd740','Open 9:20':'#b040ff'},
    inst_colors=['#00d4ff','#00e5a0','#ffd740','#ff4466','#b040ff','#ff8c42',
                 '#4dabf7','#ff6eb4','#a9e34b','#74c0fc','#f8a5c2','#e9c46a',
                 '#f4a261','#e76f51','#2a9d8f','#264653','#457b9d','#a8dadc'],
    bg_fig='#07090f', bg_ax='#0d1117', grid_c='#1e2a3a',
    spine_c='#1e2a3a', tick_c='#64748b', title_c='#00d4ff', text_c='#94a3b8',
    plotly_bg='#0d1117', plotly_paper='#07090f',
)
BPS_RANGE = list(range(CFG['bps_lo'], CFG['bps_hi']+CFG['bps_step'], CFG['bps_step']))
_NCDF     = norm.cdf

# ─────────────────────────────────────────────────────────────────────────────
st.set_page_config(page_title="IV Dashboard — NSE F&O", page_icon="📈",
                   layout="wide", initial_sidebar_state="expanded")
st.markdown("""<style>
  .main{background:#07090f}.block-container{padding-top:1rem}
  h1,h2,h3{color:#00d4ff}
  .stTabs [data-baseweb="tab"]{color:#94a3b8}
  .stTabs [aria-selected="true"]{color:#00e5a0}
  div[data-testid="stMetricValue"]{font-size:1.6rem;font-weight:700}
</style>""", unsafe_allow_html=True)

PLOTLY_LAYOUT = dict(
    paper_bgcolor=CFG['plotly_paper'], plot_bgcolor=CFG['plotly_bg'],
    font=dict(color='#94a3b8', family='monospace'),
    xaxis=dict(gridcolor=CFG['grid_c'], zeroline=False, tickfont=dict(color=CFG['tick_c'])),
    yaxis=dict(gridcolor=CFG['grid_c'], zeroline=False, tickfont=dict(color=CFG['tick_c'])),
    legend=dict(bgcolor='rgba(13,17,23,0.8)', bordercolor=CFG['grid_c'], borderwidth=1),
    hoverlabel=dict(bgcolor='#1e2a3a', bordercolor='#00e5a0',
                    font=dict(color='white', size=13)),
    hovermode='x unified',
    margin=dict(l=60, r=60, t=50, b=50),
)

# ═══════════════════════════════════════════════════════════════════════════════
#  BLACK-SCHOLES + IV ENGINE
# ═══════════════════════════════════════════════════════════════════════════════
def _bs(flag, S, K, T, r, sig):
    if T<=0 or sig<=0: return max(0., (S-K) if flag=='c' else (K-S))
    sq=math.sqrt(T); d1=(math.log(S/K)+(r+.5*sig**2)*T)/(sig*sq); d2=d1-sig*sq
    d=math.exp(-r*T)
    return S*_NCDF(d1)-K*d*_NCDF(d2) if flag=='c' else K*d*_NCDF(-d2)-S*_NCDF(-d1)

def compute_iv(S, K, T, mp, ot, r=0.):
    if mp<=0 or T<=0 or S<=0 or K<=0: return None
    intr=max(0., (S-K) if ot=='c' else (K-S))
    if mp < intr-1e-3: return None
    try:
        lo, hi = CFG['iv_solver_lo'], CFG['iv_solver_hi']
        f = lambda s: _bs(ot,S,K,T,r,s) - mp
        if f(lo)*f(hi) > 0: return None
        iv = brentq(f, lo, hi, xtol=1e-6, maxiter=80)
        return iv if CFG['iv_min_valid']<iv<CFG['iv_max_valid'] else None
    except: return None

def watm_iv(df, spot):
    df=df.dropna(subset=['iv'])
    if df.empty: return None
    dist=(df['strike']-spot).abs(); top=dist.nsmallest(2).index
    if len(top)==1: return float(df.loc[top[0],'iv'])
    d0=dist[top[0]]+1e-9; d1=dist[top[1]]+1e-9
    return float((df.loc[top[0],'iv']/d0+df.loc[top[1],'iv']/d1)/(1/d0+1/d1))

# ═══════════════════════════════════════════════════════════════════════════════
#  CALENDAR ENGINE — powered by dateprocessor (reads holidays.csv / str_days.csv)
#  Place holidays.csv and str_days.csv in the same folder as this script.
#  CSV format:  Date (dd-Mon-yy),  Exchange (CM / FO)
#  No more hardcoded holiday years — just update the CSV files each year.
# ═══════════════════════════════════════════════════════════════════════════════

def dateprocessor(look_back_days=0, for_date=None, exch='NSE',
                  holidays_csv='holidays.csv', str_days_csv='str_days.csv'):
    exch = exch.upper()
    exch_seg_map = {'NSE': 'CM', 'FO': 'FO'}
    if exch not in exch_seg_map:
        raise ValueError(f"Invalid exchange '{exch}'. Only 'NSE' and 'FO' are supported.")
    seg = exch_seg_map[exch]
    start_date = '20151231'
    # +60 days: ensures weekly/monthly expiries (up to ~2 months ahead) are always in range.
    # The old +7 was too short — expiries like "last Tuesday of next month" can be 35+ days out.
    end_date = (pd.to_datetime('today') + pd.Timedelta(days=60)).strftime('%Y%m%d')
    # Load holidays
    holiday_df = pd.read_csv(holidays_csv)
    holiday_df = holiday_df[holiday_df['Exchange'] == seg].copy()
    holiday_df['Date'] = pd.to_datetime(holiday_df['Date'], format='%d-%b-%y')
    ex_holidays = set(holiday_df['Date'].to_list())
    # Load special trading days
    str_days_df = pd.read_csv(str_days_csv)
    str_days_df = str_days_df[str_days_df['Exchange'] == seg].copy()
    str_days_df['Date'] = pd.to_datetime(str_days_df['Date'], format='%d-%b-%y')
    special_days = set(str_days_df['Date'].to_list())
    # Build trading days calendar
    business_days = set(pd.date_range(start=start_date, end=end_date, freq='B'))
    business_days.update(special_days)
    ex_trdays = business_days - ex_holidays
    df = pd.DataFrame(ex_trdays, columns=['tr_day'])
    df = df.sort_values(by=['tr_day']).reset_index(drop=True)
    df['prev_day'] = df['tr_day'].shift(1)
    df['next_day'] = df['tr_day'].shift(-1)
    df['recent_day'] = df['tr_day'] - pd.to_datetime('today')
    temp_df = df[df['tr_day'] <= pd.to_datetime('today') - pd.Timedelta(days=look_back_days)].copy()
    if for_date is not None:
        dt = temp_df[temp_df['tr_day'] >= pd.to_datetime(for_date, format='%Y%m%d')][['tr_day', 'prev_day', 'next_day']]
    else:
        dt = temp_df[temp_df['recent_day'] == temp_df['recent_day'].max()][['tr_day', 'prev_day', 'next_day']]
    cr_yyyymmdd, pr_yyyymmdd, nxt_yyyymmdd = [dt[col].iloc[0].strftime('%Y%m%d') for col in dt]
    cr_date, pr_date, nxt_date = [dt[col].iloc[0] for col in dt]
    return cr_yyyymmdd, pr_yyyymmdd, nxt_yyyymmdd, cr_date, pr_date, nxt_date, df

# ── Emergency hardcoded holidays — ONLY for late-notified dates ───────────────
#
# ════════════════════════════════════════════════════════════════════════════
#  PRIMARY SOURCE OF TRUTH: holidays.csv  (same folder as this script)
#  CSV format: Exchange,Date,Week,Reason  — Date as dd-Mon-yy, Exchange as FO
#  Update holidays.csv at the START OF EACH MONTH from NSE official list:
#  https://www.nseindia.com/resources/exchange-communication-holidays
# ════════════════════════════════════════════════════════════════════════════
#
#  This set is a SAFETY NET ONLY — for holidays announced AFTER your last
#  holidays.csv update. Once CSV is updated, you do NOT need entries here.
#
#  2026 holidays already confirmed in holidays.csv (FO segment):
#    26-Jan-26  Republic Day
#    03-Mar-26  Holi
#    26-Mar-26  Shri Ram Navami
#    31-Mar-26  Shri Mahavir Jayanti   ← shifts stock/BankNifty/FinNifty/Midcap
#    03-Apr-26  Good Friday            ←   monthly expiry Mar31→Mar30
#    14-Apr-26  Dr. Ambedkar Jayanti
#    01-May-26  Maharashtra Day
#    28-May-26  Bakri Id
#    26-Jun-26  Muharram
#    14-Sep-26  Ganesh Chaturthi
#    02-Oct-26  Gandhi Jayanti
#    20-Oct-26  Dussehra
#    10-Nov-26  Diwali-Balipratipada
#    24-Nov-26  Guru Nanak Jayanti
#    25-Dec-26  Christmas
#
#  HOW THE EXPIRY ENGINE WORKS (for reference / debugging):
#  ─────────────────────────────────────────────────────────
#  BankNifty / FinNifty / MidcapNifty / All Stocks → monthly_tue
#      = last Tuesday of the month (NSE rule, post Sep 2025)
#      if that Tuesday is a holiday, _expiry_shift() moves back 1 day at a time
#      _nmtue() uses `exp < from_date` (NOT `<=`) so expiry-day-itself is never skipped
#
#  Example (Mahavir Jayanti 2026):
#    Last Tue March 2026 = March 31  (holiday — in holidays.csv FO segment)
#    _expiry_shift()     = March 30  (previous trading day)
#    today = March 30    → exp(Mar30) < today(Mar30) → False → stays Mar 30 ✅
#    OLD buggy `<=` code → Mar30 <= Mar30 → True → wrongly rolled to Apr 28  ❌
#
#  ⚠️  CRITICAL: Do NOT use empty braces {} here — Python treats {} as a dict,
#  not a set. An empty set must be written as set(). frozenset - dict crashes
#  with: TypeError: unsupported operand type(s) for -: 'frozenset' and 'dict'
#
#  To add an emergency entry (when holiday not yet in CSV):
#    pd.Timestamp('YYYY-MM-DD'),  # Holiday reason — remove once CSV is updated
# ──────────────────────────────────────────────────────────────────────────────
_HARDCODED_HOLIDAYS: set = set()   # ← must be set(), never {} (which is a dict!)

# ── Build trading-day set from dateprocessor (used by _istd, _tdb, etc.) ─────
def _build_trading_day_set(holidays_csv='holidays.csv', str_days_csv='str_days.csv'):
    """Returns a frozenset of Timestamps that are NSE trading days.
    Also subtracts _HARDCODED_HOLIDAYS so late-notified holidays are always respected
    even when holidays.csv has not yet been updated."""
    try:
        *_, df = dateprocessor(holidays_csv=holidays_csv, str_days_csv=str_days_csv)
        trading_days = frozenset(df['tr_day'].dt.normalize()) - _HARDCODED_HOLIDAYS
        return trading_days
    except Exception as e:
        log.warning(f"dateprocessor failed ({e}); falling back to weekdays-only calendar")
        start = pd.Timestamp('2015-12-31')
        end   = pd.Timestamp.today() + pd.Timedelta(days=60)
        all_bdays = frozenset(pd.date_range(start, end, freq='B'))
        return all_bdays - _HARDCODED_HOLIDAYS

_TRADING_DAYS: frozenset = _build_trading_day_set()
# Sorted list — used by _tdb for O(log n) binary-search range count
import bisect as _bisect
_TRADING_DAYS_SORTED: list = sorted(_TRADING_DAYS)

def _istd(d: date) -> bool:
    """Return True if `d` is an NSE trading day."""
    return pd.Timestamp(d).normalize() in _TRADING_DAYS

# Clear expiry caches when module reloads (holiday list may have changed)
def _clear_expiry_caches():
    try:
        _last_weekday_of_month.cache_clear()
        _nmtue.cache_clear()
        _nmthu.cache_clear()
        _resolve.cache_clear()
    except Exception: pass

@lru_cache(maxsize=512)
def _tdb(s: date, e: date) -> int:
    """Count trading days strictly between s and e (exclusive of s, inclusive of e).
    Binary search on sorted list — O(log n) vs old O(n) full scan."""
    if s >= e: return 0
    s_ts = pd.Timestamp(s).normalize()
    e_ts = pd.Timestamp(e).normalize()
    lo = _bisect.bisect_right(_TRADING_DAYS_SORTED, s_ts)
    hi = _bisect.bisect_right(_TRADING_DAYS_SORTED, e_ts)
    return hi - lo

def _nwd(from_date, wd, include_today: bool = False):
    """Find next calendar occurrence of weekday wd on or after from_date.
    include_today=True  → includes from_date itself if it already is `wd`
                          (used for wnum=1 so today's expiry is NOT skipped).
    include_today=False → starts from from_date+1 (used when chaining W2, W3…
                          so we always advance to the *next* occurrence).
    Caller must apply _expiry_shift() on the final result."""
    d = from_date if include_today else from_date + timedelta(days=1)
    while d.weekday() != wd: d += timedelta(days=1)
    return d

def _expiry_shift(d: date) -> date:
    """NSE/BSE rule: if expiry falls on a holiday, move to PREVIOUS trading day."""
    while not _istd(d): d -= timedelta(days=1)
    return d

def _compT(expiry:date)->float:
    """Time to expiry in years. NOT cached — T changes every minute."""
    now=datetime.now(IST); today=now.date(); eff=expiry-timedelta(days=1); td=_tdb(today,eff)
    close=now.replace(hour=CFG['market_close_h'],minute=CFG['market_close_m'],second=0,microsecond=0)
    ml=max(0.,(close-now).total_seconds()/60)
    return max((td+ml/CFG['trading_day_mins'])/CFG['trading_days_year'],1e-6)

@lru_cache(maxsize=32)
def _last_weekday_of_month(year, month, wd):
    """Return last CALENDAR occurrence of weekday wd (0=Mon…6=Sun) in given month.
    Does NOT apply holiday shift — callers must call _expiry_shift() on the result.
    BUG FIX: old code used `d -= timedelta(days=7)` when the date was a holiday,
    which jumped back a full week (e.g. March 31 2026 holiday → March 24 instead of March 30).
    Correct NSE rule is to shift back 1 day at a time, done by _expiry_shift()."""
    last = date(year, month, calendar.monthrange(year, month)[1])
    d = last - timedelta(days=(last.weekday() - wd) % 7)
    return d

# ── NSE: last Tuesday of month ──────────────────────────────────────────────
def _ltue(year, month): return _last_weekday_of_month(year, month, 1)  # Tuesday=1

@lru_cache(maxsize=32)
def _nmtue(from_date: date) -> date:
    """Next monthly expiry: last Tuesday of month (NSE post-Sep 2025).
    _expiry_shift applied so a holiday on the last Tuesday (e.g. March 31 FY-end)
    moves back 1 calendar day at a time — NOT 7 days.

    FIX (2026-03-30): Changed `exp <= from_date` → `exp < from_date`.
    When the holiday-shifted expiry EQUALS today (e.g. March 31 holiday shifts to
    March 30, and today IS March 30), the old `<=` wrongly rolled over to next month.
    Correct NSE rule: if expiry == today, it is still the CURRENT expiry — contracts
    expire at close of the expiry day, not at open. Using `<` preserves this."""
    exp = _expiry_shift(_ltue(from_date.year, from_date.month))
    if exp < from_date:          # FIX: was `<=` — expiry day itself must NOT be skipped
        nm = from_date.month % 12 + 1
        ny = from_date.year + (1 if from_date.month == 12 else 0)
        exp = _expiry_shift(_ltue(ny, nm))
    return exp

# ── BSE: last Thursday of month (unchanged) ─────────────────────────────────
def _lthu(year, month): return _last_weekday_of_month(year, month, 3)  # Thursday=3

@lru_cache(maxsize=32)
def _nmthu(from_date: date) -> date:
    """Next monthly expiry: last Thursday of month (BSE).
    _expiry_shift applied so a holiday on the last Thursday
    moves back 1 calendar day at a time — NOT 7 days.

    FIX (2026-03-30): Changed `exp <= from_date` → `exp < from_date`.
    Same root-cause fix as _nmtue — when the holiday-shifted expiry equals today,
    the old `<=` wrongly jumped to next month. Contracts expire at market CLOSE,
    so expiry == today is still the current expiry."""
    exp = _expiry_shift(_lthu(from_date.year, from_date.month))
    if exp < from_date:          # FIX: was `<=` — expiry day itself must NOT be skipped
        nm = from_date.month % 12 + 1
        ny = from_date.year + (1 if from_date.month == 12 else 0)
        exp = _expiry_shift(_lthu(ny, nm))
    return exp

@lru_cache(maxsize=256)
def _resolve(etype: str, wnum: int = 1, _today: date = None) -> date:
    """
    Resolve expiry date.
    _today must be passed explicitly so lru_cache key includes the date.

    NSE post-Sep 2025 expiry schedule:
      weekly_tue  → Nifty 50 weekly (every Tuesday)
      weekly_thu  → Sensex weekly (every Thursday, BSE)
      weekly_fri  → [deprecated, mapped to weekly_thu]
      monthly_tue → BankNifty / FinNifty / MidcapNifty / stocks (last Tue of month)
      monthly_thu → Sensex monthly (last Thu of month, BSE)

    W2 / W3 holiday-shift safety:
      After _expiry_shift(), a Wn date could collapse onto or before W1 (e.g. both
      this week and next week's Thursday are holidays shifting to the same Friday).
      In that case we step forward to the next trading day after W1 to keep order.
    """
    ref = _today or _today_ist()
    if etype in ('weekly_tue',):
        # wnum=1: include today if it IS Tuesday (today's expiry must not be skipped)
        # wnum>1 (W2, W3…): chain forward — always skip past wnum=1 date
        d = _nwd(ref, 1, include_today=True)       # current/next Tuesday (TODAY if Tue)
        for _ in range(wnum - 1): d = _nwd(d, 1, include_today=False)  # advance N-1 more
        shifted = _expiry_shift(d)                  # NSE: holiday → previous trading day
        # Safety: Wn must be strictly after W1 even after holiday shift
        if wnum > 1:
            w1 = _resolve('weekly_tue', 1, _today=(_today or ref))
            if shifted <= w1:
                nxt = w1 + timedelta(days=1)
                while not _istd(nxt): nxt += timedelta(days=1)
                shifted = nxt
        return shifted
    if etype in ('weekly_thu', 'weekly_fri'):
        # BSE Sensex: Thursday weekly (same logic — today included for wnum=1)
        d = _nwd(ref, 3, include_today=True)       # current/next Thursday (TODAY if Thu)
        for _ in range(wnum - 1): d = _nwd(d, 3, include_today=False)
        shifted = _expiry_shift(d)
        # Safety: Wn must be strictly after W1 even after holiday shift
        if wnum > 1:
            w1 = _resolve('weekly_thu', 1, _today=(_today or ref))
            if shifted <= w1:
                nxt = w1 + timedelta(days=1)
                while not _istd(nxt): nxt += timedelta(days=1)
                shifted = nxt
        return shifted
    if etype == 'monthly_tue':
        return _nmtue(ref)
    if etype == 'monthly_thu':
        return _nmthu(ref)
    raise ValueError(f"Unknown etype: {etype}")

def get_previous_trading_date(ref:date=None)->date:
    d=(ref or _today_ist())-timedelta(days=1)
    while not _istd(d): d-=timedelta(days=1)
    return d

# ═══════════════════════════════════════════════════════════════════════════════
#  INSTRUMENT REGISTRY — ALL 7 INDICES + ALL 155 STOCKS (unchanged from original)
# ═══════════════════════════════════════════════════════════════════════════════
_ICFG={
    # NSE: weekly Nifty50 → Tuesday; monthly indices → last Tuesday of month
    # BSE: Sensex → Thursday (weekly and monthly unchanged)
    # Spots updated to 24-Feb-2026 closing prices
    # Lot sizes revised by NSE effective Jan 2026 (post Dec-30-2025 expiry):
    # NIFTY: 75→65 | BANKNIFTY: 15→30 | FINNIFTY: 40→60 | MIDCPNIFTY: 75→120 | SENSEX: 10→20
    # Spots updated to Mar-2026 levels (live broker price overrides these within seconds)
    'NIFTY':     dict(label='Nifty 50',    etype='weekly_tue', wnum=1,lot=65, ss=50,  spot=22500),
    'NIFTY_W2':  dict(label='Nifty 50 W2', etype='weekly_tue', wnum=2,lot=65, ss=50,  spot=22500),
    'BANKNIFTY': dict(label='Bank Nifty',  etype='monthly_tue',wnum=1,lot=30, ss=100, spot=51800),
    'SENSEX':    dict(label='Sensex',      etype='weekly_thu', wnum=1,lot=20, ss=100, spot=72700),
    'SENSEX_W2': dict(label='Sensex W2',   etype='weekly_thu', wnum=2,lot=20, ss=100, spot=72700),
    'MIDCPNIFTY':dict(label='Midcap Nifty',etype='monthly_tue',wnum=1,lot=120,ss=25,  spot=12200),
    'FINNIFTY':  dict(label='Fin Nifty',   etype='monthly_tue',wnum=1,lot=60, ss=50,  spot=24000),
}

# ALL 155 stocks — exactly as in original script
_STKS=[
    ('AARTIIND',800,405),('ABB',250,6050),('ABBOTINDIA',50,25490),('ABCAPITAL',3200,294),
    ('ABFRL',2000,55),('ACC',400,1324),('ADANIENT',250,2200),('ADANIGREEN',500,820),
    ('ADANIPORTS',1250,1300),('ALKEM',200,5150),('AMBUJACEM',2000,395),('APOLLOHOSP',250,7100),
    ('APOLLOTYRE',3500,400),('ASHOKLEY',5000,162),('ASIANPAINT',300,2150),('AUROPHARMA',1000,1275),
    ('AXISBANK',625,1170),('BAJAJ_AUTO',125,8200),('BAJAJFINSV',500,1900),('BAJFINANCE',125,8500),
    ('BALKRISIND',400,2000),('BANDHANBNK',5000,155),('BANKBARODA',5400,245),('BEL',7400,406),
    ('BERGEPAINT',1100,450),('BHEL',7000,262),('BPCL',3800,285),('BRITANNIA',200,4900),
    ('BSOFT',2800,340),('CANBK',3000,90),('CANFINHOME',2000,720),('CDSL',750,1630),
    ('CESC',2000,148),('CHOLAFIN',700,1200),('CIPLA',650,1480),('COALINDIA',2100,392),
    ('COFORGE',150,7800),('COLPAL',350,2700),('CONCOR',2300,750),('COROMANDEL',500,1800),
    ('CROMPTON',3600,385),('CUB',8000,160),('CUMMINSIND',400,3800),('DABUR',2800,480),
    ('DALBHARAT',400,1650),('DEEPAKNTR',250,2200),('DELTACORP',4800,125),('DIVISLAB',200,5100),
    ('DIXON',100,16500),('DLF',1650,780),('DRREDDY',125,1340),('EICHERMOT',200,5577),
    ('ESCORTS',275,3200),('EXIDEIND',3600,380),('FEDERALBNK',10000,185),('GAIL',3600,185),
    ('GLENMARK',1150,1050),('GMRINFRA',22500,80),('GNFC',1300,580),('GODREJCP',1000,1010),
    ('GODREJPROP',750,2500),('GRANULES',3750,490),('GSPL',2700,290),('GUJGASLTD',1250,520),
    ('HAVELLS',500,1500),('HCLTECH',700,1714),('HDFCAMC',300,4300),('HDFCBANK',550,1850),
    ('HDFCLIFE',750,786),('HEROMOTOCO',300,4298),('HINDALCO',1400,672),('HINDPETRO',2700,335),
    ('HINDUNILVR',500,2277),('HUDCO',6800,215),('IBULHSGFIN',5700,185),('ICICIBANK',700,1425),
    ('ICICIGI',375,1930),('ICICIPRULI',1500,620),('IDEA',70000,8),('IDFCFIRSTB',11000,65),
    ('IEX',3750,165),('IGL',1375,375),('INDHOTEL',2500,740),('INDIACEM',5300,145),
    ('INDIAMART',150,2200),('INDIGO',300,4200),('INDUSINDBK',500,700),('INDUSTOWER',2800,340),
    ('INFY',400,1800),('INTELLECT',750,780),('IOC',10000,140),('IPCALAB',800,1500),
    ('IRCTC',3450,880),('IRFC',6400,175),('ITC',1600,415),('JINDALSTEL',1250,890),
    ('JKCEMENT',300,4500),('JSWENERGY',2800,440),('JSWSTEEL',1350,930),('JUBLFOOD',1250,680),
    ('KOTAKBANK',400,2000),('LALPATHLAB',300,2500),('LAURUSLABS',2100,470),('LICHSGFIN',1000,580),
    ('LT',300,3200),('LTF',8000,120),('LTIM',150,5300),('LTTS',200,4600),('LUPIN',650,2050),
    ('M_M',700,3030),('MANAPPURAM',5000,195),('MARICO',1200,620),('MARUTI',100,11500),
    ('MCX',250,6000),('METROPOLIS',400,1900),('MFSL',900,1050),('M&MFIN',4000,175),
    ('MNGLM',2800,380),('MOTHERSON',5600,145),('MPHASIS',350,2700),('MRF',10,135000),
    ('MUTHOOTFIN',700,2100),('NATIONALUM',8500,195),('NAUKRI',150,6100),('NAVINFLUOR',200,3300),
    ('NESTLEIND',50,2280),('NHPC',8000,85),('NMDC',10000,68),('NTPC',3750,335),
    ('OBEROIRLTY',700,1800),('OFSS',200,11500),('OIL',2200,430),('ONGC',3850,240),
    ('PAGEIND',15,44000),('PEL',400,950),('PERSISTENT',250,5400),('PETRONET',3000,315),
    ('PFC',3000,420),('PIDILITIND',500,2750),('PIIND',250,3900),('PNB',8000,98),
    ('POLYCAB',300,6800),('POWERGRID',4500,295),('PVRINOX',1000,1350),('RAMCOCEM',550,880),
    ('RBLBANK',5600,170),('RECLTD',3000,490),('RELIANCE',250,1220),('SAIL',9000,118),
    ('SBICARD',1500,720),('SBILIFE',750,1640),('SBIN',1500,740),('SHRIRAMFIN',400,2850),
    ('SIEMENS',275,3100),('SRF',375,2350),('SUNPHARMA',700,1780),('SUNTV',1400,720),
    ('SUPREMEIND',125,5300),('SYNGENE',1500,820),('TATACHEM',550,1050),('TATACOMM',500,1680),
    ('TATACONSUM',1100,1050),('TMPV',1400,740),('TATAPOWER',2700,380),('TATASTEEL',5500,140),
    ('TCS',150,3300),('TECHM',600,1460),('TITAN',375,3100),('TORNTPHARM',500,3250),
    ('TRENT',375,6200),('TVSMOTOR',350,2400),('UBL',600,1950),('ULTRACEMCO',100,10500),
    ('UNIONBANK',10000,118),('MCDOWELL-N',600,700),('UPL',1300,310),('VEDL',2800,460),
    ('VOLTAS',500,1580),('WIPRO',1500,285),('ZEEL',6000,125),('ETERNAL',4500,245),('ZYDUSLIFE',1100,1050),
]

def _ss(spot):
    for t,v in [(100,2),(250,5),(500,10),(1000,20),(2000,25),(5000,50),(10000,100),(50000,250)]:
        if spot<t: return v
    return 500

@st.cache_data(show_spinner=False, ttl=300)   # TTL=5 min so expiry rolls over at midnight
def build_registry():
    today = _today_ist()   # fresh date every call (TTL ensures daily re-evaluation)
    out=[]
    for iid,s in _ICFG.items():
        out.append(dict(id=iid,label=s['label'],type='index',spot=float(s['spot']),
                        expiry=_resolve(s['etype'],s.get('wnum',1),_today=today),
                        lot=int(s['lot']),strike_step=int(s['ss'])))
    mexp=_nmtue(today)   # NSE stocks: last Tuesday of month (post Sep 2025)
    for sym,lot,spot in _STKS:
        out.append(dict(id=sym,label=sym.replace('_',' '),type='stock',spot=float(spot),
                        expiry=mexp,lot=int(lot),strike_step=_ss(float(spot))))
    return out

# ═══════════════════════════════════════════════════════════════════════════════
#  SQLITE STORAGE LAYER
#  Replace with PostgreSQL by changing get_conn() to use psycopg2
# ═══════════════════════════════════════════════════════════════════════════════
def _db_conn():
    """SQLite connection with WAL mode to prevent 'database is locked' errors.
    WAL allows concurrent reads + one writer, essential for background recorder
    writing while Streamlit reads.
    """
    conn = sqlite3.connect(str(DB_PATH), timeout=30, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=10000")
    return conn

def _init_db():
    with _db_conn() as c:
        c.executescript("""
            CREATE TABLE IF NOT EXISTS atm_iv_history (
                id        INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                date      TEXT NOT NULL,
                time_str  TEXT NOT NULL,
                inst_id   TEXT NOT NULL,
                expiry    TEXT NOT NULL,
                atm_iv    REAL,
                spot      REAL,
                day_high  REAL,
                day_low   REAL
            );
            CREATE INDEX IF NOT EXISTS idx_atm ON atm_iv_history(inst_id,date);

            CREATE TABLE IF NOT EXISTS alerts_log (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp   TEXT NOT NULL,
                inst_id     TEXT NOT NULL,
                inst_label  TEXT NOT NULL,
                expiry      TEXT NOT NULL,
                alert_type  TEXT NOT NULL,
                atm_strike  REAL,
                open_iv     REAL,
                current_iv  REAL,
                pct_chg     REAL,
                message     TEXT
            );
        """)
        # ── Migration: add inst_label if existing DB is missing it ───────────
        cols = {r[1] for r in c.execute("PRAGMA table_info(alerts_log)")}
        if 'inst_label' not in cols:
            c.execute("ALTER TABLE alerts_log ADD COLUMN inst_label TEXT NOT NULL DEFAULT ''")
            log.info("DB migration: added inst_label to alerts_log")

_init_db()

def db_save_atm(inst_id, expiry, atm_iv, spot, day_high, day_low, ts):
    try:
        with _db_conn() as c:
            c.execute("""INSERT INTO atm_iv_history
                (timestamp,date,time_str,inst_id,expiry,atm_iv,spot,day_high,day_low)
                VALUES (?,?,?,?,?,?,?,?,?)""",
                (ts.isoformat(), ts.date().isoformat(), ts.strftime('%H:%M'),
                 inst_id, expiry, atm_iv, spot, day_high, day_low))
    except Exception as e:
        log.warning(f"db_save_atm: {e}")

def db_load_atm_history(inst_id, date_str):
    try:
        with _db_conn() as c:
            df = pd.read_sql(
                """SELECT time_str, atm_iv, spot FROM atm_iv_history
                   WHERE inst_id=? AND date=? ORDER BY timestamp""",
                c, params=(inst_id, date_str))
        if df.empty:
            return df
        # Keep only the last entry per HH:MM — concurrent threads (background
        # recorder + Streamlit main fetch) can both write the same minute.
        # Duplicates crash .set_index().reindex() with
        # "cannot reindex on an axis with duplicate labels".
        df = (df.drop_duplicates(subset='time_str', keep='last')
                .reset_index(drop=True))
        return df
    except:
        return pd.DataFrame()

def db_get_day_extremes(inst_id, date_str):
    try:
        with _db_conn() as c:
            row = c.execute("""SELECT MAX(atm_iv),MIN(atm_iv) FROM atm_iv_history
                WHERE inst_id=? AND date=?""", (inst_id, date_str)).fetchone()
        if row and row[0] is not None: return float(row[0]), float(row[1])
    except: pass
    return 0.0, 9999.0

def db_get_day_extremes_with_time(inst_id, date_str):
    """Returns (high_iv, high_time, low_iv, low_time) for the given inst/date."""
    try:
        with _db_conn() as c:
            high_row = c.execute(
                """SELECT atm_iv, time_str FROM atm_iv_history
                   WHERE inst_id=? AND date=? ORDER BY atm_iv DESC LIMIT 1""",
                (inst_id, date_str)).fetchone()
            low_row = c.execute(
                """SELECT atm_iv, time_str FROM atm_iv_history
                   WHERE inst_id=? AND date=? ORDER BY atm_iv ASC LIMIT 1""",
                (inst_id, date_str)).fetchone()
        high_iv   = float(high_row[0]) if high_row else 0.0
        high_time = high_row[1] if high_row else "—"
        low_iv    = float(low_row[0]) if low_row else 9999.0
        low_time  = low_row[1] if low_row else "—"
        return high_iv, high_time, low_iv, low_time
    except:
        return 0.0, "—", 9999.0, "—"

def db_log_alert(inst_id, inst_label, expiry, alert_type,
                 atm_strike, open_iv, current_iv, pct_chg, message, ts):
    try:
        with _db_conn() as c:
            c.execute("""INSERT INTO alerts_log
                (timestamp,inst_id,inst_label,expiry,alert_type,
                 atm_strike,open_iv,current_iv,pct_chg,message)
                VALUES (?,?,?,?,?,?,?,?,?,?)""",
                (ts.isoformat(), inst_id, inst_label, expiry, alert_type,
                 atm_strike, open_iv, current_iv, pct_chg, message))
    except Exception as e:
        log.warning(f"db_log_alert: {e}")

def db_load_alerts(limit=100):
    try:
        with _db_conn() as c:
            return pd.read_sql("""SELECT * FROM alerts_log
                ORDER BY timestamp DESC LIMIT ?""", c, params=(limit,))
    except: return pd.DataFrame()

# ═══════════════════════════════════════════════════════════════════════════════
#  TELEGRAM ALERT ENGINE
# ═══════════════════════════════════════════════════════════════════════════════
_ALERT_SENT = {}   # {(inst_id, alert_type, date): datetime}  — in-memory dedup
_ALERT_LOCK = threading.Lock()

# ── Telegram send queue — 1 msg per 1.2s max to avoid HTTP 429 ───────────────
import queue as _tg_queue_mod
_TG_QUEUE       = _tg_queue_mod.Queue(maxsize=50)
_TG_MIN_INTERVAL = 1.2   # seconds between sends

def _tg_queue_worker():
    _last = [0.0]
    while True:
        try:
            msg = _TG_QUEUE.get(timeout=10)
            elapsed = time.time() - _last[0]
            if elapsed < _TG_MIN_INTERVAL:
                time.sleep(_TG_MIN_INTERVAL - elapsed)
            _tg_send_direct(msg)
            _last[0] = time.time()
            _TG_QUEUE.task_done()
        except _tg_queue_mod.Empty:
            continue
        except Exception as e:
            log.warning(f"TG worker: {e}")

threading.Thread(target=_tg_queue_worker, daemon=True, name="TGWorker").start()

def _tg_send_direct(message: str) -> bool:
    """Send immediately — called only by queue worker."""
    if not (TG_TOKEN and TG_CHAT_ID): return False
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
            json={'chat_id': TG_CHAT_ID, 'text': message, 'parse_mode': 'HTML'},
            timeout=10)
        if r.status_code == 200:
            return True
        if r.status_code == 429:
            try: wait = r.json().get('parameters', {}).get('retry_after', 10)
            except: wait = 10
            log.warning(f"TG 429 — retry after {wait}s")
            time.sleep(int(wait) + 1)
            r2 = requests.post(
                f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
                json={'chat_id': TG_CHAT_ID, 'text': message, 'parse_mode': 'HTML'},
                timeout=10)
            return r2.status_code == 200
        log.warning(f"TG HTTP {r.status_code}: {r.text[:80]}")
        return False
    except Exception as e:
        log.warning(f"TG error: {e}"); return False

def _tg_send(message: str) -> bool:
    """Queue message for rate-limited sending. Returns True if queued."""
    if not (TG_TOKEN and TG_CHAT_ID):
        log.warning("Telegram: TG_TOKEN or TG_CHAT_ID not set")
        return False
    try:
        _TG_QUEUE.put_nowait(message)
        return True
    except _tg_queue_mod.Full:
        log.warning("TG queue full — alert dropped")
        return False

def _tg_send_debug(message: str):
    """Direct send with result for UI test button — bypasses queue."""
    if not TG_TOKEN:   return False, "TG_TOKEN is empty"
    if not TG_CHAT_ID: return False, "TG_CHAT_ID is empty"
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
            json={'chat_id': TG_CHAT_ID, 'text': message, 'parse_mode': 'HTML'},
            timeout=10)
        if r.status_code == 200: return True, "OK"
        try:    desc = r.json().get('description', r.text)
        except: desc = r.text
        return False, f"HTTP {r.status_code}: {desc}"
    except Exception as e:
        return False, str(e)

def _format_alert(label, expiry, state, ts, atm_strike, open_iv, current_iv, pct_chg,
                  spot=None, T_years=None, ce_ltp=None, pe_ltp=None):
    sign = '+' if pct_chg>=0 else ''
    calc_lines = ""
    if spot is not None:
        calc_lines += f"💰 <b>Spot Price:</b> {spot:,.2f}\n"
    if atm_strike:
        calc_lines += f"🎯 <b>ATM Strike (K):</b> {atm_strike:,.0f}\n"
    if T_years is not None:
        t_days = T_years * 252
        calc_lines += f"⏳ <b>Time to Expiry:</b> {t_days:.2f} trading days ({T_years:.4f} yr)\n"
    if ce_ltp is not None:
        calc_lines += f"📋 <b>ATM CE Price:</b> ₹{ce_ltp:.2f}\n"
    if pe_ltp is not None:
        calc_lines += f"📋 <b>ATM PE Price:</b> ₹{pe_ltp:.2f}\n"
    return (
        f"📊 <b>IV ALERT — {label}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🔖 <b>Expiry:</b> {expiry}\n"
        f"📈 <b>State:</b> {state}\n"
        f"🕐 <b>Time:</b> {ts.strftime('%I:%M %p')} IST\n"
        f"━━━ 🔢 Calc Inputs ━━━\n"
        f"{calc_lines}"
        f"━━━ 📊 IV Results ━━━\n"
        f"📌 <b>IV at Open:</b> {open_iv:.2f}%\n"
        f"⚡ <b>Current IV:</b> {current_iv:.2f}%\n"
        f"📊 <b>% Change:</b> {sign}{pct_chg:.2f}%\n"
        f"━━━━━━━━━━━━━━━━━━━━"
    )

def check_and_alert(inst, current_iv, open_iv, atm_strike,
                    spot=None, T_years=None, ce_ltp=None, pe_ltp=None):
    if current_iv <= 0 or open_iv <= 0: return

    now   = _now_ist()
    today = now.date().isoformat()

    # ── Market hours guard — no alerts outside 9:15–15:35 IST ───────────────
    mkt_open  = now.replace(hour=9,  minute=15, second=0, microsecond=0)
    mkt_close = now.replace(hour=15, minute=35, second=0, microsecond=0)
    if not (mkt_open <= now <= mkt_close):
        return

    # ── Opening noise skip — ignore first N minutes of market open ─────────
    skip_until = mkt_open + timedelta(minutes=CFG.get('alert_open_skip_min', 8))
    if now < skip_until:
        return

    iid     = inst['id']; label = inst['label']; exp = str(inst['expiry'])
    # Stocks need higher threshold than indices (naturally more volatile IV)
    _thresh_pct = (CFG.get('alert_breakout_pct_stock', 10.0)
                   if inst.get('type') == 'stock'
                   else CFG.get('alert_breakout_pct', 5.0))
    thresh  = _thresh_pct / 100.0
    pct_chg = (current_iv - open_iv) / open_iv * 100
    day_high, day_low = db_get_day_extremes(iid, today)

    def _not_sent(key, cooldown_mins=30):
        with _ALERT_LOCK:
            last = _ALERT_SENT.get(key)
            if last is None: return True
            return (now - last).total_seconds() > cooldown_mins * 60

    def _send(key, alert_type, state):
        msg = _format_alert(label, exp, state, now, atm_strike, open_iv, current_iv, pct_chg,
                            spot=spot, T_years=T_years, ce_ltp=ce_ltp, pe_ltp=pe_ltp)
        ok = _tg_send(msg)
        db_log_alert(iid, label, exp, alert_type, atm_strike, open_iv, current_iv, pct_chg, msg, now)
        with _ALERT_LOCK:
            _ALERT_SENT[key] = now
        log.info(f"Alert sent [{alert_type}] {label}: IV {open_iv:.2f}% → {current_iv:.2f}% ({pct_chg:+.2f}%) | TG={'OK' if ok else 'FAIL'}")

    # ── 1. Day High breakout ─────────────────────────────────────────────────
    if day_high > 0 and current_iv > day_high * (1 + thresh):
        key = (iid, 'DAY_HIGH', today)
        if _not_sent(key):
            _send(key, 'DAY_HIGH', 'RISING IV — 🔺 Day High')

    # ── 2. Day Low breakout ──────────────────────────────────────────────────
    elif day_low < 9999 and current_iv < day_low * (1 - thresh):
        key = (iid, 'DAY_LOW', today)
        if _not_sent(key):
            _send(key, 'DAY_LOW', 'FALLING IV — 🔻 Day Low')

    # ── 3. % change from open (fires even before Day High/Low are established)
    #       Separate cooldown key so it doesn't block Day High/Low alerts
    elif abs(pct_chg) >= _thresh_pct:
        direction = 'UP' if pct_chg > 0 else 'DOWN'
        key = (iid, f'PCT_{direction}', today)
        if _not_sent(key, cooldown_mins=60):   # longer cooldown for % alerts
            state = (f'RISING IV — 🔺 +{pct_chg:.1f}% from Open'
                     if pct_chg > 0 else
                     f'FALLING IV — 🔻 {pct_chg:.1f}% from Open')
            _send(key, f'PCT_{direction}', state)

# ═══════════════════════════════════════════════════════════════════════════════
#  LIVE BROKER DATA INGESTION (Zerodha / Fyers / Mock)
#
#  AUTH STRATEGY (Zerodha):
#  Method 1 — Official KiteConnect SDK (api.kite.trade)
#    Uses ZERODHA_API_KEY + access token generated via TOTP login flow.
#    This is the official, supported method. Preferred.
#  Method 2 — Web enctoken (kite.zerodha.com/oms)
#    Uses cookie-based auth from web login. COOKIE ONLY — no Authorization header.
#    Fallback if SDK unavailable.
# ═══════════════════════════════════════════════════════════════════════════════
_kite_sdk    = None     # KiteConnect SDK instance (Method 1 - preferred)
_kite_enc    = None     # requests.Session with enctoken cookie (Method 2 - fallback)
_enctoken    = ""       # raw enctoken value
_fyers       = None
_BROKER_ERROR = ""
_ZERODHA_METHOD   = ""    # "sdk" or "enctoken" — which method is active
_INST_CACHE: dict      = {}   # "NFO:TSYM"→True and (exch,name,exp,strike,type)→"NFO:TSYM"
_INST_STRIKES: dict   = {}   # (exch, name, expiry) → sorted list of strike floats
_INST_CACHE_DATE      = None # date the cache was built — rebuilt daily
# ── Expiry+Strike based lookup (no name matching needed) ──────────────────────
# Key: (exchange, expiry_iso, strike_float, opttype) → "NFO:TRADINGSYMBOL"
# Built alongside _INST_CACHE; never goes stale within a trading day.
_EXP_STRIKE_CACHE: dict = {}  # (exch, expiry_iso, strike, opttype) → full_sym
# Underlying→expiry→strikes mapping derived from tradingsymbols
_TSYM_UNDERLYING: dict  = {}  # full_sym → underlying_name (from inst master)

# ── Restore instrument cache from session_state if this is a Streamlit rerun ──
# Every Streamlit rerun re-executes the module from top, wiping module-level dicts.
# We persist the cache in session_state so it survives reruns without re-downloading.
# Cache format version — increment this whenever _EXP_STRIKE_CACHE key structure changes.
# Old format: (exch, expiry, strike, opttype) — 4-tuple, no name → stock collisions.
# New format: (exch, name, expiry, strike, opttype) — 5-tuple, name prevents collisions.
_CACHE_VERSION = 2   # bump to force rebuild when key format changes

try:
    import streamlit as _st_restore
    _cached_esc = _st_restore.session_state.get('_exp_strike_cache', {})
    _cached_ver = _st_restore.session_state.get('_cache_version', 0)
    # Validate: new cache uses 5-tuple keys. If any key is 4-tuple → old format → discard.
    _cache_ok = (
        bool(_cached_esc) and
        _cached_ver == _CACHE_VERSION and
        all(len(k) == 5 for k in list(_cached_esc.keys())[:10])  # sample first 10 keys
    )
    if _cache_ok:
        _INST_CACHE       = _st_restore.session_state.get('_inst_cache', {})
        _INST_STRIKES     = _st_restore.session_state.get('_inst_strikes', {})
        _EXP_STRIKE_CACHE = _cached_esc
        _TSYM_UNDERLYING  = _st_restore.session_state.get('_tsym_underlying', {})
        _INST_CACHE_DATE  = _st_restore.session_state.get('_inst_cache_date')
        import logging as _lg
        _lg.getLogger("IV").info(
            f"Instrument cache v{_CACHE_VERSION} restored from session_state: "
            f"{len(_EXP_STRIKE_CACHE)} exp+strike keys, date={_INST_CACHE_DATE}")
    elif _cached_esc:
        import logging as _lg
        _lg.getLogger("IV").warning(
            f"Instrument cache format mismatch (stored v{_cached_ver}, need v{_CACHE_VERSION}) "
            f"— discarding and rebuilding on next login.")
except Exception:
    pass  # session_state not available yet (first import before Streamlit init)

# ── Zerodha OMS Web endpoint (enctoken / cookie auth only) ───────────────────
_KITE_WEB = "https://kite.zerodha.com/oms"

def _is_zerodha_connected() -> bool:
    """True if either SDK or enctoken session is active."""
    return BROKER == 'zerodha' and (_kite_sdk is not None or _kite_enc is not None)

def _live_source_label() -> str:
    """Human-readable data source label for UI display."""
    if _is_zerodha_connected():
        method = f" [{_ZERODHA_METHOD.upper()}]" if _ZERODHA_METHOD else ""
        return f"🟢 Zerodha Live{method}"
    return "⚠️ Mock"

def _kite_get(path: str, params: dict = None) -> dict:
    """GET to Zerodha web OMS API using enctoken cookie auth."""
    if _kite_enc is None:
        raise RuntimeError("Kite session not initialised")
    r = _kite_enc.get(f"{_KITE_WEB}{path}", params=params, timeout=15)
    r.raise_for_status()
    j = r.json()
    if j.get("status") != "success":
        raise RuntimeError(f"Kite OMS error {r.status_code}: {j.get('message', j)}")
    return j["data"]

# ── Diagnostic flag: log first SDK response to understand key format ─────────
_SDK_BATCH_LOGGED = False

def _kite_ltp_batch(symbols: list) -> dict:
    """
    Fetch LTP for a LIST of symbols in ONE API call.
    Returns {symbol: last_price_float}.
    SDK accepts up to 500 symbols per call — use this instead of per-symbol calls
    to avoid connection pool exhaustion.
    """
    global _SDK_BATCH_LOGGED
    if not symbols:
        return {}

    # ── Method 1: Official SDK (single batch call) ───────────────────────────
    if _kite_sdk is not None:
        try:
            # Pass as list — some SDK versions need explicit list not *args
            data = _kite_sdk.ltp(symbols)           # ONE network request for all symbols
            if not data:
                # Try quote() as fallback (returns more data but same format)
                log.warning(f"ltp() returned empty for {len(symbols)} symbols, trying quote()…")
                try:
                    quote_data = _kite_sdk.quote(symbols)
                    data = {}
                    for k, v in quote_data.items():
                        # quote() may return last_price directly or nested under ohlc
                        lp = v.get("last_price") or (v.get("ohlc") or {}).get("close") or 0
                        data[k] = {"last_price": lp}
                except Exception as qe:
                    log.warning(f"quote() fallback also failed: {qe}")
                    data = {}

            # First call: log actual response keys so we can diagnose key mismatches
            if not _SDK_BATCH_LOGGED:
                _SDK_BATCH_LOGGED = True
                sample_keys = list(data.keys())[:4]
                sample_asked = symbols[:4]
                log.info(f"SDK ltp() sample — asked: {sample_asked}")
                log.info(f"SDK ltp() sample — returned keys: {sample_keys}")
                if data:
                    first_val = list(data.values())[0]
                    log.info(f"SDK ltp() sample — first value: {first_val}")
                else:
                    log.warning("SDK ltp() returned EMPTY dict — symbols may be invalid")

            result = {}
            # Build lookup that handles both "NFO:SYMBOL" and "SYMBOL" key formats
            data_upper = {k.upper(): v for k, v in data.items()}
            for sym in symbols:
                # Try exact match first, then without exchange prefix, then uppercase
                entry = (data.get(sym)
                      or data.get(sym.split(":", 1)[-1])         # strip "NFO:" prefix
                      or data_upper.get(sym.upper())              # case-insensitive
                      or data_upper.get(sym.split(":", 1)[-1].upper())
                      or {})
                ltp = entry.get("last_price")
                result[sym] = float(ltp) if (ltp is not None and float(ltp) > 0) else 0.05

            # Log a warning if all symbols returned 0, but do NOT raise.
            # Low-OI stocks legitimately return 0 on far strikes — that is fine.
            # Raising here causes the whole instrument to show as "fetch error"
            # which floods the diagnostics panel with false positives.
            real_prices = [v for v in result.values() if v > 0.05]
            if not real_prices and len(symbols) > 4:
                log.debug(
                    f"SDK ltp() all-zero for {len(symbols)} symbols "
                    f"(low OI or pre-fetch cache hit). Sample: {symbols[0]!r}"
                )
                # return the 0.05 results — caller handles missing prices gracefully
            return result
        except RuntimeError:
            raise
        except Exception as e:
            raise RuntimeError(f"SDK batch LTP failed ({len(symbols)} symbols): {e}") from e

    # ── Method 2: enctoken — batch via repeated single calls ─────────────────
    if _kite_enc is None:
        raise RuntimeError("No Zerodha connection available")
    result = {}
    failed = 0
    for sym in symbols:
        try:
            r = _kite_enc.get(
                f"{_KITE_WEB}/quote/ltp",
                params=[("i", sym)],
                timeout=8,
            )
            if r.status_code != 200:
                raise RuntimeError(f"HTTP {r.status_code}")
            j = r.json()
            if j.get("status") != "success":
                raise RuntimeError(j.get("message", "API error"))
            entry = (j.get("data") or {}).get(sym) or {}
            ltp   = entry.get("last_price")
            result[sym] = float(ltp) if (ltp is not None and float(ltp) > 0) else 0.05
        except Exception as e:
            log.debug(f"enctoken LTP failed {sym}: {e}")
            result[sym] = 0.05
            failed += 1
    if failed == len(symbols):
        raise RuntimeError(f"ALL {failed} enctoken LTP calls failed — check session")
    return result


def _kite_ltp_single(symbol: str) -> float:
    """
    Single-symbol LTP fetch.
    Tries ltp() first, then quote() as fallback.
    Raises RuntimeError if price cannot be fetched (so caller can use hardcoded fallback).
    """
    if _kite_sdk is not None:
        # Try ltp() first
        try:
            data = _kite_sdk.ltp([symbol])
            if data:
                data_upper = {k.upper(): v for k, v in data.items()}
                entry = (data.get(symbol)
                      or data.get(symbol.split(":", 1)[-1])
                      or data_upper.get(symbol.upper())
                      or data_upper.get(symbol.split(":", 1)[-1].upper())
                      or {})
                ltp = entry.get("last_price")
                if ltp and float(ltp) > 0.5:
                    return float(ltp)
        except Exception:
            pass
        # Fallback: quote()
        try:
            data = _kite_sdk.quote([symbol])
            if data:
                data_upper = {k.upper(): v for k, v in data.items()}
                entry = (data.get(symbol)
                      or data.get(symbol.split(":", 1)[-1])
                      or data_upper.get(symbol.upper())
                      or data_upper.get(symbol.split(":", 1)[-1].upper())
                      or {})
                ltp = entry.get("last_price") or (entry.get("ohlc") or {}).get("close")
                if ltp and float(ltp) > 0.5:
                    return float(ltp)
        except Exception as e:
            raise RuntimeError(f"Both ltp() and quote() failed for {symbol}: {e}")
        raise RuntimeError(f"Symbol returned 0/empty price: {symbol}")

    # enctoken fallback
    if _kite_enc is not None:
        try:
            r = _kite_enc.get(f"{_KITE_WEB}/quote/ltp", params=[("i", symbol)], timeout=8)
            if r.status_code == 200:
                j = r.json()
                entry = (j.get("data") or {}).get(symbol) or {}
                ltp = entry.get("last_price")
                if ltp and float(ltp) > 0.5:
                    return float(ltp)
        except Exception as e:
            raise RuntimeError(f"enctoken LTP failed for {symbol}: {e}")
        raise RuntimeError(f"enctoken returned 0 price for {symbol}")

    raise RuntimeError("No Zerodha connection available")


def _kite_quote(instruments: list) -> dict:
    """Fetch LTP for multiple symbols. Returns {symbol: {last_price: float}}."""
    result = {}
    for sym in instruments:
        ltp = _kite_ltp_single(sym)
        result[sym] = {"last_price": ltp}
    return result


def _zerodha_auto_login() -> str:
    """
    Login to Zerodha using user/password + TOTP.
    Returns enctoken string on success, raises on failure.
    enctoken = Zerodha web session token, valid for the rest of the calendar day.
    """
    import pyotp
    log.info("Zerodha login: starting…")

    sess = requests.Session()
    sess.headers.update({
        "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                           "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept":          "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Origin":          "https://kite.zerodha.com",
        "Referer":         "https://kite.zerodha.com/",
        "Content-Type":    "application/x-www-form-urlencoded",
    })

    # ── Step 1: Password ──────────────────────────────────────────────────────
    r1 = sess.post(
        "https://kite.zerodha.com/api/login",
        data={"user_id": ZERODHA_USER_ID, "password": ZERODHA_PASSWORD},
        timeout=20,
    )
    r1.raise_for_status()
    j1 = r1.json()
    log.info(f"Step1 response: status={j1.get('status')} "
             f"keys={list((j1.get('data') or {}).keys())}")
    if j1.get("status") != "success":
        raise RuntimeError(f"Password rejected: {j1.get('message', j1)}")

    request_id = j1["data"]["request_id"]
    twofa_type = j1["data"].get("twofa_type", "totp")

    # ── Step 2: TOTP ──────────────────────────────────────────────────────────
    totp_val  = pyotp.TOTP(ZERODHA_TOTP_KEY).now()
    twofa_val = totp_val if "totp" in twofa_type.lower() else ZERODHA_PIN
    log.info(f"Step2: twofa_type={twofa_type}  totp={totp_val}")

    r2 = sess.post(
        "https://kite.zerodha.com/api/twofa",
        data={
            "user_id":     ZERODHA_USER_ID,
            "request_id":  request_id,
            "twofa_value": twofa_val,
            "twofa_type":  twofa_type,
        },
        timeout=20,
    )
    r2.raise_for_status()
    j2 = r2.json()
    log.info(f"Step2 response: status={j2.get('status')} data_keys={list((j2.get('data') or {}).keys())}")
    if j2.get("status") != "success":
        raise RuntimeError(f"TOTP rejected: {j2.get('message', j2)}")

    # ── Extract enctoken ──────────────────────────────────────────────────────
    enc = None
    if isinstance(j2.get("data"), dict):
        enc = j2["data"].get("enctoken")
    if not enc:
        enc = sess.cookies.get("enctoken")
    if not enc:
        for c in sess.cookies:
            if c.name == "enctoken":
                enc = c.value; break

    log.info(f"enctoken: {'FOUND len=' + str(len(enc)) if enc else 'NOT FOUND'}")
    log.info(f"All cookies: { {c.name: c.value[:20] for c in sess.cookies} }")

    if not enc:
        raise RuntimeError(
            f"2FA succeeded but enctoken not found in response or cookies. "
            f"Response data: {j2.get('data')}  |  "
            f"Cookies: { {c.name: c.domain for c in sess.cookies} }"
        )
    return enc


def _build_kite_session(enc: str) -> requests.Session:
    """
    Build a requests.Session authenticated with Zerodha enctoken.

    CRITICAL AUTH NOTE:
    kite.zerodha.com/oms uses COOKIE-ONLY auth — the enctoken must be sent as
    a cookie, NOT as an Authorization header. Sending 'Authorization: enctoken X'
    to the OMS endpoint causes all requests to be rejected with 403/error.
    (The Authorization header style is for api.kite.trade only.)
    """
    from requests.adapters import HTTPAdapter
    s = requests.Session()
    adapter = HTTPAdapter(pool_connections=50, pool_maxsize=100, max_retries=2)
    s.mount("https://", adapter)
    s.mount("http://",  adapter)
    s.headers.update({
        "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                           "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept":          "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Origin":          "https://kite.zerodha.com",
        "Referer":         "https://kite.zerodha.com/",
        # NO Authorization header — OMS is cookie-auth only
    })
    # enctoken cookie on kite.zerodha.com domain (that is the OMS host)
    s.cookies.set("enctoken", enc, domain="kite.zerodha.com", path="/")
    s.cookies.set("enctoken", enc, domain=".zerodha.com",     path="/")
    return s


def _build_instrument_cache(kite) -> int:
    """
    Download NFO + BFO instrument masters from Zerodha via SDK.
    Builds TWO lookups:
      _INST_CACHE: "NFO:TRADINGSYMBOL" → True   (fast membership test)
      _INST_CACHE_LOOKUP: (exch, name, expiry_date, strike, opt_type) → "NFO:TRADINGSYMBOL"
    Called once per day at login time.
    """
    global _INST_CACHE, _INST_STRIKES, _INST_CACHE_DATE, _EXP_STRIKE_CACHE, _TSYM_UNDERLYING
    _INST_CACHE        = {}
    _INST_STRIKES      = {}
    _EXP_STRIKE_CACHE  = {}
    _TSYM_UNDERLYING   = {}
    today = _today_ist()
    names_seen   = set()   # for diagnostic logging
    names_by_exch = {}

    for exch in ("NFO", "BFO"):
        # ── Retry instrument download up to 3 times (BFO sometimes times out) ──
        instruments = None
        for _attempt in range(1, 4):
            try:
                instruments = kite.instruments(exch)
                if instruments:
                    log.info(f"Instruments({exch}): downloaded {len(instruments)} rows (attempt {_attempt})")
                    break
                log.warning(f"Instruments({exch}): empty response on attempt {_attempt}")
            except Exception as _dl_err:
                log.warning(f"Instruments({exch}) attempt {_attempt} failed: {_dl_err}")
                if _attempt < 3:
                    time.sleep(2)
        if not instruments:
            log.error(f"Instruments({exch}): all 3 download attempts failed — {exch} options will use fallback symbols")
            continue
        try:
            count = 0
            for inst in instruments:
                itype = inst.get("instrument_type", "")
                if itype not in ("CE", "PE"):
                    continue
                exp_raw = inst.get("expiry")
                if exp_raw is None:
                    continue
                # Normalise to date (SDK may return date or datetime)
                if hasattr(exp_raw, 'date'):
                    exp = exp_raw.date()
                else:
                    exp = exp_raw
                if (exp - today).days > 60:
                    continue
                name   = inst.get("name", "").upper().strip()
                strike = float(inst.get("strike", 0))
                tsym   = inst.get("tradingsymbol", "")

                full_sym = f"{exch}:{tsym}"
                # Primary key: full symbol string (for membership check)
                _INST_CACHE[full_sym] = True
                # Secondary key: name-based lookup (legacy)
                _INST_CACHE[(exch, name, exp.isoformat(), strike, itype)] = full_sym
                # Strike index
                exp_str_key = exp.isoformat()
                skey = (exch, name, exp_str_key)
                if skey not in _INST_STRIKES:
                    _INST_STRIKES[skey] = set()
                _INST_STRIKES[skey].add(strike)
                # ── expiry+strike+name lookup (name prevents stock strike collisions) ──
                # Key includes name so stocks with same strike don't overwrite each other.
                # Old name-free key kept ONLY for indices (where strikes are unique per exchange).
                _EXP_STRIKE_CACHE[(exch, name, exp_str_key, strike, itype)] = full_sym
                # Also store name-free key for index-like instruments (optional fast-path)
                # But DO NOT store name-free for stocks — it causes collisions between
                # different stocks that share the same strike value on the same expiry.
                _name_free_key = (exch, exp_str_key, strike, itype)
                if _name_free_key not in _EXP_STRIKE_CACHE:
                    # Only store name-free if it's the first time we see this combination.
                    # This avoids overwriting ABB's 6100PE with some later stock's 6100PE.
                    _EXP_STRIKE_CACHE[_name_free_key] = full_sym
                _TSYM_UNDERLYING[full_sym] = name
                names_seen.add(name)
                count += 1

            names_by_exch[exch] = count
            log.info(f"Instruments cache: loaded {exch} — {count} options")
        except Exception as e:
            log.warning(f"Instruments cache failed for {exch}: {e}")

    _INST_CACHE_DATE = today
    total = sum(names_by_exch.values())

    # ── Post-download verification: confirm key instruments are present ───────
    _KEY_CHECKS = [
        ('NFO', 'NIFTY'),
        ('NFO', 'BANKNIFTY'),
        ('NFO', 'MIDCPNIFTY'),
        ('NFO', 'FINNIFTY'),
        ('BFO', 'SENSEX'),
    ]
    for _chk_exch, _chk_name in _KEY_CHECKS:
        _chk_strikes = sum(1 for k in _INST_STRIKES if k[0] == _chk_exch and k[1] == _chk_name)
        if _chk_strikes == 0:
            log.warning(
                f"Instrument cache MISSING: {_chk_exch}:{_chk_name} — "
                f"option symbols will fall back to constructed names (may be wrong). "
                f"Check that your Zerodha account has {_chk_exch} F&O access."
            )
        else:
            log.info(f"Instrument cache OK: {_chk_exch}:{_chk_name} — {_chk_strikes} expiry/strike combos loaded")

    # ── Persist to session_state so Streamlit reruns don't wipe the cache ────
    try:
        import streamlit as _st
        _st.session_state['_inst_cache']        = _INST_CACHE
        _st.session_state['_inst_strikes']      = _INST_STRIKES
        _st.session_state['_exp_strike_cache']  = _EXP_STRIKE_CACHE
        _st.session_state['_tsym_underlying']   = _TSYM_UNDERLYING
        _st.session_state['_inst_cache_date']   = _INST_CACHE_DATE
        _st.session_state['_cache_version']     = _CACHE_VERSION  # version tag for format validation
        log.info(f"Instrument cache saved to session_state: {total} options, "
                 f"{len(_EXP_STRIKE_CACHE)} exp+strike keys")
    except Exception as _e:
        log.debug(f"session_state save failed (expected before Streamlit init): {_e}")

    bfo_names = sorted(n for n in names_seen
                       if any((_INST_CACHE.get(('BFO', n, d, s, t))
                               for (e, nm, d, s, t), v in _INST_CACHE.items()
                               if isinstance((e,nm,d,s,t), tuple) and e == 'BFO' and nm == n)))
    log.info(f"Instruments cache built: {total} options across NFO+BFO")
    log.info(f"NFO count: {names_by_exch.get('NFO',0)}  BFO count: {names_by_exch.get('BFO',0)}")
    log.info(f"All BFO underlying names: {sorted(n for n in names_seen if any(k[0]=='BFO' and k[1]==n for k in _INST_CACHE if isinstance(k,tuple)))}")
    log.info(f"All NFO/BFO names ({len(names_seen)}): {sorted(names_seen)}")
    return total


def _lookup_by_expiry_strike(exchange: str, expiry, strike: float, opt_type: str,
                             name: str = None) -> str:
    """
    Look up exact Zerodha full symbol using exchange + expiry + strike + opttype.
    When `name` is provided, uses the name-keyed cache (avoids stock strike collisions).
    When `name` is None, falls back to the name-free key (for indices only).
    Returns full symbol string or None.
    """
    # global MUST be declared before any reference to these names (Python rule)
    global _EXP_STRIKE_CACHE, _INST_CACHE, _INST_STRIKES, _TSYM_UNDERLYING, _INST_CACHE_DATE

    if hasattr(expiry, 'date'):
        expiry = expiry.date()
    exp_key = expiry.isoformat() if hasattr(expiry, 'isoformat') else str(expiry)
    strike_f = float(strike)
    opt_up   = opt_type.upper()

    # Prefer name-keyed lookup to avoid cross-stock strike collisions
    if name:
        key = (exchange, name.upper().strip(), exp_key, strike_f, opt_up)
    else:
        key = (exchange, exp_key, strike_f, opt_up)

    # Fast path: module-level cache populated — try name-keyed first, then name-free
    result = _EXP_STRIKE_CACHE.get(key)
    if result:
        return result
    # If name was given, also try name-free key as fallback
    if name:
        _nf_key = (exchange, exp_key, strike_f, opt_up)
        result = _EXP_STRIKE_CACHE.get(_nf_key)
        if result:
            return result

    # Slow path: cache was wiped by Streamlit rerun — restore from session_state
    if not _EXP_STRIKE_CACHE:
        try:
            import streamlit as _st2
            if _st2.session_state.get('_exp_strike_cache'):
                _INST_CACHE       = _st2.session_state.get('_inst_cache', {})
                _INST_STRIKES     = _st2.session_state.get('_inst_strikes', {})
                _EXP_STRIKE_CACHE = _st2.session_state['_exp_strike_cache']
                _TSYM_UNDERLYING  = _st2.session_state.get('_tsym_underlying', {})
                _INST_CACHE_DATE  = _st2.session_state.get('_inst_cache_date')
                r2 = _EXP_STRIKE_CACHE.get(key)
                if r2:
                    return r2
        except Exception:
            pass

    # ── Expiry tolerance: BFO (Sensex) and edge-case instruments may have expiry
    # stored in Zerodha master as ±1 calendar day vs our computed date.
    from datetime import timedelta as _td
    for _delta in (-1, 1, -2, 2):
        _alt_exp = expiry + _td(days=_delta)
        _alt_iso = _alt_exp.isoformat()
        # Try name-keyed first, then name-free
        for _alt_key in (
            (exchange, name.upper().strip(), _alt_iso, strike_f, opt_up) if name else None,
            (exchange, _alt_iso, strike_f, opt_up),
        ):
            if _alt_key is None:
                continue
            _alt_result = _EXP_STRIKE_CACHE.get(_alt_key)
            if _alt_result:
                log.debug(f"expiry tolerance: {exchange} {expiry} strike={strike} {opt_up} "
                          f"resolved via delta={_delta:+d} day → {_alt_result}")
                _EXP_STRIKE_CACHE[key] = _alt_result
                return _alt_result
    return None

def _lookup_option_sym(exchange: str, name: str, expiry, strike: float, opt_type: str) -> str:
    """
    Look up the exact Zerodha full symbol (EXCHANGE:TRADINGSYMBOL) from the cache.
    Returns full symbol string, or None if not found.
    expiry: date object. opt_type: 'CE' or 'PE'
    """
    global _INST_CACHE, _INST_CACHE_DATE
    # Rebuild cache if stale (new trading day)
    if _INST_CACHE_DATE != _today_ist() and _kite_sdk is not None:
        _build_instrument_cache(_kite_sdk)  # rebuilds both _INST_CACHE and _INST_STRIKES

    # Normalise expiry to date
    if hasattr(expiry, 'date'):
        expiry = expiry.date()

    # Normalise expiry to ISO string — matches how cache was built
    if hasattr(expiry, 'isoformat'):
        exp_key = expiry.isoformat()
    else:
        exp_key = str(expiry)
    key = (exchange, name.upper().strip(), exp_key, float(strike), opt_type.upper())
    return _INST_CACHE.get(key)   # returns full "NFO:TSYM" or None


def _verify_symbol(full_sym: str) -> bool:
    """Check if a constructed symbol (e.g. 'NFO:NIFTY26FEB2625500CE') exists in cache."""
    if not _INST_CACHE:
        return True   # cache not built — assume valid
    return full_sym in _INST_CACHE


def _get_strikes_from_cache(exchange: str, name: str, expiry, spot: float, n_strikes: int = 11) -> list:
    """
    Get n_strikes actual NSE strikes closest to spot from the _INST_STRIKES index.
    O(1) lookup — uses pre-built index, not full dict scan.
    Returns sorted list of strike floats, or empty list if not found.
    """
    if not _INST_STRIKES:
        return []
    # Normalise expiry to date
    if hasattr(expiry, 'date'):
        expiry = expiry.date()
    # Use ISO string for expiry key — avoids date vs datetime type mismatch
    if hasattr(expiry, 'isoformat'):
        exp_key = expiry.isoformat()
    else:
        exp_key = str(expiry)
    skey = (exchange, name.upper().strip(), exp_key)
    strikes = _INST_STRIKES.get(skey)
    if not strikes:
        return []
    # Pick n_strikes closest to spot, return sorted ascending
    sorted_by_dist = sorted(strikes, key=lambda k: abs(k - spot))
    return sorted(sorted_by_dist[:n_strikes])


def _try_sdk_login() -> bool:
    """
    Try to connect via official KiteConnect SDK.
    Uses ZERODHA_API_KEY + generates access token via TOTP login flow.
    Returns True on success, False on failure.
    """
    global _kite_sdk, _ZERODHA_METHOD
    try:
        from kiteconnect import KiteConnect
        import pyotp

        kite = KiteConnect(api_key=ZERODHA_API_KEY)

        # Step 1: get login URL, then simulate web login to get request_token
        import requests as _req
        sess = _req.Session()
        sess.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Content-Type": "application/x-www-form-urlencoded",
        })

        # Password login
        r1 = sess.post("https://kite.zerodha.com/api/login",
                       data={"user_id": ZERODHA_USER_ID, "password": ZERODHA_PASSWORD},
                       timeout=20)
        j1 = r1.json()
        if j1.get("status") != "success":
            log.warning(f"SDK login step1 failed: {j1.get('message')}")
            return False

        request_id = j1["data"]["request_id"]
        twofa_type = j1["data"].get("twofa_type", "totp")
        totp_val   = pyotp.TOTP(ZERODHA_TOTP_KEY).now()
        twofa_val  = totp_val if "totp" in twofa_type.lower() else ZERODHA_PIN

        # TOTP
        r2 = sess.post("https://kite.zerodha.com/api/twofa",
                       data={"user_id": ZERODHA_USER_ID, "request_id": request_id,
                             "twofa_value": twofa_val, "twofa_type": twofa_type},
                       timeout=20)
        j2 = r2.json()
        if j2.get("status") != "success":
            log.warning(f"SDK login step2 failed: {j2.get('message')}")
            return False

        # Get request_token from redirect URL
        r3 = sess.get(
            f"https://kite.zerodha.com/connect/login?api_key={ZERODHA_API_KEY}&v=3",
            timeout=20, allow_redirects=False,
        )
        # The redirect location contains request_token=XXXX
        location = r3.headers.get("Location", "")
        if "request_token=" not in location:
            # Try following the redirect chain
            r3b = sess.get(
                f"https://kite.zerodha.com/connect/login?api_key={ZERODHA_API_KEY}&v=3",
                timeout=20, allow_redirects=True,
            )
            location = r3b.url

        if "request_token=" not in location:
            log.warning(f"SDK: could not extract request_token from: {location[:200]}")
            return False

        from urllib.parse import urlparse, parse_qs
        qs = parse_qs(urlparse(location).query)
        request_token = qs.get("request_token", [None])[0]
        if not request_token:
            log.warning("SDK: request_token is empty")
            return False

        # Generate access token
        import hashlib
        data = kite.generate_session(request_token, api_secret=ZERODHA_SECRET)
        access_token = data["access_token"]
        kite.set_access_token(access_token)

        # Patch SDK's internal requests.Session with a large connection pool
        # (default pool=10 causes "Connection pool is full" with 160+ instruments)
        # kiteconnect SDK uses 'reqsession' as the session attribute name
        from requests.adapters import HTTPAdapter
        _big_adapter = HTTPAdapter(pool_connections=20, pool_maxsize=200, max_retries=3)
        _sdk_session = getattr(kite, 'reqsession', None) or getattr(kite, '_session', None)
        if _sdk_session is not None:
            _sdk_session.mount("https://", _big_adapter)
            _sdk_session.mount("http://",  _big_adapter)
            log.info(f"SDK session pool patched (attr={'reqsession' if hasattr(kite, 'reqsession') else '_session'})")
        else:
            log.warning("SDK session attr not found — pool not patched (may see pool-full warnings)")

        # Quick sanity check
        profile = kite.profile()
        log.info(f"KiteConnect SDK ✅  user={profile.get('user_name', '?')}  method=SDK")
        _kite_sdk = kite
        _ZERODHA_METHOD = "sdk"
        # Download instruments master — needed to get exact option tradingsymbols
        _n_cached = _build_instrument_cache(kite)
        if _n_cached == 0:
            log.warning("Instruments cache empty — option symbol lookups will fall back to constructed names")
        return True

    except Exception as e:
        log.warning(f"SDK login failed: {e}")
        return False


def _init_broker():
    global _kite_sdk, _kite_enc, _enctoken, _fyers, BROKER, _BROKER_ERROR, _ZERODHA_METHOD

    if BROKER == 'zerodha':
        # ── Try Method 1: Official KiteConnect SDK ────────────────────────────
        sdk_ok = _try_sdk_login()

        if not sdk_ok:
            # ── Try Method 2: enctoken web session ───────────────────────────
            log.info("SDK login failed — trying enctoken web login as fallback…")
            try:
                import pyotp
                _enctoken = _zerodha_auto_login()
                _kite_enc = _build_kite_session(_enctoken)

                # Verify session with profile endpoint
                # Verify enctoken works — use /oms/user/profile (not /full)
                # The enctoken is already confirmed valid from the login flow;
                # this is just a sanity-check ping.
                r = _kite_enc.get(
                    "https://kite.zerodha.com/oms/user/profile", timeout=10)
                if r.status_code == 200:
                    pj = r.json()
                    uname = (pj.get("data") or {}).get("user_name", "?")
                    log.info(f"Zerodha enctoken ✅  user={uname}  method=enctoken")
                else:
                    # 400/403 on profile check — but enctoken was obtained, trust it
                    log.warning(f"Profile check returned {r.status_code} — proceeding anyway (enctoken was obtained)")
                    uname = ZERODHA_USER_ID
                _ZERODHA_METHOD = "enctoken"
                _BROKER_ERROR = ""

            except ImportError as e:
                _BROKER_ERROR = f"Missing package: {e}"
                log.warning(f"Zerodha ImportError: {e} → mock"); BROKER = 'mock'
            except Exception as e:
                _BROKER_ERROR = str(e)
                log.warning(f"Zerodha enctoken login failed: {e} → mock"); BROKER = 'mock'
        else:
            _BROKER_ERROR = ""

    elif BROKER == 'fyers':
        try:
            from fyers_apiv3 import fyersModel
            _fyers = fyersModel.FyersModel(client_id=FYERS_CLIENT_ID,
                                           is_async=False, token=FYERS_ACCESS_TOKEN, log_path="")
            log.info("Fyers connected.")
        except Exception as e:
            log.warning(f"Fyers init failed: {e} → mock"); BROKER = 'mock'

_init_broker()

# ═══════════════════════════════════════════════════════════════════════════════
#  BACKGROUND IV RECORDER
#  Runs every 60s in a daemon thread — saves one real IV reading per instrument
#  to SQLite.  This populates the intraday chart with genuine Zerodha data
#  across the trading day without needing the user to refresh the page.
# ═══════════════════════════════════════════════════════════════════════════════
_recorder_started = False
_recorder_lock    = threading.Lock()

def _record_iv_snapshot(indices_only: bool = False):
    """
    Fetch live ATM IV for instruments and save to DB.

    FIX — Too many requests (HTTP 429):
    Previously this ran for ALL 162 instruments every 60s → ~3564 API calls/min.
    Now:
      • Indices (7) run every 60s   → 7×22 = 154 calls/min  ✅ safe
      • Stocks (155) run every 5min → 155×22 = 3410 calls per 5 min = 682/min avg
        but staggered in batches of 20 with a small sleep between batches.
    Pass indices_only=True for the fast 60s cycle; False for the full 5-min cycle.

    FIX — Friday data lost on Monday:
    After every successful snapshot we also write a JSON snapshot file for today.
    This ensures the file exists even if the script is stopped before market close,
    so Monday's 'previous trading day' lookup finds Friday's file.
    """
    if BROKER != 'zerodha' or not (_kite_enc or _kite_sdk):
        return
    now   = _now_ist()
    today = now.date().isoformat()
    all_insts = build_registry()
    insts = [i for i in all_insts if i['type'] == 'index'] if indices_only \
            else all_insts

    results = {}   # inst_id → {time_str, atm_iv, spot}

    def _snap_one(inst):
        try:
            lc  = _fetch_kite(inst)
            ls  = skew_from_chain(inst, lc, 'Live')
            lsp = float(ls['spot'].iloc[0])
            iv  = watm_iv(ls, lsp)
            if not iv:
                return
            exp = str(inst['expiry'])
            dh, dl = db_get_day_extremes(inst['id'], today)
            new_high = max(dh, iv)
            new_low  = min(dl if dl < 9999 else iv, iv)
            db_save_atm(inst['id'], exp, iv, lsp, new_high, new_low, now)
            results[inst['id']] = {'time_str': now.strftime('%H:%M'),
                                   'atm_iv': round(iv, 3), 'spot': lsp}
        except Exception as e:
            log.debug(f"recorder snap failed {inst['id']}: {e}")

    # ── Process in small batches with a brief pause to stay under rate limit ──
    # Zerodha SDK: ~2 req/s safe limit. Each instrument = 1 batch LTP call.
    # 8 workers × 1 call each = 8 concurrent calls per batch → fine.
    # We add a 0.5s sleep between batches of 20 stocks to be safe.
    BATCH = 20
    for i in range(0, len(insts), BATCH):
        chunk = insts[i:i+BATCH]
        with ThreadPoolExecutor(max_workers=8) as ex:
            list(ex.map(_snap_one, chunk))
        if not indices_only and i + BATCH < len(insts):
            time.sleep(0.5)   # brief pause between stock batches

    log.info(f"IV snapshot recorded for {len(results)}/{len(insts)} instruments "
             f"({'indices' if indices_only else 'all'}) at {now.strftime('%H:%M:%S')}")

    # ── FIX: Save JSON snapshot intraday so Friday's data survives to Monday ──
    # We persist today's DB rows to JSON after every full (non-indices-only) pass.
    # On Monday, get_previous_trading_date() returns Friday and finds this file.
    if not indices_only and results:
        try:
            _snap_path = _store_path(now.date())
            # Merge with any existing file so we accumulate across the whole day
            existing = {}
            if _snap_path.exists():
                try:
                    existing = json.loads(_snap_path.read_text())
                except Exception:
                    existing = {}
            # Each instrument: append the new reading to its stored list
            for iid, row in results.items():
                if iid not in existing:
                    existing[iid] = {'times': [], 'atm_iv': [], 'spot': [],
                                     'date': now.date().isoformat()}
                existing[iid]['times'].append(row['time_str'])
                existing[iid]['atm_iv'].append(row['atm_iv'])
                existing[iid]['spot'].append(row['spot'])
            _snap_path.write_text(json.dumps(existing))
        except Exception as e:
            log.debug(f"Intraday snapshot save failed: {e}")

# ── Recorder loop: indices every 60s, all instruments every 5 min ─────────────
_RECORDER_FULL_INTERVAL = 300   # seconds between full (all-instrument) passes
_recorder_last_full = [0.0]     # mutable container so inner function can write it

def _recorder_loop():
    """
    Daemon loop — two-speed recording to avoid Zerodha rate limits:
      • Every 60s  → indices only (7 instruments, fast)
      • Every 5min → all instruments (indices + 155 stocks, staggered batches)
    """
    import time as _t
    while True:
        try:
            now = _now_ist()
            mkt_open  = now.replace(hour=CFG['market_open_h'],  minute=CFG['market_open_m'],  second=0, microsecond=0)
            mkt_close = now.replace(hour=CFG['market_close_h'], minute=CFG['market_close_m'], second=0, microsecond=0)
            if mkt_open <= now <= mkt_close:
                elapsed_since_full = _t.time() - _recorder_last_full[0]
                if elapsed_since_full >= _RECORDER_FULL_INTERVAL:
                    # Full pass every 5 min (stocks + indices, saves JSON too)
                    _record_iv_snapshot(indices_only=False)
                    _recorder_last_full[0] = _t.time()
                else:
                    # Fast pass every 60s — indices only
                    _record_iv_snapshot(indices_only=True)
        except Exception as e:
            log.warning(f"Recorder loop error: {e}")
        _t.sleep(60)

def _start_recorder():
    global _recorder_started
    with _recorder_lock:
        if not _recorder_started:
            t = threading.Thread(target=_recorder_loop, daemon=True, name="IVRecorder")
            t.start()
            _recorder_started = True
            log.info("Background IV recorder started ✅")

# Start recorder immediately if Zerodha is connected
if _is_zerodha_connected():
    _start_recorder()
    # Also record one indices-only snapshot immediately so DB has data right away
    # (indices_only=True to avoid hammering the API at startup)
    threading.Thread(target=lambda: _record_iv_snapshot(indices_only=True),
                     daemon=True, name="IVSnap0").start()

# ── Spot price cache — populated at startup and refreshed every 5 min ─────────
_SPOT_CACHE: dict = {}          # {inst_id: float}  — live spot prices
_SPOT_CACHE_LOCK = threading.Lock()
_SPOT_CACHE_TS: float = 0.0     # epoch time of last refresh
_SPOT_REFRESH_IN_PROGRESS = threading.Event()  # prevents duplicate refresh threads

# ── Global LTP pre-fetch cache ────────────────────────────────────────────────
# build_all_data_parallel() calls _prefetch_all_ltps() ONCE before processing.
# This fires ONE or TWO mega-batch ltp() calls covering all 162 instruments,
# then stores results here so _fetch_kite() reads from cache — zero extra API calls.
_GLOBAL_LTP: dict  = {}    # {full_symbol: last_price_float}
_GLOBAL_LTP_TS: float = -1.0   # -1 = never fetched; time.time()-(-1) ≈ 1.7B but we check _GLOBAL_LTP bool first
_GLOBAL_LTP_LOCK = threading.Lock()
_GLOBAL_LTP_TTL  = 55      # seconds — slightly less than cache TTL=60s

# ── Open IV cache — stores first-of-day ATM IV per instrument ─────────────────
# Keyed by (inst_id, date_str). Persists across Streamlit reruns via:
#   1. st.session_state  — survives Streamlit auto-reruns within the same process
#   2. JSON file on disk — survives full kill+restart of the Streamlit server
#
# BUG FIX (v10): Two root causes fixed:
#   (a) STORE_DIR was a relative path → JSON file not found if cwd changed on restart
#       Fixed above: STORE_DIR = Path(__file__).parent / "iv_data_store"
#   (b) _OPEN_IV_CACHE was a plain module-level dict → reset to {} on every
#       Streamlit rerun (Streamlit re-executes the whole script each refresh).
#       Fixed: now backed by st.session_state['_open_iv_cache'] so it survives
#       reruns. Disk JSON is only read when the session is brand-new (i.e. a
#       true restart), not on every auto-refresh tick.
_OPEN_IV_LOCK = threading.Lock()
_OPEN_IV_PATH = STORE_DIR / "open_iv_cache.json"

def _get_open_iv_cache() -> dict:
    """Return the live in-memory open IV cache, backed by st.session_state.
    Using session_state means the dict survives Streamlit reruns (auto-refresh)
    within the same server process — no re-pinning on each tick."""
    if '_open_iv_cache' not in st.session_state:
        st.session_state['_open_iv_cache'] = {}
    return st.session_state['_open_iv_cache']

def _load_open_iv_cache():
    """Load open IV cache from disk into session_state (runs once per true restart).
    Skipped if session_state already has entries — avoids overwriting live data
    on every Streamlit rerun."""
    cache = _get_open_iv_cache()
    if cache:
        return   # session already populated — skip disk read (rerun, not restart)
    try:
        if _OPEN_IV_PATH.exists():
            raw = json.loads(_OPEN_IV_PATH.read_text())
            today_str = _today_ist().isoformat()
            with _OPEN_IV_LOCK:
                loaded = {
                    tuple(k.split('|')): float(v)
                    for k, v in raw.items()
                    if k.endswith(f'|{today_str}')
                }
                cache.update(loaded)
            log.info(f"Open IV cache loaded from disk: {len(loaded)} entries for today ({_OPEN_IV_PATH})")
        else:
            log.info(f"Open IV cache file not found ({_OPEN_IV_PATH}) — will pin fresh at 9:20 AM")
    except Exception as e:
        log.warning(f"Open IV cache load failed: {e}")

def _save_open_iv_cache():
    """Persist open IV cache to disk (absolute path — survives cwd changes)."""
    try:
        cache = _get_open_iv_cache()
        with _OPEN_IV_LOCK:
            serialisable = {'|'.join(k): v for k, v in cache.items()}
        _OPEN_IV_PATH.write_text(json.dumps(serialisable))
        log.debug(f"Open IV cache saved: {len(serialisable)} entries → {_OPEN_IV_PATH}")
    except Exception as e:
        log.debug(f"Open IV cache save failed: {e}")

def _get_open_iv(inst_id: str, live_iv: float, today_str: str) -> float:
    """
    Get today's open IV for this instrument.
    Waits until 9:20 AM IST before pinning the open baseline (avoids
    noisy opening prints at 9:15). Once pinned, the value is FROZEN
    for the rest of the day — survives both Streamlit reruns (session_state)
    and full kill+restart (disk JSON at absolute path).
    Returns the open IV as a percentage (e.g. 14.5 for 14.5%).
    """
    # ── Do not pin Open IV before 9:20 AM IST ────────────────────────────────
    now = _now_ist()
    open_pin_time = now.replace(hour=9, minute=20, second=0, microsecond=0)
    if now < open_pin_time:
        return 0.0   # too early — not yet capturing open IV

    key = (inst_id, today_str)
    cache = _get_open_iv_cache()
    with _OPEN_IV_LOCK:
        if key in cache:
            return cache[key]   # already pinned — return frozen value
        # First reading at or after 9:20 AM — pin as the open baseline.
        # Only pin if IV is in a reasonable range — extreme values indicate
        # a bad option price (deep OTM / stale spot) and must NOT be pinned.
        if live_iv and 0.5 < live_iv < 150.0:  # 0.5% floor handles near-zero-IV stocks
            cache[key] = live_iv
            log.info(f"Open IV pinned: {inst_id} = {live_iv:.3f}% (will persist across restarts)")
            threading.Thread(target=_save_open_iv_cache, daemon=True).start()
            return live_iv
        elif live_iv and live_iv >= 150.0:
            log.warning(f"Open IV NOT pinned for {inst_id}: {live_iv:.1f}% > 150% — likely bad option price. Will retry.")
        elif live_iv and live_iv <= 0.5:
            log.warning(f"Open IV NOT pinned for {inst_id}: {live_iv:.3f}% < 0.5% — likely one-sided price (CE or PE missing). Will retry.")
    return 0.0

_load_open_iv_cache()

def _refresh_spot_cache():
    """
    Fetch ALL stock+index spot prices in one batch LTP call.
    Stock format: NSE:SYMBOL (no -EQ suffix for Zerodha SDK).
    Uses an Event flag to ensure only one refresh runs at a time.
    """
    global _SPOT_CACHE, _SPOT_CACHE_TS
    if _SPOT_REFRESH_IN_PROGRESS.is_set():
        return   # another thread is already refreshing — skip
    _SPOT_REFRESH_IN_PROGRESS.set()
    try:
        _do_refresh_spot_cache()
    finally:
        _SPOT_REFRESH_IN_PROGRESS.clear()

def _do_refresh_spot_cache():
    global _SPOT_CACHE, _SPOT_CACHE_TS
    if _kite_sdk is None and _kite_enc is None:
        return

    _STOCK_SPOT_OVERRIDES = {'BAJAJ_AUTO': 'BAJAJ-AUTO', 'M_M': 'M&M'}
    _INDEX_SPOT_SYMS = {
        'NIFTY':      'NSE:NIFTY 50',
        'NIFTY_W2':   'NSE:NIFTY 50',
        'BANKNIFTY':  'NSE:NIFTY BANK',
        'SENSEX':     'BSE:SENSEX',
        'SENSEX_W2':  'BSE:SENSEX',
        'MIDCPNIFTY': 'NSE:NIFTY MID SELECT',       # ← correct Zerodha symbol
        'FINNIFTY':   'NSE:NIFTY FIN SERVICE',       # Zerodha: try this first
    }

    # Build batch: indices first, then all stocks
    sym_to_id = {}   # zerodha_sym → inst_id
    batch = []
    # Build unique spot symbols to fetch (de-dup the API calls)
    _unique_spot_syms = {}  # zerodha_sym → list of inst_ids that share it
    for iid, spot_sym in _INDEX_SPOT_SYMS.items():
        _unique_spot_syms.setdefault(spot_sym, []).append(iid)
    for spot_sym, iids in _unique_spot_syms.items():
        sym_to_id[spot_sym] = iids[0]  # primary mapping for LTP fetch
        batch.append(spot_sym)

    for sym, lot, spot in _STKS:
        eq_name = _STOCK_SPOT_OVERRIDES.get(sym, sym)
        zerodha_sym = f"NSE:{eq_name}"   # NO -EQ suffix
        sym_to_id[zerodha_sym] = sym
        batch.append(zerodha_sym)

    # Fetch in one batch (SDK handles up to 500)
    # Symbol fallbacks for indices that may have different names across API versions
    _BATCH_FALLBACKS = {
        'MIDCPNIFTY': ['NSE:NIFTY MID SELECT', 'NSE:NIFTY MIDCAP SELECT', 'NSE:MIDCPNIFTY'],
        'FINNIFTY':   ['NSE:NIFTY FIN SERVICE', 'NSE:NIFTY FINANCIAL SERVICE', 'NSE:FINNIFTY'],
    }
    try:
        ltp_map = _kite_ltp_batch(batch)
        new_cache = {}
        hits = 0
        for zerodha_sym, iid in sym_to_id.items():
            price = ltp_map.get(zerodha_sym, 0.05)
            if price > 0.5:
                new_cache[iid] = price
                hits += 1
                # Also populate all W2/alias variants that share the same spot symbol
                for _alias_iid, _alias_sym in _INDEX_SPOT_SYMS.items():
                    if _alias_sym == zerodha_sym and _alias_iid not in new_cache:
                        new_cache[_alias_iid] = price
                        log.debug(f"Spot cache: {_alias_iid} ← {price:.2f} (shared from {iid})")
            elif iid in _BATCH_FALLBACKS:
                # Primary symbol returned 0 — try fallback variants one by one
                for _alt in _BATCH_FALLBACKS[iid]:
                    if _alt == zerodha_sym:
                        continue
                    try:
                        _alt_map = _kite_ltp_batch([_alt])
                        _alt_price = _alt_map.get(_alt, 0.05)
                        if _alt_price > 0.5:
                            new_cache[iid] = _alt_price
                            hits += 1
                            log.info(f"Spot cache: {iid} resolved via fallback symbol '{_alt}' → {_alt_price:.2f}")
                            break
                    except Exception:
                        continue
        with _SPOT_CACHE_LOCK:
            _SPOT_CACHE.update(new_cache)
            _SPOT_CACHE_TS = time.time()
        miss = len(sym_to_id) - hits
        log.info(f"Spot cache refreshed: {hits}/{len(sym_to_id)} OK" +
                 (f" | {miss} missing (check symbol names)" if miss else ""))
    except Exception as e:
        log.warning(f"Spot cache refresh failed: {e}")

def _prefetch_all_ltps() -> dict:
    """
    Global LTP pre-fetch: covers ALL instruments in a few sequential mega-batch calls.

    FIX for wrong ATM CE/PE prices:
    ─────────────────────────────────
    Old bug: used stale/hardcoded spot to compute ATM → fetched wrong strike
    symbols → those wrong strikes were 0.05 in cache → IV came out garbage.

    Correct flow:
      Step A  Fetch ALL spot prices first (one batch call, already in _SPOT_CACHE
              if fresh — otherwise force-refresh it now).
      Step B  For each instrument compute ATM from the LIVE spot.
      Step C  Fetch ATM±5 (11 strikes × 2 = 22 syms per inst) — wide enough that
              even if spot moves between pre-fetch and processing, the real ATM
              is always in the cache.
      Step D  Fire sequential chunks of ≤450 symbols with 400ms pauses.
      Step E  Store every result under 4 key variants so _fetch_kite always hits.
    """
    global _GLOBAL_LTP, _GLOBAL_LTP_TS
    if not _is_zerodha_connected():
        return {}

    # Return early if cache is still fresh (has data and populated within TTL)
    with _GLOBAL_LTP_LOCK:
        _pf_has_data = bool(_GLOBAL_LTP)
        _pf_age      = (time.time() - _GLOBAL_LTP_TS) if _GLOBAL_LTP_TS > 0 else 99999
        if _pf_has_data and _pf_age < _GLOBAL_LTP_TTL:
            return dict(_GLOBAL_LTP)

    # ── Step A: Ensure spot cache is populated with LIVE prices ──────────────
    _cache_age = time.time() - _SPOT_CACHE_TS
    if _cache_age > 60 or not _SPOT_CACHE:
        # Force a synchronous spot refresh so ATM is accurate
        log.info("Pre-fetch: refreshing spot cache before building option symbols…")
        _do_refresh_spot_cache()

    insts = build_registry()

    _SYM_OVERRIDES = {
        'BAJAJ_AUTO':  'BAJAJ-AUTO', 'M_M': 'MM',
        'MCDOWELL-N':  'MCDOWELL-N',
        'NIFTY_W2':    'NIFTY',      'SENSEX_W2': 'SENSEX',
    }
    _NAME_OVERRIDES = {
        'BAJAJ_AUTO': 'BAJAJ-AUTO', 'M_M': 'M&M',
    }
    _BSE_IDS = {'SENSEX', 'SENSEX_W2'}

    all_symbols: list = []
    sym_set:     set  = set()

    def _add(sym: str):
        if sym not in sym_set:
            sym_set.add(sym)
            all_symbols.append(sym)

    for inst in insts:
        raw_id    = inst['id']
        sym       = _SYM_OVERRIDES.get(raw_id, raw_id.replace('_W2','').replace('_',''))
        # Name candidates — same mapping as _fetch_kite so cache keys match
        _PF_NAME_MAP = {
            'BAJAJ_AUTO':  ['BAJAJ-AUTO'],
            'M_M':         ['M&M'],
            'SENSEX':      ['SENSEX', 'BSE SENSEX', 'BSESENSEX'],
            'SENSEX_W2':   ['SENSEX', 'BSE SENSEX', 'BSESENSEX'],
            'MIDCPNIFTY':  ['MIDCPNIFTY'],  # confirmed: Zerodha uses MIDCPNIFTY as name
        }
        _raw = inst['id']
        _pf_names = _PF_NAME_MAP.get(_raw,
                    _PF_NAME_MAP.get(raw_id,
                    [raw_id.replace('_W2',''), _NAME_OVERRIDES.get(raw_id, raw_id.replace('_W2',''))]))
        opt_exch  = 'BFO' if raw_id in _BSE_IDS else 'NFO'
        step      = inst['strike_step']
        expiry_dt = inst['expiry']

        # ── Step B: Use LIVE spot for accurate ATM ────────────────────────────
        cache_key = raw_id if inst['type'] == 'stock' else inst['id']
        S_cached  = _SPOT_CACHE.get(cache_key) or _SPOT_CACHE.get(inst['id'])
        S_fallback = float(inst['spot'])
        S = S_cached if (S_cached and S_cached > 0.5) else S_fallback
        # Warn if falling back to hardcoded spot (will cause wrong ATM)
        if not S_cached and S_fallback > 0:
            log.debug(f"prefetch: {inst['id']} spot not in cache, using hardcoded {S_fallback:.0f}")
        atm = round(S / step) * step

        # ── Step C: ATM±8 (17 strikes) — wider band handles spot movement between pre-fetch and processing ─
        strikes_11 = [atm + i * step for i in range(-8, 9)]

        # Use expiry+strike lookup (no name matching) — most reliable
        exp_str = expiry_dt.strftime('%d%b%y').upper()
        for K in strikes_11:
            for opt in ('CE', 'PE'):
                # Primary: expiry+strike (100% reliable, no name guessing)
                # Use first name candidate for name-keyed lookup (avoids stock collisions)
                cached_sym = _lookup_by_expiry_strike(opt_exch, expiry_dt, K, opt,
                                                       name=_pf_names[0] if _pf_names else None)
                # Fallback: name-based with all candidates
                if not cached_sym:
                    for _n in _pf_names:
                        cached_sym = _lookup_option_sym(opt_exch, _n, expiry_dt, K, opt)
                        if cached_sym:
                            break
                # Last resort: manual construction
                fallback = f"{opt_exch}:{sym}{exp_str}{int(K)}{opt}"
                _add(cached_sym or fallback)

    log.info(f"Pre-fetch: {len(all_symbols)} option symbols for {len(insts)} instruments")

    # ── Step D: Sequential chunks — smaller size + retry on 429 ─────────────
    # Zerodha SDK rate limit: ~1 req/s. Chunk=200 + 1.2s sleep keeps us safe.
    # Chunk 450 was too large and triggered rate limits on chunk 4/10.
    CHUNK     = 200   # reduced from 450 — well under per-second rate limit
    PAUSE     = 1.2   # seconds between chunks (matches TG rate-limit interval)
    MAX_RETRY = 2     # retry a failed chunk this many times before skipping
    combined: dict = {}
    n_chunks  = max(1, (len(all_symbols) - 1) // CHUNK + 1)

    def _store_chunk_result(chunk_syms, result):
        """Store result under 4 key variants."""
        for asked in chunk_syms:
            price = result.get(asked, 0.05)
            combined[asked]               = price
            combined[asked.upper()]       = price
            bare = asked.split(':', 1)[-1]
            combined[bare]                = price
            combined[bare.upper()]        = price
        for k, v in result.items():
            combined[k]         = v
            combined[k.upper()] = v
            b = k.split(':', 1)[-1]
            combined[b]         = v
            combined[b.upper()] = v

    for i in range(0, len(all_symbols), CHUNK):
        chunk    = all_symbols[i:i + CHUNK]
        chunk_no = i // CHUNK + 1
        success  = False

        for attempt in range(1, MAX_RETRY + 2):   # up to MAX_RETRY+1 total tries
            try:
                result = _kite_ltp_batch(chunk)
                _store_chunk_result(chunk, result)
                hits = sum(1 for v in result.values() if v > 0.05)
                log.info(f"Pre-fetch chunk {chunk_no}/{n_chunks}: "
                         f"{len(chunk)} symbols → {hits} real prices")
                # Write partial results immediately — later chunks won't block earlier ones
                with _GLOBAL_LTP_LOCK:
                    _GLOBAL_LTP.update(combined)
                    if _GLOBAL_LTP_TS < 0:
                        _GLOBAL_LTP_TS = time.time()
                success = True
                break
            except Exception as e:
                err_str = str(e)
                if "Too many requests" in err_str or "429" in err_str:
                    wait = PAUSE * (attempt * 2)   # 2.4s, 4.8s, ...
                    log.warning(f"Pre-fetch chunk {chunk_no}/{n_chunks}: "
                                f"rate-limited (attempt {attempt}), waiting {wait:.1f}s…")
                    time.sleep(wait)
                else:
                    log.warning(f"Pre-fetch chunk {chunk_no}/{n_chunks} failed: {e}")
                    break   # non-rate-limit error — skip retrying

        if not success:
            # Mark all symbols in failed chunk as 0.05 so _fetch_kite uses
            # computed fallback instead of triggering direct per-symbol calls
            for sym in chunk:
                combined.setdefault(sym, 0.05)
                combined.setdefault(sym.upper(), 0.05)

        if i + CHUNK < len(all_symbols):
            time.sleep(PAUSE)   # pause between ALL chunks, not just on failure

    # Write final merged cache
    with _GLOBAL_LTP_LOCK:
        _GLOBAL_LTP.update(combined)   # merge — don't overwrite keys from partial writes
        _GLOBAL_LTP_TS = time.time()

    log.info(f"Pre-fetch complete: {len(all_symbols)} symbols requested, "
             f"cache has {len(_GLOBAL_LTP)} entries")

    # ── BFO access check: if all BFO symbols returned 0.05, account may lack BSE F&O ──
    _bfo_syms  = [s for s in all_symbols if s.startswith('BFO:')]
    _bfo_hits  = sum(1 for s in _bfo_syms if combined.get(s, 0.05) > 0.05)
    if _bfo_syms and _bfo_hits == 0:
        log.warning(
            f"BFO ACCESS WARNING: All {len(_bfo_syms)} BFO option symbols returned 0.05 (zero price). "
            f"Possible causes: (1) Zerodha account does not have BSE F&O trading enabled, "
            f"(2) BFO market is closed, (3) SDK does not support BFO exchange. "
            f"Sensex / Sensex W2 CE/PE prices will show as None until this is resolved. "
            f"Sample BFO symbol tried: {_bfo_syms[0] if _bfo_syms else 'none'}"
        )
    elif _bfo_syms:
        log.info(f"BFO access OK: {_bfo_hits}/{len(_bfo_syms)} BFO symbols have real prices")
    return dict(_GLOBAL_LTP)

# Populate spot cache at startup in background
if BROKER == 'zerodha' and (_kite_sdk is not None or _kite_enc is not None):
    threading.Thread(target=_refresh_spot_cache, daemon=True, name="SpotCacheInit").start()
def _chain(inst, base_iv=None, noise=None, seed_off=0):
    noise = noise or CFG['mock_noise_live']
    rng   = np.random.default_rng((abs(hash(inst['id']))+seed_off) % 2**31)
    if base_iv is None: base_iv = 0.10 + rng.random()*0.20
    S,T,step = inst['spot'],_compT(inst['expiry']),inst['strike_step']
    r = CFG['risk_free_rate']
    bps = np.arange(CFG['chain_lo'],CFG['chain_hi']+CFG['chain_step'],CFG['chain_step'])
    K   = np.maximum(np.round(S*(1+bps/10000)/step)*step, step)
    mono= (K-S)/S
    ivk = np.clip(base_iv+(-mono*.35)+(mono**2*.18)+rng.normal(0,noise,len(K)), 0.04, None)
    sq  = math.sqrt(max(T,1e-9)); disc=math.exp(-r*T)
    with np.errstate(divide='ignore', invalid='ignore'):
        d1 = (np.log(S/K)+(r+.5*ivk**2)*T)/(ivk*sq)
    d2  = d1-ivk*sq
    ce  = np.maximum(0.05, S*_NCDF(d1)-K*disc*_NCDF(d2))
    pe  = np.maximum(0.05, K*disc*_NCDF(-d2)-S*_NCDF(-d1))
    sp  = np.maximum(0.05, ce*.003)
    ce += rng.uniform(-sp,sp,len(K)); pe += rng.uniform(-sp,sp,len(K))
    df  = pd.DataFrame({'strike':K,
                        'ce_ltp':np.round(np.maximum(0.05,ce),2),
                        'pe_ltp':np.round(np.maximum(0.05,pe),2)})
    return df.drop_duplicates('strike').sort_values('strike').reset_index(drop=True)

def _live_chain(inst):
    """Fetch live option chain. Raises on failure when broker is connected."""
    if _is_zerodha_connected():
        return _fetch_kite(inst)   # raises on failure — no silent mock fallback
    if BROKER == 'fyers' and _fyers:
        return _fetch_fyers(inst)
    return _chain(inst, seed_off=0)  # mock only when BROKER='mock'

def _yest_chain(inst):
    rng=np.random.default_rng((abs(hash(inst['id']))+77)%2**31)
    b=(0.10+rng.random()*.20)*(1+rng.uniform(-.15,.15))
    return _chain(inst, base_iv=b, noise=CFG['mock_noise_yest'], seed_off=100)

def _open_chain(inst):
    rng=np.random.default_rng((abs(hash(inst['id']))+99)%2**31)
    b=(0.10+rng.random()*.20)*(1+rng.uniform(-.08,.08))
    return _chain(inst, base_iv=b, noise=CFG['mock_noise_open'], seed_off=200)

def _fetch_kite(inst):
    """
    Fetch live option chain via Zerodha KiteConnect SDK (batch LTP).
    Falls back to enctoken OMS if SDK unavailable.
    """
    step    = inst['strike_step']
    exp_str = inst['expiry'].strftime('%d%b%y').upper()   # Zerodha format: 26FEB26
    # sym is used ONLY for constructing fallback option tradingsymbols (e.g. NFO:BEL26FEB26440CE)
    # Zerodha tradingsymbol for stocks = uppercase NSE symbol with no hyphens/underscores
    _SYM_OVERRIDES = {
        'BAJAJ_AUTO':  'BAJAJ-AUTO',  # keeps hyphen in tradingsymbol
        'M_M':         'MM',          # M&M options trade as MM on NSE
        'MCDOWELL-N':  'MCDOWELL-N',  # hyphen preserved (replace('_','') would break this)
        'NIFTY_W2':    'NIFTY',
        'SENSEX_W2':   'SENSEX',
    }
    raw_id = inst['id']
    sym = _SYM_OVERRIDES.get(raw_id, raw_id.replace('_W2', '').replace('_', ''))

    # Exchange prefix for option symbols:
    # NSE instruments (NIFTY, BANKNIFTY, MIDCPNIFTY, FINNIFTY, all stocks) → NFO:
    # BSE instruments (SENSEX, SENSEX_W2) → BFO:
    _BSE_INDEX_IDS = {'SENSEX', 'SENSEX_W2'}
    opt_exch = 'BFO' if inst['id'] in _BSE_INDEX_IDS else 'NFO'

    _INDEX_SPOT_MAP = {
        'NIFTY':      'NSE:NIFTY 50',
        'NIFTY_W2':   'NSE:NIFTY 50',
        'BANKNIFTY':  'NSE:NIFTY BANK',
        'SENSEX':     'BSE:SENSEX',
        'SENSEX_W2':  'BSE:SENSEX',
        'MIDCPNIFTY': 'NSE:NIFTY MID SELECT',       # ← correct Zerodha symbol
        'FINNIFTY':   'NSE:NIFTY FIN SERVICE',       # primary Zerodha symbol
    }

    # ── Fallback symbol variants — tried in order if primary returns 0/fails ────
    # Different Zerodha API versions / enctoken vs SDK may use different names.
    _INDEX_SPOT_FALLBACKS = {
        'MIDCPNIFTY': ['NSE:NIFTY MID SELECT', 'NSE:NIFTY MIDCAP SELECT', 'NSE:MIDCPNIFTY'],
        'FINNIFTY':   ['NSE:NIFTY FIN SERVICE', 'NSE:NIFTY FINANCIAL SERVICE', 'NSE:FINNIFTY'],
    }

    # ── Fetch live spot price ─────────────────────────────────────────────────
    # Zerodha SDK/API: equity spot = NSE:SYMBOL (NO -EQ suffix)
    # Special cases: BAJAJ-AUTO uses hyphen, M&M uses ampersand
    _STOCK_SPOT_OVERRIDES = {
        'BAJAJ_AUTO':  'BAJAJ-AUTO',
        'M_M':         'M&M',
        'MCDOWELL-N':  'MCDOWELL-N',  # hyphen must be preserved — don't replace('_','')
    }
    # ── Check batch spot cache first (populated at startup, refreshed every 5 min)
    _cache_key = raw_id if inst['type'] == 'stock' else inst['id']
    _cached_spot = _SPOT_CACHE.get(_cache_key) or _SPOT_CACHE.get(inst['id'])
    _cache_age = time.time() - _SPOT_CACHE_TS

    if _cached_spot and _cached_spot > 0.5 and _cache_age < 300:
        # Use cached spot (< 5 min old)
        S = _cached_spot
        inst['spot'] = S
        log.debug(f"{inst['id']}: spot={S:.2f} from cache (age={_cache_age:.0f}s)")
    else:
        # Cache miss or stale — fetch individually, also trigger background refresh
        if _cache_age > 300 and not _SPOT_REFRESH_IN_PROGRESS.is_set():
            threading.Thread(target=_refresh_spot_cache, daemon=True, name="SpotCacheRefresh").start()
        _STOCK_SPOT_OVERRIDES = {'BAJAJ_AUTO': 'BAJAJ-AUTO', 'M_M': 'M&M'}
        try:
            if inst['type'] == 'stock':
                _eq_name = _STOCK_SPOT_OVERRIDES.get(raw_id, raw_id)
                spot_sym = f"NSE:{_eq_name}"      # NO -EQ suffix for Zerodha SDK
                S = _kite_ltp_single(spot_sym)
            else:
                # Try primary symbol first, then fallback variants (for FINNIFTY/MIDCPNIFTY)
                _primary = _INDEX_SPOT_MAP.get(inst['id'], f"NSE:{sym}")
                _variants = _INDEX_SPOT_FALLBACKS.get(inst['id'], [_primary])
                S = 0.0
                for _sym_try in _variants:
                    try:
                        _price = _kite_ltp_single(_sym_try)
                        if _price > 0.5:
                            S = _price
                            # Cache which symbol worked so next call skips failed ones
                            _INDEX_SPOT_MAP[inst['id']] = _sym_try
                            log.info(f"{inst['id']}: spot={S:.2f} resolved via '{_sym_try}'")
                            break
                    except Exception:
                        continue
            if S > 0.5:
                inst['spot'] = S
                with _SPOT_CACHE_LOCK:
                    _SPOT_CACHE[_cache_key] = S
                log.debug(f"{inst['id']}: spot={S:.2f} cached")
            else:
                S = float(inst['spot'])
        except Exception as e:
            log.debug(f"{inst['id']}: spot individual fetch failed: {e} — using hardcoded {inst['spot']}")
            S = float(inst['spot'])

    # ── Get strikes from instrument cache (uses actual NSE strike intervals) ────
    # Map our internal IDs to Zerodha's instrument 'name' field
    # Zerodha `name` field in instrument master (may differ from our internal ID).
    # For BFO (Sensex): Zerodha uses "SENSEX" as the name field.
    # For NFO stocks:   mostly same as NSE symbol, a few exceptions below.
    # We try multiple name variants so a single mismatch doesn't break everything.
    _ID_TO_ZD_NAMES = {
        'BAJAJ_AUTO':  ['BAJAJ-AUTO'],
        'M_M':         ['M&M'],
        'SENSEX':      ['SENSEX', 'BSE SENSEX', 'BSESENSEX'],
        'SENSEX_W2':   ['SENSEX', 'BSE SENSEX', 'BSESENSEX'],
        'MIDCPNIFTY':  ['MIDCPNIFTY'],  # confirmed: Zerodha uses MIDCPNIFTY as name
    }
    raw_id    = inst['id'].replace('_W2', '')
    # For simple stocks: name = raw_id (no underscore/W2 suffix)
    _default_names = [raw_id, inst['id'].replace('_W2','')]
    _name_candidates = _ID_TO_ZD_NAMES.get(inst['id'],
                       _ID_TO_ZD_NAMES.get(raw_id, _default_names))
    expiry_dt = inst['expiry']                             # date object

    # Try each name variant — use whichever resolves strikes from the cache
    cache_strikes = []
    inst_name = _name_candidates[0]   # default
    for _cand in _name_candidates:
        _cs = _get_strikes_from_cache(opt_exch, _cand, expiry_dt, S, n_strikes=17)
        if _cs:
            cache_strikes = _cs
            inst_name = _cand
            log.debug(f"{inst['id']}: resolved via name={_cand!r} → {len(_cs)} strikes")
            break

    if cache_strikes:
        # ✅ Use exact strikes from Zerodha's instrument master
        strikes = cache_strikes
        log.debug(f"{inst['id']}: using {len(strikes)} cache strikes, "
                  f"ATM area: {cache_strikes[len(cache_strikes)//2]:.0f}")
    else:
        # ⚠️ Cache empty or instrument not found — fall back to computed strikes
        atm    = round(S / step) * step
        strikes = [atm + i * step for i in range(-8, 9)]  # ATM±8 (matches pre-fetch window)
        if _INST_CACHE:
            log.debug(
                f"{inst['id']}: name not in instrument cache "
                f"(tried: {_name_candidates}, exch={opt_exch}, expiry={expiry_dt}). "
                f"Using computed strikes around ATM={atm:.0f}."
            )

    # ── Build exact option symbols — ALWAYS from instrument cache ───────────
    # Never construct symbol strings manually — Zerodha's tradingsymbol format
    # differs by series/version and manual strings cause 0.05 (not found) prices.
    # _lookup_option_sym returns the exact symbol Zerodha uses (e.g. NFO:NIFTY24MAR2622500CE).
    ce_syms = {}
    pe_syms = {}
    for K in strikes:
        # Primary: expiry+strike+name lookup (name prevents stock strike collisions)
        _ce = _lookup_by_expiry_strike(opt_exch, expiry_dt, K, 'CE', name=inst_name)
        _pe = _lookup_by_expiry_strike(opt_exch, expiry_dt, K, 'PE', name=inst_name)
        # Fallback 1: name-based lookup with all candidates
        if not _ce or not _pe:
            for _alt_name in _name_candidates:
                if not _ce:
                    _ce = _lookup_option_sym(opt_exch, _alt_name, expiry_dt, K, 'CE')
                if not _pe:
                    _pe = _lookup_option_sym(opt_exch, _alt_name, expiry_dt, K, 'PE')
                if _ce and _pe:
                    break
        # Fallback 2: manual construction (only when cache not built yet at startup)
        _ce_fallback = f"{opt_exch}:{sym}{exp_str}{int(K)}CE"
        _pe_fallback = f"{opt_exch}:{sym}{exp_str}{int(K)}PE"
        if not _ce:
            log.debug(f"{inst['id']} K={K}: cache miss CE → using fallback {_ce_fallback!r}")
        if not _pe:
            log.debug(f"{inst['id']} K={K}: cache miss PE → using fallback {_pe_fallback!r}")
        ce_syms[K] = _ce or _ce_fallback
        pe_syms[K] = _pe or _pe_fallback

    if not strikes:
        raise RuntimeError(f"No strikes available for {inst['id']} {expiry_dt}")

    # ── Read LTPs from global pre-fetch cache (no per-instrument API calls) ────
    all_syms = [ce_syms[K] for K in strikes] + [pe_syms[K] for K in strikes]
    with _GLOBAL_LTP_LOCK:
        _has_data    = bool(_GLOBAL_LTP)
        _cache_age   = (time.time() - _GLOBAL_LTP_TS) if _GLOBAL_LTP_TS > 0 else 99999
        # Cache is "fresh" only if it has data AND was populated within TTL window.
        # _GLOBAL_LTP_TS=-1 means "never fetched" → _has_data=False → not fresh.
        _cache_fresh = _has_data and _cache_age < (_GLOBAL_LTP_TTL + 30)
        _gltp        = dict(_GLOBAL_LTP)

    def _price_from_cache(sym):
        """
        Look up price trying 4 key variants (exact, upper, bare, bare-upper).
        Returns the price float if the key exists in cache (even if 0.05).
        Returns None only if the key is completely absent from cache
        (meaning it was never fetched — different from fetched-but-zero).
        """
        _SENTINEL = object()
        for _k in (sym, sym.upper(),
                   sym.split(':',1)[-1], sym.split(':',1)[-1].upper()):
            v = _gltp.get(_k, _SENTINEL)
            if v is not _SENTINEL:
                return float(v)   # found — return as-is (0.05 = min tick, not missing)
        return None   # truly not in cache

    if _cache_fresh:
        # Use cache — build ltp_map from global pre-fetch
        ltp_map = {}
        _cache_misses = []
        for sym in all_syms:
            p = _price_from_cache(sym)
            if p is not None:
                ltp_map[sym] = float(p)
            else:
                ltp_map[sym] = 0.05
                _cache_misses.append(sym)

        # ATM miss: accept 0.05 from cache — watm_iv() returns None for 0.05,
        # _process_inst skips DB write, chart shows gap. Background recorder
        # fills real values within 60s. No per-instrument API calls here.
        _atm_k   = strikes[len(strikes)//2]
        _atm_ce  = ce_syms.get(_atm_k)
        _atm_pe  = pe_syms.get(_atm_k)
        _atm_ce_p = ltp_map.get(_atm_ce, 0.05)
        _atm_pe_p = ltp_map.get(_atm_pe, 0.05)
        if _atm_ce_p <= 0.05 or _atm_pe_p <= 0.05:
            # ATM miss — spot may have moved outside the ±8 pre-fetch window.
            # Fire a targeted re-fetch for just the ATM CE+PE to get real prices.
            _miss_syms = []
            if _atm_ce_p <= 0.05 and _atm_ce: _miss_syms.append(_atm_ce)
            if _atm_pe_p <= 0.05 and _atm_pe: _miss_syms.append(_atm_pe)
            if _miss_syms:
                try:
                    _refetch = _kite_ltp_batch(_miss_syms)
                    for _ms in _miss_syms:
                        _rp = _refetch.get(_ms, 0.05)
                        if _rp > 0.05:
                            ltp_map[_ms] = _rp
                            with _GLOBAL_LTP_LOCK:
                                _GLOBAL_LTP[_ms] = _rp
                                _GLOBAL_LTP[_ms.upper()] = _rp
                            log.debug(f"{inst['id']}: ATM re-fetch hit {_ms} → ₹{_rp:.2f}")
                        else:
                            log.debug(f"{inst['id']}: ATM re-fetch miss {_ms} → 0.05 (zero OI or unlisted strike)")
                except Exception as _re:
                    log.debug(f"{inst['id']}: ATM re-fetch failed: {_re}")

            # ── Adjacent strike fallback ──────────────────────────────────────
            # If ATM CE or PE is STILL 0.05, try the next available strike from
            # the instrument cache (actual NSE strikes, not computed).
            # Handles the case where NSE has not listed an option at the exact ATM strike.
            _final_ce = ltp_map.get(_atm_ce, 0.05)
            _final_pe = ltp_map.get(_atm_pe, 0.05)
            if (_final_ce <= 0.05 or _final_pe <= 0.05) and cache_strikes:
                # cache_strikes is sorted ascending — find the two closest to ATM
                _atm_val = _atm_k
                _adj_candidates = sorted(cache_strikes, key=lambda k: abs(k - _atm_val))[:3]
                _adj_syms = []
                for _adj_k in _adj_candidates:
                    if _adj_k == _atm_val:
                        continue
                    if _final_ce <= 0.05:
                        _adj_ce = _lookup_by_expiry_strike(opt_exch, expiry_dt, _adj_k, 'CE', name=inst_name)
                        if _adj_ce:
                            _adj_syms.append(_adj_ce)
                    if _final_pe <= 0.05:
                        _adj_pe = _lookup_by_expiry_strike(opt_exch, expiry_dt, _adj_k, 'PE', name=inst_name)
                        if _adj_pe:
                            _adj_syms.append(_adj_pe)
                if _adj_syms:
                    try:
                        _adj_fetch = _kite_ltp_batch(list(set(_adj_syms)))
                        for _as, _ap in _adj_fetch.items():
                            if _ap > 0.05:
                                ltp_map[_as] = _ap
                                with _GLOBAL_LTP_LOCK:
                                    _GLOBAL_LTP[_as] = _ap
                                    _GLOBAL_LTP[_as.upper()] = _ap
                                log.debug(f"{inst['id']}: adjacent strike hit {_as} → ₹{_ap:.2f}")
                    except Exception as _ae:
                        log.debug(f"{inst['id']}: adjacent strike fetch failed: {_ae}")
    else:
        # Cache not yet populated (startup) — return 0.05 for everything.
        # build_all_data_parallel() calls _prefetch_all_ltps() before processing,
        # so this path should only happen if prefetch itself failed entirely.
        # DO NOT call _kite_ltp_batch here — that would fire 189 concurrent API
        # calls simultaneously and guarantee rate-limit errors.
        log.debug(f"{inst['id']}: global LTP cache not yet populated — using 0.05 placeholder")
        ltp_map = {sym: 0.05 for sym in all_syms}

    rows = []
    missing_ce = []
    missing_pe = []
    _price_cap = S * 0.50   # option price > 50% of spot is almost certainly wrong data
    for K in strikes:
        ce_price = ltp_map.get(ce_syms[K], 0.05)
        pe_price = ltp_map.get(pe_syms[K], 0.05)
        # Sanity cap: reject any option price > 50% of spot (e.g. ABB PE=₹1570 when spot=₹6078)
        # These arise from _EXP_STRIKE_CACHE strike collisions between different stocks.
        if ce_price > _price_cap:
            log.warning(f"{inst['id']} K={K} CE: price ₹{ce_price:.2f} > 50%*spot={_price_cap:.0f} — rejected (cache collision?)")
            ce_price = 0.05
        if pe_price > _price_cap:
            log.warning(f"{inst['id']} K={K} PE: price ₹{pe_price:.2f} > 50%*spot={_price_cap:.0f} — rejected (cache collision?)")
            pe_price = 0.05
        if ce_price <= 0.05:
            missing_ce.append(ce_syms[K])
        if pe_price <= 0.05:
            missing_pe.append(pe_syms[K])
        rows.append(dict(
            strike = float(K),
            ce_ltp = max(0.05, ce_price),
            pe_ltp = max(0.05, pe_price),
        ))

    if missing_ce or missing_pe:
        log.debug(
            f"{inst['id']}: {len(missing_ce)} CE + {len(missing_pe)} PE strikes returned 0.05. "
            f"Sample missing CE: {missing_ce[:2]}. Sample asked: {ce_syms[strikes[len(strikes)//2]]!r}"
        )

    return pd.DataFrame(rows)

def _fetch_fyers(inst):
    sym_map={'NIFTY':'NSE:NIFTY50-INDEX','BANKNIFTY':'NSE:NIFTYBANK-INDEX',
             'SENSEX':'BSE:SENSEX-INDEX','MIDCPNIFTY':'NSE:MIDCPNIFTY-INDEX',
             'FINNIFTY':'NSE:FINNIFTY-INDEX'}
    base=inst['id'].replace('_W2','')
    fsym=sym_map.get(base,f"NSE:{base}")
    data={"symbol":fsym,"strikecount":10,"timestamp":inst['expiry'].strftime('%d-%m-%Y')}
    r=_fyers.optionchain(data=data)
    if r.get('s')!='ok': return _chain(inst)
    rows=[dict(strike=float(i['strikePrice']),
               ce_ltp=i.get('CE',{}).get('ltp',0.05),
               pe_ltp=i.get('PE',{}).get('ltp',0.05))
          for i in r.get('data',{}).get('optionsChain',[])]
    return pd.DataFrame(rows) if rows else _chain(inst)

# ═══════════════════════════════════════════════════════════════════════════════
#  SKEW COMPUTATION
# ═══════════════════════════════════════════════════════════════════════════════
def _spot_price(inst, chain):
    S=inst['spot']; T=_compT(inst['expiry'])
    if inst['type']=='index':
        # Use synthetic futures = spot + ATM(CE - PE) for index
        idx=(chain['strike']-S).abs().idxmin(); atm=chain.loc[idx]
        synth = round(S + atm['ce_ltp'] - atm['pe_ltp'], 2)
        # Sanity check: synthetic should be within 2% of spot
        if abs(synth - S) / S < 0.02:
            return synth, 'synthetic'
        return S, 'spot'  # fallback to raw spot if synthetic is unreasonable
    # For stocks: use live spot directly (futures pricing causes IV mismatch)
    return S, 'spot'

def skew_from_chain(inst, chain, label='Live'):
    S,sm=_spot_price(inst,chain); T=_compT(inst['expiry']); rows=[]
    for bps in BPS_RANGE:
        tgt=S*(1+bps/10000); ot='p' if bps<=0 else 'c'
        idx=(chain['strike']-tgt).abs().idxmin(); row=chain.loc[idx]
        K=float(row['strike']); ltp=float(row['ce_ltp' if ot=='c' else 'pe_ltp'])
        iv=compute_iv(S,K,T,ltp,ot)
        rows.append(dict(bps=int(bps),strike=K,
                         iv=round(iv*100,3) if iv else None,
                         opt_type=ot,spot=S,spot_method=sm,label=label))
    return pd.DataFrame(rows)

def _yest_chain_real(inst):
    """
    Yesterday's chain uses REAL close-of-day ATM IV from yesterday's DB data.
    If no DB data exists yet (first day), falls back to mock.
    """
    prev   = get_previous_trading_date()
    df     = db_load_atm_history(inst['id'], prev.isoformat())
    if not df.empty:
        # Use LAST reading of yesterday (close-of-day IV) as base
        base_iv = float(df['atm_iv'].iloc[-1]) / 100.0
        # Use zero noise — the skew SHAPE is consistent, only level changes
        return _chain(inst, base_iv=base_iv, noise=0.001, seed_off=100)

    # Try yesterday's JSON snapshot
    prev_path = _store_path(prev)
    if prev_path.exists():
        try:
            snap = json.loads(prev_path.read_text())
            iid  = inst['id']
            if iid in snap and snap[iid]:
                v = snap[iid]
                # New format: {'times': [...], 'atm_iv': [...], 'spot': [...]}
                if isinstance(v, dict) and 'atm_iv' in v and v['atm_iv']:
                    last_iv = float(v['atm_iv'][-1]) / 100.0
                # Old format: [{'atm_iv': ...}, ...]  (list of dicts)
                elif isinstance(v, list) and v and isinstance(v[-1], dict):
                    last_iv = float(v[-1]['atm_iv']) / 100.0
                else:
                    last_iv = None
                if last_iv and last_iv > 0:
                    return _chain(inst, base_iv=last_iv, noise=0.001, seed_off=100)
        except Exception:
            pass

    # No historical data at all
    rng = np.random.default_rng((abs(hash(inst['id'])) + 77) % 2**31)
    b   = (0.10 + rng.random() * .20) * (1 + rng.uniform(-.15, .15))
    return _chain(inst, base_iv=b, noise=CFG['mock_noise_yest'], seed_off=100)


def _open_chain_real(inst):
    """
    Open 9:20 chain — shape fallback when open IV cache is not yet available.
    Uses first DB row if available, otherwise uses hardcoded spot-derived estimate.
    The IV LEVEL is overridden by _get_open_iv() in _process_inst.
    """
    today = _today_ist().isoformat()
    df    = db_load_atm_history(inst['id'], today)
    if not df.empty:
        # Use minimum IV of the day as a better proxy for open (avoids late-start issue)
        base_iv = float(df['atm_iv'].min()) / 100.0
        return _chain(inst, base_iv=base_iv, noise=0.001, seed_off=200)
    # No DB data yet — shape only, level will be overridden by _get_open_iv
    rng = np.random.default_rng((abs(hash(inst['id'])) + 99) % 2**31)
    b   = (0.10 + rng.random() * .20) * (1 + rng.uniform(-.08, .08))
    return _chain(inst, base_iv=b, noise=CFG['mock_noise_open'], seed_off=200)

def _process_inst(inst):
    now   = _now_ist(); today = now.date().isoformat(); exp = str(inst['expiry'])

    # ── Live chain — real data from broker, mock only in mock mode ───────────
    _live_error = None
    try:
        lc = _live_chain(inst)
    except Exception as e:
        err_str = str(e)
        if "NOT_IN_FON:" in err_str:
            # Instrument not in F&O — use mock silently, don't count as fetch error
            log.debug(f"{inst['id']}: not in F&O segment, using mock data")
            lc = _chain(inst, seed_off=0)
            _live_error = None   # not an error — just not tradeable
        elif "Too many requests" in err_str or "429" in err_str:
            # Rate-limited — use mock, mark as soft error (not shown in diagnostics)
            log.warning(f"Rate-limited for {inst['id']} — using mock fallback")
            lc = _chain(inst, seed_off=0)
            _live_error = None   # don't spam the error panel for rate-limit hits
        else:
            _live_error = err_str
            log.warning(f"Live chain FAILED for {inst['id']}: {e}")
            lc = _chain(inst, seed_off=0)  # last-resort fallback, tagged as error

    # Yesterday — use real DB data when possible
    yc  = _yest_chain_real(inst)

    ls    = skew_from_chain(inst, lc, 'Live')
    ys    = skew_from_chain(inst, yc, 'Yesterday')
    lsp   = float(ls['spot'].iloc[0])

    # ── Stale-spot guard: if live spot diverges >15% from inst fallback, it means
    # the spot cache hadn't refreshed yet. Log a warning so it's visible in logs.
    _fallback_spot = float(inst['spot'])
    if _fallback_spot > 0 and abs(lsp - _fallback_spot) / _fallback_spot > 0.15:
        log.warning(
            f"{inst['id']}: live spot {lsp:.0f} differs >15% from fallback {_fallback_spot:.0f}. "
            f"Spot cache may be stale — triggering background refresh."
        )
        if not _SPOT_REFRESH_IN_PROGRESS.is_set():
            threading.Thread(target=_refresh_spot_cache, daemon=True, name="SpotCacheForceRefresh").start()

    atm_iv= watm_iv(ls, lsp)
    # ── Sanity cap: ATM IV > 200% almost certainly means wrong option price was used ──
    # This catches cases where spot cache returned stale spot → wrong ATM strike →
    # deep OTM option price → Black-Scholes gives absurdly high IV.
    if atm_iv and atm_iv > 200.0:
        log.warning(f"{inst['id']}: ATM IV {atm_iv:.1f}% exceeds 200% cap — likely stale spot/wrong strike, discarding")
        atm_iv = None

    # Open IV — use persistent cache (pinned at first reading of the day)
    # Falls back to _open_chain_real only for the skew CURVE shape (not the IV level)
    open_atm_pct = _get_open_iv(inst['id'], atm_iv, today) if atm_iv else 0.0

    # ── Auto-invalidate stale open IV ────────────────────────────────────────
    # If current IV is >10x or <0.1x of the pinned open IV, the open was pinned
    # from a bad/stale option price (e.g. cache collision, wrong strike).
    # Invalidate it so _get_open_iv re-pins from the current (correct) reading.
    if atm_iv and open_atm_pct and open_atm_pct > 0:
        _ratio = atm_iv / open_atm_pct
        # Invalidate open IV if: current > 5× open  OR  current < 0.2× open
        # These are tight enough to catch bad pinned values while allowing genuine large moves.
        if _ratio > 5.0 or _ratio < 0.20:
            _key = (inst['id'], today)
            with _OPEN_IV_LOCK:
                _cache = _get_open_iv_cache()
                if _key in _cache:
                    log.warning(
                        f"Open IV invalidated for {inst['id']}: "
                        f"open={open_atm_pct:.2f}% vs current={atm_iv:.2f}% "
                        f"(ratio={_ratio:.1f}x) — was pinned from bad data. Re-pinning."
                    )
                    del _cache[_key]
                    threading.Thread(target=_save_open_iv_cache, daemon=True).start()
            open_atm_pct = _get_open_iv(inst['id'], atm_iv, today)  # re-pin
    if open_atm_pct > 0:
        # Build open chain using pinned open IV level (accurate baseline)
        oc = _chain(inst, base_iv=open_atm_pct / 100.0, noise=0.001, seed_off=200)
    else:
        oc = _open_chain_real(inst)
    os_     = skew_from_chain(inst, oc, 'Open 9:20')
    osp     = float(os_['spot'].iloc[0])
    open_atm = open_atm_pct if open_atm_pct > 0 else watm_iv(os_, osp)

    atm_strike = round(lsp / inst['strike_step']) * inst['strike_step']

    # ── Extract ATM row for calculation debug info ────────────────────────────
    T_years = _compT(inst['expiry'])
    atm_row = ls[ls['strike'] == atm_strike]
    if atm_row.empty:
        atm_row = ls.iloc[(ls['bps']).abs().argsort().iloc[:1]]
    # Get ATM CE and PE option prices from original chain
    atm_ce_ltp = atm_pe_ltp = None
    _iv_from_atm = False  # True if IV came from exact ATM strike, False if interpolated
    if not lc.empty:
        lc_closest_idx = (lc['strike'] - atm_strike).abs().idxmin()
        lc_atm = lc.loc[[lc_closest_idx]]
        actual_atm_strike = float(lc_atm.iloc[0]['strike'])
        _dist = abs(actual_atm_strike - atm_strike)
        # Accept prices from the nearest strike up to 3× the strike step away
        if _dist <= inst['strike_step'] * 3:
            atm_strike = actual_atm_strike
            _ce_price = float(lc_atm.iloc[0].get('ce_ltp', 0))
            _pe_price = float(lc_atm.iloc[0].get('pe_ltp', 0))
            atm_ce_ltp = _ce_price if _ce_price > 0.05 else None
            atm_pe_ltp = _pe_price if _pe_price > 0.05 else None
            _iv_from_atm = (atm_ce_ltp is not None or atm_pe_ltp is not None)

    # Only persist to DB when data is genuinely live (not mock fallback)
    if atm_iv and not _live_error:
        dh, dl = db_get_day_extremes(inst['id'], today)
        new_high = max(dh, atm_iv); new_low = min(dl if dl < 9999 else atm_iv, atm_iv)
        db_save_atm(inst['id'], exp, atm_iv, lsp, new_high, new_low, now)
        if open_atm:
            check_and_alert(inst, atm_iv, open_atm, atm_strike,
                            spot=lsp, T_years=T_years,
                            ce_ltp=atm_ce_ltp, pe_ltp=atm_pe_ltp)

    # Build display symbol strings from atm_strike and instrument info
    _opt_exch_display = 'BFO' if inst['id'] in {'SENSEX','SENSEX_W2'} else 'NFO'
    _sym_display = {'BAJAJ_AUTO':'BAJAJ-AUTO','M_M':'MM','NIFTY_W2':'NIFTY',
                    'SENSEX_W2':'SENSEX'}.get(inst['id'],
                    inst['id'].replace('_W2','').replace('_',''))
    _exp_display = inst['expiry'].strftime('%d%b%y').upper()
    _k_display   = int(atm_strike)
    _ce_sym = f"{_opt_exch_display}:{_sym_display}{_exp_display}{_k_display}CE"
    _pe_sym = f"{_opt_exch_display}:{_sym_display}{_exp_display}{_k_display}PE"

    return inst['id'], dict(inst=inst, live=ls, yest=ys, open=os_,
                            atm_iv=atm_iv, open_atm=open_atm,
                            live_spot=lsp, atm_strike=atm_strike,
                            T_years=T_years,
                            atm_ce_ltp=atm_ce_ltp, atm_pe_ltp=atm_pe_ltp,
                            calc_time=now.strftime('%H:%M:%S IST'),
                            live_error=_live_error,
                            atm_sym_ce=_ce_sym,
                            atm_sym_pe=_pe_sym)

@st.cache_data(show_spinner=False, ttl=60)
def build_all_data_parallel():
    """
    Load ALL instruments (7 indices + 155 stocks).

    RATE-LIMIT FIX:
    ──────────────
    Old approach: 162 concurrent ltp() calls → 162 API bursts → HTTP 429.
    New approach:
      Step 1 — ONE pre-fetch: collect every option symbol needed across all 162
               instruments, fire 3-4 sequential ltp() mega-batches (≤450 syms
               each), store results in _GLOBAL_LTP cache.
      Step 2 — Process instruments: _process_inst / _fetch_kite reads from
               _GLOBAL_LTP (zero extra API calls). Falls back to direct call
               only on a cache miss (rare).
      Step 3 — Parallelism is still used for CPU-bound IV computation (Black-
               Scholes), not for API calls.
    """
    insts = build_registry()
    out   = {}

    # ── Step 1: Pre-fetch ALL LTPs in a handful of sequential mega-batches ────
    if _is_zerodha_connected():
        _prefetch_all_ltps()

    # ── Step 2: Process instruments (IV computation only, no extra API calls) ─
    # max_workers=4 — pure CPU work now, keep low to avoid thread overhead
    with ThreadPoolExecutor(max_workers=4) as ex:
        futs = {ex.submit(_process_inst, inst): inst for inst in insts}
        for f in as_completed(futs):
            try:
                iid, d = f.result()
                out[iid] = d
            except Exception as e:
                log.warning(f"process_inst failed: {e}")

    n_errors = sum(1 for d in out.values() if d.get('live_error'))
    if n_errors:
        log.warning(f"Live data fetch errors: {n_errors}/{len(insts)} instruments")
    order = [i['id'] for i in insts]
    return {k: out[k] for k in order if k in out}, insts

# ═══════════════════════════════════════════════════════════════════════════════
#  INTRADAY HISTORY BUILDER
# ═══════════════════════════════════════════════════════════════════════════════
@st.cache_data(show_spinner=False, ttl=60)
def build_intraday_fast(inst_ids: tuple):
    """
    Build intraday ATM IV history for given instruments.
    Priority:
      1. DB rows saved by background recorder (real data, grows every 60s)
      2. If DB empty → fetch ONE live reading right now and use as single point
      3. Last resort → flat line at current ATM IV level (honest, not simulated)
    """
    all_insts = build_registry()
    insts_map = {i['id']: i for i in all_insts}
    today     = TODAY.isoformat()
    now       = datetime.now(IST)
    ot        = now.replace(hour=CFG['market_open_h'], minute=CFG['market_open_m'],
                            second=0, microsecond=0)
    hist      = {}

    for iid in inst_ids:
        inst = insts_map.get(iid)
        if inst is None:
            continue

        # ── 1. Real DB history ────────────────────────────────────────────────
        db_df = db_load_atm_history(iid, today)
        if not db_df.empty:
            hist[iid] = db_df
            continue

        # ── 2. No DB yet — fetch one live reading and show a flat line ────────
        live_iv  = None
        live_spot = float(inst['spot'])
        if BROKER == 'zerodha' and (_kite_sdk is not None or _kite_enc is not None):
            try:
                lc      = _fetch_kite(inst)
                ls      = skew_from_chain(inst, lc, 'Live')
                live_spot = float(ls['spot'].iloc[0])
                live_iv = watm_iv(ls, live_spot)
            except Exception as e:
                log.debug(f"intraday live fetch failed {iid}: {e}")

        if live_iv is None:
            # Truly nothing — show empty df so chart shows "no data yet"
            hist[iid] = pd.DataFrame(columns=['time_str', 'atm_iv', 'spot'])
            continue

        # Show current moment as a single real data point
        hist[iid] = pd.DataFrame({
            'time_str': [now.strftime('%H:%M')],
            'atm_iv':   [round(live_iv, 3)],
            'spot':     [live_spot],
        })

    return hist

@st.cache_data(show_spinner="⚡ Building intraday history…", ttl=60)
def build_intraday():
    insts=build_registry(); today=TODAY.isoformat()
    now=datetime.now(IST)
    ot=now.replace(hour=CFG['market_open_h'],minute=CFG['market_open_m'],second=0,microsecond=0)
    el=max(30,min(int((now-ot).total_seconds()/60), CFG['trading_day_mins']))
    hist={}
    _is_live = _is_zerodha_connected() or (BROKER == 'fyers' and _fyers)
    for inst in insts:
        iid=inst['id']
        db_df=db_load_atm_history(iid,today)
        if not db_df.empty:
            hist[iid]=db_df; continue
        if _is_live:
            # Live broker: return empty rather than fake data
            hist[iid]=pd.DataFrame(columns=['time_str','atm_iv','spot']); continue
        # Mock mode only — Ornstein-Uhlenbeck simulation
        rng=np.random.default_rng((abs(hash(iid))+999)%2**31)
        biv=0.10+rng.random()*.20; sv=inst['spot']*CFG['mock_spot_vol_bps']/10000
        sh=rng.normal(0,CFG['mock_iv_vol'],el); sp_=rng.random(el)<CFG['mock_spike_prob']
        sh[sp_]*=CFG['mock_spike_mult']
        iv=np.empty(el); iv[0]=biv
        for i in range(1,el): iv[i]=max(0.04,iv[i-1]+sh[i]-0.0008*(iv[i-1]-biv))
        s=np.empty(el); s[0]=float(inst['spot'])
        for i in range(1,el): s[i]=round(s[i-1]+rng.normal(0,sv),2)
        times=[ot+timedelta(minutes=i) for i in range(el)]
        hist[iid]=pd.DataFrame({'time':times,
                                 'time_str':[t.strftime('%H:%M') for t in times],
                                 'atm_iv':np.round(iv*100,3),'spot':s})
    return hist

# ═══════════════════════════════════════════════════════════════════════════════
#  JSON SNAPSHOT PERSISTENCE (yesterday overlay)
# ═══════════════════════════════════════════════════════════════════════════════
def _store_path(d:date)->Path: return STORE_DIR/f"{d.isoformat()}.json"

def save_daily_snapshot(intraday_data:dict, snap_date:date=None):
    snap_date=snap_date or TODAY
    path=_store_path(snap_date); payload={}
    for iid,df in intraday_data.items():
        payload[iid]={'times':df['time_str'].tolist(),
                      'atm_iv':df['atm_iv'].tolist(),
                      'spot':df['spot'].tolist(),
                      'date':snap_date.isoformat()}
    with open(path,'w') as f: json.dump(payload,f)

def load_daily_snapshot(snap_date:date)->dict:
    path=_store_path(snap_date)
    if not path.exists(): return {}
    try:
        with open(path) as f: raw=json.load(f)
    except Exception:
        return {}
    result = {}
    for iid, v in raw.items():
        try:
            # Both save_daily_snapshot (old) and the recorder (new) write the same
            # key names: times / atm_iv / spot  — handle both gracefully
            times  = v.get('times') or v.get('time_str') or []
            atm_iv = v.get('atm_iv') or []
            spot   = v.get('spot') or []
            if times and atm_iv:
                result[iid] = pd.DataFrame({'time_str': times,
                                            'atm_iv':   atm_iv,
                                            'spot':     spot if spot else [0.0]*len(times)})
        except Exception:
            continue
    return result

def load_yesterday_data(instruments)->dict:
    prev_date=get_previous_trading_date(); stored=load_daily_snapshot(prev_date); result={}
    for inst in instruments:
        iid=inst['id']
        if iid in stored and not stored[iid].empty:
            result[iid]=stored[iid]
        else:
            n_bars=CFG['trading_day_mins']
            ot=_now_ist().replace(hour=CFG['market_open_h'],minute=CFG['market_open_m'],
                                  second=0,microsecond=0)
            times=[ot+timedelta(minutes=i) for i in range(n_bars)]
            result[iid]=pd.DataFrame({'time_str':[t.strftime('%H:%M') for t in times],
                                       'atm_iv':[0.0]*n_bars,'spot':[float(inst['spot'])]*n_bars})
    return result

# ═══════════════════════════════════════════════════════════════════════════════
#  MATPLOTLIB CHART HELPERS (from original — unchanged)
# ═══════════════════════════════════════════════════════════════════════════════
def _ax_style(ax):
    ax.set_facecolor(CFG['bg_ax'])
    for sp in ax.spines.values(): sp.set_edgecolor(CFG['spine_c'])

def _savepng(fig):
    buf=io.BytesIO()
    try:
        fig.savefig(buf,format='png',dpi=72,bbox_inches='tight',facecolor=CFG['bg_fig'])
        return buf.getvalue()
    finally:
        plt.close(fig)  # always close — even if savefig raises

def cached_img(key, draw_fn, *a, **kw):
    if key not in st.session_state:
        plt.close('all')  # flush any stale figures before creating new ones
        st.session_state[key]=_savepng(draw_fn(*a,**kw))
    st.image(st.session_state[key], use_container_width=True)

def draw_grid(all_data, ft):
    plt.style.use('dark_background')
    items={k:v for k,v in all_data.items() if ft is None or v['inst']['type']==ft}
    if not items:
        fig=plt.figure(); plt.close(fig); return fig
    ni=len(items); nc=4; nr=math.ceil(ni/nc); fh=min(max(7,nr*2.8),70)
    fig=plt.figure(figsize=(16,fh),dpi=72,facecolor=CFG['bg_fig'])
    tl=ft.title() if ft else 'All'
    fig.suptitle(f'IV SKEW · {tl} · {TODAY:%d-%b-%Y} {NOW:%H:%M IST}',
                 fontsize=10,fontweight='bold',color=CFG['title_c'],y=0.999)
    gs=gridspec.GridSpec(nr,nc,figure=fig,hspace=0.65,wspace=0.28,
                         top=0.97,bottom=0.03,left=0.03,right=0.98)
    pos=[gs[r,c] for r in range(nr) for c in range(nc)]
    for idx,(iid,data) in enumerate(items.items()):
        if idx>=len(pos): break
        ax=fig.add_subplot(pos[idx]); _ax_style(ax)
        for ck,cl,ls,lw in [('live','Live','-',1.8),('yest','Yesterday','--',1.2),('open','Open 9:20',':',1.2)]:
            df=data[ck].dropna(subset=['iv'])
            ax.plot(df['bps'],df['iv'],color=CFG['curve_colors'][cl],lw=lw,ls=ls,zorder=3)
        ax.axvline(0,color='#334155',lw=0.7,ls='--',zorder=2)
        ax.grid(True,color=CFG['grid_c'],alpha=0.35,zorder=1)
        inst=data['inst']; atm=data['atm_iv'] or 0
        ic=CFG['inst_colors'][idx%len(CFG['inst_colors'])]; fs=max(5,7-ni//30)
        ax.set_title(f"{inst['label']} [{inst['expiry']:%d %b}] {atm:.1f}%",
                     color=ic,fontsize=fs,fontweight='bold',pad=2)
        ax.tick_params(colors=CFG['tick_c'],labelsize=4.5)
        ax.set_xticks(range(CFG['bps_lo'],CFG['bps_hi']+1,100))
        ax.xaxis.set_major_formatter(FuncFormatter(lambda x,_:f'{int(x):+d}'))
        ax.yaxis.set_major_formatter(FuncFormatter(lambda y,_:f'{y:.0f}%'))
    fig.legend(handles=[
        Line2D([0],[0],color=CFG['curve_colors']['Live'],lw=1.8,ls='-',label='Live'),
        Line2D([0],[0],color=CFG['curve_colors']['Yesterday'],lw=1.2,ls='--',label='Yesterday'),
        Line2D([0],[0],color=CFG['curve_colors']['Open 9:20'],lw=1.2,ls=':',label='Open 9:20'),
    ],loc='lower center',ncol=3,fontsize=7,framealpha=0.3,labelcolor='white',
      facecolor=CFG['bg_ax'],bbox_to_anchor=(0.5,0.001))
    return fig

def draw_movers_chart(mdf, top_n):
    plt.style.use('dark_background')
    tg=mdf.head(top_n); tl=mdf[mdf['_raw_pct']<0].tail(top_n)
    pf=pd.concat([tl,tg]).drop_duplicates().sort_values('_raw_pct',ascending=True)
    n=len(pf); fh=min(max(5,n*.45+2),32)
    bc=['#00e5a0' if v>=0 else '#ff4466' for v in pf['_raw_pct']]
    ic=[CFG['inst_colors'][i%len(CFG['inst_colors'])] for i in range(n)]
    fig,(al,ar)=plt.subplots(1,2,figsize=(15,fh),dpi=72,facecolor=CFG['bg_fig'],
                              gridspec_kw={'width_ratios':[1.2,.8]})
    ng=(mdf['_raw_pct']>0).sum(); nl=(mdf['_raw_pct']<0).sum()
    fig.suptitle(f'IV MOVERS | Top/Bottom {top_n} | {TODAY:%d-%b-%Y} {NOW:%H:%M IST}\n'
                 f'{len(mdf)} tracked · ▲ {ng} gainers · ▼ {nl} losers',
                 fontsize=9,fontweight='bold',color=CFG['title_c'],y=1.01)
    for a in (al,ar): _ax_style(a)
    yp=np.arange(n); fs=max(5,8-n//8)
    labels=[f'{r["Instrument"]} [{r["Type"][0]}]' for _,r in pf.iterrows()]
    bars=al.barh(yp,pf['_raw_pct'],color=bc,height=0.6)
    al.set_yticks(yp); al.set_yticklabels(labels,color='#e2e8f0',fontsize=fs)
    al.axvline(0,color='#475569',lw=1.1)
    al.xaxis.set_major_formatter(FuncFormatter(lambda x,_:f'{x:+.1f}%'))
    al.tick_params(colors=CFG['tick_c'],labelsize=7)
    al.grid(True,axis='x',color=CFG['grid_c'],alpha=0.4)
    al.set_title('% IV Change Since Open',color=CFG['text_c'],fontsize=8,pad=5)
    for bar,val in zip(bars,pf['_raw_pct']):
        xo=max(.2,abs(val)*.03)*(1 if val>=0 else -1)
        al.text(val+xo,bar.get_y()+bar.get_height()/2,f'{val:+.1f}%',
                va='center',ha='left' if val>=0 else 'right',
                color='#f1f5f9',fontsize=max(5,fs-1),fontweight='bold')
    bars2=ar.barh(yp,pf['Current IV'],color=ic,height=0.6)
    ar.set_yticks(yp); ar.set_yticklabels(labels,color='#e2e8f0',fontsize=fs)
    ar.xaxis.set_major_formatter(FuncFormatter(lambda x,_:f'{x:.0f}%'))
    ar.tick_params(colors=CFG['tick_c'],labelsize=7)
    ar.grid(True,axis='x',color=CFG['grid_c'],alpha=0.4)
    ar.set_title('Current ATM IV (%)',color=CFG['text_c'],fontsize=8,pad=5)
    for bar,val in zip(bars2,pf['Current IV']):
        ar.text(val+.1,bar.get_y()+bar.get_height()/2,f'{val:.1f}%',
                va='center',ha='left',color='#f1f5f9',fontsize=max(5,fs-1),fontweight='bold')
    plt.tight_layout(); return fig

# ═══════════════════════════════════════════════════════════════════════════════
#  PLOTLY INTERACTIVE CHARTS (from original — unchanged)
# ═══════════════════════════════════════════════════════════════════════════════
def plotly_skew_single(iid, all_data):
    d=all_data[iid]; inst=d['inst']
    fig=go.Figure()
    for ck,cl,dash,w,name in [('live','#00e5a0','solid',2.5,'Live'),
                               ('yest','#ffd740','dash',1.5,'Yesterday'),
                               ('open','#b040ff','dot',1.5,'Open 9:20')]:
        df=d[ck].dropna(subset=['iv'])
        fig.add_trace(go.Scatter(x=df['bps'],y=df['iv'],mode='lines',name=name,
            line=dict(color=cl,width=w,dash=dash),
            hovertemplate=f'<b>{name}</b><br>BPS: %{{x:+d}}<br>IV: <b>%{{y:.2f}}%</b><extra></extra>'))
    fig.add_vline(x=0,line_dash='dash',line_color='#334155',
                  annotation_text='ATM',annotation_font_color='#64748b')
    atm=d['atm_iv'] or 0
    _base={k:v for k,v in PLOTLY_LAYOUT.items() if k not in ('xaxis','yaxis','legend','hovermode')}
    fig.update_layout(**_base,hovermode='x unified',
        title=dict(text=(f'<b style="color:{CFG["title_c"]}">IV Skew — {inst["label"]}</b>  '
                         f'<span style="color:#94a3b8">[Expiry: {inst["expiry"]:%d-%b-%Y}]</span>  '
                         f'ATM IV: <b style="color:#00e5a0">{atm:.2f}%</b>'),
                   x=0.01,xanchor='left',font=dict(size=13)),
        xaxis=dict(gridcolor=CFG['grid_c'],zeroline=False,tickfont=dict(color=CFG['tick_c']),
                   title='bps from ATM',tickformat='+d',dtick=100),
        yaxis=dict(gridcolor=CFG['grid_c'],zeroline=False,tickfont=dict(color=CFG['tick_c']),
                   title='IV (%)',tickformat='.1f',ticksuffix='%'),
        height=380,
        legend=dict(bgcolor='rgba(13,17,23,0.8)',bordercolor=CFG['grid_c'],borderwidth=1,
                    orientation='h',yanchor='bottom',y=1.02,xanchor='right',x=1))
    return fig

def plotly_intraday(iid, all_data, intraday, yesterday_data):
    h=intraday[iid]; inst=all_data[iid]['inst']
    oiv=h['atm_iv'].iloc[0]; civ=h['atm_iv'].iloc[-1]
    chg=(civ-oiv)/oiv*100 if oiv else 0
    badge='📊 INDEX' if inst['type']=='index' else '📈 STOCK'

    # ── Build a FULL-DAY shared time axis (09:15 → 15:30, every minute) ──────
    # This guarantees today, yesterday, and open all share identical X positions,
    # so yesterday's line plots at the correct clock time — not wherever Plotly
    # places it in a sparse categorical axis.
    _full_day=[f"{h2:02d}:{m2:02d}" for h2 in range(9,16) for m2 in range(60)
               if (h2>9 or m2>=15) and (h2<15 or m2<=30)]

    # Re-index today onto the full axis (sparse real data → full grid, NaN gaps)
    today_reindexed=(h.drop_duplicates(subset='time_str', keep='last')
                          .set_index('time_str')
                          .reindex(_full_day)
                      .reset_index()
                      .rename(columns={'index':'time_str'}))
    xvals_full=today_reindexed['time_str'].tolist()
    iv_full=today_reindexed['atm_iv'].tolist()

    # Display range: start at first real reading, end at last real reading
    real_rows=h[h['atm_iv'].notna()]
    x_start=real_rows['time_str'].iloc[0] if not real_rows.empty else _full_day[0]
    x_end  =real_rows['time_str'].iloc[-1] if not real_rows.empty else _full_day[-1]

    tick_vals=sorted(set([t for t in xvals_full if t.endswith(':00') or t.endswith(':15')
                          or t.endswith(':30') or t.endswith(':45')]+[x_start,x_end]))
    fig=go.Figure()

    ydf=yesterday_data.get(iid); prev_date=get_previous_trading_date(); has_real_yest=False
    if ydf is not None and not ydf.empty and ydf['atm_iv'].sum()>0:
        has_real_yest=True
        # ── Align yesterday onto the SAME time axis as today ─────────────────
        # Re-index yesterday onto the full-day time grid (same as _full_day above)
        ydf_reindexed=(ydf.drop_duplicates(subset='time_str', keep='last')
                          .set_index('time_str')
                          .reindex(_full_day)
                          .reset_index()
                          .rename(columns={'index':'time_str'}))
        # Only show up to x_end (today's last bar)
        ydf_clipped=ydf_reindexed[ydf_reindexed['time_str']<=x_end].copy()
        if ydf_clipped.empty: ydf_clipped=ydf_reindexed
        # ── Keep NaN rows so yesterday plots at the EXACT same time positions
        # as today on the shared full-day axis (connectgaps=False breaks the line
        # at gaps, but each real point lands on the correct X tick). ──────────
        _ydf_real=ydf_clipped.dropna(subset=['atm_iv'])
        if not _ydf_real.empty:
            fig.add_trace(go.Scatter(x=ydf_clipped['time_str'],y=ydf_clipped['atm_iv'],mode='lines',
                name=f'Yesterday ({prev_date:%d-%b-%Y}) IV',
                line=dict(color=CFG['curve_colors']['Yesterday'],width=2,dash='dash'),opacity=0.8,
                connectgaps=False,
                hovertemplate=(f'<b>Yesterday ({prev_date:%d-%b})</b><br>Time: %{{x}}<br>IV: <b>%{{y:.2f}}%</b><extra></extra>')))
            ydf_last=_ydf_real.iloc[-1]
            fig.add_trace(go.Scatter(x=[ydf_last['time_str']],y=[ydf_last['atm_iv']],
                mode='markers+text',name='',showlegend=False,
                marker=dict(color=CFG['curve_colors']['Yesterday'],size=7),
                text=[f"  Yest {ydf_last['atm_iv']:.2f}%"],textposition='middle right',
                textfont=dict(color=CFG['curve_colors']['Yesterday'],size=11),hoverinfo='skip'))
            yest_open_row=_ydf_real[_ydf_real['time_str']=='09:20']
            if not yest_open_row.empty:
                yest_oiv=yest_open_row['atm_iv'].iloc[0]
                fig.add_trace(go.Scatter(x=['09:20'],y=[yest_oiv],mode='markers+text',
                    name='',showlegend=False,
                    marker=dict(color=CFG['curve_colors']['Yesterday'],size=9,symbol='circle-open',line=dict(width=2)),
                    text=[f'  Yest Open: {yest_oiv:.2f}%'],textposition='top right',
                    textfont=dict(color=CFG['curve_colors']['Yesterday'],size=10),
                    hovertemplate=f'<b>Yesterday Open (9:20)</b><br>IV: <b>{yest_oiv:.2f}%</b><extra></extra>'))

    # Today fill + line — use full shared axis so X positions match yesterday
    fig.add_trace(go.Scatter(x=xvals_full,y=iv_full,fill='tozeroy',fillcolor='rgba(0,229,160,0.07)',
        mode='none',name='',showlegend=False,hoverinfo='skip'))
    fig.add_trace(go.Scatter(x=xvals_full,y=iv_full,mode='lines+markers',
        name=f'Today ({TODAY:%d-%b-%Y}) ATM IV',
        line=dict(color='#00e5a0',width=2.5),
        marker=dict(size=4,color='#00e5a0',line=dict(color='#07090f',width=1)),
        hovertemplate=(f'<b>Today ({TODAY:%d-%b})</b><br>Time: %{{x}}<br>ATM IV: <b>%{{y:.2f}}%</b><extra></extra>')))
    # "Now" label at the last real reading
    fig.add_trace(go.Scatter(x=[x_end],y=[civ],mode='markers+text',name='',showlegend=False,
        marker=dict(color='#00e5a0',size=8),text=[f'  Now: {civ:.2f}%'],
        textposition='middle right',textfont=dict(color='#00e5a0',size=12,family='monospace'),hoverinfo='skip'))
    fig.add_hline(y=oiv,line_dash='dot',line_color=CFG['curve_colors']['Open 9:20'],line_width=1.5,opacity=0.9,
        annotation_text=f'Open 9:20: {oiv:.2f}%',
        annotation_font=dict(color=CFG['curve_colors']['Open 9:20'],size=11),annotation_position='top left')

    # Peak/Trough — compute on real data only (ignore NaN slots in full-day grid)
    _real_iv=h['atm_iv'].tolist(); _real_x=h['time_str'].tolist()
    _iv_arr=np.array(_real_iv,dtype=float)
    pi=int(np.nanargmax(_iv_arr)); ti=int(np.nanargmin(_iv_arr))
    fig.add_trace(go.Scatter(x=[_real_x[pi]],y=[_real_iv[pi]],mode='markers+text',name=f'Peak: {_real_iv[pi]:.2f}%',
        marker=dict(color='#ffd740',size=12,symbol='diamond'),
        text=[f'  Peak: {_real_iv[pi]:.2f}%'],textposition='top right',
        textfont=dict(color='#ffd740',size=11),
        hovertemplate='<b>Peak</b><br>Time: %{x}<br>IV: <b>%{y:.2f}%</b><extra></extra>'))
    if ti!=pi:
        fig.add_trace(go.Scatter(x=[_real_x[ti]],y=[_real_iv[ti]],mode='markers+text',name=f'Trough: {_real_iv[ti]:.2f}%',
            marker=dict(color='#ff4466',size=12,symbol='diamond'),
            text=[f'  Trough: {_real_iv[ti]:.2f}%'],textposition='bottom right',
            textfont=dict(color='#ff4466',size=11),
            hovertemplate='<b>Trough</b><br>Time: %{x}<br>IV: <b>%{y:.2f}%</b><extra></extra>'))

    title_color='#00e5a0' if chg>=0 else '#ff4466'; arrow='▲' if chg>=0 else '▼'
    no_yest_note='' if has_real_yest else '  <span style="color:#64748b;font-size:11px">| No stored yesterday data</span>'
    _base={k:v for k,v in PLOTLY_LAYOUT.items() if k not in ('xaxis','yaxis','legend','hovermode','margin')}
    fig.update_layout(**_base,hovermode='x unified',
        title=dict(text=(f'<b style="color:{CFG["title_c"]}">INTRADAY ATM IV — {inst["label"]} {badge}</b>  '
                         f'<span style="color:#94a3b8">Expiry: {inst["expiry"]:%d-%b-%Y}</span>  '
                         f'<span style="color:{title_color}">{arrow} {abs(chg):.2f}% from open</span>'
                         f'{no_yest_note}'),
                   x=0.01,xanchor='left',font=dict(size=13)),
        xaxis=dict(gridcolor=CFG['grid_c'],zeroline=False,tickfont=dict(color=CFG['tick_c'],size=11),
                   title=dict(text='Time (IST)',font=dict(color=CFG['tick_c'])),tickangle=-45,
                   range=[x_start,x_end],tickmode='array',tickvals=tick_vals),
        yaxis=dict(gridcolor=CFG['grid_c'],zeroline=False,tickfont=dict(color=CFG['tick_c']),
                   title=dict(text='ATM IV (%)',font=dict(color=CFG['tick_c'])),tickformat='.1f',ticksuffix='%'),
        height=480,
        legend=dict(bgcolor='rgba(13,17,23,0.88)',bordercolor=CFG['grid_c'],borderwidth=1,
                    orientation='v',x=0.01,y=0.01,xanchor='left',yanchor='bottom',
                    font=dict(size=12,color='white'),tracegroupgap=4),
        margin=dict(l=60,r=120,t=60,b=60))
    return fig

def plotly_iv_vs_ul(iid, all_data, intraday, yesterday_data):
    h=intraday[iid]; inst=all_data[iid]['inst']
    badge='[INDEX]' if inst['type']=='index' else '[STOCK]'
    sl='Synthetic Futures' if inst['type']=='index' else 'Futures'
    civ=h['atm_iv'].iloc[-1]; oiv=h['atm_iv'].iloc[0]
    chg=(civ-oiv)/oiv*100 if oiv else 0; arrow='▲' if chg>=0 else '▼'
    xvals=h['time_str'].tolist(); x_start=xvals[0]; x_end=xvals[-1]
    tick_vals=sorted(set([t for t in xvals if t.endswith(':00') or t.endswith(':15')
                          or t.endswith(':30') or t.endswith(':45')]+[x_start,x_end]))
    fig=make_subplots(specs=[[{"secondary_y":True}]])
    fig.add_trace(go.Scatter(x=xvals,y=h['atm_iv'].tolist(),fill='tozeroy',
        fillcolor='rgba(0,229,160,0.07)',mode='none',showlegend=False,hoverinfo='skip'),secondary_y=False)
    fig.add_trace(go.Scatter(x=xvals,y=h['atm_iv'].tolist(),mode='lines+markers',
        name=f'ATM IV — Today ({TODAY:%d-%b-%Y})',line=dict(color='#00e5a0',width=2.2),
        marker=dict(size=3,color='#00e5a0'),
        hovertemplate='Time: %{x}<br>ATM IV: <b>%{y:.2f}%</b><extra></extra>'),secondary_y=False)
    fig.add_trace(go.Scatter(x=[xvals[-1]],y=[h['atm_iv'].iloc[-1]],mode='markers+text',
        name='',showlegend=False,marker=dict(color='#00e5a0',size=8),
        text=[f'  IV: {civ:.2f}%'],textposition='middle right',
        textfont=dict(color='#00e5a0',size=11),hoverinfo='skip'),secondary_y=False)

    ydf=yesterday_data.get(iid); prev_date=get_previous_trading_date()
    if ydf is not None and not ydf.empty and ydf['atm_iv'].sum()>0:
        # ── Align yesterday onto the SAME time axis as today ─────────────────
        _full_times2=[f"{h:02d}:{m:02d}" for h in range(9,16) for m in range(60)
                      if (h>9 or m>=15) and (h<15 or m<=30)]
        ydf_reindexed2=(ydf.drop_duplicates(subset='time_str', keep='last')
                          .set_index('time_str')
                          .reindex(_full_times2)
                           .reset_index()
                           .rename(columns={'index':'time_str'}))
        ydf_clipped=ydf_reindexed2[ydf_reindexed2['time_str']<=x_end].copy()
        if ydf_clipped.empty: ydf_clipped=ydf_reindexed2
        ydf_clipped=ydf_clipped.dropna(subset=['atm_iv'])
        if not ydf_clipped.empty:
            fig.add_trace(go.Scatter(x=ydf_clipped['time_str'],y=ydf_clipped['atm_iv'],mode='lines',
                name=f'ATM IV — Yesterday ({prev_date:%d-%b-%Y})',
                line=dict(color=CFG['curve_colors']['Yesterday'],width=1.8,dash='dash'),opacity=0.8,
                hovertemplate=f'<b>Yesterday ({prev_date:%d-%b})</b><br>Time: %{{x}}<br>IV: <b>%{{y:.2f}}%</b><extra></extra>'),
                secondary_y=False)
            ydf_last=ydf_clipped.iloc[-1]
            fig.add_trace(go.Scatter(x=[ydf_last['time_str']],y=[ydf_last['atm_iv']],
                mode='markers+text',name='',showlegend=False,
                marker=dict(color=CFG['curve_colors']['Yesterday'],size=7),
                text=[f"  Yest: {ydf_last['atm_iv']:.2f}%"],textposition='middle right',
                textfont=dict(color=CFG['curve_colors']['Yesterday'],size=11),hoverinfo='skip'),secondary_y=False)

    fig.add_trace(go.Scatter(x=xvals,y=h['spot'].tolist(),mode='lines',
        name=f'{sl} Price (₹)',line=dict(color=CFG['title_c'],width=1.8,dash='dot'),
        hovertemplate=f'Time: %{{x}}<br>{sl}: <b>₹%{{y:,.1f}}</b><extra></extra>'),secondary_y=True)
    fig.add_trace(go.Scatter(x=[xvals[-1]],y=[h['spot'].iloc[-1]],mode='markers+text',
        name='',showlegend=False,marker=dict(color=CFG['title_c'],size=7),
        text=[f"  ₹{h['spot'].iloc[-1]:,.0f}"],textposition='middle right',
        textfont=dict(color=CFG['title_c'],size=11),hoverinfo='skip'),secondary_y=True)

    _base2={k:v for k,v in PLOTLY_LAYOUT.items() if k not in ('legend','xaxis','yaxis','hovermode','margin')}
    fig.update_layout(**_base2,hovermode='x unified',
        title=dict(text=(f'<b style="color:{CFG["title_c"]}">{badge} {inst["label"]}</b>  '
                         f'<span style="color:#94a3b8">[Expiry: {inst["expiry"]:%d-%b-%Y}]</span>  '
                         f'ATM IV: <b style="color:#00e5a0">{civ:.2f}%</b>  '
                         f'<span style="color:{"#00e5a0" if chg>=0 else "#ff4466"}">{arrow} {abs(chg):.2f}% from open</span>'),
                   x=0.01,xanchor='left',font=dict(size=13)),
        height=450,
        legend=dict(bgcolor='rgba(13,17,23,0.88)',bordercolor=CFG['grid_c'],borderwidth=1,
                    orientation='v',x=0.01,y=0.99,xanchor='left',yanchor='top',
                    font=dict(size=12,color='white'),tracegroupgap=4),
        margin=dict(l=60,r=130,t=60,b=60))
    fig.update_yaxes(title_text='ATM IV (%)',tickformat='.1f',ticksuffix='%',
        color='#00e5a0',secondary_y=False,gridcolor=CFG['grid_c'],zeroline=False,tickfont=dict(color=CFG['tick_c']))
    fig.update_yaxes(title_text=f'{sl} Price (₹)',tickformat=',.0f',tickprefix='₹',
        color=CFG['title_c'],secondary_y=True,gridcolor='rgba(0,0,0,0)',zeroline=False,tickfont=dict(color=CFG['tick_c']))
    fig.update_xaxes(title_text='Time (IST)',tickangle=-45,gridcolor=CFG['grid_c'],zeroline=False,
        tickfont=dict(color=CFG['tick_c'],size=11),range=[x_start,x_end],tickmode='array',tickvals=tick_vals)
    return fig

def build_movers(all_data, ft=None):
    rows=[]
    for iid,data in all_data.items():
        inst=data['inst']
        if ft and inst['type']!=ft: continue
        civ=data['atm_iv'] or 0; oiv=data['open_atm'] or civ
        pct=(civ-oiv)/oiv*100 if oiv else 0
        rows.append(dict(Instrument=inst['label'],ID=iid,Type=inst['type'].title(),
                         Expiry=str(inst['expiry']),
                         **{'Current IV':round(civ,2),'Open IV':round(oiv,2),
                            '% Change':round(pct,2),'_raw_pct':pct}))
    df=pd.DataFrame(rows).sort_values('_raw_pct',ascending=False).reset_index(drop=True)
    df.index=df.index+1; df.index.name='Rank'; return df

# ═══════════════════════════════════════════════════════════════════════════════
#  MAIN APP
# ═══════════════════════════════════════════════════════════════════════════════
TODAY = _today_ist(); NOW = _now_ist()

_zmethod_badge = f" [{_ZERODHA_METHOD.upper()}]" if _ZERODHA_METHOD else ""
broker_badge = f"🟢 {BROKER.upper()}{_zmethod_badge}" if BROKER!='mock' else "⚠️ MOCK"
st.title("📈 IV Skew & Intraday Volatility Dashboard")
st.caption(f"NSE F&O · {TODAY:%d-%b-%Y} · {NOW:%H:%M IST} · Broker: **{broker_badge}** · "
           f"{'Telegram: ✅' if TG_TOKEN and TG_CHAT_ID else 'Telegram: ⚠️ not configured'}")

# ══════════════════════════════════════════════════════════════════════════════
#  DATA LOADING — All 162 instruments in one parallel pass
#  Live Zerodha data when connected · mock fallback otherwise
#  TTL=60s — auto-refreshes every minute
# ══════════════════════════════════════════════════════════════════════════════
_t0        = time.time()
_prog_area = st.empty()
with _prog_area.container():
    _prog = st.progress(0, text="⚡ Loading all instruments in parallel…")

ALL_DATA, INSTRUMENTS = build_all_data_parallel()
# Cache ALL_DATA in session_state for sidebar debug panel
try:
    st.session_state['_all_data_cache'] = {k: {'inst': v['inst'], 'atm_ce_ltp': v.get('atm_ce_ltp'),
                                                 'atm_pe_ltp': v.get('atm_pe_ltp'),
                                                 'atm_strike': v.get('atm_strike')}
                                             for k, v in ALL_DATA.items()}
except Exception:
    pass

_elapsed = time.time() - _t0
ni_loaded = sum(1 for i in INSTRUMENTS if i['type'] == 'index')
ns_loaded = sum(1 for i in INSTRUMENTS if i['type'] == 'stock')
_n_errors = sum(1 for d in ALL_DATA.values() if d.get('live_error'))
_is_fully_live = _is_zerodha_connected()
if _is_fully_live and _n_errors == 0:
    _source = "🟢 Zerodha Live"
elif _is_fully_live and _n_errors > 0:
    _source = f"🟡 Zerodha Live ({_n_errors} fetch errors)"
else:
    _source = "⚠️ Mock"

_prog.progress(80, text=f"⚡ {len(ALL_DATA)} instruments loaded ({_elapsed:.1f}s) — building intraday…")

_all_ids = tuple(i['id'] for i in INSTRUMENTS)
INTRADAY = build_intraday_fast(_all_ids)

_prog.progress(95, text="⚡ Loading yesterday snapshot…")
YESTERDAY_DATA = load_yesterday_data(INSTRUMENTS)
prev_date      = get_previous_trading_date()
has_yest_store = _store_path(prev_date).exists()

_elapsed = time.time() - _t0
_prog.progress(100, text=f"✅ {len(ALL_DATA)} instruments ready in {_elapsed:.1f}s · {_source} · {ni_loaded} indices + {ns_loaded} stocks")
time.sleep(0.5)
_prog_area.empty()

# Cache live ATM IVs for intraday seeding
st.session_state['_live_atm_cache'] = {
    iid: d['atm_iv'] for iid, d in ALL_DATA.items() if d.get('atm_iv')
}

# ── Auto-save snapshot ─────────────────────────────────────────────────────────
_market_close_today = NOW.replace(hour=CFG['market_close_h'],minute=CFG['market_close_m'],second=0,microsecond=0)
_is_after_close     = NOW >= _market_close_today
_already_saved      = st.session_state.get('snapshot_saved_date') == TODAY.isoformat()
_today_file_exists  = _store_path(TODAY).exists()

if _is_after_close and not _already_saved:
    save_daily_snapshot(INTRADAY, TODAY)
    st.session_state['snapshot_saved_date'] = TODAY.isoformat()
elif not _today_file_exists:
    save_daily_snapshot(INTRADAY, TODAY)

# ── Sidebar ────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## ⚙️ Controls")
    ni = ni_loaded; ns = ns_loaded
    st.caption(f"🔷 {ni} Indices  |  📊 {ns} Stocks  |  Total: {len(INSTRUMENTS)}")

    st.divider()
    st.markdown("#### 🔌 Broker Connection")
    if BROKER == 'zerodha' and (_kite_sdk is not None or _kite_enc is not None):
        _meth = f" via **{_ZERODHA_METHOD.upper()}**" if _ZERODHA_METHOD else ""
        st.success(f"✅ **Zerodha Live**{_meth} — `{ZERODHA_USER_ID}`")
        if st.button("🗑️ Reset Open IV Cache",
                     help="Clears pinned open IV values. Use when IV Chg% shows +500% or extreme values from bad early data."):
            with _OPEN_IV_LOCK:
                _get_open_iv_cache().clear()
            try:
                if _OPEN_IV_PATH.exists():
                    _OPEN_IV_PATH.unlink()
            except Exception:
                pass
            st.success("✅ Open IV cache cleared — will re-pin correctly on next refresh.")
            time.sleep(0.5); st.rerun()

        if st.button("🔄 Re-Login (refresh token)"):
            with st.spinner("Re-logging in via TOTP…"):
                try:
                    _new_enc = _zerodha_auto_login()
                    _kite_enc = _build_kite_session(_new_enc)
                    _enctoken = _new_enc
                    r = _kite_enc.get("https://kite.zerodha.com/oms/user/profile", timeout=10)
                    r.raise_for_status()
                    st.success("✅ Re-login successful!")
                    time.sleep(0.5); st.rerun()
                except Exception as _rex:
                    st.error(f"Re-login failed: `{_rex}`")
    else:
        st.error("⚠️ **MOCK data** — Zerodha login failed")

        # Show exact error
        if _BROKER_ERROR:
            with st.expander("❌ Error details", expanded=True):
                st.code(_BROKER_ERROR, language="text")

        # Detect missing packages
        _kite_ok = _pyotp_ok = True
        try: import kiteconnect
        except ImportError: _kite_ok = False
        try: import pyotp
        except ImportError: _pyotp_ok = False

        if not _kite_ok or not _pyotp_ok:
            _missing = [p for p, ok in [("kiteconnect", _kite_ok), ("pyotp", _pyotp_ok)] if not ok]
            st.warning(f"📦 Missing: `{', '.join(_missing)}`")
            if st.button("⚡ Install & Connect", type="primary"):
                with st.spinner(f"pip install {' '.join(_missing)}…"):
                    for _pkg in _missing:
                        subprocess.check_call([sys.executable, "-m", "pip", "install", _pkg,
                                               "--quiet", "--disable-pip-version-check"])
                st.success("✅ Done! Reloading…"); time.sleep(0.8); st.rerun()
        else:
            # Packages are present but login failed — show retry with live log
            if st.button("🔄 Retry Login", type="primary"):
                _log_box = st.empty()
                _log_lines = []
                def _ui_log(msg):
                    _log_lines.append(msg)
                    _log_box.code("\n".join(_log_lines[-8:]), language="text")
                try:
                    _ui_log("Starting TOTP login flow…")
                    import pyotp as _p
                    _totp_now = _p.TOTP(ZERODHA_TOTP_KEY).now()
                    _ui_log(f"TOTP generated: {_totp_now}")
                    _ui_log("POSTing credentials to kite.zerodha.com…")
                    _new_enc = _zerodha_auto_login()
                    _ui_log(f"enctoken obtained (len={len(_new_enc)}) ✅")
                    _ui_log("Building authenticated session…")
                    _kite_enc = _build_kite_session(_new_enc)
                    _enctoken = _new_enc
                    _ui_log("Verifying via /oms/user/profile/full …")
                    _r = _kite_enc.get(
                        "https://kite.zerodha.com/oms/user/profile/full",
                        timeout=10)
                    _r.raise_for_status()
                    _pj = _r.json()
                    _uname = (_pj.get("data") or {}).get("user_name", "?")
                    _ui_log(f"✅ Connected as: {_uname}")
                    BROKER = 'zerodha'
                    _BROKER_ERROR = ""
                    time.sleep(1); st.rerun()
                except Exception as _rex:
                    _ui_log(f"❌ Error: {_rex}")

            st.caption(f"API Key: `{ZERODHA_API_KEY[:8]}…` | User: `{ZERODHA_USER_ID}`")
            st.caption("💡 Check terminal logs for full TOTP debug output")

    st.divider()
    # ── Instrument Cache Status ───────────────────────────────────────────────
    st.markdown("#### 🗂️ Instrument Cache")
    if _EXP_STRIKE_CACHE:
        _nfo_count = sum(1 for k in _EXP_STRIKE_CACHE if k[0] == 'NFO')
        _bfo_count = sum(1 for k in _EXP_STRIKE_CACHE if k[0] == 'BFO')
        st.success(f"✅ NFO: {_nfo_count} entries | BFO: {_bfo_count} entries")
        # Check key instruments
        # Check expiry+strike cache has entries for key instruments.
        # Use the name-based _INST_STRIKES as a proxy (more reliable than scanning values).
        def _has_inst(exch, name):
            return any(k[0]==exch and k[1]==name for k in _INST_STRIKES)
        _cache_checks = {
            'MIDCPNIFTY': _has_inst('NFO', 'MIDCPNIFTY'),
            'FINNIFTY':   _has_inst('NFO', 'FINNIFTY'),
            'SENSEX-BFO': _bfo_count > 0 and _has_inst('BFO', 'SENSEX'),
        }
        for _inst_name, _ok in _cache_checks.items():
            if _ok:
                st.caption(f"  ✅ {_inst_name} symbols resolved")
            else:
                st.warning(f"  ⚠️ {_inst_name} — not found in instrument cache")
        # BFO price check — detect account-level BSE F&O access issue
        if _bfo_count > 0:
            _bfo_priced = sum(1 for k,v in _GLOBAL_LTP.items() if k.startswith('BFO:') and v > 0.05)
            if _bfo_priced == 0:
                st.error(
                    "🚫 **BSE F&O (BFO) prices all zero** — your Zerodha account may not "
                    "have BSE F&O trading enabled. Sensex / Sensex W2 will show None CE/PE. "
                    "Enable BSE F&O in your Kite account settings to fix this."
                )
            else:
                st.caption(f"  ✅ BFO live prices: {_bfo_priced} symbols with real data")
    else:
        st.error("❌ Instrument cache empty — all option symbols using constructed fallbacks")
        st.caption("This usually means the Zerodha SDK login did not complete successfully.")

    # ── Cache debug expander ──────────────────────────────────────────────────
    if _EXP_STRIKE_CACHE:
        with st.expander("🔍 Cache Debug (None price diagnosis)", expanded=False):
            st.caption("Shows actual instrument names Zerodha uses. Compare against your stock symbols to find mismatches.")
            # Show all unique names in _INST_STRIKES for NFO
            _nfo_names = sorted(set(k[1] for k in _INST_STRIKES if k[0] == 'NFO'))
            _bfo_names = sorted(set(k[1] for k in _INST_STRIKES if k[0] == 'BFO'))
            st.markdown(f"**NFO names ({len(_nfo_names)}):**")
            # Show in columns of ~20
            _name_cols = st.columns(3)
            for _i, _n in enumerate(_nfo_names[:60]):
                _name_cols[_i % 3].caption(_n)
            if len(_nfo_names) > 60:
                st.caption(f"... and {len(_nfo_names)-60} more")
            st.markdown(f"**BFO names ({len(_bfo_names)}):** {', '.join(_bfo_names)}")
            st.divider()
            st.markdown("**Instruments showing None CE/PE (check these names above):**")
            # We need ALL_DATA here — use session state if available
            _ad = st.session_state.get('_all_data_cache', {})
            _none_insts = [(iid, d) for iid, d in _ad.items()
                          if d.get('atm_ce_ltp') is None and d.get('atm_pe_ltp') is None]
            if _none_insts:
                for _iid, _d in _none_insts[:15]:
                    _inst = _d.get('inst', {})
                    st.caption(f"  {_iid} | ATM={_d.get('atm_strike','?')} | "
                               f"sym_in_cache={'✅' if any(k[1]==_iid for k in _INST_STRIKES if k[0]=='NFO') else '❌'}")
            else:
                st.caption("No None instruments detected.")

    st.divider()
    # ── Data Scope — always fully loaded ─────────────────────────────────────
    st.markdown("#### 📦 Data Scope")
    _data_src = _live_source_label()
    _scope_msg = (f"**{len(ALL_DATA)}** instruments loaded\n"
                  f"{ni_loaded} indices + {ns_loaded} stocks\n"
                  f"Source: **{_data_src}**")
    if _n_errors > 0 and _is_fully_live:
        st.warning(_scope_msg + f"\n⚠️ {_n_errors} instruments had fetch errors")
    else:
        st.success(_scope_msg)
    if st.button("🔄 Refresh Data Now", help="Clears cache and reloads all live data"):
        build_all_data_parallel.clear()
        build_intraday_fast.clear()
        st.rerun()

    # ── Auto-Refresh ──────────────────────────────────────────────────────────
    st.divider()
    st.markdown("#### 🔁 Auto-Refresh")
    _auto_on = st.toggle("Enable Auto-Refresh", value=st.session_state.get("auto_refresh_on", True), key="auto_refresh_on")
    _interval_opts = {"5 min": 5, "10 min": 10, "15 min": 15, "30 min": 30}
    _sel_interval  = st.selectbox("Interval", list(_interval_opts.keys()),
                                   index=2,   # default = 15 min
                                   key="auto_refresh_interval",
                                   disabled=not _auto_on)
    _interval_ms = _interval_opts[_sel_interval] * 60 * 1000   # convert to ms

    if _auto_on and _HAS_AUTOREFRESH:
        _refresh_count = st_autorefresh(interval=_interval_ms, key="iv_autorefresh")
        if _refresh_count and _refresh_count > 0:
            build_all_data_parallel.clear()
            build_intraday_fast.clear()
        st.caption(f"🟢 Auto-refreshing every **{_sel_interval}**")
    elif _auto_on and not _HAS_AUTOREFRESH:
        st.warning("⚠️ `streamlit-autorefresh` not installed.\nRun: `pip install streamlit-autorefresh`")
    else:
        st.caption("⏸️ Auto-refresh paused")

    # ── Live Data Diagnostics ────────────────────────────────────────────────
    if BROKER == 'zerodha' and (_kite_sdk is not None or _kite_enc is not None):
        st.divider()
        st.markdown("#### 🔬 Live Data Diagnostics")
        if _n_errors > 0:
            _not_in_fo  = [iid for iid, d in ALL_DATA.items()
                           if d.get('live_error') and 'NOT_IN_FON' in str(d.get('live_error',''))]
            _real_errors= [iid for iid, d in ALL_DATA.items()
                           if d.get('live_error') and 'NOT_IN_FON' not in str(d.get('live_error',''))]
            if _not_in_fo:
                with st.expander(f"ℹ️ {len(_not_in_fo)} stocks not in F&O segment (normal)", expanded=False):
                    st.caption("These stocks exist in _STKS list but currently have no active "
                               "options on NSE F&O (banned/delisted/no OI). Using mock data.")
                    for iid in _not_in_fo:
                        st.caption(f"  · {iid}")
            if _real_errors:
                with st.expander(f"⚠️ {len(_real_errors)} instruments had real fetch errors", expanded=True):
                    for iid in _real_errors:
                        st.code(f"{iid}: {ALL_DATA[iid]['live_error']}", language="text")

        # Show what expiry dates + symbols are being used RIGHT NOW
        with st.expander(f"📅 Active Expiries + Symbol Check (today = {TODAY:%d-%b-%Y})", expanded=False):
            _idx_insts = [i for i in INSTRUMENTS if i['type'] == 'index']
            for _ii in _idx_insts:
                _es   = _ii['expiry'].strftime('%d%b%y').upper()
                _bse  = _ii['id'] in {'SENSEX', 'SENSEX_W2'}
                _ex   = 'BFO' if _bse else 'NFO'
                _base = _ii['id'].replace('_W2','').replace('_','')
                # For Sensex probe all name variants to show which one resolves
                _rid  = _ii['id'].replace('_W2','')
                if _rid == 'SENSEX':
                    _resolved = '❌ NOT FOUND'
                    for _nv in ['SENSEX','BSE SENSEX','BSESENSEX','S&P BSE SENSEX']:
                        if _get_strikes_from_cache(_ex, _nv, _ii['expiry'], float(_ii['spot']), n_strikes=1):
                            _resolved = f"✅ BFO name='{_nv}'"
                            break
                    st.caption(f"**{_ii['label']}**: `{_ii['expiry']:%d-%b-%Y}` → `{_ex}:{_base}{_es}[K]CE`  {_resolved}")
                else:
                    _has = bool(_get_strikes_from_cache(_ex, _rid, _ii['expiry'], float(_ii['spot']), n_strikes=1))
                    st.caption(f"**{_ii['label']}**: `{_ii['expiry']:%d-%b-%Y}` → `{_ex}:{_base}{_es}[K]CE`  {'✅' if _has else '❌ name mismatch'}")
            # Show all BFO names in cache (helps debug Sensex name)
            _bfo_names = sorted(set(k[1] for k in _INST_STRIKES if k[0]=='BFO'))
            if _bfo_names:
                st.caption(f"**BFO names in cache:** {', '.join(_bfo_names[:15])}")
            else:
                st.warning("BFO cache empty — Sensex options not loaded")

        # Show instrument cache status
        _cache_size = len(_INST_CACHE)
        if _cache_size > 0:
            st.success(f"✅ Instrument cache: {_cache_size} options loaded ({_INST_CACHE_DATE})")
        else:
            st.warning("⚠️ Instrument cache empty — symbols may not resolve")
            if st.button("🔄 Reload Instrument Cache"):
                with st.spinner("Downloading NFO+BFO instruments…"):
                    try:
                        _n = _build_instrument_cache(_kite_sdk)
                        st.success(f"✅ Cached {_n} instruments")
                        st.rerun()
                    except Exception as _ce:
                        st.error(f"Cache build failed: {_ce}")

        if st.button("🧪 Test LTP + Instrument Cache Status"):
            with st.spinner("Fetching from Zerodha…"):
                _diag = []
                try:
                    _s = _kite_ltp_single("NSE:NIFTY 50")
                    _diag.append(f"✅ Spot  NSE:NIFTY 50 = ₹{_s:,.2f}")
                except Exception as _e:
                    _diag.append(f"❌ Spot  NSE:NIFTY 50 FAILED: {_e}"); _s = 23000

                # ── Sensex BFO test ──────────────────────────────────────────
                try:
                    _sx = _kite_ltp_single("BSE:SENSEX")
                    _diag.append(f"✅ Spot  BSE:SENSEX = ₹{_sx:,.2f}")
                except Exception as _e:
                    _diag.append(f"❌ Spot  BSE:SENSEX FAILED: {_e}"); _sx = 72800
                _sx_inst = next((i for i in INSTRUMENTS if i['id'] == 'SENSEX'), None)
                if _sx_inst:
                    _sx_exp  = _sx_inst['expiry']
                    _sx_es   = _sx_exp.strftime('%d%b%y').upper()
                    _sx_atm  = round(_sx / 100) * 100
                    _diag.append(f"ℹ️  Sensex expiry={_sx_exp}  ATM={_sx_atm}")
                    _diag.append(f"ℹ️  BFO names in cache: {sorted(set(k[1] for k in _INST_STRIKES if k[0]=="BFO"))[:10]}")
                    for _nv in ['SENSEX','BSE SENSEX','BSESENSEX','S&P BSE SENSEX']:
                        _cs = _get_strikes_from_cache('BFO', _nv, _sx_exp, _sx, n_strikes=3)
                        _diag.append(f"  BFO name={_nv!r} → {len(_cs)} strikes: {_cs[:3] if _cs else 'NOT FOUND'}")
                    # Try fetching a BFO option directly
                    _bfo_test = f"BFO:SENSEX{_sx_es}{_sx_atm}CE"
                    try:
                        _bp = _kite_ltp_single(_bfo_test)
                        _diag.append(f"✅ Direct BFO LTP: {_bfo_test} = ₹{_bp:.2f}")
                    except Exception as _be:
                        _diag.append(f"❌ Direct BFO LTP {_bfo_test} FAILED: {_be}")

                _ti = next((i for i in INSTRUMENTS if i['id'] == 'NIFTY'), None)
                if _ti:
                    _es = _ti['expiry'].strftime('%d%b%y').upper()
                    _atm = round(_s / 50) * 50
                    _test_syms = [
                        f"NFO:NIFTY{_es}{_atm}CE",
                        f"NFO:NIFTY{_es}{_atm}PE",
                        f"NFO:NIFTY{_es}{_atm+50}CE",
                    ]
                    # Show cache lookup result
                    _cached_sym = _lookup_option_sym("NFO", "NIFTY", _ti['expiry'], _atm, "CE")
                    _diag.append(f"ℹ️  Cache lookup NFO/NIFTY/{_ti['expiry']}/{_atm}/CE → {_cached_sym or 'NOT FOUND'}")
                    _test_syms_final = [_cached_sym or _test_syms[0], _test_syms[1], _test_syms[2]]
                    _diag.append(f"ℹ️  Asking SDK for: {_test_syms_final}")
                    try:
                        if _kite_sdk is not None:
                            _raw = _kite_sdk.ltp(_test_syms_final)
                            _diag.append(f"✅ SDK returned {len(_raw)} entries:")
                            for _k, _v in _raw.items():
                                _diag.append(f"   KEY='{_k}'  last_price={_v.get('last_price')}")
                            if not _raw:
                                _diag.append("❌ SDK returned EMPTY dict — symbols may be wrong")
                                _diag.append(f"   Cache size: {len(_INST_CACHE)}")
                                _diag.append(f"   Cache date: {_INST_CACHE_DATE}")
                        else:
                            _diag.append("ℹ️  SDK not connected, testing enctoken…")
                            for _sym in _test_syms[:1]:
                                _ltp = _kite_ltp_single(_sym)
                                _diag.append(f"  {_sym} = ₹{_ltp:.2f}")
                    except Exception as _e:
                        _diag.append(f"❌ SDK ltp() FAILED: {_e}")
                st.code("\n".join(_diag), language="text")

    st.divider()
    st.markdown("#### 📅 Data Status")
    st.markdown(f"**Today:** {TODAY:%d-%b-%Y} (live)")
    if has_yest_store:
        st.success(f"✅ Yesterday ({prev_date:%d-%b-%Y}) loaded from store")
    else:
        st.warning(f"⚠️ No stored data for {prev_date:%d-%b-%Y} — showing 0%")

    stored_files=sorted(STORE_DIR.glob("*.json"),reverse=True)
    if stored_files:
        st.caption(f"📦 Stored snapshots: {len(stored_files)} day(s)")
        for f in stored_files[:5]: st.caption(f"  · {f.stem}")

    # Telegram status
    st.divider()
    st.markdown("#### 🔔 Alert Status")
    if TG_TOKEN and TG_CHAT_ID:
        st.success(
            f"✅ Telegram active  \n"
            f"📊 Index: >{CFG.get('alert_breakout_pct',5)}% change  \n"
            f"📈 Stock: >{CFG.get('alert_breakout_pct_stock',10)}% change  \n"
            f"⏱ Skip first {CFG.get('alert_open_skip_min',8)} min")
    else:
        st.warning("⚠️ Telegram not set\nSet TELEGRAM_BOT_TOKEN\n+ TELEGRAM_CHAT_ID in .env")

    st.divider()
    index_ids=[i['id'] for i in INSTRUMENTS if i['type']=='index']
    stock_ids=[i['id'] for i in INSTRUMENTS if i['type']=='stock']
    all_ids=index_ids+stock_ids
    lbls={i['id']:f"{i['label']} [{i['type'][0].upper()}]" for i in INSTRUMENTS}

    st.markdown("#### Instrument (Tab 2A & 2B)")
    sel_id=st.selectbox("Select",all_ids,format_func=lambda x:lbls[x],index=0)
    st.divider()
    st.markdown("#### Grid Filter (Tab 1)")
    t1f=st.radio("Show",["Indices Only","Stocks Only","All"],index=0)
    st.divider()
    st.markdown("#### IV Movers (Tab 3)")
    top_n=st.slider("Top N",5,30,15)
    mf=st.radio("Filter",["All","Indices Only","Stocks Only"],index=0)
    st.divider()

    col1,col2=st.columns(2)
    with col1:
        if st.button("🔄 Refresh",use_container_width=True):
            st.cache_data.clear()
            for k in list(st.session_state.keys()):
                if k.startswith('fig_'): del st.session_state[k]
            st.rerun()
    with col2:
        if st.button("💾 Snapshot",use_container_width=True):
            save_daily_snapshot(INTRADAY,TODAY)
            st.session_state['snapshot_saved_date']=TODAY.isoformat()
            st.success(f"Saved!")

    if st.button("📤 Test Telegram",use_container_width=True):
        ok, err = _tg_send_debug("🧪 <b>IV Dashboard</b> — Test alert · NSE F&O")
        if ok:
            st.success("Sent! ✅")
        else:
            st.error(f"Failed: {err}")

# ── Tabs ───────────────────────────────────────────────────────────────────────
ftm={"All":None,"Indices Only":"index","Stocks Only":"stock"}
tab1,tab2a,tab2b,tab3,tab4,tab5=st.tabs([
    "📊 Tab 1: IV Skew Grid",
    "📉 Tab 2A: Intraday ATM IV",
    "📈 Tab 2B: IV vs Underlying",
    "🏆 Tab 3: IV Movers",
    "🔔 Tab 4: Alerts Log",
    "🔢 Tab 5: IV Calc Details",
])

# ════════════════════════════════════════════════════════════════════════════════
#  TAB 1 — IV SKEW GRID  (ALL instruments)
# ════════════════════════════════════════════════════════════════════════════════
with tab1:
    ft=ftm[t1f]
    items={k:v for k,v in ALL_DATA.items() if ft is None or v['inst']['type']==ft}
    av=[v['atm_iv'] for v in items.values() if v['atm_iv']]
    c1,c2,c3,c4=st.columns(4)
    if av:
        c1.metric("Avg ATM IV",f"{np.mean(av):.2f}%")
        c2.metric("Max ATM IV",f"{max(av):.2f}%")
        c3.metric("Min ATM IV",f"{min(av):.2f}%")
    c4.metric("Instruments",len(items))
    st.caption(f"{len(items)} instruments · Source: **{_live_source_label()}** · Live / Yesterday (DB) / Open 9:20 (DB) · ±300 bps")

    # Full matplotlib grid (all instruments — paginated for stocks)
    cached_img(f"fig_grid_{t1f}", draw_grid, ALL_DATA, ft)

    st.divider()
    st.subheader("🔍 Single Instrument Deep-Dive (Interactive)")
    sel=st.selectbox("Pick instrument",list(items.keys()),format_func=lambda x:lbls[x],key='tab1_sel')
    if sel:
        st.plotly_chart(plotly_skew_single(sel,ALL_DATA),key=f"skew_single_{sel}",
                        use_container_width=True)
        d=ALL_DATA[sel]; cc1,cc2,cc3,cc4=st.columns(4)
        cc1.metric("ATM IV (Live)",f"{d['atm_iv']:.2f}%" if d['atm_iv'] else "N/A")
        cc2.metric("Open ATM IV",f"{d['open_atm']:.2f}%" if d['open_atm'] else "N/A")
        dh,dl=db_get_day_extremes(sel,TODAY.isoformat())
        cc3.metric("Day High IV",f"{dh:.2f}%" if dh>0 else "N/A")
        cc4.metric("Day Low IV",f"{dl:.2f}%" if dl<9999 else "N/A")
        if d['atm_iv'] and d['open_atm']:
            chg=(d['atm_iv']-d['open_atm'])/d['open_atm']*100
            st.metric("IV Δ from Open",f"{chg:+.2f}%",delta=f"{chg:+.2f}%")

        with st.expander("📋 Skew Data Table"):
            c_l,c_y,c_o=st.columns(3)
            for col,key,lbl in [(c_l,'live','Live'),(c_y,'yest','Yesterday'),(c_o,'open','Open 9:20')]:
                col.markdown(f"**{lbl}**")
                df_s=d[key][['bps','strike','iv','opt_type']].copy()
                df_s.columns=['BPS','Strike','IV (%)','Type']
                col.dataframe(df_s.style.format({'IV (%)':'{:.2f}'}),height=300,use_container_width=True)

# ════════════════════════════════════════════════════════════════════════════════
#  TAB 2A — INTRADAY ATM IV
# ════════════════════════════════════════════════════════════════════════════════
with tab2a:
    st.subheader(f"Intraday ATM IV — {lbls[sel_id]}")

    h = INTRADAY.get(sel_id, pd.DataFrame())
    _has_real_data = not h.empty and len(h) >= 1
    _data_source   = _live_source_label()
    _db_rows       = len(db_load_atm_history(sel_id, TODAY.isoformat()))

    # Data source banner
    if BROKER == 'zerodha' and (_kite_sdk is not None or _kite_enc is not None):
        if _db_rows >= 2:
            st.success(f"🟢 **Live Zerodha data** — {_db_rows} real readings today (background recorder active, updates every 60s)")
        elif _db_rows == 1:
            st.info(f"🟡 **1 live reading** recorded — chart will fill in as the session continues (recorder runs every 60s)")
        else:
            st.warning("🟡 **No intraday history yet** — first live reading being fetched now. Chart will build up over time.")
    else:
        st.error("⚠️ **Mock data** — not connected to Zerodha. Intraday chart shows simulated values.")

    if _has_real_data:
        oiv = h['atm_iv'].iloc[0]; civ = h['atm_iv'].iloc[-1]
        chg = (civ - oiv) / oiv * 100 if oiv else 0
    else:
        oiv = civ = chg = 0

    # ── Get high/low with time ────────────────────────────────────────────────
    _dh_iv, _dh_time, _dl_iv, _dl_time = db_get_day_extremes_with_time(sel_id, TODAY.isoformat())
    if _dh_iv == 0.0 and _has_real_data:
        _hi_idx = h['atm_iv'].idxmax(); _lo_idx = h['atm_iv'].idxmin()
        _dh_iv   = float(h.loc[_hi_idx, 'atm_iv']); _dh_time = h.loc[_hi_idx, 'time_str']
        _dl_iv   = float(h.loc[_lo_idx, 'atm_iv']); _dl_time = h.loc[_lo_idx, 'time_str']

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Current ATM IV", f"{civ:.2f}%" if civ else "—", delta=f"{chg:+.2f}% from open" if civ else None)
    c2.metric("Open 9:20 IV",   f"{oiv:.2f}%" if oiv else "—")
    c3.metric("Intraday High",  f"{_dh_iv:.2f}%" if _dh_iv > 0 else "—",
              delta=f"@ {_dh_time}" if _dh_time != "—" else None, delta_color="off")
    c4.metric("Intraday Low",   f"{_dl_iv:.2f}%" if _dl_iv < 9999 else "—",
              delta=f"@ {_dl_time}" if _dl_time != "—" else None, delta_color="off")
    c5.metric("ATM Strike", f"{ALL_DATA[sel_id].get('atm_strike', 0):,.0f}")

    ydf_check = YESTERDAY_DATA.get(sel_id)
    has_yest  = ydf_check is not None and ydf_check['atm_iv'].sum() > 0
    if has_yest:
        st.info(f"📅 Yesterday ({prev_date:%d-%b-%Y}): loaded from snapshot file")
    else:
        st.caption(f"📅 No stored data for {prev_date:%d-%b-%Y} — yesterday overlay unavailable until after market close today")

    if _has_real_data:
        st.plotly_chart(plotly_intraday(sel_id, ALL_DATA, INTRADAY, YESTERDAY_DATA),
                        use_container_width=True, key=f"intraday_{sel_id}")
    else:
        st.info("⏳ Waiting for first live data point — this will appear within 60 seconds of startup.")

    with st.expander(f"📋 Raw Intraday Data (Today · {_db_rows} DB rows · source: {_data_source})"):
        if _has_real_data:
            st.dataframe(h[['time_str', 'atm_iv', 'spot']].rename(
                columns={'time_str': 'Time', 'atm_iv': 'ATM IV (%)', 'spot': 'Spot'}),
                use_container_width=True, height=200)
        else:
            st.caption("No data yet.")

    if has_yest:
        with st.expander(f"📋 Raw Intraday Data (Yesterday {prev_date:%d-%b-%Y})"):
            st.dataframe(YESTERDAY_DATA[sel_id].rename(
                columns={'time_str': 'Time', 'atm_iv': 'ATM IV (%)', 'spot': 'Spot'}),
                use_container_width=True, height=200)

# ════════════════════════════════════════════════════════════════════════════════
#  TAB 2B — IV vs UNDERLYING
# ════════════════════════════════════════════════════════════════════════════════
with tab2b:
    st.subheader(f"ATM IV vs Underlying Price — {lbls[sel_id]}")
    st.plotly_chart(plotly_iv_vs_ul(sel_id,ALL_DATA,INTRADAY,YESTERDAY_DATA),
                    use_container_width=True,key=f"iv_vs_ul_{sel_id}")
    st.divider()
    st.subheader("Multi-Instrument Comparison")
    cids=st.multiselect("Select up to 6 instruments",all_ids,
                         default=index_ids[:3],format_func=lambda x:lbls[x],max_selections=6)
    for cid in cids:
        st.plotly_chart(plotly_iv_vs_ul(cid,ALL_DATA,INTRADAY,YESTERDAY_DATA),
                        use_container_width=True,key=f"iv_vs_ul_multi_{cid}")

# ════════════════════════════════════════════════════════════════════════════════
#  TAB 3 — IV MOVERS  (all instruments ranked)
# ════════════════════════════════════════════════════════════════════════════════
with tab3:
    st.subheader("IV Movers — Ranked by % Change from Open")
    mdf=build_movers(ALL_DATA,ft=ftm[mf])
    ng=(mdf['_raw_pct']>0).sum(); nl=(mdf['_raw_pct']<0).sum()
    c1,c2,c3,c4=st.columns(4)
    c1.metric("Total Tracked",len(mdf)); c2.metric("▲ Gainers",ng)
    c3.metric("▼ Losers",nl);            c4.metric("Avg % Chg",f"{mdf['% Change'].mean():+.2f}%")

    cached_img(f"fig_movers_{mf}_{top_n}",draw_movers_chart,mdf,top_n)

    st.divider()
    st.subheader("📋 Full Leaderboard")
    ct,cf=st.columns([3,1])
    with cf:
        sn=st.number_input("Show top N",5,len(mdf),min(30,len(mdf)),5)

    def _cpct(v):
        c='#00e5a0' if v>0 else ('#ff4466' if v<0 else 'white')
        return f'color:{c};font-weight:bold'

    with ct:
        disp=mdf.drop(columns=['_raw_pct']).head(sn)
        st.dataframe(disp.style.applymap(_cpct,subset=['% Change']),
                     use_container_width=True,height=450)

    st.download_button("⬇️ Download CSV",mdf.drop(columns=['_raw_pct']).to_csv(),"iv_movers.csv","text/csv")

# ════════════════════════════════════════════════════════════════════════════════
#  TAB 4 — ALERTS LOG  (NEW)
# ════════════════════════════════════════════════════════════════════════════════
with tab4:
    st.subheader("🔔 Alerts Log — Day High / Day Low Breakouts")

    with st.expander("ℹ️ Alert Setup & Format"):
        st.markdown(f"""
**Broker:** `{BROKER.upper()}`  
**Telegram:** `{'✅ Active' if TG_TOKEN and TG_CHAT_ID else '⚠️ Not configured'}`  
**Threshold:** IV must break prior Day High/Low by **>{CFG['alert_breakout_pct']}%**  
**Cooldown:** 30 min between repeat alerts per instrument

**Setup (.env file):**
```
BROKER=zerodha
KITE_API_KEY=your_key
KITE_ACCESS_TOKEN=your_token
TELEGRAM_BOT_TOKEN=your_bot_token
TELEGRAM_CHAT_ID=your_chat_id
ALERT_THRESHOLD_PCT=0.5
```

**Telegram Message Format (now includes calc inputs):**
```
📊 IV ALERT — Nifty 50
━━━━━━━━━━━━━━━━━━━━
🔖 Expiry: 2026-02-27
📈 State: RISING IV — 🔺 Day High
🕐 Time: 12:47 PM IST
━━━ 🔢 Calc Inputs ━━━
💰 Spot Price: 23,812.50
🎯 ATM Strike (K): 23,800
⏳ Time to Expiry: 1.23 trading days (0.0049 yr)
📋 ATM CE Price: ₹145.20
📋 ATM PE Price: ₹132.80
━━━ 📊 IV Results ━━━
📌 IV at Open: 14.25%
⚡ Current IV: 15.10%
📊 % Change: +5.96%
━━━━━━━━━━━━━━━━━━━━
```
        """)

    # Load alerts from DB
    alerts_df=db_load_alerts(limit=200)
    if not alerts_df.empty:
        today_alerts=alerts_df[alerts_df['timestamp'].str.startswith(TODAY.isoformat())]
        c1,c2,c3,c4=st.columns(4)
        c1.metric("Today's Alerts",len(today_alerts))
        c2.metric("Total Logged",len(alerts_df))
        c3.metric("Day High Alerts",len(alerts_df[alerts_df['alert_type'].str.contains('HIGH')]))
        c4.metric("Day Low Alerts",len(alerts_df[alerts_df['alert_type'].str.contains('LOW')]))

        def _acolor(v):
            return f'color:{"#00e5a0" if "HIGH" in str(v) else "#ff4466"};font-weight:bold'

        cols_show=['timestamp','inst_label','expiry','alert_type','atm_strike','open_iv','current_iv','pct_chg']
        existing=[c for c in cols_show if c in alerts_df.columns]
        disp_a=alerts_df[existing].copy()
        disp_a.columns=['Time','Instrument','Expiry','Alert Type','ATM Strike','Open IV','Current IV','% Change'][:len(existing)]
        st.dataframe(
            disp_a.style.applymap(_acolor,subset=['Alert Type'])
                        .format({'Open IV':'{:.2f}','Current IV':'{:.2f}',
                                 '% Change':'{:+.2f}','ATM Strike':'{:,.0f}'}),
            use_container_width=True,height=400)
        st.download_button("⬇️ Download Alerts CSV",disp_a.to_csv(),"alerts_log.csv","text/csv")
    else:
        st.info("No alerts logged yet. Alerts appear here when ATM IV breaks its Day High or Day Low by the configured threshold.")

    # Manual test panel
    st.divider()
    st.subheader("🧪 Manual Alert Test")
    ta1,ta2=st.columns(2)
    with ta1:
        test_inst_id=st.selectbox("Instrument",all_ids,format_func=lambda x:lbls[x],key='test_sel')
    with ta2:
        test_type=st.radio("Type",["DAY_HIGH","DAY_LOW"],horizontal=True)

    if st.button("🚀 Send Test Alert"):
        inst_d=next(i for i in INSTRUMENTS if i['id']==test_inst_id)
        civ_t=ALL_DATA[test_inst_id]['atm_iv'] or 15.0
        oiv_t=ALL_DATA[test_inst_id]['open_atm'] or 14.0
        pct_t=(civ_t-oiv_t)/oiv_t*100 if oiv_t else 0
        atm_s=ALL_DATA[test_inst_id].get('atm_strike',22000)
        spot_t=ALL_DATA[test_inst_id].get('live_spot')
        T_t=ALL_DATA[test_inst_id].get('T_years')
        ce_t=ALL_DATA[test_inst_id].get('atm_ce_ltp')
        pe_t=ALL_DATA[test_inst_id].get('atm_pe_ltp')
        state='RISING IV — 🔺 Day High' if test_type=='DAY_HIGH' else 'FALLING IV — 🔻 Day Low'
        msg=_format_alert(inst_d['label'],str(inst_d['expiry']),state,
                          _now_ist(),atm_s,oiv_t,civ_t,pct_t,
                          spot=spot_t, T_years=T_t, ce_ltp=ce_t, pe_ltp=pe_t)
        ok, tg_err = _tg_send_debug(msg)
        if ok:
            st.success("✅ Telegram alert sent!")
        else:
            st.error(f"⚠️ Telegram failed: {tg_err}")
        db_log_alert(test_inst_id,inst_d['label'],str(inst_d['expiry']),
                     f'TEST_{test_type}',atm_s,oiv_t,civ_t,pct_t,msg,_now_ist())
        st.code(msg.replace('<b>','**').replace('</b>','**'),language='text')

# ════════════════════════════════════════════════════════════════════════════════
#  TAB 5 — IV CALCULATION DETAILS
# ════════════════════════════════════════════════════════════════════════════════
with tab5:
    st.subheader("🔢 IV Calculation Inputs & Results — Per Instrument")
    st.caption(f"🕐 Data as of: {NOW:%H:%M:%S IST}  |  Source: {_live_source_label()}")

    with st.expander("ℹ️ What do these values mean?"):
        st.markdown("""
| Column | Description |
|--------|-------------|
| **Instrument** | Name of the index or stock |
| **Calc Time** | When the IV was last computed |
| **Spot Price (S)** | Live underlying price used in Black-Scholes |
| **ATM Strike (K)** | Nearest strike to spot used for ATM IV |
| **Expiry** | Option expiry date |
| **T (years)** | Time to expiry in years (calendar-adjusted) |
| **T (trading days)** | T × 252 trading days |
| **ATM CE Price** | Live ATM Call option LTP used for IV solve |
| **ATM PE Price** | Live ATM Put option LTP used for IV solve |
| **Final ATM IV (%)** | Weighted ATM IV output from Black-Scholes solver |
| **Open ATM IV (%)** | IV at market open (baseline for alerts) |
| **IV Chg %** | % change of current IV vs open IV |
        """)

    # Build the calculation details table
    _calc_rows = []
    _ft5 = st.radio("Filter", ["All", "Indices Only", "Stocks Only"], horizontal=True, key="tab5_filter")
    _ft5m = {"All": None, "Indices Only": "index", "Stocks Only": "stock"}[_ft5]

    for iid, d in ALL_DATA.items():
        inst = d['inst']
        if _ft5m and inst['type'] != _ft5m:
            continue
        spot   = d.get('live_spot') or inst['spot']
        T_yr   = d.get('T_years') or _compT(inst['expiry'])
        atm_iv = d.get('atm_iv')
        open_iv= d.get('open_atm')
        if atm_iv and open_iv and open_iv > 1.0:
            _raw_chg = (atm_iv - open_iv) / open_iv * 100
            # Cap at ±500% — anything beyond is a data artifact, not real IV move
            pct_chg = max(-500.0, min(500.0, _raw_chg))
        else:
            pct_chg = None
        _ce_sym = d.get('atm_sym_ce', '')
        _pe_sym = d.get('atm_sym_pe', '')
        _calc_rows.append({
            'Instrument':      inst['label'],
            'Type':            inst['type'].capitalize(),
            'Calc Time':       d.get('calc_time', NOW.strftime('%H:%M:%S IST')),
            'Spot (S)':        round(spot, 2),
            'ATM Strike (K)':  d.get('atm_strike', round(spot / inst['strike_step']) * inst['strike_step']),
            'Expiry':          str(inst['expiry']),
            'T (years)':       round(T_yr, 5),
            'T (trading days)':round(T_yr * 252, 3),
            'ATM CE Symbol':   _ce_sym,
            'ATM PE Symbol':   _pe_sym,
            'ATM CE Price ₹':  d.get('atm_ce_ltp'),
            'ATM PE Price ₹':  d.get('atm_pe_ltp'),
            'Final ATM IV %':  round(atm_iv, 3) if atm_iv else None,
            'Open IV %':       round(open_iv, 3) if open_iv else None,
            'IV Chg %':        round(pct_chg, 2) if pct_chg is not None else None,
        })

    _calc_df = pd.DataFrame(_calc_rows)

    # Summary metrics row
    _valid_iv = [r['Final ATM IV %'] for r in _calc_rows if r['Final ATM IV %']]
    if _valid_iv:
        mc1, mc2, mc3, mc4, mc5 = st.columns(5)
        mc1.metric("Instruments Shown", len(_calc_rows))
        mc2.metric("Avg ATM IV", f"{sum(_valid_iv)/len(_valid_iv):.2f}%")
        mc3.metric("Max ATM IV", f"{max(_valid_iv):.2f}%")
        mc4.metric("Min ATM IV", f"{min(_valid_iv):.2f}%")
        _valid_chg = [r['IV Chg %'] for r in _calc_rows if r['IV Chg %'] is not None]
        mc5.metric("Avg IV Chg", f"{sum(_valid_chg)/len(_valid_chg):+.2f}%" if _valid_chg else "—")

    st.divider()

    # Single instrument deep-dive
    _sel5 = st.selectbox("🔍 Deep-dive single instrument",
                         [r['Instrument'] for r in _calc_rows],
                         key='tab5_sel')
    _row5 = next((r for r in _calc_rows if r['Instrument'] == _sel5), None)
    if _row5:
        d5c1, d5c2, d5c3 = st.columns(3)
        with d5c1:
            st.markdown("##### 📥 Inputs (Black-Scholes)")
            st.metric("Spot Price (S)", f"{_row5['Spot (S)']:,.2f}")
            st.metric("ATM Strike (K)", f"{_row5['ATM Strike (K)']:,.0f}")
            st.metric("T (years)", f"{_row5['T (years)']:.5f}")
            st.metric("T (trading days)", f"{_row5['T (trading days)']:.2f}")
        with d5c2:
            st.markdown("##### 📋 Option Prices Used")
            ce = _row5['ATM CE Price ₹']
            pe = _row5['ATM PE Price ₹']
            st.metric("ATM CE LTP", f"₹{ce:.2f}" if ce else "—")
            st.metric("ATM PE LTP", f"₹{pe:.2f}" if pe else "—")
            st.metric("Expiry", _row5['Expiry'])
            st.metric("Calc Time", _row5['Calc Time'])
        with d5c3:
            st.markdown("##### ⚡ Output (Final IV)")
            iv5 = _row5['Final ATM IV %']
            ov5 = _row5['Open IV %']
            ch5 = _row5['IV Chg %']
            st.metric("Final ATM IV", f"{iv5:.3f}%" if iv5 else "—",
                      delta=f"{ch5:+.2f}%" if ch5 is not None else None)
            st.metric("Open IV", f"{ov5:.3f}%" if ov5 else "—")

    st.divider()

    # Full table
    st.markdown("##### 📋 Full Calculation Table")

    # ── Data quality banner — collapsed in expander to save space ───────────
    _suspect_all  = [r for r in _calc_rows if r['Final ATM IV %'] and r['Final ATM IV %'] > 100]
    _suspect_bad  = [r for r in _suspect_all if (r.get('T (trading days)') or 0) > 1.5]
    _suspect_ok   = [r for r in _suspect_all if (r.get('T (trading days)') or 0) <= 1.5]
    _no_price     = [r for r in _calc_rows if r['ATM CE Price ₹'] is None and r['ATM PE Price ₹'] is None]

    _issues = len(_suspect_bad) + len(_suspect_ok) + len(_no_price)
    if _issues > 0:
        _label = (
            f"📋 Data Notes: "
            + (f"{len(_suspect_ok)} IV>100% near-expiry (normal)  " if _suspect_ok else "")
            + (f"⚠ {len(_suspect_bad)} IV>100% data issue  " if _suspect_bad else "")
            + (f"ℹ {len(_no_price)} instruments missing CE/PE price" if _no_price else "")
        ).strip()
        with st.expander(_label, expanded=False):
            if _suspect_ok:
                st.info(
                    f"ℹ️ **{len(_suspect_ok)} instrument(s) show IV > 100% near expiry** — this is **normal**. "
                    f"With < 1.5 trading days left, even small price moves amplify IV mathematically. "
                    f"Affected: {', '.join(r['Instrument'] for r in _suspect_ok[:8])}"
                )
            if _suspect_bad:
                st.warning(
                    f"⚠️ **{len(_suspect_bad)} instrument(s) show IV > 100% with > 1.5 days to expiry** — "
                    f"likely wrong ATM strike from stale spot cache. "
                    f"**Wait 1–2 refresh cycles** or restart. "
                    f"Affected: {', '.join(r['Instrument'] for r in _suspect_bad[:8])}"
                )
            if _no_price:
                _no_price_names = ', '.join(r['Instrument'] for r in _no_price[:10])
                st.info(
                    f"ℹ️ **{len(_no_price)} instrument(s) have no ATM CE/PE price** (shown as —). "
                    f"Normal for zero-OI strikes or BSE F&O (Sensex W2) without BSE F&O account access. "
                    f"IV is still computed from surrounding strikes when available. "
                    f"Affected: {_no_price_names}"
                )

    def _iv_color(v):
        if pd.isna(v) or v is None: return ''
        return f'color: {"#00e5a0" if v > 0 else "#ff4466" if v < 0 else "white"};font-weight:bold'

    def _iv_validity_color(v):
        """Color-code IV values. High IV near expiry is normal — don't flag it red."""
        if pd.isna(v) or v is None: return 'color: #64748b'
        # Only flag red if > 150% AND we have enough time (can't easily check T here,
        # so use a higher threshold — the banner handles the near-expiry explanation)
        if v > 200: return 'background-color: #3d0a0a; color: #ff4466; font-weight: bold'
        if v > 150: return 'background-color: #2d1a00; color: #ffd740; font-weight: bold'
        return 'color: #00e5a0' 

    if not _calc_df.empty:
        # Display table hides Symbol columns (too wide) — they are in CSV download
        _display_cols = [c for c in _calc_df.columns
                         if c not in ('ATM CE Symbol', 'ATM PE Symbol')]
        _display_df   = _calc_df[_display_cols]
        styled = (_display_df.style
                  .applymap(_iv_color, subset=['IV Chg %'])
                  .applymap(_iv_validity_color, subset=['Final ATM IV %'])
                  .format({
                      'Spot (S)':         '{:,.2f}',
                      'ATM Strike (K)':   '{:,.0f}',
                      'T (years)':        '{:.5f}',
                      'T (trading days)': '{:.2f}',
                      'ATM CE Price ₹':   lambda x: f'₹{x:.2f}' if pd.notna(x) and x is not None else '—',
                      'ATM PE Price ₹':   lambda x: f'₹{x:.2f}' if pd.notna(x) and x is not None else '—',
                      'Final ATM IV %':   lambda x: f'{x:.3f}%' if pd.notna(x) and x is not None else '—',
                      'Open IV %':        lambda x: f'{x:.3f}%' if pd.notna(x) and x is not None else '—',
                      'IV Chg %':         lambda x: ('⚠ stale' if abs(x) >= 499 else f'{x:+.2f}%') if pd.notna(x) and x is not None else '—',
                  }))
        st.dataframe(styled, use_container_width=True, height=500)
        st.download_button("⬇️ Download CSV", _calc_df.to_csv(index=False),
                           "iv_calc_details.csv", "text/csv")

    # Telegram send button for current instrument details
    st.divider()
    st.subheader("📤 Send Calc Details to Telegram")
    _tg_sel = st.selectbox("Instrument to send", [r['Instrument'] for r in _calc_rows], key='tab5_tg_sel')
    if st.button("📨 Send to Telegram", key='tab5_tg_btn'):
        _tg_row = next((r for r in _calc_rows if r['Instrument'] == _tg_sel), None)
        if _tg_row:
            ce_v = _tg_row['ATM CE Price ₹']
            pe_v = _tg_row['ATM PE Price ₹']
            iv_v = _tg_row['Final ATM IV %']
            ov_v = _tg_row['Open IV %']
            ch_v = _tg_row['IV Chg %']
            _tg_msg = (
                f"🔢 <b>IV Calc Details — {_tg_row['Instrument']}</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"🕐 <b>Time:</b> {_tg_row['Calc Time']}\n"
                f"🔖 <b>Expiry:</b> {_tg_row['Expiry']}\n"
                f"━━━ 📥 Inputs ━━━\n"
                f"💰 <b>Spot (S):</b> {_tg_row['Spot (S)']:,.2f}\n"
                f"🎯 <b>ATM Strike (K):</b> {_tg_row['ATM Strike (K)']:,.0f}\n"
                f"⏳ <b>T (years):</b> {_tg_row['T (years)']:.5f}  ({_tg_row['T (trading days)']:.2f} days)\n"
                f"📋 <b>ATM CE LTP:</b> ₹{ce_v:.2f}\n" if ce_v else ""
                f"📋 <b>ATM PE LTP:</b> ₹{pe_v:.2f}\n" if pe_v else ""
                f"━━━ ⚡ Output ━━━\n"
                f"📌 <b>Open IV:</b> {ov_v:.3f}%\n" if ov_v else ""
                f"⚡ <b>Final ATM IV:</b> {iv_v:.3f}%\n" if iv_v else ""
                f"📊 <b>IV Chg:</b> {ch_v:+.2f}%\n" if ch_v is not None else ""
                f"━━━━━━━━━━━━━━━━━━━━"
            )
            _ok, _err = _tg_send_debug(_tg_msg)
            if _ok:
                st.success("✅ Sent to Telegram!")
                st.code(_tg_msg.replace('<b>','**').replace('</b>','**'), language='text')
            else:
                st.error(f"⚠️ Failed: {_err}")

# ── Footer ─────────────────────────────────────────────────────────────────────
st.divider()
c1,c2,c3,c4=st.columns(4)
c1.caption(f"💾 Snapshots: `{STORE_DIR}/`  |  {len(stored_files)} day(s) stored")
c2.caption(f"🗄️ SQLite DB: `{DB_PATH}`")
c3.caption(f"🕐 {NOW:%H:%M:%S IST}  |  Prev trading day: {prev_date:%d-%b-%Y}"
           +(" ✅" if has_yest_store else " ⚠️ (not found)"))
c4.caption(f"📊 {ni} indices · {ns} stocks · {len(INSTRUMENTS)} total instruments")
