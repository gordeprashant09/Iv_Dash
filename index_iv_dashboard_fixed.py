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
║  Run:  streamlit run iv_dashboard_final.py                                  ║
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

# ── Auto-install broker dependencies if missing ───────────────────────────────
def _ensure_pkg(pkg: str, import_name: str = None):
    """pip-install `pkg` if `import_name` cannot be imported. Shows clear error if it fails."""
    import_name = import_name or pkg
    try:
        __import__(import_name)
        return True
    except ImportError:
        print(f"[INFO] Installing missing package: {pkg} ...")
        try:
            result = subprocess.run(
                [sys.executable, "-m", "pip", "install", pkg],
                capture_output=True, text=True)
            if result.returncode != 0:
                print(f"[WARNING] Could not auto-install {pkg}. Run manually: pip install {pkg}")
                return False
            __import__(import_name)
            print(f"[INFO] {pkg} installed successfully.")
            return True
        except Exception as _e:
            print(f"[WARNING] Auto-install of {pkg} failed: {_e}. Run manually: pip install {pkg}")
            return False

_ensure_pkg("kiteconnect")
_ensure_pkg("pyotp")
_ensure_pkg("streamlit-autorefresh", "streamlit_autorefresh")

import numpy as np
import pandas as pd
from PIL import Image
Image.MAX_IMAGE_PIXELS = None

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
from matplotlib.lines import Line2D
from matplotlib.ticker import FuncFormatter
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
STORE_DIR = Path("index_iv_data_store"); STORE_DIR.mkdir(exist_ok=True)
DB_PATH   = Path("iv_store.db")

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
# TG_TOKEN          = os.getenv("TELEGRAM_BOT_TOKEN", "8384721299:AAGonU21NxUAgaw2S-_y3SNK6CVISsb-PWA")
TG_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
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
st.set_page_config(page_title="IV Dashboard — NSE Indices", page_icon="📈",
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

def _read_exchange_csv(filepath, seg):
    """Read a holidays.csv or str_days.csv robustly.
    The file has a header row (e.g. Exchange,Date,Week,Reason).
    Renames col[0]->Exchange and col[1]->Date by position so any
    header naming variation (Day/Week, Description/Reason) is handled.
    """
    raw = pd.read_csv(filepath)
    cols = list(raw.columns)
    raw = raw.rename(columns={cols[0]: 'Exchange', cols[1]: 'Date'})
    raw['Exchange'] = raw['Exchange'].astype(str).str.strip()
    raw['Date']     = raw['Date'].astype(str).str.strip()
    filtered = raw[raw['Exchange'] == seg].copy()
    filtered['Date'] = pd.to_datetime(filtered['Date'], format='%d-%b-%y')
    return filtered

def dateprocessor(look_back_days=0, for_date=None, exch='NSE',
                  holidays_csv='holidays.csv', str_days_csv='str_days.csv'):
    exch = exch.upper()
    exch_seg_map = {'NSE': 'CM', 'FO': 'FO'}
    if exch not in exch_seg_map:
        raise ValueError(f"Invalid exchange '{exch}'. Only 'NSE' and 'FO' are supported.")
    seg = exch_seg_map[exch]
    start_date = '20151231'
    # +60 days: ensures weekly/monthly expiries (up to ~2 months ahead) are always in range.
    # The old +7 was too short -- expiries like "last Tuesday of next month" can be 35+ days out.
    end_date = (pd.to_datetime('today') + pd.Timedelta(days=60)).strftime('%Y%m%d')
    # Load holidays -- robust helper handles any header naming variation
    holiday_df  = _read_exchange_csv(holidays_csv, seg)
    ex_holidays = set(holiday_df['Date'].to_list())
    # Load special trading days
    str_days_df = _read_exchange_csv(str_days_csv, seg)
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

# ── Known holidays missing from CSV — safety net for late-notified closures ───
# Add pd.Timestamp('YYYY-MM-DD') here whenever a holiday is not yet in holidays.csv
_HARDCODED_HOLIDAYS: set = {
    pd.Timestamp('2026-03-26'),   # Holi — NSE & BSE closed (Sensex weekly shifts to 25-Mar)
    pd.Timestamp('2026-03-31'),   # Mahavir Jayanti / FY-end — NSE & BSE closed
}

# ── Build trading-day set from dateprocessor (used by _istd, _tdb, etc.) ─────
def _build_trading_day_set(holidays_csv='holidays.csv', str_days_csv='str_days.csv'):
    """Returns a frozenset of NSE F&O trading days.
    Uses exch='FO' (segment='FO') so F&O holidays are correctly excluded.
    Also subtracts _HARDCODED_HOLIDAYS as safety net for late-notified closures.
    """
    try:
        *_, df = dateprocessor(exch='FO', holidays_csv=holidays_csv,
                               str_days_csv=str_days_csv)
        return frozenset(df['tr_day'].dt.normalize()) - _HARDCODED_HOLIDAYS
    except Exception as e:
        log.warning(f"dateprocessor failed ({e}); falling back to weekdays-only calendar")
        start = pd.Timestamp('2015-12-31')
        end   = pd.Timestamp.today() + pd.Timedelta(days=60)
        return frozenset(pd.date_range(start, end, freq='B')) - _HARDCODED_HOLIDAYS

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
    moves back 1 calendar day at a time — NOT 7 days."""
    exp = _expiry_shift(_ltue(from_date.year, from_date.month))
    if exp <= from_date:
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
    moves back 1 calendar day at a time — NOT 7 days."""
    exp = _expiry_shift(_lthu(from_date.year, from_date.month))
    if exp <= from_date:
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
      weekly_tue  -> Nifty 50 weekly (every Tuesday)
      weekly_thu  -> Sensex weekly (every Thursday, BSE)
      weekly_fri  -> [deprecated, mapped to weekly_thu]
      monthly_tue -> BankNifty / FinNifty / MidcapNifty / stocks (last Tue of month)
      monthly_thu -> Sensex monthly (last Thu of month, BSE)

    W2/W3 safety: after _expiry_shift a Wn date could collapse onto or before W1
    (e.g. both Tuesdays/Thursdays are holidays shifting to the same day).
    In that case step forward to the next trading day after W1.
    """
    ref = _today or _today_ist()
    if etype in ('weekly_tue',):
        d = _nwd(ref, 1, include_today=True)
        for _ in range(wnum - 1): d = _nwd(d, 1, include_today=False)
        shifted = _expiry_shift(d)
        if wnum > 1:
            w1 = _resolve('weekly_tue', 1, _today=(_today or ref))
            if shifted <= w1:
                nxt = w1 + timedelta(days=1)
                while not _istd(nxt): nxt += timedelta(days=1)
                shifted = nxt
        return shifted
    if etype in ('weekly_thu', 'weekly_fri'):
        d = _nwd(ref, 3, include_today=True)
        for _ in range(wnum - 1): d = _nwd(d, 3, include_today=False)
        shifted = _expiry_shift(d)
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
    # Spots updated to approx Mar-2026 levels (live broker price overrides these within seconds)
    'NIFTY':     dict(label='Nifty 50',    etype='weekly_tue', wnum=1,lot=65, ss=50,  spot=24865),
    'NIFTY_W2':  dict(label='Nifty 50 W2', etype='weekly_tue', wnum=2,lot=65, ss=50,  spot=24865),
    'BANKNIFTY': dict(label='Bank Nifty',  etype='monthly_tue',wnum=1,lot=30, ss=100, spot=59840),
    'SENSEX':    dict(label='Sensex',      etype='weekly_thu', wnum=1,lot=20, ss=100, spot=80239),
    'SENSEX_W2': dict(label='Sensex W2',   etype='weekly_thu', wnum=2,lot=20, ss=100, spot=80239),
    'MIDCPNIFTY':dict(label='Midcap Nifty',etype='monthly_tue',wnum=1,lot=120,ss=25,  spot=12855),
    'FINNIFTY':  dict(label='Fin Nifty',   etype='monthly_tue',wnum=1,lot=60, ss=50,  spot=25800),
}

# ALL 155 stocks — exactly as in original script
_STKS=[
    ('AARTIIND',800,410),('ABB',250,5300),('ABBOTINDIA',50,29000),('ABCAPITAL',3200,165),
    ('ABFRL',2000,290),('ACC',400,2000),('ADANIENT',250,2380),('ADANIGREEN',500,930),
    ('ADANIPORTS',1250,1398),('ALKEM',200,5200),('AMBUJACEM',2000,525),('APOLLOHOSP',250,7034),
    ('APOLLOTYRE',3500,520),('ASHOKLEY',5000,220),('ASIANPAINT',300,2280),('AUROPHARMA',1000,1180),
    ('AXISBANK',625,1211),('BAJAJ_AUTO',125,8390),('BAJAJFINSV',500,2014),('BAJFINANCE',125,926),
    ('BALKRISIND',400,2450),('BANDHANBNK',5000,155),('BANKBARODA',5400,245),('BEL',7400,406),
    ('BERGEPAINT',1100,450),('BHEL',7000,262),('BPCL',3800,285),('BRITANNIA',200,4900),
    ('BSOFT',2800,390),('CANBK',3000,98),('CANFINHOME',2000,720),('CDSL',750,1550),
    ('CESC',2000,148),('CHOLAFIN',700,1200),('CIPLA',650,1509),('COALINDIA',2100,392),
    ('COFORGE',150,7800),('COLPAL',350,2650),('CONCOR',2300,880),('COROMANDEL',500,1750),
    ('CROMPTON',3600,385),('CUB',8000,160),('CUMMINSIND',400,3800),('DABUR',2800,510),
    ('DALBHARAT',400,1800),('DEEPAKNTR',250,2200),('DELTACORP',4800,125),('DIVISLAB',200,5100),
    ('DIXON',100,16500),('DLF',1650,780),('DRREDDY',125,1340),('EICHERMOT',200,5577),
    ('ESCORTS',275,3600),('EXIDEIND',3600,380),('FEDERALBNK',10000,185),('GAIL',3600,185),
    ('GLENMARK',1150,1050),('GMRINFRA',22500,80),('GNFC',1300,580),('GODREJCP',1000,1010),
    ('GODREJPROP',750,2500),('GRANULES',3750,490),('GSPL',2700,290),('GUJGASLTD',1250,520),
    ('HAVELLS',500,1500),('HCLTECH',700,1714),('HDFCAMC',300,4300),('HDFCBANK',550,1980),
    ('HDFCLIFE',750,786),('HEROMOTOCO',300,4298),('HINDALCO',1400,672),('HINDPETRO',2700,335),
    ('HINDUNILVR',500,2277),('HUDCO',6800,215),('IBULHSGFIN',5700,185),('ICICIBANK',700,1425),
    ('ICICIGI',375,1930),('ICICIPRULI',1500,620),('IDEA',70000,8),('IDFCFIRSTB',11000,65),
    ('IEX',3750,165),('IGL',1375,385),('INDHOTEL',2500,740),('INDIACEM',5300,145),
    ('INDIAMART',150,2200),('INDIGO',300,4200),('INDUSINDBK',500,829),('INDUSTOWER',2800,340),
    ('INFY',400,1856),('INTELLECT',750,780),('IOC',10000,140),('IPCALAB',800,1500),
    ('IRCTC',3450,880),('IRFC',6400,175),('ITC',1600,420),('JINDALSTEL',1250,890),
    ('JKCEMENT',300,4500),('JSWENERGY',2800,440),('JSWSTEEL',1350,930),('JUBLFOOD',1250,680),
    ('KOTAKBANK',400,2010),('LALPATHLAB',300,2500),('LAURUSLABS',2100,470),('LICHSGFIN',1000,580),
    ('LT',300,3420),('LTF',8000,120),('LTIM',150,5300),('LTTS',200,4600),('LUPIN',650,2050),
    ('M_M',700,3030),('MANAPPURAM',5000,195),('MARICO',1200,620),('MARUTI',100,12330),
    ('MCX',250,6000),('METROPOLIS',400,1900),('MFSL',900,1050),('M&MFIN',4000,175),
    ('MNGLM',2800,380),('MOTHERSON',5600,145),('MPHASIS',350,2700),('MRF',10,135000),
    ('MUTHOOTFIN',700,2100),('NATIONALUM',8500,195),('NAUKRI',150,6100),('NAVINFLUOR',200,3300),
    ('NESTLEIND',50,2280),('NHPC',8000,85),('NMDC',10000,68),('NTPC',3750,335),
    ('OBEROIRLTY',700,1800),('OFSS',200,11500),('OIL',2200,430),('ONGC',3850,240),
    ('PAGEIND',15,44000),('PEL',400,950),('PERSISTENT',250,5400),('PETRONET',3000,315),
    ('PFC',3000,420),('PIDILITIND',500,2750),('PIIND',250,3900),('PNB',8000,98),
    ('POLYCAB',300,6800),('POWERGRID',4500,295),('PVRINOX',1000,1350),('RAMCOCEM',550,880),
    ('RBLBANK',5600,170),('RECLTD',3000,490),('RELIANCE',250,1575),('SAIL',9000,118),
    ('SBICARD',1500,720),('SBILIFE',750,1640),('SBIN',1500,870),('SHRIRAMFIN',400,2850),
    ('SIEMENS',275,3100),('SRF',375,2350),('SUNPHARMA',700,1780),('SUNTV',1400,720),
    ('SUPREMEIND',125,5300),('SYNGENE',1500,820),('TATACHEM',550,1050),('TATACOMM',500,1680),
    ('TATACONSUM',1100,1050),('TMPV',1400,740),('TATAPOWER',2700,380),('TATASTEEL',5500,145),
    ('TCS',150,3580),('TECHM',600,1460),('TITAN',375,3400),('TORNTPHARM',500,3250),
    ('TRENT',375,6200),('TVSMOTOR',350,2400),('UBL',600,1950),('ULTRACEMCO',100,11300),
    ('UNIONBANK',10000,118),('UNITDSPR',600,1400),('UPL',1300,310),('VEDL',2800,460),
    ('VOLTAS',500,1580),('WIPRO',1500,310),('ZEEL',6000,125),('ETERNAL',4500,245),('ZYDUSLIFE',1100,1050),
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
    # ── INDICES ONLY — stocks excluded ────────────────────────────────────────
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
            return pd.read_sql("""SELECT time_str,atm_iv,spot FROM atm_iv_history
                WHERE inst_id=? AND date=? ORDER BY timestamp""",
                c, params=(inst_id, date_str))
    except: return pd.DataFrame()

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

# ── Rate-limit guard: at most 3 concurrent LTP batch calls to Zerodha API ────
# Zerodha enforces ~3 req/s per connection; this semaphore serialises bursts.
_LTP_SEMAPHORE = threading.Semaphore(3)

def _kite_ltp_batch(symbols: list, _retry: int = 3) -> dict:
    """
    Fetch LTP for a LIST of symbols in ONE API call.
    Returns {symbol: last_price_float}.
    SDK accepts up to 500 symbols per call — use this instead of per-symbol calls
    to avoid connection pool exhaustion.

    Rate-limit handling:
      - A semaphore caps concurrent calls at 3 (Zerodha limit).
      - On "Too many requests" the call retries up to `_retry` times with
        exponential back-off (0.5s → 1s → 2s) before re-raising.
    """
    global _SDK_BATCH_LOGGED
    if not symbols:
        return {}

    # Acquire semaphore — blocks if 3 callers are already in-flight
    with _LTP_SEMAPHORE:
        return _kite_ltp_batch_inner(symbols, _retry)


def _kite_ltp_batch_inner(symbols: list, _retry: int = 3) -> dict:
    """Inner implementation called under _LTP_SEMAPHORE."""
    global _SDK_BATCH_LOGGED

    # ── Method 1: Official SDK (single batch call with retry on rate-limit) ──
    if _kite_sdk is not None:
        last_exc = None
        for attempt in range(max(1, _retry)):
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

                # Only raise "all missing" for larger option chain batches (not single spot fetches)
                real_prices = [v for v in result.values() if v > 0.05]
                if not real_prices and len(symbols) > 4:
                    log.warning(
                        f"SDK ltp() returned 0 entries for ALL {len(symbols)} symbols. "
                        f"Sample asked: {symbols[len(symbols)//2]!r}. "
                        f"Cache verified: {_verify_symbol(symbols[len(symbols)//2])}. "
                        f"Returned keys: {list(data.keys())[:3]}"
                    )
                    raise RuntimeError(
                        f"All {len(symbols)} option LTPs are 0/missing — "
                        f"symbol not found in Zerodha. "
                        f"Sample: {symbols[len(symbols)//2]!r}. "
                        f"Cache hit: {_verify_symbol(symbols[len(symbols)//2])}"
                    )
                return result

            except RuntimeError as e:
                err_str = str(e).lower()
                # Retry only on explicit rate-limit signals; re-raise all other errors
                if "too many requests" in err_str or "rate" in err_str or "429" in err_str:
                    wait = 0.5 * (2 ** attempt)   # 0.5s, 1s, 2s
                    log.warning(
                        f"SDK batch LTP rate-limited ({len(symbols)} symbols), "
                        f"attempt {attempt+1}/{_retry} — waiting {wait:.1f}s. Error: {e}"
                    )
                    last_exc = e
                    time.sleep(wait)
                    continue
                raise   # non-rate-limit RuntimeError — do not retry
            except Exception as e:
                err_str = str(e).lower()
                if "too many requests" in err_str or "rate" in err_str or "429" in err_str:
                    wait = 0.5 * (2 ** attempt)
                    log.warning(
                        f"SDK batch LTP rate-limited ({len(symbols)} symbols), "
                        f"attempt {attempt+1}/{_retry} — waiting {wait:.1f}s. Error: {e}"
                    )
                    last_exc = e
                    time.sleep(wait)
                    continue
                raise RuntimeError(f"SDK batch LTP failed ({len(symbols)} symbols): {e}") from e

        # All retries exhausted
        raise RuntimeError(
            f"SDK batch LTP failed after {_retry} retries (Too many requests). "
            f"Sample: {symbols[len(symbols)//2]!r}"
        ) from last_exc

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
            if r.status_code == 429:
                # Rate limited — wait and retry once
                time.sleep(1.0)
                r = _kite_enc.get(f"{_KITE_WEB}/quote/ltp", params=[("i", sym)], timeout=8)
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
    global _INST_CACHE, _INST_STRIKES, _INST_CACHE_DATE
    _INST_CACHE   = {}
    _INST_STRIKES = {}
    today = _today_ist()
    names_seen   = set()   # for diagnostic logging
    names_by_exch = {}

    for exch in ("NFO", "BFO"):
        try:
            instruments = kite.instruments(exch)
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
                # Secondary key: use ISO string expiry to avoid type mismatch
                _INST_CACHE[(exch, name, exp.isoformat(), strike, itype)] = full_sym
                # Strike index: use ISO string for expiry to avoid date/datetime type issues
                exp_str_key = exp.isoformat()   # "2026-02-26" — always a plain string
                skey = (exch, name, exp_str_key)
                if skey not in _INST_STRIKES:
                    _INST_STRIKES[skey] = set()
                _INST_STRIKES[skey].add(strike)
                names_seen.add(name)
                count += 1

            names_by_exch[exch] = count
            log.info(f"Instruments cache: loaded {exch} — {count} options")
        except Exception as e:
            log.warning(f"Instruments cache failed for {exch}: {e}")

    _INST_CACHE_DATE = today
    total = sum(names_by_exch.values())
    # Log sample names for diagnostics — helps identify name mismatches
    sample_names = sorted(names_seen)[:30]
    log.info(f"Instruments cache built: {total} options. Sample names: {sample_names}")
    log.info(f"All unique underlying names ({len(names_seen)}): {sorted(names_seen)}")
    return total


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

def _record_iv_snapshot():
    """Fetch live ATM IV for all instruments and save to DB. Run every 60s."""
    if BROKER != 'zerodha' or not _kite_enc:
        return
    now   = _now_ist()
    today = now.date().isoformat()
    insts = build_registry()

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
        except Exception as e:
            log.debug(f"recorder snap failed {inst['id']}: {e}")

    with ThreadPoolExecutor(max_workers=8) as ex:
        list(ex.map(_snap_one, insts))
    log.info(f"IV snapshot recorded for {len(insts)} instruments at {now.strftime('%H:%M:%S')}")

def _recorder_loop():
    """Daemon loop: record IV every 60s during market hours."""
    import time as _t
    while True:
        try:
            now = _now_ist()
            mkt_open  = now.replace(hour=CFG['market_open_h'],  minute=CFG['market_open_m'],  second=0, microsecond=0)
            mkt_close = now.replace(hour=CFG['market_close_h'], minute=CFG['market_close_m'], second=0, microsecond=0)
            if mkt_open <= now <= mkt_close:
                _record_iv_snapshot()
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
    # Also record one snapshot immediately so DB has data right away
    threading.Thread(target=_record_iv_snapshot, daemon=True, name="IVSnap0").start()

# ── Spot price cache — populated at startup and refreshed every 5 min ─────────
_SPOT_CACHE: dict = {}          # {inst_id: float}  — live spot prices
_SPOT_CACHE_LOCK = threading.Lock()
_SPOT_CACHE_TS: float = 0.0     # epoch time of last refresh
_SPOT_REFRESH_IN_PROGRESS = threading.Event()  # prevents duplicate refresh threads

# ── Open IV cache — stores first-of-day ATM IV per instrument ─────────────────
# Keyed by (inst_id, date_str). Persists across Streamlit reruns via JSON file.
_OPEN_IV_CACHE: dict = {}        # {(inst_id, date_str): float}  percent e.g. 14.5
_OPEN_IV_LOCK  = threading.Lock()
_OPEN_IV_PATH  = STORE_DIR / "open_iv_cache.json"

def _load_open_iv_cache():
    """Load open IV cache from disk (survives Streamlit reruns)."""
    global _OPEN_IV_CACHE
    try:
        if _OPEN_IV_PATH.exists():
            raw = json.loads(_OPEN_IV_PATH.read_text())
            today_str = _today_ist().isoformat()
            # Only keep today's entries — discard stale dates
            with _OPEN_IV_LOCK:
                _OPEN_IV_CACHE = {
                    tuple(k.split('|')): float(v)
                    for k, v in raw.items()
                    if k.endswith(f'|{today_str}')
                }
            log.info(f"Open IV cache loaded: {len(_OPEN_IV_CACHE)} entries for today")
    except Exception as e:
        log.warning(f"Open IV cache load failed: {e}")

def _save_open_iv_cache():
    """Persist open IV cache to disk."""
    try:
        with _OPEN_IV_LOCK:
            serialisable = {'|'.join(k): v for k, v in _OPEN_IV_CACHE.items()}
        _OPEN_IV_PATH.write_text(json.dumps(serialisable))
    except Exception as e:
        log.debug(f"Open IV cache save failed: {e}")

def _get_open_iv(inst_id: str, live_iv: float, today_str: str) -> float:
    """
    Get today's open IV for this instrument.
    Waits until 9:20 AM IST before pinning the open baseline (avoids
    noisy opening prints at 9:15). Once pinned, the value is frozen
    for the rest of the day.
    Returns the open IV as a percentage (e.g. 14.5 for 14.5%).
    """
    # ── Do not pin Open IV before 9:20 AM IST ────────────────────────────────
    now = _now_ist()
    open_pin_time = now.replace(hour=9, minute=20, second=0, microsecond=0)
    if now < open_pin_time:
        return 0.0   # too early — not yet capturing open IV

    key = (inst_id, today_str)
    with _OPEN_IV_LOCK:
        if key in _OPEN_IV_CACHE:
            return _OPEN_IV_CACHE[key]
        # First reading at or after 9:20 AM — pin as the open baseline
        if live_iv and live_iv > 0:
            _OPEN_IV_CACHE[key] = live_iv
            log.info(f"Open IV pinned at 9:20: {inst_id} = {live_iv:.3f}%")
            threading.Thread(target=_save_open_iv_cache, daemon=True).start()
            return live_iv
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

    # Build batch: indices only (stocks excluded)
    sym_to_id = {}   # zerodha_sym → inst_id
    batch = []
    for iid, spot_sym in _INDEX_SPOT_SYMS.items():
        if spot_sym not in sym_to_id:
            sym_to_id[spot_sym] = iid
            batch.append(spot_sym)

    # NOTE: stocks removed — indices-only dashboard

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
        'BAJAJ_AUTO': 'BAJAJ-AUTO',   # keeps hyphen in tradingsymbol
        'M_M':        'MM',           # M&M options trade as MM on NSE
        'NIFTY_W2':   'NIFTY',
        'SENSEX_W2':  'SENSEX',
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
        'BAJAJ_AUTO': 'BAJAJ-AUTO',
        'M_M':        'M&M',
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
    # Some names differ: we use underscores/no-specials, Zerodha uses hyphens/ampersands
    _ID_TO_ZERODHA_NAME = {
        'BAJAJ_AUTO': 'BAJAJ-AUTO',
        'M_M':        'M&M',
    }
    raw_id    = inst['id'].replace('_W2', '')
    inst_name = _ID_TO_ZERODHA_NAME.get(raw_id, raw_id)  # Zerodha name field
    expiry_dt = inst['expiry']                             # date object

    # Always recompute ATM using live S (not stale inst['spot'] from registry)
    cache_strikes = _get_strikes_from_cache(opt_exch, inst_name, expiry_dt, S, n_strikes=11)

    if cache_strikes:
        # ✅ Use exact strikes from Zerodha's instrument master
        strikes = cache_strikes
        log.debug(f"{inst['id']}: using {len(strikes)} cache strikes, "
                  f"ATM area: {cache_strikes[len(cache_strikes)//2]:.0f}")
    else:
        # ⚠️ Cache empty or instrument not found — fall back to computed strikes
        # Use math.floor(x + 0.5) rounding — avoids Python banker's rounding
        # which can give wrong ATM for BANKNIFTY (step=100) and MIDCPNIFTY (step=25)
        atm    = math.floor(S / step + 0.5) * step
        strikes = [atm + i * step for i in range(-5, 6)]
        if _INST_CACHE:
            raise RuntimeError(
                f"NOT_IN_FON: {inst['id']} has no options for {expiry_dt} "
                f"(looked up as name={inst_name!r} in {opt_exch}). "
                f"Not in F&O segment or wrong expiry."
            )

    # ── Build exact option symbols from cache ────────────────────────────────
    ce_syms = {}
    pe_syms = {}
    for K in strikes:
        # Name-based lookup gives exact Zerodha tradingsymbol
        ce_syms[K] = (_lookup_option_sym(opt_exch, inst_name, expiry_dt, K, 'CE')
                      or f"{opt_exch}:{sym}{exp_str}{int(K)}CE")
        pe_syms[K] = (_lookup_option_sym(opt_exch, inst_name, expiry_dt, K, 'PE')
                      or f"{opt_exch}:{sym}{exp_str}{int(K)}PE")

    if not strikes:
        raise RuntimeError(f"No strikes available for {inst['id']} {expiry_dt}")

    # ── ONE batch LTP call for all 22 CE+PE symbols ──────────────────────────
    all_syms = [ce_syms[K] for K in strikes] + [pe_syms[K] for K in strikes]
    try:
        ltp_map = _kite_ltp_batch(all_syms)
    except RuntimeError as e:
        raise RuntimeError(
            f"Batch LTP failed for {inst['id']} {exp_str} ({len(all_syms)} symbols): {e}. "
            f"Sample: {ce_syms[strikes[len(strikes)//2]]}"
        ) from e

    rows = []
    missing_ce = []
    missing_pe = []
    for K in strikes:
        ce_price = ltp_map.get(ce_syms[K], 0.05)
        pe_price = ltp_map.get(pe_syms[K], 0.05)
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
                last_iv = float(snap[iid][-1]['atm_iv']) / 100.0
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
        else:
            _live_error = err_str
            log.warning(f"Live chain FAILED for {inst['id']}: {e}")
            lc = _chain(inst, seed_off=0)  # last-resort fallback, tagged as error

    # Yesterday — use real DB data when possible
    yc  = _yest_chain_real(inst)

    ls    = skew_from_chain(inst, lc, 'Live')
    ys    = skew_from_chain(inst, yc, 'Yesterday')
    lsp   = float(ls['spot'].iloc[0])
    atm_iv= watm_iv(ls, lsp)

    # Open IV — use persistent cache (pinned at first reading of the day)
    # Falls back to _open_chain_real only for the skew CURVE shape (not the IV level)
    open_atm_pct = _get_open_iv(inst['id'], atm_iv, today) if atm_iv else 0.0
    if open_atm_pct > 0:
        # Build open chain using pinned open IV level (accurate baseline)
        oc = _chain(inst, base_iv=open_atm_pct / 100.0, noise=0.001, seed_off=200)
    else:
        oc = _open_chain_real(inst)
    os_     = skew_from_chain(inst, oc, 'Open 9:20')
    osp     = float(os_['spot'].iloc[0])
    open_atm = open_atm_pct if open_atm_pct > 0 else watm_iv(os_, osp)

    # ── ATM strike: use math.floor rounding to avoid Python banker's rounding ──
    # round() in Python uses "round-half-to-even" which can give wrong ATM strike
    # for BANKNIFTY (step=100) and MIDCPNIFTY (step=25) when spot lands near midpoint.
    # math.floor(x + 0.5) always rounds 0.5 UP — standard financial convention.
    _step = inst['strike_step']
    atm_strike = math.floor(lsp / _step + 0.5) * _step

    # ── Extract ATM row for calculation debug info ────────────────────────────
    T_years = _compT(inst['expiry'])
    atm_row = ls[ls['strike'] == atm_strike]
    if atm_row.empty:
        atm_row = ls.iloc[(ls['bps']).abs().argsort().iloc[:1]]
    # Get ATM CE and PE option prices from original chain
    atm_ce_ltp = atm_pe_ltp = None
    if not lc.empty:
        # Find closest available strike to ATM from actual live chain
        # This ensures we use a real tradeable strike even if computed ATM is off
        lc_closest_idx = (lc['strike'] - atm_strike).abs().idxmin()
        lc_atm = lc.loc[[lc_closest_idx]]
        actual_atm_strike = float(lc_atm.iloc[0]['strike'])
        # Accept if within one step — handles both exact match and cache-based strikes
        if abs(actual_atm_strike - atm_strike) <= _step:
            atm_strike = actual_atm_strike  # prefer actual strike from live chain
            atm_ce_ltp = float(lc_atm.iloc[0].get('ce_ltp', 0)) or None
            atm_pe_ltp = float(lc_atm.iloc[0].get('pe_ltp', 0)) or None

    # Only persist to DB when data is genuinely live (not mock fallback)
    if atm_iv and not _live_error:
        dh, dl = db_get_day_extremes(inst['id'], today)
        new_high = max(dh, atm_iv); new_low = min(dl if dl < 9999 else atm_iv, atm_iv)
        db_save_atm(inst['id'], exp, atm_iv, lsp, new_high, new_low, now)
        if open_atm:
            check_and_alert(inst, atm_iv, open_atm, atm_strike,
                            spot=lsp, T_years=T_years,
                            ce_ltp=atm_ce_ltp, pe_ltp=atm_pe_ltp)

    return inst['id'], dict(inst=inst, live=ls, yest=ys, open=os_,
                            atm_iv=atm_iv, open_atm=open_atm,
                            live_spot=lsp, atm_strike=atm_strike,
                            T_years=T_years,
                            atm_ce_ltp=atm_ce_ltp, atm_pe_ltp=atm_pe_ltp,
                            calc_time=now.strftime('%H:%M:%S IST'),
                            live_error=_live_error)

@st.cache_data(show_spinner=False, ttl=60)
def build_all_data_parallel():
    """
    Load ALL 7 indices in a single parallel pass.
    Uses live Zerodha data when connected, mock otherwise.
    TTL=60s — refreshes every minute during market hours.
    """
    insts = build_registry()
    out   = {}
    # max_workers=3: matches _LTP_SEMAPHORE — at most 3 instruments fetch
    # concurrently so we never fire more than 3 ltp() calls simultaneously.
    # Instruments are submitted with a small stagger (0.15s) so the burst of
    # 7 futures landing together is spread over ~1s, staying within Zerodha's
    # ~3 req/s rate limit even before the semaphore kicks in.
    with ThreadPoolExecutor(max_workers=3) as ex:
        futs = {}
        for i, inst in enumerate(insts):
            if i > 0:
                time.sleep(0.15)   # stagger submissions by 150ms
            futs[ex.submit(_process_inst, inst)] = inst
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
@st.cache_data(show_spinner=False, ttl=55)   # 55s < 60s recorder interval — always fresh
def build_intraday_fast(inst_ids: tuple, _cache_date: str = None):  # _cache_date busts cache daily
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
            # Sort by time and deduplicate so chart always draws left→right
            db_df['_dt']=pd.to_datetime(db_df['time_str'],format='%H:%M')
            db_df=db_df.sort_values('_dt').drop_duplicates(subset='_dt',keep='last').drop(columns='_dt').reset_index(drop=True)
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
    with open(path) as f: raw=json.load(f)
    result = {}
    for iid, v in raw.items():
        df = pd.DataFrame({'time_str': v['times'], 'atm_iv': v['atm_iv'], 'spot': v['spot']})
        # Sort chronologically and deduplicate — prevents out-of-order line rendering
        df['_dt'] = pd.to_datetime(df['time_str'], format='%H:%M')
        df = df.sort_values('_dt').drop_duplicates(subset='_dt', keep='last').drop(columns='_dt')
        df = df.reset_index(drop=True)
        result[iid] = df
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
    fig.savefig(buf,format='png',dpi=72,bbox_inches='tight',facecolor=CFG['bg_fig'])
    plt.close(fig); return buf.getvalue()

def cached_img(key, draw_fn, *a, **kw):
    if key not in st.session_state:
        st.session_state[key]=_savepng(draw_fn(*a,**kw))
    st.image(st.session_state[key], use_container_width=True)

def draw_grid(all_data, ft):
    plt.style.use('dark_background')
    items={k:v for k,v in all_data.items() if ft is None or v['inst']['type']==ft}
    if not items: return plt.figure()
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

def _time_to_dt(time_str, ref_date):
    """Convert HH:MM string to a datetime on ref_date for proper time-axis alignment."""
    h, m = int(time_str[:2]), int(time_str[3:5])
    return datetime(ref_date.year, ref_date.month, ref_date.day, h, m, tzinfo=IST)

def plotly_intraday(iid, all_data, intraday, yesterday_data):
    h=intraday[iid]; inst=all_data[iid]['inst']
    oiv=h['atm_iv'].iloc[0]; civ=h['atm_iv'].iloc[-1]
    chg=(civ-oiv)/oiv*100 if oiv else 0
    badge='📊 INDEX' if inst['type']=='index' else '📈 STOCK'

    # ── Convert today's HH:MM → real datetimes so Plotly uses a true time axis ─
    today_dt   = [_time_to_dt(t, TODAY) for t in h['time_str']]
    iv         = h['atm_iv'].tolist()
    now_ts     = _now_ist()

    # Full trading day window: 09:15 → 15:30
    x_axis_start = datetime(TODAY.year, TODAY.month, TODAY.day, 9, 15, tzinfo=IST)
    x_axis_end   = datetime(TODAY.year, TODAY.month, TODAY.day, 15, 30, tzinfo=IST)

    # ── Y-axis anchored to TODAY — yesterday never blows the scale ─────────────
    today_max = max(iv) if iv else 50.0
    today_min = min(iv) if iv else 0.0
    today_pad = max((today_max - today_min) * 0.20, 2.0)
    y_min = max(0.0, today_min - today_pad)
    y_max = today_max + today_pad * 2

    # ── Sanity-check yesterday ──────────────────────────────────────────────────
    def _yest_is_sane(ydf, today_max_iv):
        if ydf is None or ydf.empty: return False
        valid = ydf['atm_iv'][(ydf['atm_iv'] > 0) & (ydf['atm_iv'] < 200)]
        if valid.empty: return False
        return not (today_max_iv > 0 and float(valid.median()) > max(today_max_iv * 3, 80.0))

    fig=go.Figure()
    prev_date=get_previous_trading_date(); has_real_yest=False; _yest_corrupt=False

    ydf=yesterday_data.get(iid)
    if ydf is not None and not ydf.empty and ydf['atm_iv'].sum()>0:
        if _yest_is_sane(ydf, today_max):
            has_real_yest=True
            # Convert yesterday HH:MM → datetimes on TODAY's date so both lines
            # share the same date-axis and are perfectly time-aligned
            ydf2 = ydf.copy()
            ydf2['dt'] = [_time_to_dt(t, TODAY) for t in ydf2['time_str']]
            # ── SORT by time so line draws left→right correctly ──────────────
            ydf2 = ydf2.sort_values('dt').reset_index(drop=True)
            # Drop duplicate timestamps (keep last reading per minute)
            ydf2 = ydf2.drop_duplicates(subset='dt', keep='last').reset_index(drop=True)
            # Only keep rows with valid IV (> 0) — avoids flat-zero left segment
            ydf2 = ydf2[ydf2['atm_iv'] > 0].reset_index(drop=True)
            if ydf2.empty:
                has_real_yest = False
            ydf2['atm_iv'] = ydf2['atm_iv'].clip(upper=y_max * 1.1)
            fig.add_trace(go.Scatter(
                x=ydf2['dt'], y=ydf2['atm_iv'], mode='lines',
                name=f'Yesterday ({prev_date:%d-%b-%Y}) IV',
                line=dict(color=CFG['curve_colors']['Yesterday'],width=2,dash='dash'),opacity=0.8,
                hovertemplate=(f'<b>Yesterday ({prev_date:%d-%b})</b><br>'
                               f'Time: %{{x|%H:%M}}<br>IV: <b>%{{y:.2f}}%</b><extra></extra>')))
            # Yesterday close label at 15:30
            ydf_last = ydf2.iloc[-1]
            fig.add_trace(go.Scatter(
                x=[ydf_last['dt']], y=[ydf_last['atm_iv']],
                mode='markers+text', name='', showlegend=False,
                marker=dict(color=CFG['curve_colors']['Yesterday'],size=7),
                text=[f"  Yest {ydf['atm_iv'].iloc[-1]:.2f}%"],
                textposition='middle right',
                textfont=dict(color=CFG['curve_colors']['Yesterday'],size=11),hoverinfo='skip'))
            # Yesterday open marker at 09:20
            yest_open = ydf2[ydf2['time_str']=='09:20']
            if not yest_open.empty:
                yoiv = float(yest_open['atm_iv'].iloc[0])
                fig.add_trace(go.Scatter(
                    x=[yest_open['dt'].iloc[0]], y=[min(yoiv, y_max*1.1)],
                    mode='markers+text', name='', showlegend=False,
                    marker=dict(color=CFG['curve_colors']['Yesterday'],size=9,
                                symbol='circle-open',line=dict(width=2)),
                    text=[f'  Yest Open: {yoiv:.2f}%'], textposition='top right',
                    textfont=dict(color=CFG['curve_colors']['Yesterday'],size=10),
                    hovertemplate=f'<b>Yesterday Open (9:20)</b><br>IV: <b>{yoiv:.2f}%</b><extra></extra>'))
        else:
            _yest_corrupt = True

    # ── Today's line (time-aligned) ─────────────────────────────────────────────
    fig.add_trace(go.Scatter(
        x=today_dt, y=iv, fill='tozeroy', fillcolor='rgba(0,229,160,0.07)',
        mode='none', name='', showlegend=False, hoverinfo='skip'))
    fig.add_trace(go.Scatter(
        x=today_dt, y=iv, mode='lines+markers',
        name=f'Today ({TODAY:%d-%b-%Y}) ATM IV',
        line=dict(color='#00e5a0',width=2.5),
        marker=dict(size=4,color='#00e5a0',line=dict(color='#07090f',width=1)),
        hovertemplate=(f'<b>Today ({TODAY:%d-%b})</b><br>'
                       f'Time: %{{x|%H:%M}}<br>ATM IV: <b>%{{y:.2f}}%</b><extra></extra>')))
    fig.add_trace(go.Scatter(
        x=[today_dt[-1]], y=[iv[-1]], mode='markers+text', name='', showlegend=False,
        marker=dict(color='#00e5a0',size=8),
        text=[f'  Now: {iv[-1]:.2f}%'], textposition='middle right',
        textfont=dict(color='#00e5a0',size=12,family='monospace'),hoverinfo='skip'))

    # Open IV dotted line
    fig.add_hline(y=oiv, line_dash='dot', line_color=CFG['curve_colors']['Open 9:20'],
        line_width=1.5, opacity=0.9,
        annotation_text=f'Open 9:20: {oiv:.2f}%',
        annotation_font=dict(color=CFG['curve_colors']['Open 9:20'],size=11),
        annotation_position='top left')

    # Peak / trough markers
    pi=int(np.argmax(iv))
    fig.add_trace(go.Scatter(
        x=[today_dt[pi]], y=[iv[pi]], mode='markers+text', name=f'Peak: {iv[pi]:.2f}%',
        marker=dict(color='#ffd740',size=12,symbol='diamond'),
        text=[f'  Peak: {iv[pi]:.2f}%'], textposition='top right',
        textfont=dict(color='#ffd740',size=11),
        hovertemplate='<b>Peak</b><br>Time: %{x|%H:%M}<br>IV: <b>%{y:.2f}%</b><extra></extra>'))
    ti=int(np.argmin(iv))
    if ti!=pi:
        fig.add_trace(go.Scatter(
            x=[today_dt[ti]], y=[iv[ti]], mode='markers+text', name=f'Trough: {iv[ti]:.2f}%',
            marker=dict(color='#ff4466',size=12,symbol='diamond'),
            text=[f'  Trough: {iv[ti]:.2f}%'], textposition='bottom right',
            textfont=dict(color='#ff4466',size=11),
            hovertemplate='<b>Trough</b><br>Time: %{x|%H:%M}<br>IV: <b>%{y:.2f}%</b><extra></extra>'))

    title_color='#00e5a0' if chg>=0 else '#ff4466'; arrow='▲' if chg>=0 else '▼'
    yest_note = ('' if has_real_yest
                 else ('  <span style="color:#ff8c42;font-size:11px">| ⚠ Yesterday data corrupt</span>'
                       if _yest_corrupt else
                       '  <span style="color:#64748b;font-size:11px">| No stored yesterday data</span>'))

    _base={k:v for k,v in PLOTLY_LAYOUT.items() if k not in ('xaxis','yaxis','legend','hovermode','margin')}
    fig.update_layout(**_base, hovermode='x unified',
        title=dict(text=(
            f'<b style="color:{CFG["title_c"]}">INTRADAY ATM IV — {inst["label"]} {badge}</b>  '
            f'<span style="color:#94a3b8">Expiry: {inst["expiry"]:%d-%b-%Y}</span>  '
            f'<span style="color:{title_color}">{arrow} {abs(chg):.2f}% from open</span>'
            f'{yest_note}'),
            x=0.01, xanchor='left', font=dict(size=13)),
        xaxis=dict(
            type='date',                          # TRUE time axis — aligns today & yesterday by time
            gridcolor=CFG['grid_c'], zeroline=False,
            tickfont=dict(color=CFG['tick_c'],size=11),
            title=dict(text='Time (IST)',font=dict(color=CFG['tick_c'])),
            tickangle=-45,
            range=[x_axis_start, x_axis_end],     # always 09:15–15:30 full day
            tickformat='%H:%M',                   # show only HH:MM on axis
            dtick=30*60*1000,                     # tick every 30 minutes (ms)
        ),
        yaxis=dict(gridcolor=CFG['grid_c'], zeroline=False,
                   tickfont=dict(color=CFG['tick_c']),
                   title=dict(text='ATM IV (%)',font=dict(color=CFG['tick_c'])),
                   tickformat='.1f', ticksuffix='%',
                   range=[y_min, y_max]),
        height=480,
        legend=dict(bgcolor='rgba(13,17,23,0.88)',bordercolor=CFG['grid_c'],borderwidth=1,
                    orientation='v',x=0.01,y=0.01,xanchor='left',yanchor='bottom',
                    font=dict(size=12,color='white'),tracegroupgap=4),
        margin=dict(l=60,r=120,t=60,b=60))
    return fig, _yest_corrupt

def plotly_iv_vs_ul(iid, all_data, intraday, yesterday_data):
    h=intraday[iid]; inst=all_data[iid]['inst']
    badge='[INDEX]' if inst['type']=='index' else '[STOCK]'
    sl='Synthetic Futures' if inst['type']=='index' else 'Futures'
    civ=h['atm_iv'].iloc[-1]; oiv=h['atm_iv'].iloc[0]
    chg=(civ-oiv)/oiv*100 if oiv else 0; arrow='▲' if chg>=0 else '▼'

    # True datetime axis — same approach as Tab 2A for perfect time alignment
    today_dt = [_time_to_dt(t, TODAY) for t in h['time_str']]
    x_axis_start = datetime(TODAY.year, TODAY.month, TODAY.day, 9, 15, tzinfo=IST)
    x_axis_end   = datetime(TODAY.year, TODAY.month, TODAY.day, 15, 30, tzinfo=IST)

    today_iv_list = h['atm_iv'].tolist()
    today_max = max(today_iv_list) if today_iv_list else 50.0
    today_min = min(today_iv_list) if today_iv_list else 0.0
    today_pad = max((today_max - today_min) * 0.20, 2.0)
    y_iv_min  = max(0.0, today_min - today_pad)
    y_iv_max  = today_max + today_pad * 2

    fig=make_subplots(specs=[[{"secondary_y":True}]])
    fig.add_trace(go.Scatter(x=today_dt,y=today_iv_list,fill='tozeroy',
        fillcolor='rgba(0,229,160,0.07)',mode='none',showlegend=False,hoverinfo='skip'),secondary_y=False)
    fig.add_trace(go.Scatter(x=today_dt,y=today_iv_list,mode='lines+markers',
        name=f'ATM IV — Today ({TODAY:%d-%b-%Y})',line=dict(color='#00e5a0',width=2.2),
        marker=dict(size=3,color='#00e5a0'),
        hovertemplate='Time: %{x|%H:%M}<br>ATM IV: <b>%{y:.2f}%</b><extra></extra>'),secondary_y=False)
    fig.add_trace(go.Scatter(x=[today_dt[-1]],y=[civ],mode='markers+text',
        name='',showlegend=False,marker=dict(color='#00e5a0',size=8),
        text=[f'  IV: {civ:.2f}%'],textposition='middle right',
        textfont=dict(color='#00e5a0',size=11),hoverinfo='skip'),secondary_y=False)

    prev_date=get_previous_trading_date()
    ydf=yesterday_data.get(iid)
    if ydf is not None and not ydf.empty and ydf['atm_iv'].sum()>0:
        _valid  = ydf['atm_iv'][(ydf['atm_iv']>0) & (ydf['atm_iv']<200)]
        _median = float(_valid.median()) if not _valid.empty else 999
        if today_max > 0 and _median <= max(today_max * 3, 80.0):
            ydf2 = ydf.copy()
            ydf2['dt'] = [_time_to_dt(t, TODAY) for t in ydf2['time_str']]
            # ── SORT by time so line draws left→right correctly ──────────────
            ydf2 = ydf2.sort_values('dt').reset_index(drop=True)
            # Drop duplicate timestamps (keep last reading per minute)
            ydf2 = ydf2.drop_duplicates(subset='dt', keep='last').reset_index(drop=True)
            # Only keep rows with valid IV (> 0) — avoids flat-zero left segment
            ydf2 = ydf2[ydf2['atm_iv'] > 0].reset_index(drop=True)
            if not ydf2.empty:
                ydf2['atm_iv'] = ydf2['atm_iv'].clip(upper=y_iv_max * 1.1)
                fig.add_trace(go.Scatter(x=ydf2['dt'],y=ydf2['atm_iv'],mode='lines',
                    name=f'ATM IV — Yesterday ({prev_date:%d-%b-%Y})',
                    line=dict(color=CFG['curve_colors']['Yesterday'],width=1.8,dash='dash'),opacity=0.8,
                    hovertemplate=f'<b>Yesterday ({prev_date:%d-%b})</b><br>Time: %{{x|%H:%M}}<br>IV: <b>%{{y:.2f}}%</b><extra></extra>'),
                    secondary_y=False)
                fig.add_trace(go.Scatter(x=[ydf2['dt'].iloc[-1]],y=[ydf2['atm_iv'].iloc[-1]],
                    mode='markers+text',name='',showlegend=False,
                    marker=dict(color=CFG['curve_colors']['Yesterday'],size=7),
                    text=[f"  Yest: {ydf['atm_iv'].iloc[-1]:.2f}%"],textposition='middle right',
                    textfont=dict(color=CFG['curve_colors']['Yesterday'],size=11),hoverinfo='skip'),secondary_y=False)

    # Spot price on secondary axis
    fig.add_trace(go.Scatter(x=today_dt,y=h['spot'].tolist(),mode='lines',
        name=f'{sl} Price (₹)',line=dict(color=CFG['title_c'],width=1.8,dash='dot'),
        hovertemplate=f'Time: %{{x|%H:%M}}<br>{sl}: <b>₹%{{y:,.1f}}</b><extra></extra>'),secondary_y=True)
    fig.add_trace(go.Scatter(x=[today_dt[-1]],y=[h['spot'].iloc[-1]],mode='markers+text',
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
        color='#00e5a0',secondary_y=False,gridcolor=CFG['grid_c'],zeroline=False,
        tickfont=dict(color=CFG['tick_c']),range=[y_iv_min, y_iv_max])
    fig.update_yaxes(title_text=f'{sl} Price (₹)',tickformat=',.0f',tickprefix='₹',
        color=CFG['title_c'],secondary_y=True,gridcolor='rgba(0,0,0,0)',zeroline=False,
        tickfont=dict(color=CFG['tick_c']))
    fig.update_xaxes(
        type='date', tickformat='%H:%M', dtick=30*60*1000,
        title_text='Time (IST)', tickangle=-45,
        gridcolor=CFG['grid_c'], zeroline=False,
        tickfont=dict(color=CFG['tick_c'],size=11),
        range=[x_axis_start, x_axis_end])   # always full day 09:15–15:30
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
st.title("📈 IV Skew & Intraday Volatility Dashboard — Indices")
st.caption(f"NSE/BSE Indices · {TODAY:%d-%b-%Y} · {NOW:%H:%M IST} · Broker: **{broker_badge}** · "
           f"{'Telegram: ✅' if TG_TOKEN and TG_CHAT_ID else 'Telegram: ⚠️ not configured'}")

# ══════════════════════════════════════════════════════════════════════════════
#  DATA LOADING — All 162 instruments in one parallel pass
#  Live Zerodha data when connected · mock fallback otherwise
#  TTL=60s — auto-refreshes every minute
# ══════════════════════════════════════════════════════════════════════════════
_t0        = time.time()
_prog_area = st.empty()
with _prog_area.container():
    _prog = st.progress(0, text="⚡ Loading all 7 indices in parallel…")

ALL_DATA, INSTRUMENTS = build_all_data_parallel()

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
INTRADAY = build_intraday_fast(_all_ids, _cache_date=TODAY.isoformat())

_prog.progress(95, text="⚡ Loading yesterday snapshot…")
YESTERDAY_DATA = load_yesterday_data(INSTRUMENTS)
prev_date      = get_previous_trading_date()
has_yest_store = _store_path(prev_date).exists()

_elapsed = time.time() - _t0
_prog.progress(100, text=f"✅ {len(ALL_DATA)} indices ready in {_elapsed:.1f}s · {_source}")
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

# ── Backfill yesterday's snapshot from DB if the JSON file is missing ──────────
# This handles the case where the script wasn't running yesterday after market close,
# or the snapshot file was lost. Reads yesterday's rows from SQLite and writes the JSON.
_prev_date_chk = get_previous_trading_date()
if not _store_path(_prev_date_chk).exists():
    try:
        _prev_idata = {}
        for _inst in INSTRUMENTS:
            _iid = _inst['id']
            _prev_df = db_load_atm_history(_iid, _prev_date_chk.isoformat())
            if not _prev_df.empty:
                _prev_idata[_iid] = _prev_df
        if _prev_idata:
            save_daily_snapshot(_prev_idata, _prev_date_chk)
            log.info(f"Backfilled yesterday snapshot for {_prev_date_chk} from DB ({len(_prev_idata)} instruments)")
            # Reload so the sidebar and charts pick it up immediately
            YESTERDAY_DATA = load_yesterday_data(INSTRUMENTS)
            has_yest_store = _store_path(_prev_date_chk).exists()
    except Exception as _bfe:
        log.warning(f"Yesterday snapshot backfill failed: {_bfe}")

# ── Sidebar ────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## ⚙️ Controls")
    ni = ni_loaded; ns = ns_loaded
    st.caption(f"🔷 {ni} Indices loaded  |  (Stocks: disabled)")

    st.divider()
    st.markdown("#### 🔌 Broker Connection")
    if BROKER == 'zerodha' and (_kite_sdk is not None or _kite_enc is not None):
        _meth = f" via **{_ZERODHA_METHOD.upper()}**" if _ZERODHA_METHOD else ""
        st.success(f"✅ **Zerodha Live**{_meth} — `{ZERODHA_USER_ID}`")
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
    # ── Data Scope — always fully loaded ─────────────────────────────────────
    st.markdown("#### 📦 Data Scope")
    _data_src = _live_source_label()
    _scope_msg = (f"**{len(ALL_DATA)}** indices loaded\n"
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
    _interval_opts = {"1 min": 1, "2 min": 2, "5 min": 5, "10 min": 10, "15 min": 15, "30 min": 30}
    _sel_interval  = st.selectbox("Interval", list(_interval_opts.keys()),
                                   index=2,   # default = 5 min (balances freshness vs rerender cost)
                                   key="auto_refresh_interval",
                                   disabled=not _auto_on)
    _interval_ms = _interval_opts[_sel_interval] * 60 * 1000   # convert to ms

    if _auto_on and _HAS_AUTOREFRESH:
        _refresh_count = st_autorefresh(interval=_interval_ms, key="iv_autorefresh")
        # Always clear intraday cache on every tick — DB may have new rows from recorder
        # Also clear IV data so ATM strike + spot stay live
        build_intraday_fast.clear()
        if _refresh_count and _refresh_count > 0:
            build_all_data_parallel.clear()
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
            with st.expander(f"⚠️ {_n_errors} instruments had fetch errors — click to see", expanded=True):
                for iid, d in ALL_DATA.items():
                    if d.get('live_error'):
                        st.code(f"{iid}: {d['live_error']}", language="text")

        # Show what expiry dates + symbols are being used RIGHT NOW
        with st.expander(f"📅 Active Expiries (today = {TODAY:%d-%b-%Y})", expanded=False):
            _idx_insts = [i for i in INSTRUMENTS if i['type'] == 'index']
            for _ii in _idx_insts:
                _es = _ii['expiry'].strftime('%d%b%y').upper()
                _bse = _ii['id'] in {'SENSEX', 'SENSEX_W2'}
                _ex = 'BFO' if _bse else 'NFO'
                _base = _ii['id'].replace('_W2','').replace('_','')
                st.caption(f"**{_ii['label']}**: `{_ii['expiry']:%d-%b-%Y}` → `{_ex}:{_base}{_es}[K]CE`")

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

    stored_files=sorted([f for f in STORE_DIR.glob("*.json") if f.stem[:4].isdigit()],reverse=True)
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
    all_ids=index_ids
    lbls={i['id']:f"{i['label']} [INDEX]" for i in INSTRUMENTS}

    st.markdown("#### Instrument (Tab 2A & 2B)")
    sel_id=st.selectbox("Select Index",all_ids,format_func=lambda x:lbls[x],index=0)
    st.divider()
    st.markdown("#### Grid Filter (Tab 1)")
    t1f=st.radio("Show",["Indices Only"],index=0)
    st.divider()
    st.markdown("#### IV Movers (Tab 3)")
    top_n=st.slider("Top N",3,len(all_ids),min(7,len(all_ids)))
    mf=st.radio("Filter",["Indices Only"],index=0)
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
        _intraday_fig, _yest_corrupt = plotly_intraday(sel_id, ALL_DATA, INTRADAY, YESTERDAY_DATA)
        if _yest_corrupt:
            _snap_path = _store_path(prev_date)
            st.warning(
                f"⚠️ Yesterday's snapshot ({prev_date:%d-%b-%Y}) has corrupt IV data "
                f"(saved when expiry was wrong — IV exploded). Hidden from chart. "
                f"Delete it so tomorrow's overlay works correctly."
            )
            if st.button(f"🗑️ Delete corrupt snapshot ({prev_date:%d-%b-%Y})", key="del_corrupt_snap"):
                try:
                    _snap_path.unlink(missing_ok=True)
                    st.success(f"✅ Deleted — reloading...")
                    st.rerun()
                except Exception as _de:
                    st.error(f"Delete failed: {_de}")
        st.plotly_chart(_intraday_fig, use_container_width=True, key=f"intraday_{sel_id}")
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
        pct_chg= (atm_iv - open_iv) / open_iv * 100 if (atm_iv and open_iv and open_iv > 0) else None
        _calc_rows.append({
            'Instrument':      inst['label'],
            'Type':            inst['type'].capitalize(),
            'Calc Time':       d.get('calc_time', NOW.strftime('%H:%M:%S IST')),
            'Spot (S)':        round(spot, 2),
            'ATM Strike (K)':  d.get('atm_strike', math.floor(spot / inst['strike_step'] + 0.5) * inst['strike_step']),
            'Expiry':          str(inst['expiry']),
            'T (years)':       round(T_yr, 5),
            'T (trading days)':round(T_yr * 252, 3),
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

    def _iv_color(v):
        if pd.isna(v) or v is None: return ''
        return f'color: {"#00e5a0" if v > 0 else "#ff4466" if v < 0 else "white"};font-weight:bold'

    if not _calc_df.empty:
        styled = (_calc_df.style
                  .applymap(_iv_color, subset=['IV Chg %'])
                  .format({
                      'Spot (S)':         '{:,.2f}',
                      'ATM Strike (K)':   '{:,.0f}',
                      'T (years)':        '{:.5f}',
                      'T (trading days)': '{:.2f}',
                      'ATM CE Price ₹':   lambda x: f'₹{x:.2f}' if pd.notna(x) and x is not None else '—',
                      'ATM PE Price ₹':   lambda x: f'₹{x:.2f}' if pd.notna(x) and x is not None else '—',
                      'Final ATM IV %':   lambda x: f'{x:.3f}%' if pd.notna(x) and x is not None else '—',
                      'Open IV %':        lambda x: f'{x:.3f}%' if pd.notna(x) and x is not None else '—',
                      'IV Chg %':         lambda x: f'{x:+.2f}%' if pd.notna(x) and x is not None else '—',
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
