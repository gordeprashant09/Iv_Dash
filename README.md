# 📈 IV Skew & Intraday Volatility Dashboard — NSE F&O

A real-time **Implied Volatility (IV) dashboard** built with Streamlit for NSE F&O traders. Covers all 7 major indices and 155 F&O stocks with live IV computation, skew charts, intraday overlays, breakout alerts, and Telegram notifications.

---

## 🗂 Files

| File | Description |
|------|-------------|
| `index_iv_dashboard_fixed.py` | Index-focused version — 7 indices + 155 stocks, fixed tab layout |
| `iv_dashboard_v8.py` | Full version (v9) — includes expiry rollover bug fix for holiday-shifted expiries |

---

## ✅ Features

### 📊 Tabs & Charts
| Tab | What it shows |
|-----|---------------|
| **Tab 1 — IV Skew Grid** | Matplotlib grid of IV skew curves for all instruments; Plotly deep-dive for single instrument |
| **Tab 2A — Intraday ATM IV** | ATM IV today vs yesterday with hover; tracks Live / Yesterday / Open 9:20 curves |
| **Tab 2B — IV vs Underlying** | Dual-axis Plotly chart — IV % and spot price on the same timeline |
| **Tab 3 — IV Movers** | Ranked list of biggest IV % changes from open — indices and stocks |
| **Tab 4 — Alerts Log** | Day High / Day Low breakout log with timestamps |
| **Tab 5 — Calc Details** | Full Black-Scholes input/output table — Spot, Strike, T, CE/PE LTP, ATM IV, IV Chg % |

### ⚙️ Engine
- **Black-Scholes IV solver** using Brent's method (`scipy.optimize.brentq`) for fast, accurate IV inversion
- **Weighted ATM IV** — averages the two nearest strikes to spot
- **Parallel computation** via `ThreadPoolExecutor` — all instruments computed simultaneously
- **Auto-refresh** via `streamlit-autorefresh` (configurable interval)

### 🗄️ Storage
- **SQLite database** (`iv_store.db` / `iv_store_v2.db`) with three tables:
  - `iv_skew` — per-strike IV snapshots
  - `atm_iv_history` — intraday ATM IV timeseries
  - `alerts_log` — breakout alert history
- **JSON snapshots** (`iv_data_store/`) — daily persistence for yesterday overlay

### 🔔 Telegram Alerts
- Fires when IV changes more than **5%** (indices) or **10%** (stocks) from open
- Opening 8 minutes skipped to filter noise
- Sends formatted HTML messages with instrument, IV change %, direction
- Manual send button per instrument from Tab 5

### 📡 Broker Support
| Broker | Mode |
|--------|------|
| Zerodha Kite | Live — auto-login via TOTP on startup |
| Fyers | Live — via access token |
| Mock | Fallback — synthetic data with realistic noise + spike simulation |

---

## 🚀 Quick Start

### 1. Install dependencies

```bash
pip install streamlit kiteconnect pyotp streamlit-autorefresh \
            numpy pandas matplotlib scipy plotly requests python-dotenv Pillow
```

> Dependencies are also auto-installed on first run if missing.

### 2. Set credentials

Create a `.env` file in the same directory:

```env
# Zerodha
KITE_API_KEY=your_api_key
KITE_SECRET=your_api_secret
KITE_USER_ID=your_user_id
KITE_PASSWORD=your_password
KITE_TOTP_KEY=your_totp_base32_key
KITE_ACCESS_TOKEN=          # leave blank — filled automatically on login

# Fyers (optional)
FYERS_CLIENT_ID=
FYERS_ACCESS_TOKEN=

# Broker selection
BROKER=zerodha              # or: fyers | mock

# Telegram
TELEGRAM_BOT_TOKEN=your_bot_token
TELEGRAM_CHAT_ID=your_chat_id

# Alert thresholds
ALERT_THRESHOLD_PCT=5.0         # % IV change to alert for indices
ALERT_THRESHOLD_PCT_STOCK=10.0  # % IV change to alert for stocks
ALERT_OPEN_SKIP_MINUTES=8       # skip first N minutes after open
```

### 3. Run the dashboard

```bash
# Index-focused version
streamlit run index_iv_dashboard_fixed.py

# Full version (recommended — includes expiry rollover fix)
streamlit run iv_dashboard_v8.py
```

Open your browser at `http://localhost:8501`

---

## 🏛 Instruments Covered

### Indices (7)
| Index | Exchange | Expiry Cycle |
|-------|----------|--------------|
| Nifty 50 | NSE | Weekly (Thursday) |
| Bank Nifty | NSE | Weekly (Wednesday) |
| Sensex | BSE | Weekly (Friday) |
| Midcap Nifty | NSE | Monthly |
| Fin Nifty | NSE | Weekly (Tuesday) |
| Nifty Next 50 | NSE | Monthly |
| BSE Sensex 50 | BSE | Monthly |

### Stocks
All **155 NSE F&O stocks** from the original instrument list (unchanged).

---

## 🔧 Configuration

Key parameters in the `CFG` dict (top of file):

| Parameter | Default | Description |
|-----------|---------|-------------|
| `risk_free_rate` | `0.0` | Risk-free rate for index options |
| `stock_carry_rate` | `0.065` | Carry rate for stock options |
| `iv_max_valid` | `5.0` | Max valid IV (500%) — filters junk |
| `bps_lo / bps_hi` | `-300 / +300` | Strike range in bps for skew chart |
| `chain_lo / chain_hi` | `-800 / +800` | Strike range for option chain fetch |
| `trading_days_year` | `252` | Used in time-to-expiry calculation |
| `alert_breakout_pct` | `5.0` | IV % change threshold for index alerts |
| `alert_breakout_pct_stock` | `10.0` | IV % change threshold for stock alerts |

---

## 🗃 Data Storage

```
project/
├── iv_data_store/          # JSON snapshots (one file per trading day per instrument)
│   └── YYYY-MM-DD_*.json
├── iv_store.db             # SQLite — index dashboard version
├── iv_store_v2.db          # SQLite — v8/v9 full version
├── index_iv_dashboard_fixed.py
└── iv_dashboard_v8.py
```

---

## 🐛 Bug Fixes (v9 — `iv_dashboard_v8.py`)

**Expiry rollover on holiday-shifted expiry days**

- **Root cause:** `_nmtue` / `_nmthu` used `exp <= from_date`, which caused a shifted expiry (e.g. March 31 holiday → March 30) to be treated as already expired on the expiry day itself, rolling forward to the next month incorrectly.
- **Fix:** Changed to `exp < from_date` — the current shifted expiry is now correctly treated as active on the day it falls.
- **Impact:** Affects any year where the last Tuesday/Thursday of the month is a public holiday and the previous trading day is today.

---

## ⚠️ Notes

- Credentials are read from `.env` first; hardcoded fallbacks in the script are for development only — **replace them or use `.env` in production**.
- The dashboard is designed to run during **NSE market hours (9:15 AM – 3:30 PM IST)**. Outside hours it uses mock/cached data.
- SQLite DB and JSON store paths in `iv_dashboard_v8.py` use absolute paths (`Path(__file__).parent / ...`) to avoid "file not found" errors when launching from a different working directory.

---

## 📦 Dependencies

```
streamlit
streamlit-autorefresh
kiteconnect
pyotp
numpy
pandas
matplotlib
scipy
plotly
requests
Pillow
python-dotenv
```
