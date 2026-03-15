import json
import os
import sqlite3
import subprocess
import sys
import time
import traceback
from collections import deque
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

ROOT = Path(__file__).resolve().parents[2]
CONTROL_DIR = ROOT / "control"
BOTS_CONTROL = CONTROL_DIR / "scripts" / "bots_control.ps1"
STATE_REGISTRY = ROOT / "state" / "active_runs.json"
SNAPSHOT_SHARED = ROOT / "runtime" / "snapshots" / "polymarket_shared_snapshot.json"
SNAPSHOT_BTC5 = ROOT / "runtime" / "snapshots" / "btc_5min_clob_snapshot.json"
SCANNER_LOG = ROOT / "polymarket_scanner" / "shared_scanner.log"
BTC5_SCANNER_LOG = ROOT / "polymarket_scanner" / "btc_5min_clob_scanner.log"
BOT5_DIR = ROOT / "polymarket_paper_bot_5min"
HOST = os.getenv("X_DASHBOARD_HOST", "127.0.0.1")
PORT = int(os.getenv("X_DASHBOARD_PORT", "8765"))
WINDOWS_NO_WINDOW = getattr(subprocess, "CREATE_NO_WINDOW", 0)
try:
    import psutil
except Exception:
    psutil = None

# Telegram notification config (from scanner .env or environment)
def _load_telegram_config():
    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()
    if token and chat_id:
        return token, chat_id
    env_path = ROOT / "polymarket_scanner" / ".env"
    if env_path.exists():
        try:
            for line in env_path.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if line.startswith("TELEGRAM_BOT_TOKEN="):
                    token = line.split("=", 1)[1].strip()
                elif line.startswith("TELEGRAM_CHAT_ID="):
                    chat_id = line.split("=", 1)[1].strip()
        except Exception:
            pass
    return token, chat_id

TG_TOKEN, TG_CHAT_ID = _load_telegram_config()
_BOT5_LIVE_BALANCE_CACHE = {"ts": 0.0, "value": None}


def _load_bot5_env() -> dict:
    values = {}
    env_path = BOT5_DIR / ".env"
    if not env_path.exists():
        return values
    try:
        for raw_line in env_path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            values[key.strip()] = value.strip()
    except Exception:
        pass
    return values


def _env_float(values: dict, key: str, default: float) -> float:
    try:
        return float(values.get(key, default))
    except Exception:
        return float(default)


def _env_int(values: dict, key: str, default: int) -> int:
    try:
        return int(values.get(key, default))
    except Exception:
        return int(default)


def bot5_initial_balance() -> float:
    raw = os.getenv("INITIAL_BALANCE", "").strip()
    if raw:
        try:
            return float(raw)
        except Exception:
            pass
    env_values = _load_bot5_env()
    if env_values:
        return _env_float(env_values, "INITIAL_BALANCE", 1000.0)
    return 1000.0


def bot5_strategy_profile() -> dict:
    env_values = _load_bot5_env()
    return {
        "initial_balance": _env_float(env_values, "INITIAL_BALANCE", 1000.0),
        "trading_mode": env_values.get("TRADING_MODE", "paper"),
        "position_size_usd": _env_float(env_values, "POSITION_SIZE_USD", 5.0),
        "entry_min_price": _env_float(env_values, "ENTRY_MIN_PRICE", 0.20),
        "entry_max_price": _env_float(env_values, "ENTRY_MAX_PRICE", 0.80),
        "min_entry_sec": _env_int(env_values, "MIN_ENTRY_SEC", 20),
        "entry_cutoff_sec": _env_int(env_values, "ENTRY_CUTOFF_SEC", 270),
        "max_entry_spread": _env_float(env_values, "MAX_ENTRY_SPREAD", 0.03),
        "momentum_min_pct": _env_float(env_values, "MOMENTUM_MIN_PCT", 0.10),
        "fallback_signal_min_pct": _env_float(env_values, "FALLBACK_SIGNAL_MIN_PCT", 0.08),
        "short_momentum_tolerance_pct": _env_float(env_values, "SHORT_MOMENTUM_TOLERANCE_PCT", 0.02),
        "fallback_entry_max_price": _env_float(env_values, "FALLBACK_ENTRY_MAX_PRICE", 0.42),
        "fallback_max_entry_spread": _env_float(env_values, "FALLBACK_MAX_ENTRY_SPREAD", 0.015),
        "fallback_min_quote_stable_passes": _env_int(env_values, "FALLBACK_MIN_QUOTE_STABLE_PASSES", 3),
        "max_loss_usd_per_trade": _env_float(env_values, "MAX_LOSS_USD_PER_TRADE", 2.0),
        "stop_loss_pct": _env_float(env_values, "STOP_LOSS_PCT", 0.50),
        "take_profit_pct": _env_float(env_values, "TAKE_PROFIT_PCT", 0.30),
        "principal_take_multiplier": _env_float(env_values, "PRINCIPAL_TAKE_MULTIPLIER", 2.0),
        "runner_final_target_price": _env_float(env_values, "RUNNER_FINAL_TARGET_PRICE", 0.97),
        "runner_trailing_stop_pct": _env_float(env_values, "RUNNER_TRAILING_STOP_PCT", 0.18),
        "daily_loss_limit_usd": _env_float(env_values, "DAILY_LOSS_LIMIT_USD", 10.0),
        "max_total_drawdown_usd": _env_float(env_values, "MAX_TOTAL_DRAWDOWN_USD", 20.0),
        "live_min_closed_trades": _env_int(env_values, "LIVE_MIN_CLOSED_TRADES", 30),
        "live_min_net_pnl_usd": _env_float(env_values, "LIVE_MIN_NET_PNL_USD", 5.0),
        "live_eval_lookback_trades": _env_int(env_values, "LIVE_EVAL_LOOKBACK_TRADES", 20),
        "live_min_profit_factor": _env_float(env_values, "LIVE_MIN_PROFIT_FACTOR", 1.20),
        "live_max_force_exit_rate": _env_float(env_values, "LIVE_MAX_FORCE_EXIT_RATE", 0.45),
        "live_max_stop_rate": _env_float(env_values, "LIVE_MAX_STOP_RATE", 0.30),
        "live_min_recent_pnl_usd": _env_float(env_values, "LIVE_MIN_RECENT_PNL_USD", 1.0),
        "health_min_closed_trades": _env_int(env_values, "HEALTH_MIN_CLOSED_TRADES", 12),
        "health_lookback_trades": _env_int(env_values, "HEALTH_LOOKBACK_TRADES", 12),
        "health_min_profit_factor": _env_float(env_values, "HEALTH_MIN_PROFIT_FACTOR", 1.05),
        "health_max_force_exit_rate": _env_float(env_values, "HEALTH_MAX_FORCE_EXIT_RATE", 0.55),
        "health_max_stop_rate": _env_float(env_values, "HEALTH_MAX_STOP_RATE", 0.35),
        "health_min_recent_pnl_usd": _env_float(env_values, "HEALTH_MIN_RECENT_PNL_USD", 0.0),
        "min_quote_stable_passes": _env_int(env_values, "MIN_QUOTE_STABLE_PASSES", 2),
        "max_quote_jump_pct": _env_float(env_values, "MAX_QUOTE_JUMP_PCT", 0.05),
        "loss_cooldown_trigger": _env_int(env_values, "CONSECUTIVE_LOSS_COOLDOWN_TRIGGER", 3),
        "loss_cooldown_slots": _env_int(env_values, "LOSS_COOLDOWN_SLOTS", 2),
        "trade_notifications_enabled": env_values.get("TRADE_NOTIFICATIONS_ENABLED", "1").strip() not in {"0", "false", "False", "no", "off"},
    }


def bot5_live_available_balance() -> float | None:
    strategy = bot5_strategy_profile()
    if str(strategy.get("trading_mode", "paper")).lower() != "live":
        return None

    env_values = _load_bot5_env()
    try:
        from common.clob_client import ClobClientManager

        mgr = ClobClientManager(
            private_key=env_values.get("POLY_PRIVATE_KEY", "").strip(),
            api_key=env_values.get("POLY_API_KEY", "").strip(),
            api_secret=env_values.get("POLY_API_SECRET", "").strip(),
            api_passphrase=env_values.get("POLY_API_PASSPHRASE", "").strip(),
            funder_address=env_values.get("POLY_FUNDER_ADDRESS", "").strip(),
            signature_type=int(env_values.get("POLY_SIGNATURE_TYPE", "0") or "0"),
            bot_label="dashboard",
        )
        value = mgr.get_collateral_balance()
    except Exception:
        value = None
    return value


def bot5_live_available_balance_strict() -> float | None:
    """
    Dashboard process context'inden bagimsiz bir sekilde,
    temiz Python process'i ile Polymarket collateral balance oku.
    """
    try:
        python_exe = Path(sys.executable)
        if python_exe.name.lower() == "pythonw.exe":
            python_exe = python_exe.with_name("python.exe")
        cmd = [
            str(python_exe),
            "-c",
            (
                "from control.dashboard.server import _load_bot5_env; "
                "from common.clob_client import ClobClientManager; "
                "env=_load_bot5_env(); "
                "mgr=ClobClientManager("
                "private_key=env.get('POLY_PRIVATE_KEY','').strip(),"
                "api_key=env.get('POLY_API_KEY','').strip(),"
                "api_secret=env.get('POLY_API_SECRET','').strip(),"
                "api_passphrase=env.get('POLY_API_PASSPHRASE','').strip(),"
                "funder_address=env.get('POLY_FUNDER_ADDRESS','').strip(),"
                "signature_type=int(env.get('POLY_SIGNATURE_TYPE','0') or '0'),"
                "bot_label='dashboard'); "
                "v = mgr.get_collateral_balance(); "
                "print('None' if v is None else v)"
            ),
        ]
        p = subprocess.run(cmd, cwd=str(ROOT), capture_output=True, text=True, timeout=20, creationflags=WINDOWS_NO_WINDOW)
        raw = (p.stdout or "").strip()
        if not raw or raw == "None":
            return None
        return float(raw)
    except Exception:
        return None


def notify_telegram(text: str):
    """Send a short notification to Telegram."""
    if not TG_TOKEN or not TG_CHAT_ID:
        return
    try:
        import urllib.request
        payload = json.dumps({"chat_id": TG_CHAT_ID, "text": text, "parse_mode": "HTML"}).encode("utf-8")
        req = urllib.request.Request(
            f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
            data=payload,
            headers={"Content-Type": "application/json"},
        )
        urllib.request.urlopen(req, timeout=10)
    except Exception:
        pass
BTC_PRICE_HISTORY = deque(maxlen=300)

HTML = r"""
<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <title>5MIN BTC Dashboard</title>
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <style>
    :root { --bg:#0b1020; --card:#121a30; --card2:#0f172a; --line:#253253; --text:#e8ecf3; --muted:#9db0d1; --ok:#4ade80; --warn:#fbbf24; --bad:#f87171; }
    body { font-family: Arial, sans-serif; background:var(--bg); color:var(--text); margin:0; }
    .wrap { max-width: 1440px; margin: 0 auto; padding: 16px; }
    h1 { margin: 0; font-size: 24px; }
    .sub { margin:6px 0 16px; color:var(--muted); font-size:13px; }
    .grid { display:grid; grid-template-columns: repeat(12, 1fr); gap: 12px; }
    .card { background:var(--card); border:1px solid var(--line); border-radius:14px; padding:14px; box-shadow:0 6px 20px rgba(0,0,0,0.18); }
    .span-3 { grid-column: span 3; } .span-4 { grid-column: span 4; } .span-6 { grid-column: span 6; } .span-8 { grid-column: span 8; } .span-12 { grid-column: span 12; }
    .muted { color:var(--muted); font-size:12px; }
    .big { font-size:28px; font-weight:700; }
    .mid { font-size:20px; font-weight:700; }
    .hero { font-size:26px; font-weight:800; line-height:1.2; }
    .ok { color:var(--ok); } .warn { color:var(--warn); } .bad { color:var(--bad); }
    .pill { display:inline-block; padding:4px 9px; border-radius:999px; font-size:12px; font-weight:700; background:#243252; }
    .kpis { display:grid; grid-template-columns: repeat(4,1fr); gap:10px; }
    .kpi { background:var(--card2); border:1px solid #1e2a44; border-radius:10px; padding:12px; }
    .label { font-size:12px; color:var(--muted); margin-bottom:6px; }
    .value { font-size:24px; font-weight:700; }
    .row { display:flex; gap:8px; flex-wrap:wrap; align-items:center; }
    .stack { display:flex; flex-direction:column; gap:8px; }
    .split { display:grid; grid-template-columns: 1fr 1fr; gap:10px; }
    .kv { display:grid; grid-template-columns: 120px 1fr; gap:6px 10px; font-size:13px; }
    .control { background:var(--card2); border:1px solid #1e2a44; border-radius:10px; padding:12px; }
    table { width:100%; border-collapse: collapse; }
    th, td { padding:9px 8px; border-bottom:1px solid #22304c; text-align:left; font-size:13px; vertical-align:top; }
    th { color:var(--muted); font-weight:600; position:sticky; top:0; background:var(--card); }
    .table-wrap { max-height:340px; overflow:auto; }
    button { background:#2a6df4; color:white; border:none; border-radius:8px; padding:8px 10px; cursor:pointer; margin-right:6px; }
    button.stop { background:#ef4444; } button.restart { background:#f59e0b; }
    pre { white-space: pre-wrap; word-break: break-word; max-height: 280px; overflow:auto; background:#0a1223; padding:12px; border-radius:10px; margin:0; }
    .mono { font-family: Consolas, monospace; }
    @media (max-width: 1100px) { .kpis { grid-template-columns: repeat(2,1fr); } .split { grid-template-columns: 1fr; } }
    @media (max-width: 900px) { .span-3,.span-4,.span-6,.span-8,.span-12 { grid-column: span 12; } .kpis { grid-template-columns: 1fr 1fr; } }
  </style>
</head>
<body>
<div class="wrap">
  <h1>5MIN BTC Dashboard</h1>
  <div class="sub">Live monitor for the BTC 5MIN scanner and bot</div>
  <div class="grid" id="app"></div>
</div>
<script>
async function j(url, opts) { const r = await fetch(url, opts); return await r.json(); }
function esc(s){ return String(s ?? '').replaceAll('&','&amp;').replaceAll('<','&lt;').replaceAll('>','&gt;'); }
function fmtNum(v, digits=2){ const n = Number(v); return Number.isFinite(n) ? n.toFixed(digits) : esc(v ?? '-'); }
function fmtTimer(v){ const n = Number(v); if(!Number.isFinite(n) || n < 0) return '-'; const m = Math.floor(n / 60); const s = Math.floor(n % 60); return `${String(m).padStart(2,'0')}:${String(s).padStart(2,'0')}`; }
function pctText(v){ const n = Number(v); if(!Number.isFinite(n)) return ''; const sign = n > 0 ? '+' : ''; return `${sign}${n.toFixed(2)}%`; }
function toneClass(v){ const n = Number(v); if(!Number.isFinite(n) || n === 0) return ''; return n > 0 ? 'ok' : 'bad'; }
function moneyNum(v){ const n = Number(String(v ?? '').replace(/[^0-9.-]/g, '')); return Number.isFinite(n) ? n : null; }
function statusClass(s){ s=(s||'').toLowerCase(); if(['active','running','ok'].includes(s)) return 'ok'; if(['draining','stale','warn','idle'].includes(s)) return 'warn'; return 'bad'; }
async function control(bot, action){
  const res = await j('/api/control', {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({bot, action})});
  alert(res.message || JSON.stringify(res));
  load();
}
let dataCache = null;
async function resetWallet(){
  const label = dataCache?.reset_wallet_display || '$1,000.00';
  if(!confirm(`Wallet will be reset to ${label} and old logs will be cleared. Continue?`)) return;
  const res = await j('/api/reset-wallet', {method:'POST', headers:{'Content-Type':'application/json'}, body: '{}'});
  alert(res.message || JSON.stringify(res));
  load();
}
function botCard(name, b, title){
  return `<div class="control"><div class="label">${esc(title || 'Bot')}</div><div class="row" style="justify-content:space-between;align-items:flex-start"><div><div class="mid">${esc(name)}</div><div class="muted">mode=${esc(b.trading_mode)} | hb_age=${esc(b.heartbeat_age_sec)}s</div></div><div class="pill ${statusClass(b.status)}">${esc(b.status)}</div></div><div class="row" style="margin-top:10px"><button onclick="control('${name}','start')">Start</button><button class="stop" onclick="control('${name}','stop')">Stop</button><button class="restart" onclick="control('${name}','restart')">Restart</button></div></div>`;
}
async function load(){
  const data = await j('/api/state');
  dataCache = data;
  const app = document.getElementById('app');
  const p = data.price || {};
  const s = data.strategy || {};
  const currentBalancePct = (() => {
    const cur = moneyNum(data.wallet_display);
    const start = moneyNum(data.start_balance_display);
    if(cur === null || start === null || start === 0) return null;
    return ((cur - start) / start) * 100;
  })();
  const trades = (data.trades || []).map(t => `<tr><td>${esc(t.ts)}</td><td>${esc(t.event)}</td><td>${esc(t.outcome)}</td><td>${fmtNum(t.price)}</td><td class="${toneClass(t.pnl_usd)}">${fmtNum(t.pnl_usd)}</td><td>${esc(t.reason)}</td></tr>`).join('');
  const entryStatusClass = statusClass(data.entry_state?.status || '');
  const botLifecycle = data.bots?.['5min']?.status || '-';
  const botStateText = botLifecycle === 'ACTIVE'
    ? (data.entry_state?.status === 'Paused' ? `Paused: ${data.entry_state?.reason || 'waiting'}` : 'Scanning for trades')
    : 'Stopped';
  const botAlerts = (data.bot_alerts || []).map(x => `<div>${esc(x)}</div>`).join('');
  const scannerAlerts = (data.scanner_alerts || []).map(x => `<div>${esc(x)}</div>`).join('');
  app.innerHTML = `
    <div class="card span-12">
      <div class="kpis">
        <div class="kpi"><div class="label">Bot Status</div><div class="value ${statusClass(botLifecycle)}">${esc(botLifecycle)}</div><div class="muted">${esc(botStateText)}</div></div>
        <div class="kpi"><div class="label">Current Balance</div><div class="value ${toneClass(currentBalancePct)}">${esc(data.wallet_display ?? '-')}</div><div class="muted ${toneClass(currentBalancePct)}">${pctText(currentBalancePct)}</div></div>
        <div class="kpi"><div class="label">Time Left</div><div class="value">${fmtTimer(data.slot_time_left_sec)}</div></div>
        <div class="kpi"><div class="label">Snapshot Age</div><div class="value">${fmtTimer(data.btc5_snapshot_age_sec)}</div></div>
        <div class="kpi"><div class="label">BTC 5m %</div><div class="value ${toneClass(data.btc_spot?.change_5m_pct)}">${esc(data.btc_spot?.change_5m_pct ?? '-')}%</div></div>
        <div class="kpi"><div class="label">Start Balance</div><div class="value">${esc(data.start_balance_display ?? '-')}</div></div>
        <div class="kpi"><div class="label">Position Size</div><div class="value">${esc(data.position_size_display ?? '-')}</div></div>
        <div class="kpi"><div class="label">Open PnL</div><div class="value ${toneClass(data.open_position?.pnl_pct)}">${esc(data.open_position?.display ?? 'No position')}</div></div>
      </div>
    </div>

    <div class="card span-12">
      <div class="label">Recent Trades</div>
      <div class="table-wrap"><table><thead><tr><th>Time</th><th>Event</th><th>Outcome</th><th>Price</th><th>PnL</th><th>Reason</th></tr></thead><tbody>${trades}</tbody></table></div>
    </div>

    <div class="card span-8">
      <div class="label">Live Market</div>
      <div class="hero">${esc(data.market_display?.title || 'Waiting for market data')}</div>
      <div class="muted">${esc(data.market_display?.subtitle || 'The scanner has not published a readable market yet.')}</div>
      <div style="height:10px"></div>
      <div class="kpis">
        <div class="kpi"><div class="label">BTC Spot</div><div class="value">${esc(data.btc_spot?.price ?? '-')}</div></div>
        <div class="kpi"><div class="label">YES Mid</div><div class="value">${fmtNum(p.yes_mid)}</div></div>
        <div class="kpi"><div class="label">NO Mid</div><div class="value">${fmtNum(p.no_mid)}</div></div>
        <div class="kpi"><div class="label">YES Spread</div><div class="value">${fmtNum(p.spread_yes)}</div></div>
        <div class="kpi"><div class="label">NO Spread</div><div class="value">${fmtNum(p.spread_no)}</div></div>
        <div class="kpi"><div class="label">Pending</div><div class="value">${esc(data.pending_count)}</div></div>
        <div class="kpi"><div class="label">Quote Ready</div><div class="value">${esc(p.book_valid)}</div></div>
      </div>
    </div>

    <div class="card span-4">
      <div class="control">
        <div class="label">Bot Summary</div>
        <div class="kv">
          <div>Mode</div><div>${esc(s.trading_mode || '-')}</div>
          <div>Entry</div><div><span class="pill ${entryStatusClass}">${esc(data.entry_state?.status || '-')}</span></div>
          <div>Bot Status</div><div><span class="pill ${statusClass(botLifecycle)}">${esc(botLifecycle)}</span></div>
          <div>Scanner Status</div><div><span class="pill ${statusClass(data.bots?.btc5scan?.status || '')}">${esc(data.bots?.btc5scan?.status || '-')}</span></div>
          <div>Last Event</div><div>${esc(data.last_event.event || '-')}</div>
          <div>Last Reason</div><div>${esc(data.last_event.reason || '-')}</div>
          <div>Closed Trades</div><div>${esc(data.closed_count)}</div>
        </div>
      </div>
      <div style="height:10px"></div>
      <div class="control">
        <div class="label">Manual Controls</div>
        <div class="muted">Manage the 5MIN bot and BTC scanner manually from here.</div>
        <div style="height:8px"></div>
        <div class="label">5MIN Bot</div>
        <div class="row">
          <button onclick="control('5min','start')" style="flex:1">Start</button>
          <button class="stop" onclick="control('5min','stop')" style="flex:1">Stop</button>
        </div>
        <div style="height:8px"></div>
        <div class="label">BTC Scanner</div>
        <div class="row">
          <button onclick="control('btc5scan','start')" style="flex:1">Start</button>
          <button class="stop" onclick="control('btc5scan','stop')" style="flex:1">Stop</button>
        </div>
        <div style="height:8px"></div>
        <button style="background:#dc2626;width:100%" onclick="resetWallet()">Reset Wallet (${esc(data.reset_wallet_display || '$1,000.00')})</button>
      </div>
    </div>

    <div class="card span-4">
      <div class="label">Strategy</div>
      <div class="kv">
        <div>Signal</div><div>Enter when the last 2 closed 1m candles match</div>
        <div>Stop Loss</div><div>${fmtNum((s.stop_loss_pct ?? 0) * 100, 0)}%</div>
        <div>Take Profit</div><div>${fmtNum((s.take_profit_pct ?? 0) * 100, 0)}%</div>
        <div>Exit</div><div>Full close</div>
        <div>Entry Band</div><div>${esc(s.entry_min_price)} - ${esc(s.entry_max_price)}</div>
        <div>Max Spread</div><div>${esc(s.max_entry_spread)}</div>
        <div>Cooldown</div><div>${esc(s.loss_cooldown_trigger)} losses -> ${esc(s.loss_cooldown_slots)} slots</div>
      </div>
    </div>

    <div class="card span-4">
      <div class="label">Bot Alerts</div>
      <pre>${botAlerts}</pre>
    </div>

    <div class="card span-4">
      <div class="label">Scanner Alerts</div>
      <pre>${scannerAlerts}</pre>
    </div>
  `;
}
load(); setInterval(load, 5000);
</script>
</body>
</html>
"""


def read_json(path: Path):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def file_age(path: Path):
    try:
        return int(time.time() - path.stat().st_mtime)
    except Exception:
        return None


def tail(path: Path, lines: int = 60):
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            data = f.readlines()[-lines:]
        return "".join(data)
    except Exception:
        return ""


def tail_errors(path: Path, lines: int = 200):
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            data = f.readlines()[-lines:]
        keep = []
        for line in data:
            low = line.lower()
            if "error" in low or "traceback" in low or "exception" in low or "failed" in low or "crash" in low:
                keep.append(line)
        return "".join(keep[-40:]) if keep else "No errors."
    except Exception:
        return "No errors."


def compact_alerts(path: Path, lines: int = 200, limit: int = 8):
    try:
        data = path.read_text(encoding="utf-8", errors="ignore").splitlines()[-lines:]
        keep = []
        for line in data:
            low = line.lower()
            if (
                "error" in low
                or "failed" in low
                or "entry paused" in low
                or "kill switch" in low
                or "settlement pending" in low
                or "runtime error" in low
            ):
                raw = line.strip()
                time_part = ""
                msg_part = raw
                if raw.startswith("[") and "]" in raw:
                    stamp = raw[1:raw.find("]")]
                    if " " in stamp:
                        time_part = stamp.split(" ", 1)[1]
                    else:
                        time_part = stamp
                    msg_part = raw[raw.find("]") + 1 :].strip()
                if "|" in msg_part:
                    msg_part = msg_part.split("|", 1)[1].strip()
                if time_part:
                    keep.append(f"{time_part} | {msg_part}")
                else:
                    keep.append(msg_part)
        if keep:
            deduped = []
            for item in keep:
                if not deduped or deduped[-1].split(" | ", 1)[-1] != item.split(" | ", 1)[-1]:
                    deduped.append(item)
            keep = deduped
        if not keep:
            return ["No important alerts."]
        selected = keep[-limit:]
        selected.reverse()
        return [_humanize_alert_line(item) for item in selected]
    except Exception:
        return ["No important alerts."]


def _humanize_alert_line(line: str) -> str:
    raw = (line or "").strip()
    if not raw:
        return raw

    if " | " in raw:
        prefix, msg = raw.split(" | ", 1)
    else:
        prefix, msg = "", raw

    low = msg.lower()

    replacements = [
        ("Kill switch aktif -- yeni pozisyon acilamaz.", "Kill switch is active. New positions are blocked."),
        ("MARKET_BUY: tum retry'lar basarisiz", "Market buy failed after all retries."),
        ("MARKET_SELL: tum retry'lar basarisiz", "Market sell failed after all retries."),
        ("CLOB order basarisiz:", "CLOB order failed:"),
        ("CLOSE failed", "Close failed"),
        ("OPEN rejected", "Open rejected"),
        ("loss cooldown active for 1 slot(s)", "Loss cooldown is active for 1 slot."),
        ("loss cooldown active for 2 slot(s)", "Loss cooldown is active for 2 slots."),
        ("Runtime Error:", "Runtime error:"),
        ("No valid book", "Quote validation failed"),
        ("tum retry'lar basarisiz", "all retries failed"),
        ("yeni pozisyon acilamaz", "new positions cannot be opened"),
    ]
    for old, new in replacements:
        msg = msg.replace(old, new)

    low = msg.lower()
    if "entry paused" in low and "loss cooldown" in low:
        msg = "Entry paused. Loss cooldown is active."
    elif "open rejected" in low and "kill switch" in low:
        msg = "Open rejected. Kill switch is active, so no new position was opened."
    elif "close failed" in low and "market_sell" in low:
        msg = "Close failed. The sell order could not be fully filled after all retries."
    elif "clob order failed" in low and "market_buy" in low:
        msg = "Open failed. The buy order could not be fully filled after all retries."
    elif "runtime error:" in low and "access is denied" in low and ".tmp" in low:
        msg = "Snapshot write failed because the snapshot file was locked by another process."

    return f"{prefix} | {msg}" if prefix else msg


def bot5_entry_state(run_dir: Path):
    state = {"status": "Active", "reason": ""}
    if not run_dir:
        return state
    log_path = run_dir / "bot.log"
    if not log_path.exists():
        return state
    try:
        data = log_path.read_text(encoding="utf-8", errors="ignore").splitlines()[-200:]
        for line in reversed(data):
            raw = line.strip()
            if "ENTRY PAUSED |" in raw:
                reason = raw.split("ENTRY PAUSED |", 1)[1].strip()
                return {"status": "Paused", "reason": reason}
            if "OPEN " in raw and "OPEN rejected" not in raw:
                return {"status": "Active", "reason": "Looking for next setup"}
        return state
    except Exception:
        return state


def latest_run_dir(bot_dir: Path):
    runs = sorted(bot_dir.glob("runs/Run_*"), key=lambda p: p.stat().st_mtime, reverse=True)
    for run in runs:
        if run.is_dir():
            return run
    return None


def parse_iso_age(ts: str):
    if not ts:
        return None
    try:
        dt = datetime.fromisoformat(ts)
        return int((datetime.now() - dt).total_seconds())
    except Exception:
        return None


def is_process_alive(pattern: str) -> bool:
    """Check if a python process matching the pattern is running without spawning shell windows."""
    if not psutil:
        return False
    try:
        import re

        rx = re.compile(pattern)
        for proc in psutil.process_iter(["name", "cmdline"]):
            try:
                name = (proc.info.get("name") or "").lower()
                if "python" not in name:
                    continue
                cmdline = proc.info.get("cmdline") or []
                cmd = " ".join(str(part) for part in cmdline if part)
                if cmd and rx.search(cmd):
                    return True
            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                continue
    except Exception:
        return False
    return False


def update_registry_status(bot_key: str, status: str):
    """Update a bot's status in active_runs.json."""
    try:
        data = read_json(STATE_REGISTRY) or {"bots": {}}
        if bot_key not in data.get("bots", {}):
            data.setdefault("bots", {})[bot_key] = {}
        data["bots"][bot_key]["status"] = status
        data["updated_at"] = datetime.now().isoformat(timespec="seconds")
        with open(STATE_REGISTRY, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
    except Exception:
        pass


def get_registry_state():
    data = read_json(STATE_REGISTRY) or {"bots": {}}
    bots = {}

    # --- 5min bot: registry + live PID verification ---
    b = data.get("bots", {}).get("5min", {})
    registry_status = b.get("status", "unknown")
    # Live check: verify process actually exists
    bot5_alive = is_process_alive(r"polymarket_paper_bot_5min\\\\runs|polymarket_paper_bot_5min\\runs|polymarket_paper_bot_5min.*manager\\.py")
    if registry_status == "ACTIVE" and not bot5_alive:
        registry_status = "STOPPED"
        update_registry_status("5min", "STOPPED")
    elif registry_status in ("STOPPED", "CLOSED", "unknown") and bot5_alive:
        registry_status = "ACTIVE"
        update_registry_status("5min", "ACTIVE")
    bots["5min"] = {
        "status": registry_status,
        "active_run_id": b.get("active_run_id"),
        "trading_mode": b.get("trading_mode", "paper"),
        "heartbeat_age_sec": parse_iso_age(b.get("last_heartbeat_at")),
    }

    # --- btc5scan: log age + live PID verification ---
    scanner_age = file_age(BTC5_SCANNER_LOG)
    if scanner_age is None:
        scanner_age = file_age(SCANNER_LOG)
    btc5_alive = is_process_alive(r"btc_5min_clob_scanner\\.py")
    if btc5_alive:
        scanner_status = "ACTIVE"
    else:
        scanner_status = "STOPPED"
    bots["btc5scan"] = {
        "status": scanner_status,
        "active_run_id": None,
        "trading_mode": "n/a",
        "heartbeat_age_sec": scanner_age,
    }
    return bots


def find_bot5_run():
    reg = read_json(STATE_REGISTRY) or {}
    run_dir = reg.get("bots", {}).get("5min", {}).get("run_dir")
    p = Path(run_dir) if run_dir else None
    if p and p.exists():
        return p
    return latest_run_dir(BOT5_DIR)


def get_balance(run_dir: Path):
    try:
        return (run_dir / "CURRENT_BALANCE.txt").read_text(encoding="utf-8").strip()
    except Exception:
        return None


def fetch_trade_rows(run_dir: Path, limit: int = 20):
    rows = []
    counts = {"open": 0, "closed": 0, "pending": 0}
    last_event = {"event": None, "price": None, "reason": None}
    if not run_dir:
        return rows, counts, last_event
    db_path = run_dir / "paper_trades.db"
    if not db_path.exists():
        return rows, counts, last_event
    try:
        conn = sqlite3.connect(str(db_path))
        counts["open"] = conn.execute("SELECT COUNT(*) FROM paper_positions WHERE status='OPEN'").fetchone()[0]
        counts["closed"] = conn.execute("SELECT COUNT(*) FROM paper_positions WHERE status='CLOSED'").fetchone()[0]
        counts["pending"] = conn.execute("SELECT COUNT(*) FROM paper_positions WHERE status='PENDING_SETTLEMENT'").fetchone()[0]
        q = "SELECT ts, event, market_slug, outcome, price, pnl_usd, reason FROM signal_journal ORDER BY id DESC LIMIT ?"
        for ts, event, slug, outcome, price, pnl, reason in conn.execute(q, (limit,)).fetchall():
            rows.append({
                "ts": datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC") if ts else None,
                "event": event,
                "market_slug": slug,
                "outcome": outcome,
                "price": price,
                "pnl_usd": pnl,
                "reason": reason,
            })
        if rows:
            last_event = {
                "event": rows[0]["event"] if isinstance(rows[0], dict) else None,
                "price": rows[0]["price"] if isinstance(rows[0], dict) else None,
                "reason": rows[0]["reason"] if isinstance(rows[0], dict) else None,
            }
        conn.close()
    except Exception:
        pass
    return rows, counts, last_event


def fetch_open_position_pnl(run_dir: Path, price: dict):
    result = {"display": "No position", "pnl_usd": None, "pnl_pct": None}
    if not run_dir:
        return result
    db_path = run_dir / "paper_trades.db"
    if not db_path.exists():
        return result
    try:
        conn = sqlite3.connect(str(db_path))
        row = conn.execute(
            "SELECT market_slug, outcome, token_id, entry_price, size_usd "
            "FROM paper_positions WHERE status='OPEN' ORDER BY id DESC LIMIT 1"
        ).fetchone()
        conn.close()
        if not row:
            return result

        market_slug, outcome, token_id, entry_price, size_usd = row
        current_price = None
        if price.get("market_slug") == market_slug:
            if token_id and token_id == price.get("yes_token_id"):
                current_price = price.get("yes_mid")
            elif token_id and token_id == price.get("no_token_id"):
                current_price = price.get("no_mid")
            elif outcome == "Up":
                current_price = price.get("yes_mid")
            else:
                current_price = price.get("no_mid")

        if current_price is None or not entry_price:
            result["display"] = "Position open"
            return result

        shares = float(size_usd) / float(entry_price)
        pnl_usd = (float(current_price) - float(entry_price)) * shares
        pnl_pct = ((float(current_price) - float(entry_price)) / float(entry_price)) * 100.0
        result["pnl_usd"] = pnl_usd
        result["pnl_pct"] = pnl_pct
        result["display"] = f"${pnl_usd:,.2f}"
        return result
    except Exception:
        return result


def _close_position_locally(
    conn: sqlite3.Connection,
    pos_id: int,
    market_slug: str,
    outcome: str,
    exit_price: float,
    pnl_usd: float,
    pnl_pct: float,
    reason: str,
):
    now_ts = int(time.time())
    try:
        conn.execute(
            "UPDATE paper_positions SET closed_ts=?, exit_price=?, pnl_usd=?, pnl_pct=?, "
            "close_reason=?, status='CLOSED', trading_mode='live', fill_price=? WHERE id=?",
            (now_ts, exit_price, pnl_usd, pnl_pct, reason, exit_price, pos_id),
        )
    except sqlite3.OperationalError:
        conn.execute(
            "UPDATE paper_positions SET closed_ts=?, exit_price=?, pnl_usd=?, pnl_pct=?, "
            "close_reason=?, status='CLOSED' WHERE id=?",
            (now_ts, exit_price, pnl_usd, pnl_pct, reason, pos_id),
        )
    try:
        conn.execute(
            "INSERT INTO signal_journal (ts, event, market_slug, outcome, price, pnl_usd, reason) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (now_ts, "CLOSE", market_slug, outcome, exit_price, pnl_usd, reason),
        )
    except Exception:
        pass


def close_5min_live_positions() -> tuple[bool, str]:
    run_dir = find_bot5_run()
    if not run_dir:
        return True, "No active run."
    db_path = run_dir / "paper_trades.db"
    if not db_path.exists():
        return True, "No trade database."

    try:
        conn = sqlite3.connect(str(db_path))
        rows = conn.execute(
            "SELECT id, market_slug, question, outcome, token_id, entry_price, size_usd "
            "FROM paper_positions WHERE status='OPEN' ORDER BY id"
        ).fetchall()
    except Exception as e:
        return False, f"Failed to read open positions: {e}"

    if not rows:
        try:
            conn.close()
        except Exception:
            pass
        return True, "No open positions."

    env_values = _load_bot5_env()
    try:
        from common.clob_client import ClobClientManager

        mgr = ClobClientManager(
            private_key=env_values.get("POLY_PRIVATE_KEY", "").strip(),
            api_key=env_values.get("POLY_API_KEY", "").strip(),
            api_secret=env_values.get("POLY_API_SECRET", "").strip(),
            api_passphrase=env_values.get("POLY_API_PASSPHRASE", "").strip(),
            funder_address=env_values.get("POLY_FUNDER_ADDRESS", "").strip(),
            signature_type=int(env_values.get("POLY_SIGNATURE_TYPE", "0") or "0"),
            bot_label="dashboard",
        )
    except Exception as e:
        return False, f"Failed to init live client: {e}"

    price = snapshot_price()
    closed = 0
    skipped = 0
    for pos_id, market_slug, question, outcome, token_id, entry_price, size_usd in rows:
        shares = mgr.get_conditional_token_balance(token_id) if token_id else None
        if not shares or shares <= 0:
            skipped += 1
            continue
        if mgr.token_has_orderbook(token_id) is False:
            skipped += 1
            continue

        exit_mid = float(entry_price or 0.5)
        if price.get("market_slug") == market_slug:
            if outcome == "Up":
                exit_mid = float(price.get("yes_mid") or exit_mid)
            else:
                exit_mid = float(price.get("no_mid") or exit_mid)
        worst = max(0.01, min(0.99, exit_mid - 0.02))
        resp = mgr.market_sell(token_id=token_id, shares=float(shares), worst_price=worst, tick_size=0.01)
        if resp.success:
            shares_float = float(size_usd or 0.0) / float(entry_price or 1.0)
            pnl_usd = (exit_mid - float(entry_price or 0.0)) * shares_float
            pnl_pct = ((exit_mid - float(entry_price or 0.0)) / float(entry_price or 1.0)) * 100.0
            _close_position_locally(
                conn=conn,
                pos_id=int(pos_id),
                market_slug=str(market_slug or ""),
                outcome=str(outcome or ""),
                exit_price=float(exit_mid),
                pnl_usd=float(pnl_usd),
                pnl_pct=float(pnl_pct),
                reason="DASHBOARD_STOP",
            )
            closed += 1
        else:
            try:
                conn.close()
            except Exception:
                pass
            return False, f"Live close failed for {question[:32]}: {resp.error}"

    try:
        conn.commit()
        conn.close()
    except Exception:
        pass
    return True, f"Closed {closed} open position(s), skipped {skipped}."


def humanize_scan_line(line: str):
    raw = line.strip()
    ts = None
    if raw.startswith("[") and "]" in raw:
        ts = raw[1:raw.find("]")]
        raw = raw[raw.find("]") + 1 :].strip()
    if "|" in raw:
        raw = raw.split("|", 1)[1].strip()

    status = "info"
    title = raw
    details = ""

    if raw.startswith("SKIP no_valid_book"):
        status = "warn"
        title = "No valid book"
        rest = raw.replace("SKIP no_valid_book |", "", 1).strip()
        parts = [p.strip() for p in rest.split(";") if p.strip()]
        nice = []
        for p in parts:
            if ":yes=" in p and "|no=" in p:
                slug, info = p.split(":yes=", 1)
                yes_part, no_part = info.split("|no=", 1)
                market_label = "Current market"
                if "|pick=ok_next_slot" in no_part:
                    market_label = "Next market"
                no_part = no_part.replace("|pick=ok_next_slot", "")
                no_part = no_part.replace("|pick=ok", "")
                yes_text = yes_part.replace("spread_invalid", "spread looks invalid").replace("bid_missing", "bid missing").replace("ask_missing", "ask missing").replace("empty_book", "empty book")
                no_text = no_part.replace("spread_invalid", "spread looks invalid").replace("bid_missing", "bid missing").replace("ask_missing", "ask missing").replace("empty_book", "empty book")
                nice.append(f"{market_label}: YES side -> {yes_text}; NO side -> {no_text}")
            else:
                nice.append(p)
        details = " | ".join(nice)
    elif raw.startswith("OK |"):
        status = "ok"
        title = "Valid snapshot"
        detail = raw.replace("OK |", "", 1).strip()
        if "|" in detail:
            parts = [p.strip() for p in detail.split("|") if p.strip()]
            kept = []
            for p in parts:
                if p.startswith("slug="):
                    continue
                if p.startswith("source="):
                    continue
                kept.append(p)
            details = " | ".join(kept)
        else:
            details = detail
    elif "scanner started" in raw.lower():
        status = "ok"
        title = "Scanner started"
    elif "stale lock cleaned" in raw.lower():
        status = "info"
        title = "Stale lock cleaned"
        details = raw.split(":", 1)[-1].strip() if ":" in raw else ""
    else:
        details = raw

    return {"ts": ts, "status": status, "title": title, "details": details, "summary": raw}


def parse_scan_events():
    events = []
    for path, kind in [(SCANNER_LOG, "shared_scanner"), (BTC5_SCANNER_LOG, "btc5_scanner")]:
        text = tail(path, 20)
        for line in text.splitlines()[-10:]:
            line = line.strip()
            if not line:
                continue
            ev = humanize_scan_line(line)
            ev["kind"] = kind
            events.append(ev)
    return events[-20:][::-1]


def geoblock_status():
    try:
        import requests
        r = requests.get("https://polymarket.com/api/geoblock", timeout=10)
        return r.json()
    except Exception as e:
        return {"blocked": None, "country": None, "ip": None, "error": str(e)}


def btc_spot_state():
    try:
        import requests
        r = requests.get("https://api.binance.com/api/v3/ticker/price", params={"symbol": "BTCUSDT"}, timeout=10)
        data = r.json()
        price = float(data.get("price"))
        change_5m = None
        # 5 dakikalik degisimi local memory yerine Binance 1m kline verisinden hesapla.
        # Boylece dashboard restart olsa bile kart bos kalmaz.
        k = requests.get(
            "https://api.binance.com/api/v3/klines",
            params={"symbol": "BTCUSDT", "interval": "1m", "limit": 6},
            timeout=10,
        )
        k_data = k.json()
        if isinstance(k_data, list) and len(k_data) >= 6:
            ref_price = float(k_data[0][1])  # 5 dakika onceki mumun open fiyati
            if ref_price > 0:
                change_5m = round(((price - ref_price) / ref_price) * 100.0, 3)
        now_ts = int(time.time())
        BTC_PRICE_HISTORY.append((now_ts, price))
        return {"price": round(price, 2), "change_5m_pct": change_5m, "samples": len(BTC_PRICE_HISTORY)}
    except Exception as e:
        return {"price": None, "change_5m_pct": None, "error": str(e)}


def format_market_display(price: dict):
    slug = price.get("market_slug")
    slot_ts = price.get("slot_ts")
    if not slug or not slot_ts:
        return {
            "title": "Waiting for market data",
            "subtitle": "No active BTC 5MIN market snapshot yet.",
            "time_window": None,
        }
    try:
        start = datetime.fromtimestamp(int(slot_ts), tz=timezone.utc)
        end = datetime.fromtimestamp(int(slot_ts) + 300, tz=timezone.utc)
        title = "BTC Up or Down - 5 Minute Market"
        subtitle = f"{start.strftime('%H:%M:%S')} to {end.strftime('%H:%M:%S')} UTC"
        return {
            "title": title,
            "subtitle": subtitle,
            "time_window": subtitle,
        }
    except Exception:
        return {
            "title": "BTC Up or Down - 5 Minute Market",
            "subtitle": slug,
            "time_window": str(slot_ts),
        }


def slot_time_left_sec(price: dict):
    slot_ts = price.get("slot_ts")
    if not slot_ts:
        return None
    try:
        remaining = (int(slot_ts) + 300) - int(time.time())
        return max(0, remaining)
    except Exception:
        return None


def snapshot_price():
    d = read_json(SNAPSHOT_BTC5)
    if not d:
        return {
            "source": None,
            "market_slug": None,
            "slot_ts": None,
            "yes_mid": None,
            "no_mid": None,
            "yes_bid": None,
            "yes_ask": None,
            "no_bid": None,
            "no_ask": None,
            "spread_yes": None,
            "spread_no": None,
            "book_valid": False,
        }
    return {
        "source": d.get("source"),
        "market_slug": d.get("market_slug"),
        "slot_ts": d.get("slot_ts"),
        "yes_token_id": d.get("yes_token_id"),
        "no_token_id": d.get("no_token_id"),
        "yes_mid": d.get("yes_mid"),
        "no_mid": d.get("no_mid"),
        "yes_bid": d.get("yes_bid"),
        "yes_ask": d.get("yes_ask"),
        "no_bid": d.get("no_bid"),
        "no_ask": d.get("no_ask"),
        "spread_yes": d.get("spread_yes"),
        "spread_no": d.get("spread_no"),
        "book_valid": d.get("book_valid"),
    }


def reset_5min_wallet():
    """Stop 5min bot, create fresh run with configured initial balance, clear old logs."""
    start_balance = bot5_initial_balance()
    # 1. Stop the bot first
    ok, msg = control_bot("5min", "stop")
    if not ok:
        return False, f"Stop failed: {msg}"

    bot_dir = ROOT / "polymarket_paper_bot_5min"
    runs_dir = bot_dir / "runs"

    # 2. Mark current run as GAME_OVER
    reg = read_json(STATE_REGISTRY) or {}
    current_run = reg.get("bots", {}).get("5min", {}).get("run_dir")
    if current_run and Path(current_run).exists():
        go_file = Path(current_run) / "GAME_OVER.txt"
        if not go_file.exists():
            go_file.write_text("Status: WALLET_RESET", encoding="utf-8")

    # 3. Create new run directory
    ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    new_run = runs_dir / f"Run_{ts}"
    new_run.mkdir(parents=True, exist_ok=True)
    (new_run / "CURRENT_BALANCE.txt").write_text(str(start_balance), encoding="utf-8")

    # 4. Clean old log files in bot dir
    for log_file in bot_dir.glob("*.log*"):
        try:
            log_file.unlink()
        except Exception:
            pass
    # Clean logs inside runs (keep new run)
    for run_path in runs_dir.iterdir():
        if run_path == new_run or not run_path.is_dir():
            continue
        for lf in run_path.glob("*.log*"):
            try:
                lf.unlink()
            except Exception:
                pass

    # 5. Update registry to point to new run
    try:
        data = read_json(STATE_REGISTRY) or {"bots": {}}
        data.setdefault("bots", {}).setdefault("5min", {})
        data["bots"]["5min"]["status"] = "STOPPED"
        data["bots"]["5min"]["active_run_id"] = f"Run_{ts}"
        data["bots"]["5min"]["run_dir"] = str(new_run)
        data["bots"]["5min"]["last_switch_at"] = datetime.now().isoformat(timespec="seconds")
        data["updated_at"] = datetime.now().isoformat(timespec="seconds")
        with open(STATE_REGISTRY, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
    except Exception:
        pass

    # 6. Start the bot with new run
    start_ok, start_msg = control_bot("5min", "start")
    notify_telegram(f"<b>5MIN Bot</b> - Wallet reset to ${start_balance:,.2f}. New run: Run_{ts}")
    return True, f"Wallet reset to ${start_balance:,.2f}. New run: Run_{ts}. Bot {'started' if start_ok else 'start failed: ' + start_msg}"

    # 7. Notify
    notify_telegram("\U0001f4b0 <b>5MIN Bot</b>  Wallet reset to $1,000. New run: " + f"Run_{ts}")

    return True, f"Wallet reset to $1,000. New run: Run_{ts}. Bot {'started' if start_ok else 'start failed: ' + start_msg}"


def control_bot(bot: str, action: str):
    if bot not in ["5min", "btc5scan"]:
        return False, f"invalid bot: {bot}"
    if action not in ["start", "stop", "restart"]:
        return False, f"invalid action: {action}"
    try:
        pre_message = ""
        if bot == "5min" and action == "stop":
            close_ok, close_msg = close_5min_live_positions()
            if not close_ok:
                return False, close_msg
            pre_message = close_msg.strip()

        cmd = [
            "powershell",
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(BOTS_CONTROL),
            "-Action",
            action,
            "-Bot",
            bot,
        ]
        p = subprocess.run(
            cmd,
            cwd=str(ROOT),
            capture_output=True,
            text=True,
            timeout=60,
            creationflags=WINDOWS_NO_WINDOW,
        )
        msg = (p.stdout or "") + ("\n" + p.stderr if p.stderr else "")
        ok = p.returncode == 0

        # Update registry after control actions
        if ok and bot == "5min":
            if action == "stop":
                update_registry_status("5min", "STOPPED")
            elif action == "start":
                update_registry_status("5min", "ACTIVE")
            elif action == "restart":
                update_registry_status("5min", "ACTIVE")

        # Telegram notification for control actions
        if ok:
            labels = {"5min": "5MIN Bot", "btc5scan": "BTC 5MIN Scanner"}
            icons = {"start": "\u25b6\ufe0f", "stop": "\u23f9\ufe0f", "restart": "\ud83d\udd04"}
            label = labels.get(bot, bot)
            icon = icons.get(action, "")
            notify_telegram(f"{icon} <b>{label}</b>  {action.upper()} via Dashboard")

        combined = msg.strip()
        if pre_message:
            combined = f"{pre_message}\n{combined}".strip()
        return ok, combined
    except Exception as e:
        return False, str(e)


class Handler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        # Hidden/pythonw launches do not guarantee stderr; avoid broken request logging.
        return

    def _json(self, obj, code=200):
        data = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
        self.send_header("Pragma", "no-cache")
        self.send_header("Expires", "0")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _html(self, text, code=200):
        data = text.encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
        self.send_header("Pragma", "no-cache")
        self.send_header("Expires", "0")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        try:
            parsed = urlparse(self.path)
            if parsed.path == "/":
                return self._html(HTML)
            if parsed.path == "/api/state":
                run_dir = find_bot5_run()
                trades, counts, last_event = fetch_trade_rows(run_dir)
                entry_state = bot5_entry_state(run_dir)
                price = snapshot_price()
                open_position = fetch_open_position_pnl(run_dir, price)
                strategy = bot5_strategy_profile()
                balance = get_balance(run_dir) if run_dir else None
                try:
                    bal_num = float(balance) if balance is not None else None
                except Exception:
                    bal_num = None
                configured_balance = float(strategy.get("initial_balance", bot5_initial_balance()))
                live_available = bot5_live_available_balance_strict()
                if live_available is None:
                    live_available = bot5_live_available_balance()
                is_live_mode = str(strategy.get("trading_mode", "paper")).lower() == "live"
                if is_live_mode:
                    current_balance_value = float(live_available) if live_available is not None else None
                else:
                    current_balance_value = float(bal_num) if bal_num is not None else float(configured_balance)
                balance_change_pct = None
                if configured_balance and current_balance_value is not None:
                    balance_change_pct = ((current_balance_value - configured_balance) / configured_balance) * 100.0
                if is_live_mode:
                    # Dashboard'da tek balance kaynagi Polymarket live wallet olsun.
                    effective_balance = float(live_available) if live_available is not None else None
                else:
                    effective_balance = float(bal_num) if bal_num is not None else float(configured_balance)

                wallet_display = f"${effective_balance:,.2f}" if effective_balance is not None else "Unavailable"
                local_equity_display = f"${bal_num:,.2f}" if bal_num is not None else f"${configured_balance:,.2f}"
                state = {
                    "bots": get_registry_state(),
                    "geoblock": geoblock_status(),
                    "btc_spot": btc_spot_state(),
                    "price": price,
                    "market_display": format_market_display(price),
                    "slot_time_left_sec": slot_time_left_sec(price),
                    "shared_snapshot_age_sec": file_age(SNAPSHOT_SHARED),
                    "btc5_snapshot_age_sec": file_age(SNAPSHOT_BTC5),
                    "balance": effective_balance,
                    "balance_change_pct": balance_change_pct,
                    "wallet_display": wallet_display,
                    "local_equity_display": local_equity_display,
                    "start_balance_display": f"${configured_balance:,.2f}",
                    "position_size_display": f"${float(strategy.get('position_size_usd', 0.0)):,.2f}",
                    "reset_wallet_display": f"${configured_balance:,.2f}",
                    "open_count": counts["open"],
                    "pending_count": counts["pending"],
                    "closed_count": counts["closed"],
                    "open_position": open_position,
                    "entry_state": entry_state,
                    "last_event": last_event,
                    "bot_alerts": compact_alerts(run_dir / "bot.log", 250) if run_dir else ["No important alerts."],
                    "scanner_alerts": compact_alerts(BTC5_SCANNER_LOG if BTC5_SCANNER_LOG.exists() else SCANNER_LOG, 250),
                    "trades": trades,
                    "scan_events": parse_scan_events(),
                    "bot5_run_dir": str(run_dir) if run_dir else None,
                    "strategy": strategy,
                    "logs": {
                        "bot5_errors": tail_errors(run_dir / "bot.log", 300) if run_dir else "No errors.",
                        "scanner_errors": tail_errors(BTC5_SCANNER_LOG if BTC5_SCANNER_LOG.exists() else SCANNER_LOG, 300),
                    },
                }
                return self._json(state)
            return self._json({"error": "not found"}, 404)
        except Exception as e:
            return self._json(
                {
                    "error": "dashboard_handler_failed",
                    "message": str(e),
                    "traceback": traceback.format_exc(),
                },
                500,
            )

    def do_POST(self):
        parsed = urlparse(self.path)
        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length) if length else b"{}"
        try:
            body = json.loads(raw.decode("utf-8"))
        except Exception:
            body = {}

        if parsed.path == "/api/control":
            ok, message = control_bot(body.get("bot", ""), body.get("action", ""))
            return self._json({"ok": ok, "message": message}, 200 if ok else 400)

        if parsed.path == "/api/reset-wallet":
            ok, message = reset_5min_wallet()
            return self._json({"ok": ok, "message": message}, 200 if ok else 400)

        return self._json({"error": "not found"}, 404)


def main():
    server = ThreadingHTTPServer((HOST, PORT), Handler)
    print(f"X dashboard listening on http://{HOST}:{PORT}", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
