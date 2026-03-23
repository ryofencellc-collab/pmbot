import sys
import os
import json
sys.path.insert(0, os.path.dirname(__file__))

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
import uvicorn

from data.database import get_conn, init_db

app = FastAPI(title="PolyEdge", version="3.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"],
                   allow_methods=["*"], allow_headers=["*"])


@app.on_event("startup")
def startup():
    init_db()
    print("[SERVER] Ready")


@app.get("/health")
def health():
    conn = get_conn()
    c    = conn.cursor()
    tables = {}
    for t in ["markets", "price_snapshots", "wu_temps", "paper_trades"]:
        try:
            c.execute(f"SELECT COUNT(*) FROM {t}")
            row = c.fetchone()
            tables[t] = row[0] if row else 0
        except Exception:
            tables[t] = 0
    conn.close()
    return {"status": "ok", "tables": tables}


@app.get("/test")
def run_test():
    import warnings
    warnings.filterwarnings('ignore')
    from scheduler import run_system_test
    return run_system_test()


@app.get("/signals")
def get_signals():
    try:
        from strategy.signals import scan_signals
        from datetime import datetime, timezone
        today        = datetime.now(timezone.utc).strftime('%Y-%m-%d')
        signals, log = scan_signals(today)
        return {"signals": signals, "log": log, "date": today}
    except Exception as e:
        return {"signals": [], "log": str(e), "date": ""}


@app.post("/morning")
def morning_session():
    from strategy.paper_trade import run_morning_session
    trades, log = run_morning_session()
    return {"trades": trades, "log": log}


@app.post("/evening")
def evening_session():
    from strategy.paper_trade import run_evening_session
    log = run_evening_session()
    return {"log": log}


@app.get("/performance")
def get_performance():
    from strategy.paper_trade import get_performance
    return get_performance()


@app.get("/trades")
def get_trades():
    conn = get_conn()
    c    = conn.cursor()
    c.execute("""SELECT trade_date, city, question, entry_price, size,
                        noaa_forecast_f, predicted_range, outcome, pnl
                 FROM paper_trades ORDER BY trade_date DESC, id DESC""")
    trades = [dict(r) for r in c.fetchall()]
    conn.close()
    return trades


@app.get("/logs")
def get_logs():
    conn = get_conn()
    c    = conn.cursor()
    try:
        c.execute("""SELECT session_type, logged_at, content
                     FROM session_logs ORDER BY id DESC LIMIT 10""")
        logs = [dict(r) for r in c.fetchall()]
    except Exception:
        logs = []
    conn.close()
    return logs


@app.get("/", response_class=HTMLResponse)
def dashboard():
    return """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>PolyEdge</title>
<style>
  * { margin: 0; padding: 0; box-sizing: border-box; }
  body { background: #0a0a0f; color: #e8e8f0; font-family: -apple-system, sans-serif; }
  .header { background: #0f0f1a; border-bottom: 1px solid #1a1a2e; padding: 16px 20px;
            display: flex; align-items: center; justify-content: space-between; }
  .logo { font-size: 18px; font-weight: 700; }
  .logo span { color: #00ff88; }
  .status-bar { display: flex; gap: 8px; flex-wrap: wrap; padding: 12px 20px;
                background: #0f0f1a; border-bottom: 1px solid #1a1a2e; }
  .status-item { display: flex; align-items: center; gap: 6px; font-size: 12px;
                 padding: 4px 10px; background: #1a1a2e; border-radius: 20px; }
  .dot { width: 8px; height: 8px; border-radius: 50%; }
  .dot-green { background: #00ff88; box-shadow: 0 0 6px #00ff88; }
  .dot-red   { background: #ff4444; box-shadow: 0 0 6px #ff4444; }
  .dot-gray  { background: #666; }
  .tabs { display: flex; border-bottom: 1px solid #1a1a2e; }
  .tab { padding: 12px 20px; font-size: 13px; cursor: pointer; border-bottom: 2px solid transparent;
         color: #666; transition: all 0.2s; }
  .tab.active { color: #00ff88; border-bottom-color: #00ff88; }
  .content { padding: 20px; max-width: 800px; margin: 0 auto; }
  .card { background: #0f0f1a; border: 1px solid #1a1a2e; border-radius: 12px;
          padding: 16px; margin-bottom: 16px; }
  .card-title { font-size: 11px; color: #666; letter-spacing: 0.1em;
                text-transform: uppercase; margin-bottom: 12px; }
  .stats { display: grid; grid-template-columns: repeat(3, 1fr); gap: 12px; }
  .stat { text-align: center; }
  .stat-val { font-size: 24px; font-weight: 700; font-family: monospace; color: #00ff88; }
  .stat-label { font-size: 11px; color: #666; margin-top: 4px; }
  .signal { border: 1px solid #1a2e1a; background: #0d1a0d; border-radius: 10px;
            padding: 14px; margin-bottom: 12px; }
  .signal-city { font-size: 16px; font-weight: 700; color: #00ff88; }
  .signal-q { font-size: 13px; color: #aaa; margin: 6px 0; }
  .signal-row { display: flex; gap: 16px; font-size: 12px; margin-top: 8px; flex-wrap: wrap; }
  .signal-tag { background: #1a2e1a; padding: 3px 8px; border-radius: 4px; color: #00ff88; }
  .trade { border-bottom: 1px solid #1a1a2e; padding: 12px 0; }
  .trade:last-child { border-bottom: none; }
  .trade-header { display: flex; justify-content: space-between; align-items: center; }
  .trade-city { font-weight: 600; font-size: 14px; }
  .trade-pnl { font-family: monospace; font-weight: 700; }
  .win { color: #00ff88; }
  .loss { color: #ff4444; }
  .pending { color: #ffaa00; }
  .trade-detail { font-size: 12px; color: #666; margin-top: 4px; }
  .btn { background: #00ff88; color: #000; border: none; padding: 12px 24px;
         border-radius: 8px; font-weight: 700; font-size: 14px; cursor: pointer;
         width: 100%; margin-bottom: 12px; }
  .btn-secondary { background: #1a1a2e; color: #00ff88; border: 1px solid #00ff88; }
  .btn:active { opacity: 0.8; }
  .test-result { border-radius: 8px; padding: 12px; margin-bottom: 8px;
                 font-size: 13px; display: flex; justify-content: space-between; }
  .test-ok   { background: #0d1a0d; border: 1px solid #1a3a1a; }
  .test-err  { background: #1a0d0d; border: 1px solid #3a1a1a; }
  .log-box { background: #060608; border: 1px solid #1a1a2e; border-radius: 8px;
             padding: 12px; font-family: monospace; font-size: 11px; color: #888;
             white-space: pre-wrap; max-height: 300px; overflow-y: auto; }
  .empty { text-align: center; color: #444; padding: 40px 20px; font-size: 14px; }
  .equity { font-size: 32px; font-weight: 700; font-family: monospace; }
  .equity.positive { color: #00ff88; }
  .alert { background: #1a0d0d; border: 1px solid #ff4444; border-radius: 8px;
           padding: 12px; margin-bottom: 12px; font-size: 13px; color: #ff8888; }
</style>
</head>
<body>
<div class="header">
  <div class="logo">POLY<span>EDGE</span></div>
  <div id="time" style="font-size:12px;color:#666"></div>
</div>
<div class="status-bar" id="statusBar">
  <div class="status-item"><div class="dot dot-gray"></div>Loading...</div>
</div>
<div class="tabs">
  <div class="tab active" onclick="showTab('signals')">Signals</div>
  <div class="tab" onclick="showTab('trades')">Trades</div>
  <div class="tab" onclick="showTab('performance')">Performance</div>
  <div class="tab" onclick="showTab('system')">System</div>
</div>
<div class="content">
  <div id="tab-signals">
    <div class="card">
      <div class="card-title">Today's Signals</div>
      <div id="signalsContent"><div class="empty">Loading signals...</div></div>
    </div>
    <button class="btn" onclick="runMorning()">▶ Run Morning Session Now</button>
    <button class="btn btn-secondary" onclick="loadSignals()">↻ Refresh Signals</button>
  </div>
  <div id="tab-trades" style="display:none">
    <div class="card">
      <div class="card-title">Trade Log — Every Decision Explained</div>
      <div id="tradesContent"><div class="empty">Loading trades...</div></div>
    </div>
    <button class="btn btn-secondary" onclick="runEvening()">▶ Check Today's Outcomes</button>
  </div>
  <div id="tab-performance" style="display:none">
    <div class="card">
      <div class="card-title">Paper Trading Results</div>
      <div id="perfContent"><div class="empty">Loading performance...</div></div>
    </div>
  </div>
  <div id="tab-system" style="display:none">
    <div class="card">
      <div class="card-title">System Status</div>
      <button class="btn" onclick="runTest()">Run Full System Test</button>
      <div id="testResults"></div>
    </div>
    <div class="card">
      <div class="card-title">Session Logs</div>
      <div id="logsContent"><div class="empty">No logs yet</div></div>
    </div>
  </div>
</div>
<script>
const API = '';
setInterval(() => {
  document.getElementById('time').textContent =
    new Date().toLocaleTimeString('en-US', {hour:'2-digit',minute:'2-digit'});
}, 1000);
function showTab(name) {
  document.querySelectorAll('.tab').forEach((t,i) => {
    t.classList.toggle('active', ['signals','trades','performance','system'][i] === name);
  });
  ['signals','trades','performance','system'].forEach(t => {
    document.getElementById('tab-'+t).style.display = t === name ? 'block' : 'none';
  });
  if (name === 'signals')     loadSignals();
  if (name === 'trades')      loadTrades();
  if (name === 'performance') loadPerformance();
  if (name === 'system')      loadLogs();
}
async function loadStatus() {
  try {
    const r = await fetch(API + '/health');
    const d = await r.json();
    document.getElementById('statusBar').innerHTML = `
      <div class="status-item"><div class="dot dot-green"></div>Server Online</div>
      <div class="status-item"><div class="dot dot-green"></div>DB: ${d.tables.markets?.toLocaleString() || 0} markets</div>
      <div class="status-item"><div class="dot dot-green"></div>${d.tables.paper_trades || 0} paper trades</div>
    `;
  } catch(e) {
    document.getElementById('statusBar').innerHTML =
      '<div class="status-item"><div class="dot dot-red"></div>Server offline</div>';
  }
}
async function loadSignals() {
  document.getElementById('signalsContent').innerHTML = '<div class="empty">Scanning markets...</div>';
  try {
    const r = await fetch(API + '/signals');
    const d = await r.json();
    const el = document.getElementById('signalsContent');
    if (!d.signals || d.signals.length === 0) {
      el.innerHTML = '<div class="empty">No signals found for today.<br>Markets may not be open yet or no edge detected.</div>';
      return;
    }
    el.innerHTML = d.signals.map(s => `
      <div class="signal">
        <div class="signal-city">🌡 ${s.city}</div>
        <div class="signal-q">${s.question}</div>
        <div class="signal-row">
          <span class="signal-tag">Forecast: ${s.forecast_f}°F</span>
          <span class="signal-tag">Entry: ${(s.entry_price*100).toFixed(1)}¢</span>
          <span class="signal-tag">Edge: ${(s.edge*100).toFixed(0)}%</span>
          <span class="signal-tag">EV: ${s.ev.toFixed(1)}x</span>
        </div>
        <div class="trade-detail" style="margin-top:8px">${s.reasoning}</div>
      </div>
    `).join('');
  } catch(e) {
    document.getElementById('signalsContent').innerHTML =
      '<div class="alert">Error: ' + e.message + '</div>';
  }
}
async function loadTrades() {
  try {
    const r = await fetch(API + '/trades');
    const d = await r.json();
    const el = document.getElementById('tradesContent');
    if (!d.length) {
      el.innerHTML = '<div class="empty">No trades yet. Run morning session to start.</div>';
      return;
    }
    el.innerHTML = d.map(t => {
      const cls = t.outcome === 'Yes' ? 'win' : t.outcome === 'No' ? 'loss' : 'pending';
      const icon = t.outcome === 'Yes' ? '✅' : t.outcome === 'No' ? '❌' : '⏳';
      const pnl = t.pnl ? (t.pnl > 0 ? '+$'+t.pnl.toFixed(2) : '-$'+Math.abs(t.pnl).toFixed(2)) : 'Pending';
      return `
        <div class="trade">
          <div class="trade-header">
            <div class="trade-city">${icon} ${t.city} — ${t.trade_date}</div>
            <div class="trade-pnl ${cls}">${pnl}</div>
          </div>
          <div class="trade-detail">${t.question}</div>
          <div class="trade-detail">Entry: ${(t.entry_price*100).toFixed(1)}¢ | Size: $${t.size?.toFixed(2)} | Forecast: ${t.noaa_forecast_f}°F | Range: ${t.predicted_range}</div>
        </div>`;
    }).join('');
  } catch(e) {
    document.getElementById('tradesContent').innerHTML = '<div class="alert">Error: ' + e.message + '</div>';
  }
}
async function loadPerformance() {
  try {
    const r = await fetch(API + '/performance');
    const d = await r.json();
    const el = document.getElementById('perfContent');
    el.innerHTML = `
      <div style="text-align:center;padding:20px 0">
        <div class="equity positive">$${d.final_capital?.toFixed(2) || '100.00'}</div>
        <div style="color:#666;font-size:12px;margin-top:4px">Starting from $100.00</div>
      </div>
      <div class="stats">
        <div class="stat"><div class="stat-val">${d.win_rate || 0}%</div><div class="stat-label">Win Rate</div></div>
        <div class="stat"><div class="stat-val">${d.total_bets || 0}</div><div class="stat-label">Total Bets</div></div>
        <div class="stat"><div class="stat-val">${d.roi || 0}%</div><div class="stat-label">ROI</div></div>
      </div>`;
  } catch(e) {
    document.getElementById('perfContent').innerHTML = '<div class="alert">Error: ' + e.message + '</div>';
  }
}
async function runTest() {
  document.getElementById('testResults').innerHTML = '<div class="empty">Running tests...</div>';
  try {
    const r = await fetch(API + '/test');
    const d = await r.json();
    document.getElementById('testResults').innerHTML = Object.entries(d).map(([k,v]) => `
      <div class="test-result ${v.status === 'ok' ? 'test-ok' : 'test-err'}">
        <span>${v.status === 'ok' ? '✅' : '❌'} ${k}</span>
        <span style="color:#666;font-size:12px">${v.message}</span>
      </div>`).join('');
  } catch(e) {
    document.getElementById('testResults').innerHTML = '<div class="alert">Test failed: ' + e.message + '</div>';
  }
}
async function loadLogs() {
  try {
    const r = await fetch(API + '/logs');
    const d = await r.json();
    const el = document.getElementById('logsContent');
    if (!d.length) { el.innerHTML = '<div class="empty">No logs yet</div>'; return; }
    el.innerHTML = d.map(l => `
      <div style="margin-bottom:12px">
        <div style="font-size:11px;color:#666;margin-bottom:4px">${l.session_type} — ${l.logged_at}</div>
        <div class="log-box">${l.content}</div>
      </div>`).join('');
  } catch(e) {}
}
async function runMorning() {
  event.target.textContent = 'Running...';
  event.target.disabled = true;
  try {
    const r = await fetch(API + '/morning', {method:'POST'});
    const d = await r.json();
    alert('Morning session complete. ' + (d.trades?.length || 0) + ' trades placed.');
    loadSignals(); loadStatus();
  } catch(e) { alert('Error: ' + e.message); }
  event.target.textContent = '▶ Run Morning Session Now';
  event.target.disabled = false;
}
async function runEvening() {
  event.target.textContent = 'Checking...';
  event.target.disabled = true;
  try {
    await fetch(API + '/evening', {method:'POST'});
    alert('Evening session complete.');
    loadTrades(); loadStatus();
  } catch(e) { alert('Error: ' + e.message); }
  event.target.textContent = '▶ Check Today\'s Outcomes';
  event.target.disabled = false;
}
loadStatus();
loadSignals();
setInterval(loadStatus, 30000);
</script>
</body>
</html>"""


if __name__ == "__main__":
    uvicorn.run("server:app", host="0.0.0.0", port=8000, reload=False)
