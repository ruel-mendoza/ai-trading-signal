import csv
import io
import json
import os
from datetime import datetime, timezone, timedelta
from fastapi import APIRouter, Query, Body
from fastapi.responses import HTMLResponse, StreamingResponse, JSONResponse
from typing import Optional

from trading_engine.database import get_all_signals, get_active_signals, get_api_usage_stats, get_setting, set_setting

router = APIRouter(prefix="/admin", tags=["admin"])

TOKYO_TZ = timezone(timedelta(hours=9))
NY_TZ = timezone(timedelta(hours=-5))
LONDON_TZ = timezone(timedelta(hours=0))


def _get_market_times() -> dict:
    now_utc = datetime.now(timezone.utc)
    tokyo_time = now_utc.astimezone(TOKYO_TZ)
    london_time = now_utc.astimezone(LONDON_TZ)

    ny_dst = _is_dst_us(now_utc)
    actual_ny_offset = -4 if ny_dst else -5
    ny_tz_actual = timezone(timedelta(hours=actual_ny_offset))
    ny_time_actual = now_utc.astimezone(ny_tz_actual)

    tokyo_minutes = tokyo_time.hour * 60 + tokyo_time.minute
    tokyo_open = 9 * 60 <= tokyo_minutes < 15 * 60

    ny_minutes = ny_time_actual.hour * 60 + ny_time_actual.minute
    ny_open = 9 * 60 + 30 <= ny_minutes < 16 * 60

    london_minutes = london_time.hour * 60 + london_time.minute
    london_open = 8 * 60 <= london_minutes < 16 * 60

    return {
        "utc": now_utc.strftime("%Y-%m-%d %H:%M:%S UTC"),
        "tokyo": {
            "time": tokyo_time.strftime("%Y-%m-%d %H:%M:%S JST"),
            "hour": tokyo_time.hour,
            "market_open": tokyo_open,
            "session": "Tokyo",
        },
        "new_york": {
            "time": ny_time_actual.strftime(f"%Y-%m-%d %H:%M:%S {'EDT' if ny_dst else 'EST'}"),
            "hour": ny_time_actual.hour,
            "market_open": ny_open,
            "session": "New York",
            "dst": ny_dst,
        },
        "london": {
            "time": london_time.strftime("%Y-%m-%d %H:%M:%S GMT"),
            "hour": london_time.hour,
            "market_open": london_open,
            "session": "London",
        },
    }


def _is_dst_us(dt: datetime) -> bool:
    year = dt.year
    march_second_sunday = _nth_weekday(year, 3, 6, 2)
    nov_first_sunday = _nth_weekday(year, 11, 6, 1)
    dst_start = march_second_sunday.replace(hour=7, tzinfo=timezone.utc)
    dst_end = nov_first_sunday.replace(hour=6, tzinfo=timezone.utc)
    return dst_start <= dt < dst_end


def _nth_weekday(year: int, month: int, weekday: int, n: int) -> datetime:
    first = datetime(year, month, 1)
    first_weekday = first.weekday()
    days_until = (weekday - first_weekday) % 7
    day = 1 + days_until + (n - 1) * 7
    return datetime(year, month, day)


def _signals_to_table_rows(signals: list[dict]) -> str:
    if not signals:
        return '<tr><td colspan="10" style="text-align:center;padding:24px;color:#94a3b8;">No signals found</td></tr>'

    rows = []
    for s in signals:
        direction = s.get("direction", "")
        dir_class = "buy" if direction == "long" else "sell"
        dir_label = "BUY" if direction == "long" else "SELL"
        status = s.get("status", "active")
        status_class = f"status-{status}"

        entry_str = f'{s.get("entry_price", 0):.5f}'
        sl_val = s.get("stop_loss")
        sl_str = f'{sl_val:.5f}' if sl_val is not None else "—"
        tp_val = s.get("take_profit")
        tp_str = f'{tp_val:.5f}' if tp_val is not None else "—"
        exit_val = s.get("exit_price")
        exit_str = f'{exit_val:.5f}' if exit_val is not None else "—"

        rows.append(f"""
        <tr>
            <td>{s.get("symbol", "")}</td>
            <td><span class="badge {dir_class}">{dir_label}</span></td>
            <td>{entry_str}</td>
            <td>{sl_str}</td>
            <td>{tp_str}</td>
            <td>{exit_str}</td>
            <td>{s.get("strategy", "")}</td>
            <td><span class="badge {status_class}">{status.upper()}</span></td>
            <td>{s.get("trigger_timeframe", "")}</td>
            <td>{s.get("created_at", "")}</td>
        </tr>""")
    return "\n".join(rows)


def _build_credit_html(stats: dict) -> str:
    pct = stats["usage_percentage"]
    monthly = stats["monthly_total"]
    limit = stats["monthly_limit"]
    daily = stats["daily_total"]
    alert = stats["alert_level"]

    bar_color = "#22c55e"
    alert_html = ""
    if alert == "caution":
        bar_color = "#eab308"
        alert_html = '<div class="alert caution">Warning: API usage has exceeded 60% of the monthly limit.</div>'
    elif alert == "warning":
        bar_color = "#f97316"
        alert_html = '<div class="alert warning">Alert: API usage has exceeded 75% of the monthly limit!</div>'
    elif alert == "critical":
        bar_color = "#ef4444"
        alert_html = '<div class="alert critical">CRITICAL: API usage has exceeded 90% of the monthly limit!</div>'

    endpoint_rows = ""
    for ep in stats.get("by_endpoint", []):
        endpoint_rows += f'<tr><td>{ep["endpoint"]}</td><td>{ep["count"]}</td><td>{ep["credits"]}</td></tr>'

    daily_rows = ""
    for dh in stats.get("daily_history", [])[:7]:
        daily_rows += f'<tr><td>{dh["day"]}</td><td>{dh["credits"]}</td></tr>'

    return f"""
    {alert_html}
    <div class="stats-grid">
        <div class="stat-card">
            <div class="stat-label">Monthly Usage</div>
            <div class="stat-value">{monthly:,} / {limit:,}</div>
            <div class="progress-bar">
                <div class="progress-fill" style="width:{min(pct,100):.1f}%;background:{bar_color};"></div>
            </div>
            <div class="stat-label">{pct:.2f}% used</div>
        </div>
        <div class="stat-card">
            <div class="stat-label">Today's Usage</div>
            <div class="stat-value">{daily:,}</div>
        </div>
        <div class="stat-card">
            <div class="stat-label">Remaining</div>
            <div class="stat-value">{limit - monthly:,}</div>
        </div>
    </div>
    <div class="tables-row">
        <div class="half-table">
            <h3>Usage by Endpoint</h3>
            <table class="data-table">
                <thead><tr><th>Endpoint</th><th>Calls</th><th>Credits</th></tr></thead>
                <tbody>{endpoint_rows if endpoint_rows else '<tr><td colspan="3" style="text-align:center;color:#94a3b8;">No API calls yet</td></tr>'}</tbody>
            </table>
        </div>
        <div class="half-table">
            <h3>Daily History (Last 7 Days)</h3>
            <table class="data-table">
                <thead><tr><th>Date</th><th>Credits</th></tr></thead>
                <tbody>{daily_rows if daily_rows else '<tr><td colspan="2" style="text-align:center;color:#94a3b8;">No history yet</td></tr>'}</tbody>
            </table>
        </div>
    </div>
    """


def _build_timezone_html(times: dict) -> str:
    def market_badge(info):
        if info["market_open"]:
            return '<span class="badge status-active">OPEN</span>'
        return '<span class="badge status-closed">CLOSED</span>'

    return f"""
    <div class="stats-grid">
        <div class="stat-card">
            <div class="stat-label">UTC</div>
            <div class="stat-value" style="font-size:1.1rem;">{times["utc"]}</div>
        </div>
        <div class="stat-card">
            <div class="stat-label">Tokyo (JST, UTC+9)</div>
            <div class="stat-value" style="font-size:1.1rem;">{times["tokyo"]["time"]}</div>
            {market_badge(times["tokyo"])}
            <div class="stat-label" style="margin-top:4px;">Session: 09:00 - 15:00 JST</div>
        </div>
        <div class="stat-card">
            <div class="stat-label">New York ({'EDT' if times['new_york']['dst'] else 'EST'})</div>
            <div class="stat-value" style="font-size:1.1rem;">{times["new_york"]["time"]}</div>
            {market_badge(times["new_york"])}
            <div class="stat-label" style="margin-top:4px;">Session: 09:30 - 16:00 ET</div>
        </div>
        <div class="stat-card">
            <div class="stat-label">London (GMT)</div>
            <div class="stat-value" style="font-size:1.1rem;">{times["london"]["time"]}</div>
            {market_badge(times["london"])}
            <div class="stat-label" style="margin-top:4px;">Session: 08:00 - 16:00 GMT</div>
        </div>
    </div>
    <div class="timezone-note">
        <strong>Strategy Timezone Logic:</strong>
        <ul>
            <li><strong>Highest/Lowest Close FX:</strong> Monitors Tokyo 8:00 AM (23:00 UTC) and New York 8:00 AM (13:00 UTC) windows for breakout/reversal setups.</li>
            <li>All candle timestamps are stored in UTC for consistency.</li>
            <li>DST is automatically handled for New York time calculations.</li>
        </ul>
    </div>
    """


def _build_settings_html() -> str:
    current_key = get_setting("fcsapi_key")
    env_key = os.environ.get("FCSAPI_KEY", "")
    has_db_key = bool(current_key)
    has_env_key = bool(env_key)
    masked_key = ""
    if current_key:
        masked_key = current_key[:4] + "•" * (len(current_key) - 8) + current_key[-4:] if len(current_key) > 8 else "•" * len(current_key)
    elif env_key:
        masked_key = env_key[:4] + "•" * (len(env_key) - 8) + env_key[-4:] if len(env_key) > 8 else "•" * len(env_key)

    source_badge = ""
    if has_db_key:
        source_badge = '<span class="badge status-active">DATABASE</span>'
    elif has_env_key:
        source_badge = '<span class="badge status-expired">ENV VARIABLE</span>'
    else:
        source_badge = '<span class="badge status-closed">NOT SET</span>'

    return f"""
    <div class="settings-section">
        <h3>FCSAPI Access Key</h3>
        <p class="settings-desc">Configure your FCSAPI API key for live market data. The key is stored securely in the application database and persists across restarts.</p>
        <div class="key-status">
            <span class="stat-label">Current Key Source:</span> {source_badge}
            <span style="margin-left:12px;color:#94a3b8;font-size:0.85rem;">{masked_key if masked_key else 'No key configured'}</span>
        </div>
        <div class="key-form" style="margin-top:16px;">
            <div style="display:flex;gap:8px;align-items:center;flex-wrap:wrap;">
                <input type="password" id="api-key-input" placeholder="Enter FCSAPI Access Key" data-testid="input-api-key"
                    style="background:#0f172a;border:1px solid #334155;color:#e2e8f0;padding:10px 14px;border-radius:6px;font-size:0.9rem;width:360px;">
                <button class="btn btn-primary" onclick="saveApiKey()" data-testid="button-save-key">Save Key</button>
                <button class="btn btn-secondary" onclick="testConnection()" data-testid="button-test-connection">Test Connection</button>
            </div>
            <div id="save-result" style="margin-top:12px;"></div>
        </div>
    </div>

    <div id="connection-result" class="settings-section" style="margin-top:20px;display:none;">
        <h3>Connection Test Result</h3>
        <div id="connection-details"></div>
    </div>

    <div class="settings-section" style="margin-top:20px;">
        <h3>API Credit Meter</h3>
        <p class="settings-desc">Visual overview of your FCSAPI monthly credit usage against the 500,000 limit.</p>
        <div id="credit-meter-container">
            <div class="stat-label" style="margin-bottom:4px;">Loading credit data...</div>
        </div>
    </div>

    <div class="settings-section" style="margin-top:20px;">
        <h3>Key Priority</h3>
        <div class="timezone-note" style="margin-top:0;">
            <ul>
                <li><strong>1st priority:</strong> Key saved in database (via this form)</li>
                <li><strong>2nd priority:</strong> Static key passed at startup</li>
                <li><strong>3rd priority:</strong> FCSAPI_KEY environment variable</li>
            </ul>
            <p style="margin-top:8px;">Saving a key here overrides the environment variable and takes effect immediately for all market data requests.</p>
        </div>
    </div>
    """


ADMIN_CSS = """
* { margin: 0; padding: 0; box-sizing: border-box; }
body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: #0f172a; color: #e2e8f0; }
.container { max-width: 1400px; margin: 0 auto; padding: 20px; }
header { background: #1e293b; border-bottom: 1px solid #334155; padding: 16px 20px; margin-bottom: 20px; }
header h1 { font-size: 1.5rem; color: #f8fafc; }
header p { font-size: 0.875rem; color: #94a3b8; margin-top: 4px; }
.tabs { display: flex; gap: 4px; margin-bottom: 20px; background: #1e293b; padding: 4px; border-radius: 8px; }
.tab { padding: 10px 20px; border-radius: 6px; cursor: pointer; font-size: 0.875rem; font-weight: 500; color: #94a3b8; text-decoration: none; transition: all 0.2s; }
.tab:hover { color: #e2e8f0; background: #334155; }
.tab.active { color: #f8fafc; background: #3b82f6; }
.section { background: #1e293b; border-radius: 12px; border: 1px solid #334155; padding: 24px; margin-bottom: 20px; }
.section h2 { font-size: 1.25rem; margin-bottom: 16px; color: #f8fafc; }
h3 { font-size: 1rem; margin-bottom: 12px; color: #cbd5e1; }
.data-table { width: 100%; border-collapse: collapse; font-size: 0.85rem; }
.data-table th { background: #0f172a; padding: 10px 12px; text-align: left; font-weight: 600; color: #94a3b8; border-bottom: 1px solid #334155; white-space: nowrap; }
.data-table td { padding: 10px 12px; border-bottom: 1px solid #1e293b; color: #e2e8f0; white-space: nowrap; }
.data-table tbody tr:hover { background: #334155; }
.badge { display: inline-block; padding: 2px 8px; border-radius: 4px; font-size: 0.75rem; font-weight: 600; text-transform: uppercase; }
.badge.buy { background: #064e3b; color: #6ee7b7; }
.badge.sell { background: #7f1d1d; color: #fca5a5; }
.badge.status-active { background: #064e3b; color: #6ee7b7; }
.badge.status-closed { background: #7f1d1d; color: #fca5a5; }
.badge.status-expired { background: #78350f; color: #fde68a; }
.stats-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 16px; margin-bottom: 20px; }
.stat-card { background: #0f172a; border: 1px solid #334155; border-radius: 8px; padding: 16px; text-align: center; }
.stat-label { font-size: 0.8rem; color: #94a3b8; margin-bottom: 4px; }
.stat-value { font-size: 1.5rem; font-weight: 700; color: #f8fafc; }
.progress-bar { background: #334155; border-radius: 4px; height: 8px; margin: 8px 0; overflow: hidden; }
.progress-fill { height: 100%; border-radius: 4px; transition: width 0.3s; }
.alert { padding: 12px 16px; border-radius: 8px; margin-bottom: 16px; font-weight: 500; }
.alert.caution { background: #422006; border: 1px solid #854d0e; color: #fde68a; }
.alert.warning { background: #431407; border: 1px solid #9a3412; color: #fdba74; }
.alert.critical { background: #450a0a; border: 1px solid #991b1b; color: #fca5a5; }
.btn { display: inline-block; padding: 8px 16px; border-radius: 6px; font-size: 0.875rem; font-weight: 500; cursor: pointer; text-decoration: none; border: none; transition: all 0.2s; margin-right: 8px; margin-bottom: 8px; }
.btn-primary { background: #3b82f6; color: white; }
.btn-primary:hover { background: #2563eb; }
.btn-secondary { background: #334155; color: #e2e8f0; }
.btn-secondary:hover { background: #475569; }
.export-bar { display: flex; gap: 8px; margin-bottom: 16px; align-items: center; flex-wrap: wrap; }
.filter-bar { display: flex; gap: 8px; margin-bottom: 16px; align-items: center; flex-wrap: wrap; }
.filter-bar select, .filter-bar input { background: #0f172a; border: 1px solid #334155; color: #e2e8f0; padding: 8px 12px; border-radius: 6px; font-size: 0.85rem; }
.tables-row { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }
.half-table { background: #0f172a; border: 1px solid #334155; border-radius: 8px; padding: 16px; }
.timezone-note { background: #0f172a; border: 1px solid #334155; border-radius: 8px; padding: 16px; margin-top: 16px; font-size: 0.85rem; color: #94a3b8; }
.timezone-note ul { margin-top: 8px; padding-left: 20px; }
.timezone-note li { margin-bottom: 4px; }
.settings-section { background: #0f172a; border: 1px solid #334155; border-radius: 8px; padding: 20px; }
.settings-desc { font-size: 0.85rem; color: #94a3b8; margin-bottom: 12px; }
.key-status { display: flex; align-items: center; gap: 8px; flex-wrap: wrap; margin-top: 8px; }
.result-success { background: #064e3b; border: 1px solid #065f46; border-radius: 8px; padding: 16px; color: #6ee7b7; }
.result-error { background: #450a0a; border: 1px solid #991b1b; border-radius: 8px; padding: 16px; color: #fca5a5; }
.credit-meter { margin-top: 12px; }
.credit-meter .meter-label { display: flex; justify-content: space-between; font-size: 0.85rem; color: #94a3b8; margin-bottom: 4px; }
.credit-meter .meter-bar { background: #334155; border-radius: 6px; height: 24px; overflow: hidden; position: relative; }
.credit-meter .meter-fill { height: 100%; border-radius: 6px; transition: width 0.5s ease; display: flex; align-items: center; justify-content: center; font-size: 0.75rem; font-weight: 600; color: white; min-width: 40px; }
.hidden { display: none; }
@media (max-width: 768px) {
    .tables-row { grid-template-columns: 1fr; }
    .stats-grid { grid-template-columns: 1fr 1fr; }
}
"""

ADMIN_JS = """
const BASE = window.location.pathname.replace(/\\/admin\\/?$/, '');

function showTab(tabName) {
    document.querySelectorAll('.tab-content').forEach(el => el.classList.add('hidden'));
    document.querySelectorAll('.tab').forEach(el => el.classList.remove('active'));
    document.getElementById('tab-' + tabName).classList.remove('hidden');
    document.querySelector('[data-tab="' + tabName + '"]').classList.add('active');
    if (tabName === 'settings') loadCreditMeter();
}

function exportSignals(format) {
    window.location.href = BASE + '/admin/export?format=' + format;
}

function refreshPage() {
    window.location.reload();
}

async function saveApiKey() {
    const input = document.getElementById('api-key-input');
    const key = input.value.trim();
    if (!key) {
        document.getElementById('save-result').innerHTML = '<div class="result-error">Please enter an API key.</div>';
        return;
    }
    try {
        const res = await fetch(BASE + '/admin/api/settings/key', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({api_key: key})
        });
        const data = await res.json();
        if (data.success) {
            document.getElementById('save-result').innerHTML = '<div class="result-success">API key saved successfully. It will be used for all future market data requests.</div>';
            input.value = '';
            setTimeout(() => window.location.reload(), 1500);
        } else {
            document.getElementById('save-result').innerHTML = '<div class="result-error">Failed to save: ' + (data.error || 'Unknown error') + '</div>';
        }
    } catch (e) {
        document.getElementById('save-result').innerHTML = '<div class="result-error">Error: ' + e.message + '</div>';
    }
}

async function testConnection() {
    const resultDiv = document.getElementById('connection-result');
    const detailsDiv = document.getElementById('connection-details');
    resultDiv.style.display = 'block';
    detailsDiv.innerHTML = '<div class="stat-label">Testing connection...</div>';

    try {
        const res = await fetch(BASE + '/admin/api/settings/test-connection', {method: 'POST'});
        const data = await res.json();
        if (data.success) {
            const total = data.total_credits || 500000;
            const used = data.used_credits || 0;
            const remaining = data.remaining_credits || (total - used);
            const pct = ((used / total) * 100).toFixed(1);
            let barColor = '#22c55e';
            if (pct >= 90) barColor = '#ef4444';
            else if (pct >= 75) barColor = '#f97316';
            else if (pct >= 60) barColor = '#eab308';

            detailsDiv.innerHTML = `
                <div class="result-success">
                    <strong>Connection Successful</strong>
                    <div style="margin-top:12px;display:grid;grid-template-columns:1fr 1fr;gap:12px;">
                        <div><span class="stat-label">Plan Type:</span><br><strong>${data.plan_type}</strong></div>
                        <div><span class="stat-label">Remaining Credits:</span><br><strong>${Number(remaining).toLocaleString()}</strong></div>
                    </div>
                    <div class="credit-meter" style="margin-top:16px;">
                        <div class="meter-label"><span>Used: ${Number(used).toLocaleString()}</span><span>Total: ${Number(total).toLocaleString()}</span></div>
                        <div class="meter-bar"><div class="meter-fill" style="width:${Math.min(pct,100)}%;background:${barColor};">${pct}%</div></div>
                    </div>
                </div>`;
        } else {
            detailsDiv.innerHTML = '<div class="result-error"><strong>Connection Failed</strong><br>' + (data.error || 'Unknown error') + '</div>';
        }
    } catch (e) {
        detailsDiv.innerHTML = '<div class="result-error"><strong>Error</strong><br>' + e.message + '</div>';
    }
}

async function loadCreditMeter() {
    const container = document.getElementById('credit-meter-container');
    if (!container) return;
    try {
        const res = await fetch(BASE + '/admin/api/usage');
        const data = await res.json();
        const used = data.monthly_total || 0;
        const total = data.monthly_limit || 500000;
        const pct = data.usage_percentage || 0;
        const daily = data.daily_total || 0;
        let barColor = '#22c55e';
        let alertText = '';
        if (pct >= 90) { barColor = '#ef4444'; alertText = 'CRITICAL'; }
        else if (pct >= 75) { barColor = '#f97316'; alertText = 'WARNING'; }
        else if (pct >= 60) { barColor = '#eab308'; alertText = 'CAUTION'; }

        container.innerHTML = `
            <div class="credit-meter">
                <div class="meter-label">
                    <span>Used: ${Number(used).toLocaleString()} credits</span>
                    <span>Limit: ${Number(total).toLocaleString()} credits</span>
                </div>
                <div class="meter-bar"><div class="meter-fill" style="width:${Math.min(pct,100)}%;background:${barColor};">${pct.toFixed(1)}%</div></div>
                <div style="display:flex;justify-content:space-between;margin-top:8px;">
                    <span class="stat-label">Remaining: ${Number(total - used).toLocaleString()}</span>
                    <span class="stat-label">Today: ${Number(daily).toLocaleString()}</span>
                    ${alertText ? '<span class="badge status-' + (pct >= 90 ? 'closed' : 'expired') + '">' + alertText + '</span>' : ''}
                </div>
            </div>`;
    } catch (e) {
        container.innerHTML = '<div class="result-error">Failed to load credit data.</div>';
    }
}

document.addEventListener('DOMContentLoaded', function() {
    const activeTab = document.querySelector('.tab.active');
    if (activeTab && activeTab.getAttribute('data-tab') === 'settings') loadCreditMeter();
});
"""


@router.get("/", response_class=HTMLResponse)
def admin_dashboard(
    strategy: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    symbol: Optional[str] = Query(None),
    tab: str = Query("signals"),
):
    signals = get_all_signals(strategy=strategy, symbol=symbol, status=status, limit=200)
    active_signals = get_active_signals()
    usage_stats = get_api_usage_stats()
    market_times = _get_market_times()

    active_count = len(active_signals)
    total_count = len(signals)

    signal_rows = _signals_to_table_rows(signals)
    credit_html = _build_credit_html(usage_stats)
    timezone_html = _build_timezone_html(market_times)
    settings_html = _build_settings_html()

    strategy_options = ""
    for s in ["", "mtf_ema", "trend_following", "sp500_momentum", "highest_lowest_fx"]:
        label = s.replace("_", " ").title() if s else "All Strategies"
        selected = "selected" if s == (strategy or "") else ""
        strategy_options += f'<option value="{s}" {selected}>{label}</option>'

    status_options = ""
    for s in ["", "active", "closed", "expired"]:
        label = s.title() if s else "All Statuses"
        selected = "selected" if s == (status or "") else ""
        status_options += f'<option value="{s}" {selected}>{label}</option>'

    alert_badge = ""
    if usage_stats["alert_level"]:
        level = usage_stats["alert_level"].upper()
        alert_badge = f' <span class="badge status-{"closed" if usage_stats["alert_level"] == "critical" else "expired"}">{level}</span>'

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Trading Engine Admin</title>
    <style>{ADMIN_CSS}</style>
</head>
<body>
    <header>
        <h1>Trading Engine Admin</h1>
        <p>Signal Management | Credit Monitor | Market Hours | Settings</p>
    </header>
    <div class="container">
        <div class="tabs">
            <a class="tab {'active' if tab == 'signals' else ''}" data-tab="signals" onclick="showTab('signals')">Signals ({total_count})</a>
            <a class="tab {'active' if tab == 'credits' else ''}" data-tab="credits" onclick="showTab('credits')">Credit Monitor{alert_badge}</a>
            <a class="tab {'active' if tab == 'timezone' else ''}" data-tab="timezone" onclick="showTab('timezone')">Market Hours</a>
            <a class="tab {'active' if tab == 'settings' else ''}" data-tab="settings" onclick="showTab('settings')">Settings</a>
        </div>

        <div id="tab-signals" class="tab-content {'hidden' if tab != 'signals' else ''}">
            <div class="section">
                <h2>Trading Signals</h2>
                <div class="filter-bar">
                    <form method="GET" style="display:flex;gap:8px;align-items:center;flex-wrap:wrap;">
                        <input type="hidden" name="tab" value="signals">
                        <select name="strategy" onchange="this.form.submit()">{strategy_options}</select>
                        <select name="status" onchange="this.form.submit()">{status_options}</select>
                        <input type="text" name="symbol" placeholder="Symbol (e.g. EUR/USD)" value="{symbol or ''}" style="width:160px;">
                        <button type="submit" class="btn btn-primary">Filter</button>
                    </form>
                </div>
                <div class="export-bar">
                    <button class="btn btn-secondary" onclick="exportSignals('csv')">Export CSV</button>
                    <button class="btn btn-secondary" onclick="exportSignals('json')">Export JSON</button>
                    <button class="btn btn-secondary" onclick="refreshPage()">Refresh</button>
                    <span style="color:#94a3b8;font-size:0.8rem;margin-left:8px;">Active: {active_count} | Total: {total_count}</span>
                </div>
                <div style="overflow-x:auto;">
                    <table class="data-table" data-testid="signals-table">
                        <thead>
                            <tr>
                                <th>Asset</th>
                                <th>Direction</th>
                                <th>Entry Price</th>
                                <th>Stop Loss</th>
                                <th>Take Profit</th>
                                <th>Exit Price</th>
                                <th>Strategy</th>
                                <th>Status</th>
                                <th>Timeframe</th>
                                <th>Timestamp</th>
                            </tr>
                        </thead>
                        <tbody>{signal_rows}</tbody>
                    </table>
                </div>
            </div>
        </div>

        <div id="tab-credits" class="tab-content {'hidden' if tab != 'credits' else ''}">
            <div class="section">
                <h2>FCSAPI Credit Monitor</h2>
                {credit_html}
            </div>
        </div>

        <div id="tab-timezone" class="tab-content {'hidden' if tab != 'timezone' else ''}">
            <div class="section">
                <h2>Market Hours & Timezone</h2>
                {timezone_html}
            </div>
        </div>

        <div id="tab-settings" class="tab-content {'hidden' if tab != 'settings' else ''}">
            <div class="section">
                <h2>Settings</h2>
                {settings_html}
            </div>
        </div>
    </div>
    <script>{ADMIN_JS}</script>
</body>
</html>"""
    return HTMLResponse(content=html)


@router.get("/export")
def export_signals(
    format: str = Query("csv", description="Export format: csv or json"),
    strategy: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    symbol: Optional[str] = Query(None),
):
    signals = get_all_signals(strategy=strategy, symbol=symbol, status=status, limit=500)

    if format == "json":
        content = json.dumps(signals, indent=2, default=str)
        return StreamingResponse(
            io.BytesIO(content.encode()),
            media_type="application/json",
            headers={"Content-Disposition": f"attachment; filename=signals_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json"},
        )

    output = io.StringIO()
    if signals:
        fields = ["symbol", "direction", "entry_price", "stop_loss", "take_profit",
                  "exit_price", "strategy", "status", "trigger_timeframe", "created_at",
                  "exit_reason", "highest_price", "lowest_price"]
        writer = csv.DictWriter(output, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for s in signals:
            writer.writerow(s)

    output.seek(0)
    return StreamingResponse(
        io.BytesIO(output.getvalue().encode()),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename=signals_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.csv"},
    )


@router.get("/api/usage")
def api_usage_stats():
    stats = get_api_usage_stats()
    return JSONResponse(content=stats)


@router.get("/api/market-times")
def market_times():
    times = _get_market_times()
    return JSONResponse(content=times)


@router.post("/api/settings/key")
def save_api_key(body: dict = Body(...)):
    api_key = body.get("api_key", "").strip()
    if not api_key:
        return JSONResponse(content={"success": False, "error": "API key cannot be empty"})
    set_setting("fcsapi_key", api_key)
    return JSONResponse(content={"success": True, "message": "API key saved successfully"})


@router.post("/api/settings/test-connection")
def test_api_connection():
    from trading_engine.fcsapi_client import FCSAPIClient
    client = FCSAPIClient()
    result = client.test_connection()
    return JSONResponse(content=result)


@router.get("/api/settings")
def get_settings():
    db_key = get_setting("fcsapi_key")
    env_key = os.environ.get("FCSAPI_KEY", "")
    has_db_key = bool(db_key)
    has_env_key = bool(env_key)
    source = "database" if has_db_key else ("environment" if has_env_key else "none")
    return JSONResponse(content={
        "api_key_configured": has_db_key or has_env_key,
        "key_source": source,
    })
