import csv
import io
import json
import os
from datetime import datetime, timezone, timedelta
from fastapi import APIRouter, Query, Body, Request, Response, Cookie, Depends
from fastapi.responses import HTMLResponse, StreamingResponse, JSONResponse, RedirectResponse
from typing import Optional

from trading_engine.database import (
    get_all_signals, get_active_signals, get_api_usage_stats, get_setting, set_setting,
    authenticate_admin, create_session, validate_session, delete_session,
    get_all_admins, create_admin, update_admin, delete_admin, get_admin_by_id,
    cleanup_expired_sessions, get_candles,
    get_all_open_positions, get_open_position,
)
from trading_engine.indicators import IndicatorEngine

router = APIRouter(prefix="/admin", tags=["admin"])


def _get_session_user(request: Request) -> Optional[dict]:
    token = request.cookies.get("admin_session")
    if not token:
        return None
    return validate_session(token)


def _require_auth(request: Request):
    user = _get_session_user(request)
    if not user:
        return None
    return user

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
        return '<tr><td colspan="8" style="text-align:center;padding:24px;color:#94a3b8;">No signals found</td></tr>'

    from datetime import datetime as dt_cls

    seen_closed = set()
    rows = []
    for s in signals:
        status = s.get("status", "OPEN")
        if status == "CLOSED":
            dedup_key = (s.get("asset"), s.get("strategy_name"), s.get("direction"))
            if dedup_key in seen_closed:
                continue
            seen_closed.add(dedup_key)

        direction = s.get("direction", "")
        dir_class = "buy" if direction == "BUY" else "sell"
        status_class = "status-active" if status == "OPEN" else "status-closed"

        entry_str = f'{s.get("entry_price", 0):.5f}'
        sl_val = s.get("stop_loss")
        sl_str = f'{sl_val:.5f}' if sl_val is not None else "—"
        tp_val = s.get("take_profit")
        tp_str = f'{tp_val:.5f}' if tp_val is not None else "—"

        ts_raw = s.get("signal_timestamp", "")
        if isinstance(ts_raw, dt_cls):
            ts_display = ts_raw.strftime("%Y-%m-%d %H:%M")
        elif ts_raw:
            try:
                dt = dt_cls.fromisoformat(str(ts_raw).replace("Z", "+00:00"))
                ts_display = dt.strftime("%Y-%m-%d %H:%M")
            except (ValueError, TypeError):
                ts_display = str(ts_raw)
        else:
            ts_display = "—"

        exit_info = ""
        if status == "CLOSED":
            exit_price = s.get("exit_price")
            exit_reason = s.get("exit_reason", "")
            if exit_price is not None:
                exit_info = f'<div style="font-size:0.75rem;color:#94a3b8;margin-top:2px;">Exit: {exit_price:.5f} ({exit_reason})</div>'
            elif exit_reason:
                exit_info = f'<div style="font-size:0.75rem;color:#94a3b8;margin-top:2px;">({exit_reason})</div>'

        rows.append(f"""
        <tr>
            <td>{s.get("asset", "")}</td>
            <td><span class="badge {dir_class}">{direction}</span></td>
            <td>{entry_str}</td>
            <td>{sl_str}</td>
            <td>{tp_str}</td>
            <td>{s.get("strategy_name", "")}</td>
            <td><span class="badge {status_class}">{status}</span>{exit_info}</td>
            <td>{ts_display}</td>
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
            <li><strong>Highest/Lowest Close FX:</strong> EUR/USD session &amp; holiday aware &mdash; evaluates at 9:00 AM and 10:00 AM ET only, skips US/JP holidays. Uses 0.25&times; H1 ATR trailing stop with previous day high/low filter.</li>
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


LOGIN_CSS = """
* { margin: 0; padding: 0; box-sizing: border-box; }
body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: #0f172a; color: #e2e8f0; display: flex; align-items: center; justify-content: center; min-height: 100vh; }
.login-card { background: #1e293b; border: 1px solid #334155; border-radius: 12px; padding: 40px; width: 100%; max-width: 400px; }
.login-card h1 { font-size: 1.5rem; color: #f8fafc; margin-bottom: 8px; text-align: center; }
.login-card p { font-size: 0.875rem; color: #94a3b8; margin-bottom: 24px; text-align: center; }
.form-group { margin-bottom: 16px; }
.form-group label { display: block; font-size: 0.85rem; color: #94a3b8; margin-bottom: 6px; }
.form-group input { width: 100%; background: #0f172a; border: 1px solid #334155; color: #e2e8f0; padding: 10px 14px; border-radius: 6px; font-size: 0.9rem; }
.form-group input:focus { outline: none; border-color: #3b82f6; }
.login-btn { width: 100%; background: #3b82f6; color: white; padding: 12px; border: none; border-radius: 6px; font-size: 0.95rem; font-weight: 600; cursor: pointer; margin-top: 8px; }
.login-btn:hover { background: #2563eb; }
.error-msg { background: #450a0a; border: 1px solid #991b1b; color: #fca5a5; padding: 10px 14px; border-radius: 6px; font-size: 0.85rem; margin-bottom: 16px; text-align: center; }
"""


def _build_login_page(error: str = "") -> str:
    error_html = f'<div class="error-msg">{error}</div>' if error else ""
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Trading Engine Admin - Login</title>
    <style>{LOGIN_CSS}</style>
</head>
<body>
    <div class="login-card">
        <h1>Trading Engine Admin</h1>
        <p>Sign in to access the dashboard</p>
        {error_html}
        <form method="POST" action="login">
            <div class="form-group">
                <label for="username">Username</label>
                <input type="text" id="username" name="username" data-testid="input-username" required autofocus>
            </div>
            <div class="form-group">
                <label for="password">Password</label>
                <input type="password" id="password" name="password" data-testid="input-password" required>
            </div>
            <button type="submit" class="login-btn" data-testid="button-login">Sign In</button>
        </form>
    </div>
</body>
</html>"""


def _get_spx_momentum_data() -> dict:
    from zoneinfo import ZoneInfo
    et_zone = ZoneInfo("America/New_York")
    et_now = datetime.now(et_zone)
    et_minutes = et_now.hour * 60 + et_now.minute
    session_start = 9 * 60 + 30
    session_end = 15 * 60 + 30
    in_session = session_start <= et_minutes <= session_end
    ny_dst = bool(et_now.dst() and et_now.dst().total_seconds() > 0)

    candles = get_candles("SPX", "30m", 300)
    current_rsi = None
    prev_rsi = None
    current_atr = None
    current_close = None
    candle_count = len(candles)

    if candles:
        closes = [c["close"] for c in candles]
        highs = [c["high"] for c in candles]
        lows = [c["low"] for c in candles]
        current_close = closes[-1]

        rsi_values = IndicatorEngine.rsi(closes, 20)
        if rsi_values:
            current_rsi = rsi_values[-1]
            if len(rsi_values) >= 2:
                prev_rsi = rsi_values[-2]

        atr_values = IndicatorEngine.atr(highs, lows, closes, 100)
        if atr_values and atr_values[-1] is not None:
            current_atr = atr_values[-1]

    active = get_active_signals(strategy_name="sp500_momentum", asset="SPX")
    active_signal = None
    if active:
        sig = active[0]
        atr_at_entry = sig.get("atr_at_entry")
        pos = get_open_position("sp500_momentum", "SPX")
        stored_highest = (pos.get("highest_price_since_entry") if pos else None) or sig["entry_price"]
        highest_close = max(stored_highest, current_close) if current_close else stored_highest
        trailing_stop = None
        if atr_at_entry is not None:
            trailing_stop = highest_close - (atr_at_entry * 2.0)
        active_signal = {
            "id": sig["id"],
            "entry_price": sig["entry_price"],
            "atr_at_entry": atr_at_entry,
            "highest_close": highest_close,
            "trailing_stop": trailing_stop,
            "direction": sig["direction"],
            "created_at": sig.get("created_at"),
            "current_close": current_close,
        }

    return {
        "et_time": et_now.strftime(f"%Y-%m-%d %H:%M:%S {'EDT' if ny_dst else 'EST'}"),
        "in_session": in_session,
        "dst_active": ny_dst,
        "current_rsi": round(current_rsi, 4) if current_rsi is not None else None,
        "prev_rsi": round(prev_rsi, 4) if prev_rsi is not None else None,
        "current_atr": round(current_atr, 6) if current_atr is not None else None,
        "current_close": round(current_close, 2) if current_close is not None else None,
        "candle_count": candle_count,
        "active_signal": active_signal,
    }


def _build_spx_momentum_html(spx_data: dict, spx_signal_rows: str, spx_signal_count: int) -> str:
    in_session = spx_data["in_session"]
    session_badge = '<span class="badge status-active">IN SESSION</span>' if in_session else '<span class="badge status-closed">OUTSIDE SESSION</span>'

    rsi_val = spx_data["current_rsi"]
    rsi_display = f"{rsi_val:.4f}" if rsi_val is not None else "N/A"
    rsi_class = ""
    if rsi_val is not None:
        if rsi_val >= 70:
            rsi_class = "color:#6ee7b7;"
        elif rsi_val <= 30:
            rsi_class = "color:#fca5a5;"

    atr_val = spx_data["current_atr"]
    atr_display = f"{atr_val:.6f}" if atr_val is not None else "N/A"

    close_val = spx_data["current_close"]
    close_display = f"{close_val:.2f}" if close_val is not None else "N/A"

    active_html = ""
    sig = spx_data["active_signal"]
    if sig:
        entry = sig["entry_price"]
        atr_entry = sig["atr_at_entry"]
        trail = sig["trailing_stop"]
        highest = sig["highest_close"]
        cur = sig.get("current_close")

        entry_display = f"{entry:.2f}"
        atr_entry_display = f"{atr_entry:.6f}" if atr_entry is not None else "N/A"
        trail_display = f"{trail:.2f}" if trail is not None else "N/A"
        highest_display = f"{highest:.2f}"
        cur_display = f"{cur:.2f}" if cur is not None else "N/A"
        pnl = ""
        if cur is not None and entry:
            diff = cur - entry
            pnl_pct = (diff / entry) * 100
            pnl_color = "#6ee7b7" if diff >= 0 else "#fca5a5"
            pnl = f'<span style="color:{pnl_color};font-weight:600;">{diff:+.2f} ({pnl_pct:+.2f}%)</span>'

        active_html = f"""
        <div class="settings-section" style="margin-top:20px;border-left:3px solid #3b82f6;">
            <h3>Active Trade</h3>
            <div class="stats-grid" style="margin-top:12px;">
                <div class="stat-card">
                    <div class="stat-label">Entry Price</div>
                    <div class="stat-value" style="font-size:1.3rem;">{entry_display}</div>
                </div>
                <div class="stat-card">
                    <div class="stat-label">Fixed ATR (at entry)</div>
                    <div class="stat-value" style="font-size:1.3rem;">{atr_entry_display}</div>
                </div>
                <div class="stat-card">
                    <div class="stat-label">Trailing Stop Level</div>
                    <div class="stat-value" style="font-size:1.3rem;color:#fbbf24;">{trail_display}</div>
                </div>
                <div class="stat-card">
                    <div class="stat-label">Highest Close</div>
                    <div class="stat-value" style="font-size:1.3rem;">{highest_display}</div>
                </div>
                <div class="stat-card">
                    <div class="stat-label">Current Close</div>
                    <div class="stat-value" style="font-size:1.3rem;">{cur_display}</div>
                </div>
                <div class="stat-card">
                    <div class="stat-label">P&L</div>
                    <div class="stat-value" style="font-size:1.3rem;">{pnl}</div>
                </div>
            </div>
            <div class="stat-label" style="margin-top:8px;">Opened: {sig.get('created_at', 'N/A')} | Direction: {sig['direction']}</div>
        </div>"""
    else:
        active_html = """
        <div class="settings-section" style="margin-top:20px;">
            <h3>Active Trade</h3>
            <p style="color:#94a3b8;padding:16px 0;">No active SPX momentum trade.</p>
        </div>"""

    return f"""
    <div class="stats-grid">
        <div class="stat-card">
            <div class="stat-label">ARCA Session (9:30-15:30 ET)</div>
            <div style="margin-top:8px;">{session_badge}</div>
            <div class="stat-label" style="margin-top:8px;">{spx_data['et_time']}</div>
            <div class="stat-label">DST: {'Active' if spx_data['dst_active'] else 'Inactive'}</div>
        </div>
        <div class="stat-card">
            <div class="stat-label">Current Close (30m)</div>
            <div class="stat-value" style="font-size:1.3rem;">{close_display}</div>
            <div class="stat-label" style="margin-top:4px;">{spx_data['candle_count']} candles loaded</div>
        </div>
        <div class="stat-card">
            <div class="stat-label">RSI(20)</div>
            <div class="stat-value" style="font-size:1.3rem;{rsi_class}">{rsi_display}</div>
            <div class="stat-label" style="margin-top:4px;">Threshold: 70</div>
        </div>
        <div class="stat-card">
            <div class="stat-label">ATR(100)</div>
            <div class="stat-value" style="font-size:1.3rem;">{atr_display}</div>
        </div>
    </div>
    {active_html}
    <div class="settings-section" style="margin-top:20px;">
        <h3>Signal History ({spx_signal_count})</h3>
        <div style="overflow-x:auto;margin-top:12px;">
            <table class="data-table" data-testid="spx-signals-table">
                <thead>
                    <tr>
                        <th>Asset</th>
                        <th>Direction</th>
                        <th>Entry Price</th>
                        <th>Stop Loss</th>
                        <th>Take Profit</th>
                        <th>Strategy</th>
                        <th>Status</th>
                        <th>Timestamp</th>
                    </tr>
                </thead>
                <tbody>{spx_signal_rows}</tbody>
            </table>
        </div>
    </div>
    <div class="timezone-note" style="margin-top:16px;">
        <strong>Strategy Rules:</strong>
        <ul>
            <li><strong>Entry:</strong> LONG when prev RSI(20) &lt; 70 AND current RSI(20) &ge; 70 (during ARCA session)</li>
            <li><strong>Exit (RSI):</strong> Close when prev RSI(20) &ge; 70 AND current RSI(20) &lt; 70</li>
            <li><strong>Exit (Trailing Stop):</strong> Close when price &lt; highest_close - (ATR_at_entry &times; 2)</li>
            <li><strong>ATR:</strong> Fixed at entry value for the duration of the trade</li>
            <li><strong>Session:</strong> Only evaluates during ARCA hours (9:30 AM - 3:30 PM ET, last valid candle)</li>
        </ul>
    </div>
    """


def _get_mtf_ema_data() -> dict:
    from zoneinfo import ZoneInfo
    et_zone = ZoneInfo("America/New_York")
    et_now = datetime.now(et_zone)
    ny_dst = bool(et_now.dst() and et_now.dst().total_seconds() > 0)

    forex_symbols = ["EUR/USD", "GBP/USD", "USD/JPY", "AUD/USD", "NZD/USD", "USD/CAD", "USD/CHF", "EUR/GBP"]
    symbols_data = []

    for symbol in forex_symbols:
        sym_info = {
            "symbol": symbol,
            "d1_ema200": None, "d1_ema50": None, "d1_close": None, "d1_count": 0,
            "h4_ema200": None, "h4_ema50": None, "h4_atr100": None, "h4_ema200_slope": None, "h4_count": 0,
            "h1_ema20": None, "h1_close": None, "h1_count": 0,
            "cond_price_above_d1_emas": False, "cond_h4_ema200_rising": False,
            "cond_dip_below_h4_50": False, "cond_dip_within_1_atr": False,
            "cond_h1_above_ema20": False, "all_conditions": False,
        }

        d1_candles = get_candles(symbol, "D1", 300)
        h4_candles = get_candles(symbol, "4H", 300)
        h1_candles = get_candles(symbol, "1H", 300)
        sym_info["d1_count"] = len(d1_candles)
        sym_info["h4_count"] = len(h4_candles)
        sym_info["h1_count"] = len(h1_candles)

        if len(d1_candles) >= 200 and len(h4_candles) >= 200 and len(h1_candles) >= 20:
            d1_closes = [c["close"] for c in d1_candles]
            h4_closes = [c["close"] for c in h4_candles]
            h4_highs = [c["high"] for c in h4_candles]
            h4_lows = [c["low"] for c in h4_candles]
            h1_closes = [c["close"] for c in h1_candles]

            d1_ema200 = IndicatorEngine.ema(d1_closes, 200)
            d1_ema50 = IndicatorEngine.ema(d1_closes, 50)
            h4_ema200 = IndicatorEngine.ema(h4_closes, 200)
            h4_ema50 = IndicatorEngine.ema(h4_closes, 50)
            h4_atr100 = IndicatorEngine.atr(h4_highs, h4_lows, h4_closes, 100)
            h1_ema20 = IndicatorEngine.ema(h1_closes, 20)

            current_price = h1_closes[-1]
            sym_info["d1_close"] = d1_closes[-1]
            sym_info["d1_ema200"] = d1_ema200[-1]
            sym_info["d1_ema50"] = d1_ema50[-1]
            sym_info["h4_ema200"] = h4_ema200[-1]
            sym_info["h4_ema50"] = h4_ema50[-1]
            sym_info["h4_atr100"] = h4_atr100[-1]
            sym_info["h1_ema20"] = h1_ema20[-1]
            sym_info["h1_close"] = current_price

            h4_ema200_prev = h4_ema200[-2] if len(h4_ema200) >= 2 else None
            if h4_ema200[-1] is not None and h4_ema200_prev is not None:
                sym_info["h4_ema200_slope"] = h4_ema200[-1] - h4_ema200_prev

            if sym_info["d1_ema200"] is not None and sym_info["d1_ema50"] is not None:
                sym_info["cond_price_above_d1_emas"] = current_price > sym_info["d1_ema200"] and current_price > sym_info["d1_ema50"]
            if sym_info["h4_ema200_slope"] is not None:
                sym_info["cond_h4_ema200_rising"] = sym_info["h4_ema200_slope"] > 0
            if sym_info["h4_ema50"] is not None:
                sym_info["cond_dip_below_h4_50"] = current_price < sym_info["h4_ema50"]
            if sym_info["h4_atr100"] is not None and sym_info["h4_ema50"] is not None and sym_info["cond_dip_below_h4_50"]:
                sym_info["cond_dip_within_1_atr"] = (sym_info["h4_ema50"] - current_price) < sym_info["h4_atr100"]
            if sym_info["h1_ema20"] is not None:
                sym_info["cond_h1_above_ema20"] = current_price > sym_info["h1_ema20"]

            sym_info["all_conditions"] = all([
                sym_info["cond_price_above_d1_emas"],
                sym_info["cond_h4_ema200_rising"],
                sym_info["cond_dip_below_h4_50"],
                sym_info["cond_dip_within_1_atr"],
                sym_info["cond_h1_above_ema20"],
            ])

        symbols_data.append(sym_info)

    active_trades = get_active_signals(strategy_name="mtf_ema")
    open_positions_list = get_all_open_positions(strategy_name="mtf_ema")
    pos_by_asset = {p["asset"]: p for p in open_positions_list}
    trade_details = []
    for sig in active_trades:
        atr_at_entry = sig.get("atr_at_entry")
        entry_price = sig["entry_price"]
        pos = pos_by_asset.get(sig["asset"])
        stored_highest = (pos.get("highest_price_since_entry") if pos else None) or entry_price
        sym_candles = get_candles(sig["asset"], "1H", 5)
        cur_close = sym_candles[-1]["close"] if sym_candles else None
        if cur_close:
            stored_highest = max(stored_highest, cur_close)
        trailing_stop = None
        if atr_at_entry is not None:
            trailing_stop = stored_highest - (atr_at_entry * 2.0)
        trade_details.append({
            "id": sig["id"],
            "symbol": sig["asset"],
            "direction": sig["direction"],
            "entry_price": entry_price,
            "atr_at_entry": atr_at_entry,
            "highest_close": stored_highest,
            "trailing_stop": trailing_stop,
            "current_close": cur_close,
            "created_at": sig.get("created_at"),
        })

    return {
        "et_time": et_now.strftime(f"%Y-%m-%d %H:%M:%S {'EDT' if ny_dst else 'EST'}"),
        "dst_active": ny_dst,
        "symbols": symbols_data,
        "active_trades": trade_details,
    }


def _build_mtf_ema_html(mtf_data: dict, mtf_signal_rows: str, mtf_signal_count: int) -> str:
    def _fmt(val, decimals=5):
        return f"{val:.{decimals}f}" if val is not None else "N/A"

    def _cond(val):
        return '<span style="color:#6ee7b7;">YES</span>' if val else '<span style="color:#fca5a5;">NO</span>'

    symbols_html = ""
    for sym in mtf_data["symbols"]:
        data_status = ""
        if sym["d1_count"] < 200 or sym["h4_count"] < 200 or sym["h1_count"] < 20:
            data_status = f'<div style="color:#fbbf24;font-size:0.8rem;margin-top:6px;">D1: {sym["d1_count"]}/200, H4: {sym["h4_count"]}/200, H1: {sym["h1_count"]}/20</div>'
        else:
            slope_val = sym["h4_ema200_slope"]
            slope_display = f"{slope_val:+.6f}" if slope_val is not None else "N/A"
            slope_color = "#6ee7b7" if slope_val is not None and slope_val > 0 else "#fca5a5"

            data_status = f"""
            <div style="display:grid;grid-template-columns:1fr 1fr;gap:4px 12px;margin-top:8px;font-size:0.8rem;">
                <div>D1 EMA200: {_fmt(sym["d1_ema200"])}</div>
                <div>D1 EMA50: {_fmt(sym["d1_ema50"])}</div>
                <div>H4 EMA200: {_fmt(sym["h4_ema200"])}</div>
                <div>H4 EMA50: {_fmt(sym["h4_ema50"])}</div>
                <div>H4 ATR100: {_fmt(sym["h4_atr100"], 6)}</div>
                <div>H4 EMA200 Slope: <span style="color:{slope_color};">{slope_display}</span></div>
                <div>H1 EMA20: {_fmt(sym["h1_ema20"])}</div>
                <div>H1 Close: {_fmt(sym["h1_close"])}</div>
            </div>
            <div style="display:grid;grid-template-columns:1fr 1fr;gap:4px 12px;margin-top:8px;font-size:0.8rem;border-top:1px solid #334155;padding-top:8px;">
                <div>Price &gt; D1 EMAs: {_cond(sym["cond_price_above_d1_emas"])}</div>
                <div>H4 EMA200 Rising: {_cond(sym["cond_h4_ema200_rising"])}</div>
                <div>Dip Below H4 50: {_cond(sym["cond_dip_below_h4_50"])}</div>
                <div>Within 1 ATR: {_cond(sym["cond_dip_within_1_atr"])}</div>
                <div>H1 &gt; EMA20: {_cond(sym["cond_h1_above_ema20"])}</div>
                <div><strong>All Met:</strong> {_cond(sym["all_conditions"])}</div>
            </div>"""

        all_badge = ""
        if sym["all_conditions"]:
            all_badge = ' <span class="badge status-active">READY</span>'

        symbols_html += f"""
        <div class="stat-card" style="min-width:250px;">
            <div class="stat-label">{sym["symbol"]}{all_badge}</div>
            {data_status}
        </div>"""

    active_html = ""
    if mtf_data["active_trades"]:
        for trade in mtf_data["active_trades"]:
            entry = trade["entry_price"]
            atr_e = trade["atr_at_entry"]
            trail = trade["trailing_stop"]
            highest = trade["highest_close"]
            cur = trade["current_close"]
            pnl = ""
            if cur is not None and entry:
                diff = cur - entry
                pnl_pct = (diff / entry) * 100
                pnl_color = "#6ee7b7" if diff >= 0 else "#fca5a5"
                pnl = f'<span style="color:{pnl_color};font-weight:600;">{diff:+.5f} ({pnl_pct:+.2f}%)</span>'

            active_html += f"""
        <div class="settings-section" style="margin-top:12px;border-left:3px solid #3b82f6;">
            <h3>{trade["symbol"]} - {trade["direction"]}</h3>
            <div class="stats-grid" style="margin-top:8px;">
                <div class="stat-card"><div class="stat-label">Entry</div><div class="stat-value" style="font-size:1.1rem;">{_fmt(entry)}</div></div>
                <div class="stat-card"><div class="stat-label">Fixed ATR</div><div class="stat-value" style="font-size:1.1rem;">{_fmt(atr_e, 6)}</div></div>
                <div class="stat-card"><div class="stat-label">Trail Stop</div><div class="stat-value" style="font-size:1.1rem;color:#fbbf24;">{_fmt(trail)}</div></div>
                <div class="stat-card"><div class="stat-label">Highest</div><div class="stat-value" style="font-size:1.1rem;">{_fmt(highest)}</div></div>
                <div class="stat-card"><div class="stat-label">Current</div><div class="stat-value" style="font-size:1.1rem;">{_fmt(cur)}</div></div>
                <div class="stat-card"><div class="stat-label">P&L</div><div class="stat-value" style="font-size:1.1rem;">{pnl}</div></div>
            </div>
            <div class="stat-label" style="margin-top:6px;">Opened: {trade.get("created_at", "N/A")}</div>
        </div>"""
    else:
        active_html = """
        <div class="settings-section" style="margin-top:20px;">
            <h3>Active Trades</h3>
            <p style="color:#94a3b8;padding:16px 0;">No active MTF EMA trades.</p>
        </div>"""

    return f"""
    <div class="stat-card" style="margin-bottom:16px;">
        <div class="stat-label">Evaluation Time</div>
        <div style="margin-top:4px;">{mtf_data['et_time']}</div>
        <div class="stat-label" style="margin-top:4px;">DST: {'Active' if mtf_data['dst_active'] else 'Inactive'}</div>
    </div>
    <div class="settings-section">
        <h3>Multi-Timeframe Conditions</h3>
        <div class="stats-grid" style="margin-top:12px;grid-template-columns:repeat(auto-fit, minmax(260px, 1fr));">
            {symbols_html}
        </div>
    </div>
    {active_html}
    <div class="settings-section" style="margin-top:20px;">
        <h3>Signal History ({mtf_signal_count})</h3>
        <div style="overflow-x:auto;margin-top:12px;">
            <table class="data-table" data-testid="mtf-signals-table">
                <thead>
                    <tr>
                        <th>Asset</th>
                        <th>Direction</th>
                        <th>Entry Price</th>
                        <th>Stop Loss</th>
                        <th>Take Profit</th>
                        <th>Strategy</th>
                        <th>Status</th>
                        <th>Timestamp</th>
                    </tr>
                </thead>
                <tbody>{mtf_signal_rows}</tbody>
            </table>
        </div>
    </div>
    <div class="settings-section" style="margin-top:20px;" data-testid="mtf-strategy-rules">
        <h3>MTF EMA Trend-Pullback Strategy Rules</h3>
        <p style="color:#94a3b8;margin-top:4px;font-size:0.85rem;">Multi-Timeframe EMA strategy using D1 + H4 + H1 timeframe synchronization with trend-pullback entry logic and dual exit management.</p>

        <div style="margin-top:16px;">
            <h4 style="color:#e2e8f0;margin-bottom:8px;font-size:0.95rem;">Covered Assets (12)</h4>
            <div style="display:flex;flex-wrap:wrap;gap:6px;">
                <span class="badge status-active">SPX</span>
                <span class="badge status-active">NDX</span>
                <span class="badge status-active">RUT</span>
                <span class="badge" style="background:#1e3a5f;color:#93c5fd;">XAU/USD</span>
                <span class="badge" style="background:#1e3a5f;color:#93c5fd;">XAG/USD</span>
                <span class="badge" style="background:#1e3a5f;color:#93c5fd;">WTI/USD</span>
                <span class="badge" style="background:#3b1f4e;color:#c4b5fd;">BTC/USD</span>
                <span class="badge" style="background:#3b1f4e;color:#c4b5fd;">ETH/USD</span>
                <span class="badge" style="background:#1e3a2f;color:#86efac;">EUR/USD</span>
                <span class="badge" style="background:#1e3a2f;color:#86efac;">USD/JPY</span>
                <span class="badge" style="background:#1e3a2f;color:#86efac;">GBP/USD</span>
                <span class="badge" style="background:#1e3a2f;color:#86efac;">AUD/USD</span>
            </div>
        </div>

        <div style="margin-top:20px;">
            <h4 style="color:#e2e8f0;margin-bottom:8px;font-size:0.95rem;">Timeframe Hierarchy</h4>
            <div class="stats-grid" style="grid-template-columns:repeat(3, 1fr);">
                <div class="stat-card">
                    <div class="stat-label">D1 (Daily)</div>
                    <div style="font-size:0.8rem;color:#94a3b8;margin-top:4px;">Trend Direction</div>
                    <div style="font-size:0.78rem;margin-top:6px;">EMA 200 &mdash; Primary trend<br>EMA 50 &mdash; Secondary trend</div>
                </div>
                <div class="stat-card">
                    <div class="stat-label">H4 (4-Hour)</div>
                    <div style="font-size:0.8rem;color:#94a3b8;margin-top:4px;">Momentum &amp; Pullback</div>
                    <div style="font-size:0.78rem;margin-top:6px;">EMA 50 &mdash; Pullback zone<br>EMA 200 &mdash; Slope acceleration<br>ATR 100 &mdash; Volatility measure</div>
                </div>
                <div class="stat-card">
                    <div class="stat-label">H1 (1-Hour)</div>
                    <div style="font-size:0.8rem;color:#94a3b8;margin-top:4px;">Entry Trigger</div>
                    <div style="font-size:0.78rem;margin-top:6px;">EMA 20 &mdash; Crossover signal<br>Candle body &mdash; Confirmation</div>
                </div>
            </div>
        </div>

        <div style="margin-top:20px;">
            <h4 style="color:#e2e8f0;margin-bottom:8px;font-size:0.95rem;">Indicators Matrix</h4>
            <div style="overflow-x:auto;">
                <table class="data-table" style="font-size:0.82rem;">
                    <thead>
                        <tr><th>Indicator</th><th style="text-align:center;">D1</th><th style="text-align:center;">H4</th><th style="text-align:center;">H1</th><th>Purpose</th></tr>
                    </thead>
                    <tbody>
                        <tr><td>EMA 20</td><td style="text-align:center;color:#6ee7b7;">&#10003;</td><td style="text-align:center;color:#6ee7b7;">&#10003;</td><td style="text-align:center;color:#6ee7b7;">&#10003;</td><td>H1 crossover trigger</td></tr>
                        <tr><td>EMA 50</td><td style="text-align:center;color:#6ee7b7;">&#10003;</td><td style="text-align:center;color:#6ee7b7;">&#10003;</td><td style="text-align:center;color:#6ee7b7;">&#10003;</td><td>Pullback zone &amp; exit level</td></tr>
                        <tr><td>EMA 200</td><td style="text-align:center;color:#6ee7b7;">&#10003;</td><td style="text-align:center;color:#6ee7b7;">&#10003;</td><td style="text-align:center;color:#6ee7b7;">&#10003;</td><td>Trend direction &amp; slope</td></tr>
                        <tr><td>ATR 100</td><td style="text-align:center;color:#6ee7b7;">&#10003;</td><td style="text-align:center;color:#6ee7b7;">&#10003;</td><td style="text-align:center;color:#6ee7b7;">&#10003;</td><td>Stop loss &amp; trailing stop</td></tr>
                    </tbody>
                </table>
            </div>
        </div>

        <div style="margin-top:20px;">
            <h4 style="color:#6ee7b7;margin-bottom:8px;font-size:0.95rem;">&#9650; Long Entry Conditions (all 4 must be met)</h4>
            <div style="border-left:3px solid #22c55e;padding-left:12px;">
                <div style="margin-bottom:8px;">
                    <div style="display:flex;align-items:center;gap:8px;"><span class="badge status-active" style="font-size:0.7rem;">1</span><strong style="font-size:0.85rem;">D1 Trend Validation</strong></div>
                    <div style="font-size:0.8rem;color:#94a3b8;margin-top:2px;padding-left:28px;">Price must be above both D1 EMA 200 and D1 EMA 50, confirming a bullish macro trend.</div>
                </div>
                <div style="margin-bottom:8px;">
                    <div style="display:flex;align-items:center;gap:8px;"><span class="badge status-active" style="font-size:0.7rem;">2</span><strong style="font-size:0.85rem;">Slope Acceleration</strong></div>
                    <div style="font-size:0.8rem;color:#94a3b8;margin-top:2px;padding-left:28px;">D1 EMA 200 must be rising (current &gt; previous). H4 EMA 200 must be accelerating upward: (current &minus; prev) &gt; (prev &minus; earlier).</div>
                </div>
                <div style="margin-bottom:8px;">
                    <div style="display:flex;align-items:center;gap:8px;"><span class="badge status-active" style="font-size:0.7rem;">3</span><strong style="font-size:0.85rem;">Pullback Validation</strong></div>
                    <div style="font-size:0.8rem;color:#94a3b8;margin-top:2px;padding-left:28px;">Price must be below H4 EMA 50 (dipped into pullback zone) AND within 1&times; H4 ATR 100 of the H4 EMA 50.</div>
                </div>
                <div style="margin-bottom:8px;">
                    <div style="display:flex;align-items:center;gap:8px;"><span class="badge status-active" style="font-size:0.7rem;">4</span><strong style="font-size:0.85rem;">H1 Confirmation</strong></div>
                    <div style="font-size:0.8rem;color:#94a3b8;margin-top:2px;padding-left:28px;">Previous H1 close was below H1 EMA 20, current H1 close is above H1 EMA 20 (crossover). Current H1 candle must have a bullish body (Close &gt; Open).</div>
                </div>
            </div>
        </div>

        <div style="margin-top:20px;">
            <h4 style="color:#fca5a5;margin-bottom:8px;font-size:0.95rem;">&#9660; Short Entry Conditions (mirrored &mdash; all 4 must be met)</h4>
            <div style="border-left:3px solid #ef4444;padding-left:12px;">
                <div style="margin-bottom:8px;">
                    <div style="display:flex;align-items:center;gap:8px;"><span class="badge status-closed" style="font-size:0.7rem;">1</span><strong style="font-size:0.85rem;">D1 Trend Validation</strong></div>
                    <div style="font-size:0.8rem;color:#94a3b8;margin-top:2px;padding-left:28px;">Price must be below both D1 EMA 200 and D1 EMA 50, confirming a bearish macro trend.</div>
                </div>
                <div style="margin-bottom:8px;">
                    <div style="display:flex;align-items:center;gap:8px;"><span class="badge status-closed" style="font-size:0.7rem;">2</span><strong style="font-size:0.85rem;">Slope Acceleration</strong></div>
                    <div style="font-size:0.8rem;color:#94a3b8;margin-top:2px;padding-left:28px;">D1 EMA 200 must be falling (current &lt; previous). H4 EMA 200 must be accelerating downward: (prev &minus; current) &gt; (earlier &minus; prev).</div>
                </div>
                <div style="margin-bottom:8px;">
                    <div style="display:flex;align-items:center;gap:8px;"><span class="badge status-closed" style="font-size:0.7rem;">3</span><strong style="font-size:0.85rem;">Pullback Validation</strong></div>
                    <div style="font-size:0.8rem;color:#94a3b8;margin-top:2px;padding-left:28px;">Price must be above H4 EMA 50 (rallied into pullback zone) AND within 1&times; H4 ATR 100 of the H4 EMA 50.</div>
                </div>
                <div style="margin-bottom:8px;">
                    <div style="display:flex;align-items:center;gap:8px;"><span class="badge status-closed" style="font-size:0.7rem;">4</span><strong style="font-size:0.85rem;">H1 Confirmation</strong></div>
                    <div style="font-size:0.8rem;color:#94a3b8;margin-top:2px;padding-left:28px;">Previous H1 close was above H1 EMA 20, current H1 close is below H1 EMA 20 (crossover). Current H1 candle must have a bearish body (Close &lt; Open).</div>
                </div>
            </div>
        </div>

        <div style="margin-top:20px;">
            <h4 style="color:#e2e8f0;margin-bottom:8px;font-size:0.95rem;">Stop Loss Selection (Whichever-is-Greater)</h4>
            <div class="stats-grid" style="grid-template-columns:1fr 1fr;">
                <div class="stat-card">
                    <div class="stat-label">Method A &mdash; ATR-Based Stop</div>
                    <div style="font-size:0.8rem;color:#94a3b8;margin-top:6px;">
                        0.5&times; H4 ATR(100) from entry price.<br>
                        Long: entry &minus; (0.5 &times; ATR)<br>
                        Short: entry + (0.5 &times; ATR)
                    </div>
                </div>
                <div class="stat-card">
                    <div class="stat-label">Method B &mdash; Structural Stop</div>
                    <div style="font-size:0.8rem;color:#94a3b8;margin-top:6px;">
                        Long: lowest H1 low below H4 EMA 50 in last 24 H1 candles &minus; 2-pip buffer<br>
                        Short: highest H1 high above H4 EMA 50 in last 24 H1 candles + 2-pip buffer
                    </div>
                </div>
            </div>
            <div class="timezone-note" style="margin-top:8px;">
                <strong>Selection Rule:</strong> The stop loss with the <strong>greater distance</strong> from entry is selected. If no structural candles qualify, ATR stop is used as fallback. If structural stop lands on the wrong side of entry, ATR stop is used.
            </div>
            <div style="margin-top:8px;font-size:0.82rem;color:#94a3b8;">
                <strong style="color:#e2e8f0;">Take Profit:</strong> 3.0&times; H4 ATR(100) from entry price. Long: entry + (3.0 &times; ATR). Short: entry &minus; (3.0 &times; ATR).
            </div>
        </div>

        <div style="margin-top:20px;">
            <h4 style="color:#e2e8f0;margin-bottom:8px;font-size:0.95rem;">Exit Rules (checked in priority order)</h4>
            <div class="stat-card" style="border-left:3px solid #f59e0b;margin-bottom:8px;">
                <div style="display:flex;align-items:center;gap:8px;margin-bottom:6px;">
                    <span class="badge" style="background:#f59e0b;color:#1e293b;font-size:0.7rem;">PRIORITY 1</span>
                    <strong style="font-size:0.9rem;">H4 EMA 50 Breach Exit</strong>
                </div>
                <div style="display:grid;grid-template-columns:1fr 1fr;gap:8px;">
                    <div style="background:#1a2332;border-radius:6px;padding:10px;">
                        <div style="font-size:0.82rem;font-weight:600;color:#6ee7b7;margin-bottom:4px;">Long Exit</div>
                        <div style="font-size:0.78rem;color:#94a3b8;">Exit immediately when an H4 candle <strong style="color:#e2e8f0;">closes below</strong> the H4 EMA 50. The pullback zone has been lost.</div>
                    </div>
                    <div style="background:#1a2332;border-radius:6px;padding:10px;">
                        <div style="font-size:0.82rem;font-weight:600;color:#fca5a5;margin-bottom:4px;">Short Exit</div>
                        <div style="font-size:0.78rem;color:#94a3b8;">Exit immediately when an H4 candle <strong style="color:#e2e8f0;">closes above</strong> the H4 EMA 50. The pullback zone has been lost.</div>
                    </div>
                </div>
            </div>
            <div class="stat-card" style="border-left:3px solid #64748b;">
                <div style="display:flex;align-items:center;gap:8px;margin-bottom:6px;">
                    <span class="badge" style="background:#475569;color:#e2e8f0;font-size:0.7rem;">PRIORITY 2</span>
                    <strong style="font-size:0.9rem;">Trailing Stop Exit</strong>
                </div>
                <div style="display:grid;grid-template-columns:1fr 1fr;gap:8px;">
                    <div style="background:#1a2332;border-radius:6px;padding:10px;">
                        <div style="font-size:0.82rem;font-weight:600;color:#6ee7b7;margin-bottom:4px;">Long Trailing Stop</div>
                        <div style="font-size:0.78rem;color:#94a3b8;">Tracks the highest price since entry. Stop = peak &minus; (2.0&times; ATR at entry). Triggers when price drops below the trailing stop.</div>
                    </div>
                    <div style="background:#1a2332;border-radius:6px;padding:10px;">
                        <div style="font-size:0.82rem;font-weight:600;color:#fca5a5;margin-bottom:4px;">Short Trailing Stop</div>
                        <div style="font-size:0.78rem;color:#94a3b8;">Tracks the lowest price since entry. Stop = trough + (2.0&times; ATR at entry). Triggers when price rises above the trailing stop.</div>
                    </div>
                </div>
            </div>
        </div>

        <div style="margin-top:20px;">
            <h4 style="color:#e2e8f0;margin-bottom:8px;font-size:0.95rem;">Exit Diagnostics Logging</h4>
            <div class="stats-grid" style="grid-template-columns:1fr 1fr;">
                <div class="stat-card">
                    <div class="stat-label">D1 EMA 200 Slope</div>
                    <div style="font-size:0.78rem;color:#94a3b8;margin-top:4px;">current &minus; previous value. Positive = rising daily trend, negative = falling.</div>
                </div>
                <div class="stat-card">
                    <div class="stat-label">H4 EMA 200 Slope</div>
                    <div style="font-size:0.78rem;color:#94a3b8;margin-top:4px;">Current and previous period slopes logged separately to show momentum direction.</div>
                </div>
                <div class="stat-card">
                    <div class="stat-label">H4 EMA 200 Acceleration</div>
                    <div style="font-size:0.78rem;color:#94a3b8;margin-top:4px;">Difference between current slope and previous slope. Shows if momentum is increasing or decaying.</div>
                </div>
                <div class="stat-card">
                    <div class="stat-label">Pullback Depth</div>
                    <div style="font-size:0.78rem;color:#94a3b8;margin-top:4px;">H4 close minus H4 EMA 50. Shows how far price has moved from the key pullback level.</div>
                </div>
            </div>
        </div>

        <div style="margin-top:20px;">
            <h4 style="color:#e2e8f0;margin-bottom:8px;font-size:0.95rem;">Data Requirements</h4>
            <div class="stats-grid" style="grid-template-columns:repeat(3, 1fr);">
                <div class="stat-card" style="text-align:center;">
                    <div class="stat-value" style="font-size:1.5rem;">200</div>
                    <div class="stat-label">D1 candles</div>
                </div>
                <div class="stat-card" style="text-align:center;">
                    <div class="stat-value" style="font-size:1.5rem;">200</div>
                    <div class="stat-label">H4 candles</div>
                </div>
                <div class="stat-card" style="text-align:center;">
                    <div class="stat-value" style="font-size:1.5rem;">20</div>
                    <div class="stat-label">H1 candles</div>
                </div>
            </div>
        </div>

        <div style="margin-top:20px;">
            <h4 style="color:#e2e8f0;margin-bottom:8px;font-size:0.95rem;">Strategy Constants</h4>
            <div style="overflow-x:auto;">
                <table class="data-table" style="font-size:0.82rem;">
                    <thead>
                        <tr><th>Constant</th><th>Value</th><th>Usage</th></tr>
                    </thead>
                    <tbody>
                        <tr><td style="font-family:monospace;font-size:0.78rem;">SL_ATR_MULT</td><td>0.5</td><td>ATR stop loss multiplier</td></tr>
                        <tr><td style="font-family:monospace;font-size:0.78rem;">TP_ATR_MULT</td><td>3.0</td><td>Take profit multiplier</td></tr>
                        <tr><td style="font-family:monospace;font-size:0.78rem;">TRAILING_STOP_ATR_MULT</td><td>2.0</td><td>Trailing stop distance</td></tr>
                        <tr><td style="font-family:monospace;font-size:0.78rem;">STRUCTURAL_LOOKBACK_H1</td><td>24</td><td>H1 candles to scan for structural stop</td></tr>
                        <tr><td style="font-family:monospace;font-size:0.78rem;">STRUCTURAL_PIP_BUFFER</td><td>0.0002</td><td>2-pip buffer on structural stop</td></tr>
                        <tr><td style="font-family:monospace;font-size:0.78rem;">EMA periods</td><td>20 / 50 / 200</td><td>Fast / Medium / Slow EMA</td></tr>
                        <tr><td style="font-family:monospace;font-size:0.78rem;">ATR period</td><td>100</td><td>Volatility lookback</td></tr>
                    </tbody>
                </table>
            </div>
        </div>
    </div>
    """


def _get_trend_following_data() -> dict:
    from zoneinfo import ZoneInfo
    et_zone = ZoneInfo("America/New_York")
    et_now = datetime.now(et_zone)
    ny_dst = bool(et_now.dst() and et_now.dst().total_seconds() > 0)

    forex_symbols = ["EUR/USD", "GBP/USD", "USD/JPY", "AUD/USD", "NZD/USD", "USD/CAD", "USD/CHF", "EUR/GBP"]
    non_forex_symbols = ["SPX", "NDX", "XAU/USD", "XAG/USD", "WTI/USD", "BTC/USD", "ETH/USD"]
    all_symbols = forex_symbols + non_forex_symbols

    def _compute_symbol_data(symbol, long_only=False):
        sym_info = {
            "symbol": symbol,
            "current_close": None,
            "sma50": None,
            "sma100": None,
            "atr100": None,
            "highest_50d": None,
            "lowest_50d": None,
            "sma_status": "N/A",
            "candle_count": 0,
            "cond_price_above_50d_high": False,
            "cond_price_below_50d_low": False,
            "cond_sma50_above_sma100": False,
            "cond_sma50_below_sma100": False,
            "long_ready": False,
            "short_ready": False,
            "long_only": long_only,
        }

        candles = get_candles(symbol, "D1", 300)
        sym_info["candle_count"] = len(candles)

        if len(candles) >= 101:
            closes = [c["close"] for c in candles]
            highs = [c["high"] for c in candles]
            lows = [c["low"] for c in candles]
            sym_info["current_close"] = closes[-1]

            sma50_vals = IndicatorEngine.sma(closes, 50)
            sma100_vals = IndicatorEngine.sma(closes, 100)
            atr_vals = IndicatorEngine.atr(highs, lows, closes, 100)

            sym_info["sma50"] = sma50_vals[-1]
            sym_info["sma100"] = sma100_vals[-1]
            sym_info["atr100"] = atr_vals[-1]

            if len(closes) > 50:
                prior_closes = closes[-51:-1]
                sym_info["highest_50d"] = max(prior_closes)
                sym_info["lowest_50d"] = min(prior_closes)

            if sym_info["sma50"] is not None and sym_info["sma100"] is not None:
                sym_info["cond_sma50_above_sma100"] = sym_info["sma50"] > sym_info["sma100"]
                sym_info["cond_sma50_below_sma100"] = sym_info["sma50"] < sym_info["sma100"]
                if sym_info["cond_sma50_above_sma100"]:
                    sym_info["sma_status"] = "Bullish"
                elif sym_info["cond_sma50_below_sma100"]:
                    sym_info["sma_status"] = "Bearish"
                else:
                    sym_info["sma_status"] = "Neutral"

            if sym_info["highest_50d"] is not None and sym_info["current_close"] is not None:
                sym_info["cond_price_above_50d_high"] = sym_info["current_close"] >= sym_info["highest_50d"]

            if sym_info["lowest_50d"] is not None and sym_info["current_close"] is not None:
                sym_info["cond_price_below_50d_low"] = sym_info["current_close"] <= sym_info["lowest_50d"]

            sym_info["long_ready"] = sym_info["cond_price_above_50d_high"] and sym_info["cond_sma50_above_sma100"]
            if not long_only:
                sym_info["short_ready"] = sym_info["cond_price_below_50d_low"] and sym_info["cond_sma50_below_sma100"]

        return sym_info

    forex_data = [_compute_symbol_data(s, long_only=False) for s in forex_symbols]
    non_forex_data = [_compute_symbol_data(s, long_only=True) for s in non_forex_symbols]

    def _gather_trades(strategy_name):
        active_trades = get_active_signals(strategy_name=strategy_name)
        open_positions_list = get_all_open_positions(strategy_name=strategy_name)
        pos_by_asset = {p["asset"]: p for p in open_positions_list}
        trade_details = []
        for sig in active_trades:
            atr_at_entry = sig.get("atr_at_entry")
            entry_price = sig["entry_price"]
            direction = sig["direction"]
            pos = pos_by_asset.get(sig["asset"])
            stored_highest = (pos.get("highest_price_since_entry") if pos else None) or entry_price
            sym_candles = get_candles(sig["asset"], "D1", 5)
            cur_close = sym_candles[-1]["close"] if sym_candles else None
            if cur_close and direction == "BUY":
                stored_highest = max(stored_highest, cur_close)
            trailing_stop = None
            if atr_at_entry is not None:
                trailing_stop = stored_highest - (atr_at_entry * 3.0)
            trade_details.append({
                "id": sig["id"],
                "symbol": sig["asset"],
                "direction": direction,
                "strategy": strategy_name,
                "entry_price": entry_price,
                "atr_at_entry": atr_at_entry,
                "highest_close": stored_highest,
                "trailing_stop": trailing_stop,
                "current_close": cur_close,
                "created_at": sig.get("created_at"),
            })
        return trade_details

    forex_trades = _gather_trades("trend_following")
    non_forex_trades = _gather_trades("trend_non_forex")

    return {
        "et_time": et_now.strftime(f"%Y-%m-%d %H:%M:%S {'EDT' if ny_dst else 'EST'}"),
        "dst_active": ny_dst,
        "forex_symbols": forex_data,
        "non_forex_symbols": non_forex_data,
        "forex_trades": forex_trades,
        "non_forex_trades": non_forex_trades,
    }


def _build_trend_following_html(tf_data: dict, tf_signal_rows: str, tf_signal_count: int) -> str:
    def _fmt(val, decimals=5):
        return f"{val:.{decimals}f}" if val is not None else "N/A"

    def _cond(val):
        return '<span style="color:#6ee7b7;">YES</span>' if val else '<span style="color:#fca5a5;">NO</span>'

    def _build_symbol_cards(symbols_list):
        html = ""
        for sym in symbols_list:
            if sym["candle_count"] < 101:
                data_status = f'<div style="color:#fbbf24;font-size:0.8rem;margin-top:6px;">D1: {sym["candle_count"]}/101 candles loaded</div>'
            else:
                sma_color = "#6ee7b7" if sym["sma_status"] == "Bullish" else ("#fca5a5" if sym["sma_status"] == "Bearish" else "#94a3b8")
                lowest_row = ""
                if not sym["long_only"]:
                    lowest_row = f'<div>50d Low: {_fmt(sym["lowest_50d"])}</div>'

                data_status = f"""
                <div style="display:grid;grid-template-columns:1fr 1fr;gap:4px 12px;margin-top:8px;font-size:0.8rem;">
                    <div>Close: {_fmt(sym["current_close"])}</div>
                    <div>50d High: {_fmt(sym["highest_50d"])}</div>
                    <div>SMA(50): {_fmt(sym["sma50"])}</div>
                    <div>SMA(100): {_fmt(sym["sma100"])}</div>
                    <div>ATR(100): {_fmt(sym["atr100"], 6)}</div>
                    <div>Trend: <span style="color:{sma_color};">{sym["sma_status"]}</span></div>
                    {lowest_row}
                </div>
                <div style="display:grid;grid-template-columns:1fr 1fr;gap:4px 12px;margin-top:8px;font-size:0.8rem;border-top:1px solid #334155;padding-top:8px;">
                    <div>Price &ge; 50d High: {_cond(sym["cond_price_above_50d_high"])}</div>
                    <div>SMA50 &gt; SMA100: {_cond(sym["cond_sma50_above_sma100"])}</div>
                    <div><strong>LONG Ready:</strong> {_cond(sym["long_ready"])}</div>"""

                if not sym["long_only"]:
                    data_status += f"""
                    <div>Price &le; 50d Low: {_cond(sym["cond_price_below_50d_low"])}</div>
                    <div>SMA50 &lt; SMA100: {_cond(sym["cond_sma50_below_sma100"])}</div>
                    <div><strong>SHORT Ready:</strong> {_cond(sym["short_ready"])}</div>"""

                data_status += "</div>"

            badges = ""
            if sym.get("long_ready"):
                badges += ' <span class="badge status-active">LONG</span>'
            if sym.get("short_ready"):
                badges += ' <span class="badge" style="background:#7f1d1d;color:#fca5a5;">SHORT</span>'
            if sym["long_only"] and sym["candle_count"] >= 101:
                badges += ' <span style="font-size:0.65rem;color:#94a3b8;margin-left:4px;">(long only)</span>'

            html += f"""
            <div class="stat-card" style="min-width:250px;">
                <div class="stat-label">{sym["symbol"]}{badges}</div>
                {data_status}
            </div>"""
        return html

    def _build_trades_html(trades, label):
        if not trades:
            return f"""
            <div class="settings-section" style="margin-top:20px;">
                <h3>{label} - Active Trades</h3>
                <p style="color:#94a3b8;padding:16px 0;">No active trades.</p>
            </div>"""

        html = ""
        for trade in trades:
            entry = trade["entry_price"]
            atr_e = trade["atr_at_entry"]
            trail = trade["trailing_stop"]
            highest = trade["highest_close"]
            cur = trade["current_close"]
            pnl = ""
            if cur is not None and entry:
                diff = cur - entry if trade["direction"] == "BUY" else entry - cur
                pnl_pct = (diff / entry) * 100
                pnl_color = "#6ee7b7" if diff >= 0 else "#fca5a5"
                pnl = f'<span style="color:{pnl_color};font-weight:600;">{diff:+.5f} ({pnl_pct:+.2f}%)</span>'

            html += f"""
            <div class="settings-section" style="margin-top:12px;border-left:3px solid #3b82f6;">
                <h3>{trade["symbol"]} - {trade["direction"]}</h3>
                <div class="stats-grid" style="margin-top:8px;">
                    <div class="stat-card"><div class="stat-label">Entry</div><div class="stat-value" style="font-size:1.1rem;">{_fmt(entry)}</div></div>
                    <div class="stat-card"><div class="stat-label">Fixed ATR</div><div class="stat-value" style="font-size:1.1rem;">{_fmt(atr_e, 6)}</div></div>
                    <div class="stat-card"><div class="stat-label">Trail Stop (3x)</div><div class="stat-value" style="font-size:1.1rem;color:#fbbf24;">{_fmt(trail)}</div></div>
                    <div class="stat-card"><div class="stat-label">Highest</div><div class="stat-value" style="font-size:1.1rem;">{_fmt(highest)}</div></div>
                    <div class="stat-card"><div class="stat-label">Current</div><div class="stat-value" style="font-size:1.1rem;">{_fmt(cur)}</div></div>
                    <div class="stat-card"><div class="stat-label">P&L</div><div class="stat-value" style="font-size:1.1rem;">{pnl}</div></div>
                </div>
                <div class="stat-label" style="margin-top:6px;">Opened: {trade.get("created_at", "N/A")}</div>
            </div>"""
        return f"""
        <div class="settings-section" style="margin-top:20px;">
            <h3>{label} - Active Trades ({len(trades)})</h3>
            {html}
        </div>"""

    forex_cards = _build_symbol_cards(tf_data["forex_symbols"])
    non_forex_cards = _build_symbol_cards(tf_data["non_forex_symbols"])
    forex_trades_html = _build_trades_html(tf_data["forex_trades"], "Forex")
    non_forex_trades_html = _build_trades_html(tf_data["non_forex_trades"], "Non-Forex")

    return f"""
    <div class="stat-card" style="margin-bottom:16px;">
        <div class="stat-label">Evaluation Time</div>
        <div style="margin-top:4px;">{tf_data['et_time']}</div>
        <div class="stat-label" style="margin-top:4px;">DST: {'Active' if tf_data['dst_active'] else 'Inactive'}</div>
        <div style="font-size:0.75rem;color:#94a3b8;margin-top:4px;">Forex evals at 5:00 PM ET | Non-Forex evals at 4:00 PM ET</div>
    </div>
    <div class="settings-section">
        <h3>Forex Breakout Conditions (D1)</h3>
        <div class="stats-grid" style="margin-top:12px;grid-template-columns:repeat(auto-fit, minmax(260px, 1fr));">
            {forex_cards}
        </div>
    </div>
    <div class="settings-section" style="margin-top:20px;">
        <h3>Non-Forex Breakout Conditions (D1) <span style="font-size:0.75rem;color:#94a3b8;font-weight:normal;">LONG only</span></h3>
        <div class="stats-grid" style="margin-top:12px;grid-template-columns:repeat(auto-fit, minmax(260px, 1fr));">
            {non_forex_cards}
        </div>
    </div>
    {forex_trades_html}
    {non_forex_trades_html}
    <div class="settings-section" style="margin-top:20px;">
        <h3>Signal History ({tf_signal_count})</h3>
        <div style="overflow-x:auto;margin-top:12px;">
            <table class="data-table" data-testid="tf-signals-table">
                <thead>
                    <tr>
                        <th>Asset</th>
                        <th>Direction</th>
                        <th>Entry Price</th>
                        <th>Stop Loss</th>
                        <th>Take Profit</th>
                        <th>Strategy</th>
                        <th>Status</th>
                        <th>Timestamp</th>
                    </tr>
                </thead>
                <tbody>{tf_signal_rows}</tbody>
            </table>
        </div>
    </div>
    <div class="timezone-note" style="margin-top:16px;">
        <strong>Strategy Rules (Trend Following):</strong>
        <ul>
            <li><strong>Forex Entry (LONG):</strong> Close &ge; highest close of last 50 days AND SMA(50) &gt; SMA(100)</li>
            <li><strong>Forex Entry (SHORT):</strong> Close &le; lowest close of last 50 days AND SMA(50) &lt; SMA(100)</li>
            <li><strong>Non-Forex Entry:</strong> LONG only &mdash; Close &ge; highest close of last 50 days AND SMA(50) &gt; SMA(100)</li>
            <li><strong>Trailing Stop:</strong> Highest close &minus; (Fixed ATR at entry &times; 3)</li>
            <li><strong>Timeframe:</strong> Daily (D1) candles</li>
            <li><strong>ATR:</strong> Fixed at entry value for the duration of the trade</li>
        </ul>
    </div>
    <div class="timezone-note" style="margin-top:16px;border-left:3px solid #f59e0b;padding:12px 16px;background:rgba(245,158,11,0.06);border-radius:6px;">
        <strong style="color:#f59e0b;">Highest Close / Lowest Close FX (EUR/USD) &mdash; Session &amp; Holiday Aware</strong>
        <ul style="margin-top:8px;">
            <li><strong>Asset:</strong> EUR/USD only</li>
            <li><strong>Evaluation Window:</strong> Runs <strong>only</strong> at 9:00 AM and 10:00 AM ET (America/New_York), DST-aware</li>
            <li><strong>Holiday Filter:</strong> Skips US and Japan public holidays (via <code>holidays</code> library &mdash; US() + JP() calendars)</li>
            <li><strong>Long Entry:</strong> Price &ge; highest close of last 50 daily candles</li>
            <li><strong>Short Entry:</strong> Price &le; lowest close of last 50 daily candles (reversal: if within 0.2% of lowest, triggers BUY instead)</li>
            <li><strong>Previous Day Filter:</strong> Rejects Longs if entry price &lt; previous trading day&rsquo;s Low; rejects Shorts if entry price &gt; previous trading day&rsquo;s High</li>
            <li><strong>ATR Source:</strong> ATR(100) calculated from <strong>H1 (hourly)</strong> candles, fixed at entry</li>
            <li><strong>Trailing Stop (Long):</strong> Exit when price &lt; highest_close_since_entry &minus; (ATR<sub>entry</sub> &times; 0.25)</li>
            <li><strong>Trailing Stop (Short):</strong> Exit when price &gt; lowest_close_since_entry + (ATR<sub>entry</sub> &times; 0.25)</li>
            <li><strong>State Tracking:</strong> highest/lowest close updated in database on every H1 evaluation while trade is active</li>
            <li><strong>Schedule:</strong> Automated via APScheduler at 09:00 and 10:00 AM US/Eastern</li>
        </ul>
    </div>
    """


def _get_forex_trend_data() -> dict:
    from zoneinfo import ZoneInfo
    et_zone = ZoneInfo("America/New_York")
    et_now = datetime.now(et_zone)
    et_minutes = et_now.hour * 60 + et_now.minute
    close_minutes = 17 * 60
    window_end = close_minutes + 30
    in_window = close_minutes <= et_minutes <= window_end
    ny_dst = bool(et_now.dst() and et_now.dst().total_seconds() > 0)

    symbols_data = []
    for symbol in ["EUR/USD", "USD/JPY", "GBP/USD"]:
        candles = get_candles(symbol, "D1", 300)
        sym_info = {
            "symbol": symbol,
            "current_close": None,
            "sma50": None,
            "sma100": None,
            "atr100": None,
            "highest_50d": None,
            "lowest_50d": None,
            "sma_status": "N/A",
            "candle_count": len(candles),
        }
        if len(candles) >= 101:
            closes = [c["close"] for c in candles]
            highs = [c["high"] for c in candles]
            lows = [c["low"] for c in candles]
            sym_info["current_close"] = closes[-1]

            sma50_vals = IndicatorEngine.sma(closes, 50)
            sma100_vals = IndicatorEngine.sma(closes, 100)
            atr_vals = IndicatorEngine.atr(highs, lows, closes, 100)

            sym_info["sma50"] = sma50_vals[-1]
            sym_info["sma100"] = sma100_vals[-1]
            sym_info["atr100"] = atr_vals[-1]

            if len(closes) > 50:
                prior_closes = closes[-51:-1]
                sym_info["highest_50d"] = max(prior_closes)
                sym_info["lowest_50d"] = min(prior_closes)

            if sym_info["sma50"] is not None and sym_info["sma100"] is not None:
                sym_info["sma_status"] = "Bullish" if sym_info["sma50"] > sym_info["sma100"] else "Bearish"

        symbols_data.append(sym_info)

    active_trades = get_active_signals(strategy_name="trend_forex")
    open_positions = get_all_open_positions(strategy_name="trend_forex")
    pos_by_asset = {p["asset"]: p for p in open_positions}
    trade_details = []
    for sig in active_trades:
        atr_at_entry = sig.get("atr_at_entry")
        direction = sig["direction"]
        entry_price = sig["entry_price"]
        pos = pos_by_asset.get(sig["asset"])

        if direction == "BUY":
            stored_extreme = (pos.get("highest_price_since_entry") if pos else None) or entry_price
            sym_candles = get_candles(sig["asset"], "D1", 5)
            cur_close = sym_candles[-1]["close"] if sym_candles else None
            if cur_close:
                stored_extreme = max(stored_extreme, cur_close)
            trailing_stop = None
            if atr_at_entry is not None:
                trailing_stop = stored_extreme - (atr_at_entry * 3.0)
            trade_details.append({
                "id": sig["id"],
                "symbol": sig["asset"],
                "direction": direction,
                "entry_price": entry_price,
                "atr_at_entry": atr_at_entry,
                "extreme_price": stored_extreme,
                "trailing_stop": trailing_stop,
                "current_close": cur_close,
                "created_at": sig.get("created_at"),
            })
        elif direction == "SELL":
            stored_extreme = (pos.get("lowest_price_since_entry") if pos else None) or entry_price
            sym_candles = get_candles(sig["asset"], "D1", 5)
            cur_close = sym_candles[-1]["close"] if sym_candles else None
            if cur_close:
                stored_extreme = min(stored_extreme, cur_close)
            trailing_stop = None
            if atr_at_entry is not None:
                trailing_stop = stored_extreme + (atr_at_entry * 3.0)
            trade_details.append({
                "id": sig["id"],
                "symbol": sig["asset"],
                "direction": direction,
                "entry_price": entry_price,
                "atr_at_entry": atr_at_entry,
                "extreme_price": stored_extreme,
                "trailing_stop": trailing_stop,
                "current_close": cur_close,
                "created_at": sig.get("created_at"),
            })

    return {
        "et_time": et_now.strftime(f"%Y-%m-%d %H:%M:%S {'EDT' if ny_dst else 'EST'}"),
        "in_window": in_window,
        "dst_active": ny_dst,
        "symbols": symbols_data,
        "active_trades": trade_details,
    }


def _build_forex_trend_html(fx_data: dict, fx_signal_rows: str, fx_signal_count: int) -> str:
    in_window = fx_data["in_window"]
    window_badge = '<span class="badge status-active">IN WINDOW</span>' if in_window else '<span class="badge status-closed">OUTSIDE WINDOW</span>'

    symbols_html = ""
    for sym in fx_data["symbols"]:
        close_display = f"{sym['current_close']:.5f}" if sym["current_close"] is not None else "N/A"
        sma50_display = f"{sym['sma50']:.5f}" if sym["sma50"] is not None else "N/A"
        sma100_display = f"{sym['sma100']:.5f}" if sym["sma100"] is not None else "N/A"
        atr_display = f"{sym['atr100']:.5f}" if sym["atr100"] is not None else "N/A"
        high_display = f"{sym['highest_50d']:.5f}" if sym["highest_50d"] is not None else "N/A"
        low_display = f"{sym['lowest_50d']:.5f}" if sym["lowest_50d"] is not None else "N/A"
        sma_color = "#6ee7b7" if sym["sma_status"] == "Bullish" else "#fca5a5" if sym["sma_status"] == "Bearish" else "#94a3b8"
        sma_badge = f'<span style="color:{sma_color};font-weight:600;">{sym["sma_status"]}</span>'

        breakout_long = ""
        breakout_short = ""
        if sym["current_close"] is not None and sym["highest_50d"] is not None:
            if sym["current_close"] > sym["highest_50d"]:
                breakout_long = ' <span class="badge buy">BREAKOUT</span>'
        if sym["current_close"] is not None and sym["lowest_50d"] is not None:
            if sym["current_close"] < sym["lowest_50d"]:
                breakout_short = ' <span class="badge sell">BREAKDOWN</span>'

        symbols_html += f"""
        <div class="settings-section" style="margin-bottom:16px;" data-testid="forex-trend-symbol-{sym['symbol'].replace('/','-')}">
            <h3>{sym['symbol']} <span style="font-size:0.8rem;color:#94a3b8;font-weight:400;">{sym['candle_count']} D1 candles</span></h3>
            <div class="stats-grid" style="margin-top:12px;">
                <div class="stat-card">
                    <div class="stat-label">Current Close</div>
                    <div class="stat-value" style="font-size:1.2rem;">{close_display}</div>
                </div>
                <div class="stat-card">
                    <div class="stat-label">50-Day High{breakout_long}</div>
                    <div class="stat-value" style="font-size:1.2rem;">{high_display}</div>
                </div>
                <div class="stat-card">
                    <div class="stat-label">50-Day Low{breakout_short}</div>
                    <div class="stat-value" style="font-size:1.2rem;">{low_display}</div>
                </div>
                <div class="stat-card">
                    <div class="stat-label">SMA(50) vs SMA(100)</div>
                    <div class="stat-value" style="font-size:1.2rem;">{sma_badge}</div>
                    <div class="stat-label" style="margin-top:4px;">{sma50_display} / {sma100_display}</div>
                </div>
                <div class="stat-card">
                    <div class="stat-label">ATR(100)</div>
                    <div class="stat-value" style="font-size:1.2rem;">{atr_display}</div>
                </div>
            </div>
        </div>"""

    active_html = ""
    if fx_data["active_trades"]:
        trade_rows = ""
        for t in fx_data["active_trades"]:
            dir_class = "buy" if t["direction"] == "BUY" else "sell"
            entry_display = f"{t['entry_price']:.5f}"
            atr_display = f"{t['atr_at_entry']:.6f}" if t["atr_at_entry"] is not None else "N/A"
            extreme_label = "Highest Close" if t["direction"] == "BUY" else "Lowest Close"
            extreme_display = f"{t['extreme_price']:.5f}"
            trail_display = f"{t['trailing_stop']:.5f}" if t["trailing_stop"] is not None else "N/A"
            cur_display = f"{t['current_close']:.5f}" if t["current_close"] is not None else "N/A"
            pnl = ""
            if t["current_close"] is not None:
                if t["direction"] == "BUY":
                    diff = t["current_close"] - t["entry_price"]
                else:
                    diff = t["entry_price"] - t["current_close"]
                pnl_color = "#6ee7b7" if diff >= 0 else "#fca5a5"
                pnl = f'<span style="color:{pnl_color};font-weight:600;">{diff:+.5f}</span>'

            trade_rows += f"""
            <tr data-testid="row-forex-trade-{t['id']}">
                <td>{t['symbol']}</td>
                <td><span class="badge {dir_class}">{t['direction']}</span></td>
                <td>{entry_display}</td>
                <td>{atr_display}</td>
                <td>{extreme_display} <span style="color:#64748b;font-size:0.75rem;">({extreme_label})</span></td>
                <td style="color:#fbbf24;font-weight:600;">{trail_display}</td>
                <td>{cur_display}</td>
                <td>{pnl}</td>
                <td>{t.get('created_at', 'N/A')}</td>
            </tr>"""

        active_html = f"""
        <div class="settings-section" style="margin-top:20px;border-left:3px solid #3b82f6;">
            <h3>Active Trades ({len(fx_data['active_trades'])})</h3>
            <div style="overflow-x:auto;margin-top:12px;">
                <table class="data-table" data-testid="forex-trend-active-table">
                    <thead>
                        <tr>
                            <th>Symbol</th>
                            <th>Direction</th>
                            <th>Entry Price</th>
                            <th>Entry ATR(100)</th>
                            <th>Tracked Extreme</th>
                            <th>Trailing Stop</th>
                            <th>Current Close</th>
                            <th>P&amp;L</th>
                            <th>Opened</th>
                        </tr>
                    </thead>
                    <tbody>{trade_rows}</tbody>
                </table>
            </div>
        </div>"""
    else:
        active_html = """
        <div class="settings-section" style="margin-top:20px;">
            <h3>Active Trades</h3>
            <p style="color:#94a3b8;padding:16px 0;">No active Forex Trend trades.</p>
        </div>"""

    return f"""
    <div class="stats-grid">
        <div class="stat-card">
            <div class="stat-label">Eval Window (5:00 PM ET)</div>
            <div style="margin-top:8px;">{window_badge}</div>
            <div class="stat-label" style="margin-top:8px;">{fx_data['et_time']}</div>
            <div class="stat-label">DST: {'Active' if fx_data['dst_active'] else 'Inactive'}</div>
        </div>
        <div class="stat-card">
            <div class="stat-label">Target Assets</div>
            <div class="stat-value" style="font-size:1.2rem;">EUR/USD, USD/JPY, GBP/USD</div>
            <div class="stat-label" style="margin-top:4px;">Daily (D1) candles</div>
        </div>
        <div class="stat-card">
            <div class="stat-label">Scheduler</div>
            <div class="stat-value" style="font-size:1.2rem;color:#6ee7b7;">Active</div>
            <div class="stat-label" style="margin-top:4px;">APScheduler @ 17:00 ET</div>
        </div>
    </div>
    {symbols_html}
    {active_html}
    <div class="settings-section" style="margin-top:20px;">
        <h3>Signal History ({fx_signal_count})</h3>
        <div style="overflow-x:auto;margin-top:12px;">
            <table class="data-table" data-testid="forex-trend-signals-table">
                <thead>
                    <tr>
                        <th>Asset</th>
                        <th>Direction</th>
                        <th>Entry Price</th>
                        <th>Stop Loss</th>
                        <th>Take Profit</th>
                        <th>Strategy</th>
                        <th>Status</th>
                        <th>Timestamp</th>
                    </tr>
                </thead>
                <tbody>{fx_signal_rows}</tbody>
            </table>
        </div>
    </div>
    <div class="timezone-note" style="margin-top:16px;">
        <strong>Strategy Rules:</strong>
        <ul>
            <li><strong>Long Entry:</strong> Close &gt; Highest Close of prior 50 days AND SMA(50) &gt; SMA(100)</li>
            <li><strong>Short Entry:</strong> Close &lt; Lowest Close of prior 50 days AND SMA(50) &lt; SMA(100)</li>
            <li><strong>Exit (Trailing Stop):</strong> Long exits when close &lt; highest_since_entry - (ATR_at_entry &times; 3); Short exits when close &gt; lowest_since_entry + (ATR_at_entry &times; 3)</li>
            <li><strong>ATR:</strong> Fixed at entry value for the duration of the trade (never recalculated)</li>
            <li><strong>Timing:</strong> Evaluates at 5:00 PM ET daily (forex daily close), automated via APScheduler</li>
            <li><strong>Reversal:</strong> Closing a Long allows a Short to open the next day if conditions are met (and vice versa)</li>
        </ul>
    </div>
    """


def _build_users_html(current_user_id: int) -> str:
    admins = get_all_admins()
    rows = ""
    for a in admins:
        is_self = a["id"] == current_user_id
        self_badge = ' <span class="badge status-active">YOU</span>' if is_self else ""
        rows += f"""
        <tr data-testid="row-admin-{a['id']}">
            <td>{a['id']}</td>
            <td>{a['username']}{self_badge}</td>
            <td>{a['created_at']}</td>
            <td>
                <button class="btn btn-secondary btn-sm" onclick="editAdmin({a['id']}, '{a['username']}')" data-testid="button-edit-admin-{a['id']}">Edit</button>
                <button class="btn btn-danger btn-sm" onclick="deleteAdmin({a['id']}, '{a['username']}')" data-testid="button-delete-admin-{a['id']}" {'disabled style="opacity:0.5;cursor:not-allowed;"' if len(admins) <= 1 else ''}>Delete</button>
            </td>
        </tr>"""

    return f"""
    <div class="settings-section">
        <h3>Admin Users</h3>
        <p class="settings-desc">Manage admin accounts that can access this dashboard.</p>
        <table class="data-table" data-testid="admin-users-table" style="margin-top:12px;">
            <thead>
                <tr>
                    <th>ID</th>
                    <th>Username</th>
                    <th>Created</th>
                    <th>Actions</th>
                </tr>
            </thead>
            <tbody>{rows}</tbody>
        </table>
    </div>

    <div class="settings-section" style="margin-top:20px;">
        <h3>Add New Admin</h3>
        <div style="display:flex;gap:8px;align-items:center;flex-wrap:wrap;margin-top:12px;">
            <input type="text" id="new-admin-username" placeholder="Username" data-testid="input-new-admin-username"
                style="background:#0f172a;border:1px solid #334155;color:#e2e8f0;padding:10px 14px;border-radius:6px;font-size:0.9rem;width:200px;">
            <input type="password" id="new-admin-password" placeholder="Password" data-testid="input-new-admin-password"
                style="background:#0f172a;border:1px solid #334155;color:#e2e8f0;padding:10px 14px;border-radius:6px;font-size:0.9rem;width:200px;">
            <button class="btn btn-primary" onclick="addAdmin()" data-testid="button-add-admin">Add Admin</button>
        </div>
        <div id="add-admin-result" style="margin-top:12px;"></div>
    </div>

    <div id="edit-modal" class="modal-overlay hidden">
        <div class="modal-card">
            <h3>Edit Admin</h3>
            <input type="hidden" id="edit-admin-id">
            <div class="form-group" style="margin-top:12px;">
                <label style="font-size:0.85rem;color:#94a3b8;margin-bottom:4px;display:block;">Username</label>
                <input type="text" id="edit-admin-username" data-testid="input-edit-admin-username"
                    style="background:#0f172a;border:1px solid #334155;color:#e2e8f0;padding:10px 14px;border-radius:6px;font-size:0.9rem;width:100%;">
            </div>
            <div class="form-group" style="margin-top:12px;">
                <label style="font-size:0.85rem;color:#94a3b8;margin-bottom:4px;display:block;">New Password (leave blank to keep current)</label>
                <input type="password" id="edit-admin-password" data-testid="input-edit-admin-password"
                    style="background:#0f172a;border:1px solid #334155;color:#e2e8f0;padding:10px 14px;border-radius:6px;font-size:0.9rem;width:100%;">
            </div>
            <div id="edit-admin-result" style="margin-top:12px;"></div>
            <div style="display:flex;gap:8px;margin-top:16px;">
                <button class="btn btn-primary" onclick="saveEditAdmin()" data-testid="button-save-edit-admin">Save Changes</button>
                <button class="btn btn-secondary" onclick="closeEditModal()">Cancel</button>
            </div>
        </div>
    </div>
    """


ADMIN_CSS = """
* { margin: 0; padding: 0; box-sizing: border-box; }
body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: #0f172a; color: #e2e8f0; }
header { background: #1e293b; border-bottom: 1px solid #334155; padding: 16px 20px; }
header h1 { font-size: 1.5rem; color: #f8fafc; }
header p { font-size: 0.875rem; color: #94a3b8; margin-top: 4px; }
.layout { display: flex; min-height: calc(100vh - 73px); }
.sidebar { width: 240px; background: #1e293b; border-right: 1px solid #334155; display: flex; flex-direction: column; position: sticky; top: 73px; height: calc(100vh - 73px); overflow-y: auto; flex-shrink: 0; }
.sidebar-nav { flex: 1; padding: 12px; display: flex; flex-direction: column; gap: 2px; }
.sidebar-group { margin-bottom: 16px; }
.sidebar-group-label { font-size: 0.7rem; font-weight: 600; text-transform: uppercase; letter-spacing: 0.05em; color: #64748b; padding: 8px 12px 4px; }
.sidebar-link { display: flex; align-items: center; gap: 10px; padding: 10px 12px; border-radius: 6px; font-size: 0.875rem; font-weight: 500; color: #94a3b8; text-decoration: none; cursor: pointer; transition: all 0.15s; border: 1px solid transparent; }
.sidebar-link:hover { color: #e2e8f0; background: #334155; }
.sidebar-link.active { color: #f8fafc; background: #3b82f6; border-color: #60a5fa; }
.sidebar-link svg { width: 18px; height: 18px; flex-shrink: 0; }
.sidebar-footer { padding: 12px; border-top: 1px solid #334155; }
.sidebar-footer .sidebar-link { color: #94a3b8; }
.sidebar-footer .sidebar-link:hover { color: #e2e8f0; }
.main-content { flex: 1; padding: 24px; min-width: 0; max-width: 1200px; }
.mobile-tab-bar { display: none; background: #1e293b; padding: 4px; border-radius: 8px; margin-bottom: 20px; overflow-x: auto; white-space: nowrap; -webkit-overflow-scrolling: touch; }
.mobile-tab-bar .tab { display: inline-block; padding: 10px 16px; border-radius: 6px; cursor: pointer; font-size: 0.8rem; font-weight: 500; color: #94a3b8; text-decoration: none; transition: all 0.2s; }
.mobile-tab-bar .tab:hover { color: #e2e8f0; background: #334155; }
.mobile-tab-bar .tab.active { color: #f8fafc; background: #3b82f6; }
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
.btn-sm { padding: 4px 10px; font-size: 0.8rem; }
.btn-danger { background: #991b1b; color: #fca5a5; }
.btn-danger:hover { background: #b91c1c; }
.modal-overlay { position: fixed; top: 0; left: 0; width: 100%; height: 100%; background: rgba(0,0,0,0.6); display: flex; align-items: center; justify-content: center; z-index: 1000; }
.modal-card { background: #1e293b; border: 1px solid #334155; border-radius: 12px; padding: 24px; width: 100%; max-width: 440px; }
.user-bar { display: flex; align-items: center; gap: 12px; margin-left: auto; }
.user-bar span { font-size: 0.85rem; color: #94a3b8; }
.user-bar .btn { margin: 0; }
.hidden { display: none; }
@media (max-width: 768px) {
    .layout { flex-direction: column; }
    .sidebar { display: none; }
    .mobile-tab-bar { display: block; }
    .main-content { padding: 16px; }
    .tables-row { grid-template-columns: 1fr; }
    .stats-grid { grid-template-columns: 1fr 1fr; }
}
"""

ADMIN_JS = """
const BASE = window.location.pathname.replace(/\\/admin\\/?$/, '');

function showTab(tabName) {
    document.querySelectorAll('.tab-content').forEach(el => el.classList.add('hidden'));
    document.querySelectorAll('.sidebar-link[data-tab]').forEach(el => el.classList.remove('active'));
    document.querySelectorAll('.mobile-tab-bar .tab').forEach(el => el.classList.remove('active'));
    document.getElementById('tab-' + tabName).classList.remove('hidden');
    var sidebarEl = document.querySelector('.sidebar-link[data-tab="' + tabName + '"]:not([data-strategy])');
    if (sidebarEl) sidebarEl.classList.add('active');
    var mobileEl = document.querySelector('.mobile-tab-bar .tab[data-tab="' + tabName + '"]');
    if (mobileEl) mobileEl.classList.add('active');
    if (tabName === 'settings') loadCreditMeter();
}

function showStrategyTab(strategyName) {
    showTab('signals');
    document.querySelectorAll('.sidebar-link[data-tab]').forEach(el => el.classList.remove('active'));
    var el = document.querySelector('.sidebar-link[data-strategy="' + strategyName + '"]');
    if (el) el.classList.add('active');
    var strategySelect = document.querySelector('.filter-bar select[name="strategy"]');
    if (strategySelect) {
        strategySelect.value = strategyName;
        strategySelect.form.submit();
    }
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

async function addAdmin() {
    const username = document.getElementById('new-admin-username').value.trim();
    const password = document.getElementById('new-admin-password').value;
    const resultDiv = document.getElementById('add-admin-result');
    if (!username || !password) {
        resultDiv.innerHTML = '<div class="result-error">Both username and password are required.</div>';
        return;
    }
    if (password.length < 4) {
        resultDiv.innerHTML = '<div class="result-error">Password must be at least 4 characters.</div>';
        return;
    }
    try {
        const res = await fetch(BASE + '/admin/api/users', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({username, password})
        });
        const data = await res.json();
        if (data.success) {
            resultDiv.innerHTML = '<div class="result-success">Admin "' + username + '" created successfully.</div>';
            document.getElementById('new-admin-username').value = '';
            document.getElementById('new-admin-password').value = '';
            setTimeout(() => window.location.reload(), 1000);
        } else {
            resultDiv.innerHTML = '<div class="result-error">' + (data.error || 'Failed to create admin.') + '</div>';
        }
    } catch (e) {
        resultDiv.innerHTML = '<div class="result-error">Error: ' + e.message + '</div>';
    }
}

function editAdmin(id, username) {
    document.getElementById('edit-admin-id').value = id;
    document.getElementById('edit-admin-username').value = username;
    document.getElementById('edit-admin-password').value = '';
    document.getElementById('edit-admin-result').innerHTML = '';
    document.getElementById('edit-modal').classList.remove('hidden');
}

function closeEditModal() {
    document.getElementById('edit-modal').classList.add('hidden');
}

async function saveEditAdmin() {
    const id = document.getElementById('edit-admin-id').value;
    const username = document.getElementById('edit-admin-username').value.trim();
    const password = document.getElementById('edit-admin-password').value;
    const resultDiv = document.getElementById('edit-admin-result');
    if (!username) {
        resultDiv.innerHTML = '<div class="result-error">Username cannot be empty.</div>';
        return;
    }
    if (password && password.length < 4) {
        resultDiv.innerHTML = '<div class="result-error">Password must be at least 4 characters.</div>';
        return;
    }
    try {
        const body = {username};
        if (password) body.password = password;
        const res = await fetch(BASE + '/admin/api/users/' + id, {
            method: 'PUT',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify(body)
        });
        const data = await res.json();
        if (data.success) {
            resultDiv.innerHTML = '<div class="result-success">Admin updated successfully.</div>';
            setTimeout(() => { closeEditModal(); window.location.reload(); }, 1000);
        } else {
            resultDiv.innerHTML = '<div class="result-error">' + (data.error || 'Failed to update admin.') + '</div>';
        }
    } catch (e) {
        resultDiv.innerHTML = '<div class="result-error">Error: ' + e.message + '</div>';
    }
}

async function deleteAdmin(id, username) {
    if (!confirm('Are you sure you want to delete admin "' + username + '"?')) return;
    try {
        const res = await fetch(BASE + '/admin/api/users/' + id, {method: 'DELETE'});
        const data = await res.json();
        if (data.success) {
            window.location.reload();
        } else {
            alert(data.error || 'Failed to delete admin.');
        }
    } catch (e) {
        alert('Error: ' + e.message);
    }
}

document.addEventListener('DOMContentLoaded', function() {
    const activeTab = document.querySelector('.tab.active');
    if (activeTab && activeTab.getAttribute('data-tab') === 'settings') loadCreditMeter();
});
"""


@router.get("/api/auth-status")
def auth_status(request: Request):
    user = _get_session_user(request)
    if user:
        return JSONResponse(content={"authenticated": True, "username": user.get("username", "")})
    return JSONResponse(content={"authenticated": False})


@router.get("/login", response_class=HTMLResponse)
def login_page(request: Request, error: str = Query("")):
    user = _get_session_user(request)
    if user:
        return RedirectResponse(url=request.scope.get("root_path", "") + "/admin/", status_code=302)
    return HTMLResponse(content=_build_login_page(error))


@router.post("/login")
async def login_submit(request: Request):
    form = await request.form()
    username = form.get("username", "").strip()
    password = form.get("password", "")
    base_path = request.scope.get("root_path", "")

    user = authenticate_admin(username, password)
    if not user:
        return HTMLResponse(content=_build_login_page("Invalid username or password."))

    cleanup_expired_sessions()
    token = create_session(user["id"])
    response = RedirectResponse(url=base_path + "/admin/", status_code=302)
    response.set_cookie(key="admin_session", value=token, httponly=True, samesite="lax", max_age=86400, path="/")
    return response


@router.get("/logout")
def logout(request: Request):
    token = request.cookies.get("admin_session")
    if token:
        delete_session(token)
    base_path = request.scope.get("root_path", "")
    response = RedirectResponse(url=base_path + "/admin/login", status_code=302)
    response.delete_cookie(key="admin_session", path="/")
    return response


@router.get("/", response_class=HTMLResponse)
def admin_dashboard(
    request: Request,
    strategy_name: Optional[str] = Query(None, alias="strategy"),
    status: Optional[str] = Query(None),
    asset: Optional[str] = Query(None, alias="symbol"),
    tab: str = Query("signals"),
):
    user = _get_session_user(request)
    if not user:
        base_path = request.scope.get("root_path", "")
        return RedirectResponse(url=base_path + "/admin/login", status_code=302)

    signals = get_all_signals(strategy_name=strategy_name, asset=asset, status=status, limit=200)
    active_signals = get_active_signals()
    usage_stats = get_api_usage_stats()
    market_times = _get_market_times()

    active_count = len(active_signals)
    total_count = len(signals)

    signal_rows = _signals_to_table_rows(signals)
    credit_html = _build_credit_html(usage_stats)
    timezone_html = _build_timezone_html(market_times)
    settings_html = _build_settings_html()
    users_html = _build_users_html(user["user_id"])

    spx_data = _get_spx_momentum_data()
    spx_signals = get_all_signals(strategy_name="sp500_momentum", limit=200)
    spx_signal_rows = _signals_to_table_rows(spx_signals)
    spx_signal_count = len(spx_signals)
    spx_html = _build_spx_momentum_html(spx_data, spx_signal_rows, spx_signal_count)

    mtf_data = _get_mtf_ema_data()
    mtf_signals = get_all_signals(strategy_name="mtf_ema", limit=200)
    mtf_signal_rows = _signals_to_table_rows(mtf_signals)
    mtf_signal_count = len(mtf_signals)
    mtf_html = _build_mtf_ema_html(mtf_data, mtf_signal_rows, mtf_signal_count)

    fx_trend_data = _get_forex_trend_data()
    fx_trend_signals = get_all_signals(strategy_name="trend_forex", limit=200)
    fx_trend_signal_rows = _signals_to_table_rows(fx_trend_signals)
    fx_trend_signal_count = len(fx_trend_signals)
    forex_trend_html = _build_forex_trend_html(fx_trend_data, fx_trend_signal_rows, fx_trend_signal_count)

    tf_data = _get_trend_following_data()
    tf_signals_forex = get_all_signals(strategy_name="trend_following", limit=200)
    tf_signals_non_forex = get_all_signals(strategy_name="trend_non_forex", limit=200)
    tf_signals_combined = sorted(tf_signals_forex + tf_signals_non_forex, key=lambda s: s.get("id", 0), reverse=True)
    tf_signal_rows = _signals_to_table_rows(tf_signals_combined)
    tf_signal_count = len(tf_signals_combined)
    trend_following_html = _build_trend_following_html(tf_data, tf_signal_rows, tf_signal_count)

    strategy_options = ""
    for s in ["", "mtf_ema", "trend_following", "sp500_momentum", "highest_lowest_fx", "trend_forex"]:
        label = s.replace("_", " ").title() if s else "All Strategies"
        selected = "selected" if s == (strategy_name or "") else ""
        strategy_options += f'<option value="{s}" {selected}>{label}</option>'

    status_options = ""
    for s in ["", "OPEN", "CLOSED"]:
        label = s if s else "All Statuses"
        selected = "selected" if s == (status or "") else ""
        status_options += f'<option value="{s}" {selected}>{label}</option>'

    alert_badge = ""
    if usage_stats["alert_level"]:
        level = usage_stats["alert_level"].upper()
        alert_badge = f' <span class="badge status-{"closed" if usage_stats["alert_level"] == "critical" else "expired"}">{level}</span>'

    logged_in_username = user["username"]

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
        <div style="display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:8px;">
            <div>
                <h1>DailyForex Premium Admin</h1>
                <p>Signal Management &amp; Strategy Monitor</p>
            </div>
            <div class="user-bar">
                <span data-testid="text-logged-in-user">Signed in as <strong>{logged_in_username}</strong></span>
                <a href="logout" class="btn btn-secondary" data-testid="button-logout" style="margin:0;">Logout</a>
            </div>
        </div>
    </header>
    <div class="layout">
        <aside class="sidebar">
            <nav class="sidebar-nav">
                <div class="sidebar-group">
                    <div class="sidebar-group-label">Overview</div>
                    <a class="sidebar-link {'active' if tab == 'signals' else ''}" data-tab="signals" onclick="showTab('signals')" data-testid="sidebar-signals">
                        <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M3 3v18h18"/><path d="m19 9-5 5-4-4-3 3"/></svg>
                        Global Overview
                    </a>
                </div>
                <div class="sidebar-group">
                    <div class="sidebar-group-label">Strategies</div>
                    <a class="sidebar-link {'active' if tab == 'mtf' else ''}" data-tab="mtf" onclick="showTab('mtf')" data-testid="sidebar-mtf">
                        <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M2 20h.01"/><path d="M7 20v-4"/><path d="M12 20v-8"/><path d="M17 20V8"/><path d="M22 4v16"/></svg>
                        MTF Algo
                    </a>
                    <a class="sidebar-link {'active' if tab == 'trend_following' else ''}" data-tab="trend_following" onclick="showTab('trend_following')" data-testid="sidebar-trend">
                        <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="22 7 13.5 15.5 8.5 10.5 2 17"/><polyline points="16 7 22 7 22 13"/></svg>
                        Trend Following
                    </a>
                    <a class="sidebar-link {'active' if tab == 'spx' else ''}" data-tab="spx" onclick="showTab('spx')" data-testid="sidebar-spx">
                        <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect width="18" height="18" x="3" y="3" rx="2"/><path d="M3 9h18"/><path d="M9 21V9"/></svg>
                        SPX 500 Momentum
                    </a>
                    <a class="sidebar-link {'active' if tab == 'forex_trend' else ''}" data-tab="forex_trend" onclick="showTab('forex_trend')" data-testid="sidebar-forex-trend">
                        <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><path d="M2 12h20"/><path d="M12 2a15.3 15.3 0 0 1 4 10 15.3 15.3 0 0 1-4 10 15.3 15.3 0 0 1-4-10 15.3 15.3 0 0 1 4-10z"/></svg>
                        Forex Trend
                    </a>
                </div>
                <div class="sidebar-group">
                    <div class="sidebar-group-label">System</div>
                    <a class="sidebar-link {'active' if tab == 'credits' else ''}" data-tab="credits" onclick="showTab('credits')" data-testid="sidebar-credits">
                        <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><path d="M16 8h-6a2 2 0 1 0 0 4h4a2 2 0 1 1 0 4H8"/><path d="M12 18V6"/></svg>
                        Credit Monitor{alert_badge}
                    </a>
                    <a class="sidebar-link {'active' if tab == 'timezone' else ''}" data-tab="timezone" onclick="showTab('timezone')" data-testid="sidebar-timezone">
                        <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><polyline points="12 6 12 12 16 14"/></svg>
                        Market Hours
                    </a>
                    <a class="sidebar-link {'active' if tab == 'settings' else ''}" data-tab="settings" onclick="showTab('settings')" data-testid="sidebar-settings">
                        <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12.22 2h-.44a2 2 0 0 0-2 2v.18a2 2 0 0 1-1 1.73l-.43.25a2 2 0 0 1-2 0l-.15-.08a2 2 0 0 0-2.73.73l-.22.38a2 2 0 0 0 .73 2.73l.15.1a2 2 0 0 1 1 1.72v.51a2 2 0 0 1-1 1.74l-.15.09a2 2 0 0 0-.73 2.73l.22.38a2 2 0 0 0 2.73.73l.15-.08a2 2 0 0 1 2 0l.43.25a2 2 0 0 1 1 1.73V20a2 2 0 0 0 2 2h.44a2 2 0 0 0 2-2v-.18a2 2 0 0 1 1-1.73l.43-.25a2 2 0 0 1 2 0l.15.08a2 2 0 0 0 2.73-.73l.22-.39a2 2 0 0 0-.73-2.73l-.15-.08a2 2 0 0 1-1-1.74v-.5a2 2 0 0 1 1-1.74l.15-.09a2 2 0 0 0 .73-2.73l-.22-.38a2 2 0 0 0-2.73-.73l-.15.08a2 2 0 0 1-2 0l-.43-.25a2 2 0 0 1-1-1.73V4a2 2 0 0 0-2-2z"/><circle cx="12" cy="12" r="3"/></svg>
                        Settings
                    </a>
                    <a class="sidebar-link {'active' if tab == 'users' else ''}" data-tab="users" onclick="showTab('users')" data-testid="sidebar-users">
                        <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M16 21v-2a4 4 0 0 0-4-4H6a4 4 0 0 0-4 4v2"/><circle cx="9" cy="7" r="4"/><path d="M22 21v-2a4 4 0 0 0-3-3.87"/><path d="M16 3.13a4 4 0 0 1 0 7.75"/></svg>
                        User Settings
                    </a>
                </div>
            </nav>
            <div class="sidebar-footer">
                <a class="sidebar-link" href="/" data-testid="sidebar-back-frontend">
                    <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="m12 19-7-7 7-7"/><path d="M19 12H5"/></svg>
                    Back to Frontend
                </a>
            </div>
        </aside>
        <main class="main-content">
            <div class="mobile-tab-bar">
                <a class="tab {'active' if tab == 'signals' else ''}" data-tab="signals" onclick="showTab('signals')">Overview</a>
                <a class="tab {'active' if tab == 'spx' else ''}" data-tab="spx" onclick="showTab('spx')">SPX 500</a>
                <a class="tab {'active' if tab == 'forex_trend' else ''}" data-tab="forex_trend" onclick="showTab('forex_trend')">FX Trend</a>
                <a class="tab {'active' if tab == 'credits' else ''}" data-tab="credits" onclick="showTab('credits')">Credits</a>
                <a class="tab {'active' if tab == 'timezone' else ''}" data-tab="timezone" onclick="showTab('timezone')">Hours</a>
                <a class="tab {'active' if tab == 'settings' else ''}" data-tab="settings" onclick="showTab('settings')">Settings</a>
                <a class="tab {'active' if tab == 'users' else ''}" data-tab="users" onclick="showTab('users')">Users</a>
            </div>

            <div id="tab-signals" class="tab-content {'hidden' if tab != 'signals' else ''}">
            <div class="section">
                <h2>Trading Signals</h2>
                <div class="filter-bar">
                    <form method="GET" style="display:flex;gap:8px;align-items:center;flex-wrap:wrap;">
                        <input type="hidden" name="tab" value="signals">
                        <select name="strategy" onchange="this.form.submit()">{strategy_options}</select>
                        <select name="status" onchange="this.form.submit()">{status_options}</select>
                        <input type="text" name="symbol" placeholder="Asset (e.g. EUR/USD)" value="{asset or ''}" style="width:160px;">
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
                                <th>Strategy</th>
                                <th>Status</th>
                                <th>Timestamp</th>
                            </tr>
                        </thead>
                        <tbody>{signal_rows}</tbody>
                    </table>
                </div>
            </div>
        </div>

        <div id="tab-mtf" class="tab-content {'hidden' if tab != 'mtf' else ''}">
            <div class="section">
                <h2>Multi-Timeframe EMA Strategy</h2>
                {mtf_html}
            </div>
        </div>

        <div id="tab-spx" class="tab-content {'hidden' if tab != 'spx' else ''}">
            <div class="section">
                <h2>SPX 500 Momentum Strategy</h2>
                {spx_html}
            </div>
        </div>

        <div id="tab-forex_trend" class="tab-content {'hidden' if tab != 'forex_trend' else ''}">
            <div class="section">
                <h2>Forex Trend Following Strategy</h2>
                {forex_trend_html}
            </div>
        </div>

        <div id="tab-trend_following" class="tab-content {'hidden' if tab != 'trend_following' else ''}">
            <div class="section">
                <h2>Trend Following Strategy</h2>
                {trend_following_html}
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

        <div id="tab-users" class="tab-content {'hidden' if tab != 'users' else ''}">
            <div class="section">
                <h2>User Settings</h2>
                {users_html}
            </div>
        </div>
        </main>
    </div>
    <script>{ADMIN_JS}</script>
</body>
</html>"""
    return HTMLResponse(content=html)


def _auth_guard(request: Request):
    user = _get_session_user(request)
    if not user:
        return JSONResponse(content={"error": "Unauthorized"}, status_code=401)
    return None


@router.get("/export")
def export_signals(
    request: Request,
    format: str = Query("csv", description="Export format: csv or json"),
    strategy: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    symbol: Optional[str] = Query(None),
):
    guard = _auth_guard(request)
    if guard:
        return guard

    signals = get_all_signals(strategy_name=strategy, asset=symbol, status=status, limit=500)

    if format == "json":
        content = json.dumps(signals, indent=2, default=str)
        return StreamingResponse(
            io.BytesIO(content.encode()),
            media_type="application/json",
            headers={"Content-Disposition": f"attachment; filename=signals_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json"},
        )

    output = io.StringIO()
    if signals:
        fields = ["asset", "direction", "entry_price", "stop_loss", "take_profit",
                  "atr_at_entry", "strategy_name", "status", "signal_timestamp", "created_at",
                  "updated_at"]
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
def api_usage_stats(request: Request):
    guard = _auth_guard(request)
    if guard:
        return guard
    stats = get_api_usage_stats()
    return JSONResponse(content=stats)


@router.get("/api/market-times")
def market_times(request: Request):
    guard = _auth_guard(request)
    if guard:
        return guard
    times = _get_market_times()
    return JSONResponse(content=times)


@router.post("/api/settings/key")
def save_api_key(request: Request, body: dict = Body(...)):
    guard = _auth_guard(request)
    if guard:
        return guard
    api_key = body.get("api_key", "").strip()
    if not api_key:
        return JSONResponse(content={"success": False, "error": "API key cannot be empty"})
    set_setting("fcsapi_key", api_key)
    return JSONResponse(content={"success": True, "message": "API key saved successfully"})


@router.post("/api/settings/test-connection")
def test_api_connection(request: Request):
    guard = _auth_guard(request)
    if guard:
        return guard
    from trading_engine.fcsapi_client import FCSAPIClient
    client = FCSAPIClient()
    result = client.test_connection()
    return JSONResponse(content=result)


@router.get("/api/settings")
def get_settings(request: Request):
    guard = _auth_guard(request)
    if guard:
        return guard
    db_key = get_setting("fcsapi_key")
    env_key = os.environ.get("FCSAPI_KEY", "")
    has_db_key = bool(db_key)
    has_env_key = bool(env_key)
    source = "database" if has_db_key else ("environment" if has_env_key else "none")
    return JSONResponse(content={
        "api_key_configured": has_db_key or has_env_key,
        "key_source": source,
    })


@router.post("/api/users")
def api_create_admin(request: Request, body: dict = Body(...)):
    guard = _auth_guard(request)
    if guard:
        return guard
    username = body.get("username", "").strip()
    password = body.get("password", "")
    if not username or not password:
        return JSONResponse(content={"success": False, "error": "Username and password are required."})
    if len(password) < 4:
        return JSONResponse(content={"success": False, "error": "Password must be at least 4 characters."})
    admin_id = create_admin(username, password)
    if admin_id is None:
        return JSONResponse(content={"success": False, "error": f'Username "{username}" already exists.'})
    return JSONResponse(content={"success": True, "id": admin_id})


@router.put("/api/users/{admin_id}")
def api_update_admin(request: Request, admin_id: int, body: dict = Body(...)):
    guard = _auth_guard(request)
    if guard:
        return guard
    username = body.get("username", "").strip()
    password = body.get("password", "")
    if not username:
        return JSONResponse(content={"success": False, "error": "Username cannot be empty."})
    if password and len(password) < 4:
        return JSONResponse(content={"success": False, "error": "Password must be at least 4 characters."})
    existing = get_admin_by_id(admin_id)
    if not existing:
        return JSONResponse(content={"success": False, "error": "Admin not found."})
    success = update_admin(admin_id, username=username, password=password if password else None)
    if not success:
        return JSONResponse(content={"success": False, "error": f'Username "{username}" already exists.'})
    return JSONResponse(content={"success": True})


@router.delete("/api/users/{admin_id}")
def api_delete_admin(request: Request, admin_id: int):
    guard = _auth_guard(request)
    if guard:
        return guard
    existing = get_admin_by_id(admin_id)
    if not existing:
        return JSONResponse(content={"success": False, "error": "Admin not found."})
    success = delete_admin(admin_id)
    if not success:
        return JSONResponse(content={"success": False, "error": "Cannot delete the last admin user."})
    return JSONResponse(content={"success": True})


@router.get("/api/spx-momentum")
def api_spx_momentum(request: Request):
    guard = _auth_guard(request)
    if guard:
        return guard
    data = _get_spx_momentum_data()
    return JSONResponse(content=data)


@router.get("/api/users")
def api_list_admins(request: Request):
    guard = _auth_guard(request)
    if guard:
        return guard
    admins = get_all_admins()
    return JSONResponse(content={"admins": admins})
