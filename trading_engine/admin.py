import csv
import io
import json
import logging
import os
from datetime import datetime, timezone, timedelta

logger = logging.getLogger("trading_engine.admin")
from fastapi import APIRouter, Query, Body, Request, Response, Cookie, Depends
from fastapi.responses import HTMLResponse, StreamingResponse, JSONResponse, RedirectResponse
from typing import Optional

from trading_engine.database import (
    get_all_signals, get_active_signals, get_api_usage_stats, get_setting, set_setting,
    authenticate_admin, create_session, validate_session, delete_session,
    get_all_admins, create_admin, update_admin, delete_admin, get_admin_by_id,
    cleanup_expired_sessions, get_candles,
    get_all_open_positions, get_open_position,
    get_recent_job_logs, get_scheduler_health_summary,
    create_partner_api_key, list_partner_api_keys, toggle_partner_api_key, delete_partner_api_key,
)
from trading_engine.indicators import IndicatorEngine

router = APIRouter(prefix="/admin", tags=["admin"], include_in_schema=False)


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


def _build_login_page(error: str = "", success: str = "") -> str:
    error_html = f'<div class="error-msg">{error}</div>' if error else ""
    success_html = f'<div class="success-msg" data-testid="text-login-success">{success}</div>' if success else ""
    reg_val = get_setting("registration_enabled")
    reg_enabled = reg_val != "false"
    reg_link = '<div class="link-row">Don\'t have an account? <a href="/api/v1/auth/register" data-testid="link-register">Create one</a></div>' if reg_enabled else ""
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Trading Engine Admin - Login</title>
    <style>{LOGIN_CSS}
.success-msg {{ background: #052e16; border: 1px solid #166534; color: #86efac; padding: 10px 14px; border-radius: 6px; font-size: 0.85rem; margin-bottom: 16px; text-align: center; }}
.link-row {{ text-align: center; margin-top: 16px; font-size: 0.85rem; color: #94a3b8; }}
.link-row a {{ color: #3b82f6; text-decoration: none; }}
.link-row a:hover {{ text-decoration: underline; }}</style>
</head>
<body>
    <div class="login-card">
        <h1>Trading Engine Admin</h1>
        <p>Sign in to access the dashboard</p>
        {error_html}
        {success_html}
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
        {reg_link}
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
                <span class="badge" style="background:#1e3a5f;color:#93c5fd;">OSX</span>
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
    non_forex_symbols = ["SPX", "NDX", "XAU/USD", "XAG/USD", "OSX", "BTC/USD", "ETH/USD"]
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

    forex_data = [_compute_symbol_data(s, long_only=True) for s in forex_symbols]
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
        <h3>Non-Forex Breakout Conditions (D1) <span style="font-size:0.75rem;color:#94a3b8;font-weight:normal;">LONG ONLY</span></h3>
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
        <strong>Strategy Rules (Trend Following &mdash; LONG ONLY):</strong>
        <ul>
            <li><strong>Forex Entry (LONG):</strong> Close &ge; highest close of last 50 days AND SMA(50) &gt; SMA(100)</li>
            <li><strong>Non-Forex Entry (LONG):</strong> Close &ge; highest close of last 50 days AND SMA(50) &gt; SMA(100)</li>
            <li><strong>Trailing Stop:</strong> Highest close since entry &minus; (Fixed ATR at entry &times; 3)</li>
            <li><strong>Exit Rule:</strong> Closing-rule gate &mdash; only the 4:59 PM ET close is evaluated, intraday spikes are ignored</li>
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


def _get_hlc_fx_data() -> dict:
    from zoneinfo import ZoneInfo
    from trading_engine.utils.holiday_manager import is_trading_holiday as _is_holiday
    et_zone = ZoneInfo("America/New_York")
    et_now = datetime.now(et_zone)
    et_minutes = et_now.hour * 60 + et_now.minute
    ny_dst = bool(et_now.dst() and et_now.dst().total_seconds() > 0)

    in_window = et_now.hour in (9, 10)
    is_holiday_today = _is_holiday(et_now)

    symbol = "EUR/USD"
    h1_candles = get_candles(symbol, "1H", 300)
    d1_candles = get_candles(symbol, "D1", 200)

    sym_info = {
        "symbol": symbol,
        "current_price": None,
        "highest_50d": None,
        "lowest_50d": None,
        "h1_atr100": None,
        "h1_candle_count": len(h1_candles),
        "d1_candle_count": len(d1_candles),
        "prev_day_high": None,
        "prev_day_low": None,
        "reversal_threshold": None,
    }

    if len(h1_candles) >= 100:
        h1_closes = [c["close"] for c in h1_candles]
        h1_highs = [c["high"] for c in h1_candles]
        h1_lows = [c["low"] for c in h1_candles]
        sym_info["current_price"] = h1_closes[-1]

        atr_vals = IndicatorEngine.atr(h1_highs, h1_lows, h1_closes, 100)
        sym_info["h1_atr100"] = atr_vals[-1] if atr_vals else None

    if len(d1_candles) >= 50:
        d_closes = [c["close"] for c in d1_candles]
        sym_info["highest_50d"] = max(d_closes[-50:])
        sym_info["lowest_50d"] = min(d_closes[-50:])
        if sym_info["lowest_50d"] is not None:
            sym_info["reversal_threshold"] = sym_info["lowest_50d"] * 0.998

        today = et_now.date()
        for candle in reversed(d1_candles):
            ts = candle.get("timestamp", "")
            try:
                if isinstance(ts, datetime):
                    c_date = ts.date()
                else:
                    c_date = datetime.strptime(str(ts)[:10], "%Y-%m-%d").date()
            except (ValueError, TypeError):
                continue
            if c_date >= today:
                continue
            if c_date.weekday() >= 5:
                continue
            if _is_holiday(c_date):
                continue
            sym_info["prev_day_high"] = candle.get("high")
            sym_info["prev_day_low"] = candle.get("low")
            break

    active_trades = get_active_signals(strategy_name="highest_lowest_fx")
    open_positions = get_all_open_positions(strategy_name="highest_lowest_fx")
    pos_by_asset = {p["asset"]: p for p in open_positions}
    trade_details = []
    for sig in active_trades:
        atr_at_entry = sig.get("atr_at_entry")
        direction = sig["direction"]
        entry_price = sig["entry_price"]
        pos = pos_by_asset.get(sig["asset"])

        if direction == "BUY":
            stored_extreme = (pos.get("highest_price_since_entry") if pos else None) or entry_price
            sym_h1 = get_candles(sig["asset"], "1H", 5)
            cur_close = sym_h1[-1]["close"] if sym_h1 else None
            if cur_close:
                stored_extreme = max(stored_extreme, cur_close)
            trailing_stop = None
            if atr_at_entry is not None:
                trailing_stop = stored_extreme - (atr_at_entry * 0.25)
            take_profit = None
            if atr_at_entry is not None:
                take_profit = entry_price + (atr_at_entry * 6.0)
            trade_details.append({
                "id": sig["id"],
                "symbol": sig["asset"],
                "direction": direction,
                "entry_price": entry_price,
                "atr_at_entry": atr_at_entry,
                "extreme_price": stored_extreme,
                "trailing_stop": trailing_stop,
                "take_profit": take_profit,
                "current_close": cur_close,
                "created_at": sig.get("created_at"),
            })
        elif direction == "SELL":
            stored_extreme = (pos.get("lowest_price_since_entry") if pos else None) or entry_price
            sym_h1 = get_candles(sig["asset"], "1H", 5)
            cur_close = sym_h1[-1]["close"] if sym_h1 else None
            if cur_close:
                stored_extreme = min(stored_extreme, cur_close)
            trailing_stop = None
            if atr_at_entry is not None:
                trailing_stop = stored_extreme + (atr_at_entry * 0.25)
            take_profit = None
            if atr_at_entry is not None:
                take_profit = entry_price - (atr_at_entry * 6.0)
            trade_details.append({
                "id": sig["id"],
                "symbol": sig["asset"],
                "direction": direction,
                "entry_price": entry_price,
                "atr_at_entry": atr_at_entry,
                "extreme_price": stored_extreme,
                "trailing_stop": trailing_stop,
                "take_profit": take_profit,
                "current_close": cur_close,
                "created_at": sig.get("created_at"),
            })

    return {
        "et_time": et_now.strftime(f"%Y-%m-%d %H:%M:%S {'EDT' if ny_dst else 'EST'}"),
        "in_window": in_window,
        "is_holiday": is_holiday_today,
        "dst_active": ny_dst,
        "symbol_info": sym_info,
        "active_trades": trade_details,
    }


def _build_hlc_fx_html(hlc_data: dict, hlc_signal_rows: str, hlc_signal_count: int) -> str:
    in_window = hlc_data["in_window"]
    is_holiday = hlc_data.get("is_holiday", False)
    if is_holiday:
        window_badge = '<span class="badge status-expired">HOLIDAY</span>'
    elif in_window:
        window_badge = '<span class="badge status-active">IN WINDOW</span>'
    else:
        window_badge = '<span class="badge status-closed">OUTSIDE WINDOW</span>'

    sym = hlc_data["symbol_info"]
    price_display = f"{sym['current_price']:.5f}" if sym["current_price"] is not None else "N/A"
    high_display = f"{sym['highest_50d']:.5f}" if sym["highest_50d"] is not None else "N/A"
    low_display = f"{sym['lowest_50d']:.5f}" if sym["lowest_50d"] is not None else "N/A"
    atr_display = f"{sym['h1_atr100']:.5f}" if sym["h1_atr100"] is not None else "N/A"
    prev_high_display = f"{sym['prev_day_high']:.5f}" if sym["prev_day_high"] is not None else "N/A"
    prev_low_display = f"{sym['prev_day_low']:.5f}" if sym["prev_day_low"] is not None else "N/A"
    reversal_display = f"{sym['reversal_threshold']:.5f}" if sym["reversal_threshold"] is not None else "N/A"

    breakout_long = ""
    breakout_short = ""
    if sym["current_price"] is not None and sym["highest_50d"] is not None:
        if sym["current_price"] >= sym["highest_50d"]:
            breakout_long = ' <span class="badge buy">BREAKOUT</span>'
    if sym["current_price"] is not None and sym["lowest_50d"] is not None:
        if sym["current_price"] <= sym["lowest_50d"]:
            breakout_short = ' <span class="badge sell">BREAKDOWN</span>'

    symbol_html = f"""
    <div class="settings-section" style="margin-bottom:16px;" data-testid="hlc-fx-symbol-{sym['symbol'].replace('/','-')}">
        <h3>{sym['symbol']} <span style="font-size:0.8rem;color:#94a3b8;font-weight:400;">{sym['h1_candle_count']} H1 / {sym['d1_candle_count']} D1 candles</span></h3>
        <div class="stats-grid" style="margin-top:12px;">
            <div class="stat-card">
                <div class="stat-label">Current Price</div>
                <div class="stat-value" style="font-size:1.2rem;">{price_display}</div>
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
                <div class="stat-label">H1 ATR(100)</div>
                <div class="stat-value" style="font-size:1.2rem;">{atr_display}</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">Prev Day High</div>
                <div class="stat-value" style="font-size:1.2rem;">{prev_high_display}</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">Prev Day Low</div>
                <div class="stat-value" style="font-size:1.2rem;">{prev_low_display}</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">Reversal Threshold (0.998)</div>
                <div class="stat-value" style="font-size:1.2rem;">{reversal_display}</div>
            </div>
        </div>
    </div>"""

    active_html = ""
    if hlc_data["active_trades"]:
        trade_rows = ""
        for t in hlc_data["active_trades"]:
            dir_class = "buy" if t["direction"] == "BUY" else "sell"
            entry_display = f"{t['entry_price']:.5f}"
            atr_display_t = f"{t['atr_at_entry']:.6f}" if t["atr_at_entry"] is not None else "N/A"
            extreme_label = "Peak" if t["direction"] == "BUY" else "Trough"
            extreme_display = f"{t['extreme_price']:.5f}"
            trail_display = f"{t['trailing_stop']:.5f}" if t["trailing_stop"] is not None else "N/A"
            tp_display = f"{t['take_profit']:.5f}" if t["take_profit"] is not None else "N/A"
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
            <tr data-testid="row-hlc-trade-{t['id']}">
                <td>{t['symbol']}</td>
                <td><span class="badge {dir_class}">{t['direction']}</span></td>
                <td>{entry_display}</td>
                <td>{atr_display_t}</td>
                <td>{extreme_display} <span style="color:#64748b;font-size:0.75rem;">({extreme_label})</span></td>
                <td style="color:#fbbf24;font-weight:600;">{trail_display}</td>
                <td style="color:#3b82f6;">{tp_display}</td>
                <td>{cur_display}</td>
                <td>{pnl}</td>
                <td>{t.get('created_at', 'N/A')}</td>
            </tr>"""

        active_html = f"""
        <div class="settings-section" style="margin-top:20px;border-left:3px solid #8b5cf6;">
            <h3>Active Trades ({len(hlc_data['active_trades'])})</h3>
            <div style="overflow-x:auto;margin-top:12px;">
                <table class="data-table" data-testid="hlc-fx-active-table">
                    <thead>
                        <tr>
                            <th>Symbol</th>
                            <th>Direction</th>
                            <th>Entry Price</th>
                            <th>Entry ATR(100)</th>
                            <th>Tracked Extreme</th>
                            <th>Trailing Stop</th>
                            <th>Take Profit</th>
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
            <p style="color:#94a3b8;padding:16px 0;">No active Highest/Lowest FX trades.</p>
        </div>"""

    return f"""
    <div class="stats-grid">
        <div class="stat-card">
            <div class="stat-label">Eval Window (9:00 &amp; 10:00 AM ET)</div>
            <div style="margin-top:8px;">{window_badge}</div>
            <div class="stat-label" style="margin-top:8px;">{hlc_data['et_time']}</div>
            <div class="stat-label">DST: {'Active' if hlc_data['dst_active'] else 'Inactive'}</div>
        </div>
        <div class="stat-card">
            <div class="stat-label">Target Asset</div>
            <div class="stat-value" style="font-size:1.2rem;">EUR/USD</div>
            <div class="stat-label" style="margin-top:4px;">H1 candles + D1 lookback</div>
        </div>
        <div class="stat-card">
            <div class="stat-label">Scheduler</div>
            <div class="stat-value" style="font-size:1.2rem;color:#6ee7b7;">Active</div>
            <div class="stat-label" style="margin-top:4px;">APScheduler @ 09:00 &amp; 10:00 ET</div>
        </div>
    </div>
    {symbol_html}
    {active_html}
    <div class="settings-section" style="margin-top:20px;">
        <h3>Signal History ({hlc_signal_count})</h3>
        <div style="overflow-x:auto;margin-top:12px;">
            <table class="data-table" data-testid="hlc-fx-signals-table">
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
                <tbody>{hlc_signal_rows}</tbody>
            </table>
        </div>
    </div>
    <div class="timezone-note" style="margin-top:16px;">
        <strong>Strategy Rules:</strong>
        <ul>
            <li><strong>Long Entry:</strong> Price &ge; 50-day highest close (D1 lookback)</li>
            <li><strong>Reversal Long:</strong> Price near 50-day lowest close (within 0.2%) &mdash; potential bounce</li>
            <li><strong>Short Entry:</strong> Price &le; 50-day lowest close AND below reversal threshold</li>
            <li><strong>Previous Day Filter:</strong> Long blocked if price &lt; prev day low; Short blocked if price &gt; prev day high</li>
            <li><strong>Holiday Filter:</strong> Skips US &amp; JP public holidays</li>
            <li><strong>Exit (Trailing Stop):</strong> Long exits when close &lt; peak - 0.25&times;ATR(100); Short exits when close &gt; trough + 0.25&times;ATR(100)</li>
            <li><strong>Take Profit:</strong> 6&times; H1 ATR(100) from entry</li>
            <li><strong>ATR:</strong> H1 ATR(100), fixed at entry value (never recalculated)</li>
            <li><strong>Timing:</strong> Evaluates at 9:00 AM and 10:00 AM ET only, automated via APScheduler</li>
        </ul>
    </div>
    """


def _get_signal_analysis_data() -> dict:
    from zoneinfo import ZoneInfo
    from trading_engine.utils.holiday_manager import is_trading_holiday as _is_holiday

    et_zone = ZoneInfo("America/New_York")
    et_now = datetime.now(et_zone)
    ny_dst = bool(et_now.dst() and et_now.dst().total_seconds() > 0)
    is_holiday = _is_holiday(et_now)
    et_hour = et_now.hour

    all_signals = get_all_signals(limit=500)
    all_positions = get_all_open_positions()
    pos_map = {}
    for p in all_positions:
        key = f"{p.get('strategy_name','')}|{p.get('asset','')}"
        pos_map[key] = p
    sig_by_strat = {}
    for s in all_signals:
        sn = s.get("strategy_name", "")
        sig_by_strat.setdefault(sn, []).append(s)

    trend_nf_symbols = ["SPX", "NDX", "XAU/USD", "XAG/USD", "OSX", "BTC/USD", "ETH/USD"]
    trend_fx_symbols = ["EUR/USD", "USD/JPY", "GBP/USD", "AUD/USD", "NZD/USD", "USD/CAD", "USD/CHF", "EUR/GBP"]
    mtf_symbols = ["SPX", "NDX", "RUT", "XAU/USD", "XAG/USD", "OSX", "BTC/USD", "ETH/USD", "EUR/USD", "USD/JPY", "GBP/USD", "AUD/USD"]

    def _dp(sym):
        return 5 if "/" in sym else 2

    trend_nf_rows = []
    for sym in trend_nf_symbols:
        d1 = get_candles(sym, "D1", 300)
        dp = _dp(sym)
        row = {"symbol": sym, "candles": len(d1), "status": "insufficient", "close": None,
               "hi50": None, "lo50": None, "sma50": None, "sma100": None, "atr100": None,
               "pct_from_hi": None, "pct_from_lo": None, "sma_bias": None,
               "long_met": False, "short_met": False, "dp": dp,
               "position": pos_map.get(f"trend_non_forex|{sym}")}
        if len(d1) >= 101:
            closes = [c["close"] for c in d1]
            highs = [c["high"] for c in d1]
            lows = [c["low"] for c in d1]
            sma50 = IndicatorEngine.sma(closes, 50)[-1]
            sma100 = IndicatorEngine.sma(closes, 100)[-1]
            atr = IndicatorEngine.atr(highs, lows, closes, 100)
            atr_val = atr[-1] if atr else None
            hi50 = max(closes[-51:-1])
            lo50 = min(closes[-51:-1])
            cur = closes[-1]
            row.update({
                "status": "ready", "close": cur, "hi50": hi50, "lo50": lo50,
                "sma50": sma50, "sma100": sma100, "atr100": atr_val,
                "pct_from_hi": (cur - hi50) / hi50 * 100,
                "pct_from_lo": (cur - lo50) / lo50 * 100,
                "sma_bias": "BULL" if sma50 > sma100 else "BEAR",
                "long_met": cur > hi50 and sma50 > sma100,
                "short_met": cur < lo50 and sma50 < sma100,
            })
        trend_nf_rows.append(row)

    trend_fx_rows = []
    for sym in trend_fx_symbols:
        d1 = get_candles(sym, "D1", 300)
        row = {"symbol": sym, "candles": len(d1), "status": "insufficient", "close": None,
               "hi50": None, "lo50": None, "sma50": None, "sma100": None, "atr100": None,
               "pct_from_hi": None, "pct_from_lo": None, "sma_bias": None,
               "long_met": False, "short_met": False, "dp": 5,
               "position": pos_map.get(f"trend_forex|{sym}")}
        if len(d1) >= 101:
            closes = [c["close"] for c in d1]
            highs = [c["high"] for c in d1]
            lows = [c["low"] for c in d1]
            sma50 = IndicatorEngine.sma(closes, 50)[-1]
            sma100 = IndicatorEngine.sma(closes, 100)[-1]
            atr = IndicatorEngine.atr(highs, lows, closes, 100)
            atr_val = atr[-1] if atr else None
            hi50 = max(closes[-51:-1])
            lo50 = min(closes[-51:-1])
            cur = closes[-1]
            row.update({
                "status": "ready", "close": cur, "hi50": hi50, "lo50": lo50,
                "sma50": sma50, "sma100": sma100, "atr100": atr_val,
                "pct_from_hi": (cur - hi50) / hi50 * 100,
                "pct_from_lo": (cur - lo50) / lo50 * 100,
                "sma_bias": "BULL" if sma50 > sma100 else "BEAR",
                "long_met": cur > hi50 and sma50 > sma100,
                "short_met": cur < lo50 and sma50 < sma100,
            })
        trend_fx_rows.append(row)

    hlc_row = {"symbol": "EUR/USD", "status": "insufficient", "close": None,
               "hi50": None, "lo50": None, "h1_atr": None, "rev_threshold": None,
               "prev_high": None, "prev_low": None,
               "long_met": False, "short_met": False, "reversal_met": False,
               "window_active": et_hour in (9, 10),
               "holiday_blocked": is_holiday,
               "position": pos_map.get("highest_lowest_fx|EUR/USD")}
    h1 = get_candles("EUR/USD", "1H", 300)
    d1 = get_candles("EUR/USD", "D1", 200)
    if len(h1) >= 100 and len(d1) >= 50:
        h1c = [c["close"] for c in h1]
        h1h = [c["high"] for c in h1]
        h1l = [c["low"] for c in h1]
        d1c = [c["close"] for c in d1]
        hi50 = max(d1c[-50:])
        lo50 = min(d1c[-50:])
        atr = IndicatorEngine.atr(h1h, h1l, h1c, 100)
        atr_val = atr[-1] if atr else None
        cur = h1c[-1]
        rev = lo50 * 0.998
        today = et_now.date()
        prev_high = None
        prev_low = None
        for candle in reversed(d1):
            ts = candle.get("timestamp", "")
            try:
                if isinstance(ts, datetime):
                    c_date = ts.date()
                else:
                    c_date = datetime.strptime(str(ts)[:10], "%Y-%m-%d").date()
            except (ValueError, TypeError):
                continue
            if c_date >= today or c_date.weekday() >= 5 or _is_holiday(c_date):
                continue
            prev_high = candle.get("high")
            prev_low = candle.get("low")
            break
        hlc_row.update({
            "status": "ready", "close": cur, "hi50": hi50, "lo50": lo50,
            "h1_atr": atr_val, "rev_threshold": rev,
            "prev_high": prev_high, "prev_low": prev_low,
            "long_met": cur >= hi50,
            "short_met": cur <= lo50 and cur <= rev,
            "reversal_met": cur <= lo50 and cur > rev,
        })

    spx_row = {"symbol": "SPX", "status": "insufficient", "close": None, "rsi20": None,
               "long_met": False, "in_session": False,
               "position": pos_map.get("sp500_momentum|SPX")}
    m30 = get_candles("SPX", "30m", 300)
    if len(m30) >= 20:
        closes_30 = [c["close"] for c in m30]
        rsi = IndicatorEngine.rsi(closes_30, 20)
        rsi_val = rsi[-1] if rsi else None
        in_session = 9 * 60 + 30 <= et_hour * 60 + et_now.minute < 15 * 60 + 30
        spx_row.update({
            "status": "ready", "close": closes_30[-1], "rsi20": rsi_val,
            "long_met": bool(rsi_val and rsi_val > 70),
            "in_session": in_session,
        })

    mtf_rows = []
    for sym in mtf_symbols:
        dp = _dp(sym)
        row = {"symbol": sym, "dp": dp, "timeframes": {},
               "all_bull": False, "all_bear": False,
               "position": pos_map.get(f"mtf_ema|{sym}")}
        bull_count = 0
        bear_count = 0
        for tf in ["D1", "4H", "1H"]:
            candles = get_candles(sym, tf, 300)
            tf_data = {"candles": len(candles), "status": "insufficient"}
            if len(candles) >= 200:
                closes = [c["close"] for c in candles]
                ema20 = IndicatorEngine.ema(closes, 20)[-1]
                ema50 = IndicatorEngine.ema(closes, 50)[-1]
                ema200 = IndicatorEngine.ema(closes, 200)[-1]
                cur = closes[-1]
                bull = ema20 > ema50 > ema200 and cur > ema20
                bear = ema20 < ema50 < ema200 and cur < ema20
                if bull:
                    bull_count += 1
                if bear:
                    bear_count += 1
                tf_data = {
                    "candles": len(candles), "status": "ready",
                    "close": cur, "ema20": ema20, "ema50": ema50, "ema200": ema200,
                    "bull": bull, "bear": bear,
                }
            row["timeframes"][tf] = tf_data
        row["all_bull"] = bull_count == 3
        row["all_bear"] = bear_count == 3
        mtf_rows.append(row)

    return {
        "et_time": et_now.strftime(f"%Y-%m-%d %H:%M:%S {'EDT' if ny_dst else 'EST'}"),
        "et_hour": et_hour,
        "is_holiday": is_holiday,
        "dst_active": ny_dst,
        "signal_counts": {k: len(v) for k, v in sig_by_strat.items()},
        "total_signals": len(all_signals),
        "open_positions": len(all_positions),
        "trend_nf": trend_nf_rows,
        "trend_fx": trend_fx_rows,
        "hlc": hlc_row,
        "spx": spx_row,
        "mtf": mtf_rows,
    }


def _build_signal_analysis_html(data: dict) -> str:
    summary_cards = f"""
    <div class="stats-grid" style="margin-bottom:24px;">
        <div class="stat-card">
            <div class="stat-label">Current Time</div>
            <div class="stat-value" style="font-size:1rem;">{data['et_time']}</div>
            <div class="stat-label" style="margin-top:4px;">DST: {'Active' if data['dst_active'] else 'Inactive'}{' | <span style=&quot;color:#f59e0b;&quot;>HOLIDAY</span>' if data['is_holiday'] else ''}</div>
        </div>
        <div class="stat-card">
            <div class="stat-label">Total Signals (All Time)</div>
            <div class="stat-value" data-testid="text-total-signals">{data['total_signals']}</div>
        </div>
        <div class="stat-card">
            <div class="stat-label">Open Positions</div>
            <div class="stat-value" data-testid="text-open-positions">{data['open_positions']}</div>
        </div>
    </div>
    """

    def _fmt(val, dp):
        return f"{val:.{dp}f}" if val is not None else '<span style="color:#64748b;">N/A</span>'

    def _pct_bar(pct_hi, pct_lo):
        if pct_hi is None or pct_lo is None:
            return ""
        total = abs(pct_hi) + abs(pct_lo)
        if total == 0:
            return ""
        pos_pct = abs(pct_lo) / total * 100
        return f"""<div style="margin-top:6px;height:6px;background:#334155;border-radius:3px;position:relative;overflow:hidden;">
            <div style="position:absolute;left:0;top:0;height:100%;width:{pos_pct:.1f}%;background:linear-gradient(90deg,#ef4444,#f59e0b,#22c55e);border-radius:3px;"></div>
        </div>
        <div style="display:flex;justify-content:space-between;font-size:11px;color:#64748b;margin-top:2px;">
            <span>Low ({pct_lo:+.1f}%)</span><span>High ({pct_hi:+.1f}%)</span>
        </div>"""

    def _cond_badge(met, label, blocked=False):
        if blocked:
            return f'<span class="badge status-expired" style="font-size:11px;">{label}: BLOCKED</span>'
        if met:
            return f'<span class="badge buy" style="font-size:11px;">{label}: MET</span>'
        return f'<span class="badge" style="font-size:11px;background:rgba(100,116,139,0.15);color:#94a3b8;">{label}: NOT MET</span>'

    def _pos_badge(pos):
        if not pos:
            return '<span style="color:#64748b;font-size:12px;">No position</span>'
        d = pos.get("direction", "")
        dc = "buy" if d == "BUY" else "sell"
        return f'<span class="badge {dc}" style="font-size:11px;">OPEN {d}</span>'

    trend_nf_html = ""
    for r in data["trend_nf"]:
        dp = r["dp"]
        if r["status"] == "insufficient":
            trend_nf_html += f"""<tr data-testid="row-analysis-tnf-{r['symbol'].replace('/','-')}">
                <td style="font-weight:600;">{r['symbol']}</td>
                <td colspan="7" style="color:#64748b;">Insufficient data ({r['candles']} candles, need 101+)</td>
                <td>{_pos_badge(r['position'])}</td></tr>"""
            continue
        sma_color = "#6ee7b7" if r["sma_bias"] == "BULL" else "#fca5a5"
        trend_nf_html += f"""<tr data-testid="row-analysis-tnf-{r['symbol'].replace('/','-')}">
            <td style="font-weight:600;">{r['symbol']}</td>
            <td>{_fmt(r['close'], dp)}</td>
            <td>{_fmt(r['hi50'], dp)} <span style="color:#64748b;font-size:11px;">({r['pct_from_hi']:+.2f}%)</span></td>
            <td>{_fmt(r['lo50'], dp)} <span style="color:#64748b;font-size:11px;">({r['pct_from_lo']:+.2f}%)</span></td>
            <td><span style="color:{sma_color};font-weight:600;">{r['sma_bias']}</span>
                <span style="color:#64748b;font-size:11px;display:block;">{_fmt(r['sma50'], dp)} / {_fmt(r['sma100'], dp)}</span></td>
            <td>{_fmt(r['atr100'], dp)}</td>
            <td>{_cond_badge(r['long_met'], 'LONG')}</td>
            <td>{_cond_badge(r['short_met'], 'SHORT')}</td>
            <td>{_pos_badge(r['position'])}</td></tr>"""

    trend_fx_html = ""
    for r in data["trend_fx"]:
        if r["status"] == "insufficient":
            trend_fx_html += f"""<tr data-testid="row-analysis-tfx-{r['symbol'].replace('/','-')}">
                <td style="font-weight:600;">{r['symbol']}</td>
                <td colspan="7" style="color:#f59e0b;">No data &mdash; awaiting first scheduler run (5:00 PM ET)</td>
                <td>{_pos_badge(r['position'])}</td></tr>"""
            continue
        sma_color = "#6ee7b7" if r["sma_bias"] == "BULL" else "#fca5a5"
        trend_fx_html += f"""<tr data-testid="row-analysis-tfx-{r['symbol'].replace('/','-')}">
            <td style="font-weight:600;">{r['symbol']}</td>
            <td>{_fmt(r['close'], 5)}</td>
            <td>{_fmt(r['hi50'], 5)} <span style="color:#64748b;font-size:11px;">({r['pct_from_hi']:+.2f}%)</span></td>
            <td>{_fmt(r['lo50'], 5)} <span style="color:#64748b;font-size:11px;">({r['pct_from_lo']:+.2f}%)</span></td>
            <td><span style="color:{sma_color};font-weight:600;">{r['sma_bias']}</span>
                <span style="color:#64748b;font-size:11px;display:block;">{_fmt(r['sma50'], 5)} / {_fmt(r['sma100'], 5)}</span></td>
            <td>{_fmt(r['atr100'], 5)}</td>
            <td>{_cond_badge(r['long_met'], 'LONG')}</td>
            <td>{_cond_badge(r['short_met'], 'SHORT')}</td>
            <td>{_pos_badge(r['position'])}</td></tr>"""

    h = data["hlc"]
    hlc_window_badge = '<span class="badge status-active">IN WINDOW</span>' if h["window_active"] else '<span class="badge status-closed">OUTSIDE</span>'
    hlc_holiday_badge = ' <span class="badge status-expired">HOLIDAY</span>' if h["holiday_blocked"] else ""
    hlc_detail_html = ""
    if h["status"] == "ready":
        hlc_detail_html = f"""
        <div class="stats-grid" style="margin-top:12px;">
            <div class="stat-card"><div class="stat-label">Current Price</div><div class="stat-value" style="font-size:1.1rem;">{_fmt(h['close'], 5)}</div></div>
            <div class="stat-card"><div class="stat-label">50-Day Highest</div><div class="stat-value" style="font-size:1.1rem;">{_fmt(h['hi50'], 5)}</div></div>
            <div class="stat-card"><div class="stat-label">50-Day Lowest</div><div class="stat-value" style="font-size:1.1rem;">{_fmt(h['lo50'], 5)}</div></div>
            <div class="stat-card"><div class="stat-label">H1 ATR(100)</div><div class="stat-value" style="font-size:1.1rem;">{_fmt(h['h1_atr'], 6)}</div></div>
            <div class="stat-card"><div class="stat-label">Reversal Threshold</div><div class="stat-value" style="font-size:1.1rem;">{_fmt(h['rev_threshold'], 5)}</div></div>
            <div class="stat-card"><div class="stat-label">Prev Day High / Low</div><div class="stat-value" style="font-size:1.1rem;">{_fmt(h['prev_high'], 5)} / {_fmt(h['prev_low'], 5)}</div></div>
        </div>
        <div style="display:flex;gap:8px;flex-wrap:wrap;margin-top:12px;">
            {_cond_badge(h['long_met'], 'LONG (price &ge; highest)')}
            {_cond_badge(h['short_met'], 'SHORT (price &lt; threshold)')}
            {_cond_badge(h['reversal_met'], 'REVERSAL LONG (near lowest)')}
            {_cond_badge(h['window_active'] and not h['holiday_blocked'], 'TIME WINDOW', blocked=h['holiday_blocked'] or not h['window_active'])}
        </div>
        <div style="margin-top:8px;">{_pos_badge(h['position'])}</div>"""
    else:
        hlc_detail_html = '<p style="color:#64748b;">Insufficient data for analysis.</p>'

    s = data["spx"]
    spx_session_badge = '<span class="badge status-active">IN SESSION</span>' if s["in_session"] else '<span class="badge status-closed">OUTSIDE</span>'
    spx_detail_html = ""
    if s["status"] == "ready":
        rsi_color = "#22c55e" if s["rsi20"] and s["rsi20"] > 70 else "#f59e0b" if s["rsi20"] and s["rsi20"] > 60 else "#94a3b8"
        spx_detail_html = f"""
        <div class="stats-grid" style="margin-top:12px;">
            <div class="stat-card"><div class="stat-label">SPX Close (30m)</div><div class="stat-value" style="font-size:1.1rem;">{_fmt(s['close'], 2)}</div></div>
            <div class="stat-card"><div class="stat-label">RSI(20)</div><div class="stat-value" style="font-size:1.1rem;color:{rsi_color};">{_fmt(s['rsi20'], 2)}</div>
                <div style="margin-top:4px;height:6px;background:#334155;border-radius:3px;position:relative;overflow:hidden;">
                    <div style="position:absolute;left:0;top:0;height:100%;width:{min(s['rsi20'] or 0, 100):.0f}%;background:{rsi_color};border-radius:3px;"></div>
                </div>
                <div style="font-size:11px;color:#64748b;margin-top:2px;">Entry threshold: 70</div>
            </div>
            <div class="stat-card"><div class="stat-label">ARCA Session</div><div style="margin-top:8px;">{spx_session_badge}</div></div>
        </div>
        <div style="display:flex;gap:8px;flex-wrap:wrap;margin-top:12px;">
            {_cond_badge(s['long_met'], 'RSI &gt; 70 (LONG)')}
            {_cond_badge(s['in_session'], 'ARCA 09:30-15:30', blocked=not s['in_session'])}
        </div>
        <div style="margin-top:8px;">{_pos_badge(s['position'])}</div>"""
    else:
        spx_detail_html = '<p style="color:#64748b;">Insufficient 30m data for analysis.</p>'

    mtf_html = ""
    for r in data["mtf"]:
        dp = r["dp"]
        sync_badge = ""
        if r["all_bull"]:
            sync_badge = '<span class="badge buy" style="font-size:11px;">ALL BULL ALIGNED</span>'
        elif r["all_bear"]:
            sync_badge = '<span class="badge sell" style="font-size:11px;">ALL BEAR ALIGNED</span>'
        else:
            sync_badge = '<span class="badge" style="font-size:11px;background:rgba(100,116,139,0.15);color:#94a3b8;">NO ALIGNMENT</span>'

        tf_cells = ""
        for tf in ["D1", "4H", "1H"]:
            tfd = r["timeframes"].get(tf, {})
            if tfd.get("status") != "ready":
                tf_cells += f'<td style="color:#64748b;">No data</td>'
                continue
            if tfd.get("bull"):
                icon = '<span style="color:#22c55e;">&#9650;</span>'
            elif tfd.get("bear"):
                icon = '<span style="color:#ef4444;">&#9660;</span>'
            else:
                icon = '<span style="color:#f59e0b;">&#9644;</span>'
            tf_cells += f'<td>{icon} <span style="font-size:11px;color:#94a3b8;">E20={_fmt(tfd["ema20"], dp)} E50={_fmt(tfd["ema50"], dp)}</span></td>'

        mtf_html += f"""<tr data-testid="row-analysis-mtf-{r['symbol'].replace('/','-')}">
            <td style="font-weight:600;">{r['symbol']}</td>
            {tf_cells}
            <td>{sync_badge}</td>
            <td>{_pos_badge(r['position'])}</td></tr>"""

    signal_dist_html = ""
    for sn, count in sorted(data["signal_counts"].items()):
        signal_dist_html += f'<div style="display:flex;justify-content:space-between;padding:4px 0;"><span style="color:#f1f5f9;">{sn}</span><span style="color:#94a3b8;">{count}</span></div>'
    if not signal_dist_html:
        signal_dist_html = '<div style="color:#64748b;">No signals generated yet.</div>'

    return f"""
    {summary_cards}

    <div class="settings-section" style="margin-bottom:20px;">
        <h3>Signal Distribution</h3>
        <div style="max-width:400px;margin-top:8px;">{signal_dist_html}</div>
    </div>

    <div class="settings-section" style="margin-bottom:20px;">
        <h3>Trend Following &mdash; Non-Forex <span style="font-size:0.8rem;color:#94a3b8;font-weight:400;">Scheduler: 4:59 PM ET | LONG ONLY | 3&times;ATR(100) trailing stop</span></h3>
        <p style="color:#94a3b8;font-size:13px;margin:4px 0 12px;">Entry: Close &gt; 50-day highest (LONG), confirmed by SMA(50) &gt; SMA(100) crossover</p>
        <div style="overflow-x:auto;">
            <table class="data-table" data-testid="table-analysis-trend-nf">
                <thead><tr>
                    <th>Asset</th><th>Close</th><th>50d High</th><th>50d Low</th>
                    <th>SMA Bias</th><th>ATR(100)</th><th>Long</th><th>Short</th><th>Position</th>
                </tr></thead>
                <tbody>{trend_nf_html}</tbody>
            </table>
        </div>
    </div>

    <div class="settings-section" style="margin-bottom:20px;">
        <h3>Trend Following &mdash; Forex <span style="font-size:0.8rem;color:#94a3b8;font-weight:400;">Scheduler: 4:59 PM ET | LONG ONLY | 3&times;ATR(100) trailing stop</span></h3>
        <p style="color:#94a3b8;font-size:13px;margin:4px 0 12px;">Entry: Close &gt; 50-day highest (LONG), confirmed by SMA(50) &gt; SMA(100) crossover</p>
        <div style="overflow-x:auto;">
            <table class="data-table" data-testid="table-analysis-trend-fx">
                <thead><tr>
                    <th>Asset</th><th>Close</th><th>50d High</th><th>50d Low</th>
                    <th>SMA Bias</th><th>ATR(100)</th><th>Long</th><th>Short</th><th>Position</th>
                </tr></thead>
                <tbody>{trend_fx_html}</tbody>
            </table>
        </div>
    </div>

    <div class="settings-section" style="margin-bottom:20px;">
        <h3>Highest/Lowest Close FX <span style="font-size:0.8rem;color:#94a3b8;font-weight:400;">EUR/USD | Scheduler: 9:00 &amp; 10:00 AM ET | 0.25&times;ATR trail, 6&times;ATR TP</span></h3>
        <p style="color:#94a3b8;font-size:13px;margin:4px 0 8px;">Entry: Price &ge; 50d highest (LONG), Price &le; 50d lowest (SHORT/REVERSAL), filtered by prev-day range &amp; holidays</p>
        <div style="display:flex;gap:8px;align-items:center;margin-bottom:8px;">
            {hlc_window_badge}{hlc_holiday_badge}
        </div>
        {hlc_detail_html}
    </div>

    <div class="settings-section" style="margin-bottom:20px;">
        <h3>SP500 Momentum <span style="font-size:0.8rem;color:#94a3b8;font-weight:400;">SPX | Scheduler: every 30m | LONG only | RSI(20) &gt; 70 entry</span></h3>
        <p style="color:#94a3b8;font-size:13px;margin:4px 0 8px;">Entry: RSI(20) crosses above 70 on 30m candles during ARCA session (09:30&ndash;15:30 ET)</p>
        {spx_detail_html}
    </div>

    <div class="settings-section" style="margin-bottom:20px;">
        <h3>Multi-Timeframe EMA <span style="font-size:0.8rem;color:#94a3b8;font-weight:400;">Scheduler: every hour | D1+H4+H1 sync | EMA 20/50/200</span></h3>
        <p style="color:#94a3b8;font-size:13px;margin:4px 0 12px;">Entry: All three timeframes must show EMA20 &gt; EMA50 &gt; EMA200 with price above EMA20 (BULL) or inverse (BEAR)</p>
        <div style="overflow-x:auto;">
            <table class="data-table" data-testid="table-analysis-mtf">
                <thead><tr>
                    <th>Asset</th><th>D1</th><th>H4</th><th>H1</th><th>Sync</th><th>Position</th>
                </tr></thead>
                <tbody>{mtf_html}</tbody>
            </table>
        </div>
    </div>

    <div class="timezone-note" style="margin-top:16px;">
        <strong>Reading This Dashboard:</strong>
        <ul>
            <li><strong>Condition badges</strong> show real-time rule evaluation &mdash; green <span style="color:#22c55e;">MET</span> means the entry condition is satisfied right now</li>
            <li><strong>% from High/Low</strong> shows how far the current close is from the 50-day extremes &mdash; closer to 0% means a breakout is near</li>
            <li><strong>SMA Bias</strong> confirms trend direction &mdash; BULL (SMA50 &gt; SMA100) required for LONG, BEAR for SHORT</li>
            <li><strong>Position column</strong> shows if the strategy already has an open trade (idempotency prevents duplicate entries)</li>
            <li><strong>MTF arrows:</strong> <span style="color:#22c55e;">&#9650;</span> = bullish alignment, <span style="color:#ef4444;">&#9660;</span> = bearish alignment, <span style="color:#f59e0b;">&#9644;</span> = no alignment on that timeframe</li>
        </ul>
    </div>
    """


def _build_users_html(current_user_id: int) -> str:
    admins = get_all_admins()
    rows = ""
    for a in admins:
        is_self = a["id"] == current_user_id
        self_badge = ' <span class="badge status-active">YOU</span>' if is_self else ""
        role = a.get("role", "CUSTOMER")
        role_color = "#3b82f6" if role == "ADMIN" else "#22c55e"
        role_bg = "rgba(59,130,246,0.15)" if role == "ADMIN" else "rgba(34,197,94,0.15)"
        role_badge = f'<span style="display:inline-block;padding:2px 10px;border-radius:9999px;font-size:11px;font-weight:600;color:{role_color};background:{role_bg};border:1px solid {role_color}33;">{role}</span>'
        rows += f"""
        <tr data-testid="row-admin-{a['id']}">
            <td>{a['id']}</td>
            <td>{a['username']}{self_badge}</td>
            <td>{role_badge}</td>
            <td>{a['created_at']}</td>
            <td>
                <button class="btn btn-secondary btn-sm" onclick="editAdmin({a['id']}, '{a['username']}', '{role}')" data-testid="button-edit-admin-{a['id']}">Edit</button>
                <button class="btn btn-danger btn-sm" onclick="deleteAdmin({a['id']}, '{a['username']}')" data-testid="button-delete-admin-{a['id']}" {'disabled style="opacity:0.5;cursor:not-allowed;"' if len(admins) <= 1 else ''}>Delete</button>
            </td>
        </tr>"""

    return f"""
    <div class="settings-section">
        <h3>User Management</h3>
        <p class="settings-desc">Manage user accounts and assign roles. Admins have full dashboard access; Customers can manage their own settings and WordPress connections.</p>
        <table class="data-table" data-testid="admin-users-table" style="margin-top:12px;">
            <thead>
                <tr>
                    <th>ID</th>
                    <th>Username</th>
                    <th>Role</th>
                    <th>Created</th>
                    <th>Actions</th>
                </tr>
            </thead>
            <tbody>{rows}</tbody>
        </table>
    </div>

    <div class="settings-section" style="margin-top:20px;">
        <h3>Add New User</h3>
        <div style="display:flex;gap:8px;align-items:center;flex-wrap:wrap;margin-top:12px;">
            <input type="text" id="new-admin-username" placeholder="Username" data-testid="input-new-admin-username"
                style="background:#0f172a;border:1px solid #334155;color:#e2e8f0;padding:10px 14px;border-radius:6px;font-size:0.9rem;width:200px;">
            <input type="password" id="new-admin-password" placeholder="Password" data-testid="input-new-admin-password"
                style="background:#0f172a;border:1px solid #334155;color:#e2e8f0;padding:10px 14px;border-radius:6px;font-size:0.9rem;width:200px;">
            <select id="new-admin-role" data-testid="select-new-admin-role"
                style="background:#0f172a;border:1px solid #334155;color:#e2e8f0;padding:10px 14px;border-radius:6px;font-size:0.9rem;width:160px;cursor:pointer;">
                <option value="CUSTOMER">Customer</option>
                <option value="ADMIN">Admin</option>
            </select>
            <button class="btn btn-primary" onclick="addAdmin()" data-testid="button-add-admin">Add User</button>
        </div>
        <div id="add-admin-result" style="margin-top:12px;"></div>
    </div>

    <div id="edit-modal" class="modal-overlay hidden">
        <div class="modal-card">
            <h3>Edit User</h3>
            <input type="hidden" id="edit-admin-id">
            <div class="form-group" style="margin-top:12px;">
                <label style="font-size:0.85rem;color:#94a3b8;margin-bottom:4px;display:block;">Username</label>
                <input type="text" id="edit-admin-username" data-testid="input-edit-admin-username"
                    style="background:#0f172a;border:1px solid #334155;color:#e2e8f0;padding:10px 14px;border-radius:6px;font-size:0.9rem;width:100%;">
            </div>
            <div class="form-group" style="margin-top:12px;">
                <label style="font-size:0.85rem;color:#94a3b8;margin-bottom:4px;display:block;">Role</label>
                <select id="edit-admin-role" data-testid="select-edit-admin-role"
                    style="background:#0f172a;border:1px solid #334155;color:#e2e8f0;padding:10px 14px;border-radius:6px;font-size:0.9rem;width:100%;cursor:pointer;">
                    <option value="CUSTOMER">Customer</option>
                    <option value="ADMIN">Admin</option>
                </select>
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
.toggle-switch { position: relative; display: inline-block; width: 48px; height: 26px; flex-shrink: 0; }
.toggle-switch input { opacity: 0; width: 0; height: 0; }
.toggle-slider { position: absolute; cursor: pointer; top: 0; left: 0; right: 0; bottom: 0; background: #334155; border-radius: 26px; transition: 0.25s; }
.toggle-slider:before { content: ""; position: absolute; height: 20px; width: 20px; left: 3px; bottom: 3px; background: #94a3b8; border-radius: 50%; transition: 0.25s; }
.toggle-switch input:checked + .toggle-slider { background: #3b82f6; }
.toggle-switch input:checked + .toggle-slider:before { transform: translateX(22px); background: #f1f5f9; }
@media (max-width: 768px) {
    .layout { flex-direction: column; }
    .sidebar { display: none; }
    .mobile-tab-bar { display: block; }
    .main-content { padding: 16px; }
    .tables-row { grid-template-columns: 1fr; }
    .stats-grid { grid-template-columns: 1fr 1fr; }
}
.btn-copy { background: rgba(56,189,248,0.1); color: #38bdf8; border: 1px solid rgba(56,189,248,0.25); padding: 4px 12px; border-radius: 6px; font-size: 12px; font-weight: 500; cursor: pointer; transition: all 0.15s; }
.btn-copy:hover { background: rgba(56,189,248,0.2); border-color: #38bdf8; }
@keyframes proximity-pulse { 0%, 100% { background: rgba(234,179,8,0.06); } 50% { background: rgba(234,179,8,0.15); } }
.proximity-row { animation: proximity-pulse 2.5s ease-in-out infinite; }
.proximity-row td { border-bottom-color: rgba(234,179,8,0.2) !important; }
.btn-mute { background: rgba(234,179,8,0.12); color: #eab308; border: 1px solid rgba(234,179,8,0.3); padding: 4px 12px; border-radius: 6px; font-size: 12px; font-weight: 500; cursor: pointer; transition: all 0.15s; }
.btn-mute:hover { background: rgba(234,179,8,0.25); border-color: #eab308; }
.market-pulse { display: flex; gap: 10px; flex-wrap: wrap; margin-bottom: 20px; }
.pulse-card { background: #0f172a; border: 1px solid #334155; border-radius: 8px; padding: 10px 16px; display: flex; align-items: center; gap: 10px; min-width: 150px; transition: border-color 0.3s; }
.pulse-card .pulse-symbol { font-size: 0.9rem; font-weight: 600; color: #f1f5f9; }
.pulse-badge { display: inline-block; padding: 2px 8px; border-radius: 4px; font-size: 0.7rem; font-weight: 600; text-transform: uppercase; letter-spacing: 0.03em; }
.pulse-badge.pulse-neutral { background: rgba(100,116,139,0.2); color: #94a3b8; border: 1px solid rgba(100,116,139,0.3); }
.pulse-badge.pulse-approaching { background: rgba(234,179,8,0.15); color: #eab308; border: 1px solid rgba(234,179,8,0.3); animation: proximity-pulse 2.5s ease-in-out infinite; }
.pulse-badge.pulse-triggered { background: rgba(34,197,94,0.15); color: #22c55e; border: 1px solid rgba(34,197,94,0.3); }
.pulse-card.pulse-card-approaching { border-color: rgba(234,179,8,0.4); }
.pulse-card.pulse-card-triggered { border-color: rgba(34,197,94,0.4); }
.ro-card { background: #1e293b; border: 1px solid #334155; border-radius: 12px; padding: 24px; margin-bottom: 20px; }
.ro-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 32px; }
@media (max-width: 640px) { .ro-grid { grid-template-columns: 1fr; gap: 20px; } }
.ro-col { display: flex; flex-direction: column; align-items: center; }
.ro-title { font-size: 0.7rem; font-weight: 600; text-transform: uppercase; letter-spacing: 0.06em; color: #64748b; margin-bottom: 16px; }
.gauge-pct { font-size: 1.75rem; font-weight: 800; line-height: 1; }
.gauge-label { font-size: 0.65rem; color: #64748b; text-transform: uppercase; margin-top: 2px; }
.ro-text { font-size: 0.85rem; color: #94a3b8; margin-top: 14px; text-align: center; line-height: 1.6; }
.ro-text strong { color: #f1f5f9; }
@keyframes recharge-blink { 0%, 100% { opacity: 1; } 50% { opacity: 0.3; } }
.badge-recharge { display: inline-block; background: #7f1d1d; color: #fca5a5; padding: 3px 12px; border-radius: 9999px; font-size: 0.7rem; font-weight: 700; text-transform: uppercase; letter-spacing: 0.04em; animation: recharge-blink 1.2s ease-in-out infinite; margin-top: 10px; }
.vbar-wrap { display: flex; align-items: flex-end; gap: 12px; height: 120px; }
.vbar-track { width: 40px; height: 100%; background: #334155; border-radius: 6px; position: relative; overflow: hidden; }
.vbar-fill { position: absolute; bottom: 0; left: 0; width: 100%; border-radius: 6px 6px 0 0; transition: height 0.6s ease, background 0.4s ease; }
.vbar-labels { display: flex; flex-direction: column; justify-content: space-between; height: 100%; }
.vbar-label { font-size: 0.7rem; color: #64748b; }
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
    if (tabName === 'analysis') loadMarketPulse();
    if (tabName === 'credits') loadResourceOverview();
    if (tabName === 'settings') loadCreditMeter();
    if (tabName === 'notifications') loadNotifConfig();
    if (tabName === 'apikeys') loadPartnerKeys();
    if (tabName === 'scheduler') loadSchedulerData();
    if (tabName === 'system') loadSystemStatus();
    if (tabName === 'recovery') loadRecoveryLogs();
    if (tabName === 'users') loadRegistrationToggle();
    if (tabName === 'wordpress') { loadUserCmsConfigs(); }
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
    const role = document.getElementById('new-admin-role').value;
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
            body: JSON.stringify({username, password, role})
        });
        const data = await res.json();
        if (data.success) {
            resultDiv.innerHTML = '<div class="result-success">User "' + username + '" created as ' + role + '.</div>';
            document.getElementById('new-admin-username').value = '';
            document.getElementById('new-admin-password').value = '';
            document.getElementById('new-admin-role').value = 'CUSTOMER';
            setTimeout(() => window.location.reload(), 1000);
        } else {
            resultDiv.innerHTML = '<div class="result-error">' + (data.error || 'Failed to create user.') + '</div>';
        }
    } catch (e) {
        resultDiv.innerHTML = '<div class="result-error">Error: ' + e.message + '</div>';
    }
}

function editAdmin(id, username, role) {
    document.getElementById('edit-admin-id').value = id;
    document.getElementById('edit-admin-username').value = username;
    document.getElementById('edit-admin-role').value = role || 'CUSTOMER';
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
    const role = document.getElementById('edit-admin-role').value;
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
        const body = {username, role};
        if (password) body.password = password;
        const res = await fetch(BASE + '/admin/api/users/' + id, {
            method: 'PUT',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify(body)
        });
        const data = await res.json();
        if (data.success) {
            resultDiv.innerHTML = '<div class="result-success">User updated successfully.</div>';
            setTimeout(() => { closeEditModal(); window.location.reload(); }, 1000);
        } else {
            resultDiv.innerHTML = '<div class="result-error">' + (data.error || 'Failed to update user.') + '</div>';
        }
    } catch (e) {
        resultDiv.innerHTML = '<div class="result-error">Error: ' + e.message + '</div>';
    }
}

async function deleteAdmin(id, username) {
    if (!confirm('Are you sure you want to delete user "' + username + '"?')) return;
    try {
        const res = await fetch(BASE + '/admin/api/users/' + id, {method: 'DELETE'});
        const data = await res.json();
        if (data.success) {
            window.location.reload();
        } else {
            alert(data.error || 'Failed to delete user.');
        }
    } catch (e) {
        alert('Error: ' + e.message);
    }
}

async function loadRegistrationToggle() {
    try {
        const res = await fetch(BASE + '/admin/api/settings/registration');
        const data = await res.json();
        document.getElementById('registration-toggle').checked = data.enabled;
    } catch (e) {}
}

async function toggleRegistration(enabled) {
    const msgEl = document.getElementById('reg-toggle-msg');
    try {
        const res = await fetch(BASE + '/admin/api/settings/registration', {
            method: 'PUT',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({enabled})
        });
        const data = await res.json();
        if (data.success) {
            msgEl.style.display = 'block';
            msgEl.style.background = 'rgba(34,197,94,0.1)';
            msgEl.style.border = '1px solid rgba(34,197,94,0.3)';
            msgEl.style.color = '#22c55e';
            msgEl.textContent = enabled ? 'Registration enabled. New users can register from the login page.' : 'Registration disabled. Only admins can create new accounts.';
            setTimeout(() => { msgEl.style.display = 'none'; }, 4000);
        } else {
            msgEl.style.display = 'block';
            msgEl.style.background = 'rgba(239,68,68,0.1)';
            msgEl.style.border = '1px solid rgba(239,68,68,0.3)';
            msgEl.style.color = '#ef4444';
            msgEl.textContent = data.error || 'Failed to update setting.';
            document.getElementById('registration-toggle').checked = !enabled;
        }
    } catch (e) {
        document.getElementById('registration-toggle').checked = !enabled;
    }
}

async function loadPartnerKeys() {
    try {
        var res = await fetch(BASE + '/admin/api/partner-keys');
        var data = await res.json();
        document.getElementById('apikeys-loading').style.display = 'none';
        document.getElementById('apikeys-content').style.display = 'block';

        var banner = document.getElementById('require-apikey-banner');
        if (data.require_api_key) {
            banner.style.background = 'rgba(239,68,68,0.08)';
            banner.style.border = '1px solid rgba(239,68,68,0.2)';
            banner.innerHTML = '<span style="color:#ef4444;font-weight:600;">REQUIRE_API_KEY = ON</span><span style="color:#94a3b8;"> — All /api/v1/ requests without a valid X-API-KEY header are rejected (401).</span>';
        } else {
            banner.style.background = 'rgba(34,197,94,0.08)';
            banner.style.border = '1px solid rgba(34,197,94,0.2)';
            banner.innerHTML = '<span style="color:#22c55e;font-weight:600;">REQUIRE_API_KEY = OFF</span><span style="color:#94a3b8;"> — API is open; keys are optional but grant higher rate limits.</span>';
        }

        var keys = data.keys || [];
        var tbody = document.getElementById('apikeys-rows');
        if (keys.length === 0) {
            tbody.innerHTML = '<tr><td colspan="8" style="text-align:center;color:#64748b;">No API keys created yet</td></tr>';
        } else {
            var rows = '';
            keys.forEach(function(k) {
                var tierColor = k.tier === 'premium' ? '#a855f7' : k.tier === 'unlimited' ? '#f59e0b' : '#3b82f6';
                var statusBadge = k.is_active
                    ? '<span class="badge status-open">Active</span>'
                    : '<span class="badge status-closed">Revoked</span>';
                var lastUsed = k.last_used_at ? k.last_used_at.replace('T', ' ').slice(0, 19) : 'Never';
                var created = k.created_at ? k.created_at.replace('T', ' ').slice(0, 19) : '--';
                var toggleBtn = k.is_active
                    ? '<button class="btn" style="font-size:12px;padding:4px 10px;color:#f59e0b;" onclick="toggleKey(' + k.id + ',false)" data-testid="button-revoke-key-' + k.id + '">Revoke</button>'
                    : '<button class="btn" style="font-size:12px;padding:4px 10px;color:#22c55e;" onclick="toggleKey(' + k.id + ',true)" data-testid="button-activate-key-' + k.id + '">Activate</button>';
                var deleteBtn = '<button class="btn" style="font-size:12px;padding:4px 10px;color:#ef4444;" onclick="deleteKey(' + k.id + ')" data-testid="button-delete-key-' + k.id + '">Delete</button>';
                rows += '<tr>';
                rows += '<td style="color:#64748b;">' + k.id + '</td>';
                rows += '<td style="font-weight:500;">' + k.label + '</td>';
                rows += '<td><span style="color:' + tierColor + ';font-weight:600;text-transform:uppercase;font-size:12px;">' + k.tier + '</span></td>';
                rows += '<td>' + k.rate_limit_per_minute + '/min</td>';
                rows += '<td>' + statusBadge + '</td>';
                rows += '<td style="font-size:13px;color:#94a3b8;">' + lastUsed + '</td>';
                rows += '<td style="font-size:13px;color:#94a3b8;">' + created + '</td>';
                rows += '<td style="display:flex;gap:6px;">' + toggleBtn + deleteBtn + '</td>';
                rows += '</tr>';
            });
            tbody.innerHTML = rows;
        }
    } catch (e) {
        document.getElementById('apikeys-loading').textContent = 'Failed to load API keys.';
    }
}

function showCreateKeyModal() {
    document.getElementById('create-key-modal').style.display = 'flex';
    document.getElementById('key-label').value = '';
    document.getElementById('key-tier').value = 'standard';
    document.getElementById('key-rate-limit').value = '120';
}

function hideCreateKeyModal() {
    document.getElementById('create-key-modal').style.display = 'none';
}

async function createPartnerKey() {
    var label = document.getElementById('key-label').value.trim();
    if (!label) { alert('Label is required'); return; }
    var tier = document.getElementById('key-tier').value;
    var rateLimit = parseInt(document.getElementById('key-rate-limit').value) || 120;
    try {
        var res = await fetch(BASE + '/admin/api/partner-keys', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({label: label, tier: tier, rate_limit_per_minute: rateLimit})
        });
        var data = await res.json();
        if (data.error) { alert(data.error); return; }
        hideCreateKeyModal();
        document.getElementById('new-key-value').value = data.key;
        document.getElementById('key-created-modal').style.display = 'flex';
        loadPartnerKeys();
    } catch (e) {
        alert('Failed to create API key');
    }
}

function hideKeyCreatedModal() {
    document.getElementById('key-created-modal').style.display = 'none';
}

function copyNewKey() {
    var input = document.getElementById('new-key-value');
    input.select();
    navigator.clipboard.writeText(input.value);
}

async function toggleKey(keyId, active) {
    try {
        await fetch(BASE + '/admin/api/partner-keys/' + keyId + '/toggle', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({active: active})
        });
        loadPartnerKeys();
    } catch (e) {
        alert('Failed to update key');
    }
}

async function deleteKey(keyId) {
    if (!confirm('Permanently delete this API key? This cannot be undone.')) return;
    try {
        await fetch(BASE + '/admin/api/partner-keys/' + keyId, {method: 'DELETE'});
        loadPartnerKeys();
    } catch (e) {
        alert('Failed to delete key');
    }
}

async function loadMarketPulse() {
    var container = document.getElementById('market-pulse-container');
    if (!container) return;
    try {
        var res = await fetch(BASE + '/admin/api/market-pulse');
        var data = await res.json();
        var assets = data.assets || [];
        if (assets.length === 0) {
            container.innerHTML = '<div style="color:#94a3b8;font-size:0.85rem;">No watchlist assets configured.</div>';
            return;
        }
        var html = '';
        for (var i = 0; i < assets.length; i++) {
            var a = assets[i];
            var badgeClass = 'pulse-neutral';
            var cardExtra = '';
            var label = 'Monitoring';
            if (a.status === 'approaching') {
                badgeClass = 'pulse-approaching';
                cardExtra = ' pulse-card-approaching';
                label = 'Approaching';
            } else if (a.status === 'triggered') {
                badgeClass = 'pulse-triggered';
                cardExtra = ' pulse-card-triggered';
                label = 'Triggered';
            }
            var detail = a.detail ? ' title="' + a.detail.replace(/"/g, '&quot;') + '"' : '';
            html += '<div class="pulse-card' + cardExtra + '"' + detail + ' data-testid="pulse-card-' + a.symbol.replace(/\\//g, '-') + '">' +
                '<span class="pulse-symbol">' + a.symbol + '</span>' +
                '<span class="pulse-badge ' + badgeClass + '" data-testid="pulse-badge-' + a.symbol.replace(/\\//g, '-') + '">' + label + '</span>' +
                '</div>';
        }
        container.innerHTML = html;
    } catch (e) {
        container.innerHTML = '<div style="color:#ef4444;font-size:0.85rem;">Failed to load market pulse.</div>';
    }
}



async function loadResourceOverview() {
    var container = document.getElementById('resource-overview');
    if (!container) return;
    try {
        var [quotaRes, storageRes] = await Promise.all([
            fetch(BASE + '/admin/api/quota-status'),
            fetch(BASE + '/admin/api/storage-stats')
        ]);
        var q = await quotaRes.json();
        var s = await storageRes.json();

        var usedPct = Math.max(0, Math.min(q.usage_pct || 0, 100));
        var remaining = q.remaining_credits || 0;
        var limit = q.credit_limit || 500000;

        var gaugeColor;
        if (usedPct >= 90) gaugeColor = '#ef4444';
        else if (usedPct >= 75) gaugeColor = '#f59e0b';
        else gaugeColor = '#22c55e';

        var svgGauge = document.getElementById('ro-gauge-svg');
        if (svgGauge) {
            var radius = 72;
            var halfCircumference = Math.PI * radius;
            var fillLength = (usedPct / 100) * halfCircumference;
            var arc = svgGauge.querySelector('.gauge-arc-fill');
            if (arc) {
                arc.style.strokeDasharray = halfCircumference;
                arc.style.strokeDashoffset = halfCircumference - fillLength;
                arc.style.stroke = gaugeColor;
            }
        }

        var gaugePct = document.getElementById('ro-gauge-pct');
        if (gaugePct) {
            gaugePct.textContent = usedPct.toFixed(1) + '%';
            gaugePct.style.color = gaugeColor;
        }

        var creditText = document.getElementById('ro-credit-text');
        if (creditText) {
            creditText.innerHTML = '<strong>' + Number(remaining).toLocaleString() + '</strong> / ' + Number(limit).toLocaleString() + ' remaining';
        }

        var rechargeBadge = document.getElementById('ro-recharge-badge');
        if (rechargeBadge) {
            var remainPct = limit > 0 ? (remaining / limit) * 100 : 0;
            rechargeBadge.style.display = remainPct < 10 ? 'inline-block' : 'none';
        }

        var dbMb = (s.database && s.database.size_mb) ? s.database.size_mb : 0;
        var maxMb = s.max_storage_mb || 1024;
        var storagePct = maxMb > 0 ? Math.max(0, Math.min((dbMb / maxMb) * 100, 100)) : 0;

        var storageColor;
        if (storagePct >= 90) storageColor = '#ef4444';
        else if (storagePct >= 70) storageColor = '#f59e0b';
        else storageColor = '#22c55e';

        var vbarFill = document.getElementById('ro-vbar-fill');
        if (vbarFill) {
            vbarFill.style.height = Math.max(storagePct, 2) + '%';
            vbarFill.style.background = storageColor;
        }

        var storageText = document.getElementById('ro-storage-text');
        if (storageText) {
            storageText.innerHTML = 'Current: <strong>' + dbMb + ' MB</strong> | Limit: <strong>' + maxMb.toFixed(0) + ' MB</strong>';
        }

        var storagePctEl = document.getElementById('ro-storage-pct');
        if (storagePctEl) {
            storagePctEl.textContent = storagePct.toFixed(1) + '%';
            storagePctEl.style.color = storageColor;
        }

        var remainPctVal = limit > 0 ? (remaining / limit * 100) : 0;
        var pctEl = document.getElementById('quota-pct-text');
        if (pctEl) {
            pctEl.textContent = remainPctVal.toFixed(1) + '% left';
            pctEl.style.color = remainPctVal >= 50 ? '#22c55e' : (remainPctVal >= 10 ? '#f59e0b' : '#ef4444');
        }
        var remEl = document.getElementById('quota-remaining');
        if (remEl) {
            remEl.textContent = remaining >= 1000 ? (remaining / 1000).toFixed(0) + 'K' : remaining.toLocaleString();
            remEl.style.color = remainPctVal >= 50 ? '#22c55e' : (remainPctVal >= 10 ? '#f59e0b' : '#ef4444');
        }
        var dailyBurn = q.daily_avg_burn || 0;
        var burnEl = document.getElementById('quota-daily-burn');
        if (burnEl) {
            burnEl.textContent = dailyBurn >= 1000 ? (dailyBurn / 1000).toFixed(1) + 'K' : Math.round(dailyBurn).toLocaleString();
        }
        var daysLeft = q.est_days_remaining || 0;
        var daysEl = document.getElementById('quota-days-left');
        if (daysEl) {
            daysEl.textContent = daysLeft >= 999 ? '∞' : daysLeft.toFixed(0);
        }

    } catch (e) {
        console.error('Resource overview failed:', e);
    }
}

async function loadSchedulerData() {
    try {
        var [healthRes, jobsRes, sysRes] = await Promise.all([
            fetch(BASE + '/admin/api/scheduler/health'),
            fetch(BASE + '/admin/api/scheduler/jobs?limit=50'),
            fetch(BASE + '/health')
        ]);
        var health = await healthRes.json();
        var jobsData = await jobsRes.json();
        var sysHealth = await sysRes.json();

        document.getElementById('sched-loading').style.display = 'none';
        document.getElementById('sched-content').style.display = 'block';

        var statusEl = document.getElementById('sched-status');
        var schedRunning = sysHealth.scheduler && sysHealth.scheduler.running;
        if (!schedRunning) {
            statusEl.textContent = 'Stopped';
            statusEl.style.color = '#ef4444';
        } else if (health.last_24h_failures > 0) {
            statusEl.textContent = 'Running (with errors)';
            statusEl.style.color = '#f59e0b';
        } else {
            statusEl.textContent = 'Running';
            statusEl.style.color = '#22c55e';
        }

        document.getElementById('sched-jobs').textContent = sysHealth.scheduler ? sysHealth.scheduler.jobs_registered : '--';
        document.getElementById('sched-success').textContent = health.last_24h_success || 0;
        document.getElementById('sched-failures').textContent = health.last_24h_failures || 0;
        document.getElementById('sched-total-logged').textContent = health.total_jobs_logged || 0;

        var wdEl = document.getElementById('sched-watchdog');
        if (sysHealth.watchdog && sysHealth.watchdog.last_heartbeat) {
            wdEl.textContent = sysHealth.watchdog.last_heartbeat.replace('T', ' ').slice(0, 19) + ' UTC';
        } else {
            wdEl.textContent = 'Waiting for first tick...';
            wdEl.style.color = '#94a3b8';
        }

        var tbody = document.getElementById('sched-job-rows');
        var jobs = jobsData.logs || [];
        if (jobs.length === 0) {
            tbody.innerHTML = '<tr><td colspan="7" style="text-align:center;color:#64748b;">No job logs recorded yet</td></tr>';
        } else {
            var rows = '';
            jobs.forEach(function(j) {
                var statusClass = j.status === 'SUCCESS' ? 'status-open' : j.status === 'FAILED' ? 'status-closed' : j.status === 'PARTIAL' ? 'status-expired' : '';
                var startTime = j.started_at ? j.started_at.replace('T', ' ').slice(0, 19) : '--';
                var duration = j.duration_seconds !== null && j.duration_seconds !== undefined ? j.duration_seconds.toFixed(1) + 's' : '--';
                rows += '<tr>';
                rows += '<td style="font-weight:500;">' + (j.strategy_name || '--') + '</td>';
                rows += '<td><span class="badge ' + statusClass + '">' + (j.status || '--') + '</span></td>';
                rows += '<td style="font-size:13px;color:#94a3b8;">' + startTime + '</td>';
                rows += '<td>' + duration + '</td>';
                rows += '<td>' + (j.assets_evaluated !== null ? j.assets_evaluated : '--') + '</td>';
                rows += '<td>' + (j.signals_generated !== null ? j.signals_generated : '0') + '</td>';
                rows += '<td style="color:' + (j.errors > 0 ? '#ef4444' : '#94a3b8') + ';">' + (j.errors || 0) + (j.error_detail ? ' - ' + j.error_detail.slice(0, 60) : '') + '</td>';
                rows += '</tr>';
            });
            tbody.innerHTML = rows;
        }
    } catch (e) {
        document.getElementById('sched-loading').textContent = 'Failed to load scheduler data.';
    }
}

async function loadSystemStatus() {
    try {
        var [healthRes, notifRes, creditRes, secRes, storageRes] = await Promise.all([
            fetch(BASE + '/health'),
            fetch(BASE + '/admin/api/notifications').catch(function() { return null; }),
            fetch(BASE + '/admin/api/usage').catch(function() { return null; }),
            fetch(BASE + '/admin/api/security/stats').catch(function() { return null; }),
            fetch(BASE + '/admin/api/storage-stats').catch(function() { return null; })
        ]);
        var health = await healthRes.json();
        var notif = notifRes ? await notifRes.json().catch(function() { return {}; }) : {};
        var credit = creditRes ? await creditRes.json().catch(function() { return {}; }) : {};
        var sec = secRes ? await secRes.json().catch(function() { return {}; }) : {};
        var storage = storageRes ? await storageRes.json().catch(function() { return {}; }) : {};

        document.getElementById('sys-loading').style.display = 'none';
        document.getElementById('sys-content').style.display = 'block';

        var banner = document.getElementById('sys-health-banner');
        var dot = document.getElementById('sys-health-dot');
        var label = document.getElementById('sys-health-label');
        var checks = document.getElementById('sys-health-checks');

        if (health.status === 'healthy') {
            banner.style.background = 'rgba(34,197,94,0.08)';
            banner.style.border = '1px solid rgba(34,197,94,0.2)';
            dot.style.background = '#22c55e';
            label.textContent = 'All Systems Healthy';
            label.style.color = '#22c55e';
            checks.textContent = 'All monitoring checks passed';
        } else {
            banner.style.background = 'rgba(239,68,68,0.08)';
            banner.style.border = '1px solid rgba(239,68,68,0.2)';
            dot.style.background = '#ef4444';
            label.textContent = 'System Degraded';
            label.style.color = '#ef4444';
            checks.textContent = 'Failed checks: ' + (health.checks_failed || []).join(', ');
        }

        var dbBadge = document.getElementById('sys-db-badge');
        if (health.database && health.database.connected) {
            dbBadge.textContent = 'Connected';
            dbBadge.className = 'badge status-open';
        } else {
            dbBadge.textContent = 'Error';
            dbBadge.className = 'badge status-closed';
        }

        var wdBadge = document.getElementById('sys-watchdog-badge');
        var wdTime = document.getElementById('sys-watchdog-time');
        if (health.watchdog && health.watchdog.last_heartbeat) {
            wdBadge.textContent = 'Active';
            wdBadge.className = 'badge status-open';
            wdTime.textContent = 'Last tick: ' + health.watchdog.last_heartbeat.replace('T', ' ').slice(0, 19) + ' UTC';
        } else {
            wdBadge.textContent = 'Pending';
            wdBadge.className = 'badge status-expired';
            wdTime.textContent = 'Waiting for first heartbeat (300s interval)';
        }

        var apiBadge = document.getElementById('sys-apikey-badge');
        if (health.api_key_configured) {
            apiBadge.textContent = 'Configured';
            apiBadge.className = 'badge status-open';
        } else {
            apiBadge.textContent = 'Not Set';
            apiBadge.className = 'badge status-closed';
        }

        var killBadge = document.getElementById('sys-killswitch-badge');
        if (credit.kill_switch_active) {
            killBadge.textContent = 'TRIGGERED';
            killBadge.className = 'badge status-closed';
        } else {
            killBadge.textContent = 'Standby';
            killBadge.className = 'badge status-open';
        }

        var webhookBadge = document.getElementById('sys-webhook-badge');
        if (notif.enabled && notif.webhook_configured) {
            webhookBadge.textContent = 'Active';
            webhookBadge.className = 'badge status-open';
        } else if (notif.webhook_configured && !notif.enabled) {
            webhookBadge.textContent = 'Disabled';
            webhookBadge.className = 'badge status-expired';
        } else {
            webhookBadge.textContent = 'Not Configured';
            webhookBadge.className = 'badge status-closed';
        }

        var schedStatus = document.getElementById('sys-sched-status');
        if (health.scheduler && health.scheduler.running) {
            schedStatus.innerHTML = '<span style="color:#22c55e;">Running</span>';
        } else {
            schedStatus.innerHTML = '<span style="color:#ef4444;">Stopped</span>';
        }
        document.getElementById('sys-sched-jobs').textContent = health.scheduler ? health.scheduler.jobs_registered : '--';

        var s24 = health.last_24h || {};
        document.getElementById('sys-24h-stats').innerHTML =
            '<span style="color:#22c55e;">' + (s24.success || 0) + '</span> / <span style="color:#ef4444;">' + (s24.failures || 0) + '</span>';

        var ts = health.timestamp || '';
        document.getElementById('sys-timestamp').textContent = ts ? ts.replace('T', ' ').slice(0, 19) + ' UTC' : '--';

        var wsClients = (health.websocket && health.websocket.clients !== undefined) ? health.websocket.clients : '--';
        document.getElementById('sys-ws-clients').textContent = wsClients;

        var secEl = document.getElementById('sec-stats');
        if (secEl && sec.tracked_ips !== undefined) {
            var parts = ['Tracking ' + sec.tracked_ips + ' IPs'];
            if (sec.blocked_ips > 0) parts.push('<span style="color:#ef4444;">' + sec.blocked_ips + ' blocked</span>');
            if (sec.cooled_down_ips > 0) parts.push('<span style="color:#f59e0b;">' + sec.cooled_down_ips + ' in cooldown</span>');
            secEl.innerHTML = parts.join(' &middot; ');
        }

        if (storage && storage.used_percent !== undefined) {
            var pct = storage.used_percent;
            var barColor, statusText, statusClass;
            if (pct < 70) {
                barColor = '#22c55e';
                statusText = 'Healthy';
                statusClass = 'status-open';
            } else if (pct < 90) {
                barColor = '#f59e0b';
                statusText = 'Warning';
                statusClass = 'status-expired';
            } else {
                barColor = '#ef4444';
                statusText = 'Critical';
                statusClass = 'status-closed';
            }
            document.getElementById('storage-pct-label').textContent = pct + '%';
            document.getElementById('storage-pct-label').style.color = barColor;
            var badge = document.getElementById('storage-status-badge');
            badge.textContent = statusText;
            badge.className = 'badge ' + statusClass;
            var dbMb = storage.database ? storage.database.size_mb : 0;
            var bkMb = storage.backups ? storage.backups.size_mb : 0;
            var bkCount = storage.backups ? storage.backups.file_count : 0;
            document.getElementById('storage-breakdown').innerHTML =
                'DB: <strong style="color:#f1f5f9;">' + dbMb + ' MB</strong> &nbsp;|&nbsp; ' +
                'Backups: <strong style="color:#f1f5f9;">' + bkMb + ' MB</strong> (' + bkCount + ' file' + (bkCount !== 1 ? 's' : '') + ')';
            var bar = document.getElementById('storage-bar');
            bar.style.width = Math.min(pct, 100) + '%';
            bar.style.background = barColor;
            document.getElementById('storage-used-label').textContent = storage.total_used_mb + ' MB used';
            document.getElementById('storage-max-label').textContent = storage.max_storage_mb + ' MB max (' + storage.max_storage_gb + ' GB)';
        }

    } catch (e) {
        document.getElementById('sys-loading').textContent = 'Failed to load system status.';
    }
}

function formatRecoveryDate(isoStr) {
    if (!isoStr) return '--';
    try {
        var d = new Date(isoStr + (isoStr.includes('Z') ? '' : 'Z'));
        var months = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
        var month = months[d.getUTCMonth()];
        var day = d.getUTCDate();
        var hr = d.getUTCHours();
        var min = String(d.getUTCMinutes()).padStart(2, '0');
        var ampm = hr >= 12 ? 'PM' : 'AM';
        hr = hr % 12 || 12;
        return month + ' ' + day + ', ' + hr + ':' + min + ' ' + ampm + ' ET';
    } catch(e) { return isoStr; }
}

async function loadRecoveryLogs() {
    try {
        var res = await fetch(BASE + '/admin/api/recovery-logs');
        var data = await res.json();
        document.getElementById('recovery-loading').style.display = 'none';
        document.getElementById('recovery-content').style.display = 'block';

        var logs = data.logs || [];
        var total = logs.length;
        var successCount = logs.filter(function(l) { return l.status === 'SUCCESS'; }).length;
        var failedCount = logs.filter(function(l) { return l.status === 'FAILED'; }).length;

        document.getElementById('recovery-total').textContent = total;
        document.getElementById('recovery-success').textContent = successCount;
        document.getElementById('recovery-failed').textContent = failedCount;

        var tbody = document.getElementById('recovery-rows');
        if (logs.length === 0) {
            tbody.innerHTML = '<tr><td colspan="5" style="text-align:center;color:#64748b;">No recovery events recorded yet</td></tr>';
            return;
        }

        var html = '';
        for (var i = 0; i < logs.length; i++) {
            var log = logs[i];
            var isProximity = log.strategy_name === 'PROXIMITY_ALERT';
            var statusBadge;
            if (isProximity) {
                statusBadge = '<span class="badge" style="background:rgba(234,179,8,0.15);color:#eab308;border:1px solid rgba(234,179,8,0.3);" data-testid="badge-recovery-status-' + log.id + '">Proximity Alert</span>';
            } else if (log.status === 'SUCCESS') {
                statusBadge = '<span class="badge status-open" data-testid="badge-recovery-status-' + log.id + '">Backfilled</span>';
            } else if (log.status === 'FAILED') {
                statusBadge = '<span class="badge status-closed" data-testid="badge-recovery-status-' + log.id + '">Failed</span>';
            } else {
                statusBadge = '<span class="badge status-expired" data-testid="badge-recovery-status-' + log.id + '">Skipped</span>';
            }

            var assets = '';
            if (Array.isArray(log.assets_affected)) {
                assets = log.assets_affected.join(', ');
            } else {
                assets = String(log.assets_affected || '--');
            }

            var strategyLabel = isProximity ? 'Proximity Alert' : (log.strategy_name || '').replace(/_/g, ' ').replace(/\\b\\w/g, function(c) { return c.toUpperCase(); });
            var rowClass = isProximity ? ' class="proximity-row"' : '';
            var muteBtn = isProximity ? ' <button class="btn-mute" onclick="muteAlert(' + log.id + ')" data-testid="button-mute-alert-' + log.id + '">Mute</button>' : '';

            html += '<tr' + rowClass + ' data-testid="row-recovery-' + log.id + '">' +
                '<td style="font-weight:500;color:' + (isProximity ? '#eab308' : '#f1f5f9') + ';">' + strategyLabel + '</td>' +
                '<td>' + statusBadge + muteBtn + '</td>' +
                '<td style="color:#94a3b8;">' + formatRecoveryDate(log.missed_window_time) + '</td>' +
                '<td style="color:#94a3b8;">' + formatRecoveryDate(log.execution_time) + '</td>' +
                '<td style="color:#e2e8f0;font-size:13px;">' + (isProximity ? '<span style="color:#eab308;font-size:12px;">' + (log.status || '') + '</span>' : assets) + '</td>' +
                '</tr>';
        }
        tbody.innerHTML = html;

    } catch (e) {
        document.getElementById('recovery-loading').textContent = 'Failed to load recovery logs.';
    }
}

async function muteAlert(id) {
    if (!confirm('Mute this proximity alert?')) return;
    try {
        var res = await fetch(BASE + '/admin/api/recovery-logs/' + id, { method: 'DELETE' });
        if (res.ok) {
            loadRecoveryLogs();
        } else {
            alert('Failed to mute alert');
        }
    } catch (e) {
        alert('Error muting alert');
    }
}

async function loadNotifConfig() {
    try {
        const res = await fetch(BASE + '/admin/api/notifications');
        const data = await res.json();
        document.getElementById('notif-loading').style.display = 'none';
        document.getElementById('notif-content').style.display = 'block';

        document.getElementById('notif-master-toggle').checked = data.enabled;
        updateNotifUIState(data.enabled);

        if (data.webhook_url) {
            document.getElementById('notif-webhook-url').value = data.webhook_url;
            document.getElementById('notif-webhook-status').innerHTML = '<span style="color:#22c55e;">Webhook configured</span> (auto-detected type: ' + detectType(data.webhook_url) + ')';
        } else {
            document.getElementById('notif-webhook-status').innerHTML = '<span style="color:#94a3b8;">No webhook URL configured</span>';
        }

        var cats = data.categories || {};
        ['new_signals', 'strategy_failures', 'credit_warnings', 'scheduler_alerts'].forEach(function(cat) {
            var el = document.getElementById('notif-cat-' + cat);
            if (el) el.checked = cats[cat] !== false;
        });
    } catch (e) {
        document.getElementById('notif-loading').textContent = 'Failed to load notification settings.';
    }
}

function detectType(url) {
    if (!url) return 'none';
    if (url.indexOf('discord.com/api/webhooks') !== -1 || url.indexOf('discordapp.com/api/webhooks') !== -1) return 'Discord';
    if (url.indexOf('hooks.slack.com') !== -1) return 'Slack';
    return 'Generic Webhook';
}

function updateNotifUIState(enabled) {
    var overlay = document.getElementById('notif-disabled-overlay');
    var cats = document.getElementById('notif-categories');
    if (enabled) {
        overlay.style.display = 'none';
        if (cats) cats.style.opacity = '1';
        if (cats) cats.style.pointerEvents = 'auto';
    } else {
        overlay.style.display = 'block';
        if (cats) cats.style.opacity = '0.5';
        if (cats) cats.style.pointerEvents = 'none';
    }
}

async function updateNotifMaster(enabled) {
    try {
        await fetch(BASE + '/admin/api/notifications', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({enabled: enabled})
        });
        updateNotifUIState(enabled);
    } catch (e) {
        alert('Failed to update setting: ' + e.message);
    }
}

async function updateNotifCategory(category, enabled) {
    try {
        var cats = {};
        cats[category] = enabled;
        await fetch(BASE + '/admin/api/notifications', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({categories: cats})
        });
    } catch (e) {
        alert('Failed to update category: ' + e.message);
    }
}

async function saveWebhookUrl() {
    var url = document.getElementById('notif-webhook-url').value.trim();
    if (!url) {
        document.getElementById('notif-webhook-status').innerHTML = '<span style="color:#ef4444;">Please enter a webhook URL.</span>';
        return;
    }
    try {
        var res = await fetch(BASE + '/admin/api/notifications', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({webhook_url: url})
        });
        var data = await res.json();
        if (data.status === 'ok') {
            document.getElementById('notif-webhook-status').innerHTML = '<span style="color:#22c55e;">Webhook URL saved successfully</span> (type: ' + detectType(url) + ')';
        } else {
            document.getElementById('notif-webhook-status').innerHTML = '<span style="color:#ef4444;">Failed to save webhook URL.</span>';
        }
    } catch (e) {
        document.getElementById('notif-webhook-status').innerHTML = '<span style="color:#ef4444;">Error: ' + e.message + '</span>';
    }
}

async function clearWebhookUrl() {
    try {
        await fetch(BASE + '/admin/api/notifications', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({webhook_url: ''})
        });
        document.getElementById('notif-webhook-url').value = '';
        document.getElementById('notif-webhook-status').innerHTML = '<span style="color:#94a3b8;">Webhook URL cleared</span>';
    } catch (e) {
        document.getElementById('notif-webhook-status').innerHTML = '<span style="color:#ef4444;">Error: ' + e.message + '</span>';
    }
}

async function testWebhook() {
    var resultEl = document.getElementById('notif-test-result');
    resultEl.innerHTML = '<span style="color:#94a3b8;">Sending test notification...</span>';
    try {
        var res = await fetch(BASE + '/admin/api/webhook/test', {method: 'POST'});
        var data = await res.json();
        if (data.status === 'ok') {
            resultEl.innerHTML = '<span style="color:#22c55e;">Test notification sent. Check your webhook endpoint.</span>';
        } else {
            resultEl.innerHTML = '<span style="color:#ef4444;">' + (data.message || 'Failed to send test.') + '</span>';
        }
    } catch (e) {
        resultEl.innerHTML = '<span style="color:#ef4444;">Error: ' + e.message + '</span>';
    }
}

document.addEventListener('DOMContentLoaded', function() {
    const activeTab = document.querySelector('.tab.active');
    var tabName = activeTab ? activeTab.getAttribute('data-tab') : 'signals';
    if (tabName === 'analysis' || !activeTab) loadMarketPulse();
    if (tabName === 'credits' || !activeTab) loadResourceOverview();
    if (tabName === 'settings') loadCreditMeter();
    if (tabName === 'notifications') loadNotifConfig();
    if (tabName === 'scheduler') loadSchedulerData();
    if (tabName === 'system') loadSystemStatus();
    if (tabName === 'users') loadRegistrationToggle();
    if (tabName === 'wordpress') { loadUserCmsConfigs(); }
});

function copyApiUrl(path) {
    var fullUrl = window.location.origin + path;
    navigator.clipboard.writeText(fullUrl).then(function() {
        var toast = document.getElementById('copy-toast');
        if (toast) {
            toast.style.display = 'block';
            setTimeout(function() { toast.style.display = 'none'; }, 2000);
        }
    }).catch(function() {
        prompt('Copy this URL:', fullUrl);
    });
}

function ucmsShowMsg(msg, isError) {
    var el = document.getElementById('ucms-status-msg');
    el.textContent = msg;
    el.style.display = 'block';
    el.style.background = isError ? '#7f1d1d' : '#14532d';
    el.style.color = isError ? '#fca5a5' : '#86efac';
    setTimeout(function() { el.style.display = 'none'; }, 5000);
}

async function loadUserCmsConfigs() {
    var colCount = IS_ADMIN ? 7 : 6;
    try {
        var resp = await fetch(BASE + '/admin/api/user-cms-configs');
        if (!resp.ok) throw new Error('Failed to load');
        var data = await resp.json();
        var tbody = document.getElementById('ucms-body');
        if (!data.length) {
            tbody.innerHTML = '<tr><td colspan="' + colCount + '" style="text-align:center;color:#64748b;">No WordPress credentials configured</td></tr>';
            return;
        }
        tbody.innerHTML = data.map(function(c) {
            var statusBadge = c.is_active
                ? '<span style="background:#14532d;color:#86efac;padding:2px 10px;border-radius:9999px;font-size:12px;">Active</span>'
                : '<span style="background:#7f1d1d;color:#fca5a5;padding:2px 10px;border-radius:9999px;font-size:12px;">Inactive</span>';
            var ownerCol = IS_ADMIN ? '<td>' + (c.owner || 'user_' + c.user_id) + '</td>' : '';
            return '<tr data-testid="row-wp-cred-' + c.id + '">' +
                '<td>' + c.id + '</td>' +
                ownerCol +
                '<td style="max-width:200px;overflow:hidden;text-overflow:ellipsis;">' + (c.site_url || '') + '</td>' +
                '<td>' + (c.wp_username || '') + '</td>' +
                '<td>' + statusBadge + '</td>' +
                '<td style="font-size:12px;color:#94a3b8;">' + (c.created_at || '') + '</td>' +
                '<td style="display:flex;gap:6px;">' +
                    '<button class="btn btn-primary" style="padding:4px 10px;font-size:12px;" onclick="ucmsTest(' + c.id + ')" data-testid="btn-wp-test-' + c.id + '">Test</button>' +
                    '<button class="btn" style="padding:4px 10px;font-size:12px;background:#7f1d1d;color:#fca5a5;" onclick="ucmsDelete(' + c.id + ')" data-testid="btn-wp-delete-' + c.id + '">Delete</button>' +
                '</td></tr>';
        }).join('');
    } catch(e) {
        document.getElementById('ucms-body').innerHTML = '<tr><td colspan="' + colCount + '" style="text-align:center;color:#f87171;">Error loading credentials</td></tr>';
    }
}

async function ucmsCreate() {
    var url = document.getElementById('ucms-url').value.trim();
    var username = document.getElementById('ucms-username').value.trim();
    var password = document.getElementById('ucms-password').value.trim();
    var userIdStr = document.getElementById('ucms-user-id').value.trim();
    if (!url || !username || !password) {
        ucmsShowMsg('Site URL, Username, and Password are required', true);
        return;
    }
    var body = {site_url: url, wp_username: username, app_password: password};
    if (userIdStr) body.user_id = parseInt(userIdStr, 10);
    try {
        var resp = await fetch(BASE + '/admin/api/user-cms-configs', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify(body)
        });
        var data = await resp.json();
        if (resp.ok) {
            ucmsShowMsg('Config created (ID: ' + data.id + ')', false);
            document.getElementById('ucms-add-form').style.display = 'none';
            document.getElementById('ucms-url').value = '';
            document.getElementById('ucms-username').value = '';
            document.getElementById('ucms-password').value = '';
            document.getElementById('ucms-user-id').value = '';
            loadUserCmsConfigs();
        } else {
            ucmsShowMsg(data.message || 'Create failed', true);
        }
    } catch(e) {
        ucmsShowMsg('Network error', true);
    }
}

async function ucmsDelete(id) {
    if (!confirm('Delete user CMS config #' + id + '?')) return;
    try {
        var resp = await fetch(BASE + '/admin/api/user-cms-configs/' + id, {method: 'DELETE'});
        var data = await resp.json();
        if (resp.ok) {
            ucmsShowMsg('Config deleted', false);
            loadUserCmsConfigs();
        } else {
            ucmsShowMsg(data.message || 'Delete failed', true);
        }
    } catch(e) {
        ucmsShowMsg('Network error', true);
    }
}

async function ucmsTest(id) {
    ucmsShowMsg('Testing connection...', false);
    try {
        var resp = await fetch(BASE + '/admin/api/user-cms-configs/' + id + '/test', {method: 'POST'});
        var data = await resp.json();
        if (data.status === 'ok') {
            ucmsShowMsg('Connection successful! Site: ' + (data.site_name || 'OK'), false);
        } else {
            ucmsShowMsg('Connection failed: ' + (data.message || 'Unknown error'), true);
        }
    } catch(e) {
        ucmsShowMsg('Network error during test', true);
    }
}
"""


@router.get("/api/auth-status")
def auth_status(request: Request):
    user = _get_session_user(request)
    if user:
        return JSONResponse(content={"authenticated": True, "username": user.get("username", ""), "role": user.get("role", "CUSTOMER")})
    return JSONResponse(content={"authenticated": False})


@router.get("/login", response_class=HTMLResponse)
def login_page(request: Request, error: str = Query(""), registered: str = Query("")):
    user = _get_session_user(request)
    if user:
        return RedirectResponse(url=request.scope.get("root_path", "") + "/admin/", status_code=302)
    success = "Account created successfully! Please sign in." if registered == "1" else ""
    return HTMLResponse(content=_build_login_page(error, success))


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

    hlc_data = _get_hlc_fx_data()
    hlc_signals = get_all_signals(strategy_name="highest_lowest_fx", limit=200)
    hlc_signal_rows = _signals_to_table_rows(hlc_signals)
    hlc_signal_count = len(hlc_signals)
    hlc_fx_html = _build_hlc_fx_html(hlc_data, hlc_signal_rows, hlc_signal_count)

    analysis_data = _get_signal_analysis_data()
    signal_analysis_html = _build_signal_analysis_html(analysis_data)

    strategy_options = ""
    strategy_choices = [
        ("", "All Strategies"),
        ("mtf_ema", "MTF EMA"),
        ("trend_non_forex", "Trend Non-Forex"),
        ("sp500_momentum", "SP500 Momentum"),
        ("trend_forex", "Trend Forex"),
        ("highest_lowest_fx", "Highest/Lowest FX"),
    ]
    for s, label in strategy_choices:
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
    user_role = user.get("role", "CUSTOMER")
    is_admin = user_role == "ADMIN"

    if not is_admin and tab in ("analysis", "mtf", "trend_following", "spx", "forex_trend", "hlc_fx", "credits", "settings", "users", "notifications", "scheduler", "system"):
        tab = "signals"

    def _sidebar_link(tab_name, label, svg, active_tab, testid):
        active_cls = "active" if active_tab == tab_name else ""
        return f'<a class="sidebar-link {active_cls}" data-tab="{tab_name}" onclick="showTab(\'{tab_name}\')" data-testid="{testid}">{svg}{label}</a>'

    def _mobile_tab(tab_name, label, active_tab):
        active_cls = "active" if active_tab == tab_name else ""
        return f'<a class="tab {active_cls}" data-tab="{tab_name}" onclick="showTab(\'{tab_name}\')">{label}</a>'

    svg_analysis = '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 21H4.6c-.56 0-.84 0-1.054-.109a1 1 0 0 1-.437-.437C3 20.24 3 19.96 3 19.4V3"/><path d="m7 14 4-4 4 4 6-6"/></svg>'
    svg_mtf = '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M2 20h.01"/><path d="M7 20v-4"/><path d="M12 20v-8"/><path d="M17 20V8"/><path d="M22 4v16"/></svg>'
    svg_trend = '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="22 7 13.5 15.5 8.5 10.5 2 17"/><polyline points="16 7 22 7 22 13"/></svg>'
    svg_spx = '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect width="18" height="18" x="3" y="3" rx="2"/><path d="M3 9h18"/><path d="M9 21V9"/></svg>'
    svg_globe = '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><path d="M2 12h20"/><path d="M12 2a15.3 15.3 0 0 1 4 10 15.3 15.3 0 0 1-4 10 15.3 15.3 0 0 1-4-10 15.3 15.3 0 0 1 4-10z"/></svg>'
    svg_hlc = '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M8 3v3a2 2 0 0 1-2 2H3"/><path d="M21 8h-3a2 2 0 0 1-2-2V3"/><path d="M3 16h3a2 2 0 0 1 2 2v3"/><path d="M16 21v-3a2 2 0 0 1 2-2h3"/></svg>'
    svg_credits = '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><path d="M16 8h-6a2 2 0 1 0 0 4h4a2 2 0 1 1 0 4H8"/><path d="M12 18V6"/></svg>'
    svg_clock = '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><polyline points="12 6 12 12 16 14"/></svg>'
    svg_settings = '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12.22 2h-.44a2 2 0 0 0-2 2v.18a2 2 0 0 1-1 1.73l-.43.25a2 2 0 0 1-2 0l-.15-.08a2 2 0 0 0-2.73.73l-.22.38a2 2 0 0 0 .73 2.73l.15.1a2 2 0 0 1 1 1.72v.51a2 2 0 0 1-1 1.74l-.15.09a2 2 0 0 0-.73 2.73l.22.38a2 2 0 0 0 2.73.73l.15-.08a2 2 0 0 1 2 0l.43.25a2 2 0 0 1 1 1.73V20a2 2 0 0 0 2 2h.44a2 2 0 0 0 2-2v-.18a2 2 0 0 1 1-1.73l.43-.25a2 2 0 0 1 2 0l.15.08a2 2 0 0 0 2.73-.73l.22-.39a2 2 0 0 0-.73-2.73l-.15-.08a2 2 0 0 1-1-1.74v-.5a2 2 0 0 1 1-1.74l.15-.09a2 2 0 0 0 .73-2.73l-.22-.38a2 2 0 0 0-2.73-.73l-.15.08a2 2 0 0 1-2 0l-.43-.25a2 2 0 0 1-1-1.73V4a2 2 0 0 0-2-2z"/><circle cx="12" cy="12" r="3"/></svg>'
    svg_users = '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M16 21v-2a4 4 0 0 0-4-4H6a4 4 0 0 0-4 4v2"/><circle cx="9" cy="7" r="4"/><path d="M22 21v-2a4 4 0 0 0-3-3.87"/><path d="M16 3.13a4 4 0 0 1 0 7.75"/></svg>'
    svg_bell = '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M6 8a6 6 0 0 1 12 0c0 7 3 9 3 9H3s3-2 3-9"/><path d="M10.3 21a1.94 1.94 0 0 0 3.4 0"/></svg>'
    svg_calendar = '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect width="18" height="18" x="3" y="4" rx="2" ry="2"/><line x1="16" x2="16" y1="2" y2="6"/><line x1="8" x2="8" y1="2" y2="6"/><line x1="3" x2="21" y1="10" y2="10"/></svg>'
    svg_shield = '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z"/></svg>'
    svg_key = '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="m21 2-2 2m-7.61 7.61a5.5 5.5 0 1 1-7.778 7.778 5.5 5.5 0 0 1 7.777-7.777zm0 0L15.5 7.5m0 0 3 3L22 7l-3-3m-3.5 3.5L19 4"/></svg>'
    svg_book = '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M4 19.5v-15A2.5 2.5 0 0 1 6.5 2H20v20H6.5a2.5 2.5 0 0 1 0-5H20"/><path d="M8 7h6"/><path d="M8 11h8"/></svg>'

    analysis_link = _sidebar_link("analysis", "Signal Analysis", svg_analysis, tab, "sidebar-analysis") if is_admin else ""

    strategies_block = ""
    if is_admin:
        strategies_block = '<div class="sidebar-group"><div class="sidebar-group-label">Strategies</div>'
        strategies_block += _sidebar_link("mtf", "MTF EMA", svg_mtf, tab, "sidebar-mtf")
        strategies_block += _sidebar_link("trend_following", "Trend Non-Forex", svg_trend, tab, "sidebar-trend")
        strategies_block += _sidebar_link("spx", "SP500 Momentum", svg_spx, tab, "sidebar-spx")
        strategies_block += _sidebar_link("forex_trend", "Trend Forex", svg_globe, tab, "sidebar-forex-trend")
        strategies_block += _sidebar_link("hlc_fx", "Highest/Lowest FX", svg_hlc, tab, "sidebar-hlc-fx")
        strategies_block += "</div>"

    system_group_label = "System" if is_admin else "Tools"
    admin_system_links = ""
    if is_admin:
        admin_system_links += _sidebar_link("credits", f"Credit Monitor{alert_badge}", svg_credits, tab, "sidebar-credits")
        admin_system_links += _sidebar_link("timezone", "Market Hours", svg_clock, tab, "sidebar-timezone")
        admin_system_links += _sidebar_link("settings", "Settings", svg_settings, tab, "sidebar-settings")
        admin_system_links += _sidebar_link("users", "User Settings", svg_users, tab, "sidebar-users")
        admin_system_links += _sidebar_link("notifications", "Notifications", svg_bell, tab, "sidebar-notifications")
        admin_system_links += _sidebar_link("apikeys", "Partner API Keys", svg_key, tab, "sidebar-apikeys")
        admin_system_links += _sidebar_link("scheduler", "Scheduler Health", svg_calendar, tab, "sidebar-scheduler")
        admin_system_links += _sidebar_link("system", "System Status", svg_shield, tab, "sidebar-system")
        admin_system_links += _sidebar_link("recovery", "Recovery Logs", svg_calendar, tab, "sidebar-recovery")

    admin_mobile_tabs = ""
    if is_admin:
        admin_mobile_tabs += _mobile_tab("analysis", "Analysis", tab)
        admin_mobile_tabs += _mobile_tab("spx", "SP500", tab)
        admin_mobile_tabs += _mobile_tab("forex_trend", "Trend FX", tab)
        admin_mobile_tabs += _mobile_tab("hlc_fx", "HLC FX", tab)
        admin_mobile_tabs += _mobile_tab("credits", "Credits", tab)
        admin_mobile_tabs += _mobile_tab("timezone", "Hours", tab)
        admin_mobile_tabs += _mobile_tab("settings", "Settings", tab)
        admin_mobile_tabs += _mobile_tab("users", "Users", tab)
        admin_mobile_tabs += _mobile_tab("notifications", "Alerts", tab)
        admin_mobile_tabs += _mobile_tab("scheduler", "Scheduler", tab)
        admin_mobile_tabs += _mobile_tab("system", "System", tab)
        admin_mobile_tabs += _mobile_tab("recovery", "Recovery", tab)

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
                    {_sidebar_link("signals", "My Signals", '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M3 3v18h18"/><path d="m19 9-5 5-4-4-3 3"/></svg>', tab, "sidebar-signals")}
                    {analysis_link}
                </div>
                {strategies_block}
                <div class="sidebar-group">
                    <div class="sidebar-group-label">{system_group_label}</div>
                    {admin_system_links}
                    {_sidebar_link("wordpress", "WordPress", svg_globe, tab, "sidebar-wordpress")}
                    {_sidebar_link("api_catalog", "API Catalog", svg_book, tab, "sidebar-api-catalog")}
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
                {_mobile_tab("signals", "My Signals", tab)}
                {admin_mobile_tabs}
                {_mobile_tab("wordpress", "WordPress", tab)}
                {_mobile_tab("api_catalog", "API", tab)}
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

        <div id="tab-analysis" class="tab-content {'hidden' if tab != 'analysis' else ''}">
            <div class="section">
                <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:16px;">
                    <h2 style="margin-bottom:0;">Market Pulse</h2>
                    <button class="btn btn-secondary" onclick="loadMarketPulse()" data-testid="button-refresh-pulse" style="font-size:13px;padding:6px 14px;">Refresh</button>
                </div>
                <div id="market-pulse-container" class="market-pulse" data-testid="market-pulse-container">
                    <div style="color:#94a3b8;font-size:0.85rem;">Loading market pulse...</div>
                </div>
            </div>
            <div class="section">
                <h2>Signal Analysis</h2>
                <p style="color:#94a3b8;margin-bottom:20px;">Real-time evaluation of all strategy entry conditions across every tracked asset.</p>
                {signal_analysis_html}
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

        <div id="tab-hlc_fx" class="tab-content {'hidden' if tab != 'hlc_fx' else ''}">
            <div class="section">
                <h2>Highest/Lowest Close FX Strategy</h2>
                {hlc_fx_html}
            </div>
        </div>

        <div id="tab-credits" class="tab-content {'hidden' if tab != 'credits' else ''}">
            <div id="resource-overview" class="ro-card" data-testid="widget-resource-overview">
                <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:20px;">
                    <h2 style="margin-bottom:0;font-size:1.1rem;color:#f8fafc;">Resource Overview</h2>
                    <button class="btn btn-secondary" onclick="loadResourceOverview()" data-testid="button-refresh-resources" style="font-size:12px;padding:5px 12px;margin:0;">Refresh</button>
                </div>
                <div class="ro-grid">
                    <div class="ro-col">
                        <div class="ro-title">API Health</div>
                        <div style="position:relative;width:180px;height:108px;">
                            <svg id="ro-gauge-svg" width="180" height="108" viewBox="0 0 180 108" data-testid="gauge-api-health">
                                <path d="M 18 90 A 72 72 0 1 1 162 90" fill="none" stroke="#334155" stroke-width="18" stroke-linecap="round" />
                                <path class="gauge-arc-fill" d="M 18 90 A 72 72 0 1 1 162 90" fill="none" stroke="#22c55e" stroke-width="18" stroke-linecap="round" style="stroke-dasharray:226.19;stroke-dashoffset:226.19;transition:stroke-dashoffset 0.8s ease,stroke 0.4s ease;" />
                            </svg>
                            <div style="position:absolute;bottom:4px;left:50%;transform:translateX(-50%);text-align:center;">
                                <div id="ro-gauge-pct" class="gauge-pct" style="color:#64748b;" data-testid="text-gauge-pct">--%</div>
                                <div class="gauge-label">Credits Used</div>
                            </div>
                        </div>
                        <div id="ro-credit-text" class="ro-text" data-testid="text-credits-remaining">Loading...</div>
                        <div id="ro-recharge-badge" class="badge-recharge" style="display:none;" data-testid="badge-recharge-soon">Recharge Soon</div>
                    </div>
                    <div class="ro-col">
                        <div class="ro-title">Storage Health</div>
                        <div class="vbar-wrap">
                            <div class="vbar-labels">
                                <div class="vbar-label">100%</div>
                                <div class="vbar-label">50%</div>
                                <div class="vbar-label">0%</div>
                            </div>
                            <div class="vbar-track" data-testid="bar-storage-vertical">
                                <div id="ro-vbar-fill" class="vbar-fill" style="height:0%;background:#334155;"></div>
                            </div>
                            <div style="display:flex;flex-direction:column;justify-content:flex-end;height:100%;">
                                <div id="ro-storage-pct" style="font-size:1.4rem;font-weight:800;color:#64748b;line-height:1;" data-testid="text-storage-pct">--%</div>
                                <div style="font-size:0.65rem;color:#64748b;text-transform:uppercase;margin-top:2px;">Used</div>
                            </div>
                        </div>
                        <div id="ro-storage-text" class="ro-text" data-testid="text-storage-detail">Loading...</div>
                    </div>
                </div>
            </div>
            <div class="section" style="margin-top:16px;">
                <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:16px;">
                    <h2 style="margin-bottom:0;">API Credits</h2>
                </div>
                <div class="stats-grid" id="api-credits-cards" data-testid="api-credits-cards">
                    <div class="stat-card">
                        <div class="stat-label">Remaining</div>
                        <div class="stat-value" id="quota-remaining" data-testid="text-quota-remaining">--</div>
                    </div>
                    <div class="stat-card">
                        <div class="stat-label">% Left</div>
                        <div class="stat-value" id="quota-pct-text" data-testid="text-quota-pct" style="color:#94a3b8;">--</div>
                    </div>
                    <div class="stat-card">
                        <div class="stat-label">Daily Avg Burn</div>
                        <div class="stat-value" id="quota-daily-burn" data-testid="text-quota-daily-burn">--</div>
                    </div>
                    <div class="stat-card">
                        <div class="stat-label">Est. Days Left</div>
                        <div class="stat-value" id="quota-days-left" data-testid="text-quota-days-left">--</div>
                    </div>
                </div>
            </div>
            <div class="section" style="margin-top:16px;">
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
            <div class="section" style="margin-bottom:20px;">
                <div style="display:flex;align-items:center;justify-content:space-between;padding:16px 20px;background:rgba(30,41,59,0.5);border:1px solid rgba(148,163,184,0.1);border-radius:10px;">
                    <div>
                        <div style="font-weight:600;color:#f1f5f9;font-size:15px;">Public Registration</div>
                        <div style="color:#94a3b8;font-size:13px;margin-top:2px;">Allow new users to register from the login page. When disabled, only admins can create accounts.</div>
                    </div>
                    <label class="toggle-switch" data-testid="toggle-registration">
                        <input type="checkbox" id="registration-toggle" onchange="toggleRegistration(this.checked)">
                        <span class="toggle-slider"></span>
                    </label>
                </div>
                <div id="reg-toggle-msg" style="display:none;margin-top:10px;padding:8px 14px;border-radius:6px;font-size:13px;" data-testid="text-registration-status"></div>
            </div>
            <div class="section">
                <h2>User Settings</h2>
                {users_html}
            </div>
        </div>
        <div id="tab-notifications" class="tab-content {'hidden' if tab != 'notifications' else ''}">
            <div class="section">
                <h2>Notification Settings</h2>
                <p style="color:#94a3b8;margin-bottom:20px;">Configure webhook notifications for trading alerts, system events, and warnings. Notifications are sent to Discord, Slack, or any generic webhook endpoint.</p>

                <div id="notif-loading" style="text-align:center;padding:40px;color:#94a3b8;">Loading notification settings...</div>
                <div id="notif-content" style="display:none;">

                <div style="display:flex;align-items:center;justify-content:space-between;padding:16px 20px;background:rgba(30,41,59,0.5);border:1px solid rgba(148,163,184,0.1);border-radius:10px;margin-bottom:24px;">
                    <div>
                        <div style="font-weight:600;color:#f1f5f9;font-size:15px;">Master Notifications Toggle</div>
                        <div style="color:#94a3b8;font-size:13px;margin-top:2px;">Enable or disable all webhook notifications globally</div>
                    </div>
                    <label class="toggle-switch" data-testid="toggle-notifications-master">
                        <input type="checkbox" id="notif-master-toggle" onchange="updateNotifMaster(this.checked)">
                        <span class="toggle-slider"></span>
                    </label>
                </div>

                <div style="padding:20px;background:rgba(30,41,59,0.5);border:1px solid rgba(148,163,184,0.1);border-radius:10px;margin-bottom:24px;">
                    <div style="font-weight:600;color:#f1f5f9;font-size:15px;margin-bottom:16px;">Webhook URL</div>
                    <div style="display:flex;gap:8px;align-items:stretch;">
                        <input type="text" id="notif-webhook-url" placeholder="https://discord.com/api/webhooks/... or https://hooks.slack.com/..." style="flex:1;padding:10px 14px;background:rgba(15,23,42,0.6);border:1px solid rgba(148,163,184,0.15);border-radius:8px;color:#f1f5f9;font-size:14px;font-family:monospace;" data-testid="input-webhook-url">
                        <button class="btn btn-primary" onclick="saveWebhookUrl()" data-testid="button-save-webhook" style="white-space:nowrap;">Save URL</button>
                        <button class="btn" onclick="clearWebhookUrl()" data-testid="button-clear-webhook" style="white-space:nowrap;background:rgba(239,68,68,0.15);color:#ef4444;border:1px solid rgba(239,68,68,0.3);">Clear</button>
                    </div>
                    <div id="notif-webhook-status" style="margin-top:10px;font-size:13px;color:#94a3b8;"></div>
                    <div style="margin-top:12px;display:flex;gap:8px;">
                        <button class="btn" onclick="testWebhook()" data-testid="button-test-webhook" style="background:rgba(59,130,246,0.15);color:#3b82f6;border:1px solid rgba(59,130,246,0.3);font-size:13px;">Send Test Notification</button>
                    </div>
                    <div id="notif-test-result" style="margin-top:8px;font-size:13px;"></div>
                </div>

                <div style="padding:20px;background:rgba(30,41,59,0.5);border:1px solid rgba(148,163,184,0.1);border-radius:10px;margin-bottom:24px;">
                    <div style="font-weight:600;color:#f1f5f9;font-size:15px;margin-bottom:4px;">Notification Categories</div>
                    <div style="color:#94a3b8;font-size:13px;margin-bottom:16px;">Choose which types of notifications to receive</div>

                    <div id="notif-categories" style="display:flex;flex-direction:column;gap:12px;">
                        <div style="display:flex;align-items:center;justify-content:space-between;padding:12px 16px;background:rgba(15,23,42,0.4);border-radius:8px;">
                            <div>
                                <div style="font-weight:500;color:#f1f5f9;font-size:14px;">New Signals</div>
                                <div style="color:#64748b;font-size:12px;">New trading signals generated by strategies</div>
                            </div>
                            <label class="toggle-switch" data-testid="toggle-category-new-signals">
                                <input type="checkbox" id="notif-cat-new_signals" onchange="updateNotifCategory('new_signals', this.checked)">
                                <span class="toggle-slider"></span>
                            </label>
                        </div>
                        <div style="display:flex;align-items:center;justify-content:space-between;padding:12px 16px;background:rgba(15,23,42,0.4);border-radius:8px;">
                            <div>
                                <div style="font-weight:500;color:#f1f5f9;font-size:14px;">Strategy Failures</div>
                                <div style="color:#64748b;font-size:12px;">Strategy run failures or partial errors</div>
                            </div>
                            <label class="toggle-switch" data-testid="toggle-category-strategy-failures">
                                <input type="checkbox" id="notif-cat-strategy_failures" onchange="updateNotifCategory('strategy_failures', this.checked)">
                                <span class="toggle-slider"></span>
                            </label>
                        </div>
                        <div style="display:flex;align-items:center;justify-content:space-between;padding:12px 16px;background:rgba(15,23,42,0.4);border-radius:8px;">
                            <div>
                                <div style="font-weight:500;color:#f1f5f9;font-size:14px;">Credit Warnings</div>
                                <div style="color:#64748b;font-size:12px;">Credit usage warnings and kill switch alerts</div>
                            </div>
                            <label class="toggle-switch" data-testid="toggle-category-credit-warnings">
                                <input type="checkbox" id="notif-cat-credit_warnings" onchange="updateNotifCategory('credit_warnings', this.checked)">
                                <span class="toggle-slider"></span>
                            </label>
                        </div>
                        <div style="display:flex;align-items:center;justify-content:space-between;padding:12px 16px;background:rgba(15,23,42,0.4);border-radius:8px;">
                            <div>
                                <div style="font-weight:500;color:#f1f5f9;font-size:14px;">Scheduler Alerts</div>
                                <div style="color:#64748b;font-size:12px;">Scheduler down or restart events</div>
                            </div>
                            <label class="toggle-switch" data-testid="toggle-category-scheduler-alerts">
                                <input type="checkbox" id="notif-cat-scheduler_alerts" onchange="updateNotifCategory('scheduler_alerts', this.checked)">
                                <span class="toggle-slider"></span>
                            </label>
                        </div>
                    </div>
                </div>

                <div id="notif-disabled-overlay" style="display:none;padding:16px 20px;background:rgba(239,68,68,0.08);border:1px solid rgba(239,68,68,0.2);border-radius:10px;margin-bottom:16px;">
                    <div style="color:#ef4444;font-weight:500;font-size:14px;">Notifications are currently disabled</div>
                    <div style="color:#94a3b8;font-size:13px;margin-top:4px;">Turn on the master toggle above to enable webhook notifications.</div>
                </div>

                </div>
            </div>
        </div>
        <div id="tab-apikeys" class="tab-content {'hidden' if tab != 'apikeys' else ''}">
            <div class="section">
                <h2>Partner API Keys</h2>
                <p style="color:#94a3b8;margin-bottom:20px;">Manage API keys for DailyForex frontend and partner integrations. Keys with valid <code style="background:#1e293b;padding:2px 6px;border-radius:4px;font-size:13px;">X-API-KEY</code> headers get higher rate limits and bypass standard IP-based throttling.</p>

                <div id="apikeys-loading" style="text-align:center;padding:40px;color:#94a3b8;">Loading API keys...</div>
                <div id="apikeys-content" style="display:none;">

                <div id="require-apikey-banner" style="padding:12px 16px;border-radius:8px;margin-bottom:20px;font-size:14px;display:flex;align-items:center;gap:10px;" data-testid="status-require-apikey"></div>

                <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:16px;flex-wrap:wrap;gap:12px;">
                    <div style="font-weight:600;color:#f1f5f9;font-size:15px;">Active Keys</div>
                    <button class="btn btn-primary" onclick="showCreateKeyModal()" data-testid="button-create-key" style="font-size:13px;padding:8px 16px;">+ Create API Key</button>
                </div>

                <div style="overflow-x:auto;">
                    <table class="data-table" data-testid="table-partner-keys">
                        <thead>
                            <tr>
                                <th>ID</th>
                                <th>Label</th>
                                <th>Tier</th>
                                <th>Rate Limit</th>
                                <th>Status</th>
                                <th>Last Used</th>
                                <th>Created</th>
                                <th>Actions</th>
                            </tr>
                        </thead>
                        <tbody id="apikeys-rows">
                            <tr><td colspan="8" style="text-align:center;color:#64748b;">No API keys created yet</td></tr>
                        </tbody>
                    </table>
                </div>

                <div style="margin-top:24px;padding:16px 20px;background:rgba(30,41,59,0.5);border:1px solid rgba(148,163,184,0.1);border-radius:10px;">
                    <div style="font-weight:600;color:#f1f5f9;margin-bottom:8px;">Rate Limit Tiers</div>
                    <div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(200px,1fr));gap:12px;font-size:13px;color:#94a3b8;">
                        <div><span style="color:#3b82f6;font-weight:600;">Standard:</span> 40 burst / 120/min / 5K/hr</div>
                        <div><span style="color:#a855f7;font-weight:600;">Premium:</span> 100 burst / 300/min / 20K/hr</div>
                        <div><span style="color:#f59e0b;font-weight:600;">Unlimited:</span> Virtually no limits</div>
                    </div>
                </div>

                <div style="margin-top:16px;padding:16px 20px;background:rgba(30,41,59,0.5);border:1px solid rgba(148,163,184,0.1);border-radius:10px;">
                    <div style="font-weight:600;color:#f1f5f9;margin-bottom:8px;">CORS Configuration</div>
                    <div style="font-size:13px;color:#94a3b8;">
                        Allowed origins: <code style="background:#1e293b;padding:2px 6px;border-radius:4px;">https://*.dailyforex.com</code>, Replit deployment URL, and localhost (dev only). Wildcard <code style="background:#1e293b;padding:2px 6px;border-radius:4px;">*</code> is explicitly blocked.
                    </div>
                </div>

                <div style="margin-top:16px;padding:16px 20px;background:rgba(30,41,59,0.5);border:1px solid rgba(148,163,184,0.1);border-radius:10px;">
                    <div style="font-weight:600;color:#f1f5f9;margin-bottom:8px;">Environment Flag</div>
                    <div style="font-size:13px;color:#94a3b8;">
                        Set <code style="background:#1e293b;padding:2px 6px;border-radius:4px;">REQUIRE_API_KEY=true</code> in environment to reject all <code>/api/v1/</code> requests without a valid <code>X-API-KEY</code> header. Auth and health endpoints remain open.
                    </div>
                </div>

                </div>
            </div>
        </div>

        <div id="create-key-modal" style="display:none;position:fixed;top:0;left:0;width:100%;height:100%;background:rgba(0,0,0,0.6);z-index:9999;display:none;align-items:center;justify-content:center;" data-testid="modal-create-key">
            <div style="background:#1e293b;border:1px solid #334155;border-radius:12px;padding:24px;max-width:440px;width:90%;">
                <h3 style="margin:0 0 16px;color:#f1f5f9;">Create Partner API Key</h3>
                <div style="margin-bottom:12px;">
                    <label style="display:block;font-size:13px;color:#94a3b8;margin-bottom:4px;">Label *</label>
                    <input id="key-label" type="text" placeholder="e.g. DailyForex Production" style="width:100%;padding:8px 12px;background:#0f172a;border:1px solid #334155;border-radius:6px;color:#f1f5f9;font-size:14px;" data-testid="input-key-label" />
                </div>
                <div style="margin-bottom:12px;">
                    <label style="display:block;font-size:13px;color:#94a3b8;margin-bottom:4px;">Tier</label>
                    <select id="key-tier" style="width:100%;padding:8px 12px;background:#0f172a;border:1px solid #334155;border-radius:6px;color:#f1f5f9;font-size:14px;" data-testid="select-key-tier">
                        <option value="standard">Standard (120/min)</option>
                        <option value="premium">Premium (300/min)</option>
                        <option value="unlimited">Unlimited</option>
                    </select>
                </div>
                <div style="margin-bottom:16px;">
                    <label style="display:block;font-size:13px;color:#94a3b8;margin-bottom:4px;">Custom Rate Limit (per minute)</label>
                    <input id="key-rate-limit" type="number" value="120" min="1" max="100000" style="width:100%;padding:8px 12px;background:#0f172a;border:1px solid #334155;border-radius:6px;color:#f1f5f9;font-size:14px;" data-testid="input-key-rate-limit" />
                </div>
                <div style="display:flex;gap:10px;justify-content:flex-end;">
                    <button class="btn" onclick="hideCreateKeyModal()" data-testid="button-cancel-key">Cancel</button>
                    <button class="btn btn-primary" onclick="createPartnerKey()" data-testid="button-submit-key">Create Key</button>
                </div>
            </div>
        </div>

        <div id="key-created-modal" style="display:none;position:fixed;top:0;left:0;width:100%;height:100%;background:rgba(0,0,0,0.6);z-index:10000;align-items:center;justify-content:center;" data-testid="modal-key-created">
            <div style="background:#1e293b;border:1px solid #334155;border-radius:12px;padding:24px;max-width:500px;width:90%;">
                <h3 style="margin:0 0 8px;color:#22c55e;">API Key Created Successfully</h3>
                <p style="color:#f59e0b;font-size:13px;margin-bottom:12px;">Copy this key now — it will not be shown again.</p>
                <div style="position:relative;">
                    <input id="new-key-value" type="text" readonly style="width:100%;padding:10px 12px;background:#0f172a;border:1px solid #334155;border-radius:6px;color:#38bdf8;font-family:monospace;font-size:14px;" data-testid="text-new-key" />
                    <button onclick="copyNewKey()" style="position:absolute;right:6px;top:50%;transform:translateY(-50%);padding:4px 10px;background:#3b82f6;border:none;border-radius:4px;color:white;font-size:12px;cursor:pointer;" data-testid="button-copy-key">Copy</button>
                </div>
                <div style="text-align:right;margin-top:16px;">
                    <button class="btn btn-primary" onclick="hideKeyCreatedModal()" data-testid="button-close-key-modal">Done</button>
                </div>
            </div>
        </div>

        <div id="tab-scheduler" class="tab-content {'hidden' if tab != 'scheduler' else ''}">
            <div class="section">
                <h2>Scheduler Health</h2>
                <p style="color:#94a3b8;margin-bottom:20px;">Monitor APScheduler status, job execution history, and watchdog heartbeat.</p>

                <div id="sched-loading" style="text-align:center;padding:40px;color:#94a3b8;">Loading scheduler data...</div>
                <div id="sched-content" style="display:none;">

                <div class="stats-grid" style="display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:16px;margin-bottom:24px;">
                    <div style="padding:16px 20px;background:rgba(30,41,59,0.5);border:1px solid rgba(148,163,184,0.1);border-radius:10px;">
                        <div style="font-size:12px;color:#64748b;text-transform:uppercase;letter-spacing:0.05em;">Status</div>
                        <div id="sched-status" style="font-size:22px;font-weight:700;margin-top:4px;" data-testid="text-scheduler-status">--</div>
                    </div>
                    <div style="padding:16px 20px;background:rgba(30,41,59,0.5);border:1px solid rgba(148,163,184,0.1);border-radius:10px;">
                        <div style="font-size:12px;color:#64748b;text-transform:uppercase;letter-spacing:0.05em;">Jobs Registered</div>
                        <div id="sched-jobs" style="font-size:22px;font-weight:700;color:#f1f5f9;margin-top:4px;" data-testid="text-scheduler-jobs">--</div>
                    </div>
                    <div style="padding:16px 20px;background:rgba(30,41,59,0.5);border:1px solid rgba(148,163,184,0.1);border-radius:10px;">
                        <div style="font-size:12px;color:#64748b;text-transform:uppercase;letter-spacing:0.05em;">24h Successes</div>
                        <div id="sched-success" style="font-size:22px;font-weight:700;color:#22c55e;margin-top:4px;" data-testid="text-scheduler-success">--</div>
                    </div>
                    <div style="padding:16px 20px;background:rgba(30,41,59,0.5);border:1px solid rgba(148,163,184,0.1);border-radius:10px;">
                        <div style="font-size:12px;color:#64748b;text-transform:uppercase;letter-spacing:0.05em;">24h Failures</div>
                        <div id="sched-failures" style="font-size:22px;font-weight:700;color:#ef4444;margin-top:4px;" data-testid="text-scheduler-failures">--</div>
                    </div>
                    <div style="padding:16px 20px;background:rgba(30,41,59,0.5);border:1px solid rgba(148,163,184,0.1);border-radius:10px;">
                        <div style="font-size:12px;color:#64748b;text-transform:uppercase;letter-spacing:0.05em;">Watchdog Heartbeat</div>
                        <div id="sched-watchdog" style="font-size:14px;font-weight:500;color:#f1f5f9;margin-top:4px;" data-testid="text-scheduler-watchdog">--</div>
                    </div>
                    <div style="padding:16px 20px;background:rgba(30,41,59,0.5);border:1px solid rgba(148,163,184,0.1);border-radius:10px;">
                        <div style="font-size:12px;color:#64748b;text-transform:uppercase;letter-spacing:0.05em;">Total Jobs Logged</div>
                        <div id="sched-total-logged" style="font-size:22px;font-weight:700;color:#f1f5f9;margin-top:4px;" data-testid="text-scheduler-total">--</div>
                    </div>
                </div>

                <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:12px;">
                    <div style="font-weight:600;color:#f1f5f9;font-size:15px;">Recent Job Logs</div>
                    <button class="btn" onclick="loadSchedulerData()" data-testid="button-refresh-scheduler" style="font-size:13px;padding:6px 14px;">Refresh</button>
                </div>
                <div style="overflow-x:auto;">
                    <table class="data-table" data-testid="table-scheduler-jobs">
                        <thead>
                            <tr>
                                <th>Strategy</th>
                                <th>Status</th>
                                <th>Started</th>
                                <th>Duration</th>
                                <th>Assets</th>
                                <th>Signals</th>
                                <th>Errors</th>
                            </tr>
                        </thead>
                        <tbody id="sched-job-rows">
                            <tr><td colspan="7" style="text-align:center;color:#64748b;">No job logs yet</td></tr>
                        </tbody>
                    </table>
                </div>

                </div>
            </div>
        </div>
        <div id="tab-system" class="tab-content {'hidden' if tab != 'system' else ''}">
            <div class="section">
                <h2>System Status</h2>
                <p style="color:#94a3b8;margin-bottom:20px;">Overview of all monitoring, alerting, and production hardening layers.</p>

                <div id="sys-loading" style="text-align:center;padding:40px;color:#94a3b8;">Loading system status...</div>
                <div id="sys-content" style="display:none;">

                <div id="sys-health-banner" style="padding:16px 20px;border-radius:10px;margin-bottom:24px;display:flex;align-items:center;gap:12px;" data-testid="banner-system-health">
                    <div id="sys-health-dot" style="width:14px;height:14px;border-radius:50%;flex-shrink:0;"></div>
                    <div>
                        <div id="sys-health-label" style="font-weight:700;font-size:18px;"></div>
                        <div id="sys-health-checks" style="font-size:13px;color:#94a3b8;margin-top:2px;"></div>
                    </div>
                    <button class="btn" onclick="loadSystemStatus()" data-testid="button-refresh-system" style="margin-left:auto;font-size:13px;padding:6px 14px;">Refresh</button>
                </div>

                <div style="font-weight:600;color:#f1f5f9;font-size:15px;margin-bottom:12px;">Production Hardening Features</div>
                <div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(280px,1fr));gap:12px;margin-bottom:24px;">

                    <div class="sys-card" style="padding:16px 20px;background:rgba(30,41,59,0.5);border:1px solid rgba(148,163,184,0.1);border-radius:10px;">
                        <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:8px;">
                            <div style="font-weight:600;color:#f1f5f9;font-size:14px;">Rate Limiting &amp; Security</div>
                            <span class="badge status-open" data-testid="status-rate-limiting">Active</span>
                        </div>
                        <div style="color:#94a3b8;font-size:13px;line-height:1.7;">
                            Multi-layer leaky bucket: <strong style="color:#f1f5f9;">20 req/2s</strong> burst &middot; <strong style="color:#f1f5f9;">60/min</strong> &middot; <strong style="color:#f1f5f9;">1000/hr</strong> per IP.<br>
                            Burst cooldown: <strong style="color:#f1f5f9;">5 min</strong>. Endpoint enumeration guard: <strong style="color:#f1f5f9;">5+ 404s/60s &rarr; 24h block</strong>.
                        </div>
                        <div id="sec-stats" style="margin-top:10px;font-size:12px;color:#64748b;"></div>
                    </div>

                    <div class="sys-card" style="padding:16px 20px;background:rgba(30,41,59,0.5);border:1px solid rgba(148,163,184,0.1);border-radius:10px;">
                        <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:8px;">
                            <div style="font-weight:600;color:#f1f5f9;font-size:14px;">Security Headers</div>
                            <span class="badge status-open" data-testid="status-security-headers">Active</span>
                        </div>
                        <div style="color:#94a3b8;font-size:13px;">Helmet middleware providing HSTS, X-Content-Type-Options, X-Frame-Options, Referrer-Policy, and CORP headers.</div>
                    </div>

                    <div class="sys-card" style="padding:16px 20px;background:rgba(30,41,59,0.5);border:1px solid rgba(148,163,184,0.1);border-radius:10px;">
                        <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:8px;">
                            <div style="font-weight:600;color:#f1f5f9;font-size:14px;">Error Handler</div>
                            <span class="badge status-open" data-testid="status-error-handler">Active</span>
                        </div>
                        <div style="color:#94a3b8;font-size:13px;">Global exception handler catches unhandled errors, logs full traceback, and returns structured JSON responses.</div>
                    </div>

                    <div class="sys-card" style="padding:16px 20px;background:rgba(30,41,59,0.5);border:1px solid rgba(148,163,184,0.1);border-radius:10px;">
                        <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:8px;">
                            <div style="font-weight:600;color:#f1f5f9;font-size:14px;">Database</div>
                            <span id="sys-db-badge" class="badge" data-testid="status-database">--</span>
                        </div>
                        <div style="color:#94a3b8;font-size:13px;">SQLite database connectivity verified via health check. Pool size: 5 connections.</div>
                    </div>

                    <div class="sys-card" style="padding:16px 20px;background:rgba(30,41,59,0.5);border:1px solid rgba(148,163,184,0.1);border-radius:10px;">
                        <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:8px;">
                            <div style="font-weight:600;color:#f1f5f9;font-size:14px;">Scheduler Watchdog</div>
                            <span id="sys-watchdog-badge" class="badge" data-testid="status-watchdog">--</span>
                        </div>
                        <div style="color:#94a3b8;font-size:13px;">Background thread monitors scheduler every <strong style="color:#f1f5f9;">300 seconds</strong>. Auto-restarts on failure.</div>
                        <div id="sys-watchdog-time" style="color:#64748b;font-size:12px;margin-top:4px;"></div>
                    </div>

                    <div class="sys-card" style="padding:16px 20px;background:rgba(30,41,59,0.5);border:1px solid rgba(148,163,184,0.1);border-radius:10px;">
                        <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:8px;">
                            <div style="font-weight:600;color:#f1f5f9;font-size:14px;">API Key</div>
                            <span id="sys-apikey-badge" class="badge" data-testid="status-api-key">--</span>
                        </div>
                        <div style="color:#94a3b8;font-size:13px;">FCSAPI v4 access key required for live market data. Managed in Settings tab.</div>
                    </div>

                    <div class="sys-card" style="padding:16px 20px;background:rgba(30,41,59,0.5);border:1px solid rgba(148,163,184,0.1);border-radius:10px;">
                        <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:8px;">
                            <div style="font-weight:600;color:#f1f5f9;font-size:14px;">Credit Kill Switch</div>
                            <span id="sys-killswitch-badge" class="badge" data-testid="status-kill-switch">--</span>
                        </div>
                        <div style="color:#94a3b8;font-size:13px;">Hard limit at <strong style="color:#f1f5f9;">495,000</strong> credits. Blocks outbound API calls when exceeded.</div>
                    </div>

                    <div class="sys-card" style="padding:16px 20px;background:rgba(30,41,59,0.5);border:1px solid rgba(148,163,184,0.1);border-radius:10px;">
                        <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:8px;">
                            <div style="font-weight:600;color:#f1f5f9;font-size:14px;">Webhook Notifications</div>
                            <span id="sys-webhook-badge" class="badge" data-testid="status-webhook">--</span>
                        </div>
                        <div style="color:#94a3b8;font-size:13px;">External alerting via Discord, Slack, or generic webhooks. Configured in Notifications tab.</div>
                    </div>

                    <div class="sys-card" style="padding:16px 20px;background:rgba(30,41,59,0.5);border:1px solid rgba(148,163,184,0.1);border-radius:10px;">
                        <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:8px;">
                            <div style="font-weight:600;color:#f1f5f9;font-size:14px;">Process Auto-Restart</div>
                            <span class="badge status-open" data-testid="status-auto-restart">Active</span>
                        </div>
                        <div style="color:#94a3b8;font-size:13px;">Node.js server automatically restarts the Python engine on crash. Graceful shutdown on SIGTERM.</div>
                    </div>

                    <div class="sys-card" style="padding:16px 20px;background:rgba(30,41,59,0.5);border:1px solid rgba(148,163,184,0.1);border-radius:10px;">
                        <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:8px;">
                            <div style="font-weight:600;color:#f1f5f9;font-size:14px;">Misfire Recovery</div>
                            <span class="badge status-open" data-testid="status-misfire">Active</span>
                        </div>
                        <div style="color:#94a3b8;font-size:13px;">All scheduled jobs have <strong style="color:#f1f5f9;">120s misfire_grace_time</strong>. Per-asset retry: 2 attempts with 5s delay.</div>
                    </div>

                    <div class="sys-card" style="padding:16px 20px;background:rgba(30,41,59,0.5);border:1px solid rgba(148,163,184,0.1);border-radius:10px;">
                        <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:8px;">
                            <div style="font-weight:600;color:#f1f5f9;font-size:14px;">WebSocket Stream</div>
                            <span class="badge status-open" data-testid="status-websocket">Active</span>
                        </div>
                        <div style="color:#94a3b8;font-size:13px;">Real-time signal push via <strong style="color:#f1f5f9;">/ws/signals</strong>. Broadcasts <code>signal:new</code> and <code>signal:closed</code> events. Auto-reconnect with 5s backoff.</div>
                    </div>

                </div>

                <div style="font-weight:600;color:#f1f5f9;font-size:15px;margin-bottom:12px;">Storage Monitor</div>
                <div style="padding:20px;background:rgba(30,41,59,0.5);border:1px solid rgba(148,163,184,0.1);border-radius:10px;margin-bottom:24px;" data-testid="widget-storage-monitor">
                    <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:12px;">
                        <div style="display:flex;align-items:center;gap:10px;">
                            <div id="storage-pct-label" style="font-size:22px;font-weight:700;color:#f1f5f9;" data-testid="text-storage-percent">--%</div>
                            <div id="storage-status-badge" class="badge" data-testid="badge-storage-status">--</div>
                        </div>
                        <div id="storage-breakdown" style="font-size:13px;color:#94a3b8;" data-testid="text-storage-breakdown">Loading...</div>
                    </div>
                    <div style="width:100%;height:10px;background:rgba(30,41,59,0.8);border-radius:5px;overflow:hidden;">
                        <div id="storage-bar" style="height:100%;width:0%;border-radius:5px;transition:width 0.6s ease,background 0.3s ease;" data-testid="bar-storage-usage"></div>
                    </div>
                    <div style="display:flex;justify-content:space-between;margin-top:8px;font-size:12px;color:#64748b;">
                        <span id="storage-used-label" data-testid="text-storage-used">-- MB used</span>
                        <span id="storage-max-label" data-testid="text-storage-max">-- MB max</span>
                    </div>
                </div>

                <div style="font-weight:600;color:#f1f5f9;font-size:15px;margin-bottom:12px;">Live Status</div>
                <div style="padding:16px 20px;background:rgba(30,41,59,0.5);border:1px solid rgba(148,163,184,0.1);border-radius:10px;">
                    <div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(200px,1fr));gap:12px;">
                        <div>
                            <div style="font-size:12px;color:#64748b;text-transform:uppercase;">Scheduler</div>
                            <div id="sys-sched-status" style="font-size:14px;font-weight:500;color:#f1f5f9;margin-top:2px;" data-testid="text-sys-scheduler">--</div>
                        </div>
                        <div>
                            <div style="font-size:12px;color:#64748b;text-transform:uppercase;">Jobs Registered</div>
                            <div id="sys-sched-jobs" style="font-size:14px;font-weight:500;color:#f1f5f9;margin-top:2px;" data-testid="text-sys-jobs">--</div>
                        </div>
                        <div>
                            <div style="font-size:12px;color:#64748b;text-transform:uppercase;">24h Success / Failures</div>
                            <div id="sys-24h-stats" style="font-size:14px;font-weight:500;color:#f1f5f9;margin-top:2px;" data-testid="text-sys-24h">--</div>
                        </div>
                        <div>
                            <div style="font-size:12px;color:#64748b;text-transform:uppercase;">Last Health Check</div>
                            <div id="sys-timestamp" style="font-size:14px;font-weight:500;color:#f1f5f9;margin-top:2px;" data-testid="text-sys-timestamp">--</div>
                        </div>
                        <div>
                            <div style="font-size:12px;color:#64748b;text-transform:uppercase;">WebSocket Clients</div>
                            <div id="sys-ws-clients" style="font-size:14px;font-weight:500;color:#f1f5f9;margin-top:2px;" data-testid="text-sys-ws-clients">--</div>
                        </div>
                    </div>
                </div>

                </div>
            </div>
        </div>
        <div id="tab-recovery" class="tab-content {'hidden' if tab != 'recovery' else ''}">
            <div class="section">
                <h2>Recovery Logs</h2>
                <p style="color:#94a3b8;margin-bottom:20px;">Audit trail of all missed-window recovery events. Shows when strategies were automatically backfilled on startup.</p>

                <div id="recovery-loading" style="text-align:center;padding:40px;color:#94a3b8;">Loading recovery logs...</div>
                <div id="recovery-content" style="display:none;">

                <div class="stats-grid" style="display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:16px;margin-bottom:24px;">
                    <div style="padding:16px 20px;background:rgba(30,41,59,0.5);border:1px solid rgba(148,163,184,0.1);border-radius:10px;">
                        <div style="font-size:12px;color:#64748b;text-transform:uppercase;letter-spacing:0.05em;">Total Events</div>
                        <div id="recovery-total" style="font-size:22px;font-weight:700;color:#f1f5f9;margin-top:4px;" data-testid="text-recovery-total">0</div>
                    </div>
                    <div style="padding:16px 20px;background:rgba(30,41,59,0.5);border:1px solid rgba(148,163,184,0.1);border-radius:10px;">
                        <div style="font-size:12px;color:#64748b;text-transform:uppercase;letter-spacing:0.05em;">Backfilled</div>
                        <div id="recovery-success" style="font-size:22px;font-weight:700;color:#22c55e;margin-top:4px;" data-testid="text-recovery-success">0</div>
                    </div>
                    <div style="padding:16px 20px;background:rgba(30,41,59,0.5);border:1px solid rgba(148,163,184,0.1);border-radius:10px;">
                        <div style="font-size:12px;color:#64748b;text-transform:uppercase;letter-spacing:0.05em;">Failed</div>
                        <div id="recovery-failed" style="font-size:22px;font-weight:700;color:#ef4444;margin-top:4px;" data-testid="text-recovery-failed">0</div>
                    </div>
                </div>

                <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:12px;">
                    <div style="font-weight:600;color:#f1f5f9;font-size:15px;">Recovery Event History</div>
                    <button class="btn" onclick="loadRecoveryLogs()" data-testid="button-refresh-recovery" style="font-size:13px;padding:6px 14px;">Refresh</button>
                </div>
                <div style="overflow-x:auto;">
                    <table class="data-table" data-testid="table-recovery-logs">
                        <thead>
                            <tr>
                                <th>Strategy</th>
                                <th>Status</th>
                                <th>Target Window</th>
                                <th>Actual Execution</th>
                                <th>Assets</th>
                            </tr>
                        </thead>
                        <tbody id="recovery-rows">
                            <tr><td colspan="5" style="text-align:center;color:#64748b;">No recovery events recorded yet</td></tr>
                        </tbody>
                    </table>
                </div>

                </div>
            </div>
        </div>
        <div id="tab-api_catalog" class="tab-content {'hidden' if tab != 'api_catalog' else ''}">
            <div class="section">
                <h2>API Catalog</h2>
                <p style="color:#94a3b8;margin-bottom:8px;">Complete reference of all Public API v1 endpoints. All endpoints are read-only (except cache flush) and serve data from the local SQLite database.</p>
                <p style="color:#64748b;font-size:13px;margin-bottom:24px;">Base URL: <code style="background:#1e293b;padding:2px 8px;border-radius:4px;color:#38bdf8;">/api/v1</code> &mdash; Cache: 4-shard TTLCache (60s default, 30s for scheduler endpoints)</p>

                <div style="font-weight:600;color:#f1f5f9;font-size:15px;margin-bottom:4px;">Signals</div>
                <p style="color:#64748b;font-size:12px;margin-bottom:12px;">Trading signal endpoints &mdash; active, historical, and filtered views</p>
                <div style="overflow-x:auto;margin-bottom:28px;">
                    <table class="data-table" data-testid="table-api-catalog-signals">
                        <thead>
                            <tr>
                                <th style="width:90px;">Method</th>
                                <th>Path</th>
                                <th>Description</th>
                                <th>Cache</th>
                                <th>Parameters</th>
                                <th style="width:70px;"></th>
                            </tr>
                        </thead>
                        <tbody>
                            <tr>
                                <td><span class="badge status-open">GET</span></td>
                                <td style="font-family:monospace;font-size:13px;color:#38bdf8;">/api/v1/signals/latest</td>
                                <td>Fetch active signals in public format (LONG/SHORT, enriched with position metadata)</td>
                                <td><span style="color:#22c55e;">60s</span></td>
                                <td style="font-size:12px;color:#94a3b8;">asset, strategy, asset_class</td>
                                <td><button class="btn-copy" onclick="copyApiUrl('/api/v1/signals/latest')" data-testid="copy-signals-latest">Copy</button></td>
                            </tr>
                            <tr>
                                <td><span class="badge status-open">GET</span></td>
                                <td style="font-family:monospace;font-size:13px;color:#38bdf8;">/api/v1/signals/history</td>
                                <td>Paginated signal history with full filtering (legacy format)</td>
                                <td><span style="color:#22c55e;">60s</span></td>
                                <td style="font-size:12px;color:#94a3b8;">asset, strategy, status, asset_class, page, size</td>
                                <td><button class="btn-copy" onclick="copyApiUrl('/api/v1/signals/history')" data-testid="copy-signals-history">Copy</button></td>
                            </tr>
                            <tr>
                                <td><span class="badge status-open">GET</span></td>
                                <td style="font-family:monospace;font-size:13px;color:#38bdf8;">/api/v1/signals/active</td>
                                <td>Currently open signals only (legacy format with BUY/SELL direction)</td>
                                <td><span style="color:#22c55e;">60s</span></td>
                                <td style="font-size:12px;color:#94a3b8;">strategy, asset, category</td>
                                <td><button class="btn-copy" onclick="copyApiUrl('/api/v1/signals/active')" data-testid="copy-signals-active">Copy</button></td>
                            </tr>
                            <tr>
                                <td><span class="badge status-open">GET</span></td>
                                <td style="font-family:monospace;font-size:13px;color:#38bdf8;">/api/v1/signals/&lbrace;id&rbrace;</td>
                                <td>Single signal by database ID (returns 404 if not found)</td>
                                <td><span style="color:#22c55e;">60s</span></td>
                                <td style="font-size:12px;color:#94a3b8;">signal_id (path)</td>
                                <td><button class="btn-copy" onclick="copyApiUrl('/api/v1/signals/1')" data-testid="copy-signal-detail">Copy</button></td>
                            </tr>
                            <tr>
                                <td><span class="badge status-open">GET</span></td>
                                <td style="font-family:monospace;font-size:13px;color:#38bdf8;">/api/v1/signals</td>
                                <td>All signals (OPEN + CLOSED) with optional filters, max 200 results</td>
                                <td><span style="color:#22c55e;">60s</span></td>
                                <td style="font-size:12px;color:#94a3b8;">strategy, asset, status, category, limit</td>
                                <td><button class="btn-copy" onclick="copyApiUrl('/api/v1/signals')" data-testid="copy-signals-all">Copy</button></td>
                            </tr>
                        </tbody>
                    </table>
                </div>

                <div style="font-weight:600;color:#f1f5f9;font-size:15px;margin-bottom:4px;">Market Data</div>
                <p style="color:#64748b;font-size:12px;margin-bottom:12px;">OHLC candles and computed technical indicators from local storage</p>
                <div style="overflow-x:auto;margin-bottom:28px;">
                    <table class="data-table" data-testid="table-api-catalog-market">
                        <thead>
                            <tr>
                                <th style="width:90px;">Method</th>
                                <th>Path</th>
                                <th>Description</th>
                                <th>Cache</th>
                                <th>Parameters</th>
                                <th style="width:70px;"></th>
                            </tr>
                        </thead>
                        <tbody>
                            <tr>
                                <td><span class="badge status-open">GET</span></td>
                                <td style="font-family:monospace;font-size:13px;color:#38bdf8;">/api/v1/market/candles</td>
                                <td>OHLC candle data for an asset/timeframe (pre-fetched by scheduler)</td>
                                <td><span style="color:#22c55e;">60s</span></td>
                                <td style="font-size:12px;color:#94a3b8;">asset (required), timeframe, limit</td>
                                <td><button class="btn-copy" onclick="copyApiUrl('/api/v1/market/candles?asset=EUR/USD&timeframe=D1')" data-testid="copy-candles">Copy</button></td>
                            </tr>
                            <tr>
                                <td><span class="badge status-open">GET</span></td>
                                <td style="font-family:monospace;font-size:13px;color:#38bdf8;">/api/v1/market/indicators</td>
                                <td>Technical indicators: SMA/EMA (20/50/100/200), RSI (14/20), ATR (14/100)</td>
                                <td><span style="color:#22c55e;">60s</span></td>
                                <td style="font-size:12px;color:#94a3b8;">asset (required), timeframe</td>
                                <td><button class="btn-copy" onclick="copyApiUrl('/api/v1/market/indicators?asset=EUR/USD&timeframe=D1')" data-testid="copy-indicators">Copy</button></td>
                            </tr>
                        </tbody>
                    </table>
                </div>

                <div style="font-weight:600;color:#f1f5f9;font-size:15px;margin-bottom:4px;">Strategies &amp; Positions</div>
                <p style="color:#64748b;font-size:12px;margin-bottom:12px;">Strategy summaries and open position tracking with trailing-stop data</p>
                <div style="overflow-x:auto;margin-bottom:28px;">
                    <table class="data-table" data-testid="table-api-catalog-strategies">
                        <thead>
                            <tr>
                                <th style="width:90px;">Method</th>
                                <th>Path</th>
                                <th>Description</th>
                                <th>Cache</th>
                                <th>Parameters</th>
                                <th style="width:70px;"></th>
                            </tr>
                        </thead>
                        <tbody>
                            <tr>
                                <td><span class="badge status-open">GET</span></td>
                                <td style="font-family:monospace;font-size:13px;color:#38bdf8;">/api/v1/strategies</td>
                                <td>List all strategies with open/closed signal counts</td>
                                <td><span style="color:#22c55e;">60s</span></td>
                                <td style="font-size:12px;color:#94a3b8;">&mdash;</td>
                                <td><button class="btn-copy" onclick="copyApiUrl('/api/v1/strategies')" data-testid="copy-strategies">Copy</button></td>
                            </tr>
                            <tr>
                                <td><span class="badge status-open">GET</span></td>
                                <td style="font-family:monospace;font-size:13px;color:#38bdf8;">/api/v1/positions</td>
                                <td>Open positions with ATR at entry, highest/lowest price since entry</td>
                                <td><span style="color:#22c55e;">60s</span></td>
                                <td style="font-size:12px;color:#94a3b8;">strategy, asset</td>
                                <td><button class="btn-copy" onclick="copyApiUrl('/api/v1/positions')" data-testid="copy-positions">Copy</button></td>
                            </tr>
                        </tbody>
                    </table>
                </div>

                <div style="font-weight:600;color:#f1f5f9;font-size:15px;margin-bottom:4px;">Performance Metrics</div>
                <p style="color:#64748b;font-size:12px;margin-bottom:12px;">Win rate, gain/loss averages &mdash; recomputed every 5 minutes by background worker</p>
                <div style="overflow-x:auto;margin-bottom:28px;">
                    <table class="data-table" data-testid="table-api-catalog-metrics">
                        <thead>
                            <tr>
                                <th style="width:90px;">Method</th>
                                <th>Path</th>
                                <th>Description</th>
                                <th>Cache</th>
                                <th>Parameters</th>
                                <th style="width:70px;"></th>
                            </tr>
                        </thead>
                        <tbody>
                            <tr>
                                <td><span class="badge status-open">GET</span></td>
                                <td style="font-family:monospace;font-size:13px;color:#38bdf8;">/api/v1/metrics</td>
                                <td>Signal performance metrics (per-asset + aggregate by default)</td>
                                <td><span style="color:#22c55e;">60s</span></td>
                                <td style="font-size:12px;color:#94a3b8;">strategy, asset, period, summary_only</td>
                                <td><button class="btn-copy" onclick="copyApiUrl('/api/v1/metrics')" data-testid="copy-metrics">Copy</button></td>
                            </tr>
                            <tr>
                                <td><span class="badge status-open">GET</span></td>
                                <td style="font-family:monospace;font-size:13px;color:#38bdf8;">/api/v1/metrics/summary</td>
                                <td>Overall platform win rate, total won/lost, per-strategy breakdown</td>
                                <td><span style="color:#22c55e;">60s</span></td>
                                <td style="font-size:12px;color:#94a3b8;">&mdash;</td>
                                <td><button class="btn-copy" onclick="copyApiUrl('/api/v1/metrics/summary')" data-testid="copy-metrics-summary">Copy</button></td>
                            </tr>
                        </tbody>
                    </table>
                </div>

                <div style="font-weight:600;color:#f1f5f9;font-size:15px;margin-bottom:4px;">Scheduler</div>
                <p style="color:#64748b;font-size:12px;margin-bottom:12px;">APScheduler monitoring &mdash; 30s cache for near-real-time data</p>
                <div style="overflow-x:auto;margin-bottom:28px;">
                    <table class="data-table" data-testid="table-api-catalog-scheduler">
                        <thead>
                            <tr>
                                <th style="width:90px;">Method</th>
                                <th>Path</th>
                                <th>Description</th>
                                <th>Cache</th>
                                <th>Parameters</th>
                                <th style="width:70px;"></th>
                            </tr>
                        </thead>
                        <tbody>
                            <tr>
                                <td><span class="badge status-open">GET</span></td>
                                <td style="font-family:monospace;font-size:13px;color:#38bdf8;">/api/v1/scheduler/status</td>
                                <td>24h success/failure counts and last job execution record</td>
                                <td><span style="color:#f59e0b;">30s</span></td>
                                <td style="font-size:12px;color:#94a3b8;">&mdash;</td>
                                <td><button class="btn-copy" onclick="copyApiUrl('/api/v1/scheduler/status')" data-testid="copy-sched-status">Copy</button></td>
                            </tr>
                            <tr>
                                <td><span class="badge status-open">GET</span></td>
                                <td style="font-family:monospace;font-size:13px;color:#38bdf8;">/api/v1/scheduler/jobs</td>
                                <td>Recent job logs with strategy, status, duration, and error details</td>
                                <td><span style="color:#f59e0b;">30s</span></td>
                                <td style="font-size:12px;color:#94a3b8;">limit</td>
                                <td><button class="btn-copy" onclick="copyApiUrl('/api/v1/scheduler/jobs')" data-testid="copy-sched-jobs">Copy</button></td>
                            </tr>
                        </tbody>
                    </table>
                </div>

                <div style="font-weight:600;color:#f1f5f9;font-size:15px;margin-bottom:4px;">Health &amp; State Management</div>
                <p style="color:#64748b;font-size:12px;margin-bottom:12px;">Liveness checks and cache control</p>
                <div style="overflow-x:auto;margin-bottom:28px;">
                    <table class="data-table" data-testid="table-api-catalog-health">
                        <thead>
                            <tr>
                                <th style="width:90px;">Method</th>
                                <th>Path</th>
                                <th>Description</th>
                                <th>Cache</th>
                                <th>Parameters</th>
                                <th style="width:70px;"></th>
                            </tr>
                        </thead>
                        <tbody>
                            <tr>
                                <td><span class="badge status-open">GET</span></td>
                                <td style="font-family:monospace;font-size:13px;color:#38bdf8;">/api/v1/health</td>
                                <td>API health with cache pool stats (shard count, hit rate, TTL)</td>
                                <td><span style="color:#94a3b8;">None</span></td>
                                <td style="font-size:12px;color:#94a3b8;">&mdash;</td>
                                <td><button class="btn-copy" onclick="copyApiUrl('/api/v1/health')" data-testid="copy-health">Copy</button></td>
                            </tr>
                            <tr>
                                <td><span class="badge status-open">GET</span></td>
                                <td style="font-family:monospace;font-size:13px;color:#38bdf8;">/api/v1/health/public</td>
                                <td>Public liveness check &mdash; returns only UP/DOWN, no internal data</td>
                                <td><span style="color:#94a3b8;">None</span></td>
                                <td style="font-size:12px;color:#94a3b8;">&mdash;</td>
                                <td><button class="btn-copy" onclick="copyApiUrl('/api/v1/health/public')" data-testid="copy-health-public">Copy</button></td>
                            </tr>
                            <tr>
                                <td><span class="badge" style="background:rgba(59,130,246,0.15);color:#60a5fa;border-color:#3b82f6;">POST</span></td>
                                <td style="font-family:monospace;font-size:13px;color:#38bdf8;">/api/v1/cache/flush</td>
                                <td>Flush all 4 TTLCache shards &mdash; forces fresh data on next request</td>
                                <td><span style="color:#94a3b8;">N/A</span></td>
                                <td style="font-size:12px;color:#94a3b8;">&mdash;</td>
                                <td><button class="btn-copy" onclick="copyApiUrl('/api/v1/cache/flush')" data-testid="copy-cache-flush">Copy</button></td>
                            </tr>
                        </tbody>
                    </table>
                </div>

                <div style="padding:16px 20px;background:rgba(30,41,59,0.5);border:1px solid rgba(148,163,184,0.1);border-radius:10px;">
                    <div style="font-weight:600;color:#f1f5f9;font-size:14px;margin-bottom:8px;">Quick Reference</div>
                    <div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(240px,1fr));gap:12px;font-size:13px;color:#94a3b8;">
                        <div><strong style="color:#e2e8f0;">Timeframes:</strong> 30m, 1H, 4H, D1</div>
                        <div><strong style="color:#e2e8f0;">Strategies:</strong> mtf_ema, trend_forex, trend_non_forex, sp500_momentum, highest_lowest_fx</div>
                        <div><strong style="color:#e2e8f0;">Asset Classes:</strong> forex, crypto, commodities, indices</div>
                        <div><strong style="color:#e2e8f0;">Signal Status:</strong> OPEN, CLOSED</div>
                        <div><strong style="color:#e2e8f0;">Metric Periods:</strong> all_time, 7d, 30d</div>
                        <div><strong style="color:#e2e8f0;">Swagger Docs:</strong> <a href="/docs" target="_blank" style="color:#38bdf8;text-decoration:underline;" data-testid="link-swagger-docs">/docs</a></div>
                    </div>
                </div>

                <div id="copy-toast" style="display:none;position:fixed;bottom:24px;right:24px;background:#22c55e;color:#fff;padding:10px 20px;border-radius:8px;font-size:14px;font-weight:500;box-shadow:0 4px 12px rgba(0,0,0,0.3);z-index:9999;" data-testid="toast-copy-success">URL copied to clipboard</div>
            </div>
        </div>

        <div id="tab-wordpress" class="tab-content {'hidden' if tab != 'wordpress' else ''}">
            <div class="section">
                <h2>WordPress Credentials</h2>
                <p style="color:#94a3b8;margin-bottom:16px;">{'Manage all WordPress site connections for signal publishing.' if is_admin else 'Manage your WordPress site connections for signal publishing.'} Passwords are encrypted at rest.</p>

                <div style="display:flex;gap:12px;margin-bottom:24px;">
                    <button class="btn btn-primary" onclick="document.getElementById('ucms-add-form').style.display='block'" data-testid="btn-ucms-add">Add WordPress Site</button>
                </div>

                <div id="ucms-add-form" style="display:none;background:#1e293b;border:1px solid #334155;border-radius:8px;padding:20px;margin-bottom:24px;">
                    <h3 style="margin-bottom:16px;color:#f1f5f9;">New WordPress Connection</h3>
                    <div style="display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-bottom:16px;">
                        <div>
                            <label style="display:block;font-size:13px;color:#94a3b8;margin-bottom:4px;">Site URL</label>
                            <input type="text" id="ucms-url" placeholder="https://yourdomain.com" style="width:100%;padding:8px 12px;background:#0f172a;border:1px solid #334155;border-radius:6px;color:#f1f5f9;" data-testid="input-ucms-url">
                        </div>
                        <div>
                            <label style="display:block;font-size:13px;color:#94a3b8;margin-bottom:4px;">WP Username</label>
                            <input type="text" id="ucms-username" placeholder="admin" style="width:100%;padding:8px 12px;background:#0f172a;border:1px solid #334155;border-radius:6px;color:#f1f5f9;" data-testid="input-ucms-username">
                        </div>
                        <div>
                            <label style="display:block;font-size:13px;color:#94a3b8;margin-bottom:4px;">Application Password</label>
                            <input type="password" id="ucms-password" placeholder="xxxx xxxx xxxx xxxx" style="width:100%;padding:8px 12px;background:#0f172a;border:1px solid #334155;border-radius:6px;color:#f1f5f9;" data-testid="input-ucms-password">
                        </div>
                        {'<div><label style="display:block;font-size:13px;color:#94a3b8;margin-bottom:4px;">Assign to User ID</label><input type="number" id="ucms-user-id" placeholder="Leave blank for yourself" style="width:100%;padding:8px 12px;background:#0f172a;border:1px solid #334155;border-radius:6px;color:#f1f5f9;" data-testid="input-ucms-user-id"></div>' if is_admin else '<input type="hidden" id="ucms-user-id" value="">'}
                    </div>
                    <div style="display:flex;gap:8px;">
                        <button class="btn btn-primary" onclick="ucmsCreate()" data-testid="btn-ucms-save">Save</button>
                        <button class="btn" onclick="document.getElementById('ucms-add-form').style.display='none'" data-testid="btn-ucms-cancel">Cancel</button>
                    </div>
                </div>

                <div id="ucms-status-msg" style="display:none;padding:10px 16px;border-radius:6px;margin-bottom:16px;font-size:14px;" data-testid="text-ucms-status"></div>

                <table class="signal-table" data-testid="table-wp-credentials">
                    <thead>
                        <tr>
                            <th>ID</th>
                            {'<th>Owner</th>' if is_admin else ''}
                            <th>Site URL</th>
                            <th>WP Username</th>
                            <th>Status</th>
                            <th>Created</th>
                            <th>Actions</th>
                        </tr>
                    </thead>
                    <tbody id="ucms-body">
                        <tr><td colspan="{'7' if is_admin else '6'}" style="text-align:center;color:#64748b;">Loading...</td></tr>
                    </tbody>
                </table>
            </div>

            <div class="section" style="margin-top:32px;" data-testid="section-wp-setup-guide">
                <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:16px;">
                    <h2 style="margin:0;">Setup Guide</h2>
                    <button class="btn" onclick="var el=document.getElementById('wp-guide-body');el.style.display=el.style.display==='none'?'block':'none';this.textContent=el.style.display==='none'?'Show Guide':'Hide Guide'" data-testid="btn-toggle-wp-guide" style="font-size:13px;padding:6px 14px;">Hide Guide</button>
                </div>
                <div id="wp-guide-body">
                    <p style="color:#94a3b8;margin-bottom:20px;">Follow these steps to connect your WordPress site for automated signal publishing via the REST API.</p>

                    <div style="display:flex;flex-direction:column;gap:20px;">

                        <div style="background:rgba(30,41,59,0.5);border:1px solid rgba(148,163,184,0.1);border-radius:10px;padding:20px;">
                            <div style="display:flex;align-items:center;gap:12px;margin-bottom:12px;">
                                <div style="width:32px;height:32px;border-radius:50%;background:rgba(59,130,246,0.15);color:#3b82f6;display:flex;align-items:center;justify-content:center;font-weight:700;font-size:14px;flex-shrink:0;">1</div>
                                <h3 style="margin:0;color:#f1f5f9;font-size:15px;">Requirements</h3>
                            </div>
                            <ul style="color:#94a3b8;font-size:13px;line-height:1.8;margin:0;padding-left:20px;">
                                <li>WordPress 5.6 or newer (Application Passwords are built-in)</li>
                                <li>A WordPress user account with <strong style="color:#f1f5f9;">Editor</strong> or <strong style="color:#f1f5f9;">Administrator</strong> role</li>
                                <li>HTTPS enabled on your WordPress site</li>
                                <li>WordPress REST API must be accessible (not blocked by a security plugin)</li>
                            </ul>
                        </div>

                        <div style="background:rgba(30,41,59,0.5);border:1px solid rgba(148,163,184,0.1);border-radius:10px;padding:20px;">
                            <div style="display:flex;align-items:center;gap:12px;margin-bottom:12px;">
                                <div style="width:32px;height:32px;border-radius:50%;background:rgba(59,130,246,0.15);color:#3b82f6;display:flex;align-items:center;justify-content:center;font-weight:700;font-size:14px;flex-shrink:0;">2</div>
                                <h3 style="margin:0;color:#f1f5f9;font-size:15px;">Generate an Application Password</h3>
                            </div>
                            <ol style="color:#94a3b8;font-size:13px;line-height:2;margin:0;padding-left:20px;">
                                <li>Log in to your WordPress admin dashboard</li>
                                <li>Go to <strong style="color:#f1f5f9;">Users &rarr; Profile</strong> (or edit the user account you want to use)</li>
                                <li>Scroll down to the <strong style="color:#f1f5f9;">Application Passwords</strong> section</li>
                                <li>Enter a name like <span style="color:#3b82f6;font-family:monospace;">DailyForex Signals</span> and click <strong style="color:#f1f5f9;">Add New Application Password</strong></li>
                                <li>Copy the generated password immediately &mdash; it will not be shown again</li>
                            </ol>
                            <div style="margin-top:12px;padding:10px 14px;background:rgba(245,158,11,0.08);border:1px solid rgba(245,158,11,0.2);border-radius:6px;font-size:12px;color:#fbbf24;">
                                <strong>Note:</strong> The password will look like <span style="font-family:monospace;">xxxx xxxx xxxx xxxx xxxx xxxx</span> with spaces. Enter it exactly as shown, including the spaces.
                            </div>
                        </div>

                        <div style="background:rgba(30,41,59,0.5);border:1px solid rgba(148,163,184,0.1);border-radius:10px;padding:20px;">
                            <div style="display:flex;align-items:center;gap:12px;margin-bottom:12px;">
                                <div style="width:32px;height:32px;border-radius:50%;background:rgba(59,130,246,0.15);color:#3b82f6;display:flex;align-items:center;justify-content:center;font-weight:700;font-size:14px;flex-shrink:0;">3</div>
                                <h3 style="margin:0;color:#f1f5f9;font-size:15px;">Add Your Site Here</h3>
                            </div>
                            <ol style="color:#94a3b8;font-size:13px;line-height:2;margin:0;padding-left:20px;">
                                <li>Click <strong style="color:#f1f5f9;">Add WordPress Site</strong> above</li>
                                <li><strong style="color:#f1f5f9;">Site URL</strong> &mdash; Your WordPress site address (e.g. <span style="font-family:monospace;color:#3b82f6;">https://yourdomain.com</span>). No trailing slash needed.</li>
                                <li><strong style="color:#f1f5f9;">WP Username</strong> &mdash; The WordPress username or email of the account that generated the Application Password</li>
                                <li><strong style="color:#f1f5f9;">Application Password</strong> &mdash; Paste the password from Step 2</li>
                                <li>Click <strong style="color:#f1f5f9;">Save</strong>, then click <strong style="color:#f1f5f9;">Test</strong> to verify the connection</li>
                            </ol>
                        </div>

                        <div style="background:rgba(30,41,59,0.5);border:1px solid rgba(148,163,184,0.1);border-radius:10px;padding:20px;">
                            <div style="display:flex;align-items:center;gap:12px;margin-bottom:12px;">
                                <div style="width:32px;height:32px;border-radius:50%;background:rgba(34,197,94,0.15);color:#22c55e;display:flex;align-items:center;justify-content:center;font-weight:700;font-size:14px;flex-shrink:0;">4</div>
                                <h3 style="margin:0;color:#f1f5f9;font-size:15px;">How It Works</h3>
                            </div>
                            <ul style="color:#94a3b8;font-size:13px;line-height:1.8;margin:0;padding-left:20px;">
                                <li>When a new trading signal is generated, it is <strong style="color:#f1f5f9;">automatically published</strong> as a WordPress post to all active sites</li>
                                <li>When a signal is closed, the WordPress post is <strong style="color:#f1f5f9;">automatically updated</strong> with exit price, reason, and outcome</li>
                                <li>Each site gets its own independent post &mdash; multiple sites are fully supported</li>
                                <li>Publishing runs in a background thread and does not block signal generation</li>
                                <li>Failed publishes are retried up to <strong style="color:#f1f5f9;">3 times</strong> with exponential backoff (2s &rarr; 30s)</li>
                                <li>Admins can manually retry publishing or update posts from the Signals tab</li>
                            </ul>
                        </div>

                        <div style="background:rgba(30,41,59,0.5);border:1px solid rgba(148,163,184,0.1);border-radius:10px;padding:20px;">
                            <div style="display:flex;align-items:center;gap:12px;margin-bottom:12px;">
                                <div style="width:32px;height:32px;border-radius:50%;background:rgba(239,68,68,0.15);color:#ef4444;display:flex;align-items:center;justify-content:center;flex-shrink:0;">
                                    <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><line x1="12" x2="12" y1="8" y2="12"/><line x1="12" x2="12.01" y1="16" y2="16"/></svg>
                                </div>
                                <h3 style="margin:0;color:#f1f5f9;font-size:15px;">Troubleshooting</h3>
                            </div>
                            <div style="display:flex;flex-direction:column;gap:12px;">
                                <div>
                                    <div style="color:#f1f5f9;font-size:13px;font-weight:600;margin-bottom:4px;">HTTP 403 &mdash; Forbidden</div>
                                    <div style="color:#94a3b8;font-size:12px;">The WordPress user may lack permissions. Ensure the account has <strong style="color:#f1f5f9;">Editor</strong> or <strong style="color:#f1f5f9;">Administrator</strong> role. Some hosting providers (e.g. WP Engine) or security plugins may block REST API access &mdash; check your host's settings.</div>
                                </div>
                                <div>
                                    <div style="color:#f1f5f9;font-size:13px;font-weight:600;margin-bottom:4px;">HTTP 401 &mdash; Unauthorized</div>
                                    <div style="color:#94a3b8;font-size:12px;">The Application Password is incorrect or has been revoked. Generate a new one from WordPress and update it here.</div>
                                </div>
                                <div>
                                    <div style="color:#f1f5f9;font-size:13px;font-weight:600;margin-bottom:4px;">HTTP 404 &mdash; Not Found</div>
                                    <div style="color:#94a3b8;font-size:12px;">The REST API endpoint could not be found. Ensure your WordPress site has the REST API enabled (it is enabled by default). Check that your Site URL is correct and does not include <span style="font-family:monospace;">/wp-admin</span> or other paths.</div>
                                </div>
                                <div>
                                    <div style="color:#f1f5f9;font-size:13px;font-weight:600;margin-bottom:4px;">Connection Timeout</div>
                                    <div style="color:#94a3b8;font-size:12px;">The WordPress site may be down or unreachable. Verify the site loads normally in a browser. Check that the URL uses <span style="font-family:monospace;">https://</span> and not <span style="font-family:monospace;">http://</span>.</div>
                                </div>
                                <div>
                                    <div style="color:#f1f5f9;font-size:13px;font-weight:600;margin-bottom:4px;">Application Passwords Section Missing</div>
                                    <div style="color:#94a3b8;font-size:12px;">Application Passwords require WordPress 5.6+. If you don't see the section under your profile, your site may be running an older version, or a plugin may have disabled the feature. Update WordPress or check your security plugin settings.</div>
                                </div>
                            </div>
                        </div>

                        <div style="padding:12px 16px;background:rgba(59,130,246,0.08);border:1px solid rgba(59,130,246,0.2);border-radius:8px;font-size:12px;color:#93c5fd;">
                            <strong>Security:</strong> All Application Passwords are encrypted at rest using Fernet symmetric encryption before being stored in the database. They are only decrypted in memory when making API calls to your WordPress site.
                        </div>

                    </div>
                </div>
            </div>
        </div>

        </main>
    </div>
    <script>var IS_ADMIN = {'true' if is_admin else 'false'};</script>
    <script>{ADMIN_JS}</script>
</body>
</html>"""
    return HTMLResponse(content=html)


def _auth_guard(request: Request):
    user = _get_session_user(request)
    if not user:
        return JSONResponse(content={"error": "Unauthorized"}, status_code=401)
    return None


def _admin_role_guard(request: Request):
    user = _get_session_user(request)
    if not user:
        return JSONResponse(content={"error": "Unauthorized"}, status_code=401)
    if user.get("role") != "ADMIN":
        return JSONResponse(content={"error": "Forbidden: Admin access required"}, status_code=403)
    return None


@router.get("/export")
def export_signals(
    request: Request,
    format: str = Query("csv", description="Export format: csv or json"),
    strategy: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    symbol: Optional[str] = Query(None),
):
    guard = _admin_role_guard(request)
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
    guard = _admin_role_guard(request)
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
    guard = _admin_role_guard(request)
    if guard:
        return guard
    api_key = body.get("api_key", "").strip()
    if not api_key:
        return JSONResponse(content={"success": False, "error": "API key cannot be empty"})
    set_setting("fcsapi_key", api_key)
    return JSONResponse(content={"success": True, "message": "API key saved successfully"})


@router.post("/api/settings/test-connection")
def test_api_connection(request: Request):
    guard = _admin_role_guard(request)
    if guard:
        return guard
    from trading_engine.fcsapi_client import FCSAPIClient
    client = FCSAPIClient()
    result = client.test_connection()
    return JSONResponse(content=result)


@router.get("/api/settings")
def get_settings(request: Request):
    guard = _admin_role_guard(request)
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


@router.get("/api/settings/registration")
def api_get_registration_setting(request: Request):
    guard = _admin_role_guard(request)
    if guard:
        return guard
    val = get_setting("registration_enabled")
    return JSONResponse(content={"enabled": val != "false"})


@router.put("/api/settings/registration")
def api_set_registration_setting(request: Request, body: dict = Body(...)):
    guard = _admin_role_guard(request)
    if guard:
        return guard
    enabled = body.get("enabled", True)
    set_setting("registration_enabled", "true" if enabled else "false")
    logger.info(f"[ADMIN] Registration {'enabled' if enabled else 'disabled'}")
    return JSONResponse(content={"success": True, "enabled": enabled})


@router.post("/api/users")
def api_create_admin(request: Request, body: dict = Body(...)):
    guard = _admin_role_guard(request)
    if guard:
        return guard
    username = body.get("username", "").strip()
    password = body.get("password", "")
    role = body.get("role", "CUSTOMER").strip().upper()
    if role not in ("ADMIN", "CUSTOMER"):
        role = "CUSTOMER"
    if not username or not password:
        return JSONResponse(content={"success": False, "error": "Username and password are required."})
    if len(password) < 4:
        return JSONResponse(content={"success": False, "error": "Password must be at least 4 characters."})
    admin_id = create_admin(username, password, role=role)
    if admin_id is None:
        return JSONResponse(content={"success": False, "error": f'Username "{username}" already exists.'})
    return JSONResponse(content={"success": True, "id": admin_id})


@router.put("/api/users/{admin_id}")
def api_update_admin(request: Request, admin_id: int, body: dict = Body(...)):
    guard = _admin_role_guard(request)
    if guard:
        return guard
    username = body.get("username", "").strip()
    password = body.get("password", "")
    role = body.get("role", "").strip().upper() or None
    if role and role not in ("ADMIN", "CUSTOMER"):
        role = None
    if not username:
        return JSONResponse(content={"success": False, "error": "Username cannot be empty."})
    if password and len(password) < 4:
        return JSONResponse(content={"success": False, "error": "Password must be at least 4 characters."})
    existing = get_admin_by_id(admin_id)
    if not existing:
        return JSONResponse(content={"success": False, "error": "User not found."})
    if role == "CUSTOMER" and existing.get("role") == "ADMIN":
        all_users = get_all_admins()
        admin_count = sum(1 for u in all_users if u.get("role") == "ADMIN")
        if admin_count <= 1:
            return JSONResponse(content={"success": False, "error": "Cannot demote the last admin user."})
    success = update_admin(admin_id, username=username, password=password if password else None, role=role)
    if not success:
        return JSONResponse(content={"success": False, "error": f'Username "{username}" already exists.'})
    return JSONResponse(content={"success": True})


@router.delete("/api/users/{admin_id}")
def api_delete_admin(request: Request, admin_id: int):
    guard = _admin_role_guard(request)
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
    guard = _admin_role_guard(request)
    if guard:
        return guard
    data = _get_spx_momentum_data()
    return JSONResponse(content=data)


@router.get("/api/users")
def api_list_admins(request: Request):
    guard = _admin_role_guard(request)
    if guard:
        return guard
    admins = get_all_admins()
    return JSONResponse(content={"admins": admins})


@router.get("/api/security/stats")
def api_security_stats(request: Request):
    guard = _admin_role_guard(request)
    if guard:
        return guard
    from trading_engine.security_middleware import get_security_stats
    return JSONResponse(content=get_security_stats())


@router.post("/api/security/unblock")
def api_security_unblock(request: Request, body: dict = Body(...)):
    guard = _admin_role_guard(request)
    if guard:
        return guard
    ip = body.get("ip", "").strip()
    if not ip:
        return JSONResponse(content={"success": False, "error": "IP address required"})
    from trading_engine.security_middleware import unblock_ip
    result = unblock_ip(ip)
    return JSONResponse(content={"success": result, "ip": ip})


@router.get("/api/partner-keys")
def api_list_partner_keys(request: Request):
    guard = _admin_role_guard(request)
    if guard:
        return guard
    keys = list_partner_api_keys()
    from trading_engine.security_middleware import REQUIRE_API_KEY
    return JSONResponse(content={"keys": keys, "require_api_key": REQUIRE_API_KEY})


@router.post("/api/partner-keys")
def api_create_partner_key(request: Request, body: dict = Body(...)):
    guard = _admin_role_guard(request)
    if guard:
        return guard
    label = body.get("label", "").strip()
    if not label:
        return JSONResponse(content={"error": "Label is required"}, status_code=400)
    tier = body.get("tier", "standard").strip()
    if tier not in ("standard", "premium", "unlimited"):
        return JSONResponse(content={"error": "Invalid tier"}, status_code=400)
    rate_limit = int(body.get("rate_limit_per_minute", 120))
    user = _get_session_user(request)
    result = create_partner_api_key(label=label, tier=tier, rate_limit=rate_limit, created_by=user["user_id"] if user else None)
    if not result:
        return JSONResponse(content={"error": "Failed to create API key"}, status_code=500)
    return JSONResponse(content=result)


@router.post("/api/partner-keys/{key_id}/toggle")
def api_toggle_partner_key(request: Request, key_id: int, body: dict = Body(...)):
    guard = _admin_role_guard(request)
    if guard:
        return guard
    active = body.get("active", True)
    ok = toggle_partner_api_key(key_id, active)
    return JSONResponse(content={"success": ok})


@router.delete("/api/partner-keys/{key_id}")
def api_delete_partner_key(request: Request, key_id: int):
    guard = _admin_role_guard(request)
    if guard:
        return guard
    ok = delete_partner_api_key(key_id)
    return JSONResponse(content={"success": ok})


@router.get("/api/storage-stats")
def api_storage_stats(request: Request):
    guard = _admin_role_guard(request)
    if guard:
        return guard
    from trading_engine.utils.system_monitor import get_storage_stats
    stats = get_storage_stats()
    return JSONResponse(content=stats)


@router.post("/api/storage/purge")
def api_storage_purge(request: Request, body: dict = Body(...)):
    guard = _admin_role_guard(request)
    if guard:
        return guard
    days = body.get("days_threshold")
    if days is None:
        return JSONResponse(content={"error": "days_threshold is required"}, status_code=400)
    try:
        days = int(days)
    except (TypeError, ValueError):
        return JSONResponse(content={"error": "days_threshold must be an integer"}, status_code=400)
    if days < 1:
        return JSONResponse(content={"error": "days_threshold must be at least 1"}, status_code=400)
    allowed = (90, 180, 365)
    if days not in allowed:
        return JSONResponse(
            content={"error": f"days_threshold must be one of {allowed}"},
            status_code=400,
        )
    from trading_engine.utils.storage_manager import purge_signals
    try:
        result = purge_signals(days)
        return JSONResponse(content=result)
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)


@router.get("/api/scheduler/health")
def api_scheduler_health(request: Request):
    guard = _admin_role_guard(request)
    if guard:
        return guard
    summary = get_scheduler_health_summary()
    return JSONResponse(content=summary)


@router.get("/api/scheduler/jobs")
def api_scheduler_job_logs(request: Request, limit: int = Query(50, ge=1, le=200)):
    guard = _admin_role_guard(request)
    if guard:
        return guard
    logs = get_recent_job_logs(limit)
    return JSONResponse(content={"logs": logs, "count": len(logs)})


@router.get("/api/quota-status")
def api_quota_status(request: Request):
    guard = _admin_role_guard(request)
    if guard:
        return guard
    from trading_engine.utils.quota_manager import check_budget_health, get_quota_status
    from trading_engine.credit_control import get_monthly_projection
    health = check_budget_health()
    raw = get_quota_status()
    projection = get_monthly_projection()
    health["last_updated"] = raw.get("last_updated")
    health["daily_avg_burn"] = projection.get("daily_rate", 0)
    remaining = health.get("remaining_credits", 0)
    daily_rate = projection.get("daily_rate", 0)
    health["est_days_remaining"] = round(remaining / daily_rate, 1) if daily_rate > 0 else 999
    return JSONResponse(content=health)


@router.get("/api/market-pulse")
def api_market_pulse(request: Request):
    user = _get_session_user(request)
    if not user:
        return JSONResponse(status_code=401, content={"detail": "Not authenticated"})
    from trading_engine.strategies.multi_timeframe import ALL_ASSETS
    from trading_engine.database import get_active_signals, get_recovery_notifications

    active_signals = get_active_signals()
    active_map = {}
    for sig in active_signals:
        sym = sig.get("asset", "")
        if sym not in active_map:
            active_map[sym] = sig

    recent_alerts = get_recovery_notifications(limit=100)
    alert_map = {}
    for alert in recent_alerts:
        if alert.get("strategy_name") in ("PROXIMITY_ALERT", "EARLY_WARNING"):
            assets_str = alert.get("assets_affected", "")
            if isinstance(assets_str, list):
                for a in assets_str:
                    alert_map.setdefault(a, alert)
            elif isinstance(assets_str, str):
                alert_map.setdefault(assets_str, alert)

    result = []
    for symbol in ALL_ASSETS:
        if symbol in active_map:
            sig = active_map[symbol]
            detail = f"Active {sig.get('direction', '')} signal via {sig.get('strategy_name', '')}"
            result.append({"symbol": symbol, "status": "triggered", "detail": detail})
        elif symbol in alert_map:
            alert = alert_map[symbol]
            detail = alert.get("status", "Approaching entry level")
            result.append({"symbol": symbol, "status": "approaching", "detail": detail})
        else:
            result.append({"symbol": symbol, "status": "neutral", "detail": "Monitoring"})

    return JSONResponse(content={"assets": result})


@router.get("/api/recovery-logs")
def api_recovery_logs(request: Request, limit: int = Query(50, ge=1, le=200)):
    guard = _admin_role_guard(request)
    if guard:
        return guard
    from trading_engine.database import get_recovery_notifications
    logs = get_recovery_notifications(limit)
    return JSONResponse(content={"logs": logs, "count": len(logs)})


@router.delete("/api/recovery-logs/{log_id}")
def api_delete_recovery_log(request: Request, log_id: int):
    guard = _admin_role_guard(request)
    if guard:
        return guard
    from trading_engine.database import SessionFactory
    from trading_engine.models import RecoveryNotification
    session = SessionFactory()
    try:
        record = session.query(RecoveryNotification).filter_by(id=log_id).first()
        if not record:
            return JSONResponse(status_code=404, content={"detail": "Not found"})
        session.delete(record)
        session.commit()
        return JSONResponse(content={"success": True})
    except Exception as e:
        session.rollback()
        return JSONResponse(status_code=500, content={"detail": str(e)})
    finally:
        session.close()


@router.get("/api/notifications")
def api_get_notification_config(request: Request):
    guard = _admin_role_guard(request)
    if guard:
        return guard
    from trading_engine.notifications import get_full_config, NOTIFICATION_CATEGORIES
    config = get_full_config()
    config["category_descriptions"] = NOTIFICATION_CATEGORIES
    return JSONResponse(content=config)


@router.post("/api/notifications")
def api_update_notification_config(request: Request, body: dict = Body(...)):
    guard = _admin_role_guard(request)
    if guard:
        return guard
    from trading_engine.notifications import (
        configure_webhook, set_notifications_enabled,
        set_category_enabled, get_full_config,
    )
    from trading_engine.database import set_setting
    import json as _json

    if "enabled" in body:
        enabled = bool(body["enabled"])
        set_notifications_enabled(enabled)
        set_setting("notifications_enabled", "true" if enabled else "false")

    if "webhook_url" in body:
        url = (body["webhook_url"] or "").strip()
        if url:
            configure_webhook(url)
            set_setting("webhook_url", url)
        else:
            configure_webhook(None)
            set_setting("webhook_url", "")

    if "categories" in body and isinstance(body["categories"], dict):
        from trading_engine.notifications import get_category_settings
        from trading_engine.database import get_setting as _get_setting
        existing_raw = _get_setting("notification_categories")
        try:
            existing_cats = _json.loads(existing_raw) if existing_raw else {}
        except Exception:
            existing_cats = {}
        merged = {**get_category_settings(), **existing_cats, **body["categories"]}
        for cat_key, cat_val in body["categories"].items():
            set_category_enabled(cat_key, bool(cat_val))
        set_setting("notification_categories", _json.dumps(merged))

    return JSONResponse(content={"status": "ok", "config": get_full_config()})


@router.get("/api/webhook")
def api_get_webhook(request: Request):
    guard = _admin_role_guard(request)
    if guard:
        return guard
    from trading_engine.notifications import get_webhook_url
    url = get_webhook_url()
    return JSONResponse(content={
        "configured": bool(url),
        "url": (url[:20] + "..." + url[-10:]) if url and len(url) > 30 else url,
    })


@router.post("/api/webhook")
def api_set_webhook(request: Request, body: dict = Body(...)):
    guard = _admin_role_guard(request)
    if guard:
        return guard
    from trading_engine.notifications import configure_webhook
    from trading_engine.database import set_setting
    url = body.get("url", "").strip()
    if url:
        configure_webhook(url)
        set_setting("webhook_url", url)
        return JSONResponse(content={"status": "ok", "message": "Webhook configured"})
    else:
        configure_webhook(None)
        set_setting("webhook_url", "")
        return JSONResponse(content={"status": "ok", "message": "Webhook cleared"})


@router.post("/api/webhook/test")
def api_test_webhook(request: Request):
    guard = _admin_role_guard(request)
    if guard:
        return guard
    from trading_engine.notifications import send_alert, get_webhook_url
    if not get_webhook_url():
        return JSONResponse(content={"status": "error", "message": "No webhook configured"}, status_code=400)
    send_alert(
        "Webhook Test",
        "This is a test notification from the AI Signals Trading Engine. If you see this, webhook delivery is working.",
        level="info",
        fields={"Test": "Successful"},
    )
    return JSONResponse(content={"status": "ok", "message": "Test notification sent"})


@router.post("/api/signals/{signal_id}/retry-publish")
def api_retry_publish(request: Request, signal_id: int):
    guard = _admin_role_guard(request)
    if guard:
        return guard
    from trading_engine.services.cms_publisher import publish_signal_to_all
    results = publish_signal_to_all(signal_id)
    if not results:
        return JSONResponse(
            content={"status": "error", "message": "No active WordPress configurations found. Add credentials in the WordPress tab."},
            status_code=503,
        )
    any_ok = any(r["status"] in ("ok", "skipped") for r in results)
    status_code = 200 if any_ok else 500
    return JSONResponse(content={"status": "ok" if any_ok else "error", "results": results}, status_code=status_code)


@router.post("/api/signals/{signal_id}/update-wp")
def api_update_wp_post(request: Request, signal_id: int):
    guard = _admin_role_guard(request)
    if guard:
        return guard
    from trading_engine.services.cms_publisher import update_closed_signal_on_all
    results = update_closed_signal_on_all(signal_id)
    if not results:
        return JSONResponse(
            content={"status": "error", "message": "No active WordPress configurations found. Add credentials in the WordPress tab."},
            status_code=503,
        )
    any_ok = any(r["status"] == "ok" for r in results)
    status_code = 200 if any_ok else 500
    return JSONResponse(content={"status": "ok" if any_ok else "error", "results": results}, status_code=status_code)


@router.get("/api/user-cms-configs")
def api_list_user_cms_configs(request: Request):
    guard = _auth_guard(request)
    if guard:
        return guard
    user = _get_session_user(request)
    from trading_engine.database import get_all_user_cms_configs
    scope_user_id = None if user and user.get("role") == "ADMIN" else (user["user_id"] if user else None)
    return JSONResponse(content=get_all_user_cms_configs(user_id=scope_user_id))


@router.post("/api/user-cms-configs")
def api_create_user_cms_config(request: Request, body: dict = Body(...)):
    guard = _auth_guard(request)
    if guard:
        return guard
    user = _get_session_user(request)
    required = ["site_url", "wp_username", "app_password"]
    for f in required:
        if not body.get(f, "").strip():
            return JSONResponse(content={"status": "error", "message": f"Missing required field: {f}"}, status_code=400)
    from trading_engine.database import create_user_cms_config
    try:
        if user.get("role") == "ADMIN" and body.get("user_id"):
            body["user_id"] = body["user_id"]
        else:
            body["user_id"] = user["user_id"]
        config_id = create_user_cms_config(body)
        return JSONResponse(content={"status": "ok", "id": config_id})
    except Exception as e:
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=500)


@router.delete("/api/user-cms-configs/{config_id}")
def api_delete_user_cms_config(request: Request, config_id: int):
    guard = _auth_guard(request)
    if guard:
        return guard
    user = _get_session_user(request)
    from trading_engine.database import delete_user_cms_config
    owner_filter = None if user and user.get("role") == "ADMIN" else (user["user_id"] if user else None)
    if delete_user_cms_config(config_id, user_id=owner_filter):
        return JSONResponse(content={"status": "ok"})
    return JSONResponse(content={"status": "error", "message": "Config not found"}, status_code=404)


@router.put("/api/user-cms-configs/{config_id}")
def api_update_user_cms_config(request: Request, config_id: int, body: dict = Body(...)):
    guard = _auth_guard(request)
    if guard:
        return guard
    user = _get_session_user(request)
    from trading_engine.database import update_user_cms_config
    owner_filter = None if user and user.get("role") == "ADMIN" else (user["user_id"] if user else None)
    if update_user_cms_config(config_id, body, user_id=owner_filter):
        return JSONResponse(content={"status": "ok"})
    return JSONResponse(content={"status": "error", "message": "Config not found"}, status_code=404)


@router.post("/api/wordpress/validate-credentials")
def api_validate_wp_credentials(request: Request, body: dict = Body(...)):
    guard = _auth_guard(request)
    if guard:
        return guard
    site_url = body.get("site_url", "").strip()
    wp_username = body.get("wp_username", "").strip()
    app_password = body.get("app_password", "").strip()
    if not site_url or not wp_username or not app_password:
        return JSONResponse(content={"status": "error", "message": "All fields required"}, status_code=400)
    from trading_engine.services.wp_connection import verify_wp_connection
    ok, message, site_name = verify_wp_connection(site_url, wp_username, app_password)
    if ok:
        return JSONResponse(content={"status": "ok", "message": message, "site_name": site_name or ""})
    return JSONResponse(content={"status": "error", "message": message})


@router.post("/api/user-cms-configs/{config_id}/test")
def api_test_user_cms_config(request: Request, config_id: int):
    guard = _auth_guard(request)
    if guard:
        return guard
    from trading_engine.database import get_user_cms_config_decrypted
    from trading_engine.services.wp_connection import verify_wp_connection
    cred = get_user_cms_config_decrypted(config_id)
    if not cred:
        return JSONResponse(content={"status": "error", "message": "Config not found"}, status_code=404)
    ok, message, site_name = verify_wp_connection(cred["site_url"], cred["wp_username"], cred["app_password"])
    if ok:
        return JSONResponse(content={"status": "ok", "message": message, "site_name": site_name or ""})
    return JSONResponse(content={"status": "error", "message": message})
