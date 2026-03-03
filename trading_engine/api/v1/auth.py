import re
import logging
from fastapi import APIRouter, Form, Request
from fastapi.responses import RedirectResponse, HTMLResponse

from trading_engine.database import (
    get_user_by_username,
    get_user_by_email,
    create_admin,
)

logger = logging.getLogger("trading_engine.auth")

router = APIRouter(prefix="/api/v1/auth", tags=["auth"])

_EMAIL_RE = re.compile(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")

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
.register-btn { width: 100%; background: #3b82f6; color: white; padding: 12px; border: none; border-radius: 6px; font-size: 0.95rem; font-weight: 600; cursor: pointer; margin-top: 8px; }
.register-btn:hover { background: #2563eb; }
.error-msg { background: #450a0a; border: 1px solid #991b1b; color: #fca5a5; padding: 10px 14px; border-radius: 6px; font-size: 0.85rem; margin-bottom: 16px; text-align: center; }
.success-msg { background: #052e16; border: 1px solid #166534; color: #86efac; padding: 10px 14px; border-radius: 6px; font-size: 0.85rem; margin-bottom: 16px; text-align: center; }
.link-row { text-align: center; margin-top: 16px; font-size: 0.85rem; color: #94a3b8; }
.link-row a { color: #3b82f6; text-decoration: none; }
.link-row a:hover { text-decoration: underline; }
"""


def _build_register_page(error: str = "", success: str = "") -> str:
    error_html = f'<div class="error-msg" data-testid="text-register-error">{error}</div>' if error else ""
    success_html = f'<div class="success-msg" data-testid="text-register-success">{success}</div>' if success else ""
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Trading Engine - Register</title>
    <style>{LOGIN_CSS}</style>
</head>
<body>
    <div class="login-card">
        <h1>Create Account</h1>
        <p>Register for a new trading signals account</p>
        {error_html}
        {success_html}
        <form method="POST" action="/api/v1/auth/register">
            <div class="form-group">
                <label for="username">Username</label>
                <input type="text" id="username" name="username" data-testid="input-reg-username" required autofocus>
            </div>
            <div class="form-group">
                <label for="email">Email</label>
                <input type="email" id="email" name="email" data-testid="input-reg-email" required>
            </div>
            <div class="form-group">
                <label for="password">Password</label>
                <input type="password" id="password" name="password" data-testid="input-reg-password" required>
            </div>
            <div class="form-group">
                <label for="confirm_password">Confirm Password</label>
                <input type="password" id="confirm_password" name="confirm_password" data-testid="input-reg-confirm" required>
            </div>
            <button type="submit" class="register-btn" data-testid="button-register">Create Account</button>
        </form>
        <div class="link-row">Already have an account? <a href="/admin/login" data-testid="link-login">Sign in</a></div>
    </div>
</body>
</html>"""


@router.get("/register", response_class=HTMLResponse)
def register_page():
    return HTMLResponse(content=_build_register_page())


@router.post("/register")
async def register_submit(
    request: Request,
    username: str = Form(...),
    email: str = Form(...),
    password: str = Form(...),
    confirm_password: str = Form(...),
):
    username = username.strip()
    email = email.strip().lower()
    base_path = request.scope.get("root_path", "")

    if not username or len(username) < 3:
        return HTMLResponse(content=_build_register_page(error="Username must be at least 3 characters."))

    if not _EMAIL_RE.match(email):
        return HTMLResponse(content=_build_register_page(error="Please enter a valid email address."))

    if len(password) < 6:
        return HTMLResponse(content=_build_register_page(error="Password must be at least 6 characters."))

    if password != confirm_password:
        return HTMLResponse(content=_build_register_page(error="Passwords do not match."))

    existing_user = get_user_by_username(username)
    if existing_user:
        return HTMLResponse(content=_build_register_page(error="That username is already taken."))

    existing_email = get_user_by_email(email)
    if existing_email:
        return HTMLResponse(content=_build_register_page(error="An account with that email already exists."))

    user_id = create_admin(username=username, password=password, email=email)
    if user_id is None:
        return HTMLResponse(content=_build_register_page(error="Registration failed. Please try again."))

    logger.info(f"[AUTH] New user registered: {username} (id={user_id}, role=CUSTOMER)")
    return RedirectResponse(
        url=base_path + "/admin/login?registered=1",
        status_code=302,
    )
