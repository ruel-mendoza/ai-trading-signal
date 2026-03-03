import os
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

_TEMPLATE_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))), "templates", "auth")


def _read_register_template(error: str = "") -> str:
    path = os.path.join(_TEMPLATE_DIR, "register.html")
    with open(path, "r") as f:
        html = f.read()
    if error:
        error_div = f'<div class="mb-4 p-3 rounded-lg bg-red-900/30 border border-red-800/50 text-red-300 text-sm text-center" data-testid="text-register-error">{error}</div>'
        html = html.replace(
            '<div id="server-error" class="hidden mb-4 p-3 rounded-lg bg-red-900/30 border border-red-800/50 text-red-300 text-sm text-center" data-testid="text-register-error">\n                SERVER_ERROR_PLACEHOLDER\n            </div>',
            error_div,
        )
    return html


@router.get("/register", response_class=HTMLResponse)
def register_page():
    return HTMLResponse(content=_read_register_template())


@router.post("/register")
async def register_submit(
    request: Request,
    full_name: str = Form(""),
    username: str = Form(...),
    email: str = Form(...),
    password: str = Form(...),
    confirm_password: str = Form(...),
):
    full_name = full_name.strip()
    username = username.strip()
    email = email.strip().lower()
    base_path = request.scope.get("root_path", "")

    if not full_name or len(full_name) < 2:
        return HTMLResponse(content=_read_register_template(error="Full name must be at least 2 characters."))

    if not username or len(username) < 3:
        return HTMLResponse(content=_read_register_template(error="Username must be at least 3 characters."))

    if not _EMAIL_RE.match(email):
        return HTMLResponse(content=_read_register_template(error="Please enter a valid email address."))

    if len(password) < 6:
        return HTMLResponse(content=_read_register_template(error="Password must be at least 6 characters."))

    if password != confirm_password:
        return HTMLResponse(content=_read_register_template(error="Passwords do not match."))

    existing_user = get_user_by_username(username)
    if existing_user:
        return HTMLResponse(content=_read_register_template(error="That username is already taken."))

    existing_email = get_user_by_email(email)
    if existing_email:
        return HTMLResponse(content=_read_register_template(error="An account with that email already exists."))

    user_id = create_admin(username=username, password=password, email=email, full_name=full_name)
    if user_id is None:
        return HTMLResponse(content=_read_register_template(error="Registration failed. Please try again."))

    logger.info(f"[AUTH] New user registered: {username} (id={user_id}, role=CUSTOMER)")
    return RedirectResponse(
        url=base_path + "/admin/login?registered=1",
        status_code=302,
    )
