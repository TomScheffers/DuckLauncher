from uuid import UUID

import asyncpg
from authlib.integrations.starlette_client import OAuth
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse, RedirectResponse, Response
from starlette.middleware.sessions import SessionMiddleware

from ducklauncher.config import CoordinatorSettings
from ducklauncher.db import sessions as db_sessions
from ducklauncher.db import users as db_users
from ducklauncher.models import AuthMeResponse, UserResponse

SESSION_COOKIE = "session_id"
ANON_SUB_PREFIX = "anon:"

router = APIRouter(prefix="/auth", tags=["auth"])
_oauth: OAuth | None = None


def configure_auth(app, settings: CoordinatorSettings) -> None:
    if not settings.auth_enabled:
        return
    app.add_middleware(
        SessionMiddleware,
        secret_key=settings.session_secret,
        session_cookie="ducklauncher_oauth",
        max_age=600,
        same_site="lax",
        https_only=False,
    )
    global _oauth
    _oauth = OAuth()
    _oauth.register(
        name="oidc",
        client_id=settings.oidc_client_id,
        client_secret=settings.oidc_client_secret,
        server_metadata_url=f"{settings.oidc_issuer_url.rstrip('/')}/.well-known/openid-configuration",
        client_kwargs={"scope": "openid email profile"},
    )


def _settings(request: Request) -> CoordinatorSettings:
    return request.app.state.settings


def _pool(request: Request) -> asyncpg.Pool:
    return request.app.state.pool


def is_anonymous_user(user: asyncpg.Record) -> bool:
    return user["sub"].startswith(ANON_SUB_PREFIX)


def _user_response(row: asyncpg.Record) -> UserResponse:
    return UserResponse(
        user_id=row["user_id"],
        email=row.get("email"),
        name=row.get("name"),
    )


async def get_current_user(request: Request) -> asyncpg.Record:
    user = getattr(request.state, "user", None)
    if user is None:
        raise HTTPException(status_code=500, detail="Session middleware not configured")
    return user


async def require_user(request: Request) -> asyncpg.Record:
    return await get_current_user(request)


def _cookie_secure(request: Request) -> bool:
    forwarded = request.headers.get("x-forwarded-proto", "")
    return request.url.scheme == "https" or forwarded == "https"


def _set_session_cookie(response: Response, request: Request, session_id: UUID) -> None:
    response.set_cookie(
        key=SESSION_COOKIE,
        value=str(session_id),
        httponly=True,
        secure=_cookie_secure(request),
        samesite="lax",
        path="/",
    )


def _clear_session_cookie(response: Response) -> None:
    response.delete_cookie(key=SESSION_COOKIE, path="/")


@router.get("/me", response_model=AuthMeResponse)
async def auth_me(request: Request) -> AuthMeResponse:
    settings = _settings(request)
    user = await get_current_user(request)
    anonymous = is_anonymous_user(user)
    return AuthMeResponse(
        auth_enabled=settings.auth_enabled,
        authenticated=not anonymous,
        anonymous=anonymous,
        user=None if anonymous else _user_response(user),
    )


@router.get("/login")
async def auth_login(request: Request) -> RedirectResponse:
    settings = _settings(request)
    if not settings.auth_enabled or _oauth is None:
        raise HTTPException(status_code=404, detail="Authentication is not configured")
    redirect_uri = settings.resolved_oidc_redirect_uri
    return await _oauth.oidc.authorize_redirect(request, redirect_uri)


@router.get("/callback")
async def auth_callback(request: Request) -> RedirectResponse:
    settings = _settings(request)
    if not settings.auth_enabled or _oauth is None:
        raise HTTPException(status_code=404, detail="Authentication is not configured")
    raw = request.cookies.get(SESSION_COOKIE)
    if raw:
        try:
            await db_sessions.delete_session(_pool(request), UUID(raw))
        except ValueError:
            pass
    token = await _oauth.oidc.authorize_access_token(request)
    userinfo = token.get("userinfo")
    if userinfo is None:
        userinfo = await _oauth.oidc.parse_id_token(token, nonce=None)
    sub = userinfo.get("sub")
    if not sub:
        raise HTTPException(status_code=400, detail="OIDC token missing sub claim")
    user = await db_users.upsert_user(
        _pool(request),
        sub=sub,
        email=userinfo.get("email"),
        name=userinfo.get("name"),
    )
    session_id = await db_sessions.create_session(
        _pool(request),
        user_id=user["user_id"],
        ttl_hours=settings.session_ttl_hours,
    )
    response = RedirectResponse(url="/ui/", status_code=302)
    _set_session_cookie(response, request, session_id)
    return response


@router.post("/logout")
async def auth_logout(request: Request) -> JSONResponse:
    raw = request.cookies.get(SESSION_COOKIE)
    if raw:
        try:
            await db_sessions.delete_session(_pool(request), UUID(raw))
        except ValueError:
            pass
    user = await db_users.create_anonymous_user(_pool(request))
    session_id = await db_sessions.create_session(
        _pool(request),
        user_id=user["user_id"],
        ttl_hours=_settings(request).session_ttl_hours,
    )
    response = JSONResponse({"status": "ok"})
    _set_session_cookie(response, request, session_id)
    return response
