from uuid import UUID

from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import Response

from ducklauncher.coordinator.auth import SESSION_COOKIE, _set_session_cookie
from ducklauncher.db import sessions as db_sessions
from ducklauncher.db import users as db_users


class UserSessionMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        pool = request.app.state.pool
        settings = request.app.state.settings
        user = None
        new_session_id = None

        raw = request.cookies.get(SESSION_COOKIE)
        if raw:
            try:
                user = await db_sessions.get_session_user(pool, UUID(raw))
            except ValueError:
                user = None

        if user is None:
            user = await db_users.create_anonymous_user(pool)
            new_session_id = await db_sessions.create_session(
                pool,
                user_id=user["user_id"],
                ttl_hours=settings.session_ttl_hours,
            )

        request.state.user = user
        response = await call_next(request)
        if new_session_id is not None:
            _set_session_cookie(response, request, new_session_id)
        return response
