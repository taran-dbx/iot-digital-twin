"""User identity extraction from Databricks Apps OAuth header."""

from __future__ import annotations

import base64
import json
import logging
from collections import OrderedDict

from fastapi import Request

logger = logging.getLogger("iot_digital_twin.auth")

_MAX_CACHE_SIZE = 1024
_user_cache: OrderedDict[str, str] = OrderedDict()


async def get_current_user(request: Request) -> str:
    """FastAPI dependency that resolves the current user email.

    In Databricks Apps the platform injects ``x-forwarded-access-token``
    which is a JWT whose ``sub`` claim contains the user email.
    We decode the payload directly (no network call needed since the
    proxy already validated the token).
    """
    token = request.headers.get("x-forwarded-access-token")
    if not token:
        return "anonymous"
    if token in _user_cache:
        _user_cache.move_to_end(token)
        return _user_cache[token]
    try:
        payload_b64 = token.split(".")[1]
        payload_b64 += "=" * (4 - len(payload_b64) % 4)
        payload = json.loads(base64.urlsafe_b64decode(payload_b64))
        username = payload.get("sub") or "anonymous"
        _user_cache[token] = username
        if len(_user_cache) > _MAX_CACHE_SIZE:
            _user_cache.popitem(last=False)
        return username
    except Exception as exc:
        logger.warning("Failed to decode user identity from token: %s", exc)
        return "anonymous"
