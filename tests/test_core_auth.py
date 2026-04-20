"""Tests for core.auth — JWT-based user identity extraction."""

from __future__ import annotations

import base64
import json
from unittest.mock import MagicMock

import pytest
import pytest_asyncio

from core import auth
from core.auth import get_current_user


@pytest.fixture(autouse=True)
def _clear_user_cache() -> None:
    """Ensure each test starts with a fresh user cache."""
    auth._user_cache.clear()


def _make_jwt(payload: dict) -> str:
    """Build a minimal unsigned JWT (header.payload.signature)."""
    header = base64.urlsafe_b64encode(json.dumps({"alg": "none"}).encode()).rstrip(b"=").decode()
    body = base64.urlsafe_b64encode(json.dumps(payload).encode()).rstrip(b"=").decode()
    return f"{header}.{body}.fakesig"


def _request_with_token(token: str | None) -> MagicMock:
    """Create a mock FastAPI Request with the given access token header."""
    request = MagicMock()
    if token is None:
        request.headers.get.return_value = None
    else:
        request.headers.get.return_value = token
    return request


class TestGetCurrentUserNoToken:
    """When no token header is present, return 'anonymous'."""

    @pytest.mark.asyncio
    async def test_returns_anonymous_when_header_missing(self) -> None:
        request = _request_with_token(None)
        result = await get_current_user(request)
        assert result == "anonymous"

    @pytest.mark.asyncio
    async def test_returns_anonymous_when_header_empty_string(self) -> None:
        request = MagicMock()
        request.headers.get.return_value = ""
        result = await get_current_user(request)
        assert result == "anonymous"


class TestGetCurrentUserValidToken:
    """When a valid JWT is provided, extract the 'sub' claim."""

    @pytest.mark.asyncio
    async def test_decodes_sub_claim(self) -> None:
        token = _make_jwt({"sub": "alice@company.com"})
        request = _request_with_token(token)
        result = await get_current_user(request)
        assert result == "alice@company.com"

    @pytest.mark.asyncio
    async def test_returns_anonymous_when_sub_missing(self) -> None:
        token = _make_jwt({"name": "Alice"})
        request = _request_with_token(token)
        result = await get_current_user(request)
        assert result == "anonymous"

    @pytest.mark.asyncio
    async def test_returns_anonymous_when_sub_is_empty(self) -> None:
        token = _make_jwt({"sub": ""})
        request = _request_with_token(token)
        result = await get_current_user(request)
        assert result == "anonymous"


class TestGetCurrentUserMalformedToken:
    """When the token cannot be decoded, return 'anonymous'."""

    @pytest.mark.asyncio
    async def test_returns_anonymous_for_garbage_token(self) -> None:
        request = _request_with_token("not.a.valid.jwt")
        result = await get_current_user(request)
        assert result == "anonymous"

    @pytest.mark.asyncio
    async def test_returns_anonymous_for_single_segment_token(self) -> None:
        request = _request_with_token("single-segment")
        result = await get_current_user(request)
        assert result == "anonymous"


class TestGetCurrentUserCaching:
    """Decoded tokens are cached for performance."""

    @pytest.mark.asyncio
    async def test_caches_decoded_token(self) -> None:
        token = _make_jwt({"sub": "bob@company.com"})
        request = _request_with_token(token)

        first = await get_current_user(request)
        second = await get_current_user(request)

        assert first == "bob@company.com"
        assert second == "bob@company.com"
        assert token in auth._user_cache

    @pytest.mark.asyncio
    async def test_different_tokens_cached_separately(self) -> None:
        token_a = _make_jwt({"sub": "alice@co.com"})
        token_b = _make_jwt({"sub": "bob@co.com"})

        result_a = await get_current_user(_request_with_token(token_a))
        result_b = await get_current_user(_request_with_token(token_b))

        assert result_a == "alice@co.com"
        assert result_b == "bob@co.com"
        assert len(auth._user_cache) == 2
