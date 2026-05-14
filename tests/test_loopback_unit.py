from __future__ import annotations

import httpx
import pytest

from registry_app import loopback


@pytest.fixture(autouse=True)
def _reset_loopback():
    loopback.set_loopback_app(None)
    yield
    loopback.set_loopback_app(None)


def test_is_same_host_matches_prefix():
    assert loopback.is_same_host(
        "https://app.example.com/test-agent", "https://app.example.com"
    )
    assert loopback.is_same_host(
        "https://app.example.com/test-agent", "https://app.example.com/"
    )


def test_is_same_host_rejects_different_origin():
    assert not loopback.is_same_host(
        "https://other.example.com/test-agent", "https://app.example.com"
    )
    assert not loopback.is_same_host("/path", None)
    assert not loopback.is_same_host("", "https://app.example.com")


def test_strip_to_path_preserves_query():
    assert loopback._strip_to_path("https://h/a/b?x=1") == "/a/b?x=1"
    assert loopback._strip_to_path("https://h") == "/"
    assert loopback._strip_to_path("https://h/test-agent") == "/test-agent"


def test_make_async_client_uses_asgi_transport_when_same_host():
    async def asgi(scope, receive, send):
        return

    loopback.set_loopback_app(asgi)
    client, url = loopback.make_async_client(
        "https://app.example.com/test-agent",
        "https://app.example.com",
        timeout=1.0,
    )
    try:
        assert isinstance(client._transport, httpx.ASGITransport)
        assert url == "/test-agent"
    finally:
        import asyncio

        asyncio.run(client.aclose())


def test_make_async_client_falls_back_to_http_when_different_host():
    async def asgi(scope, receive, send):
        return

    loopback.set_loopback_app(asgi)
    client, url = loopback.make_async_client(
        "https://elsewhere.example.com/api",
        "https://app.example.com",
        timeout=1.0,
    )
    try:
        assert not isinstance(client._transport, httpx.ASGITransport)
        assert url == "https://elsewhere.example.com/api"
    finally:
        import asyncio

        asyncio.run(client.aclose())


def test_make_async_client_falls_back_when_no_loopback_registered():
    client, url = loopback.make_async_client(
        "https://app.example.com/test-agent",
        "https://app.example.com",
        timeout=1.0,
    )
    try:
        assert not isinstance(client._transport, httpx.ASGITransport)
        assert url == "https://app.example.com/test-agent"
    finally:
        import asyncio

        asyncio.run(client.aclose())
