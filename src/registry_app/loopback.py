"""
In-process ASGI loopback helper.

When the registry gateway needs to call an agent whose api_url points back at the
same app (e.g. the bundled test agent), routing through the external Databricks
Apps reverse proxy fails because an app's service principal token isn't accepted
for self-calls. We dispatch directly to the ASGI app in-process instead — no
network hop, no auth needed.
"""

from __future__ import annotations

from typing import Any
from urllib.parse import urlparse

import httpx


_loopback_app: Any = None


def set_loopback_app(app: Any) -> None:
    global _loopback_app
    _loopback_app = app


def _strip_to_path(url: str) -> str:
    parsed = urlparse(url)
    path = parsed.path or "/"
    if parsed.query:
        path = f"{path}?{parsed.query}"
    return path


def is_same_host(url: str, base_url: str | None) -> bool:
    if not url or not base_url:
        return False
    return url.startswith(base_url.rstrip("/"))


def make_async_client(
    api_url: str, base_url: str | None, *, timeout: float
) -> tuple[httpx.AsyncClient, str]:
    """
    Returns (client, request_url). If api_url is on the same host as the app's
    base_url and a loopback app is registered, the client routes via
    httpx.ASGITransport and request_url is just the path. Otherwise the client
    speaks HTTP and request_url is the full url.
    """
    if _loopback_app is not None and is_same_host(api_url, base_url):
        client = httpx.AsyncClient(
            transport=httpx.ASGITransport(app=_loopback_app),
            base_url="http://loopback",
            timeout=timeout,
        )
        return client, _strip_to_path(api_url)
    return httpx.AsyncClient(timeout=timeout), api_url
