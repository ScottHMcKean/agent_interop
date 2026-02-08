from __future__ import annotations

import json
from contextlib import asynccontextmanager
from typing import AsyncIterator

from mcp import types
from mcp.server.streamable_http import StreamableHTTPSessionManager
from fastmcp import Server
from starlette.types import Receive, Scope, Send

from registry_app.db import get_connection
from registry_app.registry import list_agent_cards, get_agent_card


def build_mcp_app():
    app = Server("agent-registry")

    @app.list_resources()
    async def list_resources() -> list[types.Resource]:
        with get_connection() as conn:
            cards = list_agent_cards(conn, protocol="a2a")
        resources: list[types.Resource] = []
        for card in cards:
            resources.append(
                types.Resource(
                    uri=f"resource://agent_cards/{card['agent_id']}",
                    name=card["agent_id"],
                    mimeType="application/json",
                )
            )
        return resources

    @app.read_resource()
    async def read_resource(uri: str) -> str:
        prefix = "resource://agent_cards/"
        if not uri.startswith(prefix):
            raise ValueError("Unknown resource.")
        agent_id = uri.removeprefix(prefix)
        with get_connection() as conn:
            card = get_agent_card(conn, agent_id, protocol="a2a")
        if not card:
            raise ValueError("Resource not found.")
        return json.dumps(card["card_json"], indent=2)

    session_manager = StreamableHTTPSessionManager(
        app=app,
        event_store=None,
        stateless=True,
    )

    async def handle_streamable_http(
        scope: Scope, receive: Receive, send: Send
    ) -> None:
        await session_manager.handle_request(scope, receive, send)

    @asynccontextmanager
    async def lifespan(app) -> AsyncIterator[None]:
        async with session_manager.run():
            yield

    return handle_streamable_http, lifespan
