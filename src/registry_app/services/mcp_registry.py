from __future__ import annotations

import json

from registry_app.db import get_connection
from registry_app.services.mcp_gateway import register_gateway_tools
from registry_app.registry import get_agent_card, list_agent_cards
from fastmcp import FastMCP


def build_mcp_app():
    app = FastMCP("agent-registry")
    register_gateway_tools(app)

    @app.resource(
        "resource://agent_cards/{agent_id}",
        name="agent_card",
        mime_type="application/json",
    )
    def read_agent_card(agent_id: str) -> dict:
        with get_connection() as conn:
            card = get_agent_card(conn, agent_id, protocol="a2a")
        if not card:
            raise ValueError("Resource not found.")
        return card["card_json"]

    @app.resource(
        "resource://agent_cards",
        name="agent_cards",
        mime_type="application/json",
    )
    def list_agent_cards_resource() -> dict:
        with get_connection() as conn:
            cards = list_agent_cards(conn, protocol="a2a")
        return {"agents": [card["agent_id"] for card in cards]}

    mcp_app = app.http_app(
        path="/",
        transport="streamable-http",
        stateless_http=True,
    )
    return mcp_app, mcp_app.lifespan
