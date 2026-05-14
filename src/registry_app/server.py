from __future__ import annotations

from contextlib import asynccontextmanager
import json
import logging
from typing import AsyncIterator, Any

from a2a.server.apps import A2AStarletteApplication
from a2a.server.request_handlers import DefaultRequestHandler
from a2a.server.tasks import InMemoryTaskStore
from a2a.types import AgentCapabilities, AgentCard, AgentSkill
from pathlib import Path

from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Mount, Route
from starlette.staticfiles import StaticFiles

from registry_app.services.a2a_executor import RegistryAgentExecutor
from registry_app.services.http_api import build_registry_api
from registry_app.services.mcp_registry import build_mcp_app
from registry_app.services.test_agent import (
    build_test_agent_card_payload,
    build_test_agent_app,
    get_test_agent_history,
)
from registry_app.config import load_settings
from registry_app.db import get_connection
from registry_app.loopback import set_loopback_app
from registry_app.registry import (
    bootstrap_schema,
    get_agent_card,
    get_default_version,
    list_agent_cards,
    list_agents,
    register_agent_card,
    upsert_agent_protocol_card,
    upsert_agent_version,
)


logger = logging.getLogger(__name__)


def _build_agent_card() -> AgentCard:
    skill = AgentSkill(
        id="registry_gateway",
        name="Agent Registry Gateway",
        description="Routes A2A requests to MCP-first agents from Lakebase.",
        tags=["registry", "mcp", "a2a"],
        examples=[
            "Call an agent by sending JSON with agent_id and input.",
        ],
    )
    return AgentCard(
        name="Databricks Agent Registry Gateway",
        description="MCP-first registry with A2A gateway for agent execution.",
        url="/a2a",
        version="0.1.0",
        defaultInputModes=["text"],
        defaultOutputModes=["text"],
        capabilities=AgentCapabilities(streaming=False),
        skills=[skill],
        supportsAuthenticatedExtendedCard=False,
    )


def _healthcheck(_request):
    return JSONResponse({"status": "ok"})


def _test_agent_history(_request):
    return JSONResponse({"history": get_test_agent_history()})


def _normalize_card_json(card_row: dict[str, Any] | None) -> dict[str, Any] | None:
    if not card_row:
        return None
    card_json = card_row.get("card_json", card_row)
    if isinstance(card_json, str):
        try:
            card_json = json.loads(card_json)
        except json.JSONDecodeError:
            card_json = {"raw": card_json}
    return card_json


def _well_known_agent_card(request: Request):
    agent_id = request.query_params.get("agent_id")
    with get_connection() as conn:
        if agent_id:
            card_row = get_agent_card(conn, agent_id, protocol="a2a")
        else:
            cards = list_agent_cards(conn, protocol="a2a")
            card_row = cards[0] if cards else None
    card_json = _normalize_card_json(card_row)
    if not card_json:
        return JSONResponse({"detail": "Agent card not found."}, status_code=404)
    return JSONResponse(card_json)


def _seed_test_agent_card() -> None:
    settings = load_settings()
    if not settings.registry_base_url:
        return
    card_payload = build_test_agent_card_payload()
    desired_tags = {"source": "test-agent", "api_protocol": "a2a"}
    desired_api_url = f"{settings.registry_base_url}/test-agent"
    with get_connection() as conn:
        existing = get_agent_card(conn, "test-agent", protocol="a2a")
        if existing:
            version_row = get_default_version(conn, "test-agent")
            if version_row:
                current_tags = version_row.get("tags") if isinstance(version_row.get("tags"), dict) else {}
                current_api_url = version_row.get("api_url")
                if current_tags != desired_tags or current_api_url != desired_api_url:
                    upsert_agent_version(
                        conn,
                        agent_id="test-agent",
                        version=int(str(version_row.get("version", "1")).lstrip("v") or "1"),
                        api_url=desired_api_url,
                        tags=desired_tags,
                    )
                    conn.commit()
            return
        register_agent_card(
            conn,
            agent_id="test-agent",
            name=card_payload.get("name", "Test Agent"),
            description=card_payload.get("description", ""),
            owner="test-agent",
            status="active",
            version=1,
            api_url=desired_api_url,
            tags=desired_tags,
            protocol="a2a",
            card_json=card_payload,
        )
        conn.commit()


def _build_default_card_payload(
    agent: dict[str, Any], version_row: dict[str, Any]
) -> dict[str, Any]:
    agent_id = str(agent.get("agent_id", ""))
    name = agent.get("name") or agent_id
    description = agent.get("description") or ""
    tags = version_row.get("tags")
    skills = []
    if isinstance(tags, dict) and isinstance(tags.get("skills"), list):
        skills = [
            {
                "id": str(skill),
                "name": str(skill),
                "description": f"Skill: {skill}",
            }
            for skill in tags.get("skills", [])
            if str(skill).strip()
        ]
    if not skills:
        skills = [
            {
                "id": "default",
                "name": "default",
                "description": "Default skill.",
            }
        ]
    return {
        "schemaVersion": "1.0",
        "humanReadableId": agent_id,
        "agentVersion": str(version_row.get("version", "")),
        "name": name,
        "description": description,
        "url": "/a2a",
        "authSchemes": [{"scheme": "none"}],
        "defaultInputModes": ["text"],
        "defaultOutputModes": ["text"],
        "capabilities": {"streaming": False},
        "skills": skills,
        "supportsAuthenticatedExtendedCard": False,
    }


def _seed_registry_cards() -> None:
    with get_connection() as conn:
        agents = list_agents(conn)
        for agent in agents:
            agent_id = agent.get("agent_id")
            if not agent_id:
                continue
            version_row = get_default_version(conn, str(agent_id))
            if not version_row:
                continue
            version_value = version_row.get("version")
            if not version_value:
                continue
            existing = get_agent_card(
                conn, str(agent_id), version=str(version_value), protocol="a2a"
            )
            if existing:
                continue
            api_url = version_row.get("api_url")
            if not api_url:
                continue
            card_json = _build_default_card_payload(agent, version_row)
            upsert_agent_protocol_card(
                conn,
                agent_id=str(agent_id),
                version=str(version_value),
                protocol="a2a",
                card_json=card_json,
            )
        conn.commit()


class _MountRootProxy:
    def __init__(self, app: Starlette) -> None:
        self._app = app

    async def __call__(self, scope, receive, send) -> None:
        if scope["type"] != "http":
            await self._app(scope, receive, send)
            return
        new_scope = dict(scope)
        new_scope["root_path"] = f'{scope.get("root_path", "")}{scope["path"]}'
        new_scope["path"] = "/"
        new_scope["raw_path"] = b"/"
        await self._app(new_scope, receive, send)


def build_app() -> Starlette:
    agent_card = _build_agent_card()
    request_handler = DefaultRequestHandler(
        agent_executor=RegistryAgentExecutor(),
        task_store=InMemoryTaskStore(),
    )
    a2a_app = A2AStarletteApplication(
        agent_card=agent_card, http_handler=request_handler
    ).build(rpc_url="/")

    registry_api = build_registry_api()
    mcp_handler, mcp_sse_handler, mcp_lifespan = build_mcp_app()
    test_agent_app = build_test_agent_app()
    assets_dir = Path(__file__).resolve().parent / "assets"

    @asynccontextmanager
    async def lifespan(app: Starlette) -> AsyncIterator[None]:
        async with mcp_lifespan(app):
            try:
                with get_connection() as conn:
                    bootstrap_schema(conn)
            except Exception as exc:
                logger.warning("Failed to bootstrap registry schema: %s", exc)
            try:
                _seed_test_agent_card()
                _seed_registry_cards()
            except Exception as exc:
                logger.warning("Failed to seed agent cards: %s", exc)
            yield

    return Starlette(
        routes=[
            Route("/", endpoint=_healthcheck),
            Route("/.well-known/agent-card.json", endpoint=_well_known_agent_card),
            Route("/test-agent/history", endpoint=_test_agent_history),
            Route("/registry", endpoint=_MountRootProxy(registry_api)),
            Route("/registry/api", endpoint=_MountRootProxy(registry_api)),
            Route("/mcp", endpoint=_MountRootProxy(mcp_handler)),
            Route("/sse", endpoint=_MountRootProxy(mcp_sse_handler)),
            Route("/a2a", endpoint=_MountRootProxy(a2a_app)),
            Route("/test-agent", endpoint=_MountRootProxy(test_agent_app)),
            Mount("/assets", app=StaticFiles(directory=assets_dir), name="assets"),
            Mount("/registry", app=registry_api),
            Mount("/registry/api", app=registry_api),
            Mount("/mcp", app=mcp_handler),
            Mount("/sse", app=mcp_sse_handler),
            Mount("/a2a", app=a2a_app),
            Mount("/test-agent", app=test_agent_app),
        ],
        lifespan=lifespan,
    )


app = build_app()
set_loopback_app(app)
