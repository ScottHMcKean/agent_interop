from __future__ import annotations

from contextlib import asynccontextmanager
from typing import AsyncIterator

from a2a.server.apps import A2AStarletteApplication
from a2a.server.request_handlers import DefaultRequestHandler
from a2a.server.tasks import InMemoryTaskStore
from a2a.types import AgentCapabilities, AgentCard, AgentSkill
from starlette.applications import Starlette
from starlette.responses import JSONResponse
from starlette.routing import Mount, Route

from registry_app.services.a2a_executor import RegistryAgentExecutor
from registry_app.services.http_api import build_registry_api
from registry_app.services.mcp_registry import build_mcp_app


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
        url="/api/a2a",
        version="0.1.0",
        defaultInputModes=["text"],
        defaultOutputModes=["text"],
        capabilities=AgentCapabilities(streaming=False),
        skills=[skill],
        supportsAuthenticatedExtendedCard=False,
    )


def _healthcheck(_request):
    return JSONResponse({"status": "ok"})


def build_app() -> Starlette:
    agent_card = _build_agent_card()
    request_handler = DefaultRequestHandler(
        agent_executor=RegistryAgentExecutor(),
        task_store=InMemoryTaskStore(),
    )
    a2a_app = A2AStarletteApplication(
        agent_card=agent_card, http_handler=request_handler
    ).build(rpc_url="/a2a")

    registry_api = build_registry_api()
    mcp_handler, mcp_lifespan = build_mcp_app()

    @asynccontextmanager
    async def lifespan(app: Starlette) -> AsyncIterator[None]:
        async with mcp_lifespan(app):
            yield

    return Starlette(
        routes=[
            Route("/", endpoint=_healthcheck),
            Mount("/api/registry", app=registry_api),
            Mount("/api/mcp", app=mcp_handler),
            Mount("/api", app=a2a_app),
        ],
        lifespan=lifespan,
    )


app = build_app()
