from __future__ import annotations

from fastapi import FastAPI, HTTPException, Query

from registry_app.db import get_connection
from registry_app.registry import (
    get_agent,
    get_agent_card,
    get_default_version,
    get_version,
    list_agents,
    list_versions,
)


def build_registry_api() -> FastAPI:
    app = FastAPI(title="Agent Registry API")

    @app.get("/agents")
    def list_agents_route():
        with get_connection() as conn:
            return {"agents": list_agents(conn)}

    @app.get("/agents/{agent_id}")
    def get_agent_route(agent_id: str):
        with get_connection() as conn:
            agent = get_agent(conn, agent_id)
        if not agent:
            raise HTTPException(status_code=404, detail="Agent not found.")
        return agent

    @app.get("/agents/{agent_id}/versions")
    def list_versions_route(agent_id: str):
        with get_connection() as conn:
            versions = list_versions(conn, agent_id)
        return {"versions": versions}

    @app.get("/agents/{agent_id}/versions/{version}")
    def get_version_route(agent_id: str, version: str):
        with get_connection() as conn:
            result = get_version(conn, agent_id, version)
        if not result:
            raise HTTPException(status_code=404, detail="Version not found.")
        return result

    @app.get("/agents/{agent_id}/card")
    def get_card_route(
        agent_id: str, version: str | None = Query(default=None)
    ):
        with get_connection() as conn:
            if version:
                card = get_agent_card(conn, agent_id, version=version)
            else:
                default_version = get_default_version(conn, agent_id)
                if default_version:
                    card = get_agent_card(
                        conn, agent_id, version=default_version["version"]
                    )
                else:
                    card = get_agent_card(conn, agent_id)
        if not card:
            raise HTTPException(status_code=404, detail="Agent card not found.")
        return card["card_json"]

    return app
