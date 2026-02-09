from __future__ import annotations

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse, JSONResponse

from registry_app.db import get_connection
from registry_app.registry import (
    get_agent,
    get_agent_card,
    get_default_version,
    get_version,
    list_agents,
    list_versions,
    register_agent_card,
)
from registry_app.schemas import RegisterAgentCardRequest


def build_registry_api() -> FastAPI:
    app = FastAPI(title="Agent Registry API")

    @app.get("/")
    def root_route():
        return ui_route()

    @app.get("/status")
    def status_route():
        db_ok = False
        db_error = None
        try:
            with get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
                    _ = cur.fetchone()
                db_ok = True
        except Exception as exc:
            db_error = str(exc)
        return {
            "database": {"ok": db_ok, "error": db_error},
            "a2a": {"ok": True, "url": "/a2a"},
            "mcp": {"ok": True, "url": "/mcp"},
            "test_agent": {"ok": True, "url": "/test-agent"},
        }

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

    @app.post("/agent-cards")
    def register_agent_card_route(payload: RegisterAgentCardRequest):
        card = payload.card.model_dump()
        card.setdefault("humanReadableId", payload.agent_id)
        card.setdefault("agentVersion", payload.version)
        with get_connection() as conn:
            register_agent_card(
                conn,
                agent_id=payload.agent_id,
                name=card.get("name") or payload.agent_id,
                description=card.get("description", ""),
                owner=payload.owner,
                status=payload.status,
                version=payload.version,
                mcp_server_url=payload.mcp_server_url,
                tags=payload.tags,
                protocol=payload.protocol,
                card_json=card,
            )
            conn.commit()
        return JSONResponse({"status": "ok", "agent_id": payload.agent_id})

    @app.get("/ui", response_class=HTMLResponse)
    def ui_route():
        html = """
<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8"/>
    <title>Agent Registry</title>
    <style>
      body { font-family: Arial, sans-serif; margin: 20px; }
      table { border-collapse: collapse; width: 100%; margin-top: 12px; }
      th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
      th { background: #f3f3f3; }
      pre { background: #f8f8f8; padding: 12px; }
      .status { margin-bottom: 12px; }
      .ok { color: #1b5e20; }
      .bad { color: #b71c1c; }
    </style>
  </head>
  <body>
    <h2>Agent Registry</h2>
    <div class="status" id="status"></div>
    <table id="agents">
      <thead>
        <tr>
          <th>Agent ID</th>
          <th>Name</th>
          <th>Description</th>
          <th>Status</th>
          <th>Default Version</th>
        </tr>
      </thead>
      <tbody></tbody>
    </table>
    <h3>Agent Card</h3>
    <pre id="card">Select an agent to view its card.</pre>
    <script>
      async function loadStatus() {
        const statusEl = document.getElementById("status");
        const resp = await fetch("/registry/status");
        const data = await resp.json();
        const dbClass = data.database.ok ? "ok" : "bad";
        statusEl.innerHTML = `
          <div>Database: <span class="${dbClass}">${data.database.ok}</span></div>
          <div>A2A: <span class="ok">${data.a2a.ok}</span> (${data.a2a.url})</div>
          <div>MCP: <span class="ok">${data.mcp.ok}</span> (${data.mcp.url})</div>
          <div>Test Agent: <span class="ok">${data.test_agent.ok}</span> (${data.test_agent.url})</div>
        `;
      }

      async function loadAgents() {
        const tbody = document.querySelector("#agents tbody");
        tbody.innerHTML = "";
        const resp = await fetch("/registry/agents");
        const data = await resp.json();
        for (const agent of data.agents || []) {
          const tr = document.createElement("tr");
          tr.innerHTML = `
            <td>${agent.agent_id}</td>
            <td>${agent.name}</td>
            <td>${agent.description}</td>
            <td>${agent.status}</td>
            <td>${agent.default_version}</td>
          `;
          tr.addEventListener("click", () => loadCard(agent.agent_id));
          tbody.appendChild(tr);
        }
      }

      async function loadCard(agentId) {
        const pre = document.getElementById("card");
        pre.textContent = "Loading...";
        const resp = await fetch(`/registry/agents/${agentId}/card`);
        if (!resp.ok) {
          pre.textContent = "Card not found.";
          return;
        }
        const data = await resp.json();
        pre.textContent = JSON.stringify(data, null, 2);
      }

      loadStatus();
      loadAgents();
    </script>
  </body>
</html>
"""
        return HTMLResponse(html)

    return app
