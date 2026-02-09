from __future__ import annotations

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse, JSONResponse
import httpx
from databricks.sdk import WorkspaceClient
import html

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

    def _auth_headers() -> dict[str, str]:
        try:
            return WorkspaceClient().config.authenticate()
        except Exception:
            return {}

    def _validate_api_url(api_url: str) -> None:
        if not api_url:
            raise HTTPException(status_code=400, detail="Missing api_url.")
        payload = {"input": "ping", "metadata": {"source": "registry-ui"}}
        headers = _auth_headers()
        try:
            with httpx.Client(timeout=10.0) as client:
                resp = client.post(api_url, json=payload, headers=headers)
                resp.raise_for_status()
        except httpx.HTTPStatusError as exc:
            raise HTTPException(
                status_code=400,
                detail=f"API URL validation failed: {exc.response.status_code}",
            ) from exc
        except Exception as exc:
            raise HTTPException(
                status_code=400,
                detail=f"API URL validation failed: {exc}",
            ) from exc

    @app.get("/")
    def root_route():
        return list_route()

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
    def get_card_route(agent_id: str, version: str | None = Query(default=None)):
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
        if payload.api_url:
            _validate_api_url(payload.api_url)
        card = payload.card.model_dump()
        card.setdefault("humanReadableId", payload.agent_id)
        card.setdefault("url", "/a2a")
        with get_connection() as conn:
            register_agent_card(
                conn,
                agent_id=payload.agent_id,
                name=card.get("name") or payload.agent_id,
                description=card.get("description", ""),
                owner=payload.owner,
                status=payload.status,
                version=1,
                api_url=payload.api_url,
                tags=payload.tags,
                protocol=payload.protocol,
                card_json=card,
            )
            conn.commit()
        return JSONResponse({"status": "ok", "agent_id": payload.agent_id})

    def _render_page(title: str, body: str) -> str:
        return f"""
<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8"/>
    <title>{title}</title>
    <style>
      :root {{
        --bg: #f6f7fb;
        --card: #ffffff;
        --border: #e0e3eb;
        --text: #1b1f2a;
        --muted: #5b6270;
        --accent: #ff3621;
      }}
      body {{
        font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
        margin: 0;
        background: var(--bg);
        color: var(--text);
      }}
      header {{
        display: flex;
        align-items: center;
        justify-content: space-between;
        padding: 16px 24px;
        background: var(--card);
        border-bottom: 1px solid var(--border);
      }}
      header img {{ height: 28px; }}
      header nav a {{
        color: var(--text);
        text-decoration: none;
        margin-left: 16px;
        font-weight: 600;
      }}
      .container {{
        max-width: 1100px;
        margin: 24px auto;
        padding: 0 24px;
      }}
      .card {{
        background: var(--card);
        border: 1px solid var(--border);
        border-radius: 10px;
        padding: 20px;
        box-shadow: 0 2px 6px rgba(17, 24, 39, 0.06);
      }}
      .card + .card {{ margin-top: 16px; }}
      table {{ border-collapse: collapse; width: 100%; margin-top: 12px; }}
      th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
      th {{ background: #f3f3f7; }}
      pre {{ background: #f8f8f8; padding: 12px; border-radius: 8px; }}
      .status {{ margin-bottom: 12px; color: var(--muted); }}
      .ok {{ color: #1b5e20; }}
      .bad {{ color: #b71c1c; }}
      .muted {{ color: var(--muted); }}
      .grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }}
      label {{ display: block; font-weight: 600; margin-bottom: 6px; }}
      input, textarea, select {{
        width: 100%;
        border: 1px solid var(--border);
        border-radius: 6px;
        padding: 8px 10px;
        font-size: 14px;
      }}
      button {{
        background: var(--accent);
        color: white;
        border: none;
        padding: 10px 16px;
        border-radius: 6px;
        font-weight: 600;
        cursor: pointer;
      }}
      .banner {{
        background: linear-gradient(90deg, rgba(255,54,33,0.12), transparent);
        border: 1px solid var(--border);
        padding: 12px 16px;
        border-radius: 8px;
        margin-bottom: 16px;
      }}
    </style>
  </head>
  <body>
    <header>
      <img src="/assets/databricks_logo.svg" alt="Databricks"/>
      <nav>
        <a href="/registry/list">Registry</a>
        <a href="/registry/register">Register Agent</a>
        <a href="/registry/invoke">Invoke Agent</a>
      </nav>
    </header>
    <div class="container">
      {body}
    </div>
  </body>
</html>
"""

    @app.get("/list", response_class=HTMLResponse)
    def list_route():
        body = """
      <div class="banner">
        <strong>Agent Registry</strong>
        <div class="muted">Browse registered agents and inspect their cards.</div>
      </div>
      <div class="card">
        <div class="status" id="status"></div>
      </div>
      <div class="card">
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
      </div>
      <div class="card">
        <h3>Agent Card</h3>
        <pre id="card">Select an agent to view its card.</pre>
      </div>
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
"""
        return HTMLResponse(_render_page("Agent Registry", body))

    @app.get("/register", response_class=HTMLResponse)
    def register_route():
        body = """
      <div class="banner">
        <strong>Register Agent</strong>
        <div class="muted">Create or update an agent card in the registry.</div>
      </div>
      <div class="card">
        <div class="grid">
          <div>
            <label>Agent ID</label>
            <input id="agent_id" placeholder="test-agent"/>
          </div>
          <div>
            <label>Agent Name</label>
            <input id="name" placeholder="Test Agent"/>
          </div>
          <div>
            <label>Agent API URL</label>
            <input id="api_url" placeholder="https://<workspace>/serving-endpoints/<endpoint>/invocations"/>
          </div>
          <div>
            <label>Skills (comma-separated)</label>
            <input id="skills" placeholder="test,registry"/>
          </div>
        </div>
        <div style="margin-top: 12px;">
          <label>Description</label>
          <textarea id="description" rows="3" placeholder="Describe the agent..."></textarea>
        </div>
        <div style="margin-top: 12px;">
          <button id="submit">Register Agent</button>
          <span id="status_msg" class="muted" style="margin-left: 12px;"></span>
        </div>
      </div>
      <script>
        document.getElementById("submit").addEventListener("click", async () => {
          const agentId = document.getElementById("agent_id").value.trim();
          const name = document.getElementById("name").value.trim();
          const url = "/a2a";
          const apiUrl = document.getElementById("api_url").value.trim();
          const description = document.getElementById("description").value.trim();
          const skills = document.getElementById("skills").value.split(",").map(s => s.trim()).filter(Boolean);
          const statusEl = document.getElementById("status_msg");

          if (!agentId || !name || !apiUrl) {
            statusEl.textContent = "Please fill Agent ID, Name, and Agent API URL.";
            return;
          }

          const payload = {
            agent_id: agentId,
            api_url: apiUrl,
            card: {
              name: name,
              description: description || "Registered via UI",
              url: url,
              version: "1.0.0",
              defaultInputModes: ["text"],
              defaultOutputModes: ["text"],
              capabilities: { streaming: false },
              skills: skills.map(skill => ({
                id: skill,
                name: skill,
                description: "Skill: " + skill
              }))
            }
          };

          statusEl.textContent = "Submitting...";
          const resp = await fetch("/registry/agent-cards", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(payload)
          });
          if (!resp.ok) {
            statusEl.textContent = "Registration failed.";
            return;
          }
          statusEl.textContent = "Registered.";
        });
      </script>
"""
        return HTMLResponse(_render_page("Register Agent", body))

    @app.get("/invoke", response_class=HTMLResponse)
    def invoke_route():
        with get_connection() as conn:
            agents = list_agents(conn)
        options = []
        for agent in agents:
            agent_id = html.escape(str(agent.get("agent_id", "")))
            name = html.escape(str(agent.get("name", "")))
            if not agent_id:
                continue
            options.append(f'<option value="{agent_id}">{agent_id} ({name})</option>')
        options_html = "\n".join(options) or '<option value="">No agents found</option>'
        body = """
      <div class="banner">
        <strong>Invoke Agent</strong>
        <div class="muted">Send an A2A message to the registry gateway.</div>
      </div>
      <div class="card">
        <div class="grid">
          <div>
            <label>Agent</label>
            <select id="invoke_agent_id">
              __OPTIONS__
            </select>
          </div>
          <div>
            <label>Version (optional)</label>
            <input id="invoke_version" placeholder="v1"/>
          </div>
        </div>
        <div style="margin-top: 12px;">
          <label>Input</label>
          <textarea id="invoke_input" rows="4" placeholder="Hello from A2A..."></textarea>
        </div>
        <div style="margin-top: 12px;">
          <button id="invoke_submit">Invoke</button>
          <span id="invoke_status" class="muted" style="margin-left: 12px;"></span>
        </div>
      </div>
      <div class="card">
        <h3>Raw Response</h3>
        <pre id="invoke_response_raw">No response yet.</pre>
      </div>
      <div class="card">
        <h3>Messages</h3>
        <pre id="invoke_messages">No messages yet.</pre>
      </div>
      <script>
        document.getElementById("invoke_submit").addEventListener("click", async () => {
          const agentId = document.getElementById("invoke_agent_id").value.trim();
          const version = document.getElementById("invoke_version").value.trim();
          const inputText = document.getElementById("invoke_input").value.trim();
          const statusEl = document.getElementById("invoke_status");
          const rawEl = document.getElementById("invoke_response_raw");
          const messagesEl = document.getElementById("invoke_messages");

          if (!agentId || !inputText) {
            statusEl.textContent = "Please fill Agent ID and Input.";
            return;
          }

          const payload = {
            jsonrpc: "2.0",
            id: Math.random().toString(36).slice(2),
            method: "message/send",
            params: {
              message: {
                role: "user",
                messageId: Math.random().toString(36).slice(2),
                parts: [
                  {
                    kind: "text",
                    text: JSON.stringify({
                      agent_id: agentId,
                      input: inputText,
                      ...(version ? { version } : {})
                    })
                  }
                ]
              }
            }
          };

          statusEl.textContent = "Invoking...";
          const resp = await fetch("/a2a", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(payload)
          });
          const data = await resp.json().catch(() => ({}));
          rawEl.textContent = JSON.stringify(data, null, 2);
          messagesEl.textContent = JSON.stringify(data?.result?.result || {}, null, 2);
          statusEl.textContent = resp.ok ? "Done." : "Invocation failed.";
        });
      </script>
"""
        body = body.replace("__OPTIONS__", options_html)
        return HTMLResponse(_render_page("Invoke Agent", body))

    return app
