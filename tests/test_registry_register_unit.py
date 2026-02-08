from __future__ import annotations

import json

from pydantic import ValidationError

from registry_app.registry import register_agent_card
from registry_app.schemas import RegisterAgentCardRequest


class FakeCursor:
    def __init__(self):
        self.calls = []

    def execute(self, query, params=None):
        self.calls.append((query, params))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class FakeConn:
    def __init__(self):
        self.cursor_obj = FakeCursor()

    def cursor(self):
        return self.cursor_obj

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


def test_register_agent_card_validates_payload():
    payload = {
        "agent_id": "demo/agent",
        "version": "1.0.0",
        "mcp_server_url": "https://example.com/mcp",
        "card": {
            "name": "Demo Agent",
            "description": "Does things.",
            "url": "https://example.com/a2a",
            "version": "1.0.0",
            "defaultInputModes": ["text"],
            "defaultOutputModes": ["text"],
            "capabilities": {"streaming": False},
            "skills": [{"id": "demo", "name": "Demo", "description": "Demo"}],
        },
    }
    request = RegisterAgentCardRequest.model_validate(payload)
    assert request.agent_id == "demo/agent"


def test_register_agent_card_missing_required_fields():
    payload = {"agent_id": "demo/agent", "version": "1.0.0"}
    try:
        RegisterAgentCardRequest.model_validate(payload)
    except ValidationError as exc:
        assert "card" in str(exc)
    else:
        raise AssertionError("Expected validation error")


def test_register_agent_card_writes_rows():
    conn = FakeConn()
    card = {
        "name": "Demo Agent",
        "description": "Does things.",
        "url": "https://example.com/a2a",
        "version": "1.0.0",
        "defaultInputModes": ["text"],
        "defaultOutputModes": ["text"],
        "capabilities": {"streaming": False},
        "skills": [{"id": "demo", "name": "Demo", "description": "Demo"}],
    }
    register_agent_card(
        conn,
        agent_id="demo/agent",
        name="Demo Agent",
        description="Does things.",
        owner="self-registered",
        status="active",
        version="1.0.0",
        mcp_server_url="https://example.com/mcp",
        llm_endpoint_name=None,
        system_prompt=None,
        tags={"source": "unit"},
        protocol="a2a",
        card_json=card,
    )
    calls = conn.cursor_obj.calls
    assert len(calls) == 3
    payloads = [params for _query, params in calls]
    assert payloads[0][0] == "demo/agent"
    assert payloads[1][0] == "demo/agent"
    assert json.loads(payloads[2][3])["name"] == "Demo Agent"
