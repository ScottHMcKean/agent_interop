from __future__ import annotations

import asyncio
import json

from registry_app.services.mcp_gateway import _invoke_agent, _list_available_agents


class FakeCursor:
    def __init__(self, rows):
        self._rows = rows
        self._iter = iter(rows)

    def execute(self, _query, _params=None):
        self._iter = iter(self._rows)

    def fetchall(self):
        return list(self._rows)

    def fetchone(self):
        return next(self._iter, None)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class FakeConn:
    def __init__(self, rows):
        self._rows = rows

    def cursor(self):
        return FakeCursor(self._rows)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


def test_list_available_agents_filters_and_limits():
    rows = [
        {
            "agent_id": "agent-1",
            "version": 1,
            "protocol": "a2a",
            "card_json": {
                "name": "Agent One",
                "description": "First agent",
                "url": "https://example.com/a2a",
                "tags": ["alpha", "beta"],
                "skills": [{"id": "skill-1"}],
                "agentVersion": 1,
            },
        },
        {
            "agent_id": "agent-2",
            "version": 1,
            "protocol": "a2a",
            "card_json": {
                "name": "Agent Two",
                "description": "Second agent",
                "url": "https://example.com/a2a",
                "tags": ["beta"],
                "skills": [{"id": "skill-2"}],
                "agentVersion": 1,
            },
        },
    ]
    conn = FakeConn(rows)
    result = _list_available_agents(
        conn,
        tags=["alpha"],
        skills=["skill-1"],
        limit=5,
        include_full_card=False,
        list_all_versions=False,
    )
    assert len(result["agents"]) == 1
    assert result["agents"][0]["human_readable_id"] == "agent-1"


def test_list_available_agents_include_full_card():
    rows = [
        {
            "agent_id": "agent-1",
            "version": 1,
            "protocol": "a2a",
            "card_json": json.dumps(
                {
                    "name": "Agent One",
                    "description": "First agent",
                    "url": "https://example.com/a2a",
                    "tags": ["alpha"],
                    "agentVersion": 1,
                }
            ),
        }
    ]
    conn = FakeConn(rows)
    result = _list_available_agents(
        conn,
        tags=None,
        skills=None,
        limit=1,
        include_full_card=True,
        list_all_versions=False,
    )
    assert "card" in result["agents"][0]
    assert result["agents"][0]["card"]["name"] == "Agent One"


def test_list_available_agents_latest_only_by_default():
    rows = [
        {
            "agent_id": "agent-1",
            "version": 2,
            "protocol": "a2a",
            "card_json": {
                "name": "Agent One",
                "description": "Second version",
                "url": "https://example.com/a2a",
                "agentVersion": 2,
            },
        },
        {
            "agent_id": "agent-1",
            "version": 1,
            "protocol": "a2a",
            "card_json": {
                "name": "Agent One",
                "description": "First version",
                "url": "https://example.com/a2a",
                "agentVersion": 1,
            },
        },
    ]
    conn = FakeConn(rows)
    result = _list_available_agents(
        conn,
        tags=None,
        skills=None,
        limit=10,
        include_full_card=False,
        list_all_versions=False,
    )
    assert len(result["agents"]) == 1
    assert result["agents"][0]["agent_version"] == 2


def test_list_available_agents_all_versions_when_requested():
    rows = [
        {
            "agent_id": "agent-1",
            "version": 2,
            "protocol": "a2a",
            "card_json": {"name": "Agent One", "url": "https://example.com/a2a"},
        },
        {
            "agent_id": "agent-1",
            "version": 1,
            "protocol": "a2a",
            "card_json": {"name": "Agent One", "url": "https://example.com/a2a"},
        },
    ]
    conn = FakeConn(rows)
    result = _list_available_agents(
        conn,
        tags=None,
        skills=None,
        limit=10,
        include_full_card=False,
        list_all_versions=True,
    )
    assert len(result["agents"]) == 2


def test_invoke_agent_success():
    rows = [
        {
            "agent_id": "agent-1",
            "version": 1,
            "protocol": "a2a",
            "card_json": {
                "name": "Agent One",
                "description": "First agent",
                "url": "https://example.com/a2a",
                "agentVersion": 1,
                "authSchemes": [{"scheme": "none"}],
            },
        }
    ]
    conn = FakeConn(rows)
    seen = {}

    class FakeA2AClient:
        async def invoke_task(self, *, goal, input, metadata, timeout):
            seen["goal"] = goal
            seen["input"] = input
            seen["metadata"] = metadata
            seen["timeout"] = timeout
            return {"answer": "ok"}

    def factory(*, base_url, auth_config):
        seen["base_url"] = base_url
        seen["auth_config"] = auth_config
        return FakeA2AClient()

    result = asyncio.run(
        _invoke_agent(
            conn,
            agent_id="agent-1",
            task={"goal": "test", "input": {"a": 1}, "metadata": {"b": 2}},
            timeout_seconds=10,
            a2a_client_factory=factory,
        )
    )
    assert result["status"] == "success"
    assert result["result"] == {"answer": "ok"}
    assert seen["base_url"] == "https://example.com/a2a"
    assert seen["goal"] == "test"
    assert seen["timeout"] == 10


def test_invoke_agent_unknown():
    conn = FakeConn([])
    result = asyncio.run(
        _invoke_agent(
            conn,
            agent_id="missing",
            task={"goal": "test"},
            timeout_seconds=10,
            a2a_client_factory=lambda **_: None,
        )
    )
    assert result["status"] == "error"
    assert result["agent_id"] == "missing"
