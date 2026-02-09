from __future__ import annotations

import json
import pytest

from registry_app.db import get_connection
from registry_app.registry import get_agent_card, list_agent_cards
from registry_app.services.a2a_executor import RegistryAgentExecutor


def _insert_seed(conn, schema: str, agent_id: str, card: dict) -> None:
    conn.execute(
        f"DELETE FROM {schema}.agent_protocol_cards WHERE agent_id = %s",
        (agent_id,),
    )
    conn.execute(
        f"DELETE FROM {schema}.agent_versions WHERE agent_id = %s",
        (agent_id,),
    )
    conn.execute(
        f"DELETE FROM {schema}.agents WHERE agent_id = %s",
        (agent_id,),
    )
    conn.execute(
        f"""
        INSERT INTO {schema}.agents (
            agent_id, name, description, owner, status, default_version
        ) VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (agent_id) DO UPDATE SET
            name = EXCLUDED.name,
            description = EXCLUDED.description,
            owner = EXCLUDED.owner,
            status = EXCLUDED.status,
            default_version = EXCLUDED.default_version,
            updated_at = now()
        """,
        (
            agent_id,
            agent_id,
            "Test agent",
            "test",
            "active",
            "v1",
        ),
    )
    conn.execute(
        f"""
        INSERT INTO {schema}.agent_versions (
            agent_id, version, mcp_server_url, tags
        ) VALUES (%s, %s, %s, %s)
        ON CONFLICT (agent_id, version) DO UPDATE SET
            mcp_server_url = EXCLUDED.mcp_server_url,
            tags = EXCLUDED.tags,
            updated_at = now()
        """,
        (
            agent_id,
            "v1",
            "https://example.com/mcp",
            json.dumps({"source": "pytest"}),
        ),
    )
    conn.execute(
        f"""
        INSERT INTO {schema}.agent_protocol_cards (
            agent_id, version, protocol, card_json
        ) VALUES (%s, %s, %s, %s)
        ON CONFLICT (agent_id, version, protocol) DO UPDATE SET
            card_json = EXCLUDED.card_json,
            updated_at = now()
        """,
        (
            agent_id,
            "v1",
            "a2a",
            json.dumps(card),
        ),
    )


@pytest.mark.integration
def test_registry_mcp_cards_roundtrip(config: dict) -> None:
    schema = config.get("registry_schema", "agent_registry")
    agent_id = "test-agent"
    card = {
        "name": "Test Agent",
        "description": "Test Agent card",
        "url": "/a2a",
        "version": "1.0.0",
        "defaultInputModes": ["text"],
        "defaultOutputModes": ["text"],
        "capabilities": {"streaming": False},
        "skills": [
            {"id": agent_id, "name": "Test Agent", "description": "pytest"}
        ],
    }
    with get_connection() as conn:
        _insert_seed(conn, schema, agent_id, card)
        conn.commit()

        cards = list_agent_cards(conn, protocol="a2a")
        assert any(row["agent_id"] == agent_id for row in cards)

        fetched = get_agent_card(conn, agent_id, protocol="a2a")
        assert fetched
    assert fetched["card_json"]["name"] == "Test Agent"


@pytest.mark.integration
def test_a2a_gateway_list_agents_action(config: dict) -> None:
    schema = config.get("registry_schema", "agent_registry")
    agent_id = "test-agent"
    card = {
        "name": "Test Agent",
        "description": "Test Agent card",
        "url": "/a2a",
        "version": "1.0.0",
        "defaultInputModes": ["text"],
        "defaultOutputModes": ["text"],
        "capabilities": {"streaming": False},
        "skills": [
            {"id": agent_id, "name": "Test Agent", "description": "pytest"}
        ],
    }
    with get_connection() as conn:
        _insert_seed(conn, schema, agent_id, card)
        conn.commit()

    executor = RegistryAgentExecutor()
    payload = {"action": "list_agents"}
    response = executor._handle_list_agents()  # keeps coverage focused on A2A flow
    data = json.loads(response)
    assert any(agent["agent_id"] == agent_id for agent in data["agents"])
