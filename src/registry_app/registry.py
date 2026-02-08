from __future__ import annotations

import json
from typing import Any, Iterable

from psycopg import sql

from registry_app.config import load_settings


def _table(name: str) -> sql.Composed:
    schema = load_settings().registry_schema
    return sql.SQL(".").join([sql.Identifier(schema), sql.Identifier(name)])


def list_agents(conn) -> list[dict[str, Any]]:
    query = sql.SQL(
        "SELECT agent_id, name, description, owner, status, default_version, "
        "created_at, updated_at "
        "FROM {} ORDER BY agent_id"
    ).format(_table("agents"))
    with conn.cursor() as cur:
        cur.execute(query)
        return list(cur.fetchall())


def get_agent(conn, agent_id: str) -> dict[str, Any] | None:
    query = sql.SQL(
        "SELECT agent_id, name, description, owner, status, default_version, "
        "created_at, updated_at "
        "FROM {} WHERE agent_id = %s"
    ).format(_table("agents"))
    with conn.cursor() as cur:
        cur.execute(query, (agent_id,))
        return cur.fetchone()


def list_versions(conn, agent_id: str) -> list[dict[str, Any]]:
    query = sql.SQL(
        "SELECT agent_id, version, mcp_server_url, llm_endpoint_name, "
        "system_prompt, tags, created_at, updated_at "
        "FROM {} WHERE agent_id = %s ORDER BY version"
    ).format(_table("agent_versions"))
    with conn.cursor() as cur:
        cur.execute(query, (agent_id,))
        return list(cur.fetchall())


def get_version(
    conn, agent_id: str, version: str
) -> dict[str, Any] | None:
    query = sql.SQL(
        "SELECT agent_id, version, mcp_server_url, llm_endpoint_name, "
        "system_prompt, tags, created_at, updated_at "
        "FROM {} WHERE agent_id = %s AND version = %s"
    ).format(_table("agent_versions"))
    with conn.cursor() as cur:
        cur.execute(query, (agent_id, version))
        return cur.fetchone()


def get_default_version(conn, agent_id: str) -> dict[str, Any] | None:
    agent = get_agent(conn, agent_id)
    if not agent:
        return None
    default_version = agent.get("default_version")
    if default_version:
        return get_version(conn, agent_id, default_version)
    versions = list_versions(conn, agent_id)
    return versions[-1] if versions else None


def list_agent_cards(
    conn, protocol: str = "a2a"
) -> list[dict[str, Any]]:
    query = sql.SQL(
        "SELECT agent_id, version, protocol, card_json, updated_at "
        "FROM {} WHERE protocol = %s ORDER BY agent_id, version"
    ).format(_table("agent_protocol_cards"))
    with conn.cursor() as cur:
        cur.execute(query, (protocol,))
        return list(cur.fetchall())


def get_agent_card(
    conn,
    agent_id: str,
    version: str | None = None,
    protocol: str = "a2a",
) -> dict[str, Any] | None:
    if version:
        query = sql.SQL(
            "SELECT agent_id, version, protocol, card_json, updated_at "
            "FROM {} WHERE agent_id = %s AND version = %s AND protocol = %s"
        ).format(_table("agent_protocol_cards"))
        params: Iterable[Any] = (agent_id, version, protocol)
    else:
        query = sql.SQL(
            "SELECT agent_id, version, protocol, card_json, updated_at "
            "FROM {} WHERE agent_id = %s AND protocol = %s "
            "ORDER BY version DESC LIMIT 1"
        ).format(_table("agent_protocol_cards"))
        params = (agent_id, protocol)
    with conn.cursor() as cur:
        cur.execute(query, params)
        row = cur.fetchone()
    if not row:
        return None
    card_json = row.get("card_json")
    if isinstance(card_json, str):
        try:
            card_json = json.loads(card_json)
        except json.JSONDecodeError:
            card_json = {"raw": card_json}
    row["card_json"] = card_json
    return row


def upsert_agent(
    conn,
    *,
    agent_id: str,
    name: str,
    description: str,
    owner: str,
    status: str,
    default_version: str,
) -> None:
    query = sql.SQL(
        "INSERT INTO {} (agent_id, name, description, owner, status, default_version) "
        "VALUES (%s, %s, %s, %s, %s, %s) "
        "ON CONFLICT (agent_id) DO UPDATE SET "
        "name = EXCLUDED.name, "
        "description = EXCLUDED.description, "
        "owner = EXCLUDED.owner, "
        "status = EXCLUDED.status, "
        "default_version = EXCLUDED.default_version, "
        "updated_at = now()"
    ).format(_table("agents"))
    with conn.cursor() as cur:
        cur.execute(
            query,
            (agent_id, name, description, owner, status, default_version),
        )


def upsert_agent_version(
    conn,
    *,
    agent_id: str,
    version: str,
    mcp_server_url: str,
    llm_endpoint_name: str | None,
    system_prompt: str | None,
    tags: dict[str, Any] | None,
) -> None:
    query = sql.SQL(
        "INSERT INTO {} "
        "(agent_id, version, mcp_server_url, llm_endpoint_name, system_prompt, tags) "
        "VALUES (%s, %s, %s, %s, %s, %s) "
        "ON CONFLICT (agent_id, version) DO UPDATE SET "
        "mcp_server_url = EXCLUDED.mcp_server_url, "
        "llm_endpoint_name = EXCLUDED.llm_endpoint_name, "
        "system_prompt = EXCLUDED.system_prompt, "
        "tags = EXCLUDED.tags, "
        "updated_at = now()"
    ).format(_table("agent_versions"))
    with conn.cursor() as cur:
        cur.execute(
            query,
            (
                agent_id,
                version,
                mcp_server_url,
                llm_endpoint_name,
                system_prompt,
                json.dumps(tags or {}),
            ),
        )


def upsert_agent_protocol_card(
    conn,
    *,
    agent_id: str,
    version: str,
    protocol: str,
    card_json: dict[str, Any],
) -> None:
    query = sql.SQL(
        "INSERT INTO {} (agent_id, version, protocol, card_json) "
        "VALUES (%s, %s, %s, %s) "
        "ON CONFLICT (agent_id, version, protocol) DO UPDATE SET "
        "card_json = EXCLUDED.card_json, "
        "updated_at = now()"
    ).format(_table("agent_protocol_cards"))
    with conn.cursor() as cur:
        cur.execute(
            query,
            (
                agent_id,
                version,
                protocol,
                json.dumps(card_json),
            ),
        )


def register_agent_card(
    conn,
    *,
    agent_id: str,
    name: str,
    description: str,
    owner: str,
    status: str,
    version: str,
    mcp_server_url: str,
    llm_endpoint_name: str | None,
    system_prompt: str | None,
    tags: dict[str, Any] | None,
    protocol: str,
    card_json: dict[str, Any],
) -> None:
    upsert_agent(
        conn,
        agent_id=agent_id,
        name=name,
        description=description,
        owner=owner,
        status=status,
        default_version=version,
    )
    upsert_agent_version(
        conn,
        agent_id=agent_id,
        version=version,
        mcp_server_url=mcp_server_url,
        llm_endpoint_name=llm_endpoint_name,
        system_prompt=system_prompt,
        tags=tags,
    )
    upsert_agent_protocol_card(
        conn,
        agent_id=agent_id,
        version=version,
        protocol=protocol,
        card_json=card_json,
    )
