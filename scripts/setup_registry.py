from __future__ import annotations

import json
import re
from pathlib import Path
from urllib.parse import quote_plus

import psycopg
import yaml
from databricks.connect import DatabricksSession
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.database import DatabaseInstance


def _load_config() -> tuple[Path, dict]:
    config_path = Path("config.yaml")
    if not config_path.exists():
        raise RuntimeError("Missing config.yaml in repo root.")
    config = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    if not isinstance(config, dict):
        raise RuntimeError("config.yaml must be a flat dictionary")
    return config_path, config


def _slugify(value: str) -> str:
    value = value.strip().lower()
    value = re.sub(r"[^a-z0-9]+", "-", value)
    return value.strip("-") or "agent"


def _ensure_instance(w: WorkspaceClient, name: str, capacity: str, retention: int):
    for inst in w.database.list_database_instances():
        if inst.name == name:
            return inst
    w.database.create_database_instance(
        DatabaseInstance(
            name=name,
            capacity=capacity,
            retention_window_in_days=retention,
        )
    )
    for inst in w.database.list_database_instances():
        if inst.name == name:
            return inst
    raise RuntimeError("Lakebase instance not found after create.")


def _build_spark_session() -> DatabricksSession:
    return DatabricksSession.builder.serverless().getOrCreate()


def _build_lakebase_dsn(
    w: WorkspaceClient,
    host: str | None,
    db_name: str,
    user: str | None,
) -> str:
    token = w.config.oauth_token().access_token
    user = user or w.current_user.me().user_name
    if not host:
        raise RuntimeError("Missing lakebase_host in config.")
    if ":" in host:
        host, port = host.split(":", 1)
    else:
        port = "5432"
    return (
        "postgresql://"
        f"{quote_plus(user)}:{quote_plus(token)}@{host}:{port}/{db_name}"
        "?sslmode=require"
    )


def main() -> None:
    print("Loading config.yaml...")
    config_path, config = _load_config()

    catalog = config.get("catalog", "shm")
    registry_schema = config.get("registry_schema", "agent_registry")
    instance_name = config.get("lakebase_instance_name", "agent-registry")
    capacity = config.get("lakebase_capacity", "CU_1")
    retention = int(config.get("lakebase_retention_days", 7))
    lakebase_host = config.get("lakebase_host")
    lakebase_db = config.get("lakebase_db", "databricks_postgres")
    default_mcp_server_url = config.get("default_mcp_server_url")
    a2a_base_url = config.get("a2a_base_url", "/api/a2a")

    w = WorkspaceClient()
    print("Ensuring Lakebase instance...")
    instance = _ensure_instance(w, instance_name, capacity, retention)
    print(f"Using instance: {instance.name}")
    print(f"Lakebase endpoint: {instance.read_write_dns}")

    if not lakebase_host:
        lakebase_host = instance.read_write_dns

    print("Ensuring UC catalog/schema via Databricks Connect...")
    spark = _build_spark_session()
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{registry_schema}")

    print("Creating registry tables in Lakebase...")
    lakebase_dsn = config.get("lakebase_dsn") or _build_lakebase_dsn(
        w, lakebase_host, lakebase_db, lakebase_user
    )

    create_sql = f"""
CREATE SCHEMA IF NOT EXISTS {registry_schema};

CREATE TABLE IF NOT EXISTS {registry_schema}.agents (
    agent_id TEXT PRIMARY KEY,
    name TEXT,
    description TEXT,
    owner TEXT,
    status TEXT,
    default_version TEXT,
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS {registry_schema}.agent_versions (
    agent_id TEXT NOT NULL,
    version TEXT NOT NULL,
    mcp_server_url TEXT,
    llm_endpoint_name TEXT,
    system_prompt TEXT,
    tags JSONB,
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (agent_id, version),
    FOREIGN KEY (agent_id) REFERENCES {registry_schema}.agents(agent_id)
);

CREATE TABLE IF NOT EXISTS {registry_schema}.agent_protocol_cards (
    agent_id TEXT NOT NULL,
    version TEXT NOT NULL,
    protocol TEXT NOT NULL,
    card_json JSONB,
    updated_at TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (agent_id, version, protocol),
    FOREIGN KEY (agent_id, version) REFERENCES {registry_schema}.agent_versions(agent_id, version)
);
"""

    with psycopg.connect(lakebase_dsn) as conn:
        conn.execute(create_sql)
        conn.commit()

    config["lakebase_host"] = lakebase_host
    config["lakebase_db"] = lakebase_db
    config["registry_schema"] = registry_schema
    config_path.write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")
    print("Updated config.yaml with lakebase_host and lakebase_db.")

    print("Discovering serving endpoints...")
    endpoints = list(w.serving_endpoints.list())
    agents_seed = []
    if endpoints:
        for endpoint in endpoints:
            name = endpoint.name
            agents_seed.append(
                {
                    "agent_id": _slugify(name),
                    "name": name,
                    "description": f"Serving endpoint {name}",
                    "owner": getattr(endpoint, "creator", None),
                    "status": getattr(endpoint, "state", None),
                    "version": "v1",
                    "mcp_server_url": default_mcp_server_url,
                    "llm_endpoint_name": name,
                    "system_prompt": config.get("default_system_prompt"),
                    "card": {
                        "name": name,
                        "description": f"A2A wrapper for {name}",
                        "url": a2a_base_url,
                        "version": "1.0.0",
                        "defaultInputModes": ["text"],
                        "defaultOutputModes": ["text"],
                        "capabilities": {"streaming": False},
                        "skills": [
                            {
                                "id": _slugify(name),
                                "name": name,
                                "description": f"Gateway to {name}",
                                "tags": ["serving-endpoint"],
                                "examples": ["Example request"],
                            }
                        ],
                    },
                }
            )
    else:
        agents_seed.append(
            {
                "agent_id": "example-agent",
                "name": "example-agent",
                "description": "Example agent (no serving endpoints discovered)",
                "owner": None,
                "status": "unknown",
                "version": "v1",
                "mcp_server_url": default_mcp_server_url,
                "llm_endpoint_name": config.get("default_llm_endpoint"),
                "system_prompt": config.get("default_system_prompt"),
                "card": {
                    "name": "example-agent",
                    "description": "Example A2A agent card",
                    "url": a2a_base_url,
                    "version": "1.0.0",
                    "defaultInputModes": ["text"],
                    "defaultOutputModes": ["text"],
                    "capabilities": {"streaming": False},
                    "skills": [
                        {
                            "id": "example-agent",
                            "name": "example-agent",
                            "description": "Example skill",
                            "tags": ["example"],
                            "examples": ["Example request"],
                        }
                    ],
                },
            }
        )

    print(f"Seeding {len(agents_seed)} agents...")
    insert_agents = f"""
INSERT INTO {registry_schema}.agents (
    agent_id, name, description, owner, status, default_version
) VALUES (
    %(agent_id)s, %(name)s, %(description)s, %(owner)s, %(status)s, %(version)s
)
ON CONFLICT (agent_id) DO UPDATE SET
    name = EXCLUDED.name,
    description = EXCLUDED.description,
    owner = EXCLUDED.owner,
    status = EXCLUDED.status,
    default_version = EXCLUDED.default_version,
    updated_at = now();
"""

    insert_versions = f"""
INSERT INTO {registry_schema}.agent_versions (
    agent_id, version, mcp_server_url, llm_endpoint_name, system_prompt, tags
) VALUES (
    %(agent_id)s, %(version)s, %(mcp_server_url)s, %(llm_endpoint_name)s,
    %(system_prompt)s, %(tags)s
)
ON CONFLICT (agent_id, version) DO UPDATE SET
    mcp_server_url = EXCLUDED.mcp_server_url,
    llm_endpoint_name = EXCLUDED.llm_endpoint_name,
    system_prompt = EXCLUDED.system_prompt,
    tags = EXCLUDED.tags,
    updated_at = now();
"""

    insert_cards = f"""
INSERT INTO {registry_schema}.agent_protocol_cards (
    agent_id, version, protocol, card_json
) VALUES (
    %(agent_id)s, %(version)s, %(protocol)s, %(card_json)s
)
ON CONFLICT (agent_id, version, protocol) DO UPDATE SET
    card_json = EXCLUDED.card_json,
    updated_at = now();
"""

    with psycopg.connect(lakebase_dsn) as conn:
        for agent in agents_seed:
            payload = {
                "agent_id": agent["agent_id"],
                "name": agent["name"],
                "description": agent["description"],
                "owner": agent["owner"],
                "status": agent["status"],
                "version": agent["version"],
                "mcp_server_url": agent["mcp_server_url"],
                "llm_endpoint_name": agent["llm_endpoint_name"],
                "system_prompt": agent["system_prompt"],
                "tags": json.dumps({"source": "serving_endpoint"}),
                "protocol": "a2a",
                "card_json": json.dumps(agent["card"]),
            }
            conn.execute(insert_agents, payload)
            conn.execute(insert_versions, payload)
            conn.execute(insert_cards, payload)
        conn.commit()

        agents_count = conn.execute(
            f"SELECT count(*) FROM {registry_schema}.agents"
        ).fetchone()[0]
        versions_count = conn.execute(
            f"SELECT count(*) FROM {registry_schema}.agent_versions"
        ).fetchone()[0]
        cards_count = conn.execute(
            f"SELECT count(*) FROM {registry_schema}.agent_protocol_cards"
        ).fetchone()[0]

    print("Row counts:")
    print("agents:", agents_count)
    print("agent_versions:", versions_count)
    print("agent_protocol_cards:", cards_count)


if __name__ == "__main__":
    main()
