from __future__ import annotations

import argparse
import json
from pathlib import Path

from registry_app.db import get_connection
from registry_app.registry import register_agent_card
from registry_app.schemas import RegisterAgentCardRequest


def _load_card(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def main() -> None:
    parser = argparse.ArgumentParser(description="Register an A2A agent card.")
    parser.add_argument("--card", required=True, help="Path to agent card JSON.")
    parser.add_argument("--agent-id", required=True)
    parser.add_argument("--version", required=True)
    parser.add_argument("--mcp-server-url", required=True)
    parser.add_argument("--owner", default="self-registered")
    parser.add_argument("--status", default="active")
    parser.add_argument("--protocol", default="a2a")
    parser.add_argument("--tags", default="{}")
    args = parser.parse_args()

    payload = {
        "agent_id": args.agent_id,
        "version": args.version,
        "mcp_server_url": args.mcp_server_url,
        "owner": args.owner,
        "status": args.status,
        "protocol": args.protocol,
        "tags": json.loads(args.tags),
        "card": _load_card(Path(args.card)),
    }
    request = RegisterAgentCardRequest.model_validate(payload)

    with get_connection() as conn:
        register_agent_card(
            conn,
            agent_id=request.agent_id,
            name=request.card.name,
            description=request.card.description,
            owner=request.owner,
            status=request.status,
            version=request.version,
            mcp_server_url=request.mcp_server_url,
            tags=request.tags,
            protocol=request.protocol,
            card_json=request.card.model_dump(),
        )
        conn.commit()


if __name__ == "__main__":
    main()
