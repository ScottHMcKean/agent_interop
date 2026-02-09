from __future__ import annotations

import asyncio
import json
import os
from typing import Any, Callable, Iterable, Mapping

from registry_app.services.a2a_client import A2AClientProtocol, build_a2a_client
from registry_app.config import load_settings
from registry_app.db import get_connection
from registry_app.registry import get_agent_card, list_agent_cards


def _coerce_card_json(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return {"raw": value}
        return parsed if isinstance(parsed, dict) else {"raw": parsed}
    return {"raw": value}


def _extract_tags(card_json: Mapping[str, Any]) -> list[str]:
    tags = card_json.get("tags") or []
    return [str(tag) for tag in tags if isinstance(tag, str)]


def _extract_skills(card_json: Mapping[str, Any]) -> list[str]:
    skills = []
    for skill in card_json.get("skills", []) or []:
        if isinstance(skill, dict) and "id" in skill:
            skills.append(str(skill["id"]))
    return skills


def _matches_tags(tags: Iterable[str], card_tags: list[str]) -> bool:
    tags_set = {tag for tag in tags if tag}
    return tags_set.issubset(set(card_tags))


def _matches_skills(skills: Iterable[str], card_skills: list[str]) -> bool:
    skills_set = {skill for skill in skills if skill}
    if not skills_set:
        return True
    return bool(skills_set.intersection(set(card_skills)))


def _build_agent_summary(
    row: Mapping[str, Any], include_full_card: bool
) -> dict[str, Any]:
    card_json = _coerce_card_json(row.get("card_json"))
    summary = {
        "human_readable_id": row.get("agent_id")
        or card_json.get("humanReadableId")
        or card_json.get("human_readable_id"),
        "name": card_json.get("name", ""),
        "description": card_json.get("description", ""),
        "tags": _extract_tags(card_json),
        "a2a_url": card_json.get("url") or card_json.get("a2a_url") or "",
        "agent_version": row.get("version") or card_json.get("agentVersion"),
    }
    if include_full_card:
        summary["card"] = card_json
    return summary


def _list_available_agents(
    conn,
    *,
    tags: list[str] | None,
    skills: list[str] | None,
    limit: int,
    include_full_card: bool,
) -> dict[str, Any]:
    normalized_limit = max(1, min(100, int(limit)))
    rows = list_agent_cards(conn, protocol="a2a")
    agents = []
    for row in rows:
        card_json = _coerce_card_json(row.get("card_json"))
        card_tags = _extract_tags(card_json)
        card_skills = _extract_skills(card_json)
        if tags and not _matches_tags(tags, card_tags):
            continue
        if skills and not _matches_skills(skills, card_skills):
            continue
        agents.append(_build_agent_summary(row, include_full_card))
        if len(agents) >= normalized_limit:
            break
    return {"agents": agents}


def _auth_from_schemes(schemes: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    for scheme in schemes:
        scheme_name = str(scheme.get("scheme", "")).lower()
        if scheme_name in {"none", ""}:
            return {}
        if scheme_name == "apikey":
            api_key = os.getenv("A2A_API_KEY")
            if api_key:
                return {"headers": {"Authorization": f"Bearer {api_key}"}}
        if scheme_name in {"oauth2", "oauth"}:
            token = os.getenv("A2A_OAUTH_TOKEN")
            if token:
                return {"headers": {"Authorization": f"Bearer {token}"}}
    return {}


async def _invoke_agent(
    conn,
    *,
    agent_id: str,
    task: Mapping[str, Any],
    timeout_seconds: int,
    a2a_client_factory: Callable[..., A2AClientProtocol],
) -> dict[str, Any]:
    card_row = get_agent_card(conn, agent_id, protocol="a2a")
    if not card_row:
        return {
            "status": "error",
            "agent_id": agent_id,
            "error": {"message": f"Unknown agent_id: {agent_id}"},
        }
    card_json = _coerce_card_json(card_row.get("card_json"))
    a2a_url = card_json.get("url") or card_json.get("a2a_url")
    if not a2a_url:
        return {
            "status": "error",
            "agent_id": agent_id,
            "error": {"message": f"Missing a2a_url for agent_id: {agent_id}"},
        }
    if isinstance(a2a_url, str) and a2a_url.startswith("/"):
        settings = load_settings()
        base_url = settings.registry_base_url
        if not base_url:
            return {
                "status": "error",
                "agent_id": agent_id,
                "error": {
                    "message": "Relative a2a_url requires registry_base_url in config."
                },
            }
        a2a_url = base_url.rstrip("/") + a2a_url

    goal = task.get("goal")
    if not goal:
        return {
            "status": "error",
            "agent_id": agent_id,
            "error": {"message": "Missing task.goal"},
        }
    auth_config = _auth_from_schemes(card_json.get("authSchemes", []) or [])
    client: A2AClientProtocol = a2a_client_factory(
        base_url=a2a_url, auth_config=auth_config
    )
    try:
        result = await client.invoke_task(
            goal=str(goal),
            input=task.get("input", {}) or {},
            metadata=task.get("metadata", {}) or {},
            timeout=int(timeout_seconds),
        )
    except asyncio.TimeoutError:
        return {
            "status": "timeout",
            "agent_id": agent_id,
            "error": {"message": "A2A request timed out"},
        }
    except Exception as exc:  # pragma: no cover - defensive guard
        return {
            "status": "error",
            "agent_id": agent_id,
            "error": {"message": str(exc)},
        }
    return {"status": "success", "agent_id": agent_id, "result": result}


def register_gateway_tools(app) -> None:
    @app.tool(
        name="list_available_agents",
        description=(
            "List A2A agents registered in the Lakehouse, "
            "optionally filtered by tags or skills."
        ),
    )
    async def list_available_agents_tool(
        tags: list[str] | None = None,
        skills: list[str] | None = None,
        limit: int = 20,
        include_full_card: bool = False,
    ) -> dict[str, Any]:
        with get_connection() as conn:
            return _list_available_agents(
                conn,
                tags=tags,
                skills=skills,
                limit=limit,
                include_full_card=include_full_card,
            )

    @app.tool(
        name="invoke_agent",
        description=(
            "Invoke a registered A2A agent by ID with a structured task payload."
        ),
    )
    async def invoke_agent_tool(
        agent_id: str, task: dict[str, Any], timeout_seconds: int = 60
    ) -> dict[str, Any]:
        with get_connection() as conn:
            return await _invoke_agent(
                conn,
                agent_id=agent_id,
                task=task,
                timeout_seconds=timeout_seconds,
                a2a_client_factory=build_a2a_client,
            )
