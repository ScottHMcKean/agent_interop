from __future__ import annotations

import json
import logging
from typing import Any

from a2a.server.agent_execution import AgentExecutor, RequestContext
from a2a.server.events import EventQueue
from a2a.server.tasks import TaskUpdater
from a2a.types import TaskState, TextPart
from a2a.utils import new_agent_text_message, new_task
from a2a.utils.errors import ServerError
from a2a.types import InvalidParamsError, UnsupportedOperationError
import httpx
from databricks.sdk import WorkspaceClient
from registry_app.config import load_settings
from registry_app.db import get_connection
from registry_app.registry import (
    get_agent,
    get_agent_card,
    get_default_version,
    get_version,
    list_agents,
)


logger = logging.getLogger(__name__)


def _parse_json_payload(text: str) -> dict[str, Any] | None:
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return None
    return parsed if isinstance(parsed, dict) else None


def _build_agent_call_hint() -> str:
    return (
        "Send JSON text with keys: agent_id, input, optional version. "
        "Example: {'agent_id': 'genie', 'input': 'List top 3 distribution centers.'}"
    )


class RegistryAgentExecutor(AgentExecutor):
    def __init__(self) -> None:
        self.settings = load_settings()
        self.workspace_client = WorkspaceClient()

    async def execute(
        self,
        context: RequestContext,
        event_queue: EventQueue,
    ) -> None:
        user_input = context.get_user_input()
        if not user_input:
            raise ServerError(error=InvalidParamsError())

        task = context.current_task or new_task(context.message)
        await event_queue.enqueue_event(task)
        updater = TaskUpdater(event_queue, task.id, task.context_id)

        payload = _parse_json_payload(user_input)
        if not payload:
            await updater.update_status(
                TaskState.input_required,
                new_agent_text_message(_build_agent_call_hint(), task.context_id, task.id),
                final=True,
            )
            return

        action = payload.get("action")
        if action == "list_agents":
            response_text = self._handle_list_agents()
            await updater.add_artifact([TextPart(text=response_text)], name="agents")
            await updater.complete()
            return

        agent_id = payload.get("agent_id")
        if not agent_id:
            await updater.update_status(
                TaskState.input_required,
                new_agent_text_message(
                    "Missing agent_id. " + _build_agent_call_hint(),
                    task.context_id,
                    task.id,
                ),
                final=True,
            )
            return

        response_text = await self._handle_agent_call(payload)
        await updater.add_artifact([TextPart(text=response_text)], name="agent_result")
        await updater.complete()

    def _handle_list_agents(self) -> str:
        with get_connection() as conn:
            agents = list_agents(conn)
        return json.dumps({"agents": agents}, indent=2, default=str)

    async def _handle_agent_call(self, payload: dict[str, Any]) -> str:
        agent_id = str(payload["agent_id"])
        version = payload.get("version")
        prompt = payload.get("input", "")
        if not prompt:
            return "Missing input text."

        with get_connection() as conn:
            agent = get_agent(conn, agent_id)
            if not agent:
                return f"Unknown agent_id '{agent_id}'."
            agent_version = (
                get_version(conn, agent_id, str(version))
                if version
                else get_default_version(conn, agent_id)
            )
            card_version = agent_version["version"] if agent_version else None
            card = (
                get_agent_card(conn, agent_id, version=card_version, protocol="a2a")
                if card_version
                else None
            )

        if not agent_version:
            return f"No version data for '{agent_id}'."

        api_url = agent_version.get("api_url")
        if not api_url:
            return (
                f"Agent '{agent_id}' is missing api_url. "
                "Register an API endpoint to enable invocation."
            )

        headers = {}
        try:
            headers = self.workspace_client.config.authenticate()
        except Exception:
            headers = {}

        metadata = payload.get("metadata", {}) if isinstance(payload, dict) else {}
        request_payload = {
            "input": [{"role": "user", "content": prompt}],
            "metadata": metadata,
        }
        try:
            async with httpx.AsyncClient(timeout=60.0) as client:
                response = await client.post(api_url, json=request_payload, headers=headers)
                content_type = response.headers.get("content-type")
                if response.content:
                    try:
                        parsed = response.json()
                    except json.JSONDecodeError:
                        parsed = {"raw_text": response.text}
                else:
                    parsed = {"raw_text": "", "note": "Empty response body"}
                result = {
                    "request_type": "input_messages",
                    "request_body": request_payload,
                    "status_code": response.status_code,
                    "content_type": content_type,
                    "payload": parsed,
                }
        except Exception as exc:
            return json.dumps(
                {"agent": agent, "agent_card": card, "error": str(exc)},
                indent=2,
                default=str,
            )

        response = {
            "agent": agent,
            "agent_card": card,
            "result": result,
        }
        return json.dumps(response, indent=2, default=str)

    async def cancel(
        self, context: RequestContext, event_queue: EventQueue
    ) -> None:
        raise ServerError(error=UnsupportedOperationError())
