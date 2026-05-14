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
from registry_app.loopback import make_async_client
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


def _extract_openai_messages(payload: dict[str, Any]) -> list[dict[str, Any]]:
    output = payload.get("output", [])
    messages: list[dict[str, Any]] = []
    for item in output:
        if item.get("type") != "message":
            continue
        content = item.get("content", [])
        text_chunks = [
            part.get("text")
            for part in content
            if part.get("type") in {"output_text", "text"} and part.get("text")
        ]
        if not text_chunks:
            continue
        messages.append(
            {
                "role": item.get("role", "assistant"),
                "text": "\n".join(text_chunks),
                "id": item.get("id"),
            }
        )
    return messages


def _coerce_message_text(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    if isinstance(value, list):
        flattened: list[str] = []
        for item in value:
            if isinstance(item, str):
                flattened.append(item)
            elif isinstance(item, dict) and isinstance(item.get("text"), str):
                flattened.append(item["text"])
        return flattened
    if isinstance(value, dict) and isinstance(value.get("text"), str):
        return [value["text"]]
    return []


def _get_by_path(payload: dict[str, Any], path: str) -> Any:
    current: Any = payload
    for part in path.split("."):
        if isinstance(current, list):
            if part.isdigit():
                idx = int(part)
                current = current[idx] if idx < len(current) else None
            else:
                return None
        elif isinstance(current, dict):
            current = current.get(part)
        else:
            return None
    return current


def _extract_a2a_messages(payload: dict[str, Any]) -> list[dict[str, Any]]:
    result = payload.get("result")
    if not isinstance(result, dict):
        return []
    texts: list[str] = []
    for artifact in result.get("artifacts", []) or []:
        if not isinstance(artifact, dict):
            continue
        for part in artifact.get("parts", []) or []:
            if isinstance(part, dict) and isinstance(part.get("text"), str):
                texts.append(part["text"])
    if not texts:
        return []
    return [{"role": "assistant", "text": "\n".join(texts)}]


def _extract_tagged_messages(
    payload: dict[str, Any], tags: dict[str, Any] | None
) -> list[dict[str, Any]]:
    if not isinstance(tags, dict):
        return []
    protocol = tags.get("api_protocol") or tags.get("protocol") or tags.get("response_format")
    if protocol == "openai":
        return _extract_openai_messages(payload)
    if protocol == "a2a":
        return _extract_a2a_messages(payload)
    message_path = tags.get("message_path")
    if isinstance(message_path, str) and message_path:
        value = _get_by_path(payload, message_path)
        texts = _coerce_message_text(value)
        if texts:
            return [{"role": "assistant", "text": "\n".join(texts)}]
    return []


def _extract_messages(payload: dict[str, Any], tags: dict[str, Any] | None) -> list[dict[str, Any]]:
    return _extract_tagged_messages(payload, tags)


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
            tags = agent_version.get("tags") if agent_version else None

        if not agent_version:
            return f"No version data for '{agent_id}'."

        api_url = agent_version.get("api_url")
        if not api_url:
            return (
                f"Agent '{agent_id}' is missing api_url. "
                "Register an API endpoint to enable invocation."
            )
        if isinstance(api_url, str) and api_url.startswith("/"):
            base_url = self.settings.registry_base_url or ""
            if base_url:
                api_url = f"{base_url.rstrip('/')}{api_url}"

        headers = {}
        try:
            headers = self.workspace_client.config.authenticate()
        except Exception:
            headers = {}

        metadata = payload.get("metadata", {}) if isinstance(payload, dict) else {}
        api_protocol = None
        if isinstance(tags, dict):
            api_protocol = (
                tags.get("api_protocol") or tags.get("protocol") or tags.get("response_format")
            )
        if api_protocol == "a2a":
            request_payload = {
                "jsonrpc": "2.0",
                "id": f"gateway-{agent_id}",
                "method": "message/send",
                "params": {
                    "message": {
                        "role": "user",
                        "kind": "message",
                        "messageId": f"gateway-{agent_id}",
                        "parts": [{"kind": "text", "text": prompt}],
                    },
                    "metadata": metadata,
                },
            }
            request_type = "a2a_jsonrpc"
        else:
            request_payload = {
                "input": [{"role": "user", "content": prompt}],
                "metadata": metadata,
            }
            request_type = "input_messages"
        base_url = (self.settings.registry_base_url or "").rstrip("/")
        client, request_url = make_async_client(api_url, base_url, timeout=60.0)
        try:
            async with client:
                response = await client.post(request_url, json=request_payload, headers=headers)
                content_type = response.headers.get("content-type")
                if response.content:
                    try:
                        parsed = response.json()
                    except json.JSONDecodeError:
                        parsed = {"raw_text": response.text}
                else:
                    parsed = {"raw_text": "", "note": "Empty response body"}
                result = {
                    "request_type": request_type,
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

        messages = []
        if isinstance(result.get("payload"), dict):
            messages = _extract_messages(result["payload"], tags if isinstance(tags, dict) else None)

        response = {
            "agent": agent,
            "agent_card": card,
            "result": result,
            "messages": messages,
        }
        return json.dumps(response, indent=2, default=str)

    async def cancel(
        self, context: RequestContext, event_queue: EventQueue
    ) -> None:
        raise ServerError(error=UnsupportedOperationError())
