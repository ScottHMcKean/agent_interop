from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from a2a.server.agent_execution import AgentExecutor, RequestContext
from a2a.server.apps import A2AStarletteApplication
from a2a.server.events import EventQueue
from a2a.server.request_handlers import DefaultRequestHandler
from a2a.server.tasks import InMemoryTaskStore, TaskUpdater
from a2a.types import AgentCapabilities, AgentCard, AgentSkill, TextPart
from a2a.types import InvalidParamsError, UnsupportedOperationError
from a2a.utils import new_task
from a2a.utils.errors import ServerError
from starlette.applications import Starlette

_HISTORY: list[dict[str, Any]] = []


def get_test_agent_history() -> list[dict[str, Any]]:
    return list(_HISTORY)


class TestAgentExecutor(AgentExecutor):
    async def execute(
        self, context: RequestContext, event_queue: EventQueue
    ) -> None:
        user_input = context.get_user_input()
        if not user_input:
            raise ServerError(error=InvalidParamsError())

        task = context.current_task or new_task(context.message)
        await event_queue.enqueue_event(task)
        updater = TaskUpdater(event_queue, task.id, task.context_id)

        message_id = getattr(
            context.message,
            "message_id",
            getattr(context.message, "messageId", None),
        )
        record = {
            "received_at": datetime.now(timezone.utc).isoformat(),
            "context_id": task.context_id,
            "task_id": task.id,
            "message_id": message_id,
            "input_text": user_input,
        }
        _HISTORY.append(record)

        response_payload = {
            "message": "hello world from Test Agent",
            "handshake": {
                "context_id": task.context_id,
                "task_id": task.id,
                "message_id": message_id,
            },
            "request": {"input_text": user_input},
            "history_size": len(_HISTORY),
        }

        await updater.add_artifact(
            [TextPart(text=json.dumps(response_payload, indent=2))],
            name="test_agent_response",
        )
        await updater.complete()

    async def cancel(
        self, context: RequestContext, event_queue: EventQueue
    ) -> None:
        raise ServerError(error=UnsupportedOperationError())


def build_test_agent_card() -> AgentCard:
    skill = AgentSkill(
        id="test_agent",
        name="Test Agent",
        description="Returns a hello-world response with handshake details.",
        tags=["test", "a2a"],
        examples=["{\"action\": \"list_agents\"}"],
    )
    return AgentCard(
        name="Test Agent",
        description="Local Test Agent for testing handshake and payloads.",
        url="/test-agent",
        version="0.1.0",
        defaultInputModes=["text"],
        defaultOutputModes=["text"],
        capabilities=AgentCapabilities(streaming=False),
        skills=[skill],
        supportsAuthenticatedExtendedCard=False,
    )


def build_test_agent_app() -> Starlette:
    request_handler = DefaultRequestHandler(
        agent_executor=TestAgentExecutor(),
        task_store=InMemoryTaskStore(),
    )
    return A2AStarletteApplication(
        agent_card=build_test_agent_card(),
        http_handler=request_handler,
    ).build(rpc_url="/")
