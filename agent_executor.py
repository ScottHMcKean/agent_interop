from a2a.server.agent_execution import AgentExecutor, RequestContext
from a2a.server.events import EventQueue
from a2a.utils import new_agent_text_message
import json
from contextlib import asynccontextmanager
import json
import uuid
import asyncio
from typing import Any, Callable, List
from pydantic import BaseModel

import mlflow
from mlflow.pyfunc import ResponsesAgent
from mlflow.types.responses import ResponsesAgentRequest, ResponsesAgentResponse

from databricks_mcp import DatabricksOAuthClientProvider
from databricks.sdk import WorkspaceClient
from mcp.client.session import ClientSession
from mcp.client.streamable_http import streamablehttp_client

LLM_ENDPOINT_NAME = "databricks-claude-3-7-sonnet"
SYSTEM_PROMPT = "You are a helpful assistant."

# No need profile in Databricks Apps, need the profile in Databricks CLI
# DATABRICKS_CLI_PROFILE = "e2-demo-field-eng"
# workspace_client = WorkspaceClient(profile=DATABRICKS_CLI_PROFILE)
workspace_client = WorkspaceClient()
host = workspace_client.config.host
MCP_SERVER_URLS = [
    f"{host}/api/2.0/mcp/genie/01f058258b7c1139953d7d6646c0b048"
]

def _to_chat_messages(msg: dict[str, Any]) -> List[dict]:
    """
    Take a single ResponsesAgent‐style dict and turn it into one or more
    ChatCompletions‐compatible dict entries.
    """
    msg_type = msg.get("type")
    if msg_type == "function_call":
        return [
            {
                "role": "assistant",
                "content": None,
                "tool_calls": [
                    {
                        "id": msg["call_id"],
                        "type": "function",
                        "function": {
                            "name": msg["name"],
                            "arguments": msg["arguments"],
                        },
                    }
                ],
            }
        ]
    elif msg_type == "message" and isinstance(msg["content"], list):
        return [
            {
                "role": "assistant" if msg["role"] == "assistant" else msg["role"],
                "content": content["text"],
            }
            for content in msg["content"]
        ]
    elif msg_type == "function_call_output":
        return [
            {
                "role": "tool",
                "content": msg["output"],
                "tool_call_id": msg["tool_call_id"],
            }
        ]
    else:
        # fallback for plain {"role": ..., "content": "..."} or similar
        return [
            {
                k: v
                for k, v in msg.items()
                if k in ("role", "content", "name", "tool_calls", "tool_call_id")
            }
        ]


# 3) “MCP SESSION” + TOOL‐INVOCATION LOGIC
@asynccontextmanager
async def _mcp_session(server_url: str, ws: WorkspaceClient):
    async with streamablehttp_client(
        url=server_url, auth=DatabricksOAuthClientProvider(ws)
    ) as (reader, writer, _):
        async with ClientSession(reader, writer) as session:
            await session.initialize()
            yield session


def _list_tools(server_url: str, ws: WorkspaceClient):
    async def inner():
        async with _mcp_session(server_url, ws) as sess:
            return await sess.list_tools()
    return asyncio.run(inner())


def _make_exec_fn(
    server_url: str, tool_name: str, ws: WorkspaceClient
) -> Callable[..., str]:
    def exec_fn(**kwargs):
        async def call_it():
            async with _mcp_session(server_url, ws) as sess:
                resp = await sess.call_tool(name=tool_name, arguments=kwargs)
                return "".join([c.text for c in resp.content])
        return asyncio.run(call_it())
    return exec_fn


class ToolInfo(BaseModel):
    name: str
    spec: dict
    exec_fn: Callable


def _fetch_tool_infos(ws: WorkspaceClient, server_url: str) -> List[ToolInfo]:
    infos: List[ToolInfo] = []
    mcp_tools = _list_tools(server_url, ws).tools
    for t in mcp_tools:
        schema = t.inputSchema.copy()
        if "properties" not in schema:
            schema["properties"] = {}
        spec = {
            "type": "function",
            "function": {
                "name": t.name,
                "description": t.description,
                "parameters": schema,
            },
        }
        infos.append(
            ToolInfo(
                name=t.name, spec=spec, exec_fn=_make_exec_fn(server_url, t.name, ws)
            )
        )
    return infos


# 4) “SINGLE‐TURN” AGENT CLASS
class SingleTurnMCPAgent(ResponsesAgent):
    def _call_llm(self, history: List[dict], ws: WorkspaceClient, tool_infos):
        """
        Send current history → LLM, returning the raw response dict.
        """
        client = ws.serving_endpoints.get_open_ai_client()
        flat_msgs = []
        for msg in history:
            flat_msgs.extend(_to_chat_messages(msg))

        return client.chat.completions.create(
            model=LLM_ENDPOINT_NAME,
            messages=flat_msgs,
            tools=[ti.spec for ti in tool_infos],
        )

    def predict(self, request: ResponsesAgentRequest) -> ResponsesAgentResponse:
        ws = WorkspaceClient()

        # 1) build initial history: system + user
        history: List[dict] = [{"role": "system", "content": SYSTEM_PROMPT}]
        for inp in request.input:
            history.append(inp.model_dump())

        # 2) call LLM once
        tool_infos = [
            tool_info
            for mcp_server_url in MCP_SERVER_URLS
            for tool_info in _fetch_tool_infos(ws, mcp_server_url)
        ]
        tools_dict = {tool_info.name: tool_info for tool_info in tool_infos}
        llm_resp = self._call_llm(history, ws, tool_infos)
        raw_choice = llm_resp.choices[0].message.to_dict()
        raw_choice["id"] = uuid.uuid4().hex
        history.append(raw_choice)

        tool_calls = raw_choice.get("tool_calls") or []
        if tool_calls:
            # (we only support a single tool in this “single‐turn” example)
            fc = tool_calls[0]
            name = fc["function"]["name"]
            args = json.loads(fc["function"]["arguments"])
            try:
                tool_info = tools_dict[name]
                result = tool_info.exec_fn(**args)
            except Exception as e:
                result = f"Error invoking {name}: {e}"

            # 4) append the “tool” output
            history.append(
                {
                    "type": "function_call_output",
                    "role": "tool",
                    "id": uuid.uuid4().hex,
                    "tool_call_id": fc["id"],
                    "output": result,
                }
            )

            # 5) call LLM a second time and treat that reply as final
            followup = (
                self._call_llm(history, ws, tool_infos=[]).choices[0].message.to_dict()
            )
            followup["id"] = uuid.uuid4().hex

            assistant_text = followup.get("content", "")
            return ResponsesAgentResponse(
                output=[
                    {
                        "id": uuid.uuid4().hex,
                        "type": "message",
                        "role": "assistant",
                        "content": [{"type": "output_text", "text": assistant_text}],
                    }
                ],
                custom_outputs=request.custom_inputs,
            )

        # 6) if no tool_calls at all, return the assistant’s original reply
        assistant_text = raw_choice.get("content", "")
        return ResponsesAgentResponse(
            output=[
                {
                    "id": uuid.uuid4().hex,
                    "type": "message",
                    "role": "assistant",
                    "content": [{"type": "output_text", "text": assistant_text}],
                }
            ],
            custom_outputs=request.custom_inputs,
        )


mlflow.models.set_model(SingleTurnMCPAgent())

class GenieAgentExecutor(AgentExecutor):
    """Test AgentProxy Implementation."""

    def __init__(self):
        pass

    async def execute(
        self,
        context: RequestContext,
        event_queue: EventQueue,
    ) -> None:
        req = ResponsesAgentRequest(
            input=[{"role": "user", "content": context.get_user_input()}]
        )
        loop = asyncio.get_running_loop()
        resp = await loop.run_in_executor(None, SingleTurnMCPAgent().predict, req)
        result = ""
        for item in resp.output:
            if item.role == "assistant":
                result += item.content[0]["text"] + "\n"
        event_queue.enqueue_event(new_agent_text_message(result))

    async def cancel(
        self, context: RequestContext, event_queue: EventQueue
    ) -> None:
        raise Exception('cancel not supported')
