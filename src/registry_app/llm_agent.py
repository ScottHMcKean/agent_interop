from __future__ import annotations

import asyncio
import json
from typing import Any, Iterable

from databricks.sdk import WorkspaceClient

from registry_app.mcp_client import ToolInfo


def _to_chat_messages(msg: dict[str, Any]) -> list[dict[str, Any]]:
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
    if msg_type == "message" and isinstance(msg.get("content"), list):
        return [
            {
                "role": "assistant" if msg["role"] == "assistant" else msg["role"],
                "content": content["text"],
            }
            for content in msg["content"]
        ]
    if msg_type == "function_call_output":
        return [
            {
                "role": "tool",
                "content": msg["output"],
                "tool_call_id": msg["tool_call_id"],
            }
        ]
    return [
        {
            k: v
            for k, v in msg.items()
            if k in ("role", "content", "name", "tool_calls", "tool_call_id")
        }
    ]


def _flatten_history(history: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    flattened: list[dict[str, Any]] = []
    for msg in history:
        flattened.extend(_to_chat_messages(msg))
    return flattened


def _call_llm_sync(
    ws: WorkspaceClient,
    model: str,
    messages: list[dict[str, Any]],
    tools: list[dict[str, Any]] | None,
):
    client = ws.serving_endpoints.get_open_ai_client()
    return client.chat.completions.create(
        model=model,
        messages=messages,
        tools=tools or None,
    )


async def run_single_turn_agent(
    ws: WorkspaceClient,
    model: str,
    history: list[dict[str, Any]],
    tool_infos: list[ToolInfo],
) -> str:
    loop = asyncio.get_running_loop()
    flat_msgs = _flatten_history(history)
    tool_specs = [tool.spec for tool in tool_infos]
    llm_resp = await loop.run_in_executor(
        None, lambda: _call_llm_sync(ws, model, flat_msgs, tool_specs)
    )
    raw_choice = llm_resp.choices[0].message.to_dict()
    raw_choice["id"] = "llm-first"
    history.append(raw_choice)

    tool_calls = raw_choice.get("tool_calls") or []
    if tool_calls:
        tool_map = {tool.name: tool for tool in tool_infos}
        for call in tool_calls:
            name = call["function"]["name"]
            args = json.loads(call["function"]["arguments"])
            try:
                tool_info = tool_map[name]
                output = await tool_info.execute(args)
            except Exception as exc:
                output = f"Error invoking {name}: {exc}"
            history.append(
                {
                    "type": "function_call_output",
                    "role": "tool",
                    "id": f"{call['id']}-output",
                    "tool_call_id": call["id"],
                    "output": output,
                }
            )

        followup = await loop.run_in_executor(
            None, lambda: _call_llm_sync(ws, model, _flatten_history(history), [])
        )
        final_choice = followup.choices[0].message.to_dict()
        return final_choice.get("content", "")

    return raw_choice.get("content", "")
