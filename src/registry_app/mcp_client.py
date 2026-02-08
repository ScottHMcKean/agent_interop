from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Awaitable, Callable

from databricks_mcp import DatabricksOAuthClientProvider
from databricks.sdk import WorkspaceClient
from mcp.client.session import ClientSession
from mcp.client.streamable_http import streamablehttp_client


@dataclass(frozen=True)
class ToolInfo:
    name: str
    spec: dict[str, Any]
    execute: Callable[[dict[str, Any]], Awaitable[str]]


from contextlib import asynccontextmanager


@asynccontextmanager
async def _mcp_session(server_url: str, ws: WorkspaceClient):
    async with streamablehttp_client(
        url=server_url, auth=DatabricksOAuthClientProvider(ws)
    ) as (reader, writer, _):
        async with ClientSession(reader, writer) as session:
            await session.initialize()
            yield session


async def _list_tools(server_url: str, ws: WorkspaceClient):
    async with _mcp_session(server_url, ws) as session:
        return await session.list_tools()


async def _call_tool(
    server_url: str, ws: WorkspaceClient, tool_name: str, arguments: dict[str, Any]
) -> str:
    async with _mcp_session(server_url, ws) as session:
        resp = await session.call_tool(name=tool_name, arguments=arguments)
        return "".join([c.text for c in resp.content])


def _build_tool_spec(tool) -> dict[str, Any]:
    schema = dict(tool.inputSchema or {})
    schema.setdefault("properties", {})
    return {
        "type": "function",
        "function": {
            "name": tool.name,
            "description": tool.description,
            "parameters": schema,
        },
    }


async def build_tool_infos(
    ws: WorkspaceClient, server_urls: list[str]
) -> list[ToolInfo]:
    tool_infos: list[ToolInfo] = []
    seen_names: set[str] = set()
    for server_url in server_urls:
        tools_result = await _list_tools(server_url, ws)
        if not tools_result:
            continue
        for tool in tools_result.tools:
            if tool.name in seen_names:
                raise RuntimeError(
                    f"Duplicate tool name '{tool.name}' across MCP servers."
                )
            seen_names.add(tool.name)
            tool_infos.append(
                ToolInfo(
                    name=tool.name,
                    spec=_build_tool_spec(tool),
                    execute=lambda args, s=server_url, n=tool.name: _call_tool(
                        s, ws, n, args
                    ),
                )
            )
    return tool_infos
