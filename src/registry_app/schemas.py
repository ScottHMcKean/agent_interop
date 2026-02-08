from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class AgentSkill(BaseModel):
    id: str
    name: str
    description: str
    tags: list[str] | None = None
    examples: list[str] | None = None


class AgentCapabilities(BaseModel):
    streaming: bool = False


class AgentCardPayload(BaseModel):
    name: str
    description: str
    url: str
    version: str
    defaultInputModes: list[str]
    defaultOutputModes: list[str]
    capabilities: AgentCapabilities
    skills: list[AgentSkill]
    supportsAuthenticatedExtendedCard: bool | None = None
    authSchemes: list[dict[str, Any]] | None = None
    tags: list[str] | None = None
    humanReadableId: str | None = None
    agentVersion: str | None = None


class RegisterAgentCardRequest(BaseModel):
    agent_id: str = Field(..., description="Human readable agent id.")
    owner: str = "self-registered"
    status: str = "active"
    version: str
    mcp_server_url: str
    llm_endpoint_name: str | None = None
    system_prompt: str | None = None
    tags: dict[str, Any] | None = None
    protocol: str = "a2a"
    card: AgentCardPayload
