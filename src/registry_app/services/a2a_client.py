from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Protocol

import httpx


class A2AClientProtocol(Protocol):
    async def invoke_task(
        self,
        *,
        goal: str,
        input: Mapping[str, Any],
        metadata: Mapping[str, Any],
        timeout: int,
    ) -> dict[str, Any]:
        ...


@dataclass(frozen=True)
class A2AClient:
    base_url: str
    auth_headers: dict[str, str] | None = None

    async def invoke_task(
        self,
        *,
        goal: str,
        input: Mapping[str, Any],
        metadata: Mapping[str, Any],
        timeout: int,
    ) -> dict[str, Any]:
        payload = {
            "goal": goal,
            "input": dict(input),
            "metadata": dict(metadata),
        }
        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.post(
                self.base_url,
                json=payload,
                headers=self.auth_headers,
            )
            response.raise_for_status()
            if response.headers.get("content-type", "").startswith("application/json"):
                return response.json()
            return {"text": response.text}


def build_a2a_client(
    base_url: str, auth_config: Mapping[str, Any] | None
) -> A2AClient:
    headers = None
    if auth_config:
        headers = auth_config.get("headers")
    return A2AClient(base_url=base_url, auth_headers=headers)
