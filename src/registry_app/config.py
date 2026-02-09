from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path
from typing import Any

import yaml


@dataclass(frozen=True)
class Settings:
    lakebase_dsn: str | None
    lakebase_host: str | None
    lakebase_db: str | None
    lakebase_user: str | None
    registry_schema: str
    registry_base_url: str | None
    workspace_url: str | None


def _load_config() -> dict[str, Any]:
    config_path = Path("config.yaml")
    if not config_path.exists():
        raise RuntimeError("Missing required config.yaml")
    with config_path.open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle) or {}
    if not isinstance(data, dict):
        raise RuntimeError("config.yaml must be a flat dictionary")
    return data


def load_settings() -> Settings:
    config = _load_config()
    lakebase_dsn = config.get("lakebase_dsn") or None
    lakebase_host = config.get("lakebase_host") or None
    lakebase_db = config.get("lakebase_db") or None
    lakebase_user = config.get("lakebase_user") or None
    if not lakebase_dsn and not lakebase_host:
        raise RuntimeError(
            "Missing required config: provide lakebase_dsn or lakebase_host."
        )

    workspace_url = config.get("workspace_url") or None
    registry_base_url = (
        config.get("registry_base_url")
        or os.getenv("DATABRICKS_APP_URL")
        or workspace_url
    )
    if not registry_base_url:
        try:
            from databricks.sdk import WorkspaceClient

            registry_base_url = WorkspaceClient().config.host
        except Exception:
            registry_base_url = None
    if registry_base_url:
        registry_base_url = registry_base_url.rstrip("/")

    return Settings(
        lakebase_dsn=lakebase_dsn,
        lakebase_host=lakebase_host,
        lakebase_db=lakebase_db,
        lakebase_user=lakebase_user,
        registry_schema=config.get("registry_schema", "agent_registry"),
        registry_base_url=registry_base_url,
        workspace_url=workspace_url,
    )
