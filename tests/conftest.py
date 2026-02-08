from __future__ import annotations

from pathlib import Path
from typing import Iterator

import pytest
import yaml
from databricks.sdk import WorkspaceClient


def load_config() -> dict:
    config_path = Path("config.yaml")
    if not config_path.exists():
        raise RuntimeError("Missing config.yaml")
    config = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    if not isinstance(config, dict):
        raise RuntimeError("config.yaml must be a flat dictionary")
    return config


@pytest.fixture(scope="session")
def workspace_client() -> WorkspaceClient:
    try:
        client = WorkspaceClient()
        _ = client.current_user.me()
        return client
    except Exception as exc:
        pytest.skip(f"Databricks auth not available: {exc}")


@pytest.fixture(scope="session")
def config() -> dict:
    return load_config()
