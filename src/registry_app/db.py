from __future__ import annotations

from contextlib import contextmanager
from typing import Iterator
from urllib.parse import quote_plus

import psycopg
from psycopg.rows import dict_row
from databricks.sdk import WorkspaceClient

from registry_app.config import load_settings


@contextmanager
def get_connection() -> Iterator[psycopg.Connection]:
    settings = load_settings()
    if settings.lakebase_dsn:
        dsn = settings.lakebase_dsn
    else:
        host = settings.lakebase_host
        if not host:
            raise RuntimeError("Missing lakebase_host in config.")
        if ":" in host:
            host, port = host.split(":", 1)
        else:
            port = "5432"
        ws = WorkspaceClient()
        user = ws.current_user.me().user_name
        token = ws.config.oauth_token().access_token
        db_name = settings.lakebase_db or "databricks_postgres"
        dsn = (
            "postgresql://"
            f"{quote_plus(user)}:{quote_plus(token)}@{host}:{port}/{db_name}"
            "?sslmode=require"
        )
    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        yield conn
