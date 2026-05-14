from registry_app.server import build_app


def test_app_routes_include_well_known() -> None:
    app = build_app()
    paths = {route.path for route in app.routes if hasattr(route, "path")}
    assert "/.well-known/agent-card.json" in paths
    assert "/a2a" in paths
