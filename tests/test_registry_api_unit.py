from fastapi.testclient import TestClient

from registry_app.services.http_api import build_registry_api


def test_register_agent_card_validation_error():
    app = build_registry_api()
    client = TestClient(app)

    response = client.post("/register-agent-card", json={})
    assert response.status_code == 422
