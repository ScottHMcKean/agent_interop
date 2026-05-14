from __future__ import annotations

from registry_app.services.a2a_executor import (
    _extract_a2a_messages,
    _extract_tagged_messages,
    _parse_json_payload,
)


def test_parse_json_payload_returns_dict():
    assert _parse_json_payload('{"agent_id":"x"}') == {"agent_id": "x"}


def test_parse_json_payload_rejects_non_dict():
    assert _parse_json_payload("[1,2]") is None
    assert _parse_json_payload("not json") is None


def test_extract_a2a_messages_flattens_artifact_parts():
    payload = {
        "result": {
            "artifacts": [
                {
                    "parts": [
                        {"text": "hello"},
                        {"text": "world"},
                    ]
                }
            ]
        }
    }
    out = _extract_a2a_messages(payload)
    assert out == [{"role": "assistant", "text": "hello\nworld"}]


def test_extract_a2a_messages_empty_when_no_artifacts():
    assert _extract_a2a_messages({"result": {"artifacts": []}}) == []
    assert _extract_a2a_messages({}) == []


def test_extract_tagged_messages_routes_on_protocol_a2a():
    payload = {"result": {"artifacts": [{"parts": [{"text": "ok"}]}]}}
    tags = {"api_protocol": "a2a"}
    assert _extract_tagged_messages(payload, tags) == [
        {"role": "assistant", "text": "ok"}
    ]


def test_extract_tagged_messages_falls_back_to_message_path():
    payload = {"foo": {"bar": "answer"}}
    tags = {"message_path": "foo.bar"}
    assert _extract_tagged_messages(payload, tags) == [
        {"role": "assistant", "text": "answer"}
    ]
