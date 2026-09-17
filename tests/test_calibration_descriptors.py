from pathlib import Path

import pytest
import yaml


ROOT = Path(__file__).parents[1] / "examples"
DESCRIPTORS = sorted(ROOT.glob("*.yaml"))
TRUSTED_ACTIONS = {
    "capture_measurement", "emit_marker", "pulse_pump", "request_observation",
    "set_stirring", "set_temperature", "start_activity", "stop_actuator",
    "stop_activity", "wait",
}
TRUSTED_SINKS = {
    "edge.calibration_run.observation",
    "central.calibration_session.observation",
}


def _typed_ref(value, expected_type):
    assert isinstance(value, dict)
    assert value["type"] == expected_type
    assert isinstance(value["id"], str) and value["id"]


def _validate_descriptor(document):
    assert document["purpose"] == "interactive calibration"
    assert isinstance(document["parameters"], dict) and document["parameters"]
    assert 1 <= document["default_timeout"] <= 86400
    assert document["metadata"]["observation_sink_id"] in TRUSTED_SINKS

    _typed_ref(document["entry_step_id"], "step")
    steps = document["steps"]
    assert isinstance(steps, list) and steps
    step_ids = {step["id"] for step in steps}
    assert document["entry_step_id"]["id"] in step_ids

    for name, parameter in document["parameters"].items():
        assert isinstance(name, str) and isinstance(parameter, dict)
        assert parameter["type"] in {"enum", "integer", "number", "string"}
        if "minimum" in parameter:
            assert isinstance(parameter["minimum"], (int, float))
        if "maximum" in parameter:
            assert isinstance(parameter["maximum"], (int, float))
            assert parameter["minimum"] <= parameter["maximum"]

    for step in steps:
        assert step["id"] in step_ids
        assert step["kind"] in {"action", "input", "complete"}
        if step["kind"] == "input":
            _typed_ref(step["parameter"], "parameter")
            assert step["parameter"]["id"] in document["parameters"]
            assert isinstance(step["prompt"], str) and step["prompt"]
            assert 1 <= step["max_input_length"] <= 4096
        elif step["kind"] == "action":
            action = step["action"]
            _typed_ref(action, "action")
            assert action["id"] in TRUSTED_ACTIONS
            assert isinstance(action["version"], (str, int))
            assert 1 <= step["timeout_polls"] <= 1000
            assert 0 <= step["poll_interval_s"] <= 60
            for value in step.get("parameters", {}).values():
                if isinstance(value, dict) and "parameter_ref" in value:
                    assert set(value) == {"parameter_ref"}
                    assert value["parameter_ref"] in document["parameters"]

    assert document["abort_actions"]
    for action in document["abort_actions"]:
        _typed_ref(action, "action")
        assert action["id"] in TRUSTED_ACTIONS


def test_calibration_descriptor_safety_contracts():
    documents = []
    for path in DESCRIPTORS:
        document = yaml.safe_load(path.read_text())
        _validate_descriptor(document)
        documents.append(document)
    assert len(documents) == 11
    assert {doc["metadata"]["observation_sink_id"] for doc in documents} <= TRUSTED_SINKS
    od_docs = [doc for doc in documents if doc["metadata"]["calibration_type"] == "optical_density"]
    for document in od_docs:
        for parameter in document["parameters"].values():
            if parameter.get("unit") == "optical_density" or parameter.get("id") == "od_led_intensity":
                if "minimum" in parameter:
                    assert parameter["minimum"] >= 0
                if parameter.get("id") == "od_led_intensity":
                    assert parameter["maximum"] <= 255
    report = next(doc for doc in documents if doc["id"].endswith("review-reporting"))
    assert report["metadata"]["fitting_supported"] is False
    assert not any("fit" in step.get("action", {}).get("id", "") for step in report["steps"])


def test_calibration_descriptors_have_no_execution_surfaces_or_unsafe_hardware_literals():
    text = "\n".join(path.read_text() for path in DESCRIPTORS).lower()
    for forbidden in ("serial", "python", "shell", "script", "url", "sql", "4095", "31000"):
        assert forbidden not in text
    assert "repeat_count" not in text
