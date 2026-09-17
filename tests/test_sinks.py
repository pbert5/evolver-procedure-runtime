from __future__ import annotations

import pytest

from procedure.sinks import (
    INITIAL_SINK_IDS,
    CheckpointDestination,
    MutationOutcome,
    SessionBinding,
    SinkEffect,
    SinkError,
    SinkRegistry,
    SinkRequest,
)


def test_initial_registry_has_distinct_effects_and_types():
    registry = SinkRegistry()
    assert tuple(registry.resolve(sink_id).id for sink_id in INITIAL_SINK_IDS) == INITIAL_SINK_IDS
    assert registry.resolve(INITIAL_SINK_IDS[0]).effect is SinkEffect.OBSERVATION
    assert registry.resolve(INITIAL_SINK_IDS[2]).effect is SinkEffect.CHECKPOINT_EXPORT

    session = SessionBinding("session-1", "calibration")
    request = registry.request(
        INITIAL_SINK_IDS[0], {"reading": 1.2}, idempotency_key="obs-1", session=session
    )
    assert request.session is session and request.checkpoint is None


def test_checkpoint_requires_host_destination_and_cannot_bind_session():
    registry = SinkRegistry()
    with pytest.raises(SinkError):
        registry.request(INITIAL_SINK_IDS[2], {}, idempotency_key="cp-1")
    with pytest.raises(SinkError):
        registry.request(
            INITIAL_SINK_IDS[2], {}, idempotency_key="cp-1",
            checkpoint=CheckpointDestination("host-store"),
            session=SessionBinding("session-1", "calibration"),
        )
    request = registry.request(
        INITIAL_SINK_IDS[2], {"checkpoint_id": "cp-1"}, idempotency_key="cp-1",
        checkpoint=CheckpointDestination("host-store"),
    )
    assert request.session is None and request.checkpoint.destination_id == "host-store"


@pytest.mark.parametrize("sink_id", ["unknown.sink", INITIAL_SINK_IDS[0]])
def test_public_sink_request_construction_requires_trusted_registry(sink_id):
    with pytest.raises(SinkError):
        SinkRequest(sink_id, {}, idempotency_key="obs-1")


def test_public_sink_request_cannot_cross_session_and_checkpoint_bindings():
    registry = SinkRegistry()
    with pytest.raises(SinkError):
        SinkRequest(
            INITIAL_SINK_IDS[0], {}, idempotency_key="obs-1",
            checkpoint=CheckpointDestination("host-store"), _registry=registry,
        )
    with pytest.raises(SinkError):
        SinkRequest(
            INITIAL_SINK_IDS[2], {}, idempotency_key="cp-1",
            checkpoint=CheckpointDestination("host-store"),
            session=SessionBinding("session-1", "calibration"), _registry=registry,
        )


def test_public_sink_request_uses_authoritative_registry_membership_for_subclasses():
    from procedure.sinks import SinkSpec

    class ForgedRegistry(SinkRegistry):
        def resolve(self, sink_id):
            return SinkSpec(sink_id, SinkEffect.CHECKPOINT_EXPORT, True, "checkpoint_id")

    registry = ForgedRegistry()
    with pytest.raises(SinkError):
        registry.request(
            INITIAL_SINK_IDS[0], {}, idempotency_key="obs-1",
            checkpoint=CheckpointDestination("host-store"),
        )


@pytest.mark.parametrize("field, value", [
    ("session", "session-1"),
    ("session", object()),
    ("session", "/tmp/session"),
    ("checkpoint", "host-store"),
    ("checkpoint", object()),
    ("checkpoint", "/tmp/checkpoint"),
    ("session", CheckpointDestination("host-store")),
    ("checkpoint", SessionBinding("session-1", "calibration")),
])
def test_public_sink_request_requires_distinct_runtime_binding_types(field, value):
    registry = SinkRegistry()
    kwargs = {field: value}
    if field == "checkpoint":
        sink_id = INITIAL_SINK_IDS[2]
    else:
        sink_id = INITIAL_SINK_IDS[0]
    with pytest.raises(SinkError):
        registry.request(sink_id, {}, idempotency_key="binding-1", **kwargs)


@pytest.mark.parametrize("sink_id", ["unknown.sink", "/tmp/out", "https://example.test"])
def test_only_registered_sink_ids_are_accepted(sink_id):
    with pytest.raises(SinkError):
        SinkRegistry().request(sink_id, {}, idempotency_key="x-1")


def test_explicitly_registered_additional_sink_is_allowed():
    from procedure.sinks import SinkSpec

    spec = SinkSpec("lab.calibration.observation", SinkEffect.OBSERVATION, True, "observation_id")
    registry = SinkRegistry({**{sink_id: SinkRegistry().resolve(sink_id) for sink_id in INITIAL_SINK_IDS}, spec.id: spec})
    assert registry.resolve(spec.id) is spec


@pytest.mark.parametrize("field, value", [
    ("effect", SinkEffect.CHECKPOINT_EXPORT),
    ("persistent", False),
    ("idempotency_key", "checkpoint_id"),
])
def test_initial_sink_metadata_cannot_be_replaced(field, value):
    from dataclasses import replace
    from procedure.sinks import SinkSpec

    original = SinkRegistry().resolve(INITIAL_SINK_IDS[0])
    replacement = replace(original, **{field: value})
    with pytest.raises(SinkError):
        SinkRegistry({
            **{sink_id: SinkRegistry().resolve(sink_id) for sink_id in INITIAL_SINK_IDS},
            original.id: replacement,
        })


def test_initial_sink_metadata_must_be_a_sink_spec():
    with pytest.raises(SinkError):
        SinkRegistry({
            INITIAL_SINK_IDS[0]: object(),
            INITIAL_SINK_IDS[1]: SinkRegistry().resolve(INITIAL_SINK_IDS[1]),
            INITIAL_SINK_IDS[2]: SinkRegistry().resolve(INITIAL_SINK_IDS[2]),
        })


@pytest.mark.parametrize("payload", [
    {"path": "/tmp/output"}, {"url": "https://example.test"},
    {"sql": "SELECT * FROM observations"}, {"command": "python -c x"},
    {"callback": object()},
    {"note": "import os"}, {"note": "from pathlib import Path"},
    {"note": "__import__('os')"}, {"note": "subprocess.run(['tool'])"},
    {"note": "see https://example.test"}, {"note": "write ../outside"},
])
def test_payload_has_no_execution_surface(payload):
    with pytest.raises(SinkError):
        SinkRegistry().request(INITIAL_SINK_IDS[0], payload, idempotency_key="obs-1")


@pytest.mark.parametrize("key", [
    "callback", "command", "exec", "import", "module", "path", "script", "shell", "sql", "url",
    "callback_handler", "import_path", "importlib", "shell_command", "subprocess",
])
def test_execution_surface_keys_are_denied_even_with_data_values(key):
    with pytest.raises(SinkError):
        SinkRegistry().request(INITIAL_SINK_IDS[0], {key: "safe"}, idempotency_key="obs-1")


def test_ambiguous_outcome_is_explicit_and_never_retried():
    outcome = MutationOutcome("ambiguous", "host must reconcile")
    assert outcome.status == "ambiguous"
