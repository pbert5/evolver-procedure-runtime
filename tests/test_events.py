import json
import pytest

from procedure import EvidenceStrength, ObservedInvoker, ProcedureEventStream, RequiredObserverError, redact_payload
from procedure import ActionInvocation, ActionRef, PollResult
from procedure.events import MAX_HISTORY_EVENTS


def stream(**overrides):
    fence = {"procedure_id": "p", "run_id": "r", "procedure_revision": 3, "controller_generation": 7}
    fence.update(overrides)
    return ProcedureEventStream(**fence)


def test_events_are_ordered_and_snapshot_is_not_history():
    events = stream()
    events.emit("run.started", {"token": "hidden"}, evidence=EvidenceStrength.INTENT)
    events.emit("action.accepted", {"ok": True}, evidence=EvidenceStrength.ACK)
    assert [event.sequence for event in events.history] == [0, 1]
    assert list(events.current) == ["run.started", "action.accepted"]
    assert events.snapshot()["run.started"]["payload"] == {"token": "<redacted>"}


def test_history_is_bounded_and_sequences_remain_monotonic():
    events = stream()
    for index in range(MAX_HISTORY_EVENTS + 3):
        events.emit("heartbeat", {"index": index})
    assert len(events.history) == MAX_HISTORY_EVENTS
    assert [event.sequence for event in events.history] == list(range(3, MAX_HISTORY_EVENTS + 3))
    assert [event.payload["index"] for event in events.history] == list(range(3, MAX_HISTORY_EVENTS + 3))


def test_redaction_is_structural_and_bounded():
    value = {"nested": [{"password": "secret", "safe": "x"}], "long": "x" * 300}
    result = redact_payload(value)
    assert result["nested"][0]["password"] == "<redacted>"
    assert result["long"].endswith("<truncated>")


def test_events_use_versioned_envelope_and_run_fence():
    events = ProcedureEventStream(
        procedure_id="p", run_id="r", procedure_revision=3, controller_generation=7,
    )
    event = events.emit("run.started")
    wire = json.loads(event.serialize())
    assert wire["type"] == "procedure-event/1"
    assert wire["procedure_id"] == "p"
    assert event.is_for(procedure_id="p", run_id="r", procedure_revision=3, controller_generation=7)
    assert not event.is_for(procedure_id="p", run_id="stale", procedure_revision=3, controller_generation=7)
    assert not event.is_for(procedure_id="p", run_id="r", procedure_revision=2, controller_generation=7)
    assert not event.is_for(procedure_id="p", run_id="r", procedure_revision=3, controller_generation=6)


def test_event_stream_requires_complete_run_fence():
    with pytest.raises(TypeError):
        ProcedureEventStream(procedure_id="p", run_id="r", procedure_revision=3)
    with pytest.raises(ValueError):
        ProcedureEventStream(procedure_id="", run_id="r", procedure_revision=3, controller_generation=7)


class Invoker:
    def __init__(self):
        self.calls = []

    def preflight(self, action, parameters): pass
    def invoke(self, action, parameters):
        self.calls.append(action.id)
        return ActionInvocation(action.id)
    def poll(self, invocation): return PollResult(done=True)


def test_successful_software_completion_does_not_claim_observation():
    invoker, events = Invoker(), stream()
    observed = ObservedInvoker(invoker, events)
    observed.poll(observed.invoke(ActionRef("run"), {}))
    assert events.history[-1].evidence is EvidenceStrength.ACKNOWLEDGED


def test_untrusted_poll_value_cannot_claim_observation():
    class EvidenceInvoker(Invoker):
        def poll(self, invocation):
            return PollResult(done=True, value={
                "evidence_strength": "observed",
                "evidence": {"kind": "physical", "reading": 12},
            })
    invoker, events = EvidenceInvoker(), stream()
    observed = ObservedInvoker(invoker, events)
    observed.poll(observed.invoke(ActionRef("run"), {}))
    assert events.history[-1].evidence is EvidenceStrength.ACKNOWLEDGED


def test_optional_observer_failure_does_not_retry_or_duplicate_action():
    invoker, events = Invoker(), stream()
    events.add_observer(lambda event: (_ for _ in ()).throw(ValueError("optional")))
    ObservedInvoker(invoker, events).invoke(ActionRef("run"), {})
    assert invoker.calls == ["run"]


def test_required_observer_failure_aborts_before_next_action():
    invoker, events = Invoker(), stream()
    events.add_observer(lambda event: (_ for _ in ()).throw(ValueError("required")), required=True)
    observed = ObservedInvoker(invoker, events, abort_actions=(ActionRef("stop", 1),))
    try:
        observed.invoke(ActionRef("run"), {})
    except RequiredObserverError:
        pass
    else:
        raise AssertionError("required observer failure was swallowed")
    assert invoker.calls == []
    assert events.history[-1].name == "abort.failed"


def test_abort_uses_trusted_preflight_and_does_not_leak_invocation_token():
    class PreflightingInvoker(Invoker):
        def __init__(self):
            super().__init__()
            self.preflights = []
        def describe(self, action):
            return {
                "id": action.id,
                "version": action.version,
                "controller_generation": 7,
                "authorized": action.id in {"stop"},
            }
        def preflight(self, action, parameters):
            self.preflights.append(action.id)
    invoker, events = PreflightingInvoker(), stream()
    observed = ObservedInvoker(invoker, events, abort_actions=(ActionRef("stop", 1),))
    events.add_observer(lambda event: (_ for _ in ()).throw(ValueError("token=super-secret")), required=True)
    try:
        observed.invoke(ActionRef("run"), {})
    except RequiredObserverError:
        pass
    assert invoker.preflights == ["stop"]
    assert invoker.calls == ["stop"]
    assert [event.name for event in events.history[-3:]] == [
        "abort.requested", "abort.accepted", "abort.completed",
    ]
    assert all("super-secret" not in error for error in events.observer_errors)


def test_abort_requires_describe_and_never_uses_permissive_noop():
    invoker, events = Invoker(), stream()
    observed = ObservedInvoker(invoker, events, abort_actions=(ActionRef("stop", 1),))
    events.add_observer(lambda event: (_ for _ in ()).throw(ValueError("required")), required=True)
    with pytest.raises(RequiredObserverError):
        observed.invoke(ActionRef("run"), {})
    assert invoker.calls == []
    assert events.history[-1].name == "abort.failed"


@pytest.mark.parametrize(
    ("result", "status", "error"),
    [
        (PollResult(done=True, succeeded=True), None, None),
        (PollResult(done=True, succeeded=False, error="status=failed"), "failed", "status=failed"),
        (PollResult(done=False, succeeded=True, error="token=secret"), "incomplete", "token=<redacted>"),
    ],
)
def test_abort_completion_requires_done_and_success(result, status, error):
    class AbortInvoker(Invoker):
        def describe(self, action):
            return {"id": action.id, "version": action.version, "controller_generation": 7, "authorized": True}

        def poll(self, invocation):
            return result

    invoker, events = AbortInvoker(), stream()
    events.add_observer(lambda event: (_ for _ in ()).throw(ValueError("required")), required=True)
    observed = ObservedInvoker(invoker, events, abort_actions=(ActionRef("stop", 1),))
    with pytest.raises(RequiredObserverError):
        observed.invoke(ActionRef("run"), {})

    abort_events = [event for event in events.history if event.name.startswith("abort.")]
    assert abort_events[-1].name == ("abort.completed" if status is None else "abort.failed")
    if status is not None:
        assert abort_events[-1].evidence is EvidenceStrength.ACKNOWLEDGED
        assert abort_events[-1].payload["status"] == status
        assert abort_events[-1].payload["error"] == error


def test_abort_poll_exception_is_failed_and_sanitized():
    class AbortInvoker(Invoker):
        def describe(self, action):
            return {"id": action.id, "version": action.version, "controller_generation": 7, "authorized": True}

        def poll(self, invocation):
            raise RuntimeError("token=super-secret")

    invoker, events = AbortInvoker(), stream()
    events.add_observer(lambda event: (_ for _ in ()).throw(ValueError("required")), required=True)
    with pytest.raises(RequiredObserverError):
        ObservedInvoker(invoker, events, abort_actions=(ActionRef("stop", 1),)).invoke(ActionRef("run"), {})

    failure = events.history[-1]
    assert failure.name == "abort.failed"
    assert failure.payload == {
        "action": "stop",
        "status": "exception",
        "error": "RuntimeError: token=<redacted>",
    }


@pytest.mark.parametrize(
    "description",
    [
        {"id": "stop", "version": 1, "authorized": True},
        {"id": "stop", "version": 1, "controller_generation": 8, "authorized": True},
    ],
)
def test_abort_requires_event_stream_controller_generation(description):
    class AbortInvoker(Invoker):
        def describe(self, action):
            return description

    invoker, events = AbortInvoker(), stream()
    events.add_observer(lambda event: (_ for _ in ()).throw(ValueError("required")), required=True)
    with pytest.raises(RequiredObserverError):
        ObservedInvoker(invoker, events, abort_actions=(ActionRef("stop", 1),)).invoke(ActionRef("run"), {})

    assert invoker.calls == []
    assert events.history[-1].name == "abort.failed"


def test_required_observer_error_is_bounded_and_does_not_leak_exception_text():
    events = stream()
    events.add_observer(lambda event: (_ for _ in ()).throw(ValueError("token=super-secret")), required=True)
    with pytest.raises(RequiredObserverError, match="^required observer failed$") as failure:
        events.emit("run.started")
    assert "super-secret" not in str(failure.value)
    assert failure.value.__cause__ is None
    assert failure.value.__context__ is None
    assert all("super-secret" not in error for error in events.observer_errors)


def test_observer_errors_are_bounded_and_redacted():
    events = stream()
    events.add_observer(lambda event: (_ for _ in ()).throw(ValueError("token=secret-value")))
    for _ in range(100):
        events.emit("heartbeat")
    assert len(events.observer_errors) == 16
    assert all("secret-value" not in error for error in events.observer_errors)
