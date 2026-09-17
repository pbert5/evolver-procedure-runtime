import json

from procedure import EvidenceStrength, ObservedInvoker, ProcedureEventStream, RequiredObserverError, redact_payload
from procedure import ActionInvocation, ActionRef, PollResult


def test_events_are_ordered_and_snapshot_is_not_history():
    events = ProcedureEventStream()
    events.emit("run.started", {"token": "hidden"}, evidence=EvidenceStrength.INTENT)
    events.emit("action.accepted", {"ok": True}, evidence=EvidenceStrength.ACK)
    assert [event.sequence for event in events.history] == [0, 1]
    assert list(events.current) == ["run.started", "action.accepted"]
    assert events.snapshot()["run.started"]["payload"] == {"token": "<redacted>"}


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


class Invoker:
    def __init__(self):
        self.calls = []

    def preflight(self, action, parameters): pass
    def invoke(self, action, parameters):
        self.calls.append(action.id)
        return ActionInvocation(action.id)
    def poll(self, invocation): return PollResult(done=True)


def test_successful_software_completion_does_not_claim_observation():
    invoker, events = Invoker(), ProcedureEventStream()
    observed = ObservedInvoker(invoker, events)
    observed.poll(observed.invoke(ActionRef("run"), {}))
    assert events.history[-1].evidence is EvidenceStrength.ACKNOWLEDGED


def test_only_explicit_physical_evidence_can_claim_observation():
    class EvidenceInvoker(Invoker):
        def poll(self, invocation):
            return PollResult(done=True, value={
                "evidence_strength": "observed",
                "evidence": {"kind": "physical", "reading": 12},
            })
    invoker, events = EvidenceInvoker(), ProcedureEventStream()
    observed = ObservedInvoker(invoker, events)
    observed.poll(observed.invoke(ActionRef("run"), {}))
    assert events.history[-1].evidence is EvidenceStrength.OBSERVED


def test_optional_observer_failure_does_not_retry_or_duplicate_action():
    invoker, events = Invoker(), ProcedureEventStream()
    events.add_observer(lambda event: (_ for _ in ()).throw(ValueError("optional")))
    ObservedInvoker(invoker, events).invoke(ActionRef("run"), {})
    assert invoker.calls == ["run"]


def test_required_observer_failure_aborts_before_next_action():
    invoker, events = Invoker(), ProcedureEventStream()
    events.add_observer(lambda event: (_ for _ in ()).throw(ValueError("required")), required=True)
    observed = ObservedInvoker(invoker, events, abort_actions=(ActionRef("stop"),))
    try:
        observed.invoke(ActionRef("run"), {})
    except RequiredObserverError:
        pass
    else:
        raise AssertionError("required observer failure was swallowed")
    assert invoker.calls == ["stop"]


def test_abort_uses_trusted_preflight_and_does_not_leak_invocation_token():
    class PreflightingInvoker(Invoker):
        def __init__(self):
            super().__init__()
            self.preflights = []
        def describe(self, action):
            return action.id in {"stop"}
        def preflight(self, action, parameters):
            self.preflights.append(action.id)
    invoker, events = PreflightingInvoker(), ProcedureEventStream()
    observed = ObservedInvoker(invoker, events, abort_actions=(ActionRef("stop"),))
    events.add_observer(lambda event: (_ for _ in ()).throw(ValueError("token=super-secret")), required=True)
    try:
        observed.invoke(ActionRef("run"), {})
    except RequiredObserverError:
        pass
    assert invoker.preflights == ["stop"]
    assert invoker.calls == ["stop"]
    assert all("super-secret" not in error for error in events.observer_errors)


def test_observer_errors_are_bounded_and_redacted():
    events = ProcedureEventStream()
    events.add_observer(lambda event: (_ for _ in ()).throw(ValueError("token=secret-value")))
    for _ in range(100):
        events.emit("heartbeat")
    assert len(events.observer_errors) == 16
    assert all("secret-value" not in error for error in events.observer_errors)
