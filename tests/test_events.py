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


class Invoker:
    def __init__(self):
        self.calls = []

    def preflight(self, action, parameters): pass
    def invoke(self, action, parameters):
        self.calls.append(action.id)
        return ActionInvocation(action.id)
    def poll(self, invocation): return PollResult(done=True)


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

