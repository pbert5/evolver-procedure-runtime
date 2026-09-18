from __future__ import annotations

import pytest

from evolver_procedure_runtime import (
    ActionInvocation,
    ActionRef,
    ObservedInvoker,
    PollResult,
    ProcedureEngine,
    ProcedureEventStream,
    ProcedureRunError,
    RequiredObserverError,
    compile_procedure,
)


class FencedInvoker:
    controller_generation = 7

    def __init__(self, *, cleanup_generation: int = 7, cleanup_result: PollResult | None = None):
        self.cleanup_generation = cleanup_generation
        self.cleanup_result = cleanup_result or PollResult(done=True, succeeded=True, value="stopped")
        self.calls: list[str] = []

    def describe(self, action):
        return {
            "id": action.id,
            "version": action.version,
            "authorized": True,
            "controller_generation": self.cleanup_generation if action.id == "stop" else 7,
        }

    def preflight(self, action, parameters):
        pass

    def invoke(self, action, parameters):
        self.calls.append(action.id)
        return ActionInvocation(action.id)

    def poll(self, invocation):
        if invocation.token == "run":
            return PollResult(done=True, succeeded=False, error="original action failed")
        return self.cleanup_result


def failing_procedure(*, duplicate_cleanup: bool = False):
    cleanup = [{"type": "action", "id": "stop", "version": 1}]
    if duplicate_cleanup:
        cleanup.append({"type": "action", "id": "stop", "version": 1})
    return compile_procedure({
        "id": "cleanup-demo", "name": "cleanup demo", "version": 1,
        "purpose": "test", "parameters": {}, "entry_step_id": {"type": "step", "id": "run"},
        "default_timeout": 60, "metadata": {},
        "steps": [{"id": "run", "kind": "action", "action": "action:run"}],
        "abort_actions": cleanup,
    })


def test_stale_generation_cleanup_is_observable_and_never_invoked():
    invoker = FencedInvoker(cleanup_generation=8)
    engine = ProcedureEngine(invoker)
    session = engine.new_session(failing_procedure())
    engine.preflight(session)

    with pytest.raises(ProcedureRunError, match="original action failed"):
        engine.run(session)

    assert invoker.calls == ["run"]
    assert session.primary_outcome.reason == "original action failed"
    assert session.cleanup_outcome.status == "failed"
    assert session.cleanup_outcome.actions[0].status == "generation_mismatch"


def test_fresh_valid_cleanup_executes_once_and_keeps_primary_failure():
    invoker = FencedInvoker()
    engine = ProcedureEngine(invoker)
    session = engine.new_session(failing_procedure(duplicate_cleanup=True))
    engine.preflight(session)

    with pytest.raises(ProcedureRunError, match="original action failed"):
        engine.run(session)

    assert invoker.calls == ["run", "stop"]
    assert session.primary_outcome.reason == "original action failed"
    assert session.cleanup_outcome.status == "succeeded"
    assert [action.status for action in session.cleanup_outcome.actions] == ["succeeded"]


def test_cleanup_failure_is_observable_without_erasing_primary_failure():
    invoker = FencedInvoker(cleanup_result=PollResult(done=True, succeeded=False, error="stop failed"))
    engine = ProcedureEngine(invoker)
    session = engine.new_session(failing_procedure())
    engine.preflight(session)

    with pytest.raises(ProcedureRunError, match="original action failed"):
        engine.run(session)

    assert invoker.calls == ["run", "stop"]
    assert session.primary_outcome.reason == "original action failed"
    assert session.cleanup_outcome.status == "failed"
    assert session.cleanup_outcome.actions[0].error == "stop failed"


def test_cleanup_attempts_are_isolated_between_sessions_sharing_an_engine():
    invoker = FencedInvoker()
    engine = ProcedureEngine(invoker)
    first = engine.new_session(failing_procedure())
    second = engine.new_session(failing_procedure())
    engine.preflight(first)
    engine.preflight(second)

    for session in (first, second):
        with pytest.raises(ProcedureRunError):
            engine.run(session)

    assert invoker.calls == ["run", "stop", "run", "stop"]
    assert first.cleanup_outcome.status == second.cleanup_outcome.status == "succeeded"
    assert first.run_id != second.run_id


def test_required_observer_failure_uses_the_engine_cleanup_coordinator_once():
    raw = FencedInvoker()
    events = ProcedureEventStream(
        procedure_id="cleanup-demo", run_id="run-1", procedure_revision=1, controller_generation=7,
    )
    events.add_observer(lambda event: (_ for _ in ()).throw(ValueError("required")), required=True)
    engine = ProcedureEngine(ObservedInvoker(raw, events, abort_actions=(ActionRef("stop", 1),)))
    session = engine.new_session(failing_procedure())
    engine.preflight(session)

    with pytest.raises(ProcedureRunError, match="required observer failed"):
        engine.run(session)

    # The required observer rejects both the normal and cleanup intent event,
    # so the engine records the cleanup failure without bypassing that gate or
    # letting ObservedInvoker start a competing abort loop.
    assert raw.calls == []
    assert session.primary_outcome.kind == "required_observer_failed"
    assert session.cleanup_outcome.status == "failed"
