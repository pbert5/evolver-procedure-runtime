from __future__ import annotations

import pytest

from evolver_procedure_runtime import (
    ActionInvocation,
    PollResult,
    ProcedureEngine,
    SessionState,
    compile_procedure,
)


class IncrementalInvoker:
    controller_generation = 7

    def __init__(self) -> None:
        self.invocations: list[tuple[str, dict]] = []
        self.polls: list[str] = []
        self.remaining: dict[str, int] = {}

    def describe(self, action):
        return {
            "id": action.id,
            "version": action.version,
            "authorized": action.id in {"set_level", "run", "stop"},
            "controller_generation": 7,
        }

    def preflight(self, action, parameters):
        pass

    def invoke(self, action, parameters):
        self.invocations.append((action.id, parameters))
        self.remaining[action.id] = 1
        return ActionInvocation(f"{action.id}-token")

    def poll(self, invocation):
        self.polls.append(invocation.token)
        if self.remaining[invocation.token.removesuffix("-token")]:
            self.remaining[invocation.token.removesuffix("-token")] -= 1
            return PollResult(done=False)
        return PollResult(done=True, value=invocation.token)


def input_then_action():
    return compile_procedure(
        {
            "id": "incremental",
            "name": "Incremental",
            "version": 1,
            "purpose": "test",
            "parameters": {"level": {"type": "integer"}},
            "default_timeout": 60,
            "metadata": {},
            "entry_step_id": "step:input",
            "steps": [
                {
                    "id": "input",
                    "kind": "input",
                    "parameter": "parameter:level",
                    "prompt": "Level",
                    "max_input_length": 8,
                    "next_step_id": "step:run",
                },
                {
                    "id": "run",
                    "kind": "action",
                    "action": "action:set_level",
                    "parameters": {"level": {"parameter_ref": "level"}},
                    "timeout_polls": 2,
                    "next_step_id": "step:done",
                },
                {"id": "done", "kind": "complete"},
            ],
        }
    )


def test_incremental_input_is_waiting_until_explicitly_provided_and_advanced():
    invoker = IncrementalInvoker()
    engine = ProcedureEngine(invoker)
    session = engine.new_session(input_then_action())

    assert session.state is SessionState.CREATED
    engine.preflight(session)
    assert session.state is SessionState.PREFLIGHTED

    update = engine.advance(session)
    assert update.state is SessionState.WAITING_INPUT
    assert update.input_parameter == "level"
    assert invoker.invocations == []

    engine.provide_parameter(session, "level", 7)
    assert session.state is SessionState.WAITING_INPUT
    assert invoker.invocations == []

    update = engine.advance(session)
    assert update.state is SessionState.WAITING_ACTION
    assert len(invoker.invocations) == 1

    update = engine.advance(session)
    assert update.state is SessionState.WAITING_ACTION
    assert len(invoker.invocations) == 1

    update = engine.advance(session)
    assert update.state is SessionState.SUCCEEDED
    assert len(invoker.invocations) == 1


def test_invalid_incremental_input_is_rejected_without_advancing():
    engine = ProcedureEngine(IncrementalInvoker())
    session = engine.new_session(input_then_action())
    engine.preflight(session)
    engine.advance(session)

    with pytest.raises(ValueError, match="level"):
        engine.provide_parameter(session, "level", "not an integer")
    assert session.inputs == {}
    assert session.state is SessionState.WAITING_INPUT


def test_two_sessions_retain_independent_inputs_and_pending_invocations():
    invoker = IncrementalInvoker()
    engine = ProcedureEngine(invoker)
    first = engine.new_session(input_then_action())
    second = engine.new_session(input_then_action())
    engine.preflight(first)
    engine.preflight(second)

    engine.advance(first)
    engine.advance(second)
    engine.provide_parameter(first, "level", 1)
    engine.provide_parameter(second, "level", 2)
    assert engine.advance(first).state is SessionState.WAITING_ACTION
    assert engine.advance(second).state is SessionState.WAITING_ACTION
    assert [parameters for _, parameters in invoker.invocations] == [{"level": 1}, {"level": 2}]

    assert engine.advance(first).state is SessionState.WAITING_ACTION
    assert engine.advance(second).state is SessionState.SUCCEEDED
    assert len(invoker.invocations) == 2


def test_abort_from_input_wait_runs_cleanup_once_and_is_terminal():
    procedure = compile_procedure(
        {
            "id": "abortable",
            "name": "Abortable",
            "version": 1,
            "purpose": "test",
            "parameters": {"missing": {"type": "string"}},
            "entry_step_id": "step:wait",
            "default_timeout": 60,
            "metadata": {},
            "steps": [{"id": "wait", "kind": "input", "parameter": "parameter:missing", "prompt": "Missing", "max_input_length": 8}],
            "abort_actions": [{"type": "action", "id": "stop"}],
        }
    )
    invoker = IncrementalInvoker()
    engine = ProcedureEngine(invoker)
    session = engine.new_session(procedure)
    engine.preflight(session)
    assert engine.advance(session).state is SessionState.WAITING_INPUT

    assert engine.abort(session, reason="operator cancelled").state is SessionState.ABORTED
    assert engine.abort(session, reason="ignored").state is SessionState.ABORTED
    assert session.primary_outcome.kind == "aborted"
    assert session.cleanup_attempted is True
    assert [action for action, _ in invoker.invocations] == ["stop"]


def test_run_is_compatibility_loop_over_incremental_session():
    class Provider:
        def read(self, parameter, prompt, max_length):
            return 7

    invoker = IncrementalInvoker()
    engine = ProcedureEngine(invoker, input_provider=Provider(), sleep=lambda _: None)
    session = engine.new_session(input_then_action())
    engine.preflight(session)

    assert engine.run(session) == ["set_level-token"]
    assert len(invoker.invocations) == 1
