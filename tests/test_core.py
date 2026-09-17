from __future__ import annotations

import pytest

from procedure import (
    ActionInvocation,
    PollResult,
    ProcedureEngine,
    ProcedurePreflightError,
    ProcedureRunError,
    SessionState,
    compile_procedure,
)


class FakeInvoker:
    def __init__(self):
        self.preflights = []
        self.invocations = []
        self.polls = 0

    def describe(self, action):
        return action.id in {"a", "b", "trusted"} and action.version in {None, 1, "1"}

    def preflight(self, action, parameters):
        self.preflights.append((action, parameters))
        if action.id == "untrusted":
            raise ValueError("not registered")

    def invoke(self, action, parameters):
        self.invocations.append((action, parameters))
        return ActionInvocation(action.id)

    def poll(self, invocation):
        self.polls += 1
        return PollResult(done=True, value=invocation.token)


def procedure_document(*actions):
    return {
        "id": "demo",
        "version": 1,
        "steps": [{"id": f"step-{i}", "action": action} for i, action in enumerate(actions)],
    }


def test_compile_rejects_undeclared_execution_mechanisms():
    invoker = FakeInvoker()
    session = ProcedureEngine(invoker).new_session(
        compile_procedure({"id": "x", "version": 1, "steps": [{"id": "s", "action": "https://x"}]})
    )
    with pytest.raises(ProcedurePreflightError):
        ProcedureEngine(invoker).preflight(session)


def test_preflight_invokes_zero_actions_when_any_action_is_untrusted():
    invoker = FakeInvoker()
    session = ProcedureEngine(invoker).new_session(
        compile_procedure(procedure_document("trusted", "untrusted"))
    )
    with pytest.raises(Exception):
        ProcedureEngine(invoker).preflight(session)
    assert invoker.invocations == []
    assert session.state is SessionState.FAILED


def test_run_is_sequential_and_session_local():
    invoker = FakeInvoker()
    engine = ProcedureEngine(invoker)
    session = engine.new_session(compile_procedure(procedure_document("a", "b")))
    engine.preflight(session)
    assert engine.run(session) == ["a", "b"]
    assert session.state is SessionState.SUCCEEDED
    assert [action.id for action, _ in invoker.invocations] == ["a", "b"]


def test_run_requires_preflight_and_polling_is_bounded():
    invoker = FakeInvoker()
    engine = ProcedureEngine(invoker)
    session = engine.new_session(compile_procedure({"id": "x", "version": 1, "steps": [{"id": "s", "action": "a", "timeout_polls": 2}]}))
    with pytest.raises(ProcedureRunError):
        engine.run(session)


@pytest.mark.parametrize(
    "value",
    [
        " HTTP://example.test ", "HTTP ://example.test", "python:print(1)",
        "SELECT * FROM users", "/etc/passwd", "../secret", "/dev/ttyUSB0",
        {"nested": ["serial://COM3"]},
    ],
)
def test_preflight_rejects_mechanisms_even_when_nested(value):
    invoker = FakeInvoker()
    session = ProcedureEngine(invoker).new_session(
        compile_procedure({
            "id": "x", "version": 1,
            "steps": [{"id": "s", "action": "trusted", "parameters": {"value": value}}],
        })
    )
    with pytest.raises(ProcedurePreflightError):
        ProcedureEngine(invoker).preflight(session)
    assert invoker.preflights == []


def test_preflight_requires_injected_trusted_action_description():
    invoker = FakeInvoker()
    session = ProcedureEngine(invoker).new_session(
        compile_procedure({"id": "x", "version": 1, "steps": [{"id": "s", "action": "arbitrary"}]})
    )
    with pytest.raises(ProcedurePreflightError, match="not trusted"):
        ProcedureEngine(invoker).preflight(session)
