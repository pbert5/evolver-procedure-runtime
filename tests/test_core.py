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


class FailingInvoker(FakeInvoker):
    def __init__(self, *, failure="poll"):
        super().__init__()
        self.failure = failure

    def invoke(self, action, parameters):
        self.invocations.append((action, parameters))
        if self.failure == "invoke" and action.id == "run":
            raise RuntimeError("invoke failed")
        return ActionInvocation(action.id)

    def poll(self, invocation):
        self.polls += 1
        if self.failure == "poll" and invocation.token == "run":
            return PollResult(done=True, succeeded=False, error="action failed")
        if self.failure == "exception" and invocation.token == "run":
            raise RuntimeError("poll failed")
        if self.failure == "timeout" and invocation.token == "run":
            return PollResult(done=False)
        return PollResult(done=True, value=invocation.token)


class FakeInputProvider:
    def __init__(self, value):
        self.value = value
        self.calls = []

    def read(self, parameter, prompt, max_length):
        self.calls.append((parameter, prompt, max_length))
        return self.value


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


@pytest.mark.parametrize("failure", ["invoke", "poll", "timeout", "exception"])
def test_abort_actions_are_freshly_preflighted_on_every_failure_path(failure):
    invoker = FailingInvoker(failure=failure)
    invoker.describe = lambda action: action.id in {"run", "stop"}
    session = ProcedureEngine(invoker).new_session(compile_procedure({
        "id": "x", "name": "x", "version": 1, "purpose": "test", "parameters": {},
        "entry_step_id": {"type": "step", "id": "run"}, "default_timeout": 60, "metadata": {},
                "steps": [{"id": "run", "kind": "action", "action": "action:run", "timeout_polls": 2}],
        "abort_actions": [{"type": "action", "id": "stop"}],
    }))
    engine = ProcedureEngine(invoker)
    engine.preflight(session)
    with pytest.raises(ProcedureRunError):
        engine.run(session)
    assert [action.id for action, _ in invoker.invocations] == ["run", "stop"]
    assert [action.id for action, _ in invoker.preflights].count("stop") == 2
    assert invoker.preflights[-1][0].id == "stop"


def test_abort_cleanup_fails_closed_when_fresh_trust_is_stale():
    invoker = FailingInvoker(failure="poll")
    descriptions = iter([True, True, True, False])
    invoker.describe = lambda action: next(descriptions)
    session = ProcedureEngine(invoker).new_session(compile_procedure({
        "id": "x", "name": "x", "version": 1, "purpose": "test", "parameters": {},
        "entry_step_id": {"type": "step", "id": "run"}, "default_timeout": 60, "metadata": {},
        "steps": [{"id": "run", "kind": "action", "action": "action:run"}],
        "abort_actions": [{"type": "action", "id": "stop"}],
    }))
    engine = ProcedureEngine(invoker)
    engine.preflight(session)
    with pytest.raises(ProcedureRunError):
        engine.run(session)
    assert [action.id for action, _ in invoker.invocations] == ["run"]


def test_duplicate_abort_actions_do_not_duplicate_hardware_action():
    invoker = FailingInvoker(failure="poll")
    invoker.describe = lambda action: action.id in {"run", "stop"}
    session = ProcedureEngine(invoker).new_session(compile_procedure({
        "id": "x", "name": "x", "version": 1, "purpose": "test", "parameters": {},
        "entry_step_id": {"type": "step", "id": "run"}, "default_timeout": 60, "metadata": {},
        "steps": [{"id": "run", "kind": "action", "action": "action:run"}],
        "abort_actions": [
            {"type": "action", "id": "stop", "version": 1},
            {"type": "action", "id": "stop", "version": 1},
        ],
    }))
    engine = ProcedureEngine(invoker)
    engine.preflight(session)
    with pytest.raises(ProcedureRunError):
        engine.run(session)
    assert [action.id for action, _ in invoker.invocations] == ["run", "stop"]


def test_v1_input_parameter_ref_and_graph_continuation():
    invoker = FakeInvoker()
    invoker.describe = lambda action: action.id in {"set_level", "wrong"}
    provider = FakeInputProvider(7)
    procedure = compile_procedure({
        "id": "x", "name": "x", "version": 1, "purpose": "test", "parameters": {"level": {"type": "integer"}},
        "default_timeout": 60, "metadata": {},
        "entry_step_id": {"type": "step", "id": "input"},
        "steps": [
            {"id": "input", "kind": "input", "parameter": "parameter:level", "prompt": "Level", "max_input_length": 8, "next_step_id": "step:run"},
            {"id": "run", "kind": "action", "action": "action:set_level", "parameters": {"level": {"parameter_ref": "level"}}, "next_step_id": "step:done"},
            {"id": "ignored", "kind": "action", "action": "action:wrong"},
            {"id": "done", "kind": "complete"},
        ],
    })
    engine = ProcedureEngine(invoker, input_provider=provider)
    session = engine.new_session(procedure)
    engine.preflight(session)
    assert engine.run(session) == ["set_level"]
    assert provider.calls == [("level", "Level", 8)]
    assert invoker.invocations[0][1] == {"level": 7}


def test_procedure_deadline_still_runs_abort_cleanup():
    invoker = FailingInvoker(failure="timeout")
    invoker.describe = lambda action: action.id in {"run", "stop"}
    clock_values = iter([0.0, 0.1, 0.2, 2.0, 2.0])
    engine = ProcedureEngine(invoker, clock=lambda: next(clock_values))
    session = engine.new_session(compile_procedure({
        "id": "x", "name": "x", "version": 1, "purpose": "test", "parameters": {},
        "entry_step_id": {"type": "step", "id": "run"}, "default_timeout": 1, "metadata": {},
        "steps": [{"id": "run", "kind": "action", "action": "action:run", "timeout_polls": 2}],
        "abort_actions": [{"type": "action", "id": "stop"}],
    }))
    engine.preflight(session)
    with pytest.raises(ProcedureRunError):
        engine.run(session)
    assert [action.id for action, _ in invoker.invocations] == ["run", "stop"]
