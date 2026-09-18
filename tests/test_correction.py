from __future__ import annotations

import pytest

from evolver_procedure_runtime import (
    ActionInvocation,
    CorrectionUnavailable,
    ProcedureEngine,
    SessionState,
    compile_procedure,
)
from evolver_procedure_runtime.invoker import PollResult


class Invoker:
    controller_generation = 1

    def __init__(self):
        self.invocations = []

    def describe(self, action):
        return {"id": action.id, "version": action.version, "authorized": True,
                "controller_generation": 1}

    def preflight(self, action, parameters):
        return None

    def invoke(self, action, parameters):
        self.invocations.append((action.id, dict(parameters)))
        return ActionInvocation(f"{action.id}-{len(self.invocations)}")

    def poll(self, invocation):
        return PollResult(done=True, value=invocation.token)


def procedure(*, replaceable=False, action_replaceable=False):
    correction = {"mode": "replaceable", "invalidates": ["fit"]} if replaceable else None
    action_correction = {"mode": "replaceable", "invalidates": []} if action_replaceable else None
    capture = {
        "id": "capture", "kind": "input", "parameter": "parameter:reading",
        "prompt": "Reading", "max_input_length": 20, "next_step_id": "step:fit",
    }
    if correction is not None:
        capture["correction"] = correction
    fit = {
        "id": "fit", "kind": "action", "action": "action:fit",
        "next_step_id": "step:done",
    }
    if action_correction is not None:
        fit["correction"] = action_correction
    return compile_procedure({
        "id": "correction", "name": "Correction", "version": 1,
        "purpose": "test", "parameters": {"reading": {"type": "number"}},
        "entry_step_id": "step:capture", "default_timeout": 60, "metadata": {},
        "steps": [capture, fit, {"id": "done", "kind": "complete"}],
    })


def finish(engine, session):
    assert engine.advance(session).state is SessionState.WAITING_INPUT
    engine.provide_parameter(session, "reading", 31.8)
    assert engine.advance(session).state is SessionState.WAITING_ACTION
    assert engine.advance(session).state is SessionState.SUCCEEDED


def test_attempts_are_immutable_and_rerun_invalidates_forward_history():
    invoker = Invoker()
    engine = ProcedureEngine(invoker)
    session = engine.new_session(procedure())
    engine.preflight(session)
    finish(engine, session)

    history = session.attempt_history
    assert [attempt.step_id for attempt in history] == ["capture", "fit"]
    assert history[0].result == 31.8

    update = engine.rerun_from_here(session, "capture")
    assert update.state is SessionState.READY
    assert history[0].result == 31.8
    assert session.attempt_history[0].status == "superseded"
    assert session.attempt_history[1].status == "stale"
    assert session.current_step_id == "capture"
    assert invoker.invocations == [("fit", {})]

    assert engine.advance(session).state is SessionState.WAITING_INPUT
    assert len(session.attempt_history) == 3
    assert session.attempt_history[-1].status == "pending"


def test_targeted_correction_requires_policy_and_preserves_provenance():
    invoker = Invoker()
    engine = ProcedureEngine(invoker)
    session = engine.new_session(procedure(replaceable=True))
    engine.preflight(session)
    finish(engine, session)

    update = engine.correct(session, "capture", 30.1)
    assert update.state is SessionState.SUCCEEDED
    attempts = session.attempt_history
    assert attempts[0].result == 31.8
    assert attempts[0].status == "superseded"
    assert attempts[-1].result == 30.1
    assert attempts[-1].supersedes == attempts[0].attempt_id
    assert attempts[-1].status == "completed"
    assert next(item for item in attempts if item.step_id == "fit").status == "stale"
    assert invoker.invocations == [("fit", {})]


def test_correction_is_not_available_for_default_or_action_steps():
    engine = ProcedureEngine(Invoker())
    session = engine.new_session(procedure())
    engine.preflight(session)
    finish(engine, session)
    with pytest.raises(CorrectionUnavailable):
        engine.correct(session, "capture", 30.1)
    with pytest.raises(CorrectionUnavailable):
        engine.correct(session, "fit", "replacement")

    session = engine.new_session(procedure(action_replaceable=True))
    with pytest.raises(CorrectionUnavailable):
        engine.correct(session, "fit", "replacement")


def test_explicit_observation_correction_replaces_value_without_replaying_action():
    invoker = Invoker()
    engine = ProcedureEngine(invoker)
    document = procedure(action_replaceable=True)
    document = compile_procedure({
        "id": "observation", "name": "Observation", "version": 1, "purpose": "test",
        "parameters": {}, "entry_step_id": "step:observe", "default_timeout": 60, "metadata": {},
        "steps": [
            {"id": "observe", "kind": "action", "action": "action:observe",
             "correction": {"mode": "replaceable", "kind": "observation", "invalidates": []},
             "next_step_id": "step:done"},
            {"id": "done", "kind": "complete"},
        ],
    })
    session = engine.new_session(document)
    engine.preflight(session)
    assert engine.advance(session).state is SessionState.WAITING_ACTION
    assert engine.advance(session).state is SessionState.SUCCEEDED
    assert engine.correct(session, "observe", {"temperature": 30.1}).value == {"temperature": 30.1}
    assert invoker.invocations == [("observe", {})]


def test_correction_must_name_existing_steps():
    with pytest.raises(ValueError, match="invalidates.*unknown"):
        compile_procedure({
            "id": "bad-correction", "name": "Bad", "version": 1, "purpose": "test",
            "parameters": {}, "entry_step_id": "step:done", "default_timeout": 60, "metadata": {},
            "steps": [{"id": "done", "kind": "complete",
                       "correction": {"mode": "replaceable", "invalidates": ["unknown"]}}],
        })


def test_rerun_requires_explicit_continue_and_emits_provenance_events():
    invoker = Invoker()
    engine = ProcedureEngine(invoker)
    session = engine.new_session(procedure())
    engine.preflight(session)
    finish(engine, session)
    engine.rerun_from_here(session, "capture")
    assert invoker.invocations == [("fit", {})]
    assert [event.name for event in session.events][-1] == "procedure.rerun_requested"
    engine.advance(session)
    assert invoker.invocations == [("fit", {})]
    assert session.state is SessionState.WAITING_INPUT


def test_rerun_of_action_reuses_one_new_attempt_and_reinvokes_only_on_advance():
    invoker = Invoker()
    engine = ProcedureEngine(invoker)
    session = engine.new_session(procedure())
    engine.preflight(session)
    finish(engine, session)
    assert engine.rerun_from_here(session, "fit").state is SessionState.READY
    assert invoker.invocations == [("fit", {})]
    assert engine.advance(session).state is SessionState.WAITING_ACTION
    assert invoker.invocations == [("fit", {}), ("fit", {})]
    assert engine.advance(session).state is SessionState.SUCCEEDED
    assert [item.number for item in session.attempt_history if item.step_id == "fit"] == [1, 2]
