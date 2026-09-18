from __future__ import annotations

import pytest

from evolver_procedure_runtime import ActionRef, ActionResult, ProcedureCompileError, StepKind, compile_procedure


def document(**overrides):
    value = {
        "id": "demo", "name": "Demo", "version": 1, "purpose": "contract test",
        "parameters": {"answer": {"type": "string"}},
        "entry_step_id": {"type": "step", "id": "act"},
        "steps": [
            {"id": "act", "kind": "action", "action": {"type": "action", "id": "measure", "version": 1}, "parameters": {}, "timeout_polls": 2, "poll_interval_s": 0},
            {"id": "done", "kind": "complete"},
        ],
        "abort_actions": [{"type": "action", "id": "stop"}], "default_timeout": 60, "metadata": {},
    }
    value.update(overrides)
    return value


def test_compiles_top_level_contract_and_typed_refs():
    procedure = compile_procedure(document())
    assert procedure.name == "Demo"
    assert procedure.entry_step_id.id == "act"
    assert procedure.steps[0].action_ref == ActionRef("measure", 1)
    assert procedure.steps[0].kind is StepKind.ACTION


@pytest.mark.parametrize("kind", ["action", "input", "poll", "branch", "complete"])
def test_all_five_step_kinds_are_named(kind):
    raw = document(steps=[{"id": "s", "kind": "complete"}], entry_step_id={"type": "step", "id": "s"})
    if kind == "action": raw["steps"] = [{"id": "s", "kind": kind, "action": {"type": "action", "id": "a"}}]
    elif kind == "input": raw["steps"] = [{"id": "s", "kind": kind, "parameter": {"type": "parameter", "id": "answer"}, "prompt": "Answer", "max_input_length": 20}]
    elif kind == "poll": raw["steps"] = [{"id": "s", "kind": kind, "poll": {"type": "ref", "id": "ready"}}]
    elif kind == "branch": raw["steps"] = [{"id": "s", "kind": kind, "condition": {"type": "ref", "id": "ready"}, "then_step_id": {"type": "step", "id": "s"}, "else_step_id": {"type": "step", "id": "s"}}]
    assert compile_procedure(raw).steps[0].kind.value == kind


def test_rejects_untyped_refs_and_unbounded_controls():
    with pytest.raises(ProcedureCompileError): compile_procedure(document(entry_step_id="act"))
    with pytest.raises(ProcedureCompileError): compile_procedure(document(steps=[{"id": "s", "kind": "input", "parameter": {"type": "parameter", "id": "x"}, "prompt": "x", "max_input_length": 4097}], entry_step_id={"type": "step", "id": "s"}))
    with pytest.raises(ProcedureCompileError): compile_procedure(document(steps=[{"id": "s", "kind": "action", "action": {"type": "action", "id": "a"}, "timeout_polls": 1001}], entry_step_id={"type": "step", "id": "s"}))


def test_action_result_is_structured_and_no_execution_surface_exists():
    result = ActionResult(status="succeeded", value={"ok": True})
    assert result.succeeded and result.error is None
    assert not hasattr(result, "command")
