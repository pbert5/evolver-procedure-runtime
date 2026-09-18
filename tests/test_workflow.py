from pathlib import Path

import pytest

from evolver_procedure_runtime import (
    ActionInvocation, Cardinality, PollResult, ProcedureEngine, WorkflowDefinition,
    WorkflowLibrary, WorkflowPreflightError, WorkflowSession, WorkflowState,
    compile_procedure,
)


class Invoker:
    controller_generation = 1

    def __init__(self):
        self.invocations = []

    def describe(self, action):
        return {"id": action.id, "version": action.version, "authorized": True, "controller_generation": 1}

    def preflight(self, action, parameters):
        pass

    def invoke(self, action, parameters):
        self.invocations.append((action.id, parameters))
        return ActionInvocation(f"{action.id}-{len(self.invocations)}")

    def poll(self, invocation):
        return PollResult(done=True, value=invocation.token)


def procedure(identifier="p"):
    return compile_procedure({
        "id": identifier, "name": identifier, "version": 1, "purpose": "test",
        "parameters": {"value": {"type": "integer"}}, "entry_step_id": "step:input",
        "steps": [
            {"id": "input", "kind": "input", "parameter": "parameter:value", "prompt": "value", "max_input_length": 8,
             "next_step_id": "step:run"},
            {"id": "run", "kind": "action", "action": "action:run", "parameters": {"value": {"parameter_ref": "value"}},
             "next_step_id": "step:done"},
            {"id": "done", "kind": "complete"},
        ], "abort_actions": [], "default_timeout": 60, "metadata": {},
    })


def definition(cardinality="once"):
    bindings = {"value": {"instance_parameter": "value"}} if cardinality == "repeatable" else {"value": {"workflow_parameter": "initial"}}
    return WorkflowDefinition.from_mapping({
        "id": "test.workflow", "name": "Test workflow", "version": 1, "category": "test",
        "description": "workflow composition", "parameters": {"initial": {"type": "integer", "minimum": 0, "maximum": 10}},
        "requirements": {"capabilities": ["run"]}, "stages": [
            {"id": "setup", "procedure": {"id": "p", "version": 1}, "cardinality": cardinality,
             "bindings": bindings},
        ], "metadata": {"tags": ["fixture"]},
    })


def test_definition_rejects_duplicate_stage_ids_and_bad_cardinality():
    raw = {"id": "x", "name": "X", "version": 1, "category": "x", "description": "x", "parameters": {}, "requirements": {},
           "stages": [{"id": "same", "procedure": {"id": "p", "version": 1}}, {"id": "same", "procedure": {"id": "p", "version": 1}}]}
    with pytest.raises(ValueError, match="duplicate"):
        WorkflowDefinition.from_mapping(raw)
    raw["stages"] = [{"id": "s", "procedure": {"id": "p", "version": 1}, "cardinality": "loop"}]
    with pytest.raises(ValueError, match="cardinality"):
        WorkflowDefinition.from_mapping(raw)


def test_preflight_validates_parameters_and_unknown_procedure_without_action():
    invoker = Invoker()
    session = WorkflowSession(definition(), ProcedureEngine(invoker), {("p", 1): procedure()})
    with pytest.raises(ValueError, match="above maximum"):
        session.provide_parameter("initial", 11)
    with pytest.raises(WorkflowPreflightError, match="required workflow parameters"):
        WorkflowSession(definition(), ProcedureEngine(invoker), {("p", 1): procedure()}).preflight()
    with pytest.raises(WorkflowPreflightError, match="unknown workflow parameters"):
        session.preflight({"nope": 1})
    assert invoker.invocations == []

    missing = WorkflowSession(definition(), ProcedureEngine(invoker), {})
    with pytest.raises(WorkflowPreflightError, match="unknown procedure"):
        missing.preflight({"initial": 1})
    assert invoker.invocations == []


def test_once_workflow_requires_explicit_continue_after_child_completion():
    invoker = Invoker()
    session = WorkflowSession(definition(), ProcedureEngine(invoker), {("p", 1): procedure()})
    session.preflight({"initial": 4})
    assert session.state is WorkflowState.READY
    assert invoker.invocations == []
    with pytest.raises(ValueError, match="must complete"):
        session.continue_stage()
    assert session.advance().state is WorkflowState.WAITING_ACTION
    assert session.advance().state is WorkflowState.STAGE_COMPLETE
    assert len(invoker.invocations) == 1
    assert session.state is WorkflowState.STAGE_COMPLETE

    aborted = WorkflowSession(definition(), ProcedureEngine(Invoker()), {("p", 1): procedure()})
    aborted.preflight({"initial": 4})
    assert aborted.abort("operator cancelled").state is WorkflowState.ABORTED
    assert aborted.attention is False


def test_repeatable_stage_creates_independent_instances_and_library_is_deterministic(tmp_path: Path):
    invoker = Invoker()
    session = WorkflowSession(definition("repeatable"), ProcedureEngine(invoker), {("p", 1): procedure()})
    session.preflight({"initial": 2})
    first = session.add_instance("setup", {"value": 9})
    second = session.add_instance("setup", {"value": 10})
    assert first != second
    assert len(session.instances["setup"]) == 2
    assert session.instances["setup"][0].procedure_session is not session.instances["setup"][1].procedure_session
    once = WorkflowSession(definition(), ProcedureEngine(invoker), {("p", 1): procedure()})
    once.preflight({"initial": 2})
    with pytest.raises(ValueError, match="not repeatable"):
        once.add_instance("setup", {})
    with pytest.raises(WorkflowPreflightError, match="unknown stage instance"):
        session.add_instance("setup", {"not_a_binding": 1})

    path = tmp_path / "x.yaml"
    path.write_text("""id: z.workflow\nname: Zed\nversion: 1\ncategory: calibration\ndescription: z\nparameters: {}\nrequirements: {}\nstages:\n  - id: one\n    procedure: {id: p, version: 1}\n    cardinality: once\n""")
    other = tmp_path / "a.yaml"
    other.write_text(path.read_text().replace("z.workflow", "a.workflow").replace("Zed", "Aardvark"))
    library = WorkflowLibrary.from_directories([tmp_path])
    assert [item.id for item in library.list()] == ["a.workflow", "z.workflow"]
    assert library.search("aard")[0].id == "a.workflow"
