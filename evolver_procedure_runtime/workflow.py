"""Trusted, session-local composition of interactive procedure sessions.

Workflow composition deliberately owns names, ordering, parameters, and stage
cardinality only.  ProcedureEngine remains the sole execution authority.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Mapping, Sequence

from .engine import ProcedureEngine, ProcedurePreflightError, ProcedureRunError
from .model import AdvanceResult, Procedure, ProcedureSession, SessionState


class WorkflowError(ValueError):
    pass


class WorkflowPreflightError(WorkflowError):
    pass


class Cardinality(str, Enum):
    ONCE = "once"
    REPEATABLE = "repeatable"


class WorkflowState(str, Enum):
    CREATED = "created"
    PREFLIGHTED = "preflighted"
    READY = "ready"
    WAITING_INPUT = "waiting_input"
    WAITING_ACTION = "waiting_action"
    WAITING_CONDITION = "waiting_condition"
    STAGE_COMPLETE = "stage_complete"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    ABORTED = "aborted"


@dataclass(frozen=True)
class WorkflowStage:
    id: str
    procedure_id: str
    procedure_version: str | int
    cardinality: Cardinality = Cardinality.ONCE
    bindings: Mapping[str, Mapping[str, Any]] = field(default_factory=dict)
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class WorkflowDefinition:
    id: str
    name: str
    version: str | int
    category: str
    description: str
    parameters: Mapping[str, Mapping[str, Any]]
    requirements: Mapping[str, Any]
    stages: tuple[WorkflowStage, ...]
    metadata: Mapping[str, Any] = field(default_factory=dict)
    source: str | None = None

    @classmethod
    def from_mapping(cls, document: Mapping[str, Any], *, source: str | None = None) -> "WorkflowDefinition":
        required = {"id", "name", "version", "category", "description", "parameters", "requirements", "stages"}
        missing = required - set(document)
        if missing:
            raise WorkflowError(f"workflow is missing fields: {', '.join(sorted(missing))}")
        stages: list[WorkflowStage] = []
        seen: set[str] = set()
        for raw in document["stages"]:
            if not isinstance(raw, Mapping):
                raise WorkflowError("workflow stage must be a mapping")
            stage_id = raw.get("id")
            if not isinstance(stage_id, str) or not stage_id or stage_id in seen:
                raise WorkflowError(f"duplicate or invalid stage ID: {stage_id!r}")
            seen.add(stage_id)
            procedure = raw.get("procedure")
            if isinstance(procedure, Mapping):
                procedure_id, procedure_version = procedure.get("id"), procedure.get("version")
            elif isinstance(procedure, str) and "@" in procedure:
                procedure_id, procedure_version = procedure.rsplit("@", 1)
            else:
                raise WorkflowError(f"stage {stage_id} has an invalid procedure reference")
            if not isinstance(procedure_id, str) or not procedure_id or procedure_version is None:
                raise WorkflowError(f"stage {stage_id} has an incomplete procedure reference")
            try:
                cardinality = Cardinality(raw.get("cardinality", Cardinality.ONCE))
            except ValueError as exc:
                raise WorkflowError(f"stage {stage_id} has invalid cardinality") from exc
            bindings = raw.get("bindings", {})
            if not isinstance(bindings, Mapping) or any(not isinstance(value, Mapping) for value in bindings.values()):
                raise WorkflowError(f"stage {stage_id} bindings must be named mappings")
            stages.append(WorkflowStage(stage_id, procedure_id, procedure_version, cardinality, dict(bindings), dict(raw.get("metadata", {}))))
        parameters = document["parameters"]
        if not isinstance(parameters, Mapping) or any(not isinstance(value, Mapping) for value in parameters.values()):
            raise WorkflowError("workflow parameters must be named mappings")
        definition = cls(str(document["id"]), str(document["name"]), document["version"], str(document["category"]),
                        str(document["description"]), dict(parameters), dict(document["requirements"]), tuple(stages),
                        dict(document.get("metadata", {})), source)
        definition.validate()
        return definition

    def validate(self) -> None:
        if not self.id or not self.name or not self.category or not self.stages:
            raise WorkflowError("workflow identity and at least one stage are required")
        for name, spec in self.parameters.items():
            if not isinstance(name, str) or not name or "type" not in spec:
                raise WorkflowError(f"workflow parameter {name!r} must declare a type")
        if len({stage.id for stage in self.stages}) != len(self.stages):
            raise WorkflowError("workflow stage IDs must be unique")

    @property
    def digest(self) -> str:
        payload = {"id": self.id, "version": self.version, "stages": [stage.__dict__ for stage in self.stages],
                   "parameters": self.parameters, "requirements": self.requirements}
        encoded = json.dumps(payload, sort_keys=True, default=lambda value: value.value if isinstance(value, Enum) else value).encode()
        return hashlib.sha256(encoded).hexdigest()


class WorkflowLibrary:
    """Deterministic library loaded only from explicitly trusted directories."""

    def __init__(self, definitions: Sequence[WorkflowDefinition]):
        self._definitions = tuple(sorted(definitions, key=lambda item: (item.category, item.name, str(item.version), item.id)))
        keys = [(item.id, item.version) for item in self._definitions]
        if len(set(keys)) != len(keys):
            raise WorkflowError("duplicate workflow ID/version")

    @classmethod
    def from_directories(cls, directories: Sequence[str | Path]) -> "WorkflowLibrary":
        try:
            import yaml
        except ImportError as exc:  # pragma: no cover - package dependency normally supplies this
            raise WorkflowError("PyYAML is required for workflow library discovery") from exc
        definitions: list[WorkflowDefinition] = []
        for root in directories:
            directory = Path(root).resolve()
            if not directory.is_dir():
                raise WorkflowError(f"workflow directory does not exist: {root}")
            for path in sorted(directory.rglob("*.yaml")):
                if path.is_symlink() or not path.is_file():
                    raise WorkflowError(f"untrusted workflow source: {path}")
                document = yaml.safe_load(path.read_text(encoding="utf-8"))
                if not isinstance(document, Mapping):
                    raise WorkflowError(f"workflow document is not a mapping: {path}")
                definitions.append(WorkflowDefinition.from_mapping(document, source=str(path)))
        return cls(definitions)

    def list(self) -> tuple[WorkflowDefinition, ...]:
        return self._definitions

    def search(self, text: str) -> tuple[WorkflowDefinition, ...]:
        needle = text.casefold()
        return tuple(item for item in self._definitions if needle in " ".join((item.id, item.name, item.category, item.description)).casefold())

    def get(self, workflow_id: str, version: str | int | None = None) -> WorkflowDefinition:
        matches = [item for item in self._definitions if item.id == workflow_id and (version is None or item.version == version)]
        if len(matches) != 1:
            raise KeyError(f"workflow not found or ambiguous: {workflow_id}@{version}")
        return matches[0]


@dataclass
class WorkflowStageInstance:
    id: str
    stage: WorkflowStage
    procedure_session: ProcedureSession
    parameters: dict[str, Any]
    completed: bool = False


@dataclass(frozen=True)
class WorkflowAdvanceResult:
    state: WorkflowState
    stage_id: str | None = None
    instance_id: str | None = None
    child: AdvanceResult | None = None
    error: str | None = None


class WorkflowSession:
    """A non-persistent composition of independently controlled procedures."""

    def __init__(self, definition: WorkflowDefinition, engine: ProcedureEngine,
                 procedures: Mapping[tuple[str, str | int], Procedure]):
        self.definition = definition
        self.engine = engine
        self.procedures = dict(procedures)
        self.state = WorkflowState.CREATED
        self.parameters: dict[str, Any] = {}
        self.instances: dict[str, list[WorkflowStageInstance]] = {stage.id: [] for stage in definition.stages}
        self.active_stage_id: str | None = None
        self.active_instance_id: str | None = None
        self.error: str | None = None

    def preflight(self, parameters: Mapping[str, Any] | None = None) -> None:
        if self.state is not WorkflowState.CREATED:
            raise WorkflowPreflightError("workflow session is not new")
        supplied = dict(self.parameters)
        supplied.update(parameters or {})
        self.parameters = self._validate_parameters(supplied)
        for stage in self.definition.stages:
            procedure = self._procedure(stage)
            if stage.cardinality is Cardinality.ONCE and not self._has_output_binding(stage):
                self._create_instance(stage, {}, procedure)
        self.state = WorkflowState.PREFLIGHTED
        self._select_first_unfinished()

    def add_instance(self, stage_id: str, parameters: Mapping[str, Any] | None = None) -> str:
        self._require_preflight()
        stage = self._stage(stage_id)
        if stage.cardinality is not Cardinality.REPEATABLE:
            raise WorkflowError(f"stage is not repeatable: {stage_id}")
        instance = self._create_instance(stage, dict(parameters or {}), self._procedure(stage))
        self.active_stage_id, self.active_instance_id = stage.id, instance.id
        self.state = WorkflowState.READY
        return instance.id

    def provide_parameter(self, name: str, value: Any) -> None:
        if self.state is not WorkflowState.CREATED:
            raise WorkflowError("workflow parameters can only be supplied before preflight")
        if name not in self.definition.parameters:
            raise ValueError(f"workflow parameter is not declared: {name}")
        self.parameters[name] = self._validate_one(name, value)

    def advance(self) -> WorkflowAdvanceResult:
        self._require_preflight()
        if self.active_stage_id is None or self.active_instance_id is None:
            self.state = WorkflowState.SUCCEEDED
            return WorkflowAdvanceResult(self.state)
        instance = self._instance(self.active_stage_id, self.active_instance_id)
        child = self.engine.advance(instance.procedure_session)
        self.state = self._project(child.state)
        if child.state is SessionState.SUCCEEDED:
            instance.completed = True
            self.state = WorkflowState.STAGE_COMPLETE
        elif child.state in {SessionState.FAILED, SessionState.ABORTED}:
            self.error = child.error
        return WorkflowAdvanceResult(self.state, self.active_stage_id, instance.id, child, child.error)

    def continue_stage(self) -> WorkflowAdvanceResult:
        """Explicitly select the next stage; this never invokes a procedure action."""
        self._require_preflight()
        if self.active_stage_id and self.active_instance_id:
            active = self._instance(self.active_stage_id, self.active_instance_id)
            if not active.completed:
                raise WorkflowError("active stage must complete before continuation")
        if self.active_stage_id is None:
            self._select_first_unfinished()
            return WorkflowAdvanceResult(self.state, self.active_stage_id, self.active_instance_id)
        index = next(i for i, stage in enumerate(self.definition.stages) if stage.id == self.active_stage_id)
        for stage in self.definition.stages[index + 1:]:
            instances = self.instances[stage.id]
            if not instances and stage.cardinality is Cardinality.ONCE:
                instance = self._create_instance(stage, {}, self._procedure(stage))
                self.active_stage_id, self.active_instance_id = stage.id, instance.id
                self.state = WorkflowState.READY
                return WorkflowAdvanceResult(self.state, stage.id, instance.id)
            if not instances and stage.cardinality is Cardinality.REPEATABLE:
                self.active_stage_id, self.active_instance_id = stage.id, None
                self.state = WorkflowState.READY
                return WorkflowAdvanceResult(self.state, stage.id)
            for instance in instances:
                if not instance.completed:
                    self.active_stage_id, self.active_instance_id = stage.id, instance.id
                    self.state = WorkflowState.READY
                    return WorkflowAdvanceResult(self.state, stage.id, instance.id)
        self.active_stage_id = self.active_instance_id = None
        self.state = WorkflowState.SUCCEEDED
        return WorkflowAdvanceResult(self.state)

    def abort(self, reason: str = "aborted") -> WorkflowAdvanceResult:
        if self.active_stage_id and self.active_instance_id:
            instance = self._instance(self.active_stage_id, self.active_instance_id)
            child = self.engine.abort(instance.procedure_session, reason)
            self.state, self.error = WorkflowState.ABORTED, reason
            return WorkflowAdvanceResult(self.state, self.active_stage_id, instance.id, child, reason)
        self.state, self.error = WorkflowState.ABORTED, reason
        return WorkflowAdvanceResult(self.state, error=reason)

    @property
    def attention(self) -> bool:
        return self.state in {WorkflowState.WAITING_INPUT, WorkflowState.WAITING_CONDITION}

    def _create_instance(self, stage: WorkflowStage, parameters: Mapping[str, Any], procedure: Procedure) -> WorkflowStageInstance:
        session = self.engine.new_session(procedure)
        self.engine.preflight(session)
        resolved = self._bindings(stage, parameters, procedure)
        for name, value in resolved.items():
            self.engine.provide_parameter(session, name, value)
        instance = WorkflowStageInstance(f"{stage.id}-{len(self.instances[stage.id]) + 1}", stage, session, dict(resolved))
        self.instances[stage.id].append(instance)
        return instance

    def _bindings(self, stage: WorkflowStage, instance_parameters: Mapping[str, Any], procedure: Procedure) -> dict[str, Any]:
        result: dict[str, Any] = {}
        instance_sources = {binding["instance_parameter"] for binding in stage.bindings.values()
                            if set(binding) == {"instance_parameter"}}
        unknown_instance = set(instance_parameters) - instance_sources
        if unknown_instance:
            raise WorkflowPreflightError(f"unknown stage instance parameters: {', '.join(sorted(unknown_instance))}")
        for target, binding in stage.bindings.items():
            if set(binding) == {"workflow_parameter"}:
                source = binding["workflow_parameter"]
                if source not in self.parameters:
                    raise WorkflowPreflightError(f"workflow parameter is unset: {source}")
                result[target] = self.parameters[source]
            elif set(binding) == {"instance_parameter"}:
                source = binding["instance_parameter"]
                if source not in instance_parameters:
                    raise WorkflowPreflightError(f"instance parameter is unset: {source}")
                result[target] = instance_parameters[source]
            elif set(binding) == {"output_ref"}:
                reference = binding["output_ref"]
                if not isinstance(reference, Mapping) or set(reference) - {"stage", "instance", "result_index"} or "stage" not in reference or "result_index" not in reference:
                    raise WorkflowPreflightError(f"malformed output binding for {target}")
                source_instances = self.instances.get(reference["stage"], [])
                if reference.get("instance", "last") == "last":
                    source_instance = next((item for item in reversed(source_instances) if item.completed), None)
                else:
                    try:
                        source_instance = source_instances[int(reference["instance"])]
                    except (ValueError, TypeError, IndexError):
                        source_instance = None
                index = reference["result_index"]
                if source_instance is None or not source_instance.completed or not isinstance(index, int) or index < 0 or index >= len(source_instance.procedure_session.results):
                    raise WorkflowPreflightError(f"output reference is not available for {target}")
                result[target] = source_instance.procedure_session.results[index]
            else:
                raise WorkflowPreflightError(f"unsupported binding for {target}")
        unknown = set(result) - set(procedure.parameters)
        if unknown:
            raise WorkflowPreflightError(f"binding targets unknown procedure parameters: {', '.join(sorted(unknown))}")
        return result

    def _validate_parameters(self, parameters: Mapping[str, Any]) -> dict[str, Any]:
        unknown = set(parameters) - set(self.definition.parameters)
        if unknown:
            raise WorkflowPreflightError(f"unknown workflow parameters: {', '.join(sorted(unknown))}")
        missing = {name for name, spec in self.definition.parameters.items()
                   if spec.get("required", True) and name not in parameters and "default" not in spec}
        if missing:
            raise WorkflowPreflightError(f"required workflow parameters are unset: {', '.join(sorted(missing))}")
        values = dict(parameters)
        values.update({name: spec["default"] for name, spec in self.definition.parameters.items()
                       if name not in values and "default" in spec})
        return {name: self._validate_one(name, value) for name, value in values.items()}

    def _validate_one(self, name: str, value: Any) -> Any:
        spec = self.definition.parameters[name]
        expected = spec.get("type")
        valid = {"integer": type(value) is int, "number": type(value) in {int, float}, "string": isinstance(value, str), "boolean": type(value) is bool}.get(expected, True)
        if not valid:
            raise WorkflowPreflightError(f"invalid workflow parameter type: {name}")
        if "values" in spec and value not in spec["values"]:
            raise WorkflowPreflightError(f"invalid workflow parameter value: {name}")
        if "minimum" in spec and value < spec["minimum"]:
            raise WorkflowPreflightError(f"workflow parameter is below minimum: {name}")
        if "maximum" in spec and value > spec["maximum"]:
            raise WorkflowPreflightError(f"workflow parameter is above maximum: {name}")
        return value

    def _procedure(self, stage: WorkflowStage) -> Procedure:
        try:
            return self.procedures[(stage.procedure_id, stage.procedure_version)]
        except KeyError as exc:
            raise WorkflowPreflightError(f"unknown procedure reference: {stage.procedure_id}@{stage.procedure_version}") from exc

    def _stage(self, stage_id: str) -> WorkflowStage:
        try:
            return next(stage for stage in self.definition.stages if stage.id == stage_id)
        except StopIteration as exc:
            raise WorkflowError(f"unknown workflow stage: {stage_id}") from exc

    def _instance(self, stage_id: str, instance_id: str) -> WorkflowStageInstance:
        return next(instance for instance in self.instances[stage_id] if instance.id == instance_id)

    def _select_first_unfinished(self) -> None:
        for stage in self.definition.stages:
            if self.instances[stage.id]:
                instance = next((item for item in self.instances[stage.id] if not item.completed), None)
                if instance:
                    self.active_stage_id, self.active_instance_id = stage.id, instance.id
                    self.state = WorkflowState.READY
                    return
            if stage.cardinality is Cardinality.ONCE:
                instance = self._create_instance(stage, {}, self._procedure(stage))
                self.active_stage_id, self.active_instance_id = stage.id, instance.id
                self.state = WorkflowState.READY
                return
        self.active_stage_id = self.active_instance_id = None
        self.state = WorkflowState.SUCCEEDED

    def _require_preflight(self) -> None:
        if self.state is WorkflowState.CREATED:
            raise WorkflowPreflightError("workflow preflight is required")

    @staticmethod
    def _has_output_binding(stage: WorkflowStage) -> bool:
        return any(set(binding) == {"output_ref"} for binding in stage.bindings.values())

    @staticmethod
    def _project(state: SessionState) -> WorkflowState:
        return {SessionState.WAITING_INPUT: WorkflowState.WAITING_INPUT,
                SessionState.WAITING_ACTION: WorkflowState.WAITING_ACTION,
                SessionState.WAITING_CONDITION: WorkflowState.WAITING_CONDITION,
                SessionState.FAILED: WorkflowState.FAILED,
                SessionState.ABORTED: WorkflowState.ABORTED,
                SessionState.SUCCEEDED: WorkflowState.STAGE_COMPLETE}.get(state, WorkflowState.READY)
