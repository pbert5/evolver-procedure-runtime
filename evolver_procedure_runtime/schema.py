"""Strict validation and normalization for the #29 procedure document."""
from __future__ import annotations
from collections.abc import Mapping
from typing import Any
from .model import ActionRef, ParameterRef, Procedure, Step, StepKind, StepRef, TypedRef

class SchemaError(ValueError): pass
MAX_INPUT_LENGTH = 4096
MAX_POLL_COUNT = 1000
MAX_TIMEOUT = 86400
_PROCEDURE_KEYS = frozenset({"id", "name", "version", "purpose", "parameters", "entry_step_id", "steps", "abort_actions", "default_timeout", "metadata"})
_COMMON_STEP_KEYS = frozenset({"id", "kind", "next_step_id"})

def _string(value: Any, name: str) -> str:
    if not isinstance(value, str) or not value.strip(): raise SchemaError(f"{name} must be a non-empty string")
    return value

def _ref(value: Any, name: str, ref_type: type[TypedRef] = TypedRef) -> TypedRef:
    if isinstance(value, str):
        if ":" not in value: raise SchemaError(f"{name} must be a typed reference")
        kind, ident = value.split(":", 1)
        if kind not in {"action", "step", "parameter", "ref"}: raise SchemaError(f"{name} has an invalid reference type")
        value = {"type": kind, "id": ident}
    if not isinstance(value, Mapping): raise SchemaError(f"{name} must be a typed reference")
    unknown = set(value) - {"type", "id", "version"}
    if unknown: raise SchemaError(f"unknown {name} fields: {sorted(unknown)}")
    ident = _string(value.get("id"), f"{name}.id")
    expected = ref_type.__dataclass_fields__["type"].default
    kind = value.get("type", expected)
    if kind not in {expected, "ref"}: raise SchemaError(f"{name}.type must be {expected!r}")
    version = value.get("version")
    if version is not None and (isinstance(version, bool) or not isinstance(version, (str, int))): raise SchemaError(f"{name}.version must be a string or integer")
    return ref_type(id=ident, version=version)

def _mapping(value: Any, name: str) -> dict[str, Any]:
    if not isinstance(value, Mapping): raise SchemaError(f"{name} must be an object")
    return dict(value)

def validate_document(document: Any) -> None:
    if not isinstance(document, Mapping): raise SchemaError("procedure must be an object")
    unknown = set(document) - _PROCEDURE_KEYS
    if unknown: raise SchemaError(f"unknown procedure fields: {sorted(unknown)}")
    for key in ("id", "name", "purpose"): _string(document.get(key), key)
    version = document.get("version")
    if isinstance(version, bool) or not isinstance(version, (str, int)) or not str(version): raise SchemaError("version must be a non-empty string or integer")
    _mapping(document.get("parameters"), "parameters")
    _ref(document.get("entry_step_id"), "entry_step_id", StepRef)
    timeout = document.get("default_timeout")
    if type(timeout) is not int or not 1 <= timeout <= MAX_TIMEOUT: raise SchemaError(f"default_timeout must be between 1 and {MAX_TIMEOUT}")
    _mapping(document.get("metadata"), "metadata")
    if "abort_actions" in document and not isinstance(document["abort_actions"], list): raise SchemaError("abort_actions must be an array")
    for ref in document.get("abort_actions", []): _ref(ref, "abort_actions", ActionRef)
    steps = document.get("steps")
    if not isinstance(steps, list) or not steps: raise SchemaError("steps must be a non-empty array")
    ids: set[str] = set()
    for raw in steps:
        if not isinstance(raw, Mapping): raise SchemaError("each step must be an object")
        step_id = _string(raw.get("id"), "step.id")
        if step_id in ids: raise SchemaError(f"duplicate step id: {step_id}")
        ids.add(step_id)
        try: kind = StepKind(raw.get("kind"))
        except ValueError as exc: raise SchemaError("step.kind must be one of action, input, poll, branch, complete") from exc
        allowed = set(_COMMON_STEP_KEYS)
        if kind is StepKind.ACTION:
            allowed |= {"action", "parameters", "timeout_polls", "poll_interval_s"}
            _ref(raw.get("action"), "step.action", ActionRef); _mapping(raw.get("parameters", {}), "step.parameters")
        elif kind is StepKind.INPUT:
            allowed |= {"parameter", "prompt", "max_input_length"}
            _ref(raw.get("parameter"), "step.parameter", ParameterRef); _string(raw.get("prompt"), "step.prompt")
            limit = raw.get("max_input_length")
            if type(limit) is not int or not 1 <= limit <= MAX_INPUT_LENGTH: raise SchemaError(f"step.max_input_length must be between 1 and {MAX_INPUT_LENGTH}")
        elif kind is StepKind.POLL:
            allowed |= {"poll", "timeout_polls", "poll_interval_s"}; _ref(raw.get("poll"), "step.poll")
        elif kind is StepKind.BRANCH:
            allowed |= {"condition", "then_step_id", "else_step_id"}; _ref(raw.get("condition"), "step.condition"); _ref(raw.get("then_step_id"), "step.then_step_id", StepRef); _ref(raw.get("else_step_id"), "step.else_step_id", StepRef)
        unknown_step = set(raw) - allowed
        if unknown_step: raise SchemaError(f"unknown step fields: {sorted(unknown_step)}")
        if "next_step_id" in raw: _ref(raw["next_step_id"], "step.next_step_id", StepRef)
        if kind in {StepKind.ACTION, StepKind.POLL}:
            polls, interval = raw.get("timeout_polls", 1), raw.get("poll_interval_s", 0)
            if type(polls) is not int or not 1 <= polls <= MAX_POLL_COUNT: raise SchemaError(f"step.timeout_polls must be between 1 and {MAX_POLL_COUNT}")
            if isinstance(interval, bool) or not isinstance(interval, (int, float)) or not 0 <= interval <= 60: raise SchemaError("step.poll_interval_s must be between 0 and 60 seconds")
    if _ref(document["entry_step_id"], "entry_step_id", StepRef).id not in ids: raise SchemaError("entry_step_id does not name a step")

def build_procedure(document: Mapping[str, Any]) -> Procedure:
    validate_document(document); steps = []
    for raw in document["steps"]:
        kind = StepKind(raw["kind"])
        steps.append(Step(id=raw["id"], kind=kind, action_ref=_ref(raw["action"], "step.action", ActionRef) if kind is StepKind.ACTION else None, parameters=dict(raw.get("parameters", {})), input_ref=_ref(raw["parameter"], "step.parameter", ParameterRef) if kind is StepKind.INPUT else None, prompt=raw.get("prompt"), max_input_length=raw.get("max_input_length"), poll_ref=_ref(raw["poll"], "step.poll") if kind is StepKind.POLL else None, condition_ref=_ref(raw["condition"], "step.condition") if kind is StepKind.BRANCH else None, timeout_polls=raw.get("timeout_polls", 1), poll_interval_s=float(raw.get("poll_interval_s", 0)), next_step_id=_ref(raw["next_step_id"], "step.next_step_id", StepRef) if "next_step_id" in raw else None, then_step_id=_ref(raw["then_step_id"], "step.then_step_id", StepRef) if kind is StepKind.BRANCH else None, else_step_id=_ref(raw["else_step_id"], "step.else_step_id", StepRef) if kind is StepKind.BRANCH else None))
    return Procedure(id=document["id"], name=document["name"], version=document["version"], purpose=document["purpose"], parameters=dict(document["parameters"]), entry_step_id=_ref(document["entry_step_id"], "entry_step_id", StepRef), steps=tuple(steps), abort_actions=tuple(_ref(ref, "abort_actions", ActionRef) for ref in document.get("abort_actions", [])), default_timeout=document["default_timeout"], metadata=dict(document["metadata"]))
