"""Immutable, declarative procedure contract and ephemeral session values."""
from __future__ import annotations
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Mapping
from uuid import uuid4

class StepKind(str, Enum):
    ACTION = "action"
    INPUT = "input"
    POLL = "poll"
    BRANCH = "branch"
    COMPLETE = "complete"

class SessionState(str, Enum):
    CREATED = "created"
    PREFLIGHTED = "preflighted"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"

@dataclass(frozen=True)
class TypedRef:
    id: str
    version: str | int | None = None
    type: str = "ref"

@dataclass(frozen=True)
class ActionRef(TypedRef):
    type: str = "action"

@dataclass(frozen=True)
class StepRef(TypedRef):
    type: str = "step"

@dataclass(frozen=True)
class ParameterRef(TypedRef):
    type: str = "parameter"

@dataclass(frozen=True)
class ActionResult:
    status: str
    value: Any = None
    error: str | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)

    @property
    def succeeded(self) -> bool:
        return self.status in {"succeeded", "success", "ok"}

@dataclass(frozen=True)
class Step:
    id: str
    kind: StepKind = StepKind.ACTION
    action_ref: ActionRef | None = None
    parameters: Mapping[str, Any] = field(default_factory=dict)
    input_ref: ParameterRef | None = None
    prompt: str | None = None
    max_input_length: int | None = None
    poll_ref: TypedRef | None = None
    condition_ref: TypedRef | None = None
    timeout_polls: int = 1
    poll_interval_s: float = 0.0
    next_step_id: StepRef | None = None
    then_step_id: StepRef | None = None
    else_step_id: StepRef | None = None

    @property
    def action(self) -> str:
        if self.action_ref is None:
            raise AttributeError("step has no action")
        return self.action_ref.id

@dataclass(frozen=True)
class Procedure:
    id: str
    name: str
    version: str | int
    purpose: str
    parameters: Mapping[str, Any]
    entry_step_id: StepRef
    steps: tuple[Step, ...]
    abort_actions: tuple[ActionRef, ...] = ()
    default_timeout: int = 60
    metadata: Mapping[str, Any] = field(default_factory=dict)

@dataclass(frozen=True)
class PrimaryOutcome:
    """The terminal procedure reason, fixed before cleanup begins."""
    kind: str
    reason: str
    step_id: str | None = None

@dataclass(frozen=True)
class CleanupActionOutcome:
    action: ActionRef
    status: str
    error: str | None = None

@dataclass
class CleanupOutcome:
    """Run-owned cleanup result; it never replaces ``PrimaryOutcome``."""
    status: str = "not_attempted"
    actions: list[CleanupActionOutcome] = field(default_factory=list)

@dataclass
class ProcedureSession:
    """Ephemeral execution state; deliberately has no persistence/resume API."""
    procedure: Procedure
    state: SessionState = SessionState.CREATED
    current_step: int = 0
    results: list[Any] = field(default_factory=list)
    inputs: dict[str, Any] = field(default_factory=dict)
    error: str | None = None
    run_id: str = field(default_factory=lambda: str(uuid4()))
    controller_generation: int | None = None
    primary_outcome: PrimaryOutcome | None = None
    cleanup_outcome: CleanupOutcome = field(default_factory=CleanupOutcome)
    cleanup_attempted: bool = False
