"""Immutable, declarative procedure contract and ephemeral session values."""
from __future__ import annotations
from dataclasses import dataclass, field, replace
from enum import Enum
from typing import Any, Mapping
from uuid import uuid4

class StepKind(str, Enum):
    ACTION = "action"
    INPUT = "input"
    POLL = "poll"
    BRANCH = "branch"
    COMPLETE = "complete"
    CHECKPOINT = "checkpoint"


class CorrectionMode(str, Enum):
    NONE = "none"
    REPLACEABLE = "replaceable"

class SessionState(str, Enum):
    CREATED = "created"
    PREFLIGHTED = "preflighted"
    READY = "ready"
    RUNNING = "running"
    WAITING_INPUT = "waiting_input"
    WAITING_ACTION = "waiting_action"
    WAITING_CONDITION = "waiting_condition"
    PAUSED = "paused"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    ABORTED = "aborted"

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
class CorrectionPolicy:
    mode: CorrectionMode = CorrectionMode.NONE
    kind: str = "input"
    invalidates: tuple[str, ...] = ()

    @property
    def replaceable(self) -> bool:
        return self.mode is CorrectionMode.REPLACEABLE

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
    sink_id: str | None = None
    sink_payload: Mapping[str, Any] = field(default_factory=dict)
    sink_required: bool = True
    sink_idempotency_key: str | None = None
    correction: CorrectionPolicy = field(default_factory=CorrectionPolicy)

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


@dataclass(frozen=True)
class AdvanceResult:
    """Bounded, caller-visible result of one incremental transition."""

    state: SessionState
    input_parameter: str | None = None
    input_prompt: str | None = None
    input_max_length: int | None = None
    next_poll_at: float | None = None
    value: Any = None
    error: str | None = None


@dataclass(frozen=True)
class Attempt:
    """An immutable execution occurrence; session projections add lifecycle status."""

    attempt_id: str
    step_id: str
    number: int
    status: str
    result: Any = None
    evidence: Mapping[str, Any] = field(default_factory=dict)
    inputs: Mapping[str, Any] = field(default_factory=dict)
    supersedes: str | None = None


@dataclass(frozen=True)
class SessionEvent:
    name: str
    run_id: str
    step_id: str | None = None
    attempt_id: str | None = None
    payload: Mapping[str, Any] = field(default_factory=dict)

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
    warnings: list[str] = field(default_factory=list)
    current_step_id: str | None = None
    pending_invocation: Any = None
    pending_poll_count: int = 0
    next_poll_at: float | None = None
    deadline: float | None = None
    _attempts: list[Attempt] = field(default_factory=list, repr=False)
    _attempt_status: dict[str, str] = field(default_factory=dict, repr=False)
    _pending_attempt_id: str | None = field(default=None, repr=False)
    events: list[SessionEvent] = field(default_factory=list)

    @property
    def attempt_history(self) -> tuple[Attempt, ...]:
        """Return immutable attempt snapshots, including visible invalidation state."""
        return tuple(
            replace(attempt, status=self._attempt_status.get(attempt.attempt_id, attempt.status))
            for attempt in self._attempts
        )
