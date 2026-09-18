"""Bounded procedure event recording and observer dispatch.

Events are an operator-facing projection of a procedure run.  They are not an
execution log: an observer is never retried and an event failure must not make
the underlying action happen twice.
"""
from __future__ import annotations

import json
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from .invoker import ActionInvocation, ActionInvoker, PollResult
from .model import ActionRef

MAX_PAYLOAD_KEYS = 32
MAX_PAYLOAD_DEPTH = 4
MAX_PAYLOAD_STRING = 256
MAX_OBSERVER_ERRORS = 16
MAX_OBSERVER_ERROR_STRING = 256
MAX_HISTORY_EVENTS = 256
PROCEDURE_EVENT_TYPE = "procedure-event/1"


class EvidenceStrength(str, Enum):
    """What an event establishes; an acknowledgement is not physical proof."""

    NONE = "none"
    INTENT = "intent"
    ACKNOWLEDGED = "acknowledged"
    OBSERVED = "observed"

    # Short names are convenient in serialized contracts.
    ACK = ACKNOWLEDGED


_SECRET_PARTS = (
    "password", "passwd", "secret", "token", "credential", "api_key",
    "apikey", "private_key", "authorization", "cookie", "invocation",
)


def _secret_key(key: Any) -> bool:
    normalized = str(key).lower().replace("-", "_")
    return any(part in normalized for part in _SECRET_PARTS)


def redact_payload(value: Any, *, _depth: int = 0) -> Any:
    """Return a bounded, structurally redacted copy of *value*.

    Redaction is based on mapping keys before recursion, so a secret nested in
    a list or object cannot leak through a stringified payload.
    """
    if _depth >= MAX_PAYLOAD_DEPTH:
        return "<depth-limited>"
    if isinstance(value, Mapping):
        result: dict[str, Any] = {}
        items = sorted(value.items(), key=lambda item: str(item[0]))
        for index, (key, nested) in enumerate(items):
            if index >= MAX_PAYLOAD_KEYS:
                result["<more>"] = "<items-limited>"
                break
            name = str(key)
            result[name] = "<redacted>" if _secret_key(name) else redact_payload(nested, _depth=_depth + 1)
        return result
    if isinstance(value, (list, tuple, set, frozenset)):
        source = sorted(value, key=repr) if isinstance(value, (set, frozenset)) else list(value)
        values = [redact_payload(item, _depth=_depth + 1) for item in source[:MAX_PAYLOAD_KEYS]]
        if len(value) > MAX_PAYLOAD_KEYS:
            values.append("<items-limited>")
        return values
    if isinstance(value, str):
        # Exception text and opaque values have no mapping key to protect.
        # Remove common token/credential assignments before applying the bound.
        value = re.sub(
            r"(?i)(\b(?:token|secret|password|credential|authorization|api[_-]?key)\s*[:=]\s*)[^,\s;]+",
            r"\1<redacted>", value,
        )
        return value[:MAX_PAYLOAD_STRING] + ("<truncated>" if len(value) > MAX_PAYLOAD_STRING else "")
    if isinstance(value, (int, float, bool)) or value is None:
        return value
    return str(value)[:MAX_PAYLOAD_STRING]


@dataclass(frozen=True)
class ProcedureEvent:
    sequence: int
    name: str
    procedure_id: str
    run_id: str
    procedure_revision: str | int
    controller_generation: int
    evidence: EvidenceStrength = EvidenceStrength.NONE
    payload: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not isinstance(self.sequence, int) or self.sequence < 0:
            raise ValueError("event sequence must be a non-negative integer")
        for name, value in (("procedure_id", self.procedure_id), ("run_id", self.run_id)):
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{name} is required")
        if (isinstance(self.procedure_revision, bool)
                or not isinstance(self.procedure_revision, (str, int))
                or not str(self.procedure_revision).strip()):
            raise ValueError("procedure revision is required")
        if (isinstance(self.controller_generation, bool)
                or not isinstance(self.controller_generation, int)
                or self.controller_generation < 0):
            raise ValueError("controller generation is required")

    def as_dict(self) -> dict[str, Any]:
        # Keep the event fields flat for the existing operator projection while
        # making the wire type and the fencing identity mandatory and explicit.
        return {
            "type": PROCEDURE_EVENT_TYPE,
            "sequence": self.sequence,
            "name": self.name,
            "evidence": self.evidence.value,
            "procedure_id": self.procedure_id,
            "run_id": self.run_id,
            "procedure_revision": self.procedure_revision,
            "controller_generation": self.controller_generation,
            "payload": redact_payload(self.payload),
        }

    def serialize(self) -> str:
        """Serialize the versioned event envelope deterministically."""
        return json.dumps(self.as_dict(), sort_keys=True, separators=(",", ":"))

    def is_for(self, *, procedure_id: str, run_id: str,
               procedure_revision: str | int,
               controller_generation: int) -> bool:
        """Return whether this event belongs to the supplied fenced run."""
        return (
            self.procedure_id == procedure_id
            and self.run_id == run_id
            and self.procedure_revision == procedure_revision
            and self.controller_generation == controller_generation
        )


class RequiredObserverError(RuntimeError):
    """A required observer failed; the next physical action must not run."""


@dataclass(frozen=True)
class _Observer:
    callback: Callable[[ProcedureEvent], Any]
    required: bool


class ProcedureEventStream:
    """Deterministic event history plus an independent latest-event snapshot."""

    def __init__(self, observers: Mapping[Callable[[ProcedureEvent], Any], bool] | None = None,
                 *, procedure_id: str, run_id: str,
                 procedure_revision: str | int,
                 controller_generation: int) -> None:
        self._history: list[ProcedureEvent] = []
        self._next_sequence = 0
        self._current: dict[str, ProcedureEvent] = {}
        self.procedure_id = procedure_id
        self.run_id = run_id
        self.procedure_revision = procedure_revision
        self.controller_generation = controller_generation
        self._observers: list[_Observer] = [
            _Observer(callback, required) for callback, required in (observers or {}).items()
        ]
        self.observer_errors: list[str] = []
        ProcedureEvent(0, "_fence", procedure_id, run_id, procedure_revision,
                       controller_generation)

    def add_observer(self, callback: Callable[[ProcedureEvent], Any], *, required: bool = False) -> None:
        self._observers.append(_Observer(callback, required))

    def emit(self, name: str, payload: Mapping[str, Any] | None = None, *, evidence: EvidenceStrength = EvidenceStrength.NONE) -> ProcedureEvent:
        if not isinstance(evidence, EvidenceStrength):
            evidence = EvidenceStrength(evidence)
        event = ProcedureEvent(
            sequence=self._next_sequence, name=name,
            procedure_id=self.procedure_id, run_id=self.run_id,
            procedure_revision=self.procedure_revision,
            controller_generation=self.controller_generation,
            evidence=evidence, payload=redact_payload(payload or {}),
        )
        self._next_sequence += 1
        self._history.append(event)
        del self._history[:-MAX_HISTORY_EVENTS]
        self._current[name] = event
        for observer in self._observers:
            required_failure = False
            try:
                observer.callback(event)
            except Exception as exc:
                error = redact_payload(f"{type(exc).__name__}: {exc}")
                self.observer_errors.append(str(error)[:MAX_OBSERVER_ERROR_STRING])
                del self.observer_errors[:-MAX_OBSERVER_ERRORS]
                if observer.required:
                    required_failure = True
            if required_failure:
                # Raise outside the except suite so Python does not attach the
                # observer exception as an implicit context.
                raise RequiredObserverError("required observer failed")
        return event

    @property
    def history(self) -> tuple[ProcedureEvent, ...]:
        return tuple(self._history)

    @property
    def current(self) -> dict[str, ProcedureEvent]:
        return dict(self._current)

    def snapshot(self) -> dict[str, Any]:
        return {name: event.as_dict() for name, event in self._current.items()}


class ObservedInvoker:
    """Add event gates to an invoker without changing action semantics."""

    def __init__(self, invoker: ActionInvoker, events: ProcedureEventStream, *, abort_actions: tuple[ActionRef, ...] = ()) -> None:
        self._invoker = invoker
        self.events = events
        # Kept as a compatibility-only argument while cleanup ownership moves
        # to ProcedureEngine's run-owned coordinator.
        del abort_actions

    def preflight(self, action: ActionRef | str, parameters: dict[str, Any]) -> None:
        self._invoker.preflight(action, parameters)

    def describe(self, action: ActionRef | str) -> Any:
        describe = getattr(self._invoker, "describe", None)
        return describe(action) if callable(describe) else None

    def invoke(self, action: ActionRef | str, parameters: dict[str, Any]) -> ActionInvocation:
        try:
            self.events.emit("action.requested", {"action": getattr(action, "id", action), "parameters": parameters}, evidence=EvidenceStrength.INTENT)
        except RequiredObserverError:
            raise
        invocation = self._invoker.invoke(action, parameters)
        try:
            self.events.emit("action.accepted", {"action": getattr(action, "id", action), "invocation_token": getattr(invocation, "token", "")}, evidence=EvidenceStrength.ACKNOWLEDGED)
        except RequiredObserverError:
            raise
        return invocation

    def poll(self, invocation: ActionInvocation) -> PollResult:
        result = self._invoker.poll(invocation)
        if result.done:
            try:
                self.events.emit(
                    "action.completed", {"succeeded": result.succeeded, "error": result.error},
                    evidence=_completion_evidence(result),
                )
            except RequiredObserverError:
                raise
        return result


def _completion_evidence(result: PollResult) -> EvidenceStrength:
    """Only explicit underlying evidence can establish an observation.

    A successful software poll is an acknowledgement, not a physical or
    scientific observation. PollResult.value is untrusted Any and the frozen
    contract has no typed evidence-strength authority, so it cannot elevate
    this event.
    """
    if not result.succeeded:
        return EvidenceStrength.ACKNOWLEDGED
    return EvidenceStrength.ACKNOWLEDGED


# Friendly aliases for callers that describe the component as a recorder.
EventRecorder = ProcedureEventStream
EventObserverError = RequiredObserverError
