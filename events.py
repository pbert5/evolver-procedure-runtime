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
    evidence: EvidenceStrength = EvidenceStrength.NONE
    payload: Mapping[str, Any] = field(default_factory=dict)
    procedure_id: str | None = None
    run_id: str | None = None
    procedure_revision: str | int | None = None
    controller_generation: int | None = None

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
               procedure_revision: str | int | None = None,
               controller_generation: int | None = None) -> bool:
        """Return whether this event belongs to the supplied fenced run."""
        return (
            self.procedure_id == procedure_id
            and self.run_id == run_id
            and (procedure_revision is None or self.procedure_revision == procedure_revision)
            and (controller_generation is None or self.controller_generation == controller_generation)
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
                 *, procedure_id: str | None = None, run_id: str | None = None,
                 procedure_revision: str | int | None = None,
                 controller_generation: int | None = None) -> None:
        self._history: list[ProcedureEvent] = []
        self._current: dict[str, ProcedureEvent] = {}
        self.procedure_id = procedure_id
        self.run_id = run_id
        self.procedure_revision = procedure_revision
        self.controller_generation = controller_generation
        self._observers: list[_Observer] = [
            _Observer(callback, required) for callback, required in (observers or {}).items()
        ]
        self.observer_errors: list[str] = []

    def add_observer(self, callback: Callable[[ProcedureEvent], Any], *, required: bool = False) -> None:
        self._observers.append(_Observer(callback, required))

    def emit(self, name: str, payload: Mapping[str, Any] | None = None, *, evidence: EvidenceStrength = EvidenceStrength.NONE) -> ProcedureEvent:
        if not isinstance(evidence, EvidenceStrength):
            evidence = EvidenceStrength(evidence)
        event = ProcedureEvent(
            len(self._history), name, evidence, redact_payload(payload or {}),
            self.procedure_id, self.run_id, self.procedure_revision,
            self.controller_generation,
        )
        self._history.append(event)
        self._current[name] = event
        for observer in self._observers:
            try:
                observer.callback(event)
            except Exception as exc:
                error = redact_payload(f"{type(exc).__name__}: {exc}")
                self.observer_errors.append(str(error)[:MAX_OBSERVER_ERROR_STRING])
                del self.observer_errors[:-MAX_OBSERVER_ERRORS]
                if observer.required:
                    raise RequiredObserverError(str(exc)) from exc
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
        self._abort_actions = abort_actions
        self._aborted = False

    def preflight(self, action: ActionRef | str, parameters: dict[str, Any]) -> None:
        self._invoker.preflight(action, parameters)

    def describe(self, action: ActionRef | str) -> Any:
        describe = getattr(self._invoker, "describe", None)
        return describe(action) if callable(describe) else True

    def invoke(self, action: ActionRef | str, parameters: dict[str, Any]) -> ActionInvocation:
        try:
            self.events.emit("action.requested", {"action": getattr(action, "id", action), "parameters": parameters}, evidence=EvidenceStrength.INTENT)
        except RequiredObserverError:
            self._abort_once()
            raise
        invocation = self._invoker.invoke(action, parameters)
        try:
            self.events.emit("action.accepted", {"action": getattr(action, "id", action), "invocation_token": getattr(invocation, "token", "")}, evidence=EvidenceStrength.ACKNOWLEDGED)
        except RequiredObserverError:
            self._abort_once()
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
                self._abort_once()
                raise
        return result

    def _abort_once(self) -> None:
        if self._aborted:
            return
        self._aborted = True
        for action in self._abort_actions:
            try:
                # Abort actions are still trusted actions.  Route them through
                # the same describe/preflight/authorization seam as normal
                # actions; an observer failure must never grant a bypass.
                try:
                    self.events.emit("abort.requested", {"action": getattr(action, "id", action)}, evidence=EvidenceStrength.INTENT)
                except RequiredObserverError:
                    # The failing observer is already the reason for abort;
                    # it must not prevent the trusted abort control path.
                    pass
                description = self.describe(action)
                if description is False or description is None:
                    raise ValueError(f"abort action is not trusted: {getattr(action, 'id', action)}")
                self.preflight(action, {})
                invocation = self._invoker.invoke(action, {})
                try:
                    self.events.emit("abort.accepted", {"action": getattr(action, "id", action)}, evidence=EvidenceStrength.ACKNOWLEDGED)
                except RequiredObserverError:
                    pass
                self._invoker.poll(invocation)
                try:
                    self.events.emit("abort.completed", {"action": getattr(action, "id", action)}, evidence=EvidenceStrength.ACKNOWLEDGED)
                except RequiredObserverError:
                    pass
            except Exception:
                continue


def _completion_evidence(result: PollResult) -> EvidenceStrength:
    """Only explicit underlying evidence can establish an observation.

    A successful software poll is an acknowledgement, not a physical or
    scientific observation.  The frozen PollResult interface carries any
    stronger claim in its metadata, so this gate accepts only the explicit
    versioned strength marker and never infers it from ``value`` or status.
    """
    if not result.succeeded:
        return EvidenceStrength.ACKNOWLEDGED
    metadata = getattr(result, "metadata", None)
    if not isinstance(metadata, Mapping):
        metadata = result.value if isinstance(result.value, Mapping) else {}
    declared = metadata.get("evidence_strength")
    if declared == EvidenceStrength.OBSERVED.value:
        evidence = metadata.get("evidence")
        if isinstance(evidence, Mapping) and evidence.get("kind") in {"physical", "scientific"}:
            return EvidenceStrength.OBSERVED
    return EvidenceStrength.ACKNOWLEDGED


# Friendly aliases for callers that describe the component as a recorder.
EventRecorder = ProcedureEventStream
EventObserverError = RequiredObserverError
