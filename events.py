"""Bounded procedure event recording and observer dispatch.

Events are an operator-facing projection of a procedure run.  They are not an
execution log: an observer is never retried and an event failure must not make
the underlying action happen twice.
"""
from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from .invoker import ActionInvocation, ActionInvoker, PollResult
from .model import ActionRef

MAX_PAYLOAD_KEYS = 32
MAX_PAYLOAD_DEPTH = 4
MAX_PAYLOAD_STRING = 256


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
    "apikey", "private_key", "authorization", "cookie",
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

    def as_dict(self) -> dict[str, Any]:
        return {
            "sequence": self.sequence,
            "name": self.name,
            "evidence": self.evidence.value,
            "payload": redact_payload(self.payload),
        }


class RequiredObserverError(RuntimeError):
    """A required observer failed; the next physical action must not run."""


@dataclass(frozen=True)
class _Observer:
    callback: Callable[[ProcedureEvent], Any]
    required: bool


class ProcedureEventStream:
    """Deterministic event history plus an independent latest-event snapshot."""

    def __init__(self, observers: Mapping[Callable[[ProcedureEvent], Any], bool] | None = None) -> None:
        self._history: list[ProcedureEvent] = []
        self._current: dict[str, ProcedureEvent] = {}
        self._observers: list[_Observer] = [
            _Observer(callback, required) for callback, required in (observers or {}).items()
        ]
        self.observer_errors: list[str] = []

    def add_observer(self, callback: Callable[[ProcedureEvent], Any], *, required: bool = False) -> None:
        self._observers.append(_Observer(callback, required))

    def emit(self, name: str, payload: Mapping[str, Any] | None = None, *, evidence: EvidenceStrength = EvidenceStrength.NONE) -> ProcedureEvent:
        if not isinstance(evidence, EvidenceStrength):
            evidence = EvidenceStrength(evidence)
        event = ProcedureEvent(len(self._history), name, evidence, redact_payload(payload or {}))
        self._history.append(event)
        self._current[name] = event
        for observer in self._observers:
            try:
                observer.callback(event)
            except Exception as exc:
                self.observer_errors.append(f"{type(exc).__name__}: {exc}")
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
            self.events.emit("action.accepted", {"action": getattr(action, "id", action), "invocation": getattr(invocation, "token", "")}, evidence=EvidenceStrength.ACKNOWLEDGED)
        except RequiredObserverError:
            self._abort_once()
            raise
        return invocation

    def poll(self, invocation: ActionInvocation) -> PollResult:
        result = self._invoker.poll(invocation)
        if result.done:
            try:
                self.events.emit("action.completed", {"succeeded": result.succeeded, "error": result.error}, evidence=EvidenceStrength.OBSERVED if result.succeeded else EvidenceStrength.ACKNOWLEDGED)
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
                invocation = self._invoker.invoke(action, {})
                self._invoker.poll(invocation)
            except Exception:
                continue


# Friendly aliases for callers that describe the component as a recorder.
EventRecorder = ProcedureEventStream
EventObserverError = RequiredObserverError
