"""Closed, declarative sink contract for procedure observations and checkpoints.

This module deliberately creates requests; it does not perform I/O.  A host may
route a request to its already-registered implementation, but cannot provide a
path, URL, SQL statement, shell command, callback, or import as sink input.
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
import re
from types import MappingProxyType
from typing import Any, Mapping


class SinkError(ValueError):
    """Raised when a sink request is outside the frozen contract."""


class SinkEffect(str, Enum):
    OBSERVATION = "observation"
    CHECKPOINT_EXPORT = "checkpoint_export"


_IDENTIFIER = re.compile(r"^[a-z][a-z0-9]*(?:[._-][a-z0-9]+)*$")
_FORBIDDEN = re.compile(
    r"(?:^#!|\b(?:bash|cmd|eval|exec|javascript|powershell|python|shell)\s*(?:[-:(]|$)|"
    r"\b(?:subprocess|runpy|shutil)\s*[.]|\b(?:os\.(?:system|popen)|__import__|importlib)\b|"
    r"\b(?:alter|create|delete|drop|insert|pragma|select|truncate|union|update)\b)",
    re.IGNORECASE,
)
_IMPORT = re.compile(
    r"(?:\bfrom\s+(?:[.]|[a-z_]\w*(?:\.[a-z_]\w*)*)\s+import\b|"
    r"\bimport\s+[a-z_]\w*(?:\.[a-z_]\w*)*)",
    re.IGNORECASE,
)
_SCHEME = re.compile(r"^[a-z][a-z0-9+.-]*\s*:\s*/?/?", re.IGNORECASE)
_URL = re.compile(r"\b[a-z][a-z0-9+.-]*://", re.IGNORECASE)
_ABSOLUTE = re.compile(r"^(?:[a-z]:[\\/]|/|~(?:[\\/]|$)|\\\\)", re.IGNORECASE)
_RELATIVE_PATH = re.compile(r"(?:^|[\s=])\.\.?[\\/]")
_FORBIDDEN_KEYS = frozenset({
    "callback", "callable", "cmd", "code", "command", "eval", "exec", "executable",
    "import", "interpreter", "module", "path", "query", "script", "shell", "sql",
    "uri", "url", "importlib", "popen", "runpy", "shutil", "subprocess", "system",
})

INITIAL_SINK_IDS = (
    "edge.calibration_run.observation",
    "central.calibration_session.observation",
    "procedure.checkpoint.export",
)


@dataclass(frozen=True)
class SinkSpec:
    id: str
    effect: SinkEffect
    persistent: bool
    idempotency_key: str

    def __post_init__(self) -> None:
        _check_identifier(self.id, "sink ID")
        if not self.idempotency_key or not _IDENTIFIER.fullmatch(self.idempotency_key):
            raise SinkError("idempotency_key must be a trusted identifier")


@dataclass(frozen=True)
class SessionBinding:
    """Ephemeral association of a procedure execution with a host session."""

    session_id: str
    procedure_id: str

    def __post_init__(self) -> None:
        _check_identifier(self.session_id, "session ID")
        _check_identifier(self.procedure_id, "procedure ID")


@dataclass(frozen=True)
class CheckpointDestination:
    """Opaque destination identity supplied by the host, never an arbitrary path."""

    destination_id: str

    def __post_init__(self) -> None:
        _check_identifier(self.destination_id, "checkpoint destination")


@dataclass(frozen=True)
class SinkRequest:
    sink_id: str
    payload: Mapping[str, Any]
    idempotency_key: str
    session: SessionBinding | None = None
    checkpoint: CheckpointDestination | None = None

    def __post_init__(self) -> None:
        _check_identifier(self.sink_id, "sink ID")
        if not self.idempotency_key or not _IDENTIFIER.fullmatch(self.idempotency_key):
            raise SinkError("idempotency key must be a trusted identifier")
        _validate_data(self.payload, "payload")


@dataclass(frozen=True)
class MutationOutcome:
    """Host-reported outcome; ``ambiguous`` must be resolved by the host."""

    status: str
    detail: str | None = None

    def __post_init__(self) -> None:
        if self.status not in {"accepted", "rejected", "ambiguous"}:
            raise SinkError("mutation outcome must be accepted, rejected, or ambiguous")


def _check_identifier(value: Any, label: str) -> None:
    if not isinstance(value, str) or not _IDENTIFIER.fullmatch(value):
        raise SinkError(f"{label} is not a trusted identifier")


def _validate_data(value: Any, path: str) -> None:
    if isinstance(value, str):
        if value != value.strip() or any(ord(char) < 32 for char in value):
            raise SinkError(f"malformed data at {path}")
        if (_SCHEME.match(value) or _URL.search(value) or _ABSOLUTE.match(value)
                or _RELATIVE_PATH.search(value) or _IMPORT.search(value) or _FORBIDDEN.search(value)):
            raise SinkError(f"unsupported execution mechanism at {path}")
    elif isinstance(value, Mapping):
        for key, nested in value.items():
            if (not isinstance(key, str) or not _IDENTIFIER.fullmatch(key)
                    or any(part in _FORBIDDEN_KEYS for part in re.split(r"[._-]", key.casefold()))):
                raise SinkError(f"payload key at {path} is not a trusted identifier")
            _validate_data(nested, f"{path}.{key}")
    elif isinstance(value, (list, tuple)):
        for index, nested in enumerate(value):
            _validate_data(nested, f"{path}[{index}]")
    elif value is not None and not isinstance(value, (bool, int, float)):
        raise SinkError(f"unsupported payload value at {path}")


def _initial_registry() -> dict[str, SinkSpec]:
    return {
        INITIAL_SINK_IDS[0]: SinkSpec(INITIAL_SINK_IDS[0], SinkEffect.OBSERVATION, True, "observation_id"),
        INITIAL_SINK_IDS[1]: SinkSpec(INITIAL_SINK_IDS[1], SinkEffect.OBSERVATION, True, "observation_id"),
        INITIAL_SINK_IDS[2]: SinkSpec(INITIAL_SINK_IDS[2], SinkEffect.CHECKPOINT_EXPORT, True, "checkpoint_id"),
    }


class SinkRegistry:
    """Allow-list of sink IDs. Registration contains metadata, never executable code."""

    def __init__(self, specs: Mapping[str, SinkSpec] | None = None) -> None:
        initial = _initial_registry()
        values = dict(initial if specs is None else specs)
        if not set(INITIAL_SINK_IDS).issubset(values):
            raise SinkError("registry is missing an initial trusted sink ID")
        for key, spec in values.items():
            if not isinstance(spec, SinkSpec) or key != spec.id:
                raise SinkError("registry key does not match sink ID")
        for sink_id, expected in initial.items():
            if values[sink_id] != expected:
                raise SinkError("initial trusted sink metadata cannot be replaced")
        self._specs = MappingProxyType(values)

    def resolve(self, sink_id: str) -> SinkSpec:
        try:
            return self._specs[sink_id]
        except (KeyError, TypeError) as exc:
            raise SinkError("sink ID is not registered") from exc

    def request(
        self,
        sink_id: str,
        payload: Mapping[str, Any],
        *,
        idempotency_key: str,
        session: SessionBinding | None = None,
        checkpoint: CheckpointDestination | None = None,
    ) -> SinkRequest:
        spec = self.resolve(sink_id)
        if spec.effect is SinkEffect.CHECKPOINT_EXPORT:
            if checkpoint is None or session is not None:
                raise SinkError("checkpoint export requires only a host checkpoint destination")
        elif checkpoint is not None:
            raise SinkError("checkpoint destination is only valid for checkpoint export")
        request = SinkRequest(sink_id, dict(payload), idempotency_key, session, checkpoint)
        return request


SINK_CONTRACT_FROZEN = "procedure/SINK_CONTRACT_FROZEN.md"
