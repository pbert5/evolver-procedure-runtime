"""Narrow injected interfaces for the declarative procedure contract."""
from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Protocol
from .model import ActionRef

@dataclass(frozen=True)
class ActionInvocation:
    token: str

@dataclass(frozen=True)
class PollResult:
    done: bool
    succeeded: bool = True
    value: Any = None
    error: str | None = None

class ActionInvoker(Protocol):
    def preflight(self, action: ActionRef | str, parameters: dict[str, Any]) -> None: ...
    def invoke(self, action: ActionRef | str, parameters: dict[str, Any]) -> ActionInvocation: ...
    def poll(self, invocation: ActionInvocation) -> PollResult: ...

class InputProvider(Protocol):
    def read(self, parameter: str, prompt: str, max_length: int) -> Any: ...

class Clock(Protocol):
    def sleep(self, seconds: float) -> None: ...

class EventSink(Protocol):
    def emit(self, event: str, payload: dict[str, Any]) -> None: ...
