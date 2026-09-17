"""In-memory sequential procedure execution with bounded polling."""

from __future__ import annotations

import re
import time
from typing import Any

from .invoker import ActionInvoker, PollResult
from .model import Procedure, ProcedureSession, SessionState


class ProcedurePreflightError(RuntimeError):
    pass


class ProcedureRunError(RuntimeError):
    pass


_ACTION_ID = re.compile(r"^[a-z][a-z0-9_]*$")
_URI_OR_SCHEME = re.compile(r"^[a-z][a-z0-9+.-]*\s*:\s*/?/?", re.IGNORECASE)
_SQL = re.compile(r"\b(?:alter|create|delete|drop|insert|pragma|select|truncate|union|update)\b", re.IGNORECASE)
_SCRIPT = re.compile(
    r"(?:^#!|\b(?:bash|cmd|eval|exec|javascript|powershell|python|shell)\s*(?:[-:]|$))",
    re.IGNORECASE,
)
_ABSOLUTE_PATH = re.compile(r"^(?:[a-z]:[\\/]|/|~(?:[\\/]|$)|\\\\)", re.IGNORECASE)
_SERIAL_PATH = re.compile(r"^(?:/dev/(?:tty|cu)[a-z0-9._/-]*|com[0-9]+$)", re.IGNORECASE)


def _validate_parameter_value(value: Any, path: str) -> None:
    """Keep action parameters data-only; transport/execution belongs to the invoker."""
    if isinstance(value, str):
        if value != value.strip() or any(ord(char) < 32 for char in value):
            raise ProcedurePreflightError(f"malformed parameter value at {path}")
        if (_URI_OR_SCHEME.match(value) or _SQL.search(value) or _SCRIPT.search(value)
                or _ABSOLUTE_PATH.match(value) or _SERIAL_PATH.match(value)
                or any(part == ".." for part in re.split(r"[\\/]", value))):
            raise ProcedurePreflightError(f"unsupported execution mechanism at {path}")
        return
    if isinstance(value, dict):
        for key, nested in value.items():
            if not isinstance(key, str) or key != key.strip() or not key:
                raise ProcedurePreflightError(f"malformed parameter key at {path}")
            _validate_parameter_value(nested, f"{path}.{key}")
        return
    if isinstance(value, (list, tuple)):
        for index, nested in enumerate(value):
            _validate_parameter_value(nested, f"{path}[{index}]")


def _authorize_action(invoker: ActionInvoker, action: Any, parameters: dict[str, Any]) -> None:
    if action is None or not _ACTION_ID.fullmatch(action.id):
        raise ProcedurePreflightError("action ID is not a trusted identifier")
    _validate_parameter_value(parameters, "parameters")

    describe = getattr(invoker, "describe", None)
    if callable(describe):
        description = describe(action)
        if description is False or description is None:
            raise ProcedurePreflightError(f"action is not trusted: {action.id}")
    # preflight is the mandatory injected trust/version check.  It may also
    # validate the action's parameters and target-specific constraints.
    invoker.preflight(action, parameters)


class ProcedureEngine:
    def __init__(self, invoker: ActionInvoker, *, sleep=time.sleep) -> None:
        self._invoker = invoker
        self._sleep = sleep

    def new_session(self, procedure: Procedure) -> ProcedureSession:
        return ProcedureSession(procedure=procedure)

    def preflight(self, session: ProcedureSession) -> None:
        if session.state is not SessionState.CREATED:
            raise ProcedurePreflightError("session is not new")
        try:
            for step in session.procedure.steps:
                if step.action_ref is not None:
                    _authorize_action(self._invoker, step.action_ref, dict(step.parameters))
        except Exception as exc:
            session.state = SessionState.FAILED
            session.error = str(exc)
            raise ProcedurePreflightError(str(exc)) from exc
        session.state = SessionState.PREFLIGHTED

    def run(self, session: ProcedureSession) -> list[Any]:
        if session.state is not SessionState.PREFLIGHTED:
            raise ProcedureRunError("preflight is required before run")
        session.state = SessionState.RUNNING
        try:
            for index, step in enumerate(session.procedure.steps):
                session.current_step = index
                if step.action_ref is None:
                    continue
                invocation = self._invoker.invoke(step.action_ref, dict(step.parameters))
                result = PollResult(done=False)
                for poll_number in range(step.timeout_polls):
                    result = self._invoker.poll(invocation)
                    if result.done:
                        break
                    if poll_number + 1 < step.timeout_polls and step.poll_interval_s:
                        self._sleep(step.poll_interval_s)
                if not result.done:
                    raise ProcedureRunError(f"step timed out: {step.id}")
                if not result.succeeded:
                    raise ProcedureRunError(result.error or f"step failed: {step.id}")
                session.results.append(result.value)
            session.state = SessionState.SUCCEEDED
            return list(session.results)
        except ProcedureRunError as exc:
            session.state = SessionState.FAILED
            session.error = str(exc)
            raise
        except Exception as exc:
            session.state = SessionState.FAILED
            session.error = str(exc)
            raise ProcedureRunError(str(exc)) from exc
