"""Bounded, declarative procedure execution through trusted injected seams."""

from __future__ import annotations

import re
import time
from collections.abc import Mapping
from typing import Any, Callable

from .invoker import ActionInvoker, InputProvider, PollResult
from .model import Procedure, ProcedureSession, SessionState, Step, StepKind


class ProcedurePreflightError(RuntimeError):
    pass


class ProcedureRunError(RuntimeError):
    pass


_ACTION_ID = re.compile(r"^[a-z][a-z0-9_]*$")
_URI_OR_SCHEME = re.compile(r"^[a-z][a-z0-9+.-]*\s*:\s*/?/?", re.IGNORECASE)
_SQL = re.compile(r"\b(?:alter|create|delete|drop|insert|pragma|select|truncate|union|update)\b", re.IGNORECASE)
_SCRIPT = re.compile(r"(?:^#!|\b(?:bash|cmd|eval|exec|javascript|powershell|python|shell)\s*(?:[-:]|$))", re.IGNORECASE)
_ABSOLUTE_PATH = re.compile(r"^(?:[a-z]:[\\/]|/|~(?:[\\/]|$)|\\\\)", re.IGNORECASE)
_SERIAL_PATH = re.compile(r"^(?:/dev/(?:tty|cu)[a-z0-9._/-]*|com[0-9]+$)", re.IGNORECASE)


def _validate_parameter_value(value: Any, path: str) -> None:
    if isinstance(value, str):
        if value != value.strip() or any(ord(char) < 32 for char in value):
            raise ProcedurePreflightError(f"malformed parameter value at {path}")
        if (_URI_OR_SCHEME.match(value) or _SQL.search(value) or _SCRIPT.search(value)
                or _ABSOLUTE_PATH.match(value) or _SERIAL_PATH.match(value)
                or any(part == ".." for part in re.split(r"[\\/]", value))):
            raise ProcedurePreflightError(f"unsupported execution mechanism at {path}")
        return
    if isinstance(value, Mapping):
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
    if callable(describe) and describe(action) in (False, None):
        raise ProcedurePreflightError(f"action is not trusted: {action.id}")
    invoker.preflight(action, parameters)


def _resolve_parameters(value: Any, inputs: Mapping[str, Any], path: str = "parameters") -> Any:
    """Resolve only the frozen ``{parameter_ref: name}`` data shape."""
    if isinstance(value, Mapping):
        if set(value) == {"parameter_ref"}:
            name = value["parameter_ref"]
            if not isinstance(name, str) or not _ACTION_ID.fullmatch(name) or name not in inputs:
                raise ProcedureRunError(f"unknown parameter reference at {path}")
            return inputs[name]
        return {key: _resolve_parameters(nested, inputs, f"{path}.{key}") for key, nested in value.items()}
    if isinstance(value, list):
        return [_resolve_parameters(item, inputs, f"{path}[{index}]") for index, item in enumerate(value)]
    if isinstance(value, tuple):
        return tuple(_resolve_parameters(item, inputs, f"{path}[{index}]") for index, item in enumerate(value))
    return value


class ProcedureEngine:
    def __init__(self, invoker: ActionInvoker, *, input_provider: InputProvider | None = None,
                 sleep: Callable[[float], None] = time.sleep,
                 clock: Callable[[], float] = time.monotonic) -> None:
        self._invoker = invoker
        self._input_provider = input_provider
        self._sleep = sleep
        self._clock = clock

    def new_session(self, procedure: Procedure) -> ProcedureSession:
        return ProcedureSession(procedure=procedure)

    def preflight(self, session: ProcedureSession) -> None:
        if session.state is not SessionState.CREATED:
            raise ProcedurePreflightError("session is not new")
        try:
            for action, parameters in self._declared_actions(session.procedure):
                _authorize_action(self._invoker, action, parameters)
        except Exception as exc:
            session.state = SessionState.FAILED
            session.error = str(exc)
            raise ProcedurePreflightError(str(exc)) from exc
        session.state = SessionState.PREFLIGHTED

    def _declared_actions(self, procedure: Procedure):
        for step in procedure.steps:
            action = step.action_ref if step.kind is StepKind.ACTION else step.poll_ref if step.kind is StepKind.POLL else None
            if action is not None:
                yield action, dict(step.parameters)
        for action in procedure.abort_actions:
            yield action, {}

    def run(self, session: ProcedureSession) -> list[Any]:
        if session.state is not SessionState.PREFLIGHTED:
            raise ProcedureRunError("preflight is required before run")
        session.state = SessionState.RUNNING
        deadline = self._clock() + session.procedure.default_timeout
        steps = {step.id: step for step in session.procedure.steps}
        current_id = session.procedure.entry_step_id.id
        try:
            while current_id is not None:
                if self._clock() >= deadline:
                    raise ProcedureRunError("procedure timed out")
                step = steps[current_id]
                session.current_step = session.procedure.steps.index(step)
                if step.kind is StepKind.INPUT:
                    self._run_input(session, step)
                elif step.kind in {StepKind.ACTION, StepKind.POLL}:
                    self._run_action(session, step, deadline)
                elif step.kind is StepKind.BRANCH:
                    current_id = self._branch(session, step)
                    continue
                elif step.kind is StepKind.COMPLETE:
                    session.state = SessionState.SUCCEEDED
                    return list(session.results)
                current_id = step.next_step_id.id if step.next_step_id else self._following_id(session.procedure, step.id)
            session.state = SessionState.SUCCEEDED
            return list(session.results)
        except ProcedureRunError as exc:
            self._fail(session, exc)
            raise
        except Exception as exc:
            error = ProcedureRunError(str(exc))
            self._fail(session, error)
            raise error from exc

    @staticmethod
    def _following_id(procedure: Procedure, step_id: str) -> str | None:
        ids = [step.id for step in procedure.steps]
        index = ids.index(step_id) + 1
        return ids[index] if index < len(ids) else None

    def _run_input(self, session: ProcedureSession, step: Step) -> None:
        if self._input_provider is None or step.input_ref is None:
            raise ProcedureRunError("input provider is required")
        name = step.input_ref.id
        if name not in session.procedure.parameters:
            raise ProcedureRunError(f"input parameter is not declared: {name}")
        value = self._input_provider.read(name, step.prompt or "", step.max_input_length or 1)
        if len(str(value)) > (step.max_input_length or 1):
            raise ProcedureRunError(f"input exceeds maximum length: {name}")
        _validate_parameter_value(value, f"input.{name}")
        session.inputs[name] = value

    def _run_action(self, session: ProcedureSession, step: Step, deadline: float) -> None:
        action = step.action_ref or step.poll_ref
        parameters = _resolve_parameters(dict(step.parameters), session.inputs)
        _authorize_action(self._invoker, action, parameters)
        if self._clock() >= deadline:
            raise ProcedureRunError(f"step timed out: {step.id}")
        invocation = self._invoker.invoke(action, parameters)
        result = PollResult(done=False)
        for poll_number in range(step.timeout_polls):
            if self._clock() >= deadline:
                raise ProcedureRunError(f"step timed out: {step.id}")
            result = self._invoker.poll(invocation)
            if result.done:
                break
            if poll_number + 1 < step.timeout_polls and step.poll_interval_s:
                remaining = deadline - self._clock()
                if remaining <= 0:
                    raise ProcedureRunError(f"step timed out: {step.id}")
                self._sleep(min(step.poll_interval_s, remaining))
        if not result.done:
            raise ProcedureRunError(f"step timed out: {step.id}")
        if not result.succeeded:
            raise ProcedureRunError(result.error or f"step failed: {step.id}")
        session.results.append(result.value)

    def _branch(self, session: ProcedureSession, step: Step) -> str:
        condition = step.condition_ref
        evaluator = getattr(self._invoker, "evaluate_condition", None)
        if not callable(evaluator):
            evaluator = getattr(self._invoker, "check_condition", None)
        if callable(evaluator):
            outcome = bool(evaluator(condition, dict(session.inputs), tuple(session.results)))
        elif condition is not None and condition.id in session.inputs:
            outcome = bool(session.inputs[condition.id])
        else:
            raise ProcedureRunError("condition evaluator is required")
        target = step.then_step_id if outcome else step.else_step_id
        if target is None:
            raise ProcedureRunError("branch target is missing")
        return target.id

    def _fail(self, session: ProcedureSession, error: ProcedureRunError) -> None:
        session.state = SessionState.FAILED
        session.error = str(error)
        if session.cleanup_attempted:
            return
        session.cleanup_attempted = True
        seen: set[tuple[str, str | int | None]] = set()
        for action in session.procedure.abort_actions:
            key = (action.id, action.version)
            if key in seen:
                continue
            seen.add(key)
            try:
                # Cleanup authorization is intentionally fresh: the initial
                # procedure preflight may be stale after the run fails.
                _authorize_action(self._invoker, action, {})
                invocation = self._invoker.invoke(action, {})
                self._invoker.poll(invocation)
            except Exception:
                continue
