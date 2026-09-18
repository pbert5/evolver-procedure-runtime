"""Bounded, declarative procedure execution through trusted injected seams."""

from __future__ import annotations

import re
import time
from uuid import uuid4
from collections.abc import Mapping
from typing import Any, Callable

from .invoker import ActionInvoker, InputProvider, PollResult
from .model import AdvanceResult, Attempt, CleanupActionOutcome, PrimaryOutcome, Procedure, ProcedureSession, SessionState, Step, StepKind
from .events import RequiredObserverError
from .sinks import CheckpointDestination, MutationOutcome, SinkRegistry, SinkRequest


class ProcedurePreflightError(RuntimeError):
    pass


class ProcedureRunError(RuntimeError):
    pass


class CorrectionUnavailable(ProcedureRunError):
    """The descriptor does not explicitly permit the requested correction."""


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
                 clock: Callable[[], float] = time.monotonic,
                 sink_registry: SinkRegistry | None = None,
                 checkpoint_destination: CheckpointDestination | None = None,
                 checkpoint_executor: Callable[[SinkRequest], MutationOutcome] | None = None) -> None:
        self._invoker = invoker
        self._input_provider = input_provider
        self._sleep = sleep
        self._clock = clock
        self._sink_registry = sink_registry
        self._checkpoint_destination = checkpoint_destination
        self._checkpoint_executor = checkpoint_executor

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
        session.current_step_id = session.procedure.entry_step_id.id
        self._bind_run_fence(session)

    def provide_parameter(self, session: ProcedureSession, name: str, value: Any) -> None:
        """Validate and store one input without advancing the session."""
        if session.state not in {SessionState.PREFLIGHTED, SessionState.READY,
                                 SessionState.WAITING_INPUT, SessionState.RUNNING}:
            raise ProcedureRunError("session is not accepting input")
        if name not in session.procedure.parameters:
            raise ValueError(f"input parameter is not declared: {name}")
        spec = session.procedure.parameters[name]
        expected = spec.get("type") if isinstance(spec, Mapping) else None
        valid = {
            "integer": lambda item: type(item) is int,
            "number": lambda item: type(item) in {int, float},
            "string": lambda item: isinstance(item, str),
            "boolean": lambda item: type(item) is bool,
        }.get(expected, lambda item: True)
        if not valid(value):
            raise ValueError(f"input parameter has invalid type: {name}")
        if isinstance(value, str) and len(value) > self._input_limit(session, name):
            raise ValueError(f"input exceeds maximum length: {name}")
        _validate_parameter_value(value, f"input.{name}")
        session.inputs[name] = value
        if session.current_step_id is not None:
            step = next((item for item in session.procedure.steps if item.id == session.current_step_id), None)
            if step is not None and step.kind is StepKind.INPUT and step.input_ref and step.input_ref.id == name:
                self._complete_attempt(session, step.id, value, inputs={name: value})

    def rerun_from_here(self, session: ProcedureSession, step_id: str) -> AdvanceResult:
        """Request a new forward attempt without executing anything yet."""
        if session.state not in {SessionState.SUCCEEDED, SessionState.FAILED, SessionState.READY}:
            raise ProcedureRunError("rerun requires a completed session or ready boundary")
        history = session.attempt_history
        selected = next((item for item in reversed(history) if item.step_id == step_id and item.status == "completed"), None)
        if selected is None:
            raise ProcedureRunError(f"no completed attempt exists for step: {step_id}")
        ids = [step.id for step in session.procedure.steps]
        try:
            start = ids.index(step_id)
        except ValueError as exc:
            raise ProcedureRunError(f"unknown step: {step_id}") from exc
        affected = set(ids[start:])
        for attempt in history:
            if attempt.step_id in affected:
                session._attempt_status[attempt.attempt_id] = "superseded" if attempt.attempt_id == selected.attempt_id else "stale"
        for step in session.procedure.steps[start:]:
            if step.kind is StepKind.INPUT and step.input_ref is not None:
                session.inputs.pop(step.input_ref.id, None)
        pending = self._new_attempt(session, step_id, status="pending", supersedes=selected.attempt_id)
        session._pending_attempt_id = pending.attempt_id
        session.current_step_id = step_id
        session.current_step = start
        session.pending_invocation = None
        session.next_poll_at = None
        session.deadline = None
        session.error = None
        session.state = SessionState.READY
        self._record_event(session, "procedure.rerun_requested", step_id=step_id, attempt_id=pending.attempt_id,
                           payload={"supersedes": selected.attempt_id, "invalidated_steps": sorted(affected)})
        return AdvanceResult(session.state)

    def correct(self, session: ProcedureSession, step_id: str, value: Any) -> AdvanceResult:
        """Replace an explicitly correctable input while retaining all provenance."""
        step = next((item for item in session.procedure.steps if item.id == step_id), None)
        if step is None:
            raise CorrectionUnavailable(f"unknown step: {step_id}")
        if not step.correction.replaceable:
            raise CorrectionUnavailable(f"step is not target-correctable: {step_id}")
        allowed_kind = {
            StepKind.INPUT: "input",
            StepKind.ACTION: "observation",
            StepKind.CHECKPOINT: "checkpoint",
        }.get(step.kind)
        if allowed_kind != step.correction.kind:
            raise CorrectionUnavailable(f"step correction kind is not supported: {step_id}")
        if session.state not in {SessionState.SUCCEEDED, SessionState.FAILED, SessionState.READY}:
            raise CorrectionUnavailable("targeted correction requires a completed session or ready boundary")
        old = next((item for item in reversed(session.attempt_history)
                    if item.step_id == step_id and item.status == "completed"), None)
        if old is None:
            raise CorrectionUnavailable(f"no completed attempt exists for step: {step_id}")
        if step.kind is StepKind.INPUT:
            if step.input_ref is None:
                raise CorrectionUnavailable(f"step has no correctable input: {step_id}")
            self._validate_corrected_input(session, step.input_ref.id, value)
            corrected_inputs = {step.input_ref.id: value}
        else:
            try:
                _validate_parameter_value(value, f"correction.{step_id}")
            except ProcedurePreflightError as exc:
                raise CorrectionUnavailable(str(exc)) from exc
            corrected_inputs = {}
        session._attempt_status[old.attempt_id] = "superseded"
        for attempt in session.attempt_history:
            if attempt.step_id in step.correction.invalidates:
                session._attempt_status[attempt.attempt_id] = "stale"
        replacement = self._new_attempt(session, step_id, status="completed", result=value,
                                        inputs=corrected_inputs, supersedes=old.attempt_id)
        if step.input_ref is not None:
            session.inputs[step.input_ref.id] = value
        self._record_event(session, "procedure.correction_applied", step_id=step_id,
                           attempt_id=replacement.attempt_id,
                           payload={"supersedes": old.attempt_id, "invalidated_steps": list(step.correction.invalidates)})
        return AdvanceResult(session.state, value=value)

    def _validate_corrected_input(self, session: ProcedureSession, name: str, value: Any) -> None:
        # Reuse the same validation path while keeping correction explicitly non-advancing.
        if name not in session.procedure.parameters:
            raise CorrectionUnavailable(f"input parameter is not declared: {name}")
        spec = session.procedure.parameters[name]
        expected = spec.get("type") if isinstance(spec, Mapping) else None
        valid = {"integer": lambda item: type(item) is int,
                 "number": lambda item: type(item) in {int, float},
                 "string": lambda item: isinstance(item, str),
                 "boolean": lambda item: type(item) is bool}.get(expected, lambda item: True)
        if not valid(value):
            raise CorrectionUnavailable(f"input parameter has invalid type: {name}")
        if isinstance(value, str) and len(value) > self._input_limit(session, name):
            raise CorrectionUnavailable(f"input exceeds maximum length: {name}")
        try:
            _validate_parameter_value(value, f"input.{name}")
        except ProcedurePreflightError as exc:
            raise CorrectionUnavailable(str(exc)) from exc

    @staticmethod
    def _new_attempt(session: ProcedureSession, step_id: str, *, status: str,
                     result: Any = None, inputs: Mapping[str, Any] | None = None,
                     supersedes: str | None = None) -> Attempt:
        number = sum(1 for item in session._attempts if item.step_id == step_id) + 1
        attempt = Attempt(str(uuid4()), step_id, number, status, result=result,
                          inputs=dict(inputs or {}), supersedes=supersedes)
        session._attempts.append(attempt)
        session._attempt_status[attempt.attempt_id] = status
        return attempt

    @staticmethod
    def _complete_attempt(session: ProcedureSession, step_id: str, result: Any,
                          *, inputs: Mapping[str, Any] | None = None) -> Attempt:
        if session._pending_attempt_id is not None:
            for index, attempt in enumerate(session._attempts):
                if attempt.attempt_id == session._pending_attempt_id:
                    completed = Attempt(attempt.attempt_id, attempt.step_id, attempt.number, "completed",
                                        result=result, inputs=dict(inputs or attempt.inputs),
                                        supersedes=attempt.supersedes)
                    session._attempts[index] = completed
                    session._attempt_status[attempt.attempt_id] = "completed"
                    session._pending_attempt_id = None
                    return completed
        return ProcedureEngine._new_attempt(session, step_id, status="completed", result=result, inputs=inputs)

    def _record_event(self, session: ProcedureSession, name: str, *, step_id: str | None = None,
                      attempt_id: str | None = None, payload: Mapping[str, Any] | None = None) -> None:
        from .model import SessionEvent
        event = SessionEvent(name=name, run_id=session.run_id, step_id=step_id,
                             attempt_id=attempt_id, payload=dict(payload or {}))
        session.events.append(event)
        stream = getattr(self._invoker, "events", None)
        if stream is not None:
            stream.emit(name, {"step_id": step_id, "attempt_id": attempt_id, **dict(payload or {})})

    @staticmethod
    def _input_limit(session: ProcedureSession, name: str) -> int:
        step = next((item for item in session.procedure.steps
                     if item.kind is StepKind.INPUT and item.input_ref and item.input_ref.id == name), None)
        return step.max_input_length or 4096 if step else 4096

    def advance(self, session: ProcedureSession) -> AdvanceResult:
        """Perform at most one external action invocation or poll."""
        if session.state is SessionState.CREATED:
            raise ProcedureRunError("preflight is required before advance")
        if session.state in {SessionState.SUCCEEDED, SessionState.FAILED, SessionState.ABORTED}:
            return AdvanceResult(session.state, error=session.error)
        if session.deadline is None:
            session.deadline = self._clock() + session.procedure.default_timeout
        if session.current_step_id is None:
            session.current_step_id = session.procedure.entry_step_id.id
        session.state = SessionState.RUNNING
        steps = {step.id: step for step in session.procedure.steps}

        try:
            if session.pending_invocation is not None:
                if self._clock() >= session.deadline:
                    raise ProcedureRunError("procedure timed out")
                if session.next_poll_at is not None and self._clock() < session.next_poll_at:
                    session.state = SessionState.WAITING_ACTION
                    return AdvanceResult(session.state, next_poll_at=session.next_poll_at)
                step = steps[session.current_step_id]
                result = self._invoker.poll(session.pending_invocation)
                session.pending_poll_count += 1
                if not result.done:
                    if session.pending_poll_count >= step.timeout_polls:
                        raise ProcedureRunError(f"step timed out: {step.id}")
                    session.next_poll_at = self._clock() + step.poll_interval_s
                    session.state = SessionState.WAITING_ACTION
                    return AdvanceResult(session.state, next_poll_at=session.next_poll_at)
                session.pending_invocation = None
                session.next_poll_at = None
                session.pending_poll_count = 0
                if not result.succeeded:
                    raise ProcedureRunError(result.error or f"step failed: {step.id}")
                completed = self._complete_attempt(session, step.id, result.value)
                self._record_event(session, "procedure.attempt_completed", step_id=step.id,
                                   attempt_id=completed.attempt_id, payload={"status": "completed"})
                session.results.append(result.value)
                session.current_step_id = self._next_step_id(session.procedure, step)
                return self._finish_or_ready(session, steps)

            step = steps[session.current_step_id]
            session.current_step = session.procedure.steps.index(step)
            if self._clock() >= session.deadline:
                raise ProcedureRunError("procedure timed out")
            if step.kind is StepKind.INPUT:
                name = step.input_ref.id if step.input_ref else ""
                if name not in session.procedure.parameters:
                    raise ProcedureRunError(f"input parameter is not declared: {name}")
                if name not in session.inputs:
                    session.state = SessionState.WAITING_INPUT
                    return AdvanceResult(session.state, input_parameter=name,
                                         input_prompt=step.prompt,
                                         input_max_length=step.max_input_length)
                session.current_step_id = self._next_step_id(session.procedure, step)
                return self.advance(session)
            if step.kind is StepKind.ACTION or step.kind is StepKind.POLL:
                action = step.action_ref or step.poll_ref
                parameters = _resolve_parameters(dict(step.parameters), session.inputs)
                _authorize_action(self._invoker, action, parameters)
                pending = next((item for item in session._attempts
                                if item.attempt_id == session._pending_attempt_id
                                and item.step_id == step.id), None)
                if pending is None:
                    pending = self._new_attempt(session, step.id, status="pending", inputs=parameters,
                                                supersedes=None)
                    session._pending_attempt_id = pending.attempt_id
                session.pending_invocation = self._invoker.invoke(action, parameters)
                session.pending_poll_count = 0
                session.next_poll_at = self._clock()
                session.state = SessionState.WAITING_ACTION
                return AdvanceResult(session.state, next_poll_at=session.next_poll_at)
            if step.kind is StepKind.BRANCH:
                evaluator = getattr(self._invoker, "evaluate_condition", None)
                if not callable(evaluator):
                    evaluator = getattr(self._invoker, "check_condition", None)
                if not callable(evaluator):
                    session.state = SessionState.WAITING_CONDITION
                    return AdvanceResult(session.state)
                outcome = bool(evaluator(step.condition_ref, dict(session.inputs), tuple(session.results)))
                target = step.then_step_id if outcome else step.else_step_id
                if target is None:
                    raise ProcedureRunError("branch target is missing")
                session.current_step_id = target.id
                return self._finish_or_ready(session, steps)
            if step.kind is StepKind.CHECKPOINT:
                if self._sink_registry is None or self._checkpoint_destination is None or self._checkpoint_executor is None:
                    raise ProcedureRunError("checkpoint sink is not configured")
                payload = _resolve_parameters(dict(step.sink_payload), session.inputs)
                request = self._sink_registry.request(
                    step.sink_id or "", payload,
                    idempotency_key=step.sink_idempotency_key or "checkpoint",
                    checkpoint=self._checkpoint_destination,
                )
                events = getattr(self._invoker, "events", None)
                if events is not None:
                    events.emit("checkpoint.requested", {"sink_id": request.sink_id})
                outcome = self._checkpoint_executor(request)
                if not isinstance(outcome, MutationOutcome):
                    raise ProcedureRunError("checkpoint sink returned an invalid outcome")
                session.current_step_id = self._next_step_id(session.procedure, step)
                if outcome.status != "accepted":
                    message = outcome.detail or f"checkpoint sink {outcome.status}"
                    if step.sink_required:
                        raise ProcedureRunError(message)
                    session.warnings.append(message)
                    result = self._finish_or_ready(session, steps)
                    return AdvanceResult(result.state, value=result.value, error=message)
                completed = self._complete_attempt(session, step.id, request.payload)
                self._record_event(session, "procedure.attempt_completed", step_id=step.id,
                                   attempt_id=completed.attempt_id, payload={"status": "completed"})
                if events is not None:
                    events.emit("checkpoint.completed", {"sink_id": request.sink_id, "status": outcome.status})
                return self._finish_or_ready(session, steps)
            if step.kind is StepKind.COMPLETE:
                session.state = SessionState.SUCCEEDED
                return AdvanceResult(session.state, value=list(session.results))
            raise ProcedureRunError(f"unsupported step kind: {step.kind}")
        except RequiredObserverError as exc:
            error = ProcedureRunError(str(exc))
            self._fail(session, error, kind="required_observer_failed")
            return AdvanceResult(session.state, error=str(error))
        except Exception as exc:
            error = exc if isinstance(exc, ProcedureRunError) else ProcedureRunError(str(exc))
            self._fail(session, error, kind="timed_out" if "timed out" in str(error) else "failed")
            return AdvanceResult(session.state, error=str(error))

    @staticmethod
    def _next_step_id(procedure: Procedure, step: Step) -> str | None:
        return step.next_step_id.id if step.next_step_id else ProcedureEngine._following_id(procedure, step.id)

    def _finish_or_ready(self, session: ProcedureSession, steps: Mapping[str, Step]) -> AdvanceResult:
        if session.current_step_id is None:
            session.state = SessionState.SUCCEEDED
            return AdvanceResult(session.state, value=list(session.results))
        if steps[session.current_step_id].kind is StepKind.COMPLETE:
            session.state = SessionState.SUCCEEDED
            return AdvanceResult(session.state, value=list(session.results))
        session.state = SessionState.READY
        return AdvanceResult(session.state)

    def _bind_run_fence(self, session: ProcedureSession) -> None:
        """Capture the run-owned fence once; cleanup will only match it."""
        events = getattr(self._invoker, "events", None)
        if events is not None:
            session.run_id = events.run_id
            session.controller_generation = events.controller_generation
            return
        generation = getattr(self._invoker, "controller_generation", None)
        if isinstance(generation, int) and not isinstance(generation, bool) and generation >= 0:
            session.controller_generation = generation

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
        while session.state not in {SessionState.SUCCEEDED, SessionState.FAILED, SessionState.ABORTED}:
            update = self.advance(session)
            if update.state is SessionState.WAITING_INPUT:
                if self._input_provider is None:
                    error = ProcedureRunError("input provider is required")
                    self._fail(session, error, kind="failed")
                    raise error
                value = self._input_provider.read(update.input_parameter or "", update.input_prompt or "", update.input_max_length or 1)
                try:
                    self.provide_parameter(session, update.input_parameter or "", value)
                except ValueError as exc:
                    error = ProcedureRunError(str(exc))
                    self._fail(session, error, kind="failed")
                    raise error from exc
            elif update.state is SessionState.WAITING_ACTION and update.next_poll_at is not None:
                self._sleep(max(0.0, update.next_poll_at - self._clock()))
            elif update.state is SessionState.WAITING_CONDITION:
                error = ProcedureRunError("condition evaluator is required")
                self._fail(session, error, kind="failed")
                raise error
            elif update.state is SessionState.FAILED:
                raise ProcedureRunError(update.error or "procedure failed")
        if session.state is SessionState.FAILED:
            raise ProcedureRunError(session.error or "procedure failed")
        return list(session.results)

    def abort(self, session: ProcedureSession, reason: str = "aborted") -> AdvanceResult:
        if session.state in {SessionState.SUCCEEDED, SessionState.FAILED, SessionState.ABORTED}:
            return AdvanceResult(session.state, error=session.error)
        session.error = reason
        session.primary_outcome = PrimaryOutcome(kind="aborted", reason=reason,
                                                 step_id=session.current_step_id)
        session.state = SessionState.ABORTED
        session.pending_invocation = None
        self._fail(session, ProcedureRunError(reason), kind="aborted")
        session.state = SessionState.ABORTED
        return AdvanceResult(session.state, error=reason)

    @staticmethod
    def _following_id(procedure: Procedure, step_id: str) -> str | None:
        ids = [step.id for step in procedure.steps]
        index = ids.index(step_id) + 1
        return ids[index] if index < len(ids) else None

    def _fail(self, session: ProcedureSession, error: ProcedureRunError, *, kind: str) -> None:
        session.state = SessionState.FAILED
        session.error = str(error)
        if session.primary_outcome is None:
            step_id = session.procedure.steps[session.current_step].id if session.procedure.steps else None
            session.primary_outcome = PrimaryOutcome(kind=kind, reason=str(error), step_id=step_id)
        if session.cleanup_attempted:
            return
        session.cleanup_attempted = True
        session.cleanup_outcome.status = "running"
        seen: set[tuple[str, str | int | None]] = set()
        for action in session.procedure.abort_actions:
            key = (action.id, action.version)
            if key in seen:
                continue
            seen.add(key)
            self._run_cleanup_action(session, action)
        if not session.cleanup_outcome.actions:
            session.cleanup_outcome.status = "succeeded"
        elif all(result.status == "succeeded" for result in session.cleanup_outcome.actions):
            session.cleanup_outcome.status = "succeeded"
        else:
            session.cleanup_outcome.status = "failed"

    def _run_cleanup_action(self, session: ProcedureSession, action: Any) -> None:
        """Execute exactly one fresh, fenced cleanup action and retain its result."""
        try:
            description = getattr(self._invoker, "describe", lambda _: None)(action)
            if not isinstance(description, Mapping) or description.get("id") != action.id or description.get("version") != action.version:
                raise ProcedureRunError("cleanup action authorization is invalid")
            if description.get("authorized") is not True:
                session.cleanup_outcome.actions.append(CleanupActionOutcome(action, "unauthorized"))
                return
            expected = session.controller_generation
            if expected is None or description.get("controller_generation") != expected:
                session.cleanup_outcome.actions.append(CleanupActionOutcome(action, "generation_mismatch"))
                return
            self._invoker.preflight(action, {})
            invocation = self._invoker.invoke(action, {})
            result = self._invoker.poll(invocation)
            if result.done and result.succeeded:
                session.cleanup_outcome.actions.append(CleanupActionOutcome(action, "succeeded"))
            else:
                session.cleanup_outcome.actions.append(CleanupActionOutcome(
                    action, "failed" if result.done else "incomplete", result.error,
                ))
        except Exception as exc:
            session.cleanup_outcome.actions.append(CleanupActionOutcome(
                action, "failed", f"{type(exc).__name__}: {exc}",
            ))
