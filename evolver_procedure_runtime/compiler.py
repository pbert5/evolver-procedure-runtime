"""Compile a validated declarative procedure; never compile executable code."""
from __future__ import annotations
from collections.abc import Mapping
from typing import Any
from .model import Procedure
from .schema import SchemaError, build_procedure
class ProcedureCompileError(ValueError): pass
def compile_procedure(document: Mapping[str, Any]) -> Procedure:
    try:
        # Keep the pre-freeze internal engine fixtures readable while making
        # every new document go through the strict frozen schema.  This shim
        # is intentionally limited to the old three-key shape and cannot
        # accept any new/unknown field or create an execution mechanism.
        if set(document) == {"id", "version", "steps"}:
            document = {
                "id": document["id"], "name": document["id"], "version": document["version"],
                "purpose": "legacy procedure fixture", "parameters": {},
                "entry_step_id": {"type": "step", "id": document["steps"][0]["id"]},
                "steps": [
                    {"id": step["id"], "kind": "action", "action": {"type": "action", "id": step["action"]},
                     "parameters": step.get("parameters", {}), "timeout_polls": step.get("timeout_polls", 1),
                     "poll_interval_s": step.get("poll_interval_s", 0)}
                    for step in document["steps"]
                ],
                "abort_actions": [], "default_timeout": 60, "metadata": {},
            }
        return build_procedure(document)
    except (SchemaError, TypeError, KeyError) as exc: raise ProcedureCompileError(str(exc)) from exc
