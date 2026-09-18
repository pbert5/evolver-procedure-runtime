from __future__ import annotations

import subprocess
import sys


def test_public_namespace_imports_without_checkout_root_on_pythonpath(tmp_path):
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "from evolver_procedure_runtime import ProcedureEngine; print(ProcedureEngine.__name__)",
        ],
        cwd=tmp_path,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "ProcedureEngine"
