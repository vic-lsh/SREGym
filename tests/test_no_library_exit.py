"""Library code never ends the process: an exit hides the cause and kills the fastloop worker.

The worker's serve loop reports an ``Exception`` and keeps serving, so the driver can recover the
faults an injection already made. ``SystemExit`` skips that, and the worker takes its record of
those faults with it. Only a ``__main__`` entry point may exit.
"""

import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1] / "sregym"


def _is_exit_call(node: ast.AST) -> bool:
    if not isinstance(node, ast.Call):
        return False
    func = node.func
    if isinstance(func, ast.Name):
        return func.id in {"exit", "quit"}
    return (
        isinstance(func, ast.Attribute)
        and isinstance(func.value, ast.Name)
        and (func.value.id, func.attr) in {("sys", "exit"), ("os", "_exit")}
    )


def _main_guard_lines(tree: ast.Module) -> set[int]:
    lines: set[int] = set()
    for node in tree.body:
        if (
            isinstance(node, ast.If)
            and isinstance(node.test, ast.Compare)
            and isinstance(node.test.left, ast.Name)
            and node.test.left.id == "__name__"
        ):
            lines.update(range(node.lineno, node.end_lineno + 1))
    return lines


def test_only_main_entry_points_exit_the_process():
    offenders = []
    for path in sorted(ROOT.rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        allowed = _main_guard_lines(tree)
        offenders.extend(
            f"{path.relative_to(ROOT.parent)}:{node.lineno}"
            for node in ast.walk(tree)
            if _is_exit_call(node) and node.lineno not in allowed
        )
    assert offenders == []
