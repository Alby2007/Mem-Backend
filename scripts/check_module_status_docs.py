"""CI check: keep module-status docs aligned across architecture/codemap/package notes."""

from __future__ import annotations

from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
ARCH = ROOT / "docs" / "architecture.md"
CODEMAP = ROOT / "docs" / "codemap.md"
KNOWLEDGE_INIT = ROOT / "knowledge" / "__init__.py"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _assert_contains(content: str, needle: str, file_label: str, errors: list[str]) -> None:
    if needle not in content:
        errors.append(f"{file_label} missing expected text: {needle}")


def main() -> int:
    errors: list[str] = []

    arch = _read(ARCH)
    codemap = _read(CODEMAP)
    kinit = _read(KNOWLEDGE_INIT)

    checks = {
        "docs/architecture.md": [
            "## Module Status",
            "- **Live** — imported and executed in startup/request path",
            "- **Partial** — schema/API wiring is live, but downstream decision logic is not yet integrated",
            "- **Dormant** — file exists but is not wired into live runtime path",
            "| `graph_v2.py` | Still dormant",
            "| `graph_enhanced.py` | Still dormant",
            "| `confidence_intervals.py` | `ensure_confidence_columns()` runs at startup; `GET /kb/confidence` endpoint is exposed | Interval output does not yet feed back into `position_size_pct` (planned v2) |",
            "| `causal_graph.py` | Startup + overlay path + API |",
        ],
        "docs/codemap.md": [
            "### Knowledge Extension Module Status",
            "| `graph_v2.py` | Dormant |",
            "| `graph_enhanced.py` | Dormant |",
            "| `confidence_intervals.py` | Partial |",
            "| `causal_graph.py` | Live |",
        ],
        "knowledge/__init__.py": [
            "# Status taxonomy:",
            "#   PARTIAL — wired for schema/API visibility but not yet feeding downstream decision logic",
            "#   causal_graph.py       — causal edge table init + traversal + causal-edge APIs",
            "#   confidence_intervals.py — conf_n/conf_var schema + /kb/confidence API are live;",
            "# DORMANT (not wired):",
            "#   graph_v2.py           — async graph with versioning (requires aiosqlite)",
            "#   graph_enhanced.py     — sync graph with taxonomy system (separate DB schema)",
        ],
    }

    content_map = {
        "docs/architecture.md": arch,
        "docs/codemap.md": codemap,
        "knowledge/__init__.py": kinit,
    }

    for label, needles in checks.items():
        body = content_map[label]
        for needle in needles:
            _assert_contains(body, needle, label, errors)

    if errors:
        print("Module status doc check failed:")
        for err in errors:
            print(f"- {err}")
        return 1

    print("Module status doc check passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
