"""Fail when the core package mentions a specific agency."""

import sys
from pathlib import Path

FORBIDDEN = ("gzm", "ztm", "metropolia", "transportgzm", "otwartedane")


def find_violations(root: Path) -> list[str]:
    """Return one line per forbidden term found under root."""
    violations = []
    for path in sorted(root.rglob("*.py")):
        text = path.read_text(encoding="utf-8")
        for number, line in enumerate(text.splitlines(), start=1):
            lowered = line.lower()
            violations += [f"{path}:{number}: {term}" for term in FORBIDDEN if term in lowered]
    return violations


def main() -> int:
    root = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("src/gtfs_hub")
    violations = find_violations(root)
    print("\n".join(violations) or f"{root}: no agency names")
    return 1 if violations else 0


if __name__ == "__main__":
    raise SystemExit(main())
