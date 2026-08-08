#!/usr/bin/env python3
"""Check documentation conventions that MkDocs does not validate."""

from pathlib import Path
import re
import sys

import yaml


ROOT = Path(__file__).resolve().parents[1]
DOCS = ROOT / "docs"
MKDOCS = ROOT / "mkdocs.yml"

FORBIDDEN = {
    "my_gcp_project": "use the valid project ID my-gcp-project",
    "my_billing_project": "use the valid project ID my-billing-project",
    "localhost:8000": "use a relative documentation link",
    'title="DuckDB CLI"': "use an ordinary commented SQL block",
}


def markdown_files() -> list[Path]:
    return [ROOT / "README.md", *sorted(DOCS.rglob("*.md"))]


def navigation_targets(value: object):
    if isinstance(value, str):
        yield value
    elif isinstance(value, list):
        for item in value:
            yield from navigation_targets(item)
    elif isinstance(value, dict):
        for item in value.values():
            yield from navigation_targets(item)


def main() -> int:
    errors: list[str] = []

    for path in markdown_files():
        relative = path.relative_to(ROOT)
        for line_number, line in enumerate(path.read_text().splitlines(), 1):
            for text, guidance in FORBIDDEN.items():
                if text in line:
                    errors.append(f"{relative}:{line_number}: {guidance}")
            if re.match(r"^\s*D(?: |$)", line):
                errors.append(f"{relative}:{line_number}: remove the DuckDB CLI prompt")
            if re.match(r"^#{1,6} .*\s+$", line):
                errors.append(f"{relative}:{line_number}: heading has trailing whitespace")

    config = yaml.safe_load(MKDOCS.read_text())
    for target in navigation_targets(config.get("nav", [])):
        page = DOCS / target
        if not page.is_file():
            errors.append(f"mkdocs.yml: navigation target does not exist: {target}")

    if errors:
        print("Documentation checks failed:", file=sys.stderr)
        for error in errors:
            print(f"- {error}", file=sys.stderr)
        return 1

    print("Documentation conventions and navigation are valid.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
