#!/usr/bin/env python3
"""Lint SKILL files and bundled markdown references.

Checks:
- SKILL.md frontmatter validity (name/description)
- optional Agent Skills name format checks
- oversized SKILL.md bodies (>500 lines) warning
- broken local markdown references
- Python fenced code syntax in markdown

Usage:
    python tools/skill_lint.py
    python tools/skill_lint.py --strict
"""

from __future__ import annotations

import argparse
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import yaml

IGNORE_PARTS = {".git", ".ruff_cache", "node_modules", ".venv", "venv"}


@dataclass
class Finding:
    level: str  # error | warn
    path: Path
    message: str


def iter_files(root: Path, suffix: str | None = None) -> Iterable[Path]:
    for p in root.rglob("*"):
        if not p.is_file():
            continue
        if any(part in IGNORE_PARTS for part in p.parts):
            continue
        if suffix and p.suffix != suffix:
            continue
        yield p


def parse_frontmatter(text: str) -> dict | None:
    m = re.match(r"^---\n(.*?)\n---\n", text, re.S)
    if not m:
        return None
    data = yaml.safe_load(m.group(1))
    if not isinstance(data, dict):
        return None
    return data


def lint_frontmatter(skill_file: Path, findings: list[Finding]) -> None:
    text = skill_file.read_text(errors="replace")
    fm = parse_frontmatter(text)
    if fm is None:
        findings.append(Finding("error", skill_file, "missing or invalid YAML frontmatter"))
        return

    name = fm.get("name")
    description = fm.get("description")

    if not name:
        findings.append(Finding("error", skill_file, "missing frontmatter field: name"))
    if not description:
        findings.append(Finding("error", skill_file, "missing frontmatter field: description"))

    if isinstance(name, str):
        if len(name) > 64:
            findings.append(Finding("error", skill_file, "name exceeds 64 characters"))
        if not re.fullmatch(r"[a-z0-9-]+", name):
            findings.append(Finding("error", skill_file, "name contains invalid characters"))
        if name.startswith("-") or name.endswith("-") or "--" in name:
            findings.append(Finding("error", skill_file, "name has invalid hyphen placement"))
        # portability warning (agentskills spec)
        if skill_file.parent.name != name:
            findings.append(
                Finding(
                    "warn",
                    skill_file,
                    f"name '{name}' != directory '{skill_file.parent.name}' (portability warning)",
                )
            )

    if isinstance(description, str) and len(description) > 1024:
        findings.append(Finding("error", skill_file, "description exceeds 1024 characters"))

    lines = text.count("\n") + 1
    if lines > 500:
        findings.append(Finding("warn", skill_file, f"SKILL.md has {lines} lines (>500 recommended)"))

    extra_fields = [k for k in fm.keys() if k not in {"name", "description", "license", "compatibility", "metadata", "allowed-tools"}]
    if extra_fields:
        findings.append(
            Finding(
                "warn",
                skill_file,
                f"non-standard frontmatter fields: {', '.join(extra_fields)}",
            )
        )


def lint_markdown_references(md_file: Path, all_files: set[Path], findings: list[Finding], strict: bool = False) -> None:
    text = md_file.read_text(errors="replace")

    # Markdown links [text](path)
    refs = re.findall(r"\[[^\]]+\]\(([^)]+)\)", text)
    # Backtick paths like `foo/bar.md`
    refs += re.findall(r"`([^`\n]+\.md)`", text)

    for raw in refs:
        ref = raw.strip()
        if not ref or ref.startswith("http") or ref.startswith("#"):
            continue
        if "<" in ref or ">" in ref:
            continue  # template path placeholders
        if ref.startswith("@"):
            # Flag hybrid @skill/path usage (ambiguous pattern)
            if "/" in ref:
                findings.append(Finding("error", md_file, f"ambiguous hybrid @skill/path usage: {ref}"))
            continue  # skill aliases, not filesystem paths

        # remove optional anchor
        path_part = ref.split("#", 1)[0]
        target = (md_file.parent / path_part).resolve()
        if target not in all_files:
            # second chance: repo-root relative
            root_target = (Path.cwd() / path_part).resolve()
            if root_target not in all_files:
                level = "error" if strict else "warn"
                findings.append(Finding(level, md_file, f"missing local reference: {ref}"))


def lint_python_fences(md_file: Path, findings: list[Finding]) -> None:
    text = md_file.read_text(errors="replace")
    for m in re.finditer(r"```python\n(.*?)\n```", text, re.S):
        code = m.group(1)
        start_line = text[: m.start()].count("\n") + 1
        try:
            compile(code, str(md_file), "exec")
        except SyntaxError as e:
            line = start_line + ((e.lineno or 1) - 1)
            findings.append(Finding("error", md_file, f"python fence syntax error at line {line}: {e.msg}"))


def main() -> int:
    parser = argparse.ArgumentParser(description="Lint agent skills and markdown references")
    parser.add_argument("--strict", action="store_true", help="treat warnings as errors")
    args = parser.parse_args()

    root = Path.cwd().resolve()
    all_files = {p.resolve() for p in iter_files(root)}

    findings: list[Finding] = []

    for skill in sorted(iter_files(root)):
        if skill.name == "SKILL.md":
            lint_frontmatter(skill, findings)

    for md_file in sorted(iter_files(root, ".md")):
        lint_markdown_references(md_file, all_files, findings, args.strict)
        lint_python_fences(md_file, findings)

    errors = [f for f in findings if f.level == "error"]
    warns = [f for f in findings if f.level == "warn"]

    for f in findings:
        print(f"[{f.level.upper()}] {f.path.relative_to(root)}: {f.message}")

    print(f"\nSummary: {len(errors)} error(s), {len(warns)} warning(s)")

    if errors:
        return 1
    if args.strict and warns:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
