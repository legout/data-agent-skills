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


def extract_content_blocks(text: str) -> list[tuple[str, int]]:
    """Extract content blocks: code fences and bullet lists with their line numbers."""
    blocks: list[tuple[str, int]] = []
    # Code blocks ```...```
    for m in re.finditer(r"```[a-z]*\n(.*?)\n```", text, re.S):
        block = m.group(1).strip()
        start_line = text[: m.start()].count("\n") + 1
        blocks.append((block, start_line))
    # Bullet lists (consecutive lines starting with -, *, or number.)
    list_pattern = r"(?:^[ \t]*(?:[-*]|\d+\.)[ \t]+.+\n?)+"
    for m in re.finditer(list_pattern, text, re.M):
        block = m.group(0).strip()
        if len(block.splitlines()) > 1:  # Only multi-line lists
            start_line = text[: m.start()].count("\n") + 1
            blocks.append((block, start_line))
    return blocks


def lint_duplicate_content(all_md_files: list[Path], findings: list[Finding]) -> None:
    """Find markdown content blocks appearing in 3+ files with >100 identical lines total."""
    # Map of content block -> list of (file, line) where it appears
    block_locations: dict[str, list[tuple[Path, int]]] = {}

    for md_file in all_md_files:
        text = md_file.read_text(errors="replace")
        blocks = extract_content_blocks(text)
        for block, line_num in blocks:
            if len(block.splitlines()) > 5:  # Only blocks >5 lines
                if block not in block_locations:
                    block_locations[block] = []
                block_locations[block].append((md_file, line_num))

    # Flag blocks appearing in 3+ unique files with >100 total lines
    for block, locations in block_locations.items():
        unique_files = list(dict.fromkeys(loc[0] for loc in locations))  # Preserve order, dedupe
        if len(unique_files) >= 3:
            # Use unique file count for total_lines to match "3+ files with >100 lines"
            total_lines = len(block.splitlines()) * len(unique_files)
            if total_lines > 100:
                # Show grandparent/parent/name to distinguish files with same name in different dirs
                def file_location(f: Path) -> str:
                    parts = f.parts
                    return "/".join(parts[-3:]) if len(parts) >= 3 else str(f)
                files_str = ", ".join(file_location(f) for f in unique_files[:3])
                if len(unique_files) > 3:
                    files_str += f" and {len(unique_files) - 3} more"
                first_file = unique_files[0]
                findings.append(
                    Finding(
                        "warn",
                        first_file,
                        f"duplicate content block ({len(unique_files)} files, {total_lines} lines): appears in {files_str}",
                    )
                )


def lint_toc_required(md_file: Path, findings: list[Finding]) -> None:
    """Flag reference markdown files >100 lines without a Table of Contents."""
    # Only apply to reference documents (references/**/*.md), not SKILL.md files
    if "references" not in md_file.parts:
        return

    text = md_file.read_text(errors="replace")
    lines = text.splitlines()

    if len(lines) > 100:
        # Check for Table of Contents heading (various formats)
        has_toc = any(
            re.search(r"^#{1,4}\s*(?:Table of Contents|TOC|Contents)\s*$", line, re.I)
            for line in lines
        )
        if not has_toc:
            findings.append(
                Finding(
                    "warn",
                    md_file,
                    f"file has {len(lines)} lines but no Table of Contents (## Table of Contents)",
                )
            )


def lint_stale_year(md_file: Path, findings: list[Finding]) -> None:
    """Detect (YYYY) year markers in h1/h2 headings."""
    text = md_file.read_text(errors="replace")
    # Find h1/h2 headings with year markers (2020-2029)
    for m in re.finditer(r"^(#{1,2} .*)\((202[0-9])\)", text, re.M):
        heading = m.group(0)
        year = m.group(2)
        line_num = text[: m.start()].count("\n") + 1
        findings.append(
            Finding(
                "warn",
                md_file,
                f"stale year marker '({year})' in heading at line {line_num}: {heading[:50]}...",
            )
        )


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

    # Collect all markdown files for duplicate detection
    all_md_files = list(sorted(iter_files(root, ".md")))

    for md_file in all_md_files:
        lint_markdown_references(md_file, all_files, findings, args.strict)
        lint_python_fences(md_file, findings)
        lint_toc_required(md_file, findings)
        lint_stale_year(md_file, findings)

    # Cross-file duplicate content check
    lint_duplicate_content(all_md_files, findings)

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
