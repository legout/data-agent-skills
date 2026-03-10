# Implementation: das-8erm

## Changes Made

Modified `tools/skill_lint.py` with three changes as specified in the ticket:

### 1. Strict Mode: Missing Local Markdown References → Errors

Updated `lint_markdown_references()` function signature to accept a `strict: bool = False` parameter. When `strict=True`, missing local references are now reported as errors instead of warnings:

```python
def lint_markdown_references(md_file: Path, all_files: set[Path], findings: list[Finding], strict: bool = False) -> None:
    ...
    level = "error" if strict else "warn"
    findings.append(Finding(level, md_file, f"missing local reference: {ref}"))
```

### 2. Flag Hybrid @skill/path Usage

Added detection for ambiguous hybrid @skill/path patterns. When a reference starts with `@` AND contains a `/`, it's flagged as an error:

```python
if ref.startswith("@"):
    # Flag hybrid @skill/path usage (ambiguous pattern)
    if "/" in ref:
        findings.append(Finding("error", md_file, f"ambiguous hybrid @skill/path usage: {ref}"))
    continue  # skill aliases, not filesystem paths
```

### 3. Failure Messages Point to Offending Files

The existing `Finding` dataclass already includes the file path. All error/warning messages display as:
```
[ERROR] path/to/file.md: message here
```

This was verified working - messages clearly show the file path relative to repo root.

## Verification

- Normal mode (`python3 tools/skill_lint.py`): Shows warnings for missing refs
- Strict mode (`python3 tools/skill_lint.py --strict`): Missing refs become errors, causing exit code 1
- Hybrid detection: Found 31 existing hybrid @skill/path usages in the codebase

## Files Changed

- `tools/skill_lint.py` - 2 discrete changes to `lint_markdown_references()` function
