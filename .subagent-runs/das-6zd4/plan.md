# Implementation Plan

## Goal
Add three new lint checks to `tools/skill_lint.py`: duplicate content detection, TOC requirement for long references, and stale year markers in headings.

## Tasks

1. **Add duplicate content detection function**
   - File: `tools/skill_lint.py`
   - Add `lint_duplicate_content(md_file: Path, findings: list[Finding]) -> None`
   - Implementation: Find markdown content blocks (>5 lines) that appear in 3+ files
   - Pattern: Compare code blocks (```...```) and bullet lists across all markdown files
   - Threshold: Flag when same block appears in 3+ files with >100 identical lines total
   - Acceptance: Run `python tools/skill_lint.py` and verify it flags duplicate content

2. **Add TOC check function**
   - File: `tools/skill_lint.py`
   - Add `lint_toc_required(md_file: Path, findings: list[Finding]) -> None`
   - Implementation: Check markdown files >100 lines for "Table of Contents" header
   - Pattern: Look for `## Table of Contents` or similar heading
   - Threshold: Warning for .md files with >100 lines missing TOC
   - Acceptance: Test on files in `skills/` with >100 lines to verify detection

3. **Add stale year detection function**
   - File: `tools/skill_lint.py`
   - Add `lint_stale_year(md_file: Path, findings: list[Finding]) -> None`
   - Implementation: Detect `(YYYY)` year markers in headings (h1/h2)
   - Pattern: Regex `^#{1,2} .*\(202[0-9]\)` for years 2020-2029
   - Level: Warning (may be intentional)
   - Acceptance: Verify it flags `## Library selection guide (2026)` pattern

4. **Integrate new checks into main loop**
   - File: `tools/skill_lint.py`
   - Modify `main()` to call new lint functions on markdown files
   - Add after existing `lint_python_fences()` call
   - Ensure duplicate check has access to all files for cross-file comparison
   - Acceptance: All three checks run on `python tools/skill_lint.py`

5. **Test the implementation**
   - Run: `python tools/skill_lint.py`
   - Verify: No syntax errors, checks execute on all markdown files
   - Spot-check: Confirm existing findings still reported correctly

## Files to Modify
- `tools/skill_lint.py` - add three new lint functions and integrate into main loop

## New Files
None

## Dependencies
- Task 4 depends on Tasks 1, 2, 3 (functions must exist before being called)
- Task 5 depends on Task 4 (integration must be complete before testing)

## Risks
- Duplicate content detection may be slow on large file sets - consider optimizing with hash comparison
- TOC check may have false positives on short files - keep 100-line threshold
- Stale year detection may flag intentional dates - keep as warning level only