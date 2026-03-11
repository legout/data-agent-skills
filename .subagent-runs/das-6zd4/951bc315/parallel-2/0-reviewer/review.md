## Review

- What's correct
  - Added all three requested checks in `tools/skill_lint.py` and integrated them into `main()`.
  - Duplicate-content check correctly performs a cross-file pass after collecting all markdown files.
  - Stale-year detection is warning-level (not error), which matches the “may be intentional” risk noted in planning.

- Issue [Major]: TOC check is applied to all markdown files, not just long references, which is broader than ticket scope and acceptance wording (“long references without a TOC”). File: `tools/skill_lint.py` (`main()`, `lint_toc_required`). Suggested fix: restrict TOC enforcement to reference documents only (e.g., `references/**/*.md` or equivalent repo convention) before calling `lint_toc_required`.

- Issue [Minor]: Duplicate-line threshold uses occurrence count (`len(locations)`) rather than unique-file count, so repeated copies in a single file can inflate `total_lines` and trigger false positives. File: `tools/skill_lint.py` (`lint_duplicate_content`). Suggested fix: compute threshold with unique files (or explicitly dedupe `(file, block)` before counting) to align with “appears in 3+ files”.

- Note: Observations
  - `anchor-context.md` at the run path was missing; review used `implementation.md`, `plan.md`, ticket file, and diffed hunks in `tools/skill_lint.py`.

- Gate: Uncertain
