## Review

- What's correct
  - The previously reported **Major** issue is clearly resolved in `tools/skill_lint.py`: `lint_toc_required()` now returns early unless the file path contains a `references` segment, so TOC enforcement is scoped to reference docs instead of all markdown files.
  - The previously reported **Minor** issue is clearly resolved in `tools/skill_lint.py`: duplicate-content `total_lines` now uses `len(unique_files)` rather than raw occurrence count, aligning with the intended “appears in 3+ files” semantics and avoiding inflation from repeated blocks within one file.
  - The fix summary and test signal are consistent with the code changes: warning count reduction (205 → 145) and TOC warnings constrained to reference docs support correct scope narrowing.

- Note: Observations
  - Quick re-check was limited to the implementation/fix scope (`tools/skill_lint.py`) and focused on prior critical/major findings from `review.md`.
  - No new critical/major regressions were observed in the touched hunks.

- Gate: Clear pass
