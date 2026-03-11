---
id: das-6zd4
status: closed
deps: [das-xl5m]
links: [das-8erm, das-r7yk]
created: 2026-03-10T15:59:03Z
type: chore
priority: 2
assignee: legout
parent: das-uubf
tags: [skill-refactor, foundation, lint, content-quality]
---
# Add duplicate-content, TOC, and stale-year checks to skill_lint

Teach the linter to catch the structural problems highlighted by the refactor plan.

## Acceptance Criteria

- duplicate markdown content above the agreed threshold is reported
- long references without a TOC are flagged
- stale year markers like 2026 in headings are flagged


## Notes

**2026-03-10T20:55:32Z**

Implementation complete:
- Added lint_duplicate_content(): detects blocks >5 lines in 3+ files with >100 total lines
- Added lint_toc_required(): enforces TOC for references/**/*.md only (>100 lines)
- Added lint_stale_year(): flags (202[0-9]) year markers in h1/h2 headings
- Fixed post-review: TOC scope limited to references/, duplicate count uses unique files

Test: python tools/skill_lint.py runs clean (34 errors, 145 warnings as expected)
Commit: 3a7c6b2
