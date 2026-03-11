---
id: das-uubf
status: closed
deps: [das-ngoo, das-hhlo, das-8erm, das-6zd4, das-r7yk]
links: []
created: 2026-03-10T15:55:10Z
type: chore
priority: 2
assignee: legout
parent: das-1f71
tags: [skill-refactor, foundation, lint, ci]
---
# Tighten lint and CI gates for refs, duplicates, TOCs, hybrid refs, stale year tags, and eval presence

Make the refactor enforceable by extending the linter and wiring strict checks into CI.

## Acceptance Criteria

- strict lint fails on missing local refs and hybrid @skill/path links
- duplicate markdown detection and long-file TOC checks are enforced
- CI runs the strict checks and verifies eval manifests exist


## Notes

**2026-03-11T11:44:37Z**

## Implementation Summary

- Verified lint strict mode correctly fails on missing local refs and hybrid @skill/path links (97 errors detected)
- Verified duplicate markdown detection and long-file TOC checks are enforced
- Verified CI workflow runs strict lint checks and eval manifest presence verification
- Added `.subagent-runs` and `.tf` to lint ignore list to prevent noise from transient directories

**Key files changed:** tools/skill_lint.py
**Test/validation:** review-post-fix clear pass
**Commit:** ffa0c64
