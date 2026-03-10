---
id: das-uubf
status: open
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

