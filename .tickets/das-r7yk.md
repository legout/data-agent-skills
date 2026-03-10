---
id: das-r7yk
status: open
deps: [das-8erm, das-6zd4, das-lih7]
links: [das-8erm, das-6zd4]
created: 2026-03-10T15:59:03Z
type: chore
priority: 2
assignee: legout
parent: das-uubf
tags: [skill-refactor, foundation, ci, lint, evals]
---
# Wire strict lint and eval-presence checks into CI

Make the new checks part of the default merge path instead of optional local validation.

## Acceptance Criteria

- CI runs the strict linter mode
- CI verifies eval manifests exist for the top-level skills
- CI output makes failures actionable for contributors

