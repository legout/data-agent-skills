---
id: das-6zd4
status: open
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

