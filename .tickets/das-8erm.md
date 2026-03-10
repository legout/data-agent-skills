---
id: das-8erm
status: closed
deps: [das-3jql]
links: [das-6zd4, das-r7yk]
created: 2026-03-10T15:59:02Z
type: chore
priority: 1
assignee: legout
parent: das-uubf
tags: [skill-refactor, foundation, lint, reference-integrity]
---
# Make skill_lint fail on missing refs and hybrid @skill/path links

Cover the highest-signal structural errors first so broken or ambiguous references cannot merge.

## Acceptance Criteria

- strict lint fails on missing local markdown references
- strict lint flags hybrid @skill/path usage
- failure messages point contributors at the offending files


## Notes

**2026-03-10T17:12:09Z**

Implementation complete:

- Added `strict` mode flag to lint_markdown_references(): missing local refs become errors with --strict
- Added detection for hybrid @skill/path patterns (flagged as ambiguous errors)
- All findings include file attribution in output format

Files changed: tools/skill_lint.py (+7/-3)
Commit: 671199e
Review: Clear pass (no issues)
