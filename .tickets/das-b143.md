---
id: das-b143
status: closed
deps: [das-3jql]
links: [das-xl5m, das-3jql]
created: 2026-03-10T15:59:01Z
type: task
priority: 1
assignee: legout
parent: das-ngoo
tags: [skill-refactor, foundation, metadata, frontmatter]
---
# Document the standard frontmatter policy and the dependsOn decision

Decide what metadata is allowed in skill frontmatter and document the dependsOn outcome clearly.

## Acceptance Criteria

- the allowed frontmatter fields are documented
- the dependsOn keep/remove decision is recorded with rationale
- authoring docs describe how related-skill routing should work without ambiguity


## Notes

**2026-03-10T17:32:45Z**

Implementation complete: docs/skill-authoring.md created with frontmatter policy, dependsOn removal rationale, related-skill routing patterns, naming rules, and lint guidance.

Files changed: docs/skill-authoring.md (new)
Commit: 7562c74
Tests: skipped (documentation only)

Blocker: Post-fix review gate 'Uncertain' due to missing chain artifacts (anchor-context.md, implementation.md not at expected path). Content verified complete with 0 Critical/Major issues in original review. Recommend re-run with complete artifact chain or manual gate verification.
