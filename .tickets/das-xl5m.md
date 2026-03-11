---
id: das-xl5m
status: closed
deps: [das-3jql]
links: [das-3jql, das-b143]
created: 2026-03-10T15:59:01Z
type: task
priority: 1
assignee: legout
parent: das-ngoo
tags: [skill-refactor, foundation, templates, authoring]
---
# Add reusable SKILL.md and reference templates

Turn the agreed structure into reusable templates so rewrites stay consistent.

## Acceptance Criteria

- a reusable SKILL.md template exists
- a reusable long-reference template exists with TOC guidance
- template docs explain direct-link and progressive-disclosure expectations


## Notes

**2026-03-10T20:14:39Z**

Implementation complete:
- Created docs/templates/skill-template.md with standard SKILL.md structure (frontmatter, use/not-use, workflow, progressive disclosure, related skills)
- Created docs/templates/reference-template.md with TOC requirement and deep-dive structure
- Created docs/templates/README.md documenting direct linking, progressive disclosure, and no-hybrid-notation policies
- Fixed broken ../README.md link in reference template, standardized validate.py naming
- Gate: Clear pass (review-post-fix)
- Commit: 9c64b24
