---
id: das-ngoo
status: closed
deps: [das-3jql, das-xl5m, das-b143]
links: []
created: 2026-03-10T15:55:10Z
type: task
priority: 1
assignee: legout
parent: das-1f71
tags: [skill-refactor, foundation, taxonomy, templates]
---
# Finalize the 14-skill taxonomy, naming rules, templates, and dependsOn policy

Turn the plan into repo-authoritative naming and authoring rules so every rewrite follows one contract.

## Acceptance Criteria

- publish the approved 14-skill list and naming conventions
- add reusable SKILL.md and reference templates
- record the final dependsOn keep/remove decision in repo docs


## Notes

**2026-03-10T20:26:35Z**

Implementation complete:

- Created docs/TAXONOMY.md (348 lines) as single source of truth
- Documents: 14-skill taxonomy (9 DE + 5 DS), naming rules, frontmatter policy, dependsOn removal decision
- Links to: skill-authoring.md, skill-map.md, templates/, eval/
- Includes: 11-point authoring checklist, migration table, lint compliance guidance
- Commit: d544a51
- Tests: skipped (documentation only)
- Gate: Clear pass (no issues in review or post-fix)
