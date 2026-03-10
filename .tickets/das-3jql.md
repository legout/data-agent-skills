---
id: das-3jql
status: closed
deps: []
links: [das-xl5m, das-b143]
created: 2026-03-10T15:59:01Z
closed: 2026-03-10T17:57:17+01:00
type: task
priority: 0
assignee: legout
parent: das-ngoo
tags: [skill-refactor, foundation, taxonomy, naming]
---
# Approve the final 14-skill map and naming conventions

Lock the future skill names and routing language before templates and evals are expanded.

## Acceptance Criteria

- the approved 14-skill list is recorded in repo docs
- naming rules are explicit and consistent with the plan
- adjacent-skill boundaries are called out where trigger confusion is likely

## Implementation Note (2026-03-10)

- Created docs/skill-map.md with 14-skill architecture (9 DE + 5 DS)
- Documented 4 naming rules from §9.1 (action-oriented, short, consistent verbs, kebab-case)
- Added adjacent-skill boundary guidance for EDA/Visualization, Quality/Observability, Orchestration/FlowerPower
- Post-fix review: 0 Critical/Major issues, 1 non-blocking suggestion
- Commit: ddf29df

