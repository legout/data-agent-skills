---
id: das-n3x8
status: closed
deps: [das-llsd]
links: [das-ekec, das-llsd, das-09vu]
created: 2026-03-10T15:55:10Z
type: task
priority: 2
assignee: legout
parent: das-0p8u
tags: [skill-refactor, data-engineering, orchestration]
---
# Create orchestrating-data-pipelines for Prefect, Dagster, dbt, and deployment workflows

Rewrite orchestration guidance around scheduling, retries, deployment, and operational patterns.

## Acceptance Criteria

- new orchestrating-data-pipelines skill exists with clear workflow boundaries
- Prefect, Dagster, and dbt guidance is consolidated under direct references
- touched content has eval coverage and explicit related-skill routing


## Notes

**2026-03-11T09:55:57Z**

Implementation complete:
- Created orchestrating-data-pipelines skill consolidating Prefect/Dagster/dbt/FlowerPower orchestration guidance
- Fixed major location issue: moved from .pi/agent/skills/ to canonical skills/ directory
- All 5 skill files created (SKILL.md + 4 reference files)
- Eval alignment verified (5 eval cases covered)
- Review-post-fix: Clear pass (no blocking issues)
- Commit: 30b0076
