---
id: das-llsd
status: closed
deps: [das-hhlo]
links: [das-n3x8, das-ekec]
created: 2026-03-10T15:55:10Z
type: task
priority: 1
assignee: legout
parent: das-0p8u
tags: [skill-refactor, data-engineering, pipelines]
---
# Create building-data-pipelines by merging core ETL and best-practices content

Build the first refactored engineering skill and use it as the exemplar for the new structure.

## Acceptance Criteria

- new building-data-pipelines skill exists with clear scope and direct references
- relevant content from data-engineering-core and data-engineering-best-practices is absorbed
- touched content has no broken local references and has eval coverage


## Notes

**2026-03-10T21:42:27Z**

Implementation complete: Created building-data-pipelines skill by merging data-engineering-core and data-engineering-best-practices.

Key files:
- skills/building-data-pipelines/SKILL.md (main skill file with when-to-use sections)
- references/pipeline-patterns.md (ETL patterns, incremental loading)
- references/production-architecture.md (medallion, partitioning, lifecycle)
- references/crud-operations.md (append/overwrite/merge, schema evolution)
- templates/complete_etl_pipeline.py (production-ready template)

Validation:
- Follows SKILL_REFACTORING_PLAN.md standards
- Post-fix review: Clear pass
- Commit: da17ed4
