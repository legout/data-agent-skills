---
id: das-2rye
status: closed
deps: [das-g8hg]
links: [das-px1n, das-9jfk]
created: 2026-03-10T15:59:04Z
type: task
priority: 2
assignee: legout
parent: das-trf5
tags: [skill-refactor, data-engineering, storage-design, lakehouse]
---
# Consolidate lakehouse design references in designing-data-storage

Group Delta Lake, Iceberg, Hudi, and related design tradeoffs under one storage-design workflow.

## Acceptance Criteria

- lakehouse decision guidance is consolidated under designing-data-storage
- cross-links between storage formats and table formats are explicit
- touched references use the new direct-link style


## Notes

**2026-03-11T13:15:32Z**

Implementation complete: Updated 4 skill files to use @accessing-cloud-storage reference
- skills/data-engineering-storage-lakehouse/SKILL.md - Related Skills and Dependencies sections
- skills/data-engineering-storage-lakehouse/delta-lake.md - cloud storage reference
- skills/data-engineering-storage-lakehouse/iceberg.md - cloud storage reference  
- skills/data-engineering-storage-formats/SKILL.md - Fixed deprecated reference in References section
Commit: bcde9ea
Tests: Clear pass (review-post-fix)
