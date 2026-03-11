---
id: das-qdy8
status: closed
deps: [das-qee5]
links: [das-qee5, das-3wu8]
created: 2026-03-10T15:59:04Z
type: task
priority: 1
assignee: legout
parent: das-jg7i
tags: [skill-refactor, data-science, analysis, deduplication]
---
# Consolidate shared analysis references and delete duplicate copies

Remove the repeated analysis reference files that are copied across the current data-science skills.

## Acceptance Criteria

- duplicate shared analysis references are consolidated into one home
- redundant copies are deleted or retired from the migration path
- touched references keep enough depth to remain useful after deduplication


## Notes

**2026-03-10T22:28:15Z**

Implementation complete:
- Consolidated 126 duplicate reference files → 21 shared files in analyzing-data/references/
- Updated 6 SKILL.md files with correct relative paths
- Removed duplicate references/ directories from all 6 data-science skills
- Post-fix review: Clear pass
- Commit: d9c50d9
