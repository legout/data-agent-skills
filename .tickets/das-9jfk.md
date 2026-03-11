---
id: das-9jfk
status: closed
deps: [das-2rye]
links: [das-px1n, das-2rye]
created: 2026-03-10T15:59:04Z
type: task
priority: 2
assignee: legout
parent: das-trf5
tags: [skill-refactor, data-engineering, storage-design, delta-iceberg]
---
# Move Delta and Iceberg integration guidance under designing-data-storage

Finish the storage-design merge by relocating the split Delta/Iceberg integration content.

## Acceptance Criteria

- Delta and Iceberg integration guidance lives under designing-data-storage
- obsolete split integration references are removed or redirected
- touched content has clear boundaries versus accessing-cloud-storage


## Notes

**2026-03-11T13:32:21Z**

Implementation complete:

- Updated 4 files to change references from "designing-data-storage" to "@data-engineering-storage-lakehouse"
- Deleted 2 deprecated skill directories (delta-lake and iceberg integration skills)
- Fixed dangling references in data-engineering-storage-remote-access migration guide

Key files changed:
- skills/building-data-pipelines/SKILL.md
- skills/building-data-pipelines/references/crud-operations.md
- skills/building-data-pipelines/references/production-architecture.md
- skills/data-engineering-storage-remote-access/SKILL.md
- skills/data-engineering-storage-remote-access-integrations-delta-lake/SKILL.md (deleted)
- skills/data-engineering-storage-remote-access-integrations-iceberg/SKILL.md (deleted)

Validation: Passed - review-post-fix clear pass
Commit: bd2b407
