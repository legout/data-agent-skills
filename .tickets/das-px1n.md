---
id: das-px1n
status: closed
deps: [das-g8hg]
links: [das-2rye, das-9jfk]
created: 2026-03-10T15:59:04Z
type: task
priority: 2
assignee: legout
parent: das-trf5
tags: [skill-refactor, data-engineering, storage-design, formats]
---
# Consolidate file-format references in designing-data-storage

Merge Parquet, Arrow, Avro, ORC, Zarr, and similar format guidance under the new storage-design skill.

## Acceptance Criteria

- format-selection guidance is consolidated under designing-data-storage
- duplicated or shallow format references are merged or removed
- long touched references include a TOC where needed


## Notes

**2026-03-11T14:05:05Z**

Implementation complete:
- Created skills/designing-data-storage/ with SKILL.md and 5 reference files
- Moved parquet.md, delta-lake.md, iceberg.md, hudi.md from legacy skills
- Added new format-selection-guide.md consolidating decision guidance
- Updated 16 skill files with cross-references to @designing-data-storage
- Deleted legacy data-engineering-storage-formats/ and data-engineering-storage-lakehouse/ directories
- Fixed 2 Major issues: broken skill refs, missing TOC; 1 Minor: missing dependsOn

Commit: 84e1731
Gate: Clear pass (post-fix review)
