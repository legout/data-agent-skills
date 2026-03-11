# Implementation: das-9jfk

## Summary
Completed the storage-design merge by relocating Delta/Iceberg integration content and cleaning up obsolete references.

## Changes Made

### Updated References (4 files)

1. **skills/building-data-pipelines/references/crud-operations.md**
   - Changed: `` `designing-data-storage` `` → `` `@data-engineering-storage-lakehouse` ``
   - Location: References section

2. **skills/building-data-pipelines/references/production-architecture.md**
   - Changed: `` `designing-data-storage` `` → `` `@data-engineering-storage-lakehouse` ``
   - Location: References section

3. **skills/building-data-pipelines/SKILL.md**
   - Changed: `` `designing-data-storage` `` → `` `@data-engineering-storage-lakehouse` ``
   - Locations:
     - "When not to use this skill" section
     - "Related skills" section

### Deleted Deprecated Skills (2 directories)

1. **skills/data-engineering-storage-remote-access-integrations-delta-lake/**
   - Content was already marked as DEPRECATED
   - Migration path documented: use `@accessing-cloud-storage` + `@data-engineering-storage-lakehouse`

2. **skills/data-engineering-storage-remote-access-integrations-iceberg/**
   - Content was already marked as DEPRECATED
   - Migration path documented: use `@accessing-cloud-storage` + `@data-engineering-storage-lakehouse`

## Result

The Delta and Iceberg integration guidance now properly lives under the `data-engineering-storage-lakehouse` skill:
- `skills/data-engineering-storage-lakehouse/delta-lake.md` - Comprehensive Delta Lake guide
- `skills/data-engineering-storage-lakehouse/iceberg.md` - Comprehensive Iceberg guide

Clear boundaries are maintained:
- `@data-engineering-storage-lakehouse` — Lakehouse table format patterns, comparisons, and format-specific operations
- `@accessing-cloud-storage` — Cloud storage authentication and I/O patterns for all formats

## Acceptance Criteria Verification

- [x] Delta and Iceberg integration guidance lives under designing-data-storage (represented by `@data-engineering-storage-lakehouse`)
- [x] Obsolete split integration references are removed (2 deprecated skill directories deleted)
- [x] Touched content has clear boundaries versus accessing-cloud-storage (verified: lakehouse skill focuses on table format patterns, accessing-cloud-storage focuses on storage access)
