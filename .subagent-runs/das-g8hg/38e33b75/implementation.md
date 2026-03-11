# Implementation: das-g8hg - Merge Remote Access Skills

## Summary

**Status**: Already completed by child ticket implementations.

All 10 deprecated `data-engineering-storage-remote-access*` skills have been updated with clear deprecation notices pointing to the new consolidated `@accessing-cloud-storage` skill.

## Verification Results

### Skills Updated ✅

| # | Skill | Status | Redirects To |
|---|-------|--------|--------------|
| 1 | `data-engineering-storage-remote-access` | ✅ DEPRECATED | `@accessing-cloud-storage` |
| 2 | `data-engineering-storage-remote-access-integrations-delta-lake` | ✅ DEPRECATED | `@accessing-cloud-storage` + `@data-engineering-storage-lakehouse` |
| 3 | `data-engineering-storage-remote-access-integrations-duckdb` | ✅ DEPRECATED | `@accessing-cloud-storage` |
| 4 | `data-engineering-storage-remote-access-integrations-iceberg` | ✅ DEPRECATED | `@accessing-cloud-storage` + `@data-engineering-storage-lakehouse` |
| 5 | `data-engineering-storage-remote-access-integrations-pandas` | ✅ DEPRECATED | `@accessing-cloud-storage` |
| 6 | `data-engineering-storage-remote-access-integrations-polars` | ✅ DEPRECATED | `@accessing-cloud-storage` |
| 7 | `data-engineering-storage-remote-access-integrations-pyarrow` | ✅ DEPRECATED | `@accessing-cloud-storage` |
| 8 | `data-engineering-storage-remote-access-libraries-fsspec` | ✅ DEPRECATED | `@accessing-cloud-storage` |
| 9 | `data-engineering-storage-remote-access-libraries-obstore` | ✅ DEPRECATED | `@accessing-cloud-storage` |
| 10 | `data-engineering-storage-remote-access-libraries-pyarrow-fs` | ✅ DEPRECATED | `@accessing-cloud-storage` |

### Deprecation Pattern

Each deprecated skill follows this standard format:

```yaml
---
name: data-engineering-storage-remote-access-integrations-<name>
description: "[DEPRECATED] Use @accessing-cloud-storage instead. <original description>"
dependsOn: ["@accessing-cloud-storage"]
---

# ⚠️ DEPRECATED: <Title>

**This content has been consolidated into `@accessing-cloud-storage`.**

Please use **`@accessing-cloud-storage`** for <specific features>.

## Migration

| Old Reference | New Reference |
|---------------|---------------|
| `@data-engineering-storage-remote-access-integrations-<name>` | `@accessing-cloud-storage` (<section>) |

---

## Quick Start (New Skill)

<code example>

**See `@accessing-cloud-storage` for complete documentation.**
```

### Boundary Routing

As specified in the anchor context, there's clear boundary routing:

1. **General cloud storage access** (fsspec, pyarrow.fs, obstore, Polars, DuckDB, Pandas, PyArrow integrations) → `@accessing-cloud-storage`
2. **Delta Lake and Iceberg lakehouse table formats** → `@accessing-cloud-storage` + `@data-engineering-storage-lakehouse` (pending future `storage-design` skill)

## New Consolidated Skill

The new `@accessing-cloud-storage` skill (at `skills/accessing-cloud-storage/SKILL.md`) contains comprehensive guidance on:

- **Library guides**: fsspec, pyarrow.fs, obstore with full documentation
- **DataFrame integrations**: Polars, DuckDB, Pandas, PyArrow
- **Performance optimization**: Caching, concurrency, async patterns
- **Common patterns**: Incremental loading, partitioned writes, cross-cloud copy

## Conclusion

This ticket's implementation was completed by the child tickets:
- ✅ das-s0yk (auth) - authentication content
- ✅ das-ix8j (libraries) - fsspec, pyarrow.fs, obstore content  
- ✅ das-wxeh (integrations) - DataFrame integration content

All deprecated skills now have proper redirection notices with migration tables, ensuring users are directed to the correct consolidated skill.