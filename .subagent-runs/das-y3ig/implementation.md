# Implementation: das-y3ig

## Summary
Convert the `data-engineering` hub skill to non-triggerable documentation. This skill serves as an index/documentation hub and should not be triggered directly by user queries.

## Changes Made

### 1. Frontmatter Description Update
Changed from a comprehensive, triggerable description to a clear docs-only indicator:

**Before:**
```yaml
description: "Comprehensive data engineering skill suite covering core libraries (Polars, DuckDB, PyArrow), lakehouse formats, cloud storage, orchestration, streaming, quality, observability, and AI/ML pipelines."
```

**After:**
```yaml
description: "[DOCS ONLY - DO NOT TRIGGER] Index hub for data engineering skills. Use specific workflow skills instead: @building-data-pipelines for ETL workflows, @designing-data-storage for storage decisions, @accessing-cloud-storage for cloud access."
```

### 2. Deprecation Notice at Top of Content
Added a clear deprecation/docs-only banner immediately after the frontmatter:

```markdown
> **⚠️ DOCS ONLY - DO NOT USE DIRECTLY**
> 
> This is a documentation index hub. It will not trigger automatically.
> 
> **Use these specific skills instead:**
> - `@building-data-pipelines` - ETL workflows and data pipelines
> - `@designing-data-storage` - File formats and lakehouse decisions  
> - `@accessing-cloud-storage` - Cloud storage access patterns
> - `@data-engineering-core` - Core libraries (Polars, DuckDB, PyArrow)
> - `@assuring-data-pipelines` - Data quality and observability
> - `@managing-data-catalogs` - Data catalog systems
> - `@data-engineering-orchestration` - Workflow orchestration
> - `@data-engineering-streaming` - Real-time streaming pipelines
> - `@data-engineering-ai-ml` - AI/ML data pipelines
> - `@data-engineering-best-practices` - Architecture patterns
```

### 3. Updated Content References
Updated the skill map and quick reference sections to reference `@building-data-pipelines` as the primary workflow skill.

## File Modified
- `skills/data-engineering/SKILL.md`

## Rationale
The data-engineering skill was designed as a hub/index that organizes other skills. However, its comprehensive description caused it to trigger on general data engineering queries, creating a poor user experience since it only provides links to other skills rather than actionable guidance.

By making it explicitly non-triggerable:
1. Users get directed to specific, actionable skills immediately
2. The hub remains useful as documentation for those who navigate to it directly
3. No confusion about which skill to use for what purpose
