# Implementation: building-data-pipelines Skill

## Summary

Created new skill `building-data-pipelines` by merging content from `data-engineering-core` and `data-engineering-best-practices` following SKILL_REFACTORING_PLAN.md standards.

## Directory Structure

```
skills/building-data-pipelines/
├── SKILL.md
├── references/
│   ├── pipeline-patterns.md
│   ├── production-architecture.md
│   └── crud-operations.md
└── templates/
    └── complete_etl_pipeline.py
```

## Standards Compliance

### 1. Action-Oriented Naming ✅
- Skill name: `building-data-pipelines` (verb-noun format)
- Description states what the skill does and when to use it

### 2. When-To-Use / When-Not-To-Use Sections ✅
- Clear "When to use this skill" section with bullet points
- Explicit "When not to use this skill" section routing to other skills
- Related skills listed with purpose for each

### 3. Direct File References ✅
- All references use direct file paths: `references/pipeline-patterns.md`
- No hybrid `@skill/path` notation used
- Related skills referenced by simple name without path hybrid

### 4. Tool Selection Table ✅
- Quick tool selection table in SKILL.md
- Covers Polars, DuckDB, PyArrow for different tasks
- Includes decision rule for choosing tools

### 5. Consolidated References ✅
Content organized into workflow-focused files:

| File | Content Source |
|------|----------------|
| `pipeline-patterns.md` | ETL patterns, incremental loading (from data-engineering-core patterns/ + best-practices incremental section) |
| `production-architecture.md` | Medallion architecture, partitioning, lifecycle, cost optimization (from best-practices detailed) |
| `crud-operations.md` | Append/overwrite/merge across tools, schema evolution (from best-practices CRUD section) |

## Content Mapping

### From data-engineering-core:
- Core library patterns (Polars lazy, DuckDB SQL, PyArrow interchange)
- ETL pipeline structure and patterns
- Incremental loading with watermarks
- Resilience patterns (retry, circuit breaker, validation)
- Testing patterns
- Complete ETL pipeline template

### From data-engineering-best-practices:
- Medallion architecture (Bronze/Silver/Gold)
- Dataset lifecycle management
- Partitioning strategies
- File sizing guidelines (256MB-1GB target)
- CRUD operations matrix
- Schema evolution policies
- Cost optimization strategies
- Decision checklist and anti-patterns

## Key Features

1. **Progressive Disclosure**: SKILL.md provides quick reference, detailed content in references/
2. **Self-Contained**: All references within skill directory (no external dependencies)
3. **Workflow-Focused**: References organized by use case rather than tool
4. **Production-Ready**: Includes templates, validation patterns, and operational guidance
5. **Clear Boundaries**: Explicit guidance on when to use related skills

## Migration Notes

This skill consolidates and replaces:
- `data-engineering-core` → content merged into building-data-pipelines
- `data-engineering-best-practices` → content merged into building-data-pipelines

Both original skills can be deprecated once migration is complete.
