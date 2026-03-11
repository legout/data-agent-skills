Done! I've created `/Users/volker/coding/libs/data-agent-skills/anchor-context.md` with comprehensive implementation context for ticket das-llsd.

## Summary

**Ticket**: Create `building-data-pipelines` skill by merging `data-engineering-core` and `data-engineering-best-practices`

**Key findings**:
- Both source skills have substantial content (~600+ lines each with detailed references)
- Pre-existing eval coverage exists: 5 task evaluations + 15 trigger evaluations already defined
- The new skill will combine: Polars/DuckDB/PyArrow tools + production patterns (medallion architecture, partitioning, schema evolution, incremental loading)
- Following the refactoring plan, this is the first skill to establish the pattern for the new 14-skill architecture

**Recommended Path**: A (Minimal) - start by creating the new skill folder structure and merging SKILL.md files, then consolidate references into workflow-focused files.