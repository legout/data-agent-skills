# Implementation Plan

## Goal
Create a new "orchestrating-data-pipelines" skill in project scope by consolidating content from the existing `~/.pi/agent/skills/data-engineering-orchestration/` skill.

## Tasks

### 1. Create Skill Directory Structure
- **File**: `.pi/agent/skills/orchestrating-data-pipelines/` (new directory)
- **Changes**: Create the target directory for the new skill
- **Acceptance**: Directory exists and is ready for skill files

### 2. Create Main SKILL.md
- **File**: `.pi/agent/skills/orchestrating-data-pipelines/SKILL.md`
- **Changes**: Consolidate from `~/.pi/agent/skills/data-engineering-orchestration/SKILL.md`
- **Key content to preserve**:
  - Frontmatter with new name `orchestrating-data-pipelines` and updated description
  - Quick comparison table (Prefect, Dagster, dbt, FlowerPower)
  - "When to Use Which?" guidance section
  - Skill dependencies section
  - FlowerPower alternative coverage (keep lightweight alternative section)
  - Common patterns section (Retry Pattern, Idempotency, State Management, Dependency Management)
  - Production recommendations checklist
- **Acceptance**: Main skill file contains overview, comparison, and navigation to detailed guides

### 3. Create Prefect Guide
- **File**: `.pi/agent/skills/orchestrating-data-pipelines/prefect.md`
- **Changes**: Copy from `~/.pi/agent/skills/data-engineering-orchestration/prefect.md`
- **Key content to preserve**:
  - Installation instructions
  - Core concepts (Flow, Task, Deployment, Agent)
  - Basic flow example with Polars/DuckDB
  - Advanced features: retries, caching, parameters, logging
  - Scheduling & Deployments (Prefect 3 syntax)
  - Prefect Cloud vs Server comparison
  - Best practices checklist
  - Production patterns (State Persistence, Notifications, Concurrency Limits)
- **Acceptance**: Complete Prefect guide with working code examples

### 4. Create Dagster Guide
- **File**: `.pi/agent/skills/orchestrating-data-pipelines/dagster.md`
- **Changes**: Copy from `~/.pi/agent/skills/data-engineering-orchestration/dagster.md`
- **Key content to preserve**:
  - Installation instructions
  - Core concepts (Asset, Op, Job, Sensor, Schedule, Resource)
  - Asset-based pipeline example (new style with `@asset`)
  - Ops & Jobs (legacy style)
  - Resources for shared connections
  - Schemas and Types coverage
  - Sensors & Schedules
  - Partitioned Assets
  - Dagster + dbt Integration section
  - Best practices checklist
- **Acceptance**: Complete Dagster guide with working code examples

### 5. Create dbt Guide
- **File**: `.pi/agent/skills/orchestrating-data-pipelines/dbt.md`
- **Changes**: Copy from `~/.pi/agent/skills/data-engineering-orchestration/dbt.md`
- **Key content to preserve**:
  - Why dbt? section with value proposition
  - Installation with adapter options
  - Complete project structure overview
  - profiles.yml configurations (DuckDB, Postgres examples)
  - Models with Jinja macros (ref, source, config, var)
  - Sources definition in YAML
  - Tests (schema tests, singular tests, generic tests)
  - Snapshots (SCD Type 2)
  - Seeds for CSV loading
  - Commands reference (debug, deps, compile, run, test, build, etc.)
  - Configuration (dbt_project.yml)
  - Materializations (view, table, incremental, ephemeral)
  - Python API example
  - Best practices checklist
- **Acceptance**: Complete dbt guide with working code examples

### 6. Create Integrations Directory and Cloud Storage Guide
- **File**: `.pi/agent/skills/orchestrating-data-pipelines/integrations/cloud-storage.md`
- **Changes**: Copy from `~/.pi/agent/skills/data-engineering-orchestration/integrations/cloud-storage.md`
- **Key content to preserve**:
  - dbt-duckdb with S3/GCS configuration
  - dbt-postgres with S3 (aws_s3 extension)
  - External tables patterns
  - Best practices for credentials
  - Credential management for production
- **Acceptance**: Cloud storage integration guide preserved

### 7. Update Cross-References
- **Files**: All `.md` files in the new skill
- **Changes**: Update internal skill references from `@data-engineering-orchestration/...` to `@orchestrating-data-pipelines/...`
- **Specific updates needed**:
  - SKILL.md: Change references to `@data-engineering-orchestration/prefect.md` → `@orchestrating-data-pipelines/prefect.md`
  - SKILL.md: Change references to `@data-engineering-orchestration/dagster.md` → `@orchestrating-data-pipelines/dagster.md`
  - SKILL.md: Change references to `@data-engineering-orchestration/dbt.md` → `@orchestrating-data-pipelines/dbt.md`
  - SKILL.md: Change references to `@data-engineering-orchestration/integrations/...` → `@orchestrating-data-pipelines/integrations/...`
  - dagster.md: Update dbt.md reference
  - dbt.md: Update cloud-storage.md reference
  - cloud-storage.md: Update dbt.md reference
- **Acceptance**: All internal cross-references point to the new skill location

### 8. Verify Eval Alignment
- **File**: `eval/orchestrating-data-pipelines.json` (already exists)
- **Changes**: Verify existing eval covers the consolidated content
- **Evals to verify**:
  - eval-001: Prefect Workflow - covered by prefect.md
  - eval-002: Dagster Pipeline - covered by dagster.md
  - eval-003: dbt Transformations - covered by dbt.md
  - eval-004: Orchestrator Selection - covered by SKILL.md comparison section
  - eval-005: Production Deployment - covered across all guides
- **Acceptance**: All 5 eval tasks have corresponding content in the new skill

### 9. Content Verification Checklist
- **Task**: Systematically verify all content transferred (per lesson das-3wu8)
- **Checklist**:
  - [ ] All code examples copied correctly
  - [ ] All YAML configurations preserved
  - [ ] All tables formatted correctly
  - [ ] All bullet lists preserved
  - [ ] All hyperlinks preserved
  - [ ] FlowerPower section kept in SKILL.md
  - [ ] Quick comparison table preserved
  - [ ] Common patterns section preserved
  - [ ] Production recommendations preserved
- **Acceptance**: All source content is accounted for in destination

## Files to Modify
- `eval/orchestrating-data-pipelines.json` - verify existing eval coverage (read-only verification)

## New Files
1. `.pi/agent/skills/orchestrating-data-pipelines/SKILL.md` - Main skill overview
2. `.pi/agent/skills/orchestrating-data-pipelines/prefect.md` - Prefect orchestration guide
3. `.pi/agent/skills/orchestrating-data-pipelines/dagster.md` - Dagster orchestration guide
4. `.pi/agent/skills/orchestrating-data-pipelines/dbt.md` - dbt transformation guide
5. `.pi/agent/skills/orchestrating-data-pipelines/integrations/cloud-storage.md` - Cloud storage integration

## Dependencies
- Task 1 (create directory) must complete before Tasks 2-6
- Tasks 2-6 (create content files) are independent and can be done in any order
- Task 7 (update cross-references) depends on Tasks 2-6
- Task 8 (verify eval) depends on Tasks 2-6
- Task 9 (verification) depends on all previous tasks

## Risks
1. **Cross-reference errors**: Internal links must be updated to new skill name. Missing any will break skill navigation.
2. **Skill dependency updates**: Other skills may reference `@data-engineering-orchestration`. These should continue working but verify no broken links.
3. **Content loss**: Large skill with multiple files - systematic verification needed per lesson das-3wu8.
4. **FlowerPower confusion**: FlowerPower content exists both in data-engineering-orchestration SKILL.md and as standalone `@flowerpower` skill. Keep the reference section but don't duplicate full FlowerPower guide.
5. **Source skill retirement**: Decide whether to keep `data-engineering-orchestration` as redirect or retire it (out of scope for this plan).

## Notes
- Skill name follows action-oriented pattern (verb-based "orchestrating") per lesson das-3jql
- Source skill is external (`~/.pi/agent/skills/`) so cannot be linted from repo (per lesson das-s0yk)
- Eval already exists at `eval/orchestrating-data-pipelines.json` with 5 task evaluations
- Target location is project scope: `.pi/agent/skills/orchestrating-data-pipelines/`
