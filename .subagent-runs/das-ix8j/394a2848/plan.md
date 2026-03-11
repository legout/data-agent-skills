# Implementation Plan

## Goal
Consolidate three remote-access library skills (fsspec, pyarrow.fs, obstore) and the main data-engineering-storage-remote-access skill into a single coherent skill called `accessing-cloud-storage`.

## Background
- **Source skills to merge**:
  - `data-engineering-storage-remote-access` (SKILL.md + performance.md + patterns.md)
  - `data-engineering-storage-remote-access-libraries-fsspec` (SKILL.md)
  - `data-engineering-storage-remote-access-libraries-pyarrow-fs` (SKILL.md)
  - `data-engineering-storage-remote-access-libraries-obstore` (SKILL.md)
- **Target location**: `skills/accessing-cloud-storage/`
- **Already analyzed**: Lines 20-22 and 163-165 in accessing-cloud-storage/SKILL.md need updates (from context)
- **Dependencies**: das-llsd completed; das-wxeh (framework integrations) follows this ticket

## Tasks

### Task 1: Create skill directory structure
- **Action**: Create `skills/accessing-cloud-storage/` directory
- **Output**: New directory ready for skill files
- **Acceptance**: Directory exists and is writable

### Task 2: Create main SKILL.md with consolidated library content
- **File**: `skills/accessing-cloud-storage/SKILL.md`
- **Changes**: 
  - Start with comparison table from data-engineering-storage-remote-access/SKILL.md
  - Include "When to Use Which?" decision guide
  - Inline the three library deep-dives (fsspec, pyarrow.fs, obstore) as sections instead of cross-references
  - Add Quick Start Example showing all three approaches
  - Include Authentication reference to external skill
  - List integration skills as related (polars, duckdb, pandas, pyarrow, delta-lake, iceberg)
- **Acceptance**: File contains complete library guidance with comparison table, decision matrix, and detailed sections for each library

### Task 3: Create performance.md section
- **File**: `skills/accessing-cloud-storage/performance.md`
- **Changes**: Copy content from data-engineering-storage-remote-access/performance.md
  - Caching strategies (SimpleCache, BlockCache)
  - Concurrent operations (fsspec async, obstore async, ThreadPool)
  - Parquet-specific optimizations (column pruning, row group selection, dataset scanning)
  - Key takeaways summary
- **Acceptance**: Performance optimization guidance is complete and accurate

### Task 4: Create patterns.md section
- **File**: `skills/accessing-cloud-storage/patterns.md`
- **Changes**: Copy content from data-engineering-storage-remote-access/patterns.md
  - Incremental loading with checkpoint pattern
  - Writing partitioned datasets (Hive partitioning)
  - Cross-cloud copy patterns (S3 ↔ GCS ↔ Azure)
  - Performance tips and error handling
- **Acceptance**: Common patterns documented with code examples

### Task 5: Update frontmatter in main SKILL.md
- **File**: `skills/accessing-cloud-storage/SKILL.md`
- **Changes**: 
  - Set `name: accessing-cloud-storage`
  - Set `description: "Access cloud storage (S3, GCS, Azure) in Python using fsspec, pyarrow.fs, or obstore. Includes performance optimization, patterns for incremental loading, partitioned writes, and cross-cloud copy."`
  - Set `dependsOn: ["@data-engineering-core", "@data-engineering-storage-authentication", "@data-engineering-storage-formats"]`
- **Acceptance**: Frontmatter is valid YAML with correct skill metadata

### Task 6: Verify content completeness
- **Action**: Review all created files for:
  - All code examples are present and correct
  - Comparison table is complete
  - Decision guidance is clear
  - No broken cross-references (update to new skill paths)
  - Consistent formatting and headers
- **Acceptance**: All content from source skills is preserved and well-organized

## Files to Modify
- `skills/accessing-cloud-storage/SKILL.md` - Main skill file with consolidated library content
- `skills/accessing-cloud-storage/performance.md` - Performance optimization guide
- `skills/accessing-cloud-storage/patterns.md` - Common usage patterns

## New Files
- `skills/accessing-cloud-storage/` - Skill directory
- `skills/accessing-cloud-storage/SKILL.md` - Main skill file (~350-400 lines)
- `skills/accessing-cloud-storage/performance.md` - Performance guide (~150 lines)
- `skills/accessing-cloud-storage/patterns.md` - Patterns guide (~150 lines)

## Dependencies
- Task 2 depends on Task 1 (directory must exist)
- Task 3 and Task 4 can run in parallel after Task 1
- Task 5 must run after Task 2
- Task 6 runs last to verify all content

## Risks
1. **Content duplication**: Ensure library sections are properly consolidated without duplication
2. **Cross-reference updates**: Check that references to integration skills use correct paths
3. **Code example accuracy**: Verify all code examples from source files are preserved
4. **Skill naming**: Confirm `accessing-cloud-storage` is the correct target name per project conventions

## Estimated Effort
Medium complexity - merging ~450 lines of documentation across 6 source files into 3 cohesive output files.
