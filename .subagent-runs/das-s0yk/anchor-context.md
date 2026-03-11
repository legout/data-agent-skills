# Anchor Context for das-s0yk

## Ticket Summary
- **ID**: das-s0yk
- **Title**: Merge provider auth guidance into accessing-cloud-storage
- **Scope**: Merge AWS, GCP, and Azure auth guidance from `data-engineering-storage-authentication` into the new `accessing-cloud-storage` skill
- **Why**: Consolidate fragmented storage-access skill tree into coherent workflow-centered skill (per SKILL_REFACTORING_PLAN.md)

## Complexity Assessment
- **Level**: Medium (content consolidation, no new algorithms)
- **LOC estimate**: ~200 lines (SKILL.md updates + file moves)
- **Type**: Content migration and reference updates

## Research Gaps
- **None** - existing knowledge sufficient
- Pattern already established by das-llsd (building-data-pipelines)

## External Libraries
- None required - this is documentation consolidation

## Testing Requirements
- Run `python3 tools/skill_lint.py` to verify no broken references
- Verify all local references resolve correctly
- Check that SKILL.md structure follows refactoring plan standards

## Recommended Path
**Path A (Minimal)** - This is a content consolidation task:
1. Create `accessing-cloud-storage` skill directory structure
2. Move auth files from `data-engineering-storage-authentication/` to `accessing-cloud-storage/references/`
3. Create/update SKILL.md with auth as primary section
4. Update references to use direct file paths (no hybrid @skill/path notation)
5. Delete the source skill folder after verification
6. Run linter to validate

## File Hints

### Source files to merge:
- `skills/data-engineering-storage-authentication/SKILL.md` (main content)
- `skills/data-engineering-storage-authentication/aws.md` (AWS auth details)
- `skills/data-engineering-storage-authentication/gcp.md` (GCP auth details)
- `skills/data-engineering-storage-authentication/azure.md` (Azure auth details)
- `skills/data-engineering-storage-authentication/patterns.md` (auth patterns)
- `skills/data-engineering-storage-authentication/testing.md` (testing strategies)

### Target structure:
```
skills/accessing-cloud-storage/
├── SKILL.md (new - includes auth section)
├── references/
│   ├── aws-auth.md
│   ├── gcp-auth.md
│   ├── azure-auth.md
│   ├── auth-patterns.md
│   └── auth-testing.md
└── (future: library references from das-ix8j)
```

### Reference pattern to follow:
- `skills/building-data-pipelines/` - created by das-llsd as the exemplar

## Acceptance Criteria (from ticket)
1. Provider auth guidance is consolidated under accessing-cloud-storage
2. References clearly separate auth setup from storage-design choices
3. Direct links replace indirect or hybrid routing where touched

## Notes
- This ticket is part of a larger parent (das-g8hg) that also includes:
  - das-ix8j: library references (fsspec, pyarrow.fs, obstore)
  - das-wxeh: framework integrations (Polars, DuckDB, Pandas, PyArrow)
- This ticket (das-s0yk) focuses ONLY on auth guidance
- Do NOT include library/integration content - those are separate tickets
