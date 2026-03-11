# Anchor Context: das-px1n

## Ticket Summary

Consolidate file-format references (Parquet, Arrow, Avro, ORC, Zarr, Lance) under the storage-design skill. Remove duplicated or shallow format references and add TOCs to long touched references.

## Complexity Assessment

**Medium complexity** - This is a content consolidation task that requires:
1. Creating or updating a storage-design skill with format guidance
2. Updating cross-references across multiple skills
3. Adding TOCs to long reference files
4. Potentially deprecating the existing `data-engineering-storage-formats` skill

**LOC estimate**: 100-200 lines of content updates + reference migrations

## Research Gaps

**None** - Existing knowledge is sufficient. The current `data-engineering-storage-formats/SKILL.md` is comprehensive (500+ lines) and covers all required formats.

## External Libraries

No new external libraries needed. This is a documentation/skill restructuring task.

## Testing Requirements

1. **Lint validation**: All updated SKILL.md files must pass `python3 tools/skill_lint.py --strict`
2. **Reference integrity**: No broken references to deprecated skills
3. **TOC presence**: Long reference files (>100 lines) should have TOCs

## Recommended Path

**Path B (Standard)** - Content-rich consolidation requiring:
1. Planning the consolidation structure
2. Implementing format reference updates
3. Parallel review + test validation
4. Fix pass if needed
5. Final close

## Key Files

### Primary targets
- `skills/data-engineering-storage-formats/SKILL.md` - Main format guidance (500+ lines, already comprehensive)
- `skills/data-engineering-storage-lakehouse/SKILL.md` - Lakehouse formats (Delta, Iceberg, Hudi)

### Files with format references to update
- `skills/building-data-pipelines/SKILL.md` - May reference formats
- `skills/accessing-cloud-storage/SKILL.md` - May reference formats
- Any other skills with `@data-engineering-storage-formats` references

### Decision point
Based on related tickets (das-2rye, das-9jfk), the pattern has been:
- Keep existing skills and update references to use canonical ones
- Use `@data-engineering-storage-lakehouse` for lakehouse content
- Use `@accessing-cloud-storage` for cloud storage access

The SKILL_REFACTORING_PLAN.md Section 5 calls for creating `designing-data-storage` by merging formats + lakehouse, but related tickets have not created this skill yet.

**Recommendation**: Check if parent ticket das-trf5 requires creating `designing-data-storage` or if this ticket should focus on format reference consolidation within existing skills.

## Implementation Approach

1. **Survey format references**: Find all skills referencing format content
2. **Consolidate**: Update references to point to canonical location
3. **Remove duplicates**: If any shallow/duplicate format content exists, merge or remove
4. **Add TOCs**: For any long reference files touched, ensure TOC is present
5. **Deprecation stubs**: If consolidating to a new skill, create deprecation stubs for old skills

## Acceptance Criteria Mapping

| Criteria | Approach |
|----------|----------|
| Format-selection guidance consolidated under designing-data-storage | Create/update canonical storage skill with format guidance |
| Duplicated/shallow format references merged or removed | Survey and merge duplicate content |
| Long touched references include TOC | Add TOCs to any long files modified |
