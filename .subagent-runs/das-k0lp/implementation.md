# Implementation: das-k0lp

## Summary
Refactored `data-engineering-catalogs` skill into new `managing-data-catalogs` skill using direct-link style format.

## Changes Made

### New Skill Created: managing-data-catalogs

**Main File:** `skills/managing-data-catalogs/SKILL.md`
- Direct-link style with frontmatter
- "When to use / When not to use" sections
- Quick comparison tables for catalog types
- Links to 5 detailed guides
- Cross-skill references in "See also" section

**Detailed Guides Created:**
1. `hive-metastore.md` - Docker deployment, PyIceberg integration, tuning
2. `aws-glue-catalog.md` - Glue setup, IAM permissions, crawlers, cross-service access
3. `rest-catalog.md` - Tabular SaaS, Nessie, Git-like branching, multi-engine patterns
4. `duckdb-catalog.md` - ATTACH patterns, unified views, limitations
5. `open-source-catalogs.md` - Amundsen vs DataHub vs OpenMetadata comparison

**Reference File Copied:**
- `duckdb-multisource.md` - Preserved from original skill

### Reference Updates

Updated `@data-engineering-catalogs` → `@managing-data-catalogs` in:
- `skills/data-engineering/SKILL.md` (2 occurrences)
- `skills/designing-data-storage/SKILL.md` (1 occurrence)
- `skills/data-engineering-best-practices/best-practices-detailed.md` (1 occurrence)
- `skills/flowerpower/SKILL.md` (1 occurrence)

### Eval Files Verified

Both eval files already had correct `skill_name: "managing-data-catalogs"`:
- `eval/managing-data-catalogs.json`
- `eval/trigger-eval/managing-data-catalogs.json`

## Structure

```
skills/managing-data-catalogs/
├── SKILL.md                    # Main entry point (direct-link style)
├── hive-metastore.md           # Detailed guide
├── aws-glue-catalog.md         # Detailed guide
├── rest-catalog.md             # Detailed guide
├── duckdb-catalog.md           # Detailed guide
├── open-source-catalogs.md     # Detailed guide
└── duckdb-multisource.md       # Reference file
```

## Dependencies

New skill depends on:
- `@designing-data-storage` - Table format details
- `@accessing-cloud-storage` - Cloud storage access
- `@data-engineering-storage-authentication` - Auth patterns

## Backward Compatibility

The original `data-engineering-catalogs` skill directory remains intact but should be considered deprecated. All references in the codebase have been updated to point to the new skill name.
