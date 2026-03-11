# Implementation Plan

## Goal
Refactor the `data-engineering-catalogs` skill into a new `managing-data-catalogs` skill using the direct-link style format, and update all existing references.

## Tasks

### 1. Create New Skill Directory and Main File
**Task**: Create `skills/managing-data-catalogs/` directory and `SKILL.md`
- **File**: `skills/managing-data-catalogs/SKILL.md`
- **Changes**: Create new skill file with direct-link style:
  - Add frontmatter with `name: managing-data-catalogs` and updated description
  - Include "When to use this skill" / "When not to use this skill" sections
  - Add quick comparison tables (catalog types, tool comparisons)
  - Use direct links to sub-topics instead of embedded detailed guides
  - Reference `designing-data-storage` for Iceberg/Delta table format details
  - Reference `accessing-cloud-storage` for cloud storage auth patterns
- **Content structure** (based on direct-link style):
  - Quick catalog type comparison table
  - When to use which catalog (Hive, Glue, Tabular, DuckDB-as-catalog)
  - Core patterns section
  - Links to detailed guides within the skill
  - "See also" section with cross-skill references
- **Acceptance**: File exists with proper frontmatter and direct-link structure

### 2. Create Detailed Guide: Hive Metastore
**Task**: Create `skills/managing-data-catalogs/hive-metastore.md`
- **File**: `skills/managing-data-catalogs/hive-metastore.md`
- **Changes**: Extract and adapt Hive Metastore content from old skill
  - Deployment options (Docker, self-hosted)
  - Configuration parameters
  - PyIceberg integration code examples
  - Pros/cons summary
- **Acceptance**: Complete Hive Metastore guide with setup and usage examples

### 3. Create Detailed Guide: AWS Glue Catalog
**Task**: Create `skills/managing-data-catalogs/aws-glue-catalog.md`
- **File**: `skills/managing-data-catalogs/aws-glue-catalog.md`
- **Changes**: Extract and adapt AWS Glue content
  - GlueCatalog setup with PyIceberg
  - Crawler configuration
  - IAM permissions
  - Cross-service access patterns (Athena, EMR)
  - Unity Catalog federation notes
- **Acceptance**: Complete AWS Glue guide with authentication and integration examples

### 4. Create Detailed Guide: REST Catalog and Tabular
**Task**: Create `skills/managing-data-catalogs/rest-catalog.md`
- **File**: `skills/managing-data-catalogs/rest-catalog.md`
- **Changes**: Extract REST catalog and Tabular content
  - Tabular catalog setup with PyIceberg
  - Git-like branching operations
  - Authentication patterns
  - Nessie catalog patterns (if applicable)
  - Multi-engine access examples
- **Acceptance**: Complete REST catalog guide covering Tabular and Nessie patterns

### 5. Create Detailed Guide: DuckDB as Multi-Source Catalog
**Task**: Create `skills/managing-data-catalogs/duckdb-catalog.md`
- **File**: `skills/managing-data-catalogs/duckdb-catalog.md`
- **Changes**: Extract DuckDB-as-catalog pattern
  - ATTACH patterns for multiple sources
  - PostgreSQL, Delta, Iceberg attachment examples
  - Unified multi-source views
  - Limitations and when NOT to use
  - Small team / PoC use cases
- **Acceptance**: Complete DuckDB catalog guide with practical examples

### 6. Create Detailed Guide: Open Source Catalogs Comparison
**Task**: Create `skills/managing-data-catalogs/open-source-catalogs.md`
- **File**: `skills/managing-data-catalogs/open-source-catalogs.md`
- **Changes**: Extract Amundsen, DataHub, OpenMetadata comparison
  - Feature comparison table
  - Deployment complexity notes
  - Selection recommendations by use case
  - Current status (e.g., Amundsen development status)
- **Acceptance**: Complete comparison guide with clear recommendations

### 7. Update Reference in data-engineering Main Skill
**Task**: Update catalog reference in main data-engineering skill
- **File**: `skills/data-engineering/SKILL.md`
- **Changes**:
  - Line 23: Change `@data-engineering-catalogs` to `@managing-data-catalogs`
  - Line 49: Change `@data-engineering-catalogs` to `@managing-data-catalogs`
- **Acceptance**: Both references updated to new skill name

### 8. Update Reference in designing-data-storage
**Task**: Update catalog reference in storage design skill
- **File**: `skills/designing-data-storage/SKILL.md`
- **Changes**:
  - Line 305: Change `@data-engineering-catalogs` to `@managing-data-catalogs`
- **Acceptance**: Reference updated to new skill name

### 9. Update Reference in data-engineering-best-practices
**Task**: Update catalog reference in best practices
- **File**: `skills/data-engineering-best-practices/best-practices-detailed.md`
- **Changes**:
  - Line 895: Change `@data-engineering-catalogs` to `@managing-data-catalogs`
- **Acceptance**: Reference updated to new skill name

### 10. Update Reference in flowerpower
**Task**: Update catalog reference in flowerpower skill
- **File**: `skills/flowerpower/SKILL.md`
- **Changes**:
  - Line 351: Change `@data-engineering-catalogs` to `@managing-data-catalogs`
- **Acceptance**: Reference updated to new skill name

### 11. Verify Eval Files Reference Correct Skill
**Task**: Check eval files use correct skill name
- **File**: `eval/managing-data-catalogs.json`
- **File**: `eval/trigger-eval/managing-data-catalogs.json`
- **Changes**: Ensure both files reference `managing-data-catalogs` as skill_name
- **Acceptance**: Both eval files have correct skill_name field

## Files to Modify

| File | Changes |
|------|---------|
| `skills/managing-data-catalogs/SKILL.md` | Create new main skill file |
| `skills/managing-data-catalogs/hive-metastore.md` | Create detailed guide |
| `skills/managing-data-catalogs/aws-glue-catalog.md` | Create detailed guide |
| `skills/managing-data-catalogs/rest-catalog.md` | Create detailed guide |
| `skills/managing-data-catalogs/duckdb-catalog.md` | Create detailed guide |
| `skills/managing-data-catalogs/open-source-catalogs.md` | Create detailed guide |
| `skills/data-engineering/SKILL.md` | Update 2 references |
| `skills/designing-data-storage/SKILL.md` | Update 1 reference |
| `skills/data-engineering-best-practices/best-practices-detailed.md` | Update 1 reference |
| `skills/flowerpower/SKILL.md` | Update 1 reference |
| `eval/managing-data-catalogs.json` | Verify skill_name field |
| `eval/trigger-eval/managing-data-catalogs.json` | Verify skill_name field |

## New Files

- `skills/managing-data-catalogs/SKILL.md` - Main skill entry point
- `skills/managing-data-catalogs/hive-metastore.md` - Hive Metastore detailed guide
- `skills/managing-data-catalogs/aws-glue-catalog.md` - AWS Glue detailed guide
- `skills/managing-data-catalogs/rest-catalog.md` - REST/Tabular catalog guide
- `skills/managing-data-catalogs/duckdb-catalog.md` - DuckDB multi-source pattern
- `skills/managing-data-catalogs/open-source-catalogs.md` - Tool comparison guide

## Dependencies

```
Task 1 (Main SKILL.md)
    ├── Task 2 (Hive Metastore) ──┐
    ├── Task 3 (AWS Glue) ────────┼── Can be done in parallel
    ├── Task 4 (REST Catalog) ────┤    after Task 1
    ├── Task 5 (DuckDB) ──────────┤
    └── Task 6 (Open Source) ─────┘

Tasks 7-11 (Reference Updates) ─── Can be done in parallel
                                      after Task 1
```

## Risks

1. **Content Loss**: Ensure all content from original `data-engineering-catalogs` skill is preserved when splitting into separate files
2. **Broken Links**: After updating references, verify no other files reference `@data-engineering-catalogs` (search again before finalizing)
3. **Skill Dependencies**: The new skill has `dependsOn` that may need updating - check if dependencies still make sense
4. **Backward Compatibility**: Consider whether to keep old skill as a redirect/deprecation notice or remove entirely
5. **Eval Alignment**: Ensure eval prompts still make sense with the refactored structure

## Reference: @data-engineering-catalogs References Found

| Location | Line | Current Reference |
|----------|------|-------------------|
| `skills/data-engineering/SKILL.md` | 23 | `@data-engineering-catalogs` |
| `skills/data-engineering/SKILL.md` | 49 | `@data-engineering-catalogs` |
| `skills/designing-data-storage/SKILL.md` | 305 | `@data-engineering-catalogs` |
| `skills/data-engineering-best-practices/best-practices-detailed.md` | 895 | `@data-engineering-catalogs` |
| `skills/flowerpower/SKILL.md` | 351 | `@data-engineering-catalogs` |
