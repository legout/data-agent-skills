## Review

- **What's correct**
  - Consolidation to `skills/designing-data-storage/` is broadly implemented: new combined skill exists and old skill references were updated across touched files.
  - Old canonical references were removed from the skill tree (`@data-engineering-storage-formats`, `@data-engineering-storage-lakehouse`), and replaced with `@designing-data-storage` in the modified files.
  - TOCs were added to moved long-form references (`delta-lake.md`, `iceberg.md`, `hudi.md`), and `parquet.md` retains a TOC.

- **Issue [Major]**: Broken related-skill references introduced in the new consolidated skill.
  - **File**: `skills/designing-data-storage/SKILL.md` (lines 279-280)
  - **Description**: The new file references `@engineering-ai-pipelines` and `@managing-data-catalogs`, but those skills do not exist in this repository (existing skills are `@data-engineering-ai-ml` and `@data-engineering-catalogs`). This creates broken cross-skill navigation.
  - **Suggested fix**: Replace with valid skill names:
    - `@engineering-ai-pipelines` → `@data-engineering-ai-ml` (or `@engineering-ml-features` if that was intended)
    - `@managing-data-catalogs` → `@data-engineering-catalogs`

- **Issue [Major]**: TOC missing in a long newly-created file.
  - **File**: `skills/designing-data-storage/SKILL.md` (291 lines)
  - **Description**: The new consolidated SKILL file exceeds the long-file threshold and has no Table of Contents, while the task explicitly calls out TOC presence for long files.
  - **Suggested fix**: Add a TOC near the top with anchors for major sections (quick comparison, format selection, matrix, references, code examples, best practices, related skills).

- **Issue [Minor]**: `dependsOn` metadata was dropped in the new consolidated skill.
  - **File**: `skills/designing-data-storage/SKILL.md`
  - **Description**: Both deleted predecessor skills declared `dependsOn: ["@data-engineering-core"]`, but the new merged skill has no `dependsOn`. If dependency metadata is used by the loader/routing system, this is a behavioral regression.
  - **Suggested fix**: Add frontmatter dependency, at minimum `dependsOn: ["@data-engineering-core"]` (and any other intended prerequisite skills).

- **Note: Observations**
  - The requested `plan.md` and `anchor-context.md` at the provided paths were not present, so acceptance validation was performed against the stated task focus and actual changed hunks.

- **Gate: Fail**
