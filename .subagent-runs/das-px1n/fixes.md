## Fixes Applied

- Fixed [Major]: Broken related-skill references in `skills/designing-data-storage/SKILL.md` — Replaced non-existent skill references:
  - `@engineering-ai-pipelines` → `@data-engineering-ai-ml`
  - `@managing-data-catalogs` → `@data-engineering-catalogs`

- Fixed [Major]: Missing TOC in `skills/designing-data-storage/SKILL.md` (291 lines) — Added Table of Contents section with anchor links to all major sections: Quick Format Comparison, When to Use Which?, Format Selection Matrix, Detailed Reference Guides, Code Examples, Best Practices, Compression Codec Comparison, Related Skills, and References.

- Fixed [Minor]: Missing `dependsOn` metadata in `skills/designing-data-storage/SKILL.md` — Added `dependsOn: ["@data-engineering-core"]` to frontmatter to restore the dependency declaration that both predecessor skills (`data-engineering-storage-formats` and `data-engineering-storage-lakehouse`) had.

## Status
All critical and major issues resolved. 0 minor/suggestions skipped.
