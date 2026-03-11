# Implementation Plan

## Goal
Consolidate Polars, DuckDB, Pandas, and PyArrow integration guidance into the `accessing-cloud-storage` skill by adding a DataFrame Integration section to SKILL.md, following the inline deep-dive pattern from das-ix8j.

## Tasks

1. **Add DataFrame Integration Section to accessing-cloud-storage/SKILL.md**
   - File: `skills/accessing-cloud-storage/SKILL.md`
   - Changes: Add new section after "Library Deep Dives" section (~line 90)
   - Content to include:
     - Quick comparison table of the 4 frameworks for cloud storage access
     - When to use which framework
     - 4 subsections (one per framework) with:
       - Key integration approach (native URI, fsspec bridge, or PyArrow bridge)
       - 2-3 concise code examples showing cloud storage I/O
       - Link to library layer for authentication/setup details
       - Link to data-engineering-core for framework basics
   - Acceptance: Section renders correctly, all code examples are valid, no duplication of library setup

2. **Create Integration Summary Entries for Each Framework**
   - File: `skills/accessing-cloud-storage/SKILL.md`
   - Changes: Add to the "DataFrame Integrations" bullet list in the table of contents area (~line 65)
   - Replace external skill references with inline summaries:
     - Change: `- @data-engineering-storage-remote-access-integrations-polars` → brief inline summary + link to library layer
     - Same for DuckDB, Pandas, PyArrow
   - Acceptance: All 4 frameworks have inline summaries pointing to library layer for details

3. **Update Quick Start Example**
   - File: `skills/accessing-cloud-storage/SKILL.md`
   - Changes: Add framework-specific examples to Quick Start section (~line 115)
   - Add example showing Polars native cloud URI usage
   - Add example showing DuckDB HTTPFS query
   - Keep existing fsspec/pyarrow.fs/obstore examples
   - Acceptance: Quick start shows all 3 library approaches + 2 framework approaches

4. **Verify Boundaries with data-engineering-storage-formats**
   - File: `skills/accessing-cloud-storage/SKILL.md`
   - Changes: Add explicit boundary note in DataFrame Integration section
   - Content: "For format-specific details (Parquet, Arrow, etc.), see @data-engineering-storage-formats"
   - Ensure no format deep-dives are duplicated (compression, schema evolution, etc.)
   - Acceptance: Clear boundary statement exists, no format duplication

5. **Remove References to Separate Integration Skills**
   - File: `skills/accessing-cloud-storage/SKILL.md`
   - Changes: Update "Detailed Guides" section (~line 55)
   - Remove or consolidate the 4 integration skill references:
     - `@data-engineering-storage-remote-access-integrations-polars`
     - `@data-engineering-storage-remote-access-integrations-duckdb`
     - `@data-engineering-storage-remote-access-integrations-pandas`
     - `@data-engineering-storage-remote-access-integrations-pyarrow`
   - Replace with inline content from Tasks 1-2
   - Keep references to library skills and other infrastructure skills
   - Acceptance: No standalone integration skill references remain

## Files to Modify

- `skills/accessing-cloud-storage/SKILL.md` - Add DataFrame Integration section, update navigation, add boundary notes

## New Files (if any)

None - all content should be inlined following das-ix8j pattern

## Dependencies

- **Prerequisite**: das-ix8j (library layer consolidation) should be completed first - this adds framework layer on top
- **Soft dependency**: Content from the 4 integration skills must be available for consolidation

## Risks

1. **Content duplication risk**: Must avoid duplicating authentication setup (point to library layer) and format details (point to data-engineering-storage-formats)
2. **Scope creep risk**: Framework integration section should stay focused on cloud storage I/O patterns, not general framework usage
3. **Inconsistency risk**: Code examples must match the library layer patterns (fsspec, pyarrow.fs, obstore) already established in das-ix8j
4. **Missing boundary clarity**: Must explicitly state that format details live in data-engineering-storage-formats to avoid confusion

## Content Source Mapping

Source skill content to consolidate into inline sections:

| Source Skill | Key Content to Inline | What to Reference Instead |
|--------------|----------------------|---------------------------|
| data-engineering-storage-remote-access-integrations-polars | Native cloud URI (`s3://`), fsspec bridge, PyArrow dataset bridge | Authentication → library layer; General Polars → data-engineering-core |
| data-engineering-storage-remote-access-integrations-duckdb | HTTPFS extension, COPY TO/FROM, Delta scanning | Setup → library layer; SQL patterns → data-engineering-core |
| data-engineering-storage-remote-access-integrations-pandas | fsspec auto-detection, explicit filesystem | Authentication → library layer; pandas basics → data-engineering-core |
| data-engineering-storage-remote-access-integrations-pyarrow | Native filesystem, dataset scanning | Already covered in library layer, just show integration |

## Verification Checklist

- [ ] SKILL.md has new "DataFrame Integration" section with 4 framework subsections
- [ ] Each framework section has 2-3 code examples
- [ ] Each framework section links to library layer for auth/setup
- [ ] No authentication setup is duplicated (all references to library layer)
- [ ] No format deep-dives are duplicated (references to data-engineering-storage-formats)
- [ ] Quick Start section has framework examples added
- [ ] "Detailed Guides" section no longer lists standalone integration skills
- [ ] All code examples are syntactically valid Python
