---
id: das-wxeh
status: closed
deps: [das-ix8j]
links: [das-s0yk, das-ix8j]
created: 2026-03-10T15:59:03Z
type: task
priority: 2
assignee: legout
parent: das-g8hg
tags: [skill-refactor, data-engineering, cloud-storage, integrations]
---
# Consolidate framework integration references in accessing-cloud-storage

Move Polars, DuckDB, Pandas, and PyArrow remote-I/O integration guidance into the new skill.

## Acceptance Criteria

- integration guidance for Polars, DuckDB, Pandas, and PyArrow is consolidated
- integrations point back to the library layer instead of duplicating setup detail
- touched content has explicit boundaries versus designing-data-storage


## Notes

**2026-03-11T12:02:47Z**

Implementation complete:
- Added DataFrame Integration section with Polars, DuckDB, Pandas, PyArrow subsections
- Each framework has comparison table, when-to-use guidance, 2-3 code examples
- All frameworks reference @data-engineering-storage-authentication for setup
- Fixed 2 Major issues (TOC summaries, auth refs) and 2 Minor issues (missing import, comment clarity)
- Review-post-fix: Clear pass (4/4 checks)
- Commit: 3ebdffd
