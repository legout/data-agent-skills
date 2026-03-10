---
id: das-wxeh
status: open
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

