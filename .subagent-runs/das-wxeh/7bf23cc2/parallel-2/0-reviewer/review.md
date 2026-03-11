## Review

- What's correct
  - Consolidation into a single `DataFrame Integration` section is implemented and clearly structured (comparison table, when-to-use guidance, per-framework subsections).
  - The four standalone integration skill references (Polars/DuckDB/Pandas/PyArrow) were removed from `Detailed Guides`, aligning with the consolidation goal.
  - Quick Start now includes framework-level examples (Polars + DuckDB) in addition to library-level approaches.
  - A format-boundary note is present and correctly points to `@data-engineering-storage-formats`.

- Issue [Major]: Plan requirement for inline TOC summaries per framework was not implemented. The plan required replacing each of the 4 integration bullets with inline summary entries in the “DataFrame Integrations” list. Current content only has a single “See DataFrame Integration section below...” line.
  - File: `/Users/volker/.pi/agent/skills/data-engineering-storage-remote-access/SKILL.md`
  - Suggested fix: In `### DataFrame Integrations`, add 4 bullets (Polars, DuckDB, Pandas, PyArrow) with short one-line summaries and links/mentions to the appropriate auth/setup layer, while keeping Delta/Iceberg links.

- Issue [Major]: Not all framework subsections link to the authentication/setup layer as required by plan/acceptance criteria.
  - File: `/Users/volker/.pi/agent/skills/data-engineering-storage-remote-access/SKILL.md`
  - Details: Polars and DuckDB mention `@data-engineering-storage-authentication`, but Pandas and PyArrow sections do not.
  - Suggested fix: Add explicit auth/setup references (e.g., “See `@data-engineering-storage-authentication` for credentials and cloud auth patterns”) in Pandas and PyArrow subsections.

- Issue [Minor]: Some code snippets are likely incomplete/inconsistent for copy-paste use.
  - File: `/Users/volker/.pi/agent/skills/data-engineering-storage-remote-access/SKILL.md`
  - Details:
    - Quick Start uses `pq.read_table(...)` without showing `import pyarrow.parquet as pq` in that snippet block.
    - Polars partitioned write example uses `partition_by=[...]` on `write_parquet(...)`; this may not match common Polars API usage across versions (often done via `use_pyarrow=True` + `pyarrow_options`).
  - Suggested fix: Ensure each snippet is self-contained for imports and align Polars partitioned-write example with version-stable API usage.

- Note: Observations
  - Scope reviewed only against the implementation-described changes for this ticket.
  - `anchor-context.md` was referenced in the task but not present at the provided path.

- Gate: Fail
