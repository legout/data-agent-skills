Done! I've created the anchor context for ticket **das-wxeh** at `/Users/volker/coding/libs/data-agent-skills/anchor-context.md`.

**Summary:**
- **Task**: Consolidate Polars, DuckDB, Pandas, and PyArrow integration guidance into the `accessing-cloud-storage` skill
- **Pattern to follow**: Similar to das-ix8j (library layer consolidation) - inline library deep-dives in SKILL.md rather than separate files
- **Target**: `skills/accessing-cloud-storage/SKILL.md` (add DataFrame integration section)
- **Sources**: 4 integration skills to merge
- **Key acceptance criteria**: Integrations point back to library layer (fsspec, pyarrow.fs, obstore) instead of duplicating setup detail; boundaries with `data-engineering-storage-formats` are explicit

**Complexity**: Medium - straightforward content merging following established precedent from das-ix8j/das-s0yk, no research needed.