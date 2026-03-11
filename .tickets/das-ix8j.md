---
id: das-ix8j
status: closed
deps: [das-llsd]
links: [das-s0yk, das-wxeh]
created: 2026-03-10T15:59:03Z
type: task
priority: 2
assignee: legout
parent: das-g8hg
tags: [skill-refactor, data-engineering, cloud-storage, libraries]
---
# Consolidate remote-access library references in accessing-cloud-storage

Group fsspec, pyarrow.fs, and obstore guidance into one coherent library layer.

## Acceptance Criteria

- fsspec, pyarrow.fs, and obstore guidance is consolidated
- library-selection notes are colocated with the new skill
- touched references use the new direct-link style


## Notes

**2026-03-11T08:29:05Z**

Implementation complete:
- Consolidated 4 remote-access library skills (fsspec, pyarrow.fs, obstore) into accessing-cloud-storage
- Inlined library deep-dives in SKILL.md for cohesive library selection layer
- Added performance.md (caching, concurrency, Parquet optimization)
- Added patterns.md (incremental loading, partitioned writes, cross-cloud copy)
- Tests: 4/4 checks passed
- Review post-fix: Clear pass
- Commit: 83ab7a5
