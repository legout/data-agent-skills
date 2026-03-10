---
id: das-ix8j
status: open
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

