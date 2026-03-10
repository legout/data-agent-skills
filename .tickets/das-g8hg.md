---
id: das-g8hg
status: open
deps: [das-llsd, das-s0yk, das-ix8j, das-wxeh]
links: [das-09vu, das-k0lp, das-trf5]
created: 2026-03-10T15:55:10Z
type: task
priority: 2
assignee: legout
parent: das-0p8u
tags: [skill-refactor, data-engineering, cloud-storage]
---
# Create accessing-cloud-storage by merging auth, remote access, and integration skills

Collapse the fragmented storage-access skill tree into one coherent workflow-centered skill.

## Acceptance Criteria

- new accessing-cloud-storage skill exists with provider/auth, library, and integration references
- obsolete remote-access child-skill content is mapped into the new skill
- touched content has clear routing boundaries versus storage-design work

