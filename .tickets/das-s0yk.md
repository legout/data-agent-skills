---
id: das-s0yk
status: open
deps: [das-llsd]
links: [das-ix8j, das-wxeh]
created: 2026-03-10T15:59:03Z
type: task
priority: 1
assignee: legout
parent: das-g8hg
tags: [skill-refactor, data-engineering, cloud-storage, auth]
---
# Merge provider auth guidance into accessing-cloud-storage

Pull AWS, GCP, and Azure auth guidance under the new storage-access skill boundary.

## Acceptance Criteria

- provider auth guidance is consolidated under accessing-cloud-storage
- references clearly separate auth setup from storage-design choices
- direct links replace indirect or hybrid routing where touched

