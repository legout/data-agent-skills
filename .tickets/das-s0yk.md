---
id: das-s0yk
status: closed
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


## Notes

**2026-03-11T06:34:28Z**

## Implementation Summary

- Created accessing-cloud-storage skill at ~/.pi/agent/skills/accessing-cloud-storage/
- Consolidated AWS/GCP/Azure auth guidance from data-engineering-storage-authentication
- Fixed major issue: removed concatenated GCP/Azure content from aws.md (lines 230+)
- Fixed minor issue: updated gcp.md reference to references/gcp.md in patterns.md

## Key Files

- ~/.pi/agent/skills/accessing-cloud-storage/SKILL.md (new)
- ~/.pi/agent/skills/accessing-cloud-storage/references/aws.md, gcp.md, azure.md, patterns.md, testing.md (5 files)

## Validation

- Lint tool run but cannot verify external skill path (~/.pi/agent/skills/)
- Post-fix review gate: Uncertain

## Commit

- d4d60eb

## Blocker

Post-fix gate "Uncertain" - lint tool cannot validate skill files outside repo at ~/.pi/agent/skills/. Source folder skills/data-engineering-storage-authentication/ NOT deleted pending clear pass. Requires follow-up run with either: (1) scoped lint mode, (2) manual verification acceptance, or (3) migration of skill into repo.

**2026-03-11T06:52:43Z**

Implementation complete: consolidated cloud storage auth into accessing-cloud-storage skill. Key files: SKILL.md + 5 reference files (aws.md, gcp.md, azure.md, patterns.md, testing.md). Legacy data-engineering-storage-authentication directory removed. Commit: 71d3f4c. Post-fix review: clear pass.
