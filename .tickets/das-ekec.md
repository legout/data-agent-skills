---
id: das-ekec
status: closed
deps: [das-llsd]
links: [das-n3x8, das-llsd]
created: 2026-03-10T15:55:10Z
type: task
priority: 2
assignee: legout
parent: das-0p8u
tags: [skill-refactor, data-engineering, assurance]
---
# Create assuring-data-pipelines by merging quality and observability guidance

Combine data quality and observability into one operational assurance skill.

## Acceptance Criteria

- new assuring-data-pipelines skill exists with quality, observability, and feedback-loop guidance
- Great Expectations, Pandera, OpenTelemetry, and Prometheus guidance is consolidated
- touched content has eval coverage and no broken refs


## Notes

**2026-03-11T07:57:36Z**

Implementation complete: merged data-engineering-quality and data-engineering-observability into assuring-data-pipelines skill.

- Created skills/assuring-data-pipelines/SKILL.md with "two pillars" structure (quality + observability)
- Updated 13 files to reference @assuring-data-pipelines instead of legacy skills
- Fixed 2 missed reference migrations post-review (flowerpower/SKILL.md, data-engineering-core/core-detailed.md)
- Post-fix gate: clear pass

Key files:
- skills/assuring-data-pipelines/SKILL.md (new, 598 lines)
- skills/data-engineering*/SKILL.md (13 updated)

Commit: 973d0d6
