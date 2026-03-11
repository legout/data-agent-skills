---
id: das-09vu
status: closed
deps: [das-llsd, das-n3x8]
links: [das-g8hg, das-k0lp, das-trf5, das-n3x8]
created: 2026-03-10T15:55:11Z
type: task
priority: 3
assignee: legout
parent: das-0p8u
tags: [skill-refactor, data-engineering, flowerpower]
---
# Refresh using-flowerpower as a dedicated framework skill with scripts and clear boundaries

Keep FlowerPower dedicated while tightening its overlap with generic orchestration and pipeline skills.

## Acceptance Criteria

- using-flowerpower remains a dedicated skill with direct script references
- overlap boundaries versus building-data-pipelines and orchestrating-data-pipelines are explicit
- touched content has eval coverage and no broken refs


## Notes

**2026-03-11T14:49:49Z**

Implementation complete:
- Renamed skill frontmatter from 'flowerpower' to 'using-flowerpower'
- Added explicit Skill Boundaries section comparing against canonical skills (building-data-pipelines, orchestrating-data-pipelines)
- Updated intro, rule-of-thumb, and See Also sections to reference proper skill names
- Minor: name/directory mismatch (using-flowerpower vs flowerpower) remains as non-blocking lint warning
Commit: 70aa495
