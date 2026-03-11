---
id: das-u0hp
status: closed
deps: [das-jg7i]
links: [das-jg7i, das-r62z, das-hoav, das-nd1t]
created: 2026-03-10T15:55:12Z
type: task
priority: 2
assignee: legout
parent: das-68kl
tags: [skill-refactor, data-science, features]
---
# Create engineering-ml-features with leakage-safe preprocessing and representation guidance

Refactor feature-engineering guidance into the new focused skill.

## Acceptance Criteria

- new engineering-ml-features skill exists with clear workflow guidance
- preprocessing, encoding, datetime, and text-feature references are consolidated without duplication
- touched content has eval coverage and no broken refs


## Notes

**2026-03-11T11:11:38Z**

Implementation complete:
- Created engineering-ml-features skill with SKILL.md + 4 reference files
- Covers: categorical encoding, numeric scaling, datetime features, text features, leakage-safe pipelines, feature selection
- Fixed 2 Major issues (TargetEncoder cv param unsupported → sklearn.preprocessing.TargetEncoder; RandomizedLasso deprecated → stability_selection function)
- Fixed 1 Minor (division by zero guard for uppercase_ratio)
- Commit: 373110e
- Post-fix review: Clear pass
