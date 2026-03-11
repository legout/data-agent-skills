---
id: das-qee5
status: closed
deps: [das-llsd]
links: [das-qdy8, das-3wu8]
created: 2026-03-10T15:59:04Z
type: task
priority: 1
assignee: legout
parent: das-jg7i
tags: [skill-refactor, data-science, analysis, skeleton]
---
# Author the analyzing-data skill skeleton and routing boundaries

Define the new analysis skill's scope before content is merged into it.

## Acceptance Criteria

- analyzing-data SKILL.md skeleton exists
- boundaries versus feature engineering, notebooks, and data apps are explicit
- direct reference slots are laid out for the main analysis topics


## Notes

**2026-03-10T21:55:54Z**

Implementation complete:
- Created skills/analyzing-data/SKILL.md with 7 standard sections
- Added 4 reference placeholders (profiling, statistical-tests, visualization, large-dataset-eda)
- Fixed related-skill routing to use plain backtick notation per plan
- Commit: db065c7
- Gate: Clear pass (post-fix review verified)
