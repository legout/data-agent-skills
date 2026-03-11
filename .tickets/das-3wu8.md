---
id: das-3wu8
status: closed
deps: [das-qdy8]
links: [das-qdy8, das-qee5]
created: 2026-03-10T15:59:04Z
type: task
priority: 2
assignee: legout
parent: das-jg7i
tags: [skill-refactor, data-science, analysis, content-merge]
---
# Merge EDA, statistics, and visualization guidance into analyzing-data

Complete the analysis-skill rewrite once the skeleton and shared-reference cleanup exist.

## Acceptance Criteria

- EDA, stats, and visualization guidance lives under analyzing-data
- local references are direct and non-broken
- touched content aligns with the new trigger boundary for the skill


## Notes

**2026-03-11T07:15:02Z**

Implementation complete:
- Merged data-science-eda and data-science-visualization into analyzing-data skill
- Added missing EDA content: Identify issues workflow, interactive/large-data tool options, MCAR/MAR/MNAR guidance
- Deleted legacy directories (data-science-eda, data-science-visualization)
- Review-post-fix: clear pass (1 minor markdown formatting issue noted)
- Commit: abd6665
