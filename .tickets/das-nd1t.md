---
id: das-nd1t
status: closed
deps: [das-u0hp]
links: [das-u0hp]
created: 2026-03-10T15:55:12Z
type: task
priority: 2
assignee: legout
parent: das-68kl
tags: [skill-refactor, data-science, model-evaluation]
---
# Create evaluating-ml-models with CV, metrics, tuning, and experiment tracking guidance

Refactor model-evaluation guidance into the new focused skill.

## Acceptance Criteria

- new evaluating-ml-models skill exists with direct references for CV, metrics, tuning, and experiment tracking
- duplicated evaluation references are removed from the old data-science tree
- touched content has eval coverage and no broken refs


## Notes

**2026-03-11T11:32:07Z**

Implementation complete:
- Created skills/evaluating-ml-models/ with SKILL.md and 4 reference files
- Added evals/evaluating-ml-models.json (5 task_evals, 20 trigger_evals)
- Deprecated data-science-model-evaluation with redirect notice
- Fixed 1 minor issue (dependency wording in Related skills)
- Tests: 6/6 passed, review-post-fix: clear pass
- Commit: e6109f5
