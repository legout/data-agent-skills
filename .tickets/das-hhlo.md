---
id: das-hhlo
status: closed
deps: [das-ngoo, das-lih7, das-yfvl, das-gp3f]
links: []
created: 2026-03-10T15:55:10Z
type: task
priority: 1
assignee: legout
parent: das-1f71
tags: [skill-refactor, foundation, evals, triggers]
---
# Scaffold eval manifests and trigger-eval sets for all future skills

Create the evaluation skeletons the plan calls for before broad content rewrites begin.

## Acceptance Criteria

- add eval manifests for all 14 future skills
- add positive and near-miss trigger cases for each skill
- document how contributors should add or update eval coverage


## Notes

**2026-03-10T21:21:16Z**

Verification Complete ✅

- All 14 task evaluation manifests exist in eval/ (70 evaluations total)
- All 14 trigger evaluation manifests exist in eval/trigger-eval/ (210 evaluations total)
- Trigger evals have proper category distribution: 6 positive, 3 near-miss, 6 negative per skill
- eval/README.md provides comprehensive contributor documentation with schemas, workflows, and maintenance guidance

Work was completed in das-lih7. This ticket verified completeness of the eval scaffolding for all 14 target skills defined in SKILL_REFACTORING_PLAN.md §5.2.

All acceptance criteria satisfied - clear pass.
