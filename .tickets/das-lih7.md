---
id: das-lih7
status: in_progress
deps: [das-3jql]
links: [das-yfvl, das-gp3f]
created: 2026-03-10T15:59:02Z
type: task
priority: 1
assignee: legout
parent: das-hhlo
tags: [skill-refactor, foundation, evals, manifests]
---
# Create the eval directory layout and manifest templates

Lay down the common eval structure so per-skill coverage can be added incrementally.

## Acceptance Criteria

- eval and trigger-eval directory structure exists
- a reusable manifest format is documented or scaffolded
- contributors can tell where new skill eval files belong


## Notes

**2026-03-10T19:47:06Z**

Implementation complete but gate uncertain - requires follow-up

**Completed:**
- Created eval/ structure with 14 target-skill manifests (task + trigger)
- Added 70 task evaluations (5 per skill) and 210 trigger evaluations (15 per skill)
- Created eval/README.md with manifest format documentation
- Fixed README wording to clarify 14-skill scope
- Committed as fb16288

**Blocker (Major):**
- Scope mismatch: eval/ and eval/trigger-eval/ contain 43 manifests each (29 legacy + 14 target)
- SKILL_REFACTORING_PLAN.md Section 10 specifies only 14 target-skill pairs
- Legacy manifests need removal/relocation before close

**Next steps:**
- Remove or relocate 29 legacy-named manifests from eval/ and eval/trigger-eval/
- Re-verify manifest count matches 14-skill target architecture
