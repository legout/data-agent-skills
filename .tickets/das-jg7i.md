---
id: das-jg7i
status: closed
deps: [das-llsd, das-qee5, das-qdy8, das-3wu8]
links: [das-r62z, das-hoav, das-u0hp]
created: 2026-03-10T15:55:11Z
type: task
priority: 2
assignee: legout
parent: das-68kl
tags: [skill-refactor, data-science, analysis]
---
# Create analyzing-data by merging EDA and visualization guidance

Remove the heaviest data-science overlap by combining EDA and visualization into one analysis skill.

## Acceptance Criteria

- new analyzing-data skill exists with direct references for EDA, statistics, and visualization patterns
- duplicated shared analysis references are collapsed instead of copied
- touched content has eval coverage and no broken refs


## Notes

**2026-03-11T09:18:14Z**

Implementation: Fixed 4 broken related-skill references in skills/analyzing-data/SKILL.md:
- building-data-apps → data-science-interactive-apps
- engineering-ml-features → data-science-feature-engineering
- evaluating-ml-models → data-science-model-evaluation
- working-in-notebooks → data-science-notebooks
Applied in both 'When NOT to use this skill' and 'Related skills' sections.
Commit: 7f3afce

Blocker: Post-fix review gate 'Uncertain' - chain artifacts (anchor-context.md, implementation.md) missing at expected path (.subagent-runs/das-jg7i/2f6694e5/). Functional objective verified complete (no broken refs remain), but procedural gate not cleared per fix-loop policy (maxFixPasses=1).
