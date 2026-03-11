---
id: das-r62z
status: closed
deps: [das-jg7i]
links: [das-jg7i, das-hoav, das-u0hp]
created: 2026-03-10T15:55:12Z
type: task
priority: 2
assignee: legout
parent: das-68kl
tags: [skill-refactor, data-science, notebooks]
---
# Create working-in-notebooks for Jupyter, marimo, and notebook publishing workflows

Refactor notebook guidance into the new workflow-centered skill with cleaner routing boundaries.

## Acceptance Criteria

- new working-in-notebooks skill exists with direct references for Jupyter, marimo, sharing, and reproducibility
- notebook-specific broken refs are fixed as part of the rewrite
- touched content has eval coverage and explicit boundaries versus app-building


## Notes

**2026-03-11T10:37:56Z**

Implementation complete:
- Created skills/working-in-notebooks/ with SKILL.md + 3 references (jupyter-guide, marimo-guide, reproducibility-patterns)
- Removed dependsOn from frontmatter per lint standards
- Fixed Major issue: corrected marimo API (@mo.cell → @app.cell) with runnable context
- Added evals/working-in-notebooks.json (5 task + 15 trigger evaluations)
- Commit: 6406ef6
