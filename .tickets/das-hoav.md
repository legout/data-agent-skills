---
id: das-hoav
status: closed
deps: [das-r62z]
links: [das-jg7i, das-r62z, das-u0hp]
created: 2026-03-10T15:55:12Z
type: task
priority: 3
assignee: legout
parent: das-68kl
tags: [skill-refactor, data-science, data-apps]
---
# Create building-data-apps for Streamlit, Panel, Gradio, Dash, and NiceGUI workflows

Refactor app-building guidance into its own skill without overlapping notebook triggers.

## Acceptance Criteria

- new building-data-apps skill exists with direct references for the supported frameworks
- app-building references are consolidated without duplicating notebook guidance
- touched content has eval coverage and explicit boundaries versus working-in-notebooks


## Notes

**2026-03-11T16:31:14Z**

Implementation complete:

• Created building-data-apps skill with comprehensive coverage of 5 frameworks (Streamlit, Panel, Gradio, Dash, NiceGUI)
• Added 7 reference files: framework advanced guides, selection decision matrix, deployment patterns
• Fixed 3 Major issues: wildcard websocket origins (security), Docker healthcheck (correctness), Panel reactive patterns (correctness)
• Fixed 3 Minor issues: missing imports, incorrect PreventUpdate path
• Deprecated data-science-interactive-apps with migration notice
• All acceptance criteria met, review-post-fix gate clear pass

Key files: skills/building-data-apps/SKILL.md, references/*.md (7)
Commit: 90113df
