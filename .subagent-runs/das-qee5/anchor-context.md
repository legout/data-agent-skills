I've created the anchor-context.md at `/Users/volker/coding/libs/data-agent-skills/anchor-context.md`.

**Summary:**

The ticket **das-qee5** is a skeleton creation task - establishing the structural framework for a new `analyzing-data` skill that will eventually merge EDA and visualization content. Key points:

- **Complexity**: Simple (<100 LOC) - using existing skill templates
- **Path**: A (Minimal) - existing patterns from `data-science-eda`, `data-science-feature-engineering`, etc. provide clear templates
- **No research needed** - the refactoring plan (SKILL_REFACTORING_PLAN.md) explicitly defines this skill
- **Routing boundaries** must be explicit to related skills (feature engineering, notebooks, data apps)
- **Reference slots** needed for: EDA, statistics, and visualization topics

The anchor context provides concrete file hints for:
- The new skeleton location (`skills/analyzing-data/SKILL.md`)
- 4 existing skill templates to reference
- Frontmatter template
- 7 key sections to include in the skeleton