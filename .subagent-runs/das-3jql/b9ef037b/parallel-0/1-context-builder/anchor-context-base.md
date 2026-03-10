# Anchor Context

## Ticket Summary
- **ID**: das-3jql
- **What**: Approve and document the final 14-skill map with naming rules and boundary clarifications
- **Why**: Lock the future skill names and routing language before templates and evals are expanded
- **Scope**: Documentation files for skill taxonomy and naming conventions in the data-agent-skills repo

## Complexity Assessment
- **Level**: simple
- **Rationale**: This is a documentation task to record approved skill taxonomy and naming conventions. The 14 skills already exist in the codebase (visible in /skills/ directory). The task involves documenting what already exists with clear naming rules and boundary clarifications.
- **LOC Estimate**: <50 (documentation only)

## Research Gaps
- None - the 14-skill map appears to already exist in the codebase (visible as directories in /skills/). The task is to formalize and document the taxonomy.

## External Libraries
- None - this is a documentation-only task

## Testing Requirements
- Not applicable - documentation task with no code changes

## Recommended Path
- **Path**: A (Minimal)
- **Rationale**: This is a straightforward documentation task. The work involves:
  1. Recording the approved 14-skill list in repo docs
  2. Documenting explicit naming rules consistent with the plan
  3. Calling out adjacent-skill boundaries where trigger confusion is likely
  
- **Research needed?**: no

## Lessons Applied
- No prior AGENTS.md or knowledge files found to apply lessons from

## Related Context
- Parent ticket: das-ngoo (Finalize the 14-skill taxonomy, naming rules, templates, and dependsOn policy)
- Child tickets that depend on this: 
  - das-xl5m (Add reusable SKILL.md and reference templates)
  - das-b143 (Document the standard frontmatter policy and the dependsOn decision)

---

## Implementation Details

### The 14-Skill Map (from SKILL_REFACTORING_PLAN.md)

| Proposed Skill | Current Skills Folded In |
|---|---|
| `building-data-pipelines` | data-engineering-core, data-engineering-best-practices |
| `accessing-cloud-storage` | data-engineering-storage-authentication, data-engineering-storage-remote-access, +9 integrations |
| `designing-data-storage` | data-engineering-storage-formats, data-engineering-storage-lakehouse |
| `managing-data-catalogs` | data-engineering-catalogs |
| `orchestrating-data-pipelines` | data-engineering-orchestration |
| `assuring-data-pipelines` | data-engineering-quality, data-engineering-observability |
| `building-streaming-pipelines` | data-engineering-streaming |
| `engineering-ai-pipelines` | data-engineering-ai-ml |
| `using-flowerpower` | flowerpower |
| `analyzing-data` | data-science-eda, data-science-visualization |
| `engineering-ml-features` | data-science-feature-engineering |
| `evaluating-ml-models` | data-science-model-evaluation |
| `working-in-notebooks` | data-science-notebooks |
| `building-data-apps` | data-science-interactive-apps |

### Naming Convention

Current (verbose, taxonomic):
- `data-engineering-storage-remote-access-integrations-polars`

Proposed (short, action-oriented):
- `building-data-pipelines`
- `accessing-cloud-storage`
- `designing-data-storage`

### Adjacent-Skill Boundaries (Trigger Confusion Areas)

1. **EDA vs Visualization**: Both handle exploratory charts
   - EDA: "understanding dataset structure, distributions"
   - Visualization: "creating exploratory charts during EDA"
   - Overlap in user intent

2. **Quality vs Observability**: Both handle pipeline reliability
   - Quality: "data validation, schema checks"
   - Observability: "tracing, metrics, performance monitoring"
   - Fuzzy boundary for "pipeline health" queries

3. **Orchestration vs FlowerPower**: Both handle pipeline execution
   - Orchestration: Prefect, Dagster, dbt (scheduling, state persistence)
   - FlowerPower: Hamilton DAGs (code-first, lightweight)
   - Distinction: production scheduling vs configuration-driven DAGs

### Acceptance Criteria

- [ ] The approved 14-skill list is recorded in repo docs
- [ ] Naming rules are explicit and consistent with the plan
- [ ] Adjacent-skill boundaries are called out where trigger confusion is likely

### Output Location

Create documentation in one of:
- `docs/skill-map.md` (new)
- Update `README.md` with approved taxonomy
- Or create `docs/naming-conventions.md`
