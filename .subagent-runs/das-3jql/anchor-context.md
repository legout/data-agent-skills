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
- Current skills visible in repo: data-engineering*, data-science-*, flowerpower (approximately 29 skills)

---

## Code Context

### Files Retrieved

#### Primary Documentation
1. `SKILL_REFACTORING_PLAN.md` (lines 1-820) - **Main reference for 14-skill architecture**
   - Contains the complete proposed target architecture (Section 5)
   - Migration map from 29 skills to 14 skills (Section 6)
   - Naming strategy (Section 9.1)
   - Framework/tool disposition matrix (Section 7)
   - Status: Pre-implementation plan, not yet approved

2. `README.md` (lines 1-90) - **Current state documentation**
   - Shows current 29 skills organized by category
   - Documents installation and development workflow

3. `.tickets/das-3jql.md` - **Ticket specification**
   - Acceptance criteria: approved 14-skill list, explicit naming rules, adjacent-skill boundaries

4. `.tickets/das-ngoo.md` - **Parent ticket**
   - Depends on das-3jql being completed first
   - Finalizes templates and dependsOn policy

#### Current Skill Files (representative samples)
5. `skills/data-engineering/SKILL.md` (lines 1-90) - **Hub skill (to be deprecated)**
   - Acts as an index to all engineering skills
   - Will be converted to non-triggerable docs-only

6. `skills/data-engineering-core/SKILL.md` (lines 1-135) - **Core skill example**
   - Shows current naming convention: `data-engineering-core`
   - Shows `dependsOn` frontmatter usage

7. `skills/data-science-eda/SKILL.md` (lines 1-105) - **EDA skill**
   - Overlaps with data-science-visualization
   - Uses `dependsOn: ["@data-engineering-core"]`

8. `skills/data-science-visualization/SKILL.md` (lines 1-225) - **Visualization skill**
   - Trigger confusion area with EDA (both handle exploratory charts)

9. `skills/data-engineering-quality/SKILL.md` (lines 1-290) - **Quality skill**
   - Overlaps with data-engineering-observability

10. `skills/data-engineering-observability/SKILL.md` (lines 1-285) - **Observability skill**
    - Boundary confusion with data-engineering-quality

11. `skills/flowerpower/SKILL.md` (lines 1-340) - **FlowerPower skill**
    - Distinct from orchestration (has specific framework workflow)
    - Heavy `dependsOn` list (5 dependencies)

#### Tools
12. `tools/skill_lint.py` - **Linting tool**
    - Currently produces 49 warnings (mostly `dependsOn` and broken refs)

### Dependency Graph

```
Current 29 Skills (to be refactored to 14)
├── data-engineering (HUB - to be deprecated)
│   └── references all sub-skills via @-routing
├── data-engineering-core
│   └── (foundation for most other skills)
├── data-engineering-best-practices
│   └── overlaps with core
├── data-engineering-storage-authentication
├── data-engineering-storage-remote-access
│   └── splits into 9+ integration sub-skills
├── data-engineering-storage-formats
├── data-engineering-storage-lakehouse
├── data-engineering-catalogs
├── data-engineering-orchestration
├── data-engineering-quality ←→ data-engineering-observability (adjacent)
├── data-engineering-streaming
├── data-engineering-ai-ml
├── data-science-eda ←→ data-science-visualization (adjacent/trigger confusion)
├── data-science-feature-engineering
├── data-science-model-evaluation
├── data-science-notebooks
├── data-science-interactive-apps
└── flowerpower
    └── overlaps with orchestration
```

### Key Code

#### Proposed 14-Skill Map (from SKILL_REFACTORING_PLAN.md Section 5.2)

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

#### Naming Convention (Section 9.1)

Current (verbose, taxonomic):
- `data-engineering-storage-remote-access-integrations-polars`

Proposed (short, action-oriented):
- `building-data-pipelines`
- `accessing-cloud-storage`
- `designing-data-storage`

#### Frontmatter Pattern (Current)

```yaml
---
name: data-engineering-core
description: "Core Python data engineering: Polars, DuckDB, PyArrow..."
dependsOn: ["@data-engineering-core"]
---
```

**Pending Decision**: Whether to keep or remove `dependsOn` (see das-b143)

### Architecture Notes

#### Current State
- **29 top-level skills** in `skills/` directory
- Major duplication in data-science references (21 duplicate groups, 105 redundant copies)
- 22 broken local markdown references
- Hub skill `data-engineering` acts as broad catch-all (problematic for routing)

#### Trigger Confusion Areas (Adjacent-Skill Boundaries)
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

#### Pending Child Tickets
- **das-xl5m**: Add reusable SKILL.md and reference templates (depends on das-3jql)
- **das-b143**: Document standard frontmatter policy and dependsOn decision (depends on das-3jql)

### Start Here

#### Primary Files to Review
1. **Start with**: `SKILL_REFACTORING_PLAN.md` (Section 5.2 and 9.1)
   - This contains the approved 14-skill map and naming rules
   - The task is to formalize/document what is already proposed

2. **Then check**: `.tickets/das-3jql.md` for exact acceptance criteria

#### What Needs to Be Done
The task is to **document** what already exists in the plan:
1. ✅ The 14-skill list is already defined in SKILL_REFACTORING_PLAN.md
2. ⏳ Naming rules need to be extracted and made explicit
3. ⏳ Adjacent-skill boundaries need to be called out (EDA↔Viz, Quality↔Observability, Orchestration↔FlowerPower)

#### Recommended Output Location
Create documentation in one of:
- `docs/skill-map.md` (new)
- Update `README.md` with approved taxonomy
- Or create `docs/naming-conventions.md`
