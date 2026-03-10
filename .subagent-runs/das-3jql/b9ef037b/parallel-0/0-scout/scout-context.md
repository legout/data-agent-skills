# Scout Context

## Files Retrieved

### Primary Documentation
1. **`SKILL_REFACTORING_PLAN.md`** (lines 1-820) - **Main reference for 14-skill architecture**
   - Section 5.2 contains the complete proposed target architecture (14 skills)
   - Section 6 contains the migration map (29 → 14 skills)
   - Section 9.1 defines naming strategy (action-oriented names)
   - Section 7 contains framework/tool disposition matrix
   - **Why relevant**: This is the source of truth for the skill taxonomy and naming conventions

2. **`README.md`** (lines 1-90) - **Current state documentation**
   - Shows current 29 skills organized by category
   - Documents installation and development workflow
   - **Why relevant**: Shows the current skill organization before refactoring

3. **`.tickets/das-3jql.md`** - **Ticket specification**
   - Acceptance criteria: approved 14-skill list, explicit naming rules, adjacent-skill boundaries
   - **Why relevant**: Defines the exact deliverables required

### Current Skill Files (representative samples)
4. **`skills/data-engineering-core/SKILL.md`** (lines 1-135) - **Core skill example**
   - Shows current naming convention: `data-engineering-core`
   - Uses `dependsOn` frontmatter
   - Shows `@skill-name` routing syntax in body
   - **Why relevant**: Example of current skill structure

5. **`skills/data-science-eda/SKILL.md`** - **EDA skill**
   - Overlaps with `data-science-visualization`
   - Uses trigger routing to related skills

6. **`skills/data-science-visualization/SKILL.md`** - **Visualization skill**
   - Trigger confusion area with EDA
   - Example of adjacent-skill boundary issue

7. **`skills/data-engineering-quality/SKILL.md`** - **Quality skill**
   - Overlaps with `data-engineering-observability`
   - Shows boundary confusion area

8. **`skills/flowerpower/SKILL.md`** - **FlowerPower skill**
   - Distinct from orchestration (has specific framework workflow)
   - Heavy `dependsOn` list

### Tools
9. **`tools/skill_lint.py`** (lines 1-50) - **Linting tool**
   - Validates frontmatter, references, Python syntax
   - Currently produces 49 warnings (mostly `dependsOn` and broken refs)
   - **Why relevant**: Part of the naming/quality infrastructure

### Directory Structure
10. **`skills/`** - Contains all 29 current skills
    - `data-engineering*` (23 skills)
    - `data-science*` (5 skills)
    - `flowerpower` (1 skill)

---

## Dependency Graph

```
Current 29 Skills (to be refactored to 14)
├── data-engineering (HUB - to be deprecated → docs/skill-map.md)
│   └── references all sub-skills via @-routing
├── data-engineering-core → merges into → building-data-pipelines
├── data-engineering-best-practices → merges into → building-data-pipelines
├── data-engineering-storage-authentication → merges into → accessing-cloud-storage
├── data-engineering-storage-remote-access (+9 integrations) → merges into → accessing-cloud-storage
├── data-engineering-storage-formats → merges into → designing-data-storage
├── data-engineering-storage-lakehouse → merges into → designing-data-storage
├── data-engineering-catalogs → merges into → managing-data-catalogs
├── data-engineering-orchestration → merges into → orchestrating-data-pipelines
├── data-engineering-quality ←→ data-engineering-observability → merge to → assuring-data-pipelines
├── data-engineering-streaming → merges into → building-streaming-pipelines
├── data-engineering-ai-ml → merges into → engineering-ai-pipelines
├── flowerpower → merges into → using-flowerpower
├── data-science-eda ←→ data-science-visualization → merge to → analyzing-data
├── data-science-feature-engineering → merges into → engineering-ml-features
├── data-science-model-evaluation → merges into → evaluating-ml-models
├── data-science-notebooks → merges into → working-in-notebooks
└── data-science-interactive-apps → merges into → building-data-apps
```

---

## Key Code

### Proposed 14-Skill Map (from SKILL_REFACTORING_PLAN.md Section 5.2)

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

### Naming Convention (Section 9.1)

**Current (verbose, taxonomic)**:
- `data-engineering-storage-remote-access-integrations-polars`

**Proposed (short, action-oriented)**:
- `building-data-pipelines`
- `accessing-cloud-storage`
- `designing-data-storage`

### Frontmatter Pattern (Current)

```yaml
---
name: data-engineering-core
description: "Core Python data engineering: Polars, DuckDB, PyArrow..."
dependsOn: ["@data-engineering-core"]
---
```

### Skill Routing Syntax

```markdown
# In SKILL.md body, use @-prefixed skill names:
- `@data-engineering-storage-lakehouse` — Delta/Iceberg/Hudi behavior
- `@data-science-eda` — Exploration patterns
```

---

## Architecture Notes

### Current State
- **29 top-level skills** in `skills/` directory
- Major duplication in data-science references (21 duplicate groups, 105 redundant copies)
- 22 broken local markdown references
- Hub skill `data-engineering` acts as broad catch-all (problematic for routing)
- Linting produces 49 warnings (mostly `dependsOn` and broken refs)

### Trigger Confusion Areas (Adjacent-Skill Boundaries)

1. **EDA vs Visualization** (`analyzing-data`)
   - EDA: "understanding dataset structure, distributions"
   - Visualization: "creating exploratory charts during EDA"
   - Solution: Merge into single `analyzing-data` skill

2. **Quality vs Observability** (`assuring-data-pipelines`)
   - Quality: "data validation, schema checks"
   - Observability: "tracing, metrics, performance monitoring"
   - Solution: Merge into single `assuring-data-pipelines` skill

3. **Orchestration vs FlowerPower**
   - Orchestration: Prefect, Dagster, dbt (scheduling, state persistence)
   - FlowerPower: Hamilton DAGs (code-first, lightweight)
   - Solution: Keep separate (`orchestrating-data-pipelines` vs `using-flowerpower`)

### Documentation Output Location
The task requires creating:
- **`docs/skill-map.md`** - Main documentation file for the 14-skill taxonomy

---

## Start Here

### Primary Files to Review
1. **Start with**: `SKILL_REFACTORING_PLAN.md` (Section 5.2 and 9.1)
   - This contains the approved 14-skill map and naming rules
   - The task is to formalize/document what is already proposed

2. **Then check**: `.tickets/das-3jql.md` for exact acceptance criteria

### What Needs to Be Done
The task is to **document** what already exists in the plan:
1. ✅ The 14-skill list is already defined in SKILL_REFACTORING_PLAN.md
2. ⏳ Naming rules need to be extracted and made explicit
3. ⏳ Adjacent-skill boundaries need to be called out:
   - EDA ↔ Visualization → `analyzing-data`
   - Quality ↔ Observability → `assuring-data-pipelines`
   - Orchestration ↔ FlowerPower → kept separate

### Output Location
Create documentation at: **`docs/skill-map.md`** (new file)

### Related Tickets
- **Parent**: das-ngoo - Finalize the 14-skill taxonomy, naming rules, templates, and dependsOn policy
- **Child**: das-xl5m - Add reusable SKILL.md and reference templates (depends on das-3jql)
- **Child**: das-b143 - Document standard frontmatter policy and dependsOn decision (depends on das-3jql)
