# Data Agent Skills Taxonomy

**Version:** 1.0.0  
**Status:** Authoritative  
**Last Updated:** 2026-03-10

This document is the **single source of truth** for the 14-skill taxonomy, naming conventions, and authoring standards for the data agent skills library.

---

## Quick Reference

| Document | Purpose |
|----------|---------|
| **This file (TAXONOMY.md)** | Skill taxonomy, naming rules, policy decisions |
| [skill-authoring.md](./skill-authoring.md) | Detailed authoring guide, frontmatter spec, routing patterns |
| [skill-map.md](./skill-map.md) | Detailed skill boundaries, migration mappings, adjacent skill guidance |
| [templates/README.md](./templates/README.md) | Reusable templates for SKILL.md and reference files |
| [../eval/README.md](../eval/README.md) | Evaluation manifests and testing methodology |

---

## The 14-Skill Taxonomy

The skill library is organized around **workflows** rather than deep taxonomic hierarchies. This consolidation (from 29 to 14 skills) eliminates duplication and clarifies trigger boundaries.

### Data Engineering Skills (9)

| Skill | Purpose | Merged From |
|-------|---------|-------------|
| `building-data-pipelines` | Core batch ETL/dataframe/SQL patterns + production architecture rules | `data-engineering-core`, `data-engineering-best-practices` |
| `accessing-cloud-storage` | Auth + remote object storage access + library/tool integrations | `data-engineering-storage-authentication`, `data-engineering-storage-remote-access`, all integration sub-skills |
| `designing-data-storage` | File formats + lakehouse table formats + storage design tradeoffs | `data-engineering-storage-formats`, `data-engineering-storage-lakehouse` |
| `managing-data-catalogs` | Catalog architecture, metadata systems, and multi-source access patterns | `data-engineering-catalogs` |
| `orchestrating-data-pipelines` | Prefect, Dagster, dbt, scheduling, retries, deployment patterns | `data-engineering-orchestration` |
| `assuring-data-pipelines` | Data quality + observability + operational validation loops | `data-engineering-quality`, `data-engineering-observability` |
| `building-streaming-pipelines` | Kafka, MQTT, NATS JetStream, streaming architecture | `data-engineering-streaming` |
| `engineering-ai-pipelines` | Embeddings, vector stores, RAG, LLM monitoring, batch inference | `data-engineering-ai-ml` |
| `using-flowerpower` | Dedicated FlowerPower/Hamilton workflow with executable scripts | `flowerpower` |

### Data Science Skills (5)

| Skill | Purpose | Merged From |
|-------|---------|-------------|
| `analyzing-data` | EDA + statistical exploration + visualization selection and patterns | `data-science-eda`, `data-science-visualization` |
| `engineering-ml-features` | Feature engineering, representation choices, leakage-safe preprocessing | `data-science-feature-engineering` |
| `evaluating-ml-models` | Cross-validation, metrics, model comparison, tuning, experiment tracking | `data-science-model-evaluation` |
| `working-in-notebooks` | Jupyter/marimo/reproducible notebook workflows | `data-science-notebooks` |
| `building-data-apps` | Streamlit/Panel/Gradio/Dash/NiceGUI app-building workflows | `data-science-interactive-apps` |

---

## Naming Conventions

### Rule 1: Use Action-Oriented Names

Skill names must start with a **verb** that describes what the user is doing:

| ✅ Good | ❌ Bad |
|---------|--------|
| `building-data-pipelines` | `data-engineering-core` |
| `accessing-cloud-storage` | `data-engineering-storage-remote-access` |
| `designing-data-storage` | `data-engineering-storage-lakehouse` |
| `analyzing-data` | `data-science-eda` |
| `evaluating-ml-models` | `data-science-model-evaluation` |

### Rule 2: Keep Names Short

Target **2-4 words** maximum. Avoid deep taxonomic nesting.

| ✅ Good | ❌ Bad |
|---------|--------|
| `accessing-cloud-storage` | `data-engineering-storage-remote-access-integrations-polars` |
| `designing-data-storage` | `data-engineering-storage-formats-and-lakehouse` |
| `assuring-data-pipelines` | `data-engineering-quality-and-observability` |

### Rule 3: Use Consistent Verb Conventions

| Verb | Use When |
|------|----------|
| `building-*` | Constructing pipelines, systems, or infrastructure |
| `accessing-*` | Connecting to, authenticating with, or reading from external systems |
| `designing-*` | Making architectural decisions, selecting formats, or planning storage |
| `managing-*` | Administrative, catalog, or metadata operations |
| `orchestrating-*` | Scheduling, coordination, and workflow management |
| `assuring-*` | Quality, validation, monitoring, and operational safety |
| `engineering-*` | Specialized technical construction (features, AI pipelines) |
| `analyzing-*` | Exploration, EDA, and insight generation |
| `evaluating-*` | Measurement, comparison, and assessment |
| `working-in-*` | Environment-specific workflows (notebooks) |
| `using-*` | Framework-specific dedicated workflows (FlowerPower) |

### Rule 4: Use Kebab-Case

All skill names use **kebab-case** (lowercase with hyphens):

| ✅ Good | ❌ Bad |
|---------|--------|
| `building-data-apps` | `buildingDataApps`, `building_data_apps` |
| `working-in-notebooks` | `workingInNotebooks`, `working_in_notebooks` |

### Technical Constraints

- Maximum **64 characters**
- Only lowercase letters, numbers, and hyphens
- Cannot start or end with a hyphen
- Cannot contain consecutive hyphens (`--`)
- Must match the directory name exactly

---

## Frontmatter Policy

Every `SKILL.md` file must include YAML frontmatter at the top.

### Required Fields

```yaml
---
name: skill-name                    # Must match directory name
description: |                      # Max 1024 characters
  [Third-person description]. Use when [scenarios].
  Triggers on: [keywords].
---
```

### Optional Fields (When Needed)

| Field | Use When |
|-------|----------|
| `license` | Specific licensing terms or dependencies |
| `compatibility` | Platform/version constraints |
| `metadata` | Structured tooling metadata (use sparingly) |
| `allowed-tools` | Explicit tool allowlist (rarely needed) |

### ❌ Deprecated Fields

| Field | Status | Rationale |
|-------|--------|-----------|
| `dependsOn` | **REMOVED** | Use related-skill routing in body instead. See [Decision: Removal of dependsOn](#decision-removal-of-dependson) below. |

---

## Decision: Removal of dependsOn

### Context

Historically, 27 of 29 skills included a `dependsOn` field to declare dependencies on other skills.

### Decision

**Remove `dependsOn` from all skill frontmatter.** Express dependencies through explicit **related-skill routing** in the skill body instead.

### Rationale

1. **Lint Compliance**: The skill lint tool warns on `dependsOn` as a non-standard frontmatter field
2. **Portability**: Not all agent runtimes support dependency resolution through frontmatter
3. **Transparency**: Body-level routing is visible and contextual
4. **Standards Alignment**: Claude Agent Skills guidance emphasizes simple, standard frontmatter

### Replacement Pattern

Instead of:
```yaml
---
name: my-skill
dependsOn: ["other-skill"]
---
```

Use:
```markdown
## Related Skills

- For [use case], see `other-skill`
```

---

## Reference Patterns

### Local References

Use **plain relative paths** for files within the same skill:

```markdown
[Topic](./references/topic.md)
[Overview](../SKILL.md)
```

### Cross-Skill References

Use **plain skill names in backticks** for references to other skills:

```markdown
For storage format decisions, see `designing-data-storage`.

## Related Skills

- `building-data-pipelines` — For ETL construction workflows
- `accessing-cloud-storage` — For cloud storage authentication
```

### ❌ Prohibited Patterns

```markdown
<!-- Don't use hybrid notation -->
@skill-name/path.md

<!-- Don't use relative paths to other skills -->
../other-skill/SKILL.md
```

---

## File Structure

Every skill follows this layout:

```
skill-name/
├── SKILL.md              # Main entry point with workflow guidance
├── references/
│   ├── <topic>.md        # Deep-dive reference documents
│   └── ...
├── scripts/
│   ├── <utility>.py      # Validation, scaffolding, or utility scripts
│   └── ...
└── assets/               # Only if truly needed
```

### Reference Standards

1. References must be linked **directly from SKILL.md**
2. No nested reference mazes
3. No hybrid `@skill/path` notation
4. Every reference over **100 lines** must include a **table of contents**
5. Every reference must be either:
   - A substantial practical deep-dive with examples, or
   - A routing page with strong outbound links and clear guidance

---

## Migration Quick Reference

| Old Skill | New Skill |
|-----------|-----------|
| `data-engineering-core` | `building-data-pipelines` |
| `data-engineering-best-practices` | `building-data-pipelines` |
| `data-engineering-storage-authentication` | `accessing-cloud-storage` |
| `data-engineering-storage-remote-access` | `accessing-cloud-storage` |
| `data-engineering-storage-formats` | `designing-data-storage` |
| `data-engineering-storage-lakehouse` | `designing-data-storage` |
| `data-engineering-quality` | `assuring-data-pipelines` |
| `data-engineering-observability` | `assuring-data-pipelines` |
| `data-science-eda` | `analyzing-data` |
| `data-science-visualization` | `analyzing-data` |
| `flowerpower` | `using-flowerpower` |

---

## Templates

Use these templates when creating new skills:

| Template | Location | Purpose |
|----------|----------|---------|
| Skill template | [templates/skill-template.md](./templates/skill-template.md) | Main SKILL.md entry point |
| Reference template | [templates/reference-template.md](./templates/reference-template.md) | Deep-dive reference documents |

See [templates/README.md](./templates/README.md) for usage guidance.

---

## Evaluation

Each skill has evaluation manifests:

| Manifest | Location | Purpose |
|----------|----------|---------|
| Task evaluation | `eval/<skill-name>.json` | 3-5 task evaluations for output quality |
| Trigger evaluation | `eval/trigger-eval/<skill-name>.json` | 10-20 trigger evaluations for routing accuracy |

See [eval/README.md](../eval/README.md) for methodology.

---

## Lint Compliance

Run the skill linter to validate skills:

```bash
python3 tools/skill_lint.py

# CI check (treats warnings as errors)
python3 tools/skill_lint.py --strict
```

### Lint Checks

- Frontmatter has required fields (`name`, `description`)
- Name format compliance (kebab-case, length, valid characters)
- No non-standard frontmatter fields (warns on `dependsOn`)
- No broken local markdown references
- Python code blocks have valid syntax
- SKILL.md not oversized (>500 lines warning)
- No hybrid `@skill/path` notation

---

## Checklist for New Skills

Before submitting a new skill:

- [ ] Frontmatter has `name` and `description` only (or approved optional fields)
- [ ] No `dependsOn` field in frontmatter
- [ ] Skill name matches directory name
- [ ] Name follows naming conventions (action-oriented, 2-4 words, kebab-case)
- [ ] Description uses third person and includes trigger keywords
- [ ] Related skills documented with plain skill names (not hybrid notation)
- [ ] Local references use plain relative paths
- [ ] References >100 lines have a table of contents
- [ ] No broken local references
- [ ] No hybrid `@skill/path` notation
- [ ] Skill lint passes (`python3 tools/skill_lint.py`)

---

## Related Documents

- [skill-authoring.md](./skill-authoring.md) — Detailed authoring standards
- [skill-map.md](./skill-map.md) — Detailed skill boundaries and migration guidance
- [templates/README.md](./templates/README.md) — Template usage guide
- [../eval/README.md](../eval/README.md) — Evaluation methodology
- [../SKILL_REFACTORING_PLAN.md](../SKILL_REFACTORING_PLAN.md) — Original refactoring plan (historical)

---

## Changelog

### 1.0.0 (2026-03-10)

- Initial authoritative taxonomy publication
- 14-skill consolidation from 29 legacy skills
- Action-oriented naming convention formalized
- `dependsOn` removal policy enacted
- Template library published
- Evaluation framework established
