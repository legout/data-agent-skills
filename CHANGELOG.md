# Changelog

All notable changes to the data platform agent skills library.

## [2.0.0] - Skill Architecture Refactor

### Overview

Major refactor reducing the skill library from **29 skills to 14 workflow-centered skills**. This is a breaking change that restructures the entire taxonomy around user intent rather than tool categories.

### Goals Achieved

- **Reduced cognitive load**: Fewer, more focused skills
- **Eliminated duplication**: Removed ~4,060 duplicate lines across data-science references
- **Action-oriented naming**: Skills now start with verbs (`building-`, `accessing-`, `evaluating-`)
- **Workflow-centered grouping**: Topics grouped by what users do, not by tool taxonomy
- **Improved trigger boundaries**: Clearer distinction between similar skills

---

### Breaking Changes

#### Removed Skills (29 → 14)

The following skills have been removed and consolidated. For the complete mapping, see [docs/migration-map.md](docs/migration-map.md).

**Data Engineering (23 skills → 9 new skills):**

| Old Skill | New Destination |
|-----------|-----------------|
| `data-engineering` | Documentation-only at [docs/skill-map.md](docs/skill-map.md) |
| `data-engineering-core` | `building-data-pipelines` |
| `data-engineering-best-practices` | `building-data-pipelines` |
| `data-engineering-storage-authentication` | `accessing-cloud-storage` |
| `data-engineering-storage-remote-access` | `accessing-cloud-storage` |
| `data-engineering-storage-remote-access-libraries-fsspec` | `accessing-cloud-storage` |
| `data-engineering-storage-remote-access-libraries-pyarrow-fs` | `accessing-cloud-storage` |
| `data-engineering-storage-remote-access-libraries-obstore` | `accessing-cloud-storage` |
| `data-engineering-storage-remote-access-integrations-polars` | `accessing-cloud-storage` |
| `data-engineering-storage-remote-access-integrations-duckdb` | `accessing-cloud-storage` |
| `data-engineering-storage-remote-access-integrations-pandas` | `accessing-cloud-storage` |
| `data-engineering-storage-remote-access-integrations-pyarrow` | `accessing-cloud-storage` |
| `data-engineering-storage-remote-access-integrations-delta-lake` | `designing-data-storage` |
| `data-engineering-storage-remote-access-integrations-iceberg` | `designing-data-storage` |
| `data-engineering-storage-formats` | `designing-data-storage` |
| `data-engineering-storage-lakehouse` | `designing-data-storage` |
| `data-engineering-catalogs` | `managing-data-catalogs` |
| `data-engineering-orchestration` | `orchestrating-data-pipelines` |
| `data-engineering-quality` | `assuring-data-pipelines` |
| `data-engineering-observability` | `assuring-data-pipelines` |
| `data-engineering-streaming` | `building-streaming-pipelines` |
| `data-engineering-ai-ml` | `engineering-ai-pipelines` |
| `flowerpower` | `using-flowerpower` |

**Data Science (6 skills → 5 new skills):**

| Old Skill | New Destination |
|-----------|-----------------|
| `data-science-eda` | `analyzing-data` |
| `data-science-visualization` | `analyzing-data` |
| `data-science-feature-engineering` | `engineering-ml-features` |
| `data-science-model-evaluation` | `evaluating-ml-models` |
| `data-science-notebooks` | `working-in-notebooks` |
| `data-science-interactive-apps` | `building-data-apps` |

See [docs/migration-map.md](docs/migration-map.md) for the complete mapping with notes.

#### Frontmatter Changes (Planned)

- **Planned**: `dependsOn` field removal from skill frontmatter
- **Rationale**: Dependencies will be expressed through explicit related-skill routing in skill bodies
- **Impact**: Skills with external dependency resolution may need updates

---

### New Skills

| Skill | Description |
|-------|-------------|
| `building-data-pipelines` | Batch ETL with Polars, DuckDB, PyArrow |
| `accessing-cloud-storage` | Cloud auth and storage access patterns |
| `designing-data-storage` | File formats and lakehouse table formats |
| `managing-data-catalogs` | Data catalog architecture and metadata |
| `orchestrating-data-pipelines` | Prefect, Dagster, dbt workflows |
| `assuring-data-pipelines` | Quality + observability combined |
| `building-streaming-pipelines` | Kafka, MQTT, NATS JetStream |
| `engineering-ai-pipelines` | Embeddings, vectors, RAG, LLM monitoring |
| `using-flowerpower` | FlowerPower/Hamilton DAG framework |
| `analyzing-data` | EDA + visualization unified |
| `engineering-ml-features` | ML feature engineering |
| `evaluating-ml-models` | Model validation and tuning |
| `working-in-notebooks` | Jupyter, marimo workflows |
| `building-data-apps` | Streamlit, Panel, Gradio apps |

---

### Structural Improvements

#### Reference Consolidation

- **Before**: 183 markdown files with 21 duplicate content groups
- **After**: Consolidated references, zero intentional duplication
- **Removed**: ~105 redundant file copies

#### Standardized Layout

All skills now follow:

```
skill-name/
├── SKILL.md              # Main entry point
├── references/           # Deep-dive documents
├── scripts/              # Validation and scaffolding
└── assets/               # Only when needed
```

#### Reference Quality Standards

- Every reference >100 lines must include a Table of Contents
- No 30–50 line stub references (expand, merge, or delete)
- Direct linking from SKILL.md (no nested reference mazes)
- No hybrid `@skill/path` notation

---

### Lint and Validation

#### Enhanced Lint Checks

The `tools/skill_lint.py` now checks:

1. **Missing local references** — Error in strict mode
2. **Duplicate content blocks** — Warning for 3+ files with >100 identical lines
3. **Hybrid notation** — Error on `@skill/path` patterns
4. **TOC required** — Warning for references >100 lines without Table of Contents
5. **Stale year markers** — Warning on `(YYYY)` in headings

#### Evaluation Infrastructure (Planned)

- New `evals/` directory with skill evaluation manifests
- Trigger evaluations (positive + negative prompts) — recommended
- Task evaluations for output quality verification

---

### Documentation Additions

| Document | Purpose |
|----------|---------|
| [docs/migration-map.md](docs/migration-map.md) | Old-to-new skill mapping |
| [docs/TAXONOMY.md](docs/TAXONOMY.md) | Framework/tool disposition matrix |
| [docs/skill-authoring.md](docs/skill-authoring.md) | Authoring standards |
| [docs/templates/skill-template.md](docs/templates/skill-template.md) | SKILL.md template |
| [docs/templates/reference-template.md](docs/templates/reference-template.md) | Reference doc template |
| [CONTRIBUTING.md](CONTRIBUTING.md) | Contributor guidelines |

---

### Migration Guide

1. **List installed skills**:
   ```bash
   ls ~/.pi/agent/skills/
   ```

2. **Remove old skills**:
   ```bash
   rm -rf ~/.pi/agent/skills/data-engineering/
   rm -rf ~/.pi/agent/skills/data-engineering-core/
   # ... etc
   ```

3. **Install new skills**:
   ```bash
   npx skills add legout/data-platform-agent-skills --all
   # Or selectively:
   npx skills add legout/data-platform-agent-skills --skill building-data-pipelines
   ```

4. **Verify migration**:
   ```bash
   python3 tools/skill_lint.py --strict
   ```

See [docs/migration-map.md](docs/migration-map.md) for the complete old-to-new mapping.

---

## [1.0.0] - Initial Release

### Added

- 29 skills covering data engineering, data science, and pipeline frameworks
- Core data engineering: Polars, DuckDB, PyArrow, PostgreSQL
- Storage: Lakehouse formats (Delta, Iceberg, Hudi), cloud access, auth
- Orchestration: Prefect, Dagster, dbt
- Data science: EDA, visualization, feature engineering, model evaluation
- Notebooks: Jupyter, marimo
- Interactive apps: Streamlit, Panel, Gradio
- Pipeline framework: FlowerPower/Hamilton
- Basic lint tool: `tools/skill_lint.py`

---

## Versioning Notes

This project follows [Semantic Versioning](https://semver.org/):

- **MAJOR**: Breaking changes (skill removals, renames, frontmatter changes)
- **MINOR**: New skills, significant feature additions
- **PATCH**: Bug fixes, reference updates, documentation improvements

### Upgrade Compatibility

| From | To | Action Required |
|------|-----|-----------------|
| 1.x | 2.x | Full migration required; see [docs/migration-map.md](docs/migration-map.md) |
