# Data Agent Skills Map

This document defines the approved 14-skill architecture for the data engineering and data science skill library.

## Overview

The skill library has been refactored from 29 skills to **14 top-level skills**, organized around **workflows** rather than taxonomic hierarchies. This consolidation eliminates duplication, clarifies trigger boundaries, and improves maintainability.

---

## The 14 Skills

### Data Engineering (9 skills)

| Skill | Purpose | Sources Merged |
|-------|---------|----------------|
| `building-data-pipelines` | Core batch ETL/dataframe/SQL patterns + production architecture rules | `data-engineering-core`, `data-engineering-best-practices` |
| `accessing-cloud-storage` | Auth + remote object storage access + library/tool integrations | `data-engineering-storage-authentication`, `data-engineering-storage-remote-access`, all remote-access library/integration skills |
| `designing-data-storage` | File formats + lakehouse table formats + storage design tradeoffs | `data-engineering-storage-formats`, `data-engineering-storage-lakehouse`, Delta/Iceberg integration details |
| `managing-data-catalogs` | Catalog architecture, metadata systems, and multi-source access patterns | `data-engineering-catalogs` |
| `orchestrating-data-pipelines` | Prefect, Dagster, dbt, scheduling, retries, deployment patterns | `data-engineering-orchestration` |
| `assuring-data-pipelines` | Data quality + observability + operational validation loops | `data-engineering-quality`, `data-engineering-observability` |
| `building-streaming-pipelines` | Kafka, MQTT, NATS JetStream, streaming architecture | `data-engineering-streaming` |
| `engineering-ai-pipelines` | Embeddings, vector stores, RAG, LLM monitoring, batch inference | `data-engineering-ai-ml` |
| `using-flowerpower` | Dedicated FlowerPower/Hamilton workflow with executable scripts | `flowerpower` |

### Data Science (5 skills)

| Skill | Purpose | Sources Merged |
|-------|---------|----------------|
| `analyzing-data` | EDA + statistical exploration + visualization selection and patterns | `data-science-eda`, `data-science-visualization` |
| `engineering-ml-features` | Feature engineering, representation choices, leakage-safe preprocessing | `data-science-feature-engineering` |
| `evaluating-ml-models` | Cross-validation, metrics, model comparison, tuning, experiment tracking | `data-science-model-evaluation` |
| `working-in-notebooks` | Jupyter/marimo/reproducible notebook workflows | `data-science-notebooks` |
| `building-data-apps` | Streamlit/Panel/Gradio/Dash/NiceGUI app-building workflows | `data-science-interactive-apps` |

---

## Naming Rules

### Rule 1: Use Action-Oriented Names

Skill names must start with a **verb** that describes what the user is doing:

| ✅ Good | ❌ Bad |
|---------|--------|
| `building-data-pipelines` | `data-engineering-core` |
| `accessing-cloud-storage` | `data-engineering-storage-remote-access` |
| `designing-data-storage` | `data-engineering-storage-lakehouse` |
| `analyzing-data` | `data-science-eda` |
| `evaluating-ml-models` | `data-science-model-evaluation` |

**Why:** Action-oriented names align with user intent and improve trigger matching.

### Rule 2: Keep Names Short

Target **2-4 words** maximum. Avoid deep taxonomic nesting.

| ✅ Good | ❌ Bad |
|---------|--------|
| `accessing-cloud-storage` | `data-engineering-storage-remote-access-integrations-polars` |
| `designing-data-storage` | `data-engineering-storage-formats-and-lakehouse` |
| `assuring-data-pipelines` | `data-engineering-quality-and-observability` |

**Why:** Short names are easier to remember, reference, and maintain.

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

---

## Adjacent Skill Boundaries

### EDA vs Visualization → `analyzing-data`

**Historical confusion:** EDA and visualization skills overlapped heavily, with duplicated references.

**Resolution:** Merged into `analyzing-data` with clear internal boundaries:

| Use `analyzing-data` when... | Don't use when... |
|------------------------------|-------------------|
| Exploring a new dataset | Building an interactive dashboard |
| Choosing the right visualization | Deploying a Streamlit app |
| Statistical profiling and data quality checks | Creating a production data app |
| Understanding distributions and correlations | Needing notebook-specific workflows |

**Trigger guidance:**
- "Profile this dataset" → `analyzing-data`
- "What viz should I use?" → `analyzing-data`
- "Build me a dashboard" → `building-data-apps`
- "Create a Streamlit app" → `building-data-apps`

---

### Quality vs Observability → `assuring-data-pipelines`

**Historical confusion:** Quality (Great Expectations, Pandera) and observability (OpenTelemetry, Prometheus) were separate but logically adjacent.

**Resolution:** Merged into `assuring-data-pipelines` with internal sections:

| Topic | Tools | Purpose |
|-------|-------|---------|
| **Data Quality** | Great Expectations, Pandera | Schema validation, data quality tests |
| **Observability** | OpenTelemetry, Prometheus, Grafana | Traces, metrics, monitoring, alerting |

**Trigger guidance:**
- "Validate my data" → `assuring-data-pipelines`
- "Check data quality" → `assuring-data-pipelines`
- "Monitor my pipeline" → `assuring-data-pipelines`
- "Add observability" → `assuring-data-pipelines`
- "Set up alerting" → `assuring-data-pipelines`

**Key distinction:** Quality = data correctness; Observability = operational visibility. Both live in the same skill because they're both "assurance" concerns that operate on running pipelines.

---

### Orchestration vs FlowerPower

**Historical confusion:** Both involve pipeline orchestration, but FlowerPower has a distinct framework workflow with executable scripts.

**Resolution:** Keep as separate skills with clear boundaries:

| Use `orchestrating-data-pipelines` when... | Use `using-flowerpower` when... |
|--------------------------------------------|----------------------------------|
| Choosing between Prefect, Dagster, or dbt | Specifically using FlowerPower/Hamilton |
| General orchestration patterns | Need FlowerPower-specific scripts |
| Scheduling and retry logic | Hamilton DAG construction |
| Deployment patterns for orchestrators | uv + Hamilton workflows |

**Trigger guidance:**
- "Should I use Prefect or Dagster?" → `orchestrating-data-pipelines`
- "How do I schedule pipelines?" → `orchestrating-data-pipelines`
- "Set up FlowerPower" → `using-flowerpower`
- "Hamilton DAG help" → `using-flowerpower`
- "FlowerPower script" → `using-flowerpower`

**Key distinction:** `orchestrating-data-pipelines` is about **choosing and using** general orchestrators; `using-flowerpower` is about **executing** a specific framework workflow with dedicated scripts.

---

## What Disappeared

| Old Skill | New State |
|-----------|-----------|
| `data-engineering` | Converted to non-triggerable documentation (this file) |
| `data-engineering-storage-remote-access-libraries-*` | Folded into `accessing-cloud-storage` references |
| `data-engineering-storage-remote-access-integrations-*` | Folded into `accessing-cloud-storage` or `designing-data-storage` references |

---

## Migration Quick Reference

| If you were using... | Now use... |
|----------------------|------------|
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

## Description Strategy

Every skill description must:

1. Be in **third person**
2. State what the skill does
3. State when it should be used
4. Include trigger language for likely user wording
5. Avoid vague "comprehensive suite" phrasing

**Example:**
```yaml
name: building-data-pipelines
description: |
  Guides users through building batch ETL pipelines using Polars, DuckDB, 
  and PyArrow. Use when constructing data transformation workflows, 
  choosing between dataframe libraries, or designing production pipeline 
  architecture. Triggers on: ETL, data pipeline, batch processing, 
  Polars, DuckDB, PyArrow, data transformation.
```

---

## File Structure

Each skill follows this layout:

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

---

## Reference Standards

1. References must be linked **directly from SKILL.md**
2. No nested reference mazes
3. No hybrid `@skill/path` notation
4. Use **plain file paths** for local references
5. Use **plain skill names** for related-skill routing
6. Every reference over **100 lines** must include a **table of contents**
7. Every reference must either:
   - Be a substantial practical deep-dive, or
   - Include enough authoritative links to be genuinely useful
