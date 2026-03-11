# Skill Migration Map

This document maps old skill names to their new equivalents following the architecture refactor from 29 skills to 14 workflow-centered skills.

## Overview

The skill library was refactored to:
- **Reduce skill count** from 29 to 14 top-level skills
- **Eliminate duplicate content** across data-science skills
- **Adopt action-oriented names** that reflect user intent
- **Group by workflow** rather than arbitrary taxonomy depth

---

## Data Engineering Migrations

| Old Skill Name | New Skill Name | Notes |
|----------------|----------------|-------|
| `data-engineering` | *See [Skill Map](./skill-map.md)* | Converted to non-triggerable documentation |
| `data-engineering-core` | `building-data-pipelines` | Merged with best-practices |
| `data-engineering-best-practices` | `building-data-pipelines` | Merged into pipelines skill |
| `data-engineering-storage-authentication` | `accessing-cloud-storage` | Auth + access unified |
| `data-engineering-storage-remote-access` | `accessing-cloud-storage` | Consolidated with auth |
| `data-engineering-storage-remote-access-libraries-fsspec` | `accessing-cloud-storage` | Library reference |
| `data-engineering-storage-remote-access-libraries-pyarrow-fs` | `accessing-cloud-storage` | Library reference |
| `data-engineering-storage-remote-access-libraries-obstore` | `accessing-cloud-storage` | Library reference |
| `data-engineering-storage-remote-access-integrations-polars` | `accessing-cloud-storage` | Integration pattern |
| `data-engineering-storage-remote-access-integrations-duckdb` | `accessing-cloud-storage` | Integration pattern |
| `data-engineering-storage-remote-access-integrations-pandas` | `accessing-cloud-storage` | Integration pattern |
| `data-engineering-storage-remote-access-integrations-pyarrow` | `accessing-cloud-storage` | Integration pattern |
| `data-engineering-storage-remote-access-integrations-delta-lake` | `designing-data-storage` | Lakehouse content |
| `data-engineering-storage-remote-access-integrations-iceberg` | `designing-data-storage` | Lakehouse content |
| `data-engineering-storage-formats` | `designing-data-storage` | Merged with lakehouse |
| `data-engineering-storage-lakehouse` | `designing-data-storage` | Merged with formats |
| `data-engineering-catalogs` | `managing-data-catalogs` | Renamed for clarity |
| `data-engineering-orchestration` | `orchestrating-data-pipelines` | Action-oriented rename |
| `data-engineering-quality` | `assuring-data-pipelines` | Merged with observability |
| `data-engineering-observability` | `assuring-data-pipelines` | Merged with quality |
| `data-engineering-streaming` | `building-streaming-pipelines` | Action-oriented rename |
| `data-engineering-ai-ml` | `engineering-ai-pipelines` | Action-oriented rename |
| `flowerpower` | `using-flowerpower` | Action-oriented rename |

---

## Data Science Migrations

| Old Skill Name | New Skill Name | Notes |
|----------------|----------------|-------|
| `data-science-eda` | `analyzing-data` | Merged with visualization |
| `data-science-visualization` | `analyzing-data` | Merged into analysis skill |
| `data-science-feature-engineering` | `engineering-ml-features` | Action-oriented rename |
| `data-science-model-evaluation` | `evaluating-ml-models` | Action-oriented rename |
| `data-science-notebooks` | `working-in-notebooks` | Action-oriented rename |
| `data-science-interactive-apps` | `building-data-apps` | Action-oriented rename |

---

## New Skill Summaries

| New Skill | Purpose | Covers |
|-----------|---------|--------|
| `building-data-pipelines` | Core batch ETL/dataframe/SQL patterns | Polars, DuckDB, PyArrow, PostgreSQL, production architecture |
| `accessing-cloud-storage` | Auth + remote object storage access | AWS/GCP/Azure auth, fsspec, pyarrow.fs, obstore, library integrations |
| `designing-data-storage` | File formats + lakehouse table formats | Parquet, Arrow, Delta Lake, Iceberg, Hudi, format tradeoffs |
| `managing-data-catalogs` | Catalog architecture and metadata | Hive, Glue, REST catalogs, Amundsen/DataHub/OpenMetadata |
| `orchestrating-data-pipelines` | Workflow orchestration | Prefect, Dagster, dbt, scheduling, retries, deployment |
| `assuring-data-pipelines` | Quality + observability | Great Expectations, Pandera, OpenTelemetry, Prometheus |
| `building-streaming-pipelines` | Real-time data pipelines | Kafka, MQTT, NATS JetStream |
| `engineering-ai-pipelines` | AI/ML data workflows | Embeddings, vector stores, RAG, LLM monitoring |
| `using-flowerpower` | FlowerPower/Hamilton framework | DAG-based pipelines with Hamilton |
| `analyzing-data` | EDA + visualization | Profiling, statistical tests, Matplotlib, Seaborn, Plotly, Altair |
| `engineering-ml-features` | ML feature preparation | Encoding, scaling, datetime/text features, feature selection |
| `evaluating-ml-models` | Model validation | Cross-validation, metrics, tuning, experiment tracking |
| `working-in-notebooks` | Notebook workflows | Jupyter, JupyterLab, marimo, reproducibility |
| `building-data-apps` | Interactive data applications | Streamlit, Panel, Gradio, Dash, NiceGUI |

---

## Migration Checklist

When updating from old skills:

- [ ] Remove old skill installations: `rm -rf ~/.pi/agent/skills/{old-skill-name}/`
- [ ] Install new skill: `npx skills add legout/data-platform-agent-skills --skill {new-skill-name}`
- [ ] Update any `@old-skill-name` references in your prompts
- [ ] Verify new skill triggers correctly for your use cases

---

## Related Documentation

- [Skill Map](./skill-map.md) — Current skill taxonomy and decision guide
- [Skill Authoring Guide](./skill-authoring.md) — Standards for skill development
- [TAXONOMY.md](./TAXONOMY.md) — Detailed framework/tool disposition matrix
- [CHANGELOG.md](../CHANGELOG.md) — Full refactor changelog
- [CONTRIBUTING.md](../CONTRIBUTING.md) — Contributor guidelines
