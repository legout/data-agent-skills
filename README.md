# Data Platform Agent Skills

A curated skill library for coding agents focused on data engineering, data science, and ML workflows.

## Install with `npx skills`

List all available skills:

```bash
npx skills add legout/data-platform-agent-skills --list
```

Install specific skills:

```bash
npx skills add legout/data-platform-agent-skills \
  --skill data-engineering-core \
  --skill data-science-eda
```

Install all skills:

```bash
npx skills add legout/data-platform-agent-skills --skill '*'
```

## Repository layout

- `skills/` — installable skill directories (for `npx skills` discovery)
- `data-engineering/` — data engineering source content
- `data-science/` — data science source content
- `flowerpower-skill/` — FlowerPower skill package
- `tools/` — utility scripts

## Skill categories

### Data Engineering
- Core: Polars, DuckDB, PyArrow, PostgreSQL
- Storage: Lakehouse (Delta, Iceberg, Hudi), cloud access, auth
- Orchestration: Prefect, Dagster, dbt
- Quality, Observability, Streaming

### Data Science
- EDA (Exploratory Data Analysis)
- Feature Engineering
- Model Evaluation

## Notes

This repository intentionally excludes internal development planning documents.
