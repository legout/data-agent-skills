# Data Platform Agent Skills

A curated skill library for coding agents focused on data engineering, data science, and interactive data applications.

## Install with `npx skills`

```bash
# List all available skills
npx skills add legout/data-platform-agent-skills --list

# Install specific skills
npx skills add legout/data-platform-agent-skills \
  --skill analyzing-data \
  --skill building-data-pipelines \
  --skill designing-data-storage

# Install all skills
npx skills add legout/data-platform-agent-skills --all
```

### ⚠️ Migration Warning (Breaking Change)

If you previously installed skills before March 2025, **you must remove the old structure first** to avoid conflicts:

```bash
# Remove old skill folders (if they exist)
rm -rf ~/.pi/agent/skills/data-engineering/
rm -rf ~/.pi/agent/skills/data-engineering-core/
rm -rf ~/.pi/agent/skills/data-engineering-best-practices/
rm -rf ~/.pi/agent/skills/data-engineering-orchestration/
rm -rf ~/.pi/agent/skills/data-engineering-quality/
rm -rf ~/.pi/agent/skills/data-engineering-observability/
rm -rf ~/.pi/agent/skills/data-engineering-ai-ml/
rm -rf ~/.pi/agent/skills/data-engineering-catalogs/
rm -rf ~/.pi/agent/skills/data-engineering-storage-remote-access/
rm -rf ~/.pi/agent/skills/data-science/
rm -rf ~/.pi/agent/skills/data-science-eda/
rm -rf ~/.pi/agent/skills/data-science-feature-engineering/
rm -rf ~/.pi/agent/skills/data-science-interactive-apps/
rm -rf ~/.pi/agent/skills/data-science-model-evaluation/
rm -rf ~/.pi/agent/skills/data-science-notebooks/

# Now install fresh with npx skills
npx skills add legout/data-platform-agent-skills --all
```

**Why this happens:** We consolidated 21 legacy skills into 14 focused skills with clearer naming (e.g., `data-engineering-core` → `building-data-pipelines`).

---

## Repository Structure

```
data-platform-agent-skills/
├── skills/                    # All skills (14 focused skills)
│   ├── accessing-cloud-storage/
│   ├── analyzing-data/
│   ├── assuring-data-pipelines/
│   ├── building-data-apps/
│   ├── building-data-pipelines/
│   ├── building-streaming-pipelines/
│   ├── designing-data-storage/
│   ├── engineering-ai-pipelines/
│   ├── engineering-ml-features/
│   ├── evaluating-ml-models/
│   ├── flowerpower/
│   ├── managing-data-catalogs/
│   ├── orchestrating-data-pipelines/
│   └── working-in-notebooks/
├── tools/                     # Development utilities
│   └── skill_lint.py         # Lint skills for correctness
└── README.md
```

---

## Development Workflow

### Edit skills directly

```bash
# Edit a skill
skills/analyzing-data/SKILL.md

# Lint before committing
python3 tools/skill_lint.py

# Test locally
npx skills add . --list
npx skills add . --skill analyzing-data

# Commit and push
git add skills/analyzing-data/
git commit -m "Update analyzing-data skill"
git push
```

### Lint skills before committing

```bash
python3 tools/skill_lint.py
```

Checks:
- Frontmatter validity
- Python code syntax in fenced blocks
- Reference file existence
- SKILL.md line count (<500 recommended)

---

## Skill Categories (14 Focused Skills)

### Data Platform (10 skills)
- **accessing-cloud-storage** — S3, GCS, Azure storage with fsspec, pyarrow.fs, obstore
- **analyzing-data** — Exploratory data analysis, profiling, visualization
- **assuring-data-pipelines** — Data quality, observability, validation (Great Expectations, OpenTelemetry)
- **building-data-apps** — Streamlit, Panel, Gradio, Dash interactive applications
- **building-data-pipelines** — Core batch ETL with Polars, DuckDB, PyArrow
- **building-streaming-pipelines** — Kafka, MQTT, NATS streaming data
- **designing-data-storage** — Lakehouse (Delta, Iceberg), formats, partitioning
- **engineering-ai-pipelines** — Embeddings, vector stores, RAG, LLM monitoring
- **managing-data-catalogs** — Metadata systems, Glue, Tabular, REST catalogs
- **orchestrating-data-pipelines** — Prefect, Dagster, dbt workflow orchestration

### Data Science / ML (3 skills)
- **engineering-ml-features** — Feature engineering, preprocessing, selection
- **evaluating-ml-models** — Cross-validation, metrics, hyperparameter tuning
- **working-in-notebooks** — Jupyter, JupyterLab, marimo workflows

### Pipeline Framework (1 skill)
- **flowerpower** — Hamilton DAG-based pipeline framework

---

## Adding New Skills

1. Create skill directory under `skills/`:
   ```bash
   mkdir skills/my-new-skill
   ```

2. Create SKILL.md with frontmatter:
   ```yaml
   ---
   name: my-new-skill
   description: "Clear description of what this skill does"
   dependsOn: ["@data-engineering-core"]
   ---
   ```

3. Keep SKILL.md concise (<500 lines)

4. Run `python3 tools/skill_lint.py` to validate

5. Test locally: `npx skills add . --list`

---

## Notes

- All skills live directly under `skills/` in a flat structure
- Internal development docs (`ARCHITECTURE_DECISIONS.md`, `INTEGRATION_SUMMARY.md`) are excluded from repo
- All Python code in skills is validated for syntax correctness
