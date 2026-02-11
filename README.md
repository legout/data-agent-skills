# Data Platform Agent Skills

A curated skill library for coding agents focused on data engineering, data science, and interactive data applications.

## Install with `npx skills`

```bash
# List all available skills
npx skills add legout/data-platform-agent-skills --list

# Install specific skills
npx skills add legout/data-platform-agent-skills \
  --skill data-engineering-core \
  --skill data-science-eda \
  --skill data-science-visualization

# Install all skills
npx skills add legout/data-platform-agent-skills --all
```

### ⚠️ Migration Warning (Breaking Change)

If you previously installed skills manually (symlinked or copied the nested `data-engineering/` directory), **you must remove the old structure first** to avoid conflicts:

```bash
# Remove old nested skill structure (if exists)
rm -rf ~/.pi/agent/skills/data-engineering/
rm -rf ~/.pi/agent/skills/data-science/
rm -rf ~/.pi/agent/skills/flowerpower-skill/

# Now install fresh with npx skills
npx skills add legout/data-platform-agent-skills --all
```

**Why this happens:** The old structure had skills nested (e.g., `data-engineering/core/`). The new structure uses flat names (`data-engineering-core/`). Both have the same skill `name:` in frontmatter, causing pi to detect conflicts.

---

## Repository Structure

```
data-platform-agent-skills/
├── skills/                    # All skills (flat structure)
│   ├── data-engineering-core/
│   ├── data-engineering-storage-lakehouse/
│   ├── data-science-eda/
│   ├── data-science-visualization/
│   ├── flowerpower/
│   └── ...
├── tools/                     # Development utilities
│   └── skill_lint.py         # Lint skills for correctness
└── README.md
```

---

## Development Workflow

### Edit skills directly

```bash
# Edit a skill
skills/data-science-visualization/SKILL.md

# Lint before committing
python3 tools/skill_lint.py

# Test locally
npx skills add . --list
npx skills add . --skill data-science-visualization

# Commit and push
git add skills/data-science-visualization/
git commit -m "Update visualization skill"
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

## Skill Categories

### Data Engineering (23 skills)
- **Core**: Polars, DuckDB, PyArrow, PostgreSQL
- **Storage**: Lakehouse (Delta, Iceberg, Hudi), cloud access, auth
- **Orchestration**: Prefect, Dagster, dbt
- **Quality, Observability, Streaming**

### Data Science (8 skills)
- **EDA** — Exploratory Data Analysis
- **Visualization** — Matplotlib, Seaborn, Plotly, Altair, HoloViz, Bokeh
- **Feature Engineering** — ML feature preparation
- **Model Evaluation** — Validation and tuning
- **Notebooks** — Jupyter, marimo
- **Interactive Apps** — Streamlit, Panel, Gradio

### Pipeline Framework (1 skill)
- **FlowerPower** — Hamilton DAG-based pipelines

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
