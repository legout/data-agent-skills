# Data Platform Agent Skills

A curated skill library for coding agents focused on data engineering, data science, and interactive data applications.

## Install with `npx skills`

**From GitHub (recommended):**

```bash
# List all available skills
npx skills add legout/data-platform-agent-skills --list

# Install specific skills
npx skills add legout/data-platform-agent-skills \
  --skill data-engineering-core \
  --skill data-science-eda \
  --skill data-science-visualization

# Install all skills
npx skills add legout/data-platform-agent-skills --skill '*'
```

**From local clone:**

```bash
git clone https://github.com/legout/data-platform-agent-skills.git
cd data-platform-agent-skills
python3 scripts/build_skills.py  # Generate skills/ directory
npx skills add . --skill data-science-visualization
```

---

## Repository Structure

```
data-platform-agent-skills/
├── data-engineering/          # Source: 23 data engineering skills
│   ├── core/
│   ├── storage/
│   ├── orchestration/
│   └── ...
├── data-science/              # Source: 8 data science skills
│   ├── eda/
│   ├── visualization/
│   ├── notebooks/
│   └── ...
├── flowerpower-skill/         # Source: FlowerPower framework
├── tools/                     # Development utilities
│   ├── skill_lint.py         # Lint skills for correctness
│   └── build_skills.py       # Generate skills/ for npx
└── skills/                    # GENERATED (gitignored)
                               # Flat installable skill packages
```

**Key principle:** Source directories (`data-engineering/`, `data-science/`, `flowerpower-skill/`) are authoritative. The `skills/` directory is a **generated build artifact** for `npx skills` compatibility.

---

## Development Workflow

### Edit source, not `skills/`

```bash
# Edit source skill
data-science/visualization/SKILL.md

# Regenerate skills/ for testing
python3 scripts/build_skills.py

# Test locally
npx skills add . --list

# Commit and push
git add data-science/visualization/
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

1. Create skill directory in appropriate source location:
   - Data engineering: `data-engineering/<skill-name>/SKILL.md`
   - Data science: `data-science/<skill-name>/SKILL.md`

2. Follow frontmatter format:
   ```yaml
   ---
   name: data-science-my-skill
   description: "Clear description of what this skill does"
   dependsOn: ["@data-science-eda"]
   ---
   ```

3. Keep SKILL.md concise (<500 lines), link to `references/` for details

4. Run `python3 tools/skill_lint.py` to validate

5. Build and test: `python3 scripts/build_skills.py && npx skills add . --list`

---

## Notes

- `skills/` directory is gitignored — never edit directly
- Internal development docs (`ARCHITECTURE_DECISIONS.md`, `INTEGRATION_SUMMARY.md`) are excluded from repo
- All Python code in skills is validated for syntax correctness
