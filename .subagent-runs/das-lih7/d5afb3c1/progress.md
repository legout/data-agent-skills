# Progress

## Status
Completed

## Tasks
- [x] Created `eval/` directory
- [x] Created `eval/trigger-eval/` subdirectory
- [x] Created task evaluation JSON manifests for all 29 skills
- [x] Created trigger evaluation JSON manifests for all 29 skills
- [x] Created `eval/README.md` with manifest format documentation

## Files Changed
- `eval/README.md` - Documentation for the manifest format and contribution guidelines
- `eval/*.json` - 29 task evaluation manifest files (one per skill)
- `eval/trigger-eval/*.json` - 29 trigger evaluation manifest files (one per skill)

## Notes

### Manifest Structure

Each skill has two manifest files:

1. **Task Evaluation Manifest** (`eval/{skill-name}.json`):
   - Contains 2-3 task evaluations per skill
   - Fields: `skill_name`, `task_evaluations` (array with `id`, `name`, `description`, `prompt`, `expected_behavior`, `success_criteria`, `tags`)

2. **Trigger Evaluation Manifest** (`eval/trigger-eval/{skill-name}.json`):
   - Contains 5-7 trigger evaluations per skill (positive, negative, and near-miss cases)
   - Fields: `skill_name`, `trigger_evaluations` (array with `id`, `prompt`, `expected_trigger`, `rationale`, `category`)

### Skills Covered

All 29 skills from the `skills/` directory have corresponding evaluation manifests:

**Data Engineering (22 skills):**
- data-engineering
- data-engineering-ai-ml
- data-engineering-best-practices
- data-engineering-catalogs
- data-engineering-core
- data-engineering-observability
- data-engineering-orchestration
- data-engineering-quality
- data-engineering-storage-authentication
- data-engineering-storage-formats
- data-engineering-storage-lakehouse
- data-engineering-storage-remote-access
- data-engineering-storage-remote-access-integrations-delta-lake
- data-engineering-storage-remote-access-integrations-duckdb
- data-engineering-storage-remote-access-integrations-iceberg
- data-engineering-storage-remote-access-integrations-pandas
- data-engineering-storage-remote-access-integrations-polars
- data-engineering-storage-remote-access-integrations-pyarrow
- data-engineering-storage-remote-access-libraries-fsspec
- data-engineering-storage-remote-access-libraries-obstore
- data-engineering-storage-remote-access-libraries-pyarrow-fs
- data-engineering-streaming

**Data Science (6 skills):**
- data-science-eda
- data-science-feature-engineering
- data-science-interactive-apps
- data-science-model-evaluation
- data-science-notebooks
- data-science-visualization

**FlowerPower (1 skill):**
- flowerpower

### Contributor Guidelines

Contributors should place new skill eval files at:
- Task evaluations: `eval/{skill-name}.json`
- Trigger evaluations: `eval/trigger-eval/{skill-name}.json`

See `eval/README.md` for the complete format specification.
