## Fixes Applied

- Fixed [Major]: Skill created in wrong location in `skills/orchestrating-data-pipelines/` — Moved entire skill directory from `.pi/agent/skills/orchestrating-data-pipelines/` to `skills/orchestrating-data-pipelines/` to match the repo's canonical skill location.

## Skipped

- Skipped [Minor]: Lint warnings about "ambiguous hybrid @skill/path usage" — These are pre-existing patterns used consistently across ALL skills in this repository (data-engineering-orchestration, data-engineering-ai-ml, etc.). A repo-wide refactoring would be needed to change this convention, which is out of scope for this fix pass.

## Status

All critical and major issues resolved. 1 minor issue skipped (pre-existing pattern).

## Files Moved

| Source | Destination |
|--------|-------------|
| `.pi/agent/skills/orchestrating-data-pipelines/SKILL.md` | `skills/orchestrating-data-pipelines/SKILL.md` |
| `.pi/agent/skills/orchestrating-data-pipelines/prefect.md` | `skills/orchestrating-data-pipelines/prefect.md` |
| `.pi/agent/skills/orchestrating-data-pipelines/dagster.md` | `skills/orchestrating-data-pipelines/dagster.md` |
| `.pi/agent/skills/orchestrating-data-pipelines/dbt.md` | `skills/orchestrating-data-pipelines/dbt.md` |
| `.pi/agent/skills/orchestrating-data-pipelines/integrations/cloud-storage.md` | `skills/orchestrating-data-pipelines/integrations/cloud-storage.md` |
