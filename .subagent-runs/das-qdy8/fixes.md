## Fixes Applied

- Fixed [Major]: Broken relative paths in `skills/data-science-eda/SKILL.md` — Changed `../../analyzing-data/references/...` to `../analyzing-data/references/...` (4 references)
- Fixed [Major]: Broken relative paths in `skills/data-science-feature-engineering/SKILL.md` — Changed `../../analyzing-data/references/...` to `../analyzing-data/references/...` (4 references)
- Fixed [Major]: Broken relative paths in `skills/data-science-model-evaluation/SKILL.md` — Changed `../../analyzing-data/references/...` to `../analyzing-data/references/...` (4 references)
- Fixed [Major]: Broken relative paths in `skills/data-science-notebooks/SKILL.md` — Changed `../../analyzing-data/references/...` to `../analyzing-data/references/...` (2 references)

## Status
All critical and major issues resolved. 0 minor/suggestions skipped.

## Verification
- All 14 progressive disclosure paths now correctly resolve to `skills/analyzing-data/references/`
- Target reference files confirmed to exist in the shared directory
- Path pattern: from `skills/<skill>/SKILL.md`, the correct relative path to `skills/analyzing-data/references/` is `../analyzing-data/references/` (one level up, not two)
