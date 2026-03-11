## Review

- What's correct
  - The implementation correctly centralizes progressive-disclosure references toward `skills/analyzing-data/references/` and removes duplicated per-skill `references/` directories from the 6 targeted data-science skills.
  - Target reference files used in updated links do exist under `skills/analyzing-data/references/` (e.g., `profiling-automation.md`, `metrics-guide.md`, `streamlit-advanced.md`, etc.).
  - `data-science-visualization/SKILL.md` and `data-science-interactive-apps/SKILL.md` use correct relative paths (`../analyzing-data/references/...`).

- Issue [Major]: Broken relative paths in 4 SKILL.md files (`../../analyzing-data/...` resolves outside `skills/`), causing progressive-disclosure links to point to non-existent files.
  - File: `skills/data-science-eda/SKILL.md`
  - File: `skills/data-science-feature-engineering/SKILL.md`
  - File: `skills/data-science-model-evaluation/SKILL.md`
  - File: `skills/data-science-notebooks/SKILL.md`
  - Suggested fix: Replace `../../analyzing-data/references/...` with `../analyzing-data/references/...` in all four files. From each skill directory (`skills/<skill>/`), the correct relative path to `skills/analyzing-data/references/` is one level up, not two.

- Note: Observations
  - This issue is ticket-blocking because it introduces broken documentation links in skill progressive disclosure and would be caught by the referenced lint validation (`tools/skill_lint.py`).
  - No additional regressions were identified within the changed ticket scope.

- Gate: Fail
