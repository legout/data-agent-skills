## Review

- What's correct
  - Plan scope is largely implemented: new `skills/evaluating-ml-models/` skill exists with a full `SKILL.md`, local `references/` docs, and `evals/evaluating-ml-models.json`.
  - The 4 reference files were copied correctly from `skills/analyzing-data/references/` (content matches).
  - `skills/data-science-model-evaluation/SKILL.md` was updated with a clear deprecation notice and now routes users to `@evaluating-ml-models`, removing the old path-based progressive disclosure links that could become stale.
  - Trigger eval coverage is good (20 total with positive + negative routing), and negative cases separate this skill from `engineering-ml-features` and `analyzing-data`.

- Issue [Minor]: Incorrect dependency wording in related skills section, file: `skills/evaluating-ml-models/SKILL.md`
  - Description: In `## Related skills`, the bullet says `engineering-ml-features — Features to evaluate (depends on this skill)`, but this is inverted. The actual dependency is the opposite (`evaluating-ml-models` depends on `@engineering-ml-features` in frontmatter).
  - Suggested fix: Reword to avoid incorrect dependency direction, e.g. `engineering-ml-features — Upstream feature engineering before evaluation`.

- Note: Observations
  - `skills/analyzing-data/SKILL.md` did not need modification for this ticket because it does not reference the moved evaluation guides.
  - No security concerns identified in the changed scope (docs/evals only).

- Gate: Clear pass
