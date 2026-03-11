## Review

- What's correct
  - `docs/migration-map.md` provides a complete old→new mapping aligned with the refactor plan (data engineering + data science), plus actionable migration steps.
  - `CONTRIBUTING.md` clearly covers the required contributor topics: templates, eval expectations, and lint workflow/checks.
  - Cross-linking is good overall (migration map, taxonomy, authoring guide, templates, changelog, contributing), avoiding unnecessary duplication.

- Issue [Major]: `CHANGELOG.md` has an internal inconsistency in breaking-change accounting and omits several renamed/removed old skill names in the “Removed Skills (29 → 14)” section. It states “Data Engineering (15 removed)” and additionally “Data Science (6 removed)”, which does not reconcile with a 29→14 reduction, and it misses old names like `data-engineering-catalogs`, `data-engineering-orchestration`, `data-engineering-streaming`, `data-engineering-ai-ml`, and `flowerpower`.  
  - File: `CHANGELOG.md`  
  - Suggested fix: Rework the breaking-changes section to be numerically consistent and exhaustive. Prefer a canonical table (or link directly to `docs/migration-map.md`) containing all old names and their new destinations.

- Issue [Major]: `CHANGELOG.md` presents several claims as completed facts that are not true in the current repo state (e.g., “Removed `dependsOn` field from all skill frontmatter”, “Each skill includes trigger evaluations”, broad “after” completion claims). This can mislead users during migration.
  - File: `CHANGELOG.md`
  - Suggested fix: Either (a) change wording to future/target-state phrasing, or (b) scope claims to what is actually delivered in this release and defer unfinished items to a “Planned/Upcoming” section.

- Issue [Minor]: `CONTRIBUTING.md` is inconsistent on trigger evals: it says trigger evaluations are “optional but recommended” in structure guidance, but the submission checklist requires 10–20 trigger evals unconditionally.
  - File: `CONTRIBUTING.md`
  - Suggested fix: Make policy explicit and consistent (required vs recommended), including any exceptions.

- Note: Observations
  - `anchor-context.md` at the requested run path was not present; review used `implementation.md`, ticket acceptance criteria, and the created files directly.
  - No code/security regressions observed in scope (docs-only change).

- Gate: Fail
