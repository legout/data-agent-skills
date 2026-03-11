## Review

- What's correct
  - Implementation scope is correctly limited to `skills/data-engineering/SKILL.md`.
  - Frontmatter description was changed to include an explicit non-triggerable marker: `[DOCS ONLY - DO NOT TRIGGER]`.
  - A clear docs-only/deprecation banner was added at the top, with explicit routing to replacement skills.
  - Updated references in changed hunks are consistent with current skill names (e.g., `@building-data-pipelines`, `@accessing-cloud-storage`).
  - Change matches the ticket intent in `anchor-context.md`: convert the hub to documentation-only behavior while keeping it as an index.

- Issue [Suggestion]: Consider shortening the docs-only description further to reduce semantic trigger risk, file `skills/data-engineering/SKILL.md`, suggested fix: keep the prefix but remove most domain keywords from the description (e.g., `"[DOCS ONLY - DO NOT TRIGGER] Index hub; use linked specialized skills."`).

- Note: No functional regressions found in the reviewed changed hunks. No dangling references were identified among newly introduced skill links.

- Gate: Clear pass
