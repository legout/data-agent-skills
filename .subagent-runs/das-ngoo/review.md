## Review

- What's correct
  - `docs/TAXONOMY.md` was added as scoped in the implementation summary, and the content is consistent with ticket goals: 14-skill taxonomy, naming rules, frontmatter policy, `dependsOn` removal decision, templates/eval references, and authoring checklist.
  - Links referenced from `docs/TAXONOMY.md` resolve to existing files in `docs/` and `eval/`.
  - The documented frontmatter/lint guidance matches current linter behavior (including `dependsOn` warning behavior and allowed optional fields).

- Issue: None found in changed scope.

- Note: Observations
  - Review scope was constrained to the introduced change (`docs/TAXONOMY.md` new file) based on `implementation.md` and `git diff` for that file.
  - `anchor-context.md` was not present at the provided path; review used `implementation.md` plus repository state.

- Gate: Clear pass
