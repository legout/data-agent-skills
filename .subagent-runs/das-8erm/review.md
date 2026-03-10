## Review

- What's correct
  - `tools/skill_lint.py` now threads `strict` into `lint_markdown_references(...)`, and missing local markdown references are emitted as `error` in strict mode (`--strict`), satisfying the strict-fail requirement.
  - Hybrid `@skill/path` references are now explicitly detected and reported as errors via `ambiguous hybrid @skill/path usage: ...`.
  - Findings continue to include file attribution in output (`[{LEVEL}] <path>: <message>`), which satisfies the “point contributors at offending files” acceptance criterion.
  - Change scope is tight and limited to the ticket target (`tools/skill_lint.py`) with no unrelated logic changes.

- Note: Observations
  - `anchor-context.md` was not present at the provided path during review; review was completed using `implementation.md`, ticket acceptance criteria, and the actual `git diff` for changed hunks.

- Gate: Clear pass
