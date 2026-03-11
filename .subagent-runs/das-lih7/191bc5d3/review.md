## Review

- What's correct
  - Scope adherence: changes are limited to deleting legacy manifest files under `eval/` and `eval/trigger-eval/`.
  - Plan alignment: remaining manifest set matches the 14 target skills defined in `SKILL_REFACTORING_PLAN.md` §5.2.
  - Symmetry preserved: both directories now contain the same 14 skill manifests.
  - Acceptance criteria met: non-target manifests were removed and remaining JSON manifest count is `14 + 14 = 28`.

- Issue [Suggestion]: None in changed scope.

- Note: Observations
  - `anchor-context.md` expected cleanup-only work; implementation matches that intent.
  - No content edits were made to retained manifests, reducing regression risk.

- Gate: Clear pass
