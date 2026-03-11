## Review

- What's correct
  - Implementation scope is minimal and aligned with the ticket: only `tools/skill_lint.py` was changed in product code.
  - The change correctly updates `IGNORE_PARTS` to skip transient directories (`.subagent-runs`, `.tf`) during file traversal.
  - This prevents strict lint noise from ephemeral ticket/subagent artifacts and keeps lint focused on repository content that should be enforced.
  - No regressions found in the touched hunk: filtering logic in `iter_files()` remains consistent and the new entries follow the existing ignore mechanism.

- Issue [Suggestion]: Add a short inline comment near `IGNORE_PARTS` documenting why `.subagent-runs` and `.tf` are intentionally excluded, to prevent accidental removal in future cleanup.
  - File: `tools/skill_lint.py`
  - Suggested fix: add a one-line comment such as “exclude transient agent/ticket working directories from lint scope”.

- Note: Observations
  - `anchor-context.md` was referenced in the task input but not present at the provided path; review was performed using `implementation.md` and the actual diff for changed code.
  - Review constrained to implementation-touched scope per instruction.

- Gate: Clear pass
