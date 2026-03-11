## Review

- What's correct
  - `skills/analyzing-data/SKILL.md` is the correct file to update for this ticket.
  - The intended replacement targets are clear from anchor context and current skill inventory.

- Issue [Major]: Implementation artifact missing, so changed scope cannot be verified from the requested source of truth, file: `.subagent-runs/das-jg7i/2f6694e5/implementation.md`, suggested fix: write the implementation summary with touched files/hunks so review scope is auditable.

- Issue [Major]: Broken related-skill references are still present (acceptance criterion "no broken refs" not met), file: `skills/analyzing-data/SKILL.md`, suggested fix:
  - Replace `building-data-apps` → `data-science-interactive-apps`
  - Replace `engineering-ml-features` → `data-science-feature-engineering`
  - Replace `evaluating-ml-models` → `data-science-model-evaluation`
  - Replace `working-in-notebooks` → `data-science-notebooks`
  - Apply in both sections: **When NOT to use this skill** and **Related skills**.

- Note: Observations
  - I constrained review to ticket scope (broken refs in `analyzing-data`) and checked current repo state plus `git diff`/`git log`.
  - No implementation changes for this ticket were detectable in the current working tree/commit history.

- Gate: Fail
