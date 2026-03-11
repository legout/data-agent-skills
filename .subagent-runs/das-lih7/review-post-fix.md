## Review

- What's correct
  - The previously reported wording issue in `eval/README.md` is fixed: it now correctly says the manifests are for the **14 target skills**.
  - The target-skill manifest pairs exist (task + trigger) for the 14 refactored skill names.
  - JSON syntax is valid for manifests checked in `eval/*.json` and `eval/trigger-eval/*.json`.

- Issue [Major]: Scope mismatch with the ticket/plan remains, file: `eval/*.json`, `eval/trigger-eval/*.json`, suggested fix: remove or relocate non-target legacy manifests so this ticket delivers only the 14 target-skill pairs (28 JSON files total) described in `SKILL_REFACTORING_PLAN.md` Section 10.
  - Observed in current changes: `eval/` has 43 task manifests and `eval/trigger-eval/` has 43 trigger manifests, including 29 extra legacy-named skills (e.g., `data-engineering-core`, `data-science-eda`, `flowerpower`, etc.).
  - Risk: downstream eval tooling that scans directories may run against unintended manifests, making ticket behavior and metrics ambiguous.

- Note: Quick re-check focus was on post-fix safety. While the README minor is resolved, the manifest set is not an unambiguous 14-skill-only scaffold, so I cannot confidently give a clean close.

- Gate: Uncertain
