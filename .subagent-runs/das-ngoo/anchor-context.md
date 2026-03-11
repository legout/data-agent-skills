I've written the anchor-context.md file for ticket das-ngoo. Here's a summary:

**Ticket: das-ngoo** - Finalize the 14-skill taxonomy, naming rules, templates, and dependsOn policy

**Key Findings:**
- This is a **simple** task - all major artifacts already exist from prior tickets:
  - `docs/skill-map.md` (das-3jql) - 14-skill taxonomy + naming conventions
  - `docs/skill-authoring.md` (das-b143) - Frontmatter policy + dependsOn removal decision  
  - `docs/templates/` (das-xl5m) - SKILL.md + reference templates
  - `eval/` (das-lih7) - 14-skill evaluation manifests

**Recommended Path: A (Minimal)** - The work is likely verification/consolidation rather than creation:
1. Verify docs are consistent and complete
2. Mark artifacts as "published/authoritative"
3. Ensure cross-references are correct
4. Run lint to verify no regressions

**No research gaps** - All key artifacts are in place.

**Testing:** Run `python3 tools/skill_lint.py --strict` + verify JSON manifests are valid.