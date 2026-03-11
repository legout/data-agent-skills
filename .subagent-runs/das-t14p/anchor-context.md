Done! I've created `/Users/volker/coding/libs/data-agent-skills/anchor-context.md` with the implementation context for ticket das-t14p.

## Summary

**Ticket**: das-t14p - Publish migration map, changelog, and contributor guidance

**Key Findings**:
- **Status**: Simple documentation task (Path A)
- **Files to create**:
  1. `docs/migration-map.md` - or enhance existing `docs/skill-map.md` migration section
  2. `CHANGELOG.md` - does NOT exist, create at root
  3. `CONTRIBUTING.md` - does NOT exist, create at root

**No research needed** - All source materials already exist:
- `SKILL_REFACTORING_PLAN.md` has complete migration tables (sections 6.1/6.2)
- `docs/skill-map.md` already has migration mapping
- `docs/skill-authoring.md` has authoring standards
- `docs/templates/` has skill/reference templates
- Eval infrastructure already exists in `eval/` and `evals/`

**Lessons applied** from `.tf/AGENTS.md`:
- Single Source of Truth - link to existing docs rather than duplicating
- Progressive Disclosure - push detail to reference docs
- Canonical Skill Names - use plain names in documentation