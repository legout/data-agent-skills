# Anchor Context: das-y3ig

## Ticket Summary

Convert the `data-engineering` hub skill to non-triggerable documentation so it no longer competes in triggering with the new workflow-centered skills.

## Complexity Assessment

**Simple (Path A)** - Configuration changes only:
- Modify `skills/data-engineering/SKILL.md` frontmatter to remove triggerability
- Update description to clearly indicate it's a docs-only index
- Verify no dangling references remain

## Research Gaps

None. The migration mapping is already documented in `docs/skill-map.md`.

## External Libraries

None required.

## Testing Requirements

None. This is a documentation/configuration change with no executable code.

## Recommended Path

**Path A (Minimal)** - This is a straightforward config change:
1. Modify frontmatter to make the skill non-triggerable
2. Update content to clearly mark as deprecated/docs-only
3. Verify references point to the new skills

## File Hints

- `skills/data-engineering/SKILL.md` - Main file to modify (change description, add deprecation notice)
- `docs/skill-map.md` - Already contains migration mapping (reference only)
- `docs/TAXONOMY.md` - May need to verify consistency

## Acceptance Criteria Mapping

1. ✅ `docs/skill-map.md` replaces the triggerable hub behavior - Already exists with complete mapping
2. 🔄 Old-to-new engineering skill routing documented - In skill-map.md, verify hub references it
3. 🔄 Old hub clearly marked/removed from trigger path - Modify SKILL.md frontmatter

## Implementation Notes

The key change is modifying the frontmatter description. Current description triggers on any data engineering question. Need to change it to:
- Indicate it's a docs-only index
- Not compete in triggering
- Point users to the new workflow-centered skills

Option A: Change description to "DEPRECATED: Index-only reference. Use specific skills like @building-data-pipelines instead."
Option B: Remove the skill entirely (may break existing references)
Option C: Rename skill and update description to be clearly non-triggerable
