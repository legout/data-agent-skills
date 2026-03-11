Done. I've written the anchor context to `/Users/volker/coding/libs/data-agent-skills/anchor-context.md`.

## Summary

**Ticket das-6zd4** asks to add three new checks to `tools/skill_lint.py`:

1. **Duplicate content detection** - flag markdown files with duplicated content above a threshold
2. **TOC check** - flag long references (>100 lines) without a table of contents
3. **Stale-year detection** - flag year markers like `(2026)` in headings

### Key Findings:

- **Complexity**: Medium - three independent lint functions, ~100-200 LOC
- **TOC threshold**: Resolved - template specifies 100 lines
- **Stale-year pattern**: Resolved - detect `(2026)` in headings
- **Duplicate threshold**: Research gap - needs definition (suggest 3+ copies or >100 duplicate lines)

### Primary File:
- `tools/skill_lint.py` - add new lint functions

### Dependencies:
- `das-xl5m` (completed) - created templates that define the 100-line TOC threshold