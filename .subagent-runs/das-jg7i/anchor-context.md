# Anchor Context for das-jg7i

## Ticket Summary
Create analyzing-data by merging EDA and visualization guidance. All dependencies completed; remaining work is fixing broken related-skill references.

## Complexity Assessment
**Simple** - Dependencies (das-qee5, das-qdy8, das-3wu8) already:
- Created analyzing-data skeleton
- Consolidated 126 duplicate references → 21 shared files
- Merged EDA and visualization content
- Deleted legacy data-science-eda and data-science-visualization directories

## Remaining Issue
The analyzing-data skill's "Related skills" section references future skill names that don't exist yet:
- `building-data-apps` → should be `data-science-interactive-apps`
- `engineering-ml-features` → should be `data-science-feature-engineering`
- `evaluating-ml-models` → should be `data-science-model-evaluation`
- `working-in-notebooks` → should be `data-science-notebooks`

These will become correct AFTER tickets das-r62z, das-hoav, das-u0hp, and das-nd1t are completed. For now, must use current skill names.

## Research Gaps
None - straightforward reference update.

## External Libraries
None required.

## Testing Requirements
- Eval already exists: `eval/analyzing-data.json`
- Post-fix review to verify no broken refs

## File Hints
- `skills/analyzing-data/SKILL.md` lines 139-142 (Related skills section)

## Recommended Path
**Path A (Minimal)** - Single file edit to fix 4 broken references.
