# Implementation: das-b143

## Summary

Created `docs/skill-authoring.md` documenting the standard frontmatter policy and dependsOn decision.

## Files Changed

### New Files
- `docs/skill-authoring.md` - Skill authoring guide (220 lines)

## Implementation Details

### 1. Allowed Frontmatter Fields

Documented required and optional fields:
- **Required**: `name`, `description`
- **Optional**: `license`, `compatibility`, `metadata`, `allowed-tools`
- **Prohibited**: `dependsOn`

### 2. dependsOn Removal Decision

Recorded the decision from SKILL_REFACTORING_PLAN.md section 9.3:
- **Context**: 27 of 29 skills used dependsOn
- **Decision**: Remove from all skill frontmatter
- **Rationale**: Lint compliance, Claude best practices, transparent dependencies, runtime portability
- **Replacement**: Related-skill routing in skill body

### 3. Related-Skill Routing Patterns

Documented correct patterns:
- Use plain skill names in backticks
- Include "Related skills" section in SKILL.md
- No hybrid @skill/path notation
- No relative paths to other skills

### 4. Additional Standards

Included:
- Skill naming rules (action-oriented, 2-4 words, verb conventions)
- Reference standards (direct linking, no nested mazes, TOC for >100 lines)
- Description strategy (third person, trigger keywords)
- Lint compliance guidance

## Verification

- Content cross-checked against `tools/skill_lint.py`
- All acceptance criteria from ticket addressed
- No code changes required (documentation only)
