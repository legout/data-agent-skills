# Skill Templates

This directory contains reusable templates for creating skills in the data agent skills library. These templates enforce the [skill-authoring standards](../skill-authoring.md) and implement the [SKILL_REFACTORING_PLAN.md](../../SKILL_REFACTORING_PLAN.md) architecture.

## Templates Overview

| Template | Purpose | When to Use |
|----------|---------|-------------|
| [`skill-template.md`](./skill-template.md) | Main skill entry point | Creating a new `SKILL.md` file |
| [`reference-template.md`](./reference-template.md) | Deep-dive reference docs | Creating references in `references/` |

## Core Principles

### 1. Direct Linking

All references must be linked **directly from SKILL.md**. Users should never navigate through multiple reference files to find content.

✅ **Correct:**
```markdown
## Progressive Disclosure

- **[Topic](./references/topic.md)** — Deep dive on specific subject
- **[Another Topic](./references/another.md)** — Related deep dive
```

❌ **Incorrect:**
```markdown
## References

See [routing.md](./references/routing.md) for all topics.
```

### 2. Progressive Disclosure

Structure content in layers:

| Layer | Location | Content |
|-------|----------|---------|
| **Quick decisions** | SKILL.md Decision Checklist | Routing table for skill selection |
| **Workflow** | SKILL.md Core Workflow | High-level operating procedure |
| **Quick reference** | SKILL.md Progressive Disclosure | Links to detailed references |
| **Deep dives** | `references/*.md` | Comprehensive topic coverage |

**Rule:** Put decision-making content in SKILL.md. Put implementation details in references.

### 3. No Hybrid Notation

Use plain identifiers for all references:

| Type | Format | Example |
|------|--------|---------|
| Local files | Plain relative paths | `./references/topic.md` |
| Cross-skill | Plain skill names in backticks | `` `other-skill` `` |

❌ **Never use:**
- `@skill-name/path.md` — Hard to lint, ambiguous semantics
- `../other-skill/SKILL.md` — Breaks if structure changes

### 4. Table of Contents for Long References

Any reference file over 100 lines must include a TOC:

```markdown
## Table of Contents

- [Section 1](#section-1)
- [Section 2](#section-2)
```

Use your editor's markdown TOC generator or add manually.

## Using the Templates

### Creating a New Skill

1. Create the skill directory:
   ```bash
   mkdir -p skills/your-skill-name/{references,scripts}
   ```

2. Copy and customize the template:
   ```bash
   cp docs/templates/skill-template.md skills/your-skill-name/SKILL.md
   ```

3. Fill in all `[bracketed placeholders]`

4. Update frontmatter:
   - `name`: Must match directory name (kebab-case)
   - `description`: Third person, include trigger keywords

5. Run the linter:
   ```bash
   python3 tools/skill_lint.py
   ```

### Creating a New Reference

1. Determine if the topic warrants a standalone reference:
   - **Major topic** used frequently → Create full reference
   - **Minor topic** or edge case → Merge into broader reference
   - **Neither substantial nor used** → Don't create

2. Copy the template:
   ```bash
   cp docs/templates/reference-template.md skills/your-skill/references/topic-name.md
   ```

3. Fill in all sections, removing unused ones

4. Add the reference to SKILL.md's Progressive Disclosure section

5. Include a TOC if the file exceeds 100 lines

## File Structure

Every skill should follow this structure:

```
skill-name/
├── SKILL.md              # Main entry (use skill-template.md)
├── references/
│   ├── topic.md          # Deep dives (use reference-template.md)
│   └── ...
├── scripts/
│   ├── validate.py       # Validation utilities
│   └── ...
└── assets/               # Only if truly needed
```

## Lint Compliance

All skills must pass the skill linter:

```bash
# Basic check
python3 tools/skill_lint.py

# CI check (treats warnings as errors)
python3 tools/skill_lint.py --strict
```

### Current Lint Checks

- Frontmatter has required fields (`name`, `description`)
- Name format compliance (kebab-case, length, valid characters)
- No non-standard frontmatter fields (warns on `dependsOn`)
- No broken local markdown references
- Python code blocks have valid syntax
- SKILL.md not oversized (>500 lines warning)

## Frontmatter Standards

### Required Fields

```yaml
---
name: skill-name
description: |
  [Third-person, max 1024 chars]. Use when [scenarios].
  Triggers on: [keywords].
---
```

### Optional Fields (When Needed)

- `license` — Specific licensing terms
- `compatibility` — Platform/version constraints
- `metadata` — Structured tooling metadata (use sparingly)
- `allowed-tools` — Explicit tool allowlist (rarely needed)

### Deprecated Fields

- ❌ `dependsOn` — Removed per [skill-authoring.md](../skill-authoring.md#decision-removal-of-dependson). Use related-skill routing in the body instead.

## Related Skills Routing

When referencing other skills in SKILL.md or references:

```markdown
## Related Skills

- `[building-data-pipelines]` — For ETL construction workflows
- `[accessing-cloud-storage]` — For cloud storage authentication
```

Or inline:
```markdown
For storage format decisions, see `[designing-data-storage]`.
```

## Quality Checklist

Before submitting a new skill:

- [ ] Frontmatter has `name` and `description` only (or approved optional fields)
- [ ] No `dependsOn` field in frontmatter
- [ ] Skill name matches directory name
- [ ] Description uses third person and includes trigger keywords
- [ ] Related skills documented with plain skill names (not hybrid notation)
- [ ] Local references use plain file paths
- [ ] References >100 lines have a table of contents
- [ ] No broken local references
- [ ] Skill lint passes (`python3 tools/skill_lint.py`)

## Further Reading

- [Skill Authoring Guide](../skill-authoring.md) — Full authoring standards
- [Skill Map](../skill-map.md) — Library organization and taxonomy
- [SKILL_REFACTORING_PLAN.md](../../SKILL_REFACTORING_PLAN.md) — Architecture decisions
