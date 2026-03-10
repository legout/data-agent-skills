# Skill Authoring Guide

This document defines the standards for authoring skills in the data agent skills library. It covers frontmatter policy, skill naming, reference patterns, and cross-skill routing.

---

## Frontmatter Policy

Every `SKILL.md` file must include YAML frontmatter at the top of the file.

### Required Fields

| Field | Type | Description |
|-------|------|-------------|
| `name` | string | The skill identifier. Must match the directory name. Use kebab-case (lowercase with hyphens). Max 64 characters. |
| `description` | string | A concise explanation of what the skill does and when to use it. Max 1024 characters. Use third person. Include trigger keywords. |

### Optional Fields

The following optional fields are recognized by the lint tool and may be used when appropriate:

| Field | Type | Use When |
|-------|------|----------|
| `license` | string | The skill has specific licensing terms or dependencies with license requirements. |
| `compatibility` | string | The skill has known compatibility constraints (e.g., Python versions, platform requirements). |
| `metadata` | object | Additional structured metadata needed by tooling (use sparingly). |
| `allowed-tools` | list | Explicit allowlist of tools the skill may invoke (rarely needed). |

### Prohibited Fields

The following fields are **explicitly deprecated** and must not be used:

| Field | Status | Rationale |
|-------|--------|-----------|
| `dependsOn` | **REMOVED** | See [Decision: Removal of dependsOn](#decision-removal-of-dependson) below. |

### Example Frontmatter

```yaml
---
name: building-data-pipelines
description: |
  Guides users through building batch ETL pipelines using Polars, DuckDB, 
  and PyArrow. Use when constructing data transformation workflows, 
  choosing between dataframe libraries, or designing production pipeline 
  architecture. Triggers on: ETL, data pipeline, batch processing, 
  Polars, DuckDB, PyArrow, data transformation.
---
```

---

## Decision: Removal of dependsOn

### Context

Historically, many skills included a `dependsOn` field in frontmatter to declare dependencies on other skills. At the time of this decision, **27 of 29 skills** used this field.

### Decision

**Remove `dependsOn` from all skill frontmatter.**

### Rationale

1. **Lint Compliance**: The skill lint tool (`tools/skill_lint.py`) warns on `dependsOn` as a non-standard frontmatter field. Standardizing on portable metadata improves compatibility across different agent runtimes.

2. **Claude Best Practices**: The official Claude Agent Skills guidance emphasizes simple, standard frontmatter. Complex dependency declarations belong in the skill body where they can be contextualized.

3. **Transparent Dependencies**: Dependencies can be expressed more clearly through explicit related-skill routing in the skill body, rather than through opaque metadata.

4. **Runtime Portability**: Not all agent runtimes support dependency resolution through frontmatter. Body-level routing works everywhere.

### What Replaces dependsOn

Instead of `dependsOn`, use explicit **related-skill routing** in the `SKILL.md` body:

```markdown
## When to use this skill

Use this skill for cloud storage authentication and access patterns.

## Related skills

- For storage format decisions, see `designing-data-storage`
- For ETL pipeline construction, see `building-data-pipelines`
```

---

## Skill Naming Rules

Skill names must follow these conventions:

### Rule 1: Use Action-Oriented Names

Start with a **verb** that describes what the user is doing:

| ✅ Good | ❌ Bad |
|---------|--------|
| `building-data-pipelines` | `data-engineering-core` |
| `accessing-cloud-storage` | `data-engineering-storage-remote-access` |
| `evaluating-ml-models` | `data-science-model-evaluation` |

### Rule 2: Keep Names Short

Target **2-4 words** maximum. Avoid deep taxonomic nesting.

| ✅ Good | ❌ Bad |
|---------|--------|
| `accessing-cloud-storage` | `data-engineering-storage-remote-access-integrations-polars` |
| `designing-data-storage` | `data-engineering-storage-formats-and-lakehouse` |

### Rule 3: Use Consistent Verb Conventions

| Verb | Use When |
|------|----------|
| `building-*` | Constructing pipelines, systems, or infrastructure |
| `accessing-*` | Connecting to, authenticating with, or reading from external systems |
| `designing-*` | Making architectural decisions, selecting formats, or planning storage |
| `managing-*` | Administrative, catalog, or metadata operations |
| `orchestrating-*` | Scheduling, coordination, and workflow management |
| `assuring-*` | Quality, validation, monitoring, and operational safety |
| `engineering-*` | Specialized technical construction (features, AI pipelines) |
| `analyzing-*` | Exploration, EDA, and insight generation |
| `evaluating-*` | Measurement, comparison, and assessment |
| `working-in-*` | Environment-specific workflows (notebooks) |
| `using-*` | Framework-specific dedicated workflows (e.g., FlowerPower) |

### Rule 4: Use Kebab-Case

All skill names use **kebab-case** (lowercase with hyphens):

| ✅ Good | ❌ Bad |
|---------|--------|
| `building-data-apps` | `buildingDataApps`, `building_data_apps` |

### Technical Constraints

- Maximum 64 characters
- Only lowercase letters, numbers, and hyphens
- Cannot start or end with a hyphen
- Cannot contain consecutive hyphens (`--`)
- Must match the directory name exactly

---

## Related-Skill Routing

When a skill needs to reference another skill, use **plain skill names**, not hybrid notation.

### ✅ Correct Patterns

```markdown
For storage format decisions, see `designing-data-storage`.

If you need orchestration patterns, use `orchestrating-data-pipelines`.

See also: `building-data-pipelines`, `accessing-cloud-storage`
```

### ❌ Incorrect Patterns

```markdown
Do not use hybrid @skill/path notation:
- @designing-data-storage/formats.md  ← AMBIGUOUS
- @building-data-pipelines            ← Acceptable but prefer plain name

Do not use relative paths to other skills:
- ../designing-data-storage/SKILL.md  ← BREAKS IF STRUCTURE CHANGES
```

### Routing Guidance

1. **Use plain skill names** in backticks for inline references
2. **Include a "Related skills" or "When not to use" section** in every SKILL.md
3. **Be explicit about boundaries** — clearly state when to use this skill vs another
4. **Do not nest references** — link directly from SKILL.md, not through intermediate routing files

---

## Reference Standards

### File Structure

```
skill-name/
├── SKILL.md              # Main entry point with workflow guidance
├── references/
│   ├── <topic>.md        # Deep-dive reference documents
│   └── ...
├── scripts/
│   ├── <utility>.py      # Validation, scaffolding, or utility scripts
│   └── ...
└── assets/               # Only if truly needed
```

### Reference File Requirements

1. **Direct linking**: References must be linked directly from `SKILL.md`
2. **No nested mazes**: Users should not navigate through multiple reference files to find content
3. **No hybrid notation**: Use plain file paths for local references, plain skill names for cross-skill references
4. **Table of contents**: Every reference over 100 lines must include a TOC
5. **Quality bar**: Every reference must be either:
   - A substantial practical deep-dive with examples, or
   - A routing page with strong outbound links and clear "when to read this" guidance

### Small-File Policy

Avoid 30–50 line stub references:

- **Major topics**: Expand into full deep references
- **Minor topics**: Merge into broader thematic references
- **Neither substantial nor frequently used**: Delete

---

## Description Strategy

Every skill description must:

1. **Be in third person** — "Guides users..." not "Guide users..."
2. **State what the skill does**
3. **State when it should be used**
4. **Include trigger language** — keywords users actually type
5. **Avoid vague phrasing** — no "comprehensive suite" or "powerful toolkit"

### Example

```yaml
description: |
  Guides users through building batch ETL pipelines using Polars, DuckDB, 
  and PyArrow. Use when constructing data transformation workflows, 
  choosing between dataframe libraries, or designing production pipeline 
  architecture. Triggers on: ETL, data pipeline, batch processing, 
  Polars, DuckDB, PyArrow, data transformation.
```

---

## Lint Compliance

The `tools/skill_lint.py` script validates skill files. Run it regularly:

```bash
python3 tools/skill_lint.py

# For CI, treat warnings as errors:
python3 tools/skill_lint.py --strict
```

### Current Lint Checks

- Frontmatter has required fields (`name`, `description`)
- Name format compliance (kebab-case, length, valid characters)
- No non-standard frontmatter fields (warns on `dependsOn`)
- No broken local markdown references
- Python code blocks have valid syntax
- SKILL.md not oversized (>500 lines warning)

---

## Summary Checklist

When authoring or updating a skill:

- [ ] Frontmatter has `name` and `description` only (or approved optional fields)
- [ ] No `dependsOn` field in frontmatter
- [ ] Skill name matches directory name
- [ ] Description uses third person and includes trigger keywords
- [ ] Related skills are documented with plain skill names (not hybrid notation)
- [ ] Local references use plain file paths
- [ ] References >100 lines have a table of contents
- [ ] No broken local references
- [ ] Skill lint passes (`python3 tools/skill_lint.py`)
