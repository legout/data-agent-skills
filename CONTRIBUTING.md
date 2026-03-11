# Contributing Guide

Thank you for contributing to the data platform agent skills library. This document provides guidance for contributors on templates, evaluations, and code quality.

---

## Quick Start

1. **Fork and clone** the repository
2. **Install dependencies**: `pip install pyyaml` (for lint tool)
3. **Read the authoring standards**: [docs/skill-authoring.md](docs/skill-authoring.md)
4. **Make your changes**
5. **Run the linter**: `python3 tools/skill_lint.py --strict`
6. **Submit a pull request**

---

## Skill Development

### Standards

All skills must follow the standards documented in:

- **[docs/skill-authoring.md](docs/skill-authoring.md)** — Frontmatter policy, naming rules, reference patterns, cross-skill routing
- **[docs/TAXONOMY.md](docs/TAXONOMY.md)** — Framework/tool disposition matrix
- **[docs/skill-map.md](docs/skill-map.md)** — Current skill taxonomy

### Templates

Use the provided templates when creating new skills or references:

| Template | Use For |
|----------|---------|
| [docs/templates/skill-template.md](docs/templates/skill-template.md) | New SKILL.md files |
| [docs/templates/reference-template.md](docs/templates/reference-template.md) | New reference documents |

### Naming Conventions

Skills must use **action-oriented, kebab-case names**:

| ✅ Good | ❌ Bad |
|---------|--------|
| `building-data-pipelines` | `data-engineering-core` |
| `accessing-cloud-storage` | `data-engineering-storage-remote-access` |
| `evaluating-ml-models` | `data-science-model-evaluation` |

See [docs/skill-authoring.md#skill-naming-rules](docs/skill-authoring.md#skill-naming-rules) for complete naming rules.

---

## Evaluation Requirements

### Evaluation Infrastructure

Each skill should include evaluation manifests in the `evals/` directory:

```
evals/
├── {skill-name}.json       # Task evaluations (required)
└── trigger-evals/          # Trigger evaluations (recommended)
    └── {skill-name}.json
```

### Evaluation Manifest Format

Task evaluations (`evals/{skill-name}.json`) — **Required**:

```json
{
  "skill": "building-data-pipelines",
  "version": "2.0.0",
  "evaluations": [
    {
      "id": "etl-basic",
      "task": "Build a batch ETL pipeline using Polars",
      "criteria": ["uses lazy evaluation", "handles errors", "includes validation"],
      "tags": ["polars", "etl"]
    }
  ]
}
```

Trigger evaluations (`evals/trigger-evals/{skill-name}.json`) — **Recommended but optional**:

```json
{
  "skill": "building-data-pipelines",
  "positive": [
    "How do I build a data pipeline with Polars?",
    "ETL best practices for production"
  ],
  "negative": [
    "How do I visualize data?",
    "Streamlit vs Gradio"
  ]
}
```

### Evaluation Checklist

Before submitting a skill:

- [ ] 3–5 task evaluations defined (required)
- [ ] 10–20 trigger evaluations defined (recommended, optional)
- [ ] Evaluations test correct skill triggering
- [ ] Evaluations verify output quality vs. no-skill baseline

---

## Lint Requirements

### Running the Linter

```bash
# Basic lint check
python3 tools/skill_lint.py

# Strict mode (treats warnings as errors) — required for CI
python3 tools/skill_lint.py --strict
```

### Lint Checks

| Check | Level | Description |
|-------|-------|-------------|
| Frontmatter validity | Error | `name` and `description` required |
| Name format | Error | Kebab-case, ≤64 chars, matches directory |
| Non-standard frontmatter | Warning | Fields other than `name`, `description`, `license`, `compatibility`, `metadata`, `allowed-tools` |
| SKILL.md size | Warning | >500 lines triggers warning |
| Broken local references | Error (strict) | Missing markdown file references |
| Python syntax | Error | Invalid Python in fenced code blocks |
| Hybrid notation | Error | `@skill/path` patterns not allowed |
| Duplicate content | Warning | Same block in 3+ files with >100 total lines |
| Missing TOC | Warning | References >100 lines without Table of Contents |
| Stale year markers | Warning | `(YYYY)` in h1/h2 headings |

### Fixing Common Issues

**Broken local references:**
```markdown
<!-- Instead of relative paths to other skills: -->
See [../other-skill/SKILL.md](../other-skill/SKILL.md)

<!-- Use plain skill names: -->
See `other-skill-name`
```

**Hybrid notation:**
```markdown
<!-- Instead of: -->
See @designing-data-storage/formats.md

<!-- Use: -->
See `designing-data-storage` or [formats](./references/formats.md)
```

**Missing TOC in long references:**
```markdown
<!-- Add at top of references/*.md files >100 lines: -->
## Table of Contents

- [Overview](#overview)
- [Examples](#examples)
- [See Also](#see-also)
```

---

## Pull Request Guidelines

### PR Checklist

Before submitting:

- [ ] `python3 tools/skill_lint.py --strict` passes
- [ ] Task evaluation manifest exists in `evals/` (required)
- [ ] Trigger evaluation manifest in `evals/trigger-evals/` (recommended)
- [ ] Templates followed (if creating new files)
- [ ] No duplicate content introduced
- [ ] Related skills documented in SKILL.md
- [ ] Migration notes added (if replacing existing skill)

### PR Description Template

```markdown
## Summary
Brief description of changes.

## Type of Change
- [ ] Bug fix
- [ ] New skill
- [ ] Skill update
- [ ] Documentation
- [ ] Tooling

## Testing
- [ ] `skill_lint.py --strict` passes
- [ ] Evaluations added/updated
- [ ] Tested skill triggering locally

## Migration Notes
If this replaces/modifies an existing skill, document the mapping.
```

---

## Development Workflow

### Local Testing

1. **Install skill locally**:
   ```bash
   npx skills add . --skill {skill-name}
   ```

2. **Test skill triggering**:
   - Start pi with the skill installed
   - Verify it triggers on expected prompts
   - Verify it does NOT trigger on unrelated prompts

3. **Verify references**:
   - Check all local markdown links work
   - Verify no broken file references

### Skill Structure Validation

```bash
# Verify skill structure
ls skills/{skill-name}/
# Expected: SKILL.md, references/, scripts/ (optional), assets/ (optional)

# Verify references have TOCs
head -20 skills/{skill-name}/references/*.md
```

---

## Code of Conduct

- Be respectful and constructive in reviews
- Focus feedback on the skill, not the author
- Suggest specific improvements rather than vague criticism
- Help maintain the quality bar — skills should be genuinely useful

---

## Questions?

- **Authoring standards**: See [docs/skill-authoring.md](docs/skill-authoring.md)
- **Taxonomy decisions**: See [docs/TAXONOMY.md](docs/TAXONOMY.md)
- **Skill mapping**: See [docs/migration-map.md](docs/migration-map.md)
- **Changelog**: See [CHANGELOG.md](CHANGELOG.md)

---

## License

By contributing, you agree that your contributions will be licensed under the same license as the project.
