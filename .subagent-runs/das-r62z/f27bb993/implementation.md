# Implementation: working-in-notebooks Skill

## Summary

Successfully created the `working-in-notebooks` skill following the SKILL_REFACTORING_PLAN.md standards and plan.md requirements.

## Files Created

### 1. SKILL.md
**Location:** `skills/working-in-notebooks/SKILL.md`

**Key features:**
- **Frontmatter:** Only `name` and `description` (no `dependsOn` per lint standards)
- **When to use:** Clear trigger boundaries for notebook workflows
- **When NOT to use:** Explicit boundary documentation vs `building-data-apps`, `analyzing-data`, etc.
- **Tool selection guide:** Decision checklist and comparison table for Jupyter/marimo/VS Code/Colab
- **Core workflow:** Step-by-step reproducible notebook creation
- **Progressive disclosure:** Direct file paths to references (no `@skill` hybrid syntax)
- **Related skills table:** Clear relationship documentation

### 2. references/jupyter-guide.md
**Location:** `skills/working-in-notebooks/references/jupyter-guide.md`

**Content:**
- Table of Contents (over 100 lines)
- Jupyter vs JupyterLab comparison
- Magic commands (%timeit, %debug, %watermark, etc.)
- Cell types and best practices
- Kernel management
- Widgets and interactivity (ipywidgets)
- VS Code integration
- Google Colab specifics
- Extensions
- Troubleshooting

### 3. references/marimo-guide.md
**Location:** `skills/working-in-notebooks/references/marimo-guide.md`

**Content:**
- Table of Contents (over 100 lines)
- Reactive execution model explanation
- Pure Python format benefits
- UI components (sliders, dropdowns, tables)
- State management patterns
- Converting from Jupyter (`marimo convert`)
- Running marimo (edit mode, run mode)
- Version control best practices
- Marimo vs Jupyter decision guide
- Advanced features

### 4. references/reproducibility-patterns.md
**Location:** `skills/working-in-notebooks/references/reproducibility-patterns.md`

**Content:**
- Table of Contents (over 100 lines)
- Reproducibility checklist
- Setting random seeds (numpy, torch, tensorflow)
- Environment management (venv, conda, uv, poetry)
- Dependency pinning strategies
- Data versioning (DVC, Git LFS)
- Container patterns (Docker)
- Git and pre-commit hooks (nbstripout)
- Secrets management (.env, cloud secret managers)
- Avoiding hardcoded paths (pathlib)
- Validation and testing

### 5. evals/working-in-notebooks.json
**Location:** `evals/working-in-notebooks.json`

**Content:**
- 5 task evaluations (convert Jupyter to marimo, make reproducible, magic commands, reactive execution, git best practices)
- 20 trigger evaluations (10 positive, 10 negative boundary cases)
- Explicit boundary testing vs `building-data-apps` and `analyzing-data`

## Key Design Decisions

### 1. No `dependsOn` in Frontmatter
Following the SKILL_REFACTORING_PLAN.md section 9.3, removed `dependsOn` (non-standard field that triggers lint warnings).

### 2. Direct File Paths for References
All references use plain file paths:
- `references/jupyter-guide.md` (local)
- `../analyzing-data/references/notebook-testing.md` (cross-skill)

No hybrid `@skill/path` notation used.

### 3. Clear Boundary Documentation
Explicit "When NOT to use" section with:
- Comparison table vs related skills
- Quick boundary check (notebook vs app distinction)
- Related skills table at end

### 4. Progressive Disclosure
SKILL.md is concise (~350 lines) with deep details in references:
- Jupyter deep dive: ~430 lines
- Marimo deep dive: ~480 lines
- Reproducibility patterns: ~580 lines

### 5. External References Preserved
Links to existing references in `analyzing-data` skill:
- `../analyzing-data/references/notebook-testing.md`
- `../analyzing-data/references/sharing-publishing.md`

These remain in their original location per plan.md.

## Lint Verification

Running `python tools/skill_lint.py`:
- ✅ No errors in `skills/working-in-notebooks/`
- ✅ No warnings in `skills/working-in-notebooks/`
- ✅ All reference files have Table of Contents (over 100 lines)
- ✅ No hybrid `@skill/path` syntax
- ✅ No `dependsOn` in frontmatter

Remaining errors in output are from:
- `.subagent-runs/` directories (temporary implementation files)
- Other existing skills (not part of this implementation)

## Migration from data-science-notebooks

This skill replaces `data-science-notebooks` with these changes:

| From | To |
|------|-----|
| `dependsOn: [...]` | Removed |
| Magic commands in SKILL.md | Moved to `references/jupyter-guide.md` |
| Marimo basics in SKILL.md | Expanded to `references/marimo-guide.md` |
| `@skill` references | Plain file paths |
| Implicit boundaries | Explicit when-to-use/when-not-to-use |

## Acceptance Criteria Verification

1. ✅ `skills/working-in-notebooks/` directory exists with proper structure
2. ✅ SKILL.md has no `dependsOn` in frontmatter
3. ✅ SKILL.md has clear when-to-use and when-not-to-use sections
4. ✅ Boundary vs `building-data-apps` and `analyzing-data` documented
5. ✅ References use direct file paths (no `@skill` hybrid syntax)
6. ✅ Reference files over 100 lines have TOC
7. ✅ `evals/working-in-notebooks.json` exists with task + trigger evals
8. ✅ `python tools/skill_lint.py` passes with zero errors/warnings for new skill
