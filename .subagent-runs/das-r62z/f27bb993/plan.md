# Implementation Plan

## Goal
Create the `working-in-notebooks` skill by refactoring `data-science-notebooks` with direct references for Jupyter, marimo, sharing, and reproducibility, following the SKILL_REFACTORING_PLAN.md standards.

---

## Tasks

### 1. Create new skill directory structure
- **Task**: Create `skills/working-in-notebooks/` directory with standard layout
- **Files to create**:
  - `skills/working-in-notebooks/SKILL.md` (main skill file)
  - `skills/working-in-notebooks/references/` (directory for reference docs)
- **Acceptance**: Directory exists with proper structure

### 2. Refactor SKILL.md following new standards
- **Task**: Rewrite SKILL.md with new structure per SKILL_REFACTORING_PLAN.md section 8.2
- **Changes**:
  - Remove `dependsOn` from frontmatter (currently triggers lint warning)
  - Add `name` and `description` only in frontmatter
  - Add clear **when to use this skill** section
  - Add clear **when NOT to use this skill** section with boundary documentation
  - Add decision checklist for Jupyter vs marimo vs other tools
  - Include core workflow for reproducible notebooks
  - Add validation/feedback loop section
  - Use progressive disclosure with direct file paths to references
  - Document related skills with clear boundaries
- **Acceptance**: SKILL.md passes `python tools/skill_lint.py --strict`

### 3. Create Jupyter reference file
- **Task**: Create `references/jupyter-guide.md` with comprehensive Jupyter/JupyterLab coverage
- **Content to include**:
  - JupyterLab IDE features and extensions
  - Magic commands (%load_ext, %timeit, %debug, %watermark)
  - Cell types and best practices
  - Kernel management
  - Widgets and interactivity (ipywidgets)
  - VS Code + Jupyter integration
  - Google Colab specifics
  - Common anti-patterns
- **Acceptance**: File has TOC (over 100 lines), practical examples, authoritative links

### 4. Create marimo reference file
- **Task**: Create `references/marimo-guide.md` with comprehensive marimo coverage
- **Content to include**:
  - Reactive execution model
  - Pure Python (.py) format advantages
  - UI components (mo.ui.slider, mo.ui.dropdown, etc.)
  - State management differences from Jupyter
  - Version control best practices
  - Converting from Jupyter: `marimo convert`
  - Running marimo: `marimo edit`, `marimo run`
  - When to choose marimo over Jupyter
- **Acceptance**: File has TOC (over 100 lines), practical examples, authoritative links

### 5. Create reproducibility reference file
- **Task**: Create `references/reproducibility-patterns.md`
- **Content to include**:
  - Setting random seeds (numpy, random, torch)
  - Environment management (requirements.txt, environment.yml, uv, poetry)
  - Pinning dependencies
  - Data versioning (DVC mention)
  - Container patterns for notebooks
  - nbstripout and pre-commit hooks
  - Avoiding hardcoded paths and secrets
- **Acceptance**: File has TOC, practical examples

### 6. Document boundaries vs related skills
- **Task**: Add clear boundary documentation in SKILL.md
- **Boundaries to document**:
  - **vs building-data-apps**: Notebooks are for exploration/analysis; apps are for stakeholder-facing interactive tools
  - **vs analyzing-data**: analyzing-data is for EDA patterns; working-in-notebooks is for the notebook environment/workflow
  - **vs engineering-ml-features**: Feature engineering is domain-specific; notebooks are the container
- **Acceptance**: Boundary statements are clear and referenced in when-not-to-use section

### 7. Link to existing references
- **Task**: Update SKILL.md progressive disclosure section with direct paths
- **Links to add**:
  - `../analyzing-data/references/notebook-testing.md` — Unit tests, nbval, papermill
  - `../analyzing-data/references/sharing-publishing.md` — nbconvert, Quarto, Voilà
- **Acceptance**: All referenced files exist at specified paths

### 8. Create eval coverage
- **Task**: Create `evals/working-in-notebooks.json` with evaluation scaffolding
- **Content**:
  - 3-5 task evaluations covering Jupyter workflows, marimo workflows, and reproducibility
  - 10-15 trigger evaluations (positive + negative) distinguishing from building-data-apps and analyzing-data
- **Acceptance**: Eval file follows repo conventions, includes both task and trigger evals

### 9. Verify and clean up
- **Task**: Run lint and verify implementation
- **Commands**:
  - `python tools/skill_lint.py --strict`
- **Acceptance**: Zero errors, zero warnings for new skill

---

## Files to Modify

| File | Changes |
|------|---------|
| `skills/working-in-notebooks/SKILL.md` | Create new following refactored standards |
| `skills/working-in-notebooks/references/jupyter-guide.md` | Create new comprehensive reference |
| `skills/working-in-notebooks/references/marimo-guide.md` | Create new comprehensive reference |
| `skills/working-in-notebooks/references/reproducibility-patterns.md` | Create new comprehensive reference |
| `evals/working-in-notebooks.json` | Create eval scaffolding |

---

## New Files Summary

| File | Purpose |
|------|---------|
| `skills/working-in-notebooks/SKILL.md` | Main skill with workflow guidance and tool selection |
| `skills/working-in-notebooks/references/jupyter-guide.md` | Deep reference for Jupyter/JupyterLab |
| `skills/working-in-notebooks/references/marimo-guide.md` | Deep reference for marimo |
| `skills/working-in-notebooks/references/reproducibility-patterns.md` | Patterns for reproducible notebooks |
| `evals/working-in-notebooks.json` | Task and trigger evaluations |

---

## Dependencies

- Task 1 (directory creation) must complete before Tasks 2-5
- Task 2 (SKILL.md) should reference files created in Tasks 3-5, so draft structure first
- Task 6 (boundaries) is part of Task 2
- Task 7 (links) depends on analyzing-data references existing (verified: they exist)
- Task 8 (evals) can be done in parallel with content creation
- Task 9 (verify) must be last

---

## Risks

1. **Trigger boundary confusion**: Must clearly distinguish `working-in-notebooks` from `building-data-apps`. Mitigation: Explicit when-not-to-use section with examples.

2. **Content overlap with analyzing-data**: analyzing-data has notebook-testing.md and sharing-publishing.md. Mitigation: Keep those as external links, focus working-in-notebooks on environment/workflow.

3. **Reference file length**: If jupyter-guide or marimo-guide exceed scope, they become unwieldy. Mitigation: Focus on practical patterns, link to official docs for exhaustive API coverage.

4. **Missing eval coverage**: Evals must distinguish from similar skills. Mitigation: Include negative trigger examples like "build a dashboard" → should trigger building-data-apps, not working-in-notebooks.

---

## Migration Notes for SKILL.md

The new skill consolidates and replaces `data-science-notebooks`. Key changes:

| From (old) | To (new) |
|------------|----------|
| `dependsOn: ["@data-science-eda", "@data-engineering-core"]` | Removed (non-standard frontmatter) |
| `@data-science-eda` references | `analyzing-data` with proper file paths |
| Tool selection table | Keep and enhance with decision criteria |
| Magic commands section | Move to `references/jupyter-guide.md` |
| marimo section | Expand to `references/marimo-guide.md` |
| Progressive disclosure with `@skill` syntax | Convert to direct file paths |

---

## Acceptance Criteria Summary

1. ✅ `skills/working-in-notebooks/` directory exists with proper structure
2. ✅ SKILL.md has no `dependsOn` in frontmatter
3. ✅ SKILL.md has clear when-to-use and when-not-to-use sections
4. ✅ Boundary vs `building-data-apps` and `analyzing-data` is documented
5. ✅ References use direct file paths (no `@skill` hybrid syntax)
6. ✅ Reference files over 100 lines have TOC
7. ✅ `evals/working-in-notebooks.json` exists with task + trigger evals
8. ✅ `python tools/skill_lint.py --strict` passes with zero errors/warnings
