# Implementation Summary: das-nd1t

## Overview
Created the `evaluating-ml-models` skill by refactoring model evaluation content from `analyzing-data/references/` into a self-contained skill, following the pattern from `engineering-ml-features`.

## Files Created

### 1. Main Skill File
- **`skills/evaluating-ml-models/SKILL.md`** (6,955 bytes)
  - YAML frontmatter with `name: evaluating-ml-models`, description, and `dependsOn: ["@engineering-ml-features", "@data-engineering-core"]`
  - When to use / When NOT to use sections
  - Quick tool selection table (CV, metrics, tuning, tracking)
  - Evaluation workflow (5 steps: CV strategy, metrics, analysis, HPO, tracking)
  - Core implementation rules with code examples
  - Common anti-patterns table
  - Progressive disclosure pointing to local references/
  - Related skills section

### 2. Reference Files (copied from analyzing-data)
- **`skills/evaluating-ml-models/references/cross-validation.md`** (809 bytes)
- **`skills/evaluating-ml-models/references/metrics-guide.md`** (929 bytes)
- **`skills/evaluating-ml-models/references/hyperparameter-tuning.md`** (1,214 bytes)
- **`skills/evaluating-ml-models/references/experiment-tracking.md`** (667 bytes)

### 3. Evaluation Suite
- **`evals/evaluating-ml-models.json`** (9,145 bytes)
  - 5 task_evals covering CV strategy, metrics selection, Optuna tuning, MLflow tracking, regression metrics
  - 20 trigger_evals (10 positive, 10 negative)
  - Negative triggers route to engineering-ml-features or analyzing-data to avoid overlap

## Files Modified

### 1. data-science-model-evaluation/SKILL.md
- Added deprecation notice at top: "[DEPRECATED] Use `@evaluating-ml-models` instead"
- Updated `dependsOn` to reference new skill
- Updated progressive disclosure section to remove broken references and point to new skill

## Key Design Decisions

1. **analyzing-data SKILL.md unchanged** - It did not reference the moved files (cross-validation.md, etc.), so no changes needed

2. **DependsOn chain** - `evaluating-ml-models` depends on `engineering-ml-features` (following the pattern where evaluation comes after feature engineering)

3. **Deprecation not removal** - Kept `data-science-model-evaluation` with deprecation notice for backward compatibility

4. **Trigger differentiation** - Clear separation in evals:
   - `evaluating-ml-models`: CV, metrics, tuning, tracking, model comparison
   - `engineering-ml-features`: encoding, scaling, feature selection, text features

## Verification

All files verified:
- Directory structure: `skills/evaluating-ml-models/references/` created
- 4 reference files copied with identical content
- SKILL.md renders with proper YAML frontmatter
- Evals file is valid JSON with correct structure
- No broken references remain
